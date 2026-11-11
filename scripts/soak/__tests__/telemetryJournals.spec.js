const crypto = require('node:crypto')
const churn = require('../telemetryChurn')
const { ReferenceWork, ReferenceShareLedger } = require('./fixtures/journalReference')
const { NativeJournal, PendingSampler } = require('../../../testHelpers/processMemory')
const { HandlerJournal } = require('../../../testHelpers/fixedJournal')

const sha = (value) => crypto.createHash('sha256').update(JSON.stringify(value)).digest('hex')

/** Drives the same public handler or producer slots with a deterministic fractional clock. */
const replay = async (Work, kind) => {
  let now = 0.125
  let handler
  const client = {
    events: { ACKNOWLEDGE: 'ack', FETCH: 'fetch' },
    on: () => {},
    subscribe: async () => {},
    run: async ({ eachMessage }) => (handler = eachMessage),
    send: async () => {},
    commitOffsets: async () => {},
  }
  const work = new Work({
    kind,
    client,
    stem: 'journal',
    topic: 'topic-λ',
    ctl: 'ctl',
    clock: { now: () => (now += 0.125) },
    sleep: async (ms) => (now += ms),
    slots: Array.from({ length: 100 }, (_, i) => i * churn.CORPUS.batchMs),
  })
  const before = work.journalState?.()
  await work.start()
  if (kind === 'producer') await work.pace()
  else {
    for (let i = 0; i < churn.CORPUS.records; i++)
      await handler({
        partition: i % 3,
        message: { offset: String(i), key: churn.recordId('journal', kind, i) },
      })
    await work.commit()
  }
  return { work, before }
}

/** Nested wire facts include exact int64 offsets, absent fields, UTF-8 strings and Float64 times. */
const exchange = (n) => ({
  n,
  segment: 'segment-λ',
  socket: 2,
  apiKey: 78,
  at: n + 0.125,
  responseAt: n + 0.625,
  request: {
    groupId: 'group',
    topics: [{ topicId: 'T', partitions: [{ partition: 0, batches: [] }] }],
  },
  response: {
    errorCode: 0,
    lockMs: undefined,
    topics: [
      {
        topicId: 'T',
        partitions: [
          {
            partition: 0,
            fetchErrorCode: 0,
            acquired: [{ firstOffset: '9223372036854775806', lastOffset: '9223372036854775807' }],
          },
        ],
      },
    ],
  },
})

/** Uses the old ledger's independent array writes and facts hash as the replay oracle. */
const shareReplay = (Ledger) => {
  let at = 0.125
  const ledger = new Ledger({ group: 'group', clock: { now: () => (at += 0.125) } })
  const before = ledger.journalState?.()
  for (let n = 1; n <= 100; n++) {
    ledger.exchanges.push(exchange(n))
    ledger.record({
      payload: {
        topic: 'topic-λ',
        partition: 0,
        firstOffset: '9223372036854775806',
        lastOffset: '9223372036854775807',
        type: 'release',
        error: n % 2 ? { name: 'KafkaJSShareAcknowledgementError', reason: 'session-lost' } : null,
      },
    })
  }
  return { ledger, before }
}

/** Fails on any missing, duplicate, reordered or changed archived observation. */
const exact = (expected, actual) => {
  if (JSON.stringify(expected) !== JSON.stringify(actual)) throw new Error('journal replay differs')
}

describe('telemetry fixed journals', () => {
  test('replays the full 10000-record producer and consumer ids without growing storage', async () => {
    for (const kind of ['producer', 'consumer', 'shareConsumer']) {
      const expected = await replay(ReferenceWork, kind)
      const actual = await replay(churn.OrdinaryWork, kind)
      expect(actual.before).toBeDefined()
      expect(actual.work.journalState().byteLength).toBe(actual.before.byteLength)
      exact(expected.work.ids(), actual.work.ids())
      expect(sha(actual.work.ids())).toBe(sha(expected.work.ids()))
    }
  })

  test('replays exact share facts and exchangesSha256 without retaining event strings or objects', () => {
    const expected = shareReplay(ReferenceShareLedger)
    const actual = shareReplay(churn.ShareLedger)
    expect(actual.before).toBeDefined()
    expect(actual.ledger.journalState().byteLength).toBe(actual.before.byteLength)
    exact(expected.ledger.facts(), actual.ledger.facts())
    expect(actual.ledger.facts()).toEqual(expected.ledger.facts())
    expect(actual.ledger.facts().exchangesSha256).toBe(expected.ledger.facts().exchangesSha256)
    const fact = exchange(101)
    actual.ledger.exchanges.push(fact)
    fact.request.groupId = 'mutated'
    expect(actual.ledger.facts().exchanges[100].request.groupId).toBe('group')
  })

  test('fails row and byte overflow before truncating or overwriting any archived fact', () => {
    const journal = new churn.FixedJournal({ name: 'control', capacity: 1, bytes: 128 })
    journal.push({ at: 1.125, text: 'λ' })
    const before = journal.values()
    const cursor = journal.cursor
    const baseline = { journals: journal.journalState() }
    expect(() => journal.push({ at: 2 })).toThrow(/control.*overflow/)
    expect(journal.values()).toEqual(before)
    expect(journal.cursor).toBe(cursor)
    expect(journal.journalState().overflow).toBe(true)
    expect(
      churn.JournalConstancy.violations({
        baseline,
        samples: [],
        teardown: { journals: journal.journalState() },
      })
    ).toContain('journal overflow')
    const columns = new churn.FixedJournal({
      name: 'labels',
      capacity: 70,
      bytes: 8192,
      columns: true,
    })
    columns.push({ groupId: 'λ\ud800', unknown: 'λ' })
    expect(columns.values()).toEqual([{ groupId: 'λ\ud800', unknown: 'λ' }])
    for (let n = 0; n < 63; n++) columns.push({ groupId: String(n) })
    const state = columns.journalState()
    const contents = Buffer.from(columns.storage)
    expect(() => columns.push({ groupId: 'overflow' })).toThrow(/symbol overflow/)
    expect(columns.journalState()).toEqual({ ...state, overflow: true })
    expect(columns.storage.equals(contents)).toBe(true)
    const bytes = new churn.FixedJournal({ name: 'bytes', capacity: 2, bytes: 32 })
    expect(() => bytes.push('x'.repeat(100))).toThrow(/bytes.*overflow/)
    expect(bytes.length).toBe(0)
    expect(bytes.cursor).toBe(0)
  })

  test('fails the verdict when journal storage changes or its cursor leaves capacity', () => {
    const snapshot = (cursor) => ({ journals: { byteLength: 4096, cursor } })
    const input = {
      baseline: snapshot(0),
      samples: [{ snapshot: snapshot(40) }, { snapshot: snapshot(80) }],
      teardown: snapshot(100),
    }
    expect(churn.JournalConstancy.violations(input)).toEqual([])
    input.samples[1].snapshot.journals.byteLength++
    expect(churn.JournalConstancy.violations(input)).toContain('journal storage constancy')
    expect(churn.JournalConstancy.apply({ ok: true, violations: [] }, input)).toMatchObject({
      ok: false,
      outcome: 'failed',
    })
    input.samples[1].snapshot.journals.byteLength--
    input.teardown.journals.cursor = 4097
    expect(churn.JournalConstancy.violations(input)).toContain('journal cursor bounds')
    delete input.teardown.journals
    expect(churn.JournalConstancy.violations(input)).toContain('journal storage constancy')
  })

  test('keeps native and pending-window storage constant and preserves every scalar outcome', () => {
    const memory = { current: () => ({ at: 1.125, rss: 2, external: 3 }) }
    const sampler = new PendingSampler(memory, { set: () => 1, clear: () => {}, capacity: 40 })
    const journal = new NativeJournal({ memory, sampler, capacity: 40, fixed: true })
    const before = journal.journalState()
    const referenceSampler = new PendingSampler(memory, { set: () => 1, clear: () => {} })
    const reference = new NativeJournal({ memory, sampler: referenceSampler, capacity: 40 })
    for (let i = 0; i < 40; i++) {
      const row = journal.begin('gzip', { buffer: Buffer.alloc(1) })
      journal.end(row, Buffer.alloc(2))
      if (i < 32) {
        const old = reference.begin('gzip', { buffer: Buffer.alloc(1) })
        reference.end(old, Buffer.alloc(2))
      }
      if (i === 31) {
        expect(journal.summary()).toEqual(reference.summary())
        expect(sampler.summary()).toEqual(referenceSampler.summary())
      }
    }
    expect(journal.journalState().byteLength).toBe(before.byteLength)
    expect(journal.summary().rows).toHaveLength(40)
    expect(journal.summary().rows.every((row) => row.outcome === 'resolved')).toBe(true)
    expect(sampler.summary().pendingWindows).toHaveLength(40)
    expect(() => journal.begin('gzip', {})).toThrow(/overflow/)
  })

  test('stores UTF-8 strings compactly and preserves lone surrogates losslessly', () => {
    const journal = new churn.FixedJournal({ name: 'strings', capacity: 4, bytes: 96 })
    const strings = ['x'.repeat(20), 'λ'.repeat(10), '\ud800x', 'x\udc00']
    journal.push(...strings)
    expect(journal.values()).toEqual(strings)
    expect(journal.cursor).toBe(84)
    const scalars = new churn.FixedJournal({ name: 'numbers', capacity: 12, bytes: 128 })
    const values = [
      -0,
      NaN,
      Infinity,
      -Infinity,
      -32769,
      -32768,
      32767,
      32768,
      -2147483648,
      2147483647,
      2147483648,
      0.125,
    ]
    scalars.push(...values)
    expect(scalars.values()).toEqual(values)
  })

  test('counts only valid distinct corpus ids toward handler completeness', () => {
    const journal = new HandlerJournal(10000, 10004)
    for (let id = 0; id < 9999; id++) journal.enter(0, String(id), id, 0.125)
    for (const id of [0, -1, 10000]) journal.enter(0, '0', id, 0.125)
    expect(journal.unique).toBe(9999)
    journal.enter(0, '9999', 9999, 0.125)
    expect(journal.unique).toBe(10000)
  })

  test('reserves share shapes from the empty-round floor and finite corpus within a bounded arena', () => {
    const storage = churn.journalLimits()
    expect(storage.exchanges).toBeGreaterThanOrEqual(32400 + 20000)
    expect(storage.exchangeBytes + storage.acknowledgementBytes).toBeLessThan(29000000)
    const ledger = new churn.ShareLedger({ group: 'group', clock: { now: () => 0 } })
    const before = ledger.journalState()
    for (let n = 1; n <= storage.exchanges; n++) ledger.exchanges.push(exchange(n))
    expect(ledger.journalState().byteLength).toBe(before.byteLength)
    expect(ledger.facts().exchanges).toHaveLength(storage.exchanges)
  })
})

describe('telemetry journal ownership diagnostic', () => {
  test('compares ownership at every checkpoint and checks real retained sensitivity in both journal modes', () => {
    const fs = require('node:fs')
    const path = require('node:path')
    const childProcess = require('node:child_process')
    const root = process.env.CI
      ? path.resolve('.tmp/e2e/journal-ownership')
      : require('node:os').tmpdir()
    fs.mkdirSync(root, { recursive: true })
    const directory = fs.mkdtempSync(path.join(root, 'journal-ownership-'))
    try {
      const run = (mode, kind, seeded = false) =>
        JSON.parse(
          childProcess.execFileSync(
            process.execPath,
            [
              '--expose-gc',
              path.join(__dirname, 'fixtures/journalOwnershipChild.js'),
              mode,
              kind,
              String(seeded),
            ],
            { timeout: 45000, maxBuffer: 16 * 1024 * 1024, encoding: 'utf8' }
          )
        )
      for (const kind of ['producer', 'consumer', 'shareConsumer']) {
        const old = run('legacy', kind)
        const fixed = run('fixed', kind)
        fs.writeFileSync(path.join(directory, `${kind}.json`), JSON.stringify({ old, fixed }))
        expect(fixed).toMatchObject({
          diagnostic: true,
          acceptanceEligible: false,
          attributionComplete: false,
        })
        expect(fixed.corpusSha256).toBe(old.corpusSha256)
        expect(fixed.scheduleSha256).toBe(old.scheduleSha256)
        expect(fixed.idsSha256).toBe(old.idsSha256)
        expect(fixed.factsSha256).toBe(old.factsSha256)
        expect(fixed.storage.mode).toBe('fixed')
        expect(old.storage.mode).toBe('legacy')
        expect(fixed.storage.before.byteLength).toBeGreaterThan(0)
        expect(fixed.storage.closed.byteLength).toBe(fixed.storage.before.byteLength)
        expect(fixed.storage.before.columns[0].columns[0].format).toBe('scalar-utf8-1')
        if (kind === 'shareConsumer')
          expect(fixed.storage.before.columns[1].columns[0].format).toBe('share-columns-utf8-1')
        expect(old.storage.before).toBeNull()
        expect(old.storage.closed).toBeNull()
        for (const [index, checkpoint] of fixed.ownership.entries()) {
          const { journals, ...ownership } = checkpoint
          const { journals: legacyJournals, ...legacyOwnership } = old.ownership[index]
          expect(ownership).toEqual(legacyOwnership)
          expect(legacyJournals).toBeNull()
          expect(journals.byteLength).toBe(fixed.storage.before.byteLength)
        }
        expect(fixed.violations).not.toContain('retained slope')
        expect(fixed.violations).not.toContain('retained endpoint')
        for (const result of [old, fixed]) {
          expect(result.ownership.map((row) => row.checkpoint)).toEqual([
            'M',
            'first',
            'last',
            'closed',
          ])
          expect(result.ownership.every((row) => row.positiveAlive)).toBe(true)
          expect(result.ownership.at(-1).recordTargetsAlive).toBeLessThan(10)
        }
      }
      for (const mode of ['legacy', 'fixed']) {
        const seeded = run(mode, 'consumer', true)
        fs.writeFileSync(path.join(directory, `seeded-${mode}.json`), JSON.stringify(seeded))
        expect(seeded.violations).toEqual(
          expect.arrayContaining(['retained slope', 'retained endpoint'])
        )
        expect(seeded.ownership.at(-1).recordTargetsAlive).toBe(10000)
        expect(seeded.ownership.at(-1).positiveAlive).toBe(true)
      }
    } finally {
      if (!process.env.CI) fs.rmSync(directory, { recursive: true, force: true })
    }
  }, 240000)
})

describe('telemetry journal calibration', () => {
  test('matches the unchanged slope and endpoint evaluator on flat and seeded virtual vectors', () => {
    const { RetainedCalibration } = require('./fixtures/journalOwnershipChild')
    const { TELEMETRY, TelemetryResourceVerdict } = require('../soak')
    for (const rate of [0, 131072, 524288]) {
      const samples = TELEMETRY.sampleAt.map((at, index) => ({
        admittedAtMs: at,
        completedAtMs: at + 300 + (index % 2 ? 37 : 0),
        snapshot: {
          registryDigest: 'diagnostic',
          memory: {
            retainedBytes: 10000000 + Math.round((rate * (at - 60000)) / 60000),
          },
        },
      }))
      const input = { samples, measurementMs: TELEMETRY.measureMs }
      const actual = RetainedCalibration.evaluate(input)
      const full = TelemetryResourceVerdict.evaluate({
        ...input,
        clientCount: 1,
        baseline: samples[0].snapshot,
        teardown: samples.at(-1).snapshot,
        cleanupMs: 1,
      })
      expect(actual.scope).toBe('slope-and-endpoint-only')
      expect(actual.measured.slopeBytesPerMinute).toBe(full.measured.slopeBytesPerMinute)
      expect(actual.measured.endpointBytes).toBe(full.measured.endpointBytes)
      expect(actual.violations).toEqual(
        full.violations.filter((label) => ['retained slope', 'retained endpoint'].includes(label))
      )
      expect(actual.ok).toBe(rate === 0)
      if (rate === 524288)
        expect(actual.violations).toEqual(['retained slope', 'retained endpoint'])
    }
  })

  test('records actual admission and completion times and inventories every settled boundary', async () => {
    const { RetainedCalibration } = require('./fixtures/journalOwnershipChild')
    let now = 1060019
    const inventory = { weakRefs: 1, weakMaps: 0, maps: 0, buffers: 2, byteLength: 4096 }
    const memory = {
      settled: async () => {
        now += 237
        return { at: now, heapUsed: 100, external: 20, arrayBuffers: 10, retained: 120 }
      },
    }
    const calibration = new RetainedCalibration({
      memory,
      now: () => now,
      origin: 1000000,
      inventory: () => inventory,
    })
    const row = await calibration.take('first', 60000)
    expect(row).toMatchObject({
      checkpoint: 'first',
      scheduledAtMs: 60000,
      admittedAtMs: 60019,
      completedAtMs: 60256,
      observer: inventory,
      snapshot: { memory: { retainedBytes: 120, heapUsed: 100, external: 20, arrayBuffers: 10 } },
    })
    inventory.weakRefs++
    await expect(calibration.take('last', 359000)).rejects.toThrow('observer storage changed')
  })

  test('rejects incomplete or virtualized real-time calibration evidence', () => {
    const { RetainedCalibration } = require('./fixtures/journalOwnershipChild')
    expect(() => RetainedCalibration.evaluate({ samples: [], measurementMs: 1 })).toThrow(
      'calibration duration or samples'
    )
  })
})
