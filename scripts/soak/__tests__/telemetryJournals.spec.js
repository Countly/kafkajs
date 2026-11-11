const crypto = require('node:crypto')
const churn = require('../telemetryChurn')
const { ReferenceWork, ReferenceShareLedger } = require('./fixtures/journalReference')
const { NativeJournal, PendingSampler } = require('../../../testHelpers/processMemory')
const { TelemetryResourceVerdict } = require('../soak')

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
    expect(actual.ledger.facts().exchangesSha256).toBe(expected.ledger.facts().exchangesSha256)
    const fact = exchange(101)
    actual.ledger.exchanges.push(fact)
    fact.request.groupId = 'mutated'
    expect(actual.ledger.facts().exchanges[100].request.groupId).toBe('group')
  })

  test.each(['missing', 'duplicate', 'reordered', 'wrong-timestamp'])(
    'rejects %s replay input',
    (fault) => {
      const expected = shareReplay(ReferenceShareLedger).ledger.facts()
      const actual = shareReplay(churn.ShareLedger).ledger.facts()
      if (fault === 'missing') actual.exchanges.pop()
      if (fault === 'duplicate') actual.exchanges.push(actual.exchanges[0])
      if (fault === 'reordered') actual.exchanges.reverse()
      if (fault === 'wrong-timestamp') actual.exchanges[0].at += 0.125
      expect(() => exact(expected, actual)).toThrow('journal replay differs')
    }
  )

  test('fails row and byte overflow before truncating or overwriting any archived fact', () => {
    const journal = new churn.FixedJournal({ name: 'control', capacity: 1, bytes: 128 })
    journal.push({ at: 1.125, text: 'λ' })
    const before = journal.values()
    const cursor = journal.cursor
    expect(() => journal.push({ at: 2 })).toThrow(/control.*overflow/)
    expect(journal.values()).toEqual(before)
    expect(journal.cursor).toBe(cursor)
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
    for (let i = 0; i < 40; i++) {
      const row = journal.begin('gzip', { buffer: Buffer.alloc(1) })
      journal.end(row, Buffer.alloc(2))
    }
    expect(journal.journalState().byteLength).toBe(before.byteLength)
    expect(journal.summary().rows).toHaveLength(40)
    expect(journal.summary().rows.every((row) => row.outcome === 'resolved')).toBe(true)
    expect(sampler.summary().pendingWindows).toHaveLength(40)
    expect(() => journal.begin('gzip', {})).toThrow(/overflow/)
  })

  test('retained per-record product objects still trip slope and endpoint in a measured stand-in', () => {
    const retained = []
    const snapshot = (bytes) => ({ memory: { retainedBytes: bytes }, registryDigest: 'same' })
    const input = {
      clientCount: 1,
      baseline: snapshot(0),
      samples: [],
      teardown: snapshot(0),
      cleanupMs: 1,
      measurementMs: 300000,
    }
    for (let sample = 0; sample < 11; sample++) {
      for (let record = 0; record < 1000; record++)
        retained.push({ payload: Buffer.alloc(256, 0x6d) })
      input.samples.push({
        admittedAtMs: 60000 + sample * 29900,
        completedAtMs: 60001 + sample * 29900,
        snapshot: snapshot(retained.reduce((sum, object) => sum + object.payload.length, 0)),
      })
    }
    const result = TelemetryResourceVerdict.evaluate(input)
    expect(result.violations).toEqual(
      expect.arrayContaining(['retained slope', 'retained endpoint'])
    )
    expect(retained[10000].payload[0]).toBe(0x6d)
  })
})

describe('telemetry journal ownership diagnostic', () => {
  test('pairs old and fixed journals on one schedule and keeps seeded product retention visible', () => {
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
      const seeded = run('fixed', 'consumer', true)
      fs.writeFileSync(path.join(directory, 'seeded.json'), JSON.stringify(seeded))
      expect(seeded.violations).toEqual(
        expect.arrayContaining(['retained slope', 'retained endpoint'])
      )
      expect(seeded.ownership.at(-1).recordTargetsAlive).toBe(10000)
      expect(seeded.ownership.at(-1).positiveAlive).toBe(true)
    } finally {
      if (!process.env.CI) fs.rmSync(directory, { recursive: true, force: true })
    }
  }, 240000)
})
