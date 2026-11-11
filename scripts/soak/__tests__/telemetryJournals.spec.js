const crypto = require('node:crypto')
const fs = require('node:fs')
const v8 = require('node:v8')
const churn = require('../telemetryChurn')
const controls = require('../journalControls')
const { TELEMETRY } = require('../soak')
const { MEMORY_FLAGS } = require('../../../testHelpers/processMemory')
const { ReferenceWork, ReferenceShareLedger } = require('./fixtures/journalReference')
const { NativeJournal, PendingSampler } = require('../../../testHelpers/processMemory')
const { HandlerJournal, journalState } = require('../../../testHelpers/fixedJournal')

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

describe('share journal packing', () => {
  const HEX = '0123456789abcdef'.repeat(4)
  const MIN = '-9223372036854775808'
  const MAX = '9223372036854775807'
  const open = (options, capacity = 128, bytes = 1 << 20) =>
    new churn.FixedJournal({ name: 'packing', capacity, bytes, ...options })
  /** The bytes of one fact in a layout, with its 4-byte length prefix. */
  const rowBytes = (fact, options = { packed: true }) => {
    const journal = open(options, 1, 8192)
    journal.push(fact)
    return journal.cursor
  }

  /** The largest exchange header: int32 identifiers, Float64 clocks, full labels and two digests. */
  const worstExchange = (request, response, over = {}) => ({
    n: 2147483647,
    segment: 'x'.repeat(249),
    socket: -2147483648,
    apiKey: 79,
    apiVersion: 32767,
    correlationId: -2147483648,
    at: 0.1 + 0.2,
    responseAt: 1 / 3,
    requestSha256: HEX,
    responseSha256: HEX,
    request: {
      groupId: 'g'.repeat(249),
      isRenewAck: true,
      topics: request ? [{ topicId: 'T'.repeat(36), partitions: [request] }] : [],
    },
    response: {
      errorCode: -32768,
      lockMs: -2147483648,
      topics: response ? [{ topicId: 'T'.repeat(36), partitions: [response] }] : [],
    },
    ...over,
  })
  const worstRequest = (batches = []) => ({ partition: 2147483647, batches })
  const worstResponse = (acquired = []) => ({
    partition: -2147483648,
    fetchErrorCode: -32768,
    acknowledgeErrorCode: -32768,
    acquired,
  })
  const worstAcknowledgement = () => ({
    topic: 'x'.repeat(249),
    partition: 2147483647,
    firstOffset: MIN,
    lastOffset: MIN,
    type: 'release',
    error: { name: 'N'.repeat(249), reason: 'R'.repeat(249) },
    at: 0.1 + 0.2,
  })

  /** Facts at every encoding edge: exact ints, fractional times, missing fields, UTF-16, duplicates. */
  const edgeFacts = () => {
    const exchange = (over) => ({
      n: 1,
      segment: 'segment',
      socket: 1,
      apiKey: 78,
      apiVersion: 2,
      correlationId: 7,
      at: 0.1 + 0.2,
      responseAt: 1 / 3,
      requestSha256: HEX,
      responseSha256: null,
      request: { groupId: 'g', isRenewAck: false, topics: [] },
      response: null,
      ...over,
    })
    const range = (firstOffset, lastOffset) => ({ firstOffset, lastOffset, deliveryCount: 1 })
    const partition = (acquired) => ({
      partition: -1,
      fetchErrorCode: null,
      acknowledgeErrorCode: 0,
      acquired,
    })
    const facts = [
      exchange(),
      exchange({
        n: 2147483647,
        socket: -2147483648,
        correlationId: -1,
        requestSha256: HEX.toUpperCase(),
        responseSha256: HEX,
      }),
      exchange({ requestSha256: undefined, responseSha256: undefined, response: undefined }),
      exchange({ requestSha256: 5, responseSha256: HEX.slice(1) }),
      exchange({ requestSha256: `\ud800${'a'.repeat(63)}`, responseSha256: 'λ'.repeat(64) }),
      exchange({ segment: '\ud800', at: -0, responseAt: 5e-324 }),
      exchange({ segment: 'λ😀', at: 2 ** 53 + 2, responseAt: Number.MAX_VALUE }),
      exchange({ segment: 'x'.repeat(250), at: NaN, responseAt: Infinity }),
      exchange({
        response: {
          errorCode: 0,
          lockMs: 2147483647,
          topics: [
            {
              topicId: 'T',
              partitions: [
                partition([
                  range(MIN, MAX),
                  range('0', '0'),
                  range('-1', '9223372036854775808'),
                  range('007', 12),
                  range(null, undefined),
                ]),
              ],
            },
          ],
        },
      }),
      exchange({
        request: {
          groupId: 'g',
          topics: [
            {
              topicId: 'T',
              partitions: [
                {
                  partition: 0,
                  batches: [
                    { firstOffset: MIN, lastOffset: MAX, types: [1, 2, 3, 4] },
                    { firstOffset: '5', lastOffset: '٣', types: [] },
                  ],
                },
              ],
            },
          ],
        },
      }),
      { 'k\udc00': 1, topic: 'T', offset: '12', firstOffset: HEX },
      {},
      {
        topic: 'T',
        partition: 1,
        firstOffset: '5',
        lastOffset: '6',
        type: 'reject',
        error: { name: 'E\ud800', reason: 'λ' },
        at: 0.5,
      },
      { topic: 'T', partition: 1, firstOffset: '5', lastOffset: '6', type: 'reject', error: null },
    ]
    return [...facts, facts[0], facts[8]]
  }

  test('stores digests and int64 offsets as raw bytes only where the text is exactly reconstructible', () => {
    expect(rowBytes({ requestSha256: HEX })).toBe(43)
    expect(rowBytes({ requestSha256: HEX }, { columns: true })).toBe(79)
    expect(rowBytes({ firstOffset: MIN })).toBe(19)
    expect(rowBytes({ firstOffset: MIN }, { columns: true })).toBe(35)
    for (const [field, value] of [
      ['requestSha256', HEX.toUpperCase()],
      ['responseSha256', HEX.slice(1)],
      ['requestSha256', `${HEX}0`],
      ['firstOffset', '9223372036854775808'],
      ['lastOffset', '-9223372036854775809'],
      ['firstOffset', '007'],
      ['lastOffset', '-0'],
      ['firstOffset', '+1'],
      ['lastOffset', ' 1'],
      ['firstOffset', '1e3'],
      ['firstOffset', ''],
      ['lastOffset', '٣'],
      ['topic', HEX],
      ['offset', '12'],
    ])
      expect(rowBytes({ [field]: value })).toBe(rowBytes({ [field]: value }, { columns: true }))
  })

  test('decodes the legacy columns and the packed encoding of the same facts to identical archives', () => {
    const facts = edgeFacts()
    const legacy = open({ columns: true })
    const packed = open({ packed: true })
    legacy.push(...facts)
    packed.push(...facts)
    expect(packed.length).toBe(facts.length)
    expect(packed.values()).toStrictEqual(legacy.values())
    expect(packed.values()).toStrictEqual(facts)
    expect(sha(packed.values())).toBe(sha(legacy.values()))
    expect(packed.cursor).toBeLessThan(legacy.cursor)
    expect(packed.journalState().format).toBe('share-packed-1')
    expect(legacy.journalState().format).toBe('share-columns-utf8-1')
    const fact = { requestSha256: HEX, firstOffset: MAX }
    packed.push(fact)
    fact.requestSha256 = 'mutated'
    fact.firstOffset = '0'
    expect(packed.values().at(-1)).toStrictEqual({ requestSha256: HEX, firstOffset: MAX })
  })

  test('reserves each share row at its proved worst-case size in the packed layout', () => {
    const header = rowBytes(worstExchange())
    const pair = rowBytes(worstExchange(worstRequest(), worstResponse())) - header
    const record =
      rowBytes(
        worstExchange(
          worstRequest([{ firstOffset: MIN, lastOffset: MIN, types: [4] }]),
          worstResponse([{ firstOffset: MIN, lastOffset: MIN, deliveryCount: 32767 }])
        )
      ) -
      header -
      pair
    const acknowledgement = rowBytes(worstAcknowledgement())
    expect({ header, pair, record, acknowledgement }).toEqual({
      header: 167,
      pair: 72,
      record: 63,
      acknowledgement: 67,
    })
    const reserve = churn.SHARE_ROW_BYTES
    expect(reserve).toEqual({ header: 176, partitionPair: 80, record: 64, acknowledgement: 80 })
    for (const [proved, reserved] of [
      [header, reserve.header],
      [pair, reserve.partitionPair],
      [record, reserve.record],
      [acknowledgement, reserve.acknowledgement],
    ]) {
      expect(reserved).toBeGreaterThanOrEqual(proved)
      expect(reserved - proved).toBeLessThan(16)
    }
    const unpacked = worstExchange(undefined, undefined, {
      requestSha256: HEX.toUpperCase(),
      responseSha256: HEX.toUpperCase(),
    })
    expect(rowBytes(unpacked)).toBeGreaterThan(reserve.header)
    const storage = churn.journalLimits()
    expect(storage.exchangeBytes).toBe(54532 * 176 + 98796 * 80 + 11000 * 64)
    expect(storage.acknowledgementBytes).toBe(11000 * 80)
  })

  test('fails every share journal dimension fatally, even when the caller catches it', () => {
    const header = rowBytes(worstExchange())
    const event = { payload: { topic: 'T', partition: 0, firstOffset: MIN, lastOffset: MIN } }
    const acknowledged = { ...event, payload: { ...event.payload, type: 'release', error: null } }
    const acknowledgement = rowBytes({ ...acknowledged.payload, at: 0.5 })
    const storage = (over) => ({
      exchanges: 100,
      exchangeBytes: 1 << 20,
      acknowledgements: 100,
      acknowledgementBytes: 1 << 20,
      ...over,
    })
    const exchange = (i) => worstExchange(undefined, undefined, { segment: `segment-${i}` })
    const cases = [
      [
        'exchange rows',
        storage({ exchanges: 2 }),
        (ledger) => ledger.exchanges.push(exchange(0), exchange(0)),
        (ledger) => ledger.exchanges.push(exchange(0)),
        /share exchanges journal overflow \(rows 2\/2/,
      ],
      [
        'exchange bytes',
        storage({ exchangeBytes: 2 * header + 1 }),
        (ledger) => ledger.exchanges.push(exchange(0), exchange(0)),
        (ledger) => ledger.exchanges.push(exchange(0)),
        /share exchanges journal overflow \(rows 2\/100/,
      ],
      [
        'exchange labels',
        storage({}),
        (ledger) => {
          for (let i = 0; i < 64; i++) ledger.exchanges.push({ segment: `label-${i}` })
        },
        (ledger) => ledger.exchanges.push({ segment: 'label-64' }),
        /share exchanges journal symbol overflow/,
      ],
      [
        'acknowledgement rows',
        storage({ acknowledgements: 1 }),
        (ledger) => ledger.record(acknowledged),
        (ledger) => ledger.record(acknowledged),
        /share acknowledgements journal overflow \(rows 1\/1/,
      ],
      [
        'acknowledgement bytes',
        storage({ acknowledgementBytes: acknowledgement + 1 }),
        (ledger) => ledger.record(acknowledged),
        (ledger) => ledger.record(acknowledged),
        /share acknowledgements journal overflow \(rows 1\/100/,
      ],
    ]
    for (const [, limits, fill, overflow, message] of cases) {
      const ledger = new churn.ShareLedger({
        group: 'group',
        clock: { now: () => 0.5 },
        storage: limits,
      })
      fill(ledger)
      const before = ledger.journalState()
      const archive = ledger.facts()
      const bytes = Buffer.concat([ledger.exchanges.storage, ledger.settled.storage])
      expect(() => overflow(ledger)).toThrow(message)
      expect(ledger.facts()).toStrictEqual(archive)
      expect(Buffer.concat([ledger.exchanges.storage, ledger.settled.storage]).equals(bytes)).toBe(
        true
      )
      const after = ledger.journalState()
      expect(after.byteLength).toBe(before.byteLength)
      expect(after.columns.filter((state) => state.overflow)).toHaveLength(1)
      expect(
        churn.JournalConstancy.violations({
          baseline: { journals: before },
          samples: [],
          teardown: { journals: after },
        })
      ).toContain('journal overflow')
    }
    expect(rowBytes({ segment: 'x'.repeat(250) })).toBe(265)
  })

  test('declares exactly the bytes each worker allocates and fills before the disabled control', () => {
    const { TELEMETRY } = require('../soak')
    const storage = churn.journalLimits(TELEMETRY)
    const clock = { now: () => 0 }
    const memory = { current: () => ({ at: 1.125, rss: 2, external: 3 }) }
    const declared = {}
    for (const kind of churn.KINDS) {
      const ledger =
        kind === 'shareConsumer' ? new churn.ShareLedger({ group: 'group', clock, storage }) : null
      const ordinary = new churn.OrdinaryWork({
        kind,
        client: null,
        stem: 'stem',
        topic: 'topic',
        ctl: 'ctl',
        clock,
        ledger,
        slots: [],
        storage,
      })
      const sampler = new PendingSampler(memory, {
        set: () => 1,
        clear: () => {},
        capacity: churn.NATIVE_ROWS,
      })
      const journal = new NativeJournal({
        memory,
        sampler,
        capacity: churn.NATIVE_ROWS,
        fixed: true,
      })
      const samples = new churn.FixedJournal({
        name: 'memory samples',
        capacity: storage.samples,
        bytes: storage.sampleBytes,
      })
      declared[kind] = churn.declaredJournalBytes(kind, TELEMETRY)
      expect(
        journalState([ordinary, ...(ledger ? [ledger] : []), journal, sampler, samples]).byteLength
      ).toBe(declared[kind])
      for (const archive of ledger ? [ledger.exchanges, ledger.settled] : [])
        expect(archive.storage.equals(Buffer.alloc(archive.byteLength, 0xa5))).toBe(true)
    }
    expect(declared).toEqual({
      producer: 4070384,
      consumer: 4070384,
      admin: 4070384,
      shareConsumer: 23188464,
    })
  })
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
    const { TELEMETRY } = require('../soak')
    for (const corrupt of [
      (row) => (row.completedAtMs = row.admittedAtMs - 1),
      (row) => (row.admittedAtMs = 1),
      (row) => (row.completedAtMs = 360001),
      (row) => (row.snapshot.memory.retainedBytes = NaN),
    ]) {
      const samples = TELEMETRY.sampleAt.map((at) => ({
        admittedAtMs: at,
        completedAtMs: at + 300,
        snapshot: { memory: { retainedBytes: 10000000 } },
      }))
      corrupt(samples[5])
      expect(() => RetainedCalibration.evaluate({ samples, measurementMs: 300000 })).toThrow(
        'invalid calibration sample'
      )
    }
  })
})

/** A component verdict as the child's evaluator forms it from a measured slope and endpoint. */
const verdictOf = (slopeBytesPerMinute, endpointBytes) => {
  const violations = [
    ...(slopeBytesPerMinute > TELEMETRY.slopeBytesPerMinute ? ['retained slope'] : []),
    ...(endpointBytes > TELEMETRY.endpointBytes ? ['retained endpoint'] : []),
  ]
  return {
    scope: 'slope-and-endpoint-only',
    ok: violations.length === 0,
    violations,
    measured: { samples: 11, slopeBytesPerMinute, endpointBytes },
  }
}
const arm = (kind, mode, seeded, protocol = 'resource') => ({ kind, mode, seeded, protocol })
const LIMITS = { slope: TELEMETRY.slopeBytesPerMinute, endpoint: TELEMETRY.endpointBytes }
const CLEAN = verdictOf(LIMITS.slope / 2, LIMITS.endpoint / 4)
const BOTH = verdictOf(LIMITS.slope * 4, LIMITS.endpoint * 1.3)
const FALLBACK = {
  scope: 'slope-and-endpoint-only',
  ok: false,
  violations: ['calibration evidence'],
  error: 'invalid calibration sample',
}

describe('telemetry journal calibration roles', () => {
  test('assigns each arm its role: null, seeded, contamination-positive, or record only', () => {
    for (const kind of ['consumer', 'shareConsumer'])
      expect(controls.armOf(arm(kind, 'legacy', false))).toEqual({
        role: 'contamination-positive',
        judgedBy: 'slope violation',
      })
    expect(controls.armOf(arm('producer', 'legacy', false))).toEqual({
      role: 'contamination-positive',
      judgedBy: 'paired ordering',
    })
    expect(controls.armOf(arm('admin', 'legacy', false)).judgedBy).toBe('record only')
    for (const kind of controls.KINDS) {
      expect(controls.armOf(arm(kind, 'fixed', false))).toEqual({
        role: 'null',
        judgedBy: 'no violation',
      })
      for (const mode of ['legacy', 'fixed'])
        expect(controls.armOf(arm(kind, mode, true)).role).toBe('seeded-positive')
      expect(controls.armOf(arm(kind, 'fixed', false, 'live')).judgedBy).toBe('record only')
      expect(controls.armOf(arm(kind, 'fixed', true, 'live')).judgedBy).toBe('both violations')
    }
  })

  test('accepts the consumer and share legacy contamination only when the retained slope is detected', () => {
    for (const kind of ['consumer', 'shareConsumer']) {
      const legacy = arm(kind, 'legacy', false)
      expect(() => controls.judgeArm(legacy, BOTH)).not.toThrow()
      expect(() =>
        controls.judgeArm(legacy, verdictOf(LIMITS.slope * 2, LIMITS.endpoint / 2))
      ).not.toThrow()
      expect(() => controls.judgeArm(legacy, verdictOf(100, LIMITS.endpoint * 2))).toThrow(
        'legacy contamination was not detected'
      )
      expect(() => controls.judgeArm(legacy, CLEAN)).toThrow(
        'legacy contamination was not detected'
      )
      const dropped = { ...BOTH, violations: ['retained slope'] }
      expect(() => controls.judgeArm(legacy, dropped)).toThrow('endpoint violation agrees')
    }
  })

  test('requires the fixed unseeded null to pass and every seeded arm to show both violations', () => {
    for (const kind of controls.KINDS) {
      const fixed = arm(kind, 'fixed', false)
      expect(() => controls.judgeArm(fixed, CLEAN)).not.toThrow()
      expect(() => controls.judgeArm(fixed, verdictOf(LIMITS.slope * 2, 1))).toThrow('null failed')
      expect(() => controls.judgeArm(fixed, verdictOf(1, LIMITS.endpoint * 2))).toThrow(
        'null failed'
      )
      for (const mode of ['legacy', 'fixed']) {
        const seeded = arm(kind, mode, true)
        expect(() => controls.judgeArm(seeded, BOTH)).not.toThrow()
        expect(() => controls.judgeArm(seeded, CLEAN)).toThrow()
        expect(() => controls.judgeArm(seeded, verdictOf(LIMITS.slope * 2, 1))).toThrow()
      }
    }
  })

  test('judges the producer legacy arm by paired ordering against its fixed null only', () => {
    const legacy = arm('producer', 'legacy', false)
    expect(() => controls.judgeArm(legacy, CLEAN)).not.toThrow()
    const old = verdictOf(85 * 1024, 540 * 1024)
    const fixed = verdictOf(41 * 1024, 322 * 1024)
    expect(() => controls.judgeOrdering(old, fixed)).not.toThrow()
    expect(() => controls.judgeOrdering(fixed, old)).toThrow('legacy slope')
    expect(() => controls.judgeOrdering(old, verdictOf(41 * 1024, 540 * 1024))).toThrow(
      'legacy endpoint'
    )
    expect(() => controls.judgeOrdering(verdictOf(70 * 1024, 900 * 1024), fixed)).not.toThrow()
    expect(() => controls.judgeOrdering(verdictOf(41 * 1024, 900 * 1024), fixed)).toThrow(
      'legacy slope'
    )
  })

  test('never accepts a missing, partial or contradictory verdict as a detection or a null', () => {
    const arms = [
      arm('consumer', 'legacy', false),
      arm('consumer', 'fixed', false),
      arm('consumer', 'fixed', true),
      arm('admin', 'legacy', false),
      arm('admin', 'fixed', false, 'live'),
    ]
    const contradictory = { ...BOTH, ok: true }
    const unlabelled = { ...BOTH, violations: [...BOTH.violations, 'identity roles'] }
    const nonFinite = verdictOf(NaN, 0)
    for (const target of arms)
      for (const bad of [undefined, {}, FALLBACK, contradictory, unlabelled, nonFinite])
        expect(() => controls.judgeArm(target, bad)).toThrow()
    expect(() => controls.judgeOrdering(undefined, CLEAN)).toThrow()
    expect(() => controls.judgeOrdering(BOTH, FALLBACK)).toThrow('measured slope and endpoint')
  })

  test('records the admin legacy arm and the live-flags unseeded null without judging them', () => {
    expect(() => controls.judgeArm(arm('admin', 'legacy', false), BOTH)).not.toThrow()
    expect(() => controls.judgeArm(arm('admin', 'legacy', false), CLEAN)).not.toThrow()
    for (const kind of controls.KINDS) {
      const live = arm(kind, 'fixed', false, 'live')
      expect(() => controls.judgeArm(live, BOTH)).not.toThrow()
      expect(() => controls.judgeArm(live, CLEAN)).not.toThrow()
      expect(() => controls.judgeArm(arm(kind, 'fixed', true, 'live'), CLEAN)).toThrow()
    }
  })

  test('parses the workload and protocol selection and rejects any other option', () => {
    expect(controls.parseArguments(['out'])).toEqual({ directory: 'out', options: {} })
    expect(controls.parseArguments(['out', '--kinds=admin,producer', '--protocols=live'])).toEqual({
      directory: 'out',
      options: { kinds: ['admin', 'producer'], protocols: ['live'] },
    })
    expect(() => controls.parseArguments(['out', '--kind=admin'])).toThrow('Unknown option')
    expect(() => controls.run('out', { kinds: ['broker'] })).toThrow('unknown workload kind')
    expect(() => controls.run('out', { protocols: ['fast'] })).toThrow('unknown protocol')
  })
})

describe('telemetry journal calibration children', () => {
  /** Runs the controls with every child scripted, returning the argument vectors and the summary. */
  const scripted = (options, respond) => {
    const calls = []
    const files = new Map()
    const spawn = jest
      .spyOn(require('node:child_process'), 'spawnSync')
      .mockImplementation((...call) => {
        calls.push(call)
        return respond(call[1]) ?? { status: 0, stdout: '{}', stderr: '' }
      })
    const mkdir = jest.spyOn(fs, 'mkdirSync').mockImplementation(() => {})
    const write = jest.spyOn(fs, 'writeFileSync').mockImplementation((file, data) => {
      files.set(file.split('/').pop(), data)
    })
    let error
    try {
      jest.isolateModules(() => {
        try {
          require('../journalControls').run('controls', options)
        } catch (thrown) {
          error = thrown
        }
      })
    } finally {
      spawn.mockRestore()
      mkdir.mockRestore()
      write.mockRestore()
    }
    return { calls, error, summary: JSON.parse(files.get('summary.json')) }
  }
  const argsOf = ([, args]) => {
    const script = args.findIndex((arg) => arg.endsWith('.js'))
    return {
      flags: args.slice(0, script),
      script: args[script].split('/').pop(),
      rest: args.slice(script + 1),
    }
  }

  test('runs the live-flags arms with the exact worker flags, fixed mode only, for every workload', () => {
    const { calls, error, summary } = scripted({ protocols: ['live'] }, () => null)
    expect(error).toBeInstanceOf(AggregateError)
    expect(calls).toHaveLength(8)
    const arms = calls.map(argsOf)
    for (const { flags, script } of arms) {
      expect(flags).toEqual(['--expose-gc'])
      expect(script).toBe('journalSensitivityChild.js')
    }
    expect(arms.map((row) => row.rest)).toEqual(
      ['producer', 'consumer', 'admin', 'shareConsumer'].flatMap((kind) => [
        ['fixed', kind, 'false', 'live'],
        ['fixed', kind, 'true', 'live'],
      ])
    )
    expect(summary.protocols).toEqual(['live'])
    expect(summary.failures.map((row) => row.name)).toContain('sensitivity-admin-fixed-true-live')
  })

  test('runs the admin kind through all four resource arms with the resource flags and no ownership child', () => {
    const { calls } = scripted({ kinds: ['admin'], protocols: ['resource'] }, () => null)
    const arms = calls.map(argsOf)
    expect(arms.map((row) => row.script)).toEqual(Array(4).fill('journalSensitivityChild.js'))
    for (const { flags } of arms) expect(flags).toEqual(MEMORY_FLAGS)
    expect(arms.map((row) => row.rest)).toEqual([
      ['legacy', 'admin', 'false', 'resource'],
      ['legacy', 'admin', 'true', 'resource'],
      ['fixed', 'admin', 'false', 'resource'],
      ['fixed', 'admin', 'true', 'resource'],
    ])
  })

  test('never accepts a timed-out, crashed or malformed legacy unseeded child as a detection', () => {
    const timeout = Object.assign(new Error('spawnSync node ETIMEDOUT'), { code: 'ETIMEDOUT' })
    const answers = [
      { error: timeout, status: null, signal: 'SIGTERM', stdout: '', stderr: '' },
      { status: 1, signal: null, stdout: '{}', stderr: 'crash' },
      { status: 0, signal: null, stdout: 'not json', stderr: '' },
      { status: 0, signal: null, stdout: JSON.stringify({ verdict: FALLBACK }), stderr: '' },
    ]
    for (const [index, answer] of answers.entries()) {
      const { error, summary } = scripted(
        { kinds: ['consumer'], protocols: ['resource'] },
        (args) =>
          args.includes('legacy') &&
          args.includes('false') &&
          args.some((a) => a.endsWith('journalSensitivityChild.js'))
            ? answer
            : null
      )
      expect(error).toBeInstanceOf(AggregateError)
      expect(summary.failures.map((row) => row.name)).toContain('sensitivity-consumer-legacy-false')
      const rows = summary.summary.filter((row) => row.mode === 'legacy' && row.seeded === false)
      expect(rows).toHaveLength(index === 3 ? 1 : 0)
      for (const row of rows) expect(row).toMatchObject({ role: 'contamination-positive' })
    }
  })
})

describe('telemetry engine code shadow', () => {
  /** Makes each engine code reading deterministic, in call order. */
  const sizes = (...readings) => {
    const spy = jest.spyOn(v8, 'getHeapCodeStatistics')
    for (const [code, bytecode] of readings)
      spy.mockReturnValueOnce({
        code_and_metadata_size: code,
        bytecode_and_metadata_size: bytecode,
      })
    return spy
  }

  test('journals code and bytecode sizes of every reading in fixed storage and fails an overflow', () => {
    const spy = sizes([1000, 500], [1100, 500])
    try {
      const engine = new churn.EngineCodeJournal({ capacity: 2 })
      const before = engine.journalState()
      engine.record({ phase: 'c0', at: 1.5 })
      engine.record({ phase: 'settled', at: 2.5 })
      expect(engine.journalState().byteLength).toBe(before.byteLength)
      expect(engine.values()).toEqual([
        { phase: 'c0', at: 1.5, code: 1000, bytecode: 500 },
        { phase: 'settled', at: 2.5, code: 1100, bytecode: 500 },
      ])
      const full = engine.journalState()
      expect(() => engine.record({ phase: 'final', at: 3.5 })).toThrow(/engine code.*overflow/)
      expect(engine.journalState()).toEqual({ ...full, overflow: true })
      expect(engine.values()).toHaveLength(2)
      expect(
        churn.JournalConstancy.violations({
          baseline: { journals: before },
          samples: [],
          teardown: { journals: engine.journalState() },
        })
      ).toContain('journal overflow')
    } finally {
      spy.mockRestore()
    }
  })

  test('takes the engine sizes beside a settled reading without changing the reading', async () => {
    const spy = sizes([2048, 1024])
    try {
      const engine = new churn.EngineCodeJournal({ capacity: 4 })
      let now = 10
      const reading = await churn.settledReading('c0', {
        gc: jest.fn(),
        pause: async () => {},
        now: () => (now += 1),
        engine,
      })
      expect(reading).toMatchObject({ phase: 'c0', retained: reading.heapUsed + reading.external })
      expect(Object.keys(reading)).not.toContain('engine')
      expect(engine.values()).toEqual([{ phase: 'c0', at: reading.at, code: 2048, bytecode: 1024 }])
      const plain = await churn.settledReading('c1', {
        gc: jest.fn(),
        pause: async () => {},
        now: () => (now += 1),
      })
      expect(plain.phase).toBe('c1')
      expect(engine.values()).toHaveLength(1)
    } finally {
      spy.mockRestore()
    }
  })

  test('reports raw, code and data slope for a code-driven series and marks an unjoined one incomplete', () => {
    const points = Array.from({ length: 6 }, (_, i) => ({
      minutes: i + 1,
      raw: 10000000 + 100000 * (i + 1),
      code: 2000000 + 40000 * (i + 1),
    }))
    const report = churn.EngineCodeJournal.shadow(points)
    expect(report).toMatchObject({
      scope: 'shadow',
      acceptance: false,
      complete: true,
      readings: 6,
    })
    expect(report.raw.slopeBytesPerMinute).toBeCloseTo(100000, 3)
    expect(report.code.slopeBytesPerMinute).toBeCloseTo(40000, 3)
    expect(report.data.slopeBytesPerMinute).toBeCloseTo(60000, 3)
    expect([
      report.raw.endpointBytes,
      report.code.endpointBytes,
      report.data.endpointBytes,
    ]).toEqual([500000, 200000, 300000])
    expect(churn.EngineCodeJournal.shadow(points.slice(0, 1))).toMatchObject({ complete: false })
    expect(churn.EngineCodeJournal.shadow([points[0], { ...points[0], raw: 1 }])).toMatchObject({
      complete: false,
      error: expect.stringContaining('different x'),
    })
    const rows = points.map((p, i) => [2000000 + 40000 * (i + 1) - 500, 500])
    const spy = sizes(...rows)
    try {
      const engine = new churn.EngineCodeJournal()
      const samples = points.map((p, i) => {
        engine.record({ phase: 'settled', at: 100 + i })
        return {
          completedAtMs: p.minutes * 60000,
          snapshot: { memory: { at: 100 + i, retainedBytes: p.raw } },
        }
      })
      const joined = engine.report(samples)
      expect(joined.data.slopeBytesPerMinute).toBeCloseTo(60000, 3)
      expect(joined.code.endpointBytes).toBe(200000)
      samples[2].snapshot.memory.at = 999
      expect(engine.report(samples)).toEqual({
        scope: 'shadow',
        acceptance: false,
        complete: false,
        missing: 1,
      })
    } finally {
      spy.mockRestore()
    }
  })

  test('takes the final reading engine sizes while building the reference and lists the journal as allocated before the control', () => {
    const worker = new churn.TelemetryWorkerProcess({
      kind: 'admin',
      soak: { TELEMETRY },
      proc: {},
      env: {},
    })
    const parts = {
      loaded: {},
      control: {},
      host: { before: {} },
      memory: { source: null, gcCapability: () => ({ native: true }) },
      journal: { summary: () => ({}) },
      sampler: { summary: () => ({}) },
      foreground: [],
      allocation: {},
    }
    const engine = new churn.EngineCodeJournal({ capacity: 2 })
    const reference = worker.reference({ ...parts, engine })
    expect(engine.values()).toEqual([
      {
        phase: 'final',
        at: reference.final.at,
        code: expect.any(Number),
        bytecode: expect.any(Number),
      },
    ])
    expect(reference.journals.allocationBeforeControl).toContain('engine-code')
    expect(worker.reference(parts).final.phase).toBe('final')
  })

  test('forwards a worker shadow report beside, never into, its judged resource verdict', () => {
    const record = jest.fn()
    const verdict = jest.fn(() => ({
      ok: true,
      outcome: 'passed',
      violations: [],
      inconclusive: [],
      measured: {},
    }))
    const churnRun = new churn.TelemetryChurn({
      fixture: {},
      processes: { workers: new Map() },
      decoder: {},
      record,
      clock: { now: () => 0 },
      limits: {},
      verdict,
      brokers: [],
      stem: 'stem',
      line: '4.3.1',
      directory: '/evidence',
    })
    const shadow = { scope: 'shadow', acceptance: false, complete: true, readings: 11 }
    const results = churn.KINDS.map((kind) => ({
      kind,
      clientId: `stem-${kind}`,
      uuid: `uuid-${kind}`,
      input: {},
      facts: { requirements: { ok: true, violations: [] } },
      ...(kind === 'producer' ? { shadow } : {}),
    }))
    const exits = Object.fromEntries(churn.KINDS.map((kind) => [kind, { kind }]))
    churnRun.judge(
      results,
      { errors: [], exits, oracle: null },
      { t0: 0, epochAt: 1, boundaryAt: 2, cleanupEndAt: 3 },
      new churn.TelemetrySchedule({ warmupMs: 60000, measureMs: 300000 })
    )
    const rows = record.mock.calls
      .map(([row]) => row)
      .filter((row) => row.kind === 'resource-verdict')
    expect(rows.map((row) => row.clientKind)).toEqual(churn.KINDS)
    expect(rows.find((row) => row.clientKind === 'producer').shadow).toEqual(shadow)
    for (const row of rows) {
      expect(row.result).not.toHaveProperty('shadow')
      if (row.clientKind !== 'producer') expect(row).not.toHaveProperty('shadow')
    }
    expect(verdict).toHaveBeenCalledTimes(4)
    for (const [input] of verdict.mock.calls) expect(input).not.toHaveProperty('shadow')
  })
})
