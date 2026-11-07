const crypto = require('node:crypto')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { EventEmitter } = require('node:events')
const { performance } = require('node:perf_hooks')
const {
  LIMITS,
  parseArgs,
  parseCommand,
  identity,
  verifyDelivery,
  quantile,
  assertIdle,
  assertReleased,
  HeaderStream,
  Frames,
  AcquisitionLog,
  measureWindow,
  evaluateSession,
  controls,
  produceCorpus,
  ShareThroughput,
  CorpusFixture,
} = require('../shareThroughput')

const message = (offset, key = 'k', value = 'v') => ({
  offset: String(offset),
  key: key === null ? null : Buffer.from(key),
  value: value === null ? null : Buffer.from(value),
})

/**
 * Complete, deterministic acquisitions: four full ones on each of three nodes. It supplies no
 * broker and no measured timing, only the timestamps the gates read.
 */
const windowFacts = (maxRecords = 500) => {
  const frames = { rows: [], failure: null }
  const log = { groups: new Map(), failure: null }
  for (let node = 0; node < 3; node++)
    for (let index = 0; index < 4; index++) {
      const frame = {
        endpoint: `n${node}`,
        apiKey: 78,
        writeAt: index * 100,
        responseAt: index * 100 + 10,
      }
      const group = {
        frame,
        calls: [],
        entries: [],
        settled: maxRecords,
        settledAt: (index + 1) * 100 + 10,
      }
      const call = {
        callback: log.groups.size,
        topic: 't',
        partition: node,
        messages: Array.from({ length: maxRecords }, (_, i) => message(index * maxRecords + i)),
        returnedAt: index * 100 + 20,
        lock: LIMITS.lock,
        group,
      }
      group.calls.push(call)
      group.entries = call.messages.map((entry) => ({
        call,
        message: entry,
        ackAt: group.settledAt,
      }))
      log.groups.set(frame, group)
      frames.rows.push(frame)
    }
  frames.rows.push({ endpoint: 'n0', apiKey: 79, writeAt: 500, responseAt: 510 })
  return { log, frames, maxRecords, target: maxRecords * 6 }
}

/** Four records in two handler calls, with the matching producer corpus and observation stream. */
const deliveryFacts = () => {
  const call = (first) => ({
    topic: 't',
    partition: 0,
    messages: [message(first), message(first + 1)],
  })
  const callbacks = [call(0), call(2)]
  return {
    expected: callbacks.flatMap(({ topic, partition, messages }) =>
      messages.map((entry) => identity({ topic, partition, message: entry }))
    ),
    callbacks,
    observed: callbacks.flatMap(({ topic, partition, messages }, callback) =>
      messages.map((entry) => ({ topic, partition, message: entry, callback }))
    ),
    claimedMessages: 4,
    claimedBatches: 2,
  }
}

const request = (apiKey, correlationId, size = 8) => {
  const frame = Buffer.alloc(size + 4)
  frame.writeInt32BE(size)
  frame.writeInt16BE(apiKey, 4)
  frame.writeInt16BE(15, 6)
  frame.writeInt32BE(correlationId, 8)
  return frame
}

const response = (correlationId, size = 4) => {
  const frame = Buffer.alloc(size + 4)
  frame.writeInt32BE(size)
  frame.writeInt32BE(correlationId, 4)
  return frame
}

const fakeSocket = () => {
  const socket = new EventEmitter()
  socket.write = jest.fn(() => false)
  return socket
}

const sessionOf = (value) => {
  const arm = () => ({
    a: Array.from({ length: 8 }, () => ({ ...value })),
    aprime: Array.from({ length: 8 }, () => ({ ...value })),
  })
  return {
    key: 'tuple',
    arms: { record_limit: arm(), batch_optimized: arm(), ordinary: arm() },
  }
}

const validObservation = {
  status: 'valid',
  controls: true,
  recordsPerSecond: 100,
  p99: 10,
  fetchPer1000: 2,
  heapDelta: 0,
}

describe('Share throughput controls', () => {
  afterEach(() => jest.restoreAllMocks())

  test('counts two batches and independently counts one thousand messages', () => {
    expect(controls()).toEqual({
      status: 'controls-passed',
      arithmetic: { batches: 2, messages: 1000 },
      rejected: ['drop', 'omit', 'duplicate', 'fabricatedSum'],
      aaVoid: true,
      missingWindow: 'inconclusive',
    })
  })

  test('keeps handler entries separate from messages for batches and single messages', () => {
    const facts = deliveryFacts()
    expect(verifyDelivery(facts)).toEqual({ batches: 2, messages: 4 })
    expect(() => verifyDelivery({ ...facts, claimedBatches: 4 })).toThrow('Callback count differs')
    const single = facts.callbacks.flatMap(({ topic, partition, messages }) =>
      messages.map((entry) => ({ topic, partition, messages: [entry] }))
    )
    expect(
      verifyDelivery({
        ...facts,
        callbacks: single,
        observed: single.map((call, callback) => ({
          ...call,
          message: call.messages[0],
          callback,
        })),
        claimedBatches: 4,
      })
    ).toEqual({ batches: 4, messages: 4 })
  })

  test('uses every stable identity field and exact large offsets', () => {
    const row = { topic: 't', partition: 1, message: message('9007199254740993', null, 'a') }
    expect(identity(row)).toBe('["t",1,"9007199254740993",null,"61"]')
    const changed = [
      { ...row, topic: 'u' },
      { ...row, partition: 2 },
      { ...row, message: { ...row.message, offset: '9007199254740992' } },
      { ...row, message: { ...row.message, key: Buffer.alloc(0) } },
      { ...row, message: { ...row.message, value: Buffer.from('b') } },
      { ...row, message: { ...row.message, value: null } },
    ]
    for (const other of changed) expect(identity(other)).not.toBe(identity(row))
    expect(() => identity({ ...row, message: { ...row.message, offset: '-1' } })).toThrow()
    expect(() => identity({ ...row, message: { ...row.message, offset: '01' } })).toThrow()
  })

  test.each([
    ['drop', 'Observed batch count differs'],
    ['omit', 'Independent message sum differs'],
    ['duplicate', 'Duplicate callback identity'],
    ['fabricated sum', 'Fabricated message sum'],
    ['foreign identity', 'Callback corpus differs'],
  ])('refuses the independent %s corruption with its own diagnostic', (kind, diagnostic) => {
    const facts = deliveryFacts()
    if (kind === 'drop') {
      facts.callbacks = facts.callbacks.slice(0, 1)
      facts.claimedBatches = 1
      facts.claimedMessages = 2
    }
    if (kind === 'omit') facts.observed = facts.observed.slice(1)
    if (kind === 'duplicate') {
      facts.callbacks = [...facts.callbacks, facts.callbacks[0]]
      facts.observed = [
        ...facts.observed,
        ...facts.observed.slice(0, 2).map((row) => ({ ...row, callback: 2 })),
      ]
      facts.claimedBatches = 3
      facts.claimedMessages = 6
    }
    if (kind === 'fabricated sum') facts.claimedMessages = 5
    if (kind === 'foreign identity') {
      const [first] = facts.callbacks
      first.messages = [message(99), first.messages[1]]
      facts.observed = facts.observed.map((row, i) =>
        i === 0 ? { ...row, message: first.messages[0] } : row
      )
    }
    expect(() => verifyDelivery(facts)).toThrow(diagnostic)
  })
})

describe('Share throughput window', () => {
  test('starts at the first measured write and ends after the last piggyback answer', () => {
    const result = measureWindow(windowFacts())
    expect(result).toMatchObject({
      status: 'valid',
      records: 3000,
      callbacks: 6,
      independentMessageSum: 3000,
      start: 100,
      end: 310,
      fetch: 9,
      ack: 0,
      fetchPer1000: 3,
      p99: 90,
      latencySamples: 3000,
    })
    expect(result.recordsPerSecond).toBeCloseTo((3000 * 1000) / 210)
    expect(result.identities).toHaveLength(3000)
    expect(new Set(result.identities).size).toBe(3000)
  })

  test('refuses a steady acknowledgement frame but excludes the close after the window', () => {
    const facts = windowFacts()
    facts.frames.rows.push({ endpoint: 'n0', apiKey: 79, writeAt: 120, responseAt: 130 })
    expect(() => measureWindow(facts)).toThrow('Steady ShareAcknowledge')
    expect(measureWindow(windowFacts()).ack).toBe(0)
  })

  test.each([
    ['without an initial full acquisition', (f) => [...f.log.groups.values()][0].settled--],
    ['without a full successor on a node', (f) => [...f.log.groups.values()].at(-1).entries.pop()],
    ['with an unsettled acquisition in the run', (f) => [...f.log.groups.values()][6].settled--],
    [
      'with one node',
      (f) => {
        for (const [frame, group] of f.log.groups)
          if (group.frame.endpoint === 'n2') f.log.groups.delete(frame)
      },
    ],
    ['with a declared count that is not whole acquisitions', (f) => (f.target = 3001)],
    ['with no piggyback answer', (f) => (f.frames.rows[3].responseAt = 999)],
  ])('is inconclusive %s', (_, corrupt) => {
    const facts = windowFacts()
    corrupt(facts)
    expect(measureWindow(facts)).toMatchObject({ status: 'inconclusive' })
  })

  test('is inconclusive for an empty observation', () => {
    expect(
      measureWindow({
        frames: { rows: [], failure: null },
        log: { groups: new Map(), failure: null },
        maxRecords: 500,
      })
    ).toMatchObject({ status: 'inconclusive' })
  })

  test('refuses duplicate measured identities', () => {
    const facts = windowFacts()
    const groups = [...facts.log.groups.values()]
    groups[2].entries[0].message = groups[1].entries[0].message
    expect(() => measureWindow(facts)).toThrow('Duplicate measured identity')
  })

  test('makes the maxRecords one frame control move and refuses a frozen counter', () => {
    const result = measureWindow({ ...windowFacts(1), positive: true })
    expect(result.fetchPer1000).toBeGreaterThanOrEqual(LIMITS.frameFloor)
    expect(() => measureWindow(windowFacts(1))).toThrow('ShareFetch frame ceiling')
    const frozen = windowFacts(1)
    frozen.frames.rows = frozen.frames.rows.filter((row) => row.apiKey !== 78)
    expect(() => measureWindow({ ...frozen, positive: true })).toThrow('No ShareFetch observed')
  })

  test('refuses more ShareFetch frames than full acquisitions allow', () => {
    const facts = windowFacts()
    for (let i = 0; i < 20; i++)
      facts.frames.rows.push({ endpoint: 'n0', apiKey: 78, writeAt: 150, responseAt: 151 })
    expect(() => measureWindow(facts)).toThrow('ShareFetch frame ceiling')
  })

  test('refuses half-lock completion, a late acknowledgement and an early one', () => {
    const slow = windowFacts()
    ;[...slow.log.groups.values()][1].calls[0].lock = 20
    expect(() => measureWindow(slow)).toThrow('half lock')

    const late = windowFacts()
    const group = [...late.log.groups.values()][1]
    for (const entry of group.entries) entry.ackAt = 1000
    group.settledAt = 1000
    late.frames.rows.at(-1).writeAt = 1200
    expect(() => measureWindow(late)).toThrow('latency ceiling')

    const early = windowFacts()
    ;[...early.log.groups.values()][1].entries[0].ackAt = 0
    expect(() => measureWindow(early)).toThrow('Acknowledgement precedes return')
  })

  test('surfaces an earlier observer fault before measuring', () => {
    const facts = windowFacts()
    facts.frames.failure = new Error('observer fault')
    expect(() => measureWindow(facts)).toThrow('observer fault')
    const log = windowFacts()
    log.log.failure = new Error('log fault')
    expect(() => measureWindow(log)).toThrow('log fault')
  })
})

describe('Share throughput frame accounting', () => {
  test('parses split and coalesced headers without keeping payload bytes', () => {
    const seen = []
    const ended = []
    const parser = new HeaderStream(
      12,
      (header, at) => seen.push([header.readInt16BE(4), at]),
      (_, at) => ended.push(at)
    )
    const frame = request(78, 1, 60)
    parser.push(frame.subarray(0, 3), 1)
    parser.push(Buffer.concat([frame.subarray(3), frame]), 2)
    expect(seen).toEqual([
      [78, 1],
      [78, 2],
    ])
    expect(ended).toEqual([2, 2])
    expect(parser.prefix).toHaveLength(12)
  })

  test('refuses an impossible frame size', () => {
    const parser = new HeaderStream(8, () => {})
    expect(() => parser.push(Buffer.from([0xff, 0xff, 0xff, 0xff, 0, 0, 0, 1]), 0)).toThrow(
      'Invalid Kafka frame size'
    )
  })

  test('records written headers and keeps the socket write behavior', () => {
    const socket = fakeSocket()
    const original = socket.write
    const observer = new Frames(
      () => socket,
      () => 10
    )
    const wrapped = observer.socket({ host: 'broker', port: 9092 })
    const callback = jest.fn()
    expect(wrapped.write(request(78, 7), 'binary', callback)).toBe(false)
    expect(original).toHaveBeenCalledWith(expect.any(Buffer), 'binary', callback)
    wrapped.emit('data', response(7))
    expect(observer.rows).toMatchObject([
      {
        socketId: 1,
        endpoint: 'broker:9092',
        correlationId: 7,
        apiKey: 78,
        apiVersion: 15,
        writeAt: 10,
        responseAt: 10,
      },
    ])
    expect(observer.acquisition('broker:9092', 11)).toBe(observer.rows[0])
    expect(observer.failure).toBeNull()
  })

  test('times a fragmented request from its first byte and a response from its last', () => {
    let now = 0
    const socket = fakeSocket()
    const observer = new Frames(
      () => socket,
      () => ++now
    )
    const wrapped = observer.socket({ host: 'broker', port: 9092 })
    const frame = request(79, 3, 20)
    wrapped.write(frame.subarray(0, 5))
    wrapped.write(frame.subarray(5))
    const answer = response(3, 12)
    wrapped.emit('data', answer.subarray(0, 6))
    wrapped.emit('data', answer.subarray(6))
    expect(observer.rows).toMatchObject([{ apiKey: 79, writeAt: 1, responseAt: 4 }])
    expect(observer.failure).toBeNull()
  })

  test('matches coalesced responses by socket and allows correlation reuse across sockets', () => {
    const observer = new Frames(fakeSocket)
    const first = observer.socket({ host: 'a', port: 1 })
    const second = observer.socket({ host: 'b', port: 2 })
    first.write(request(78, 7))
    first.write(request(78, 8))
    second.write(request(78, 7))
    first.emit('data', Buffer.concat([response(7), response(8)]))
    expect(observer.rows.map((row) => row.responseAt !== null)).toEqual([true, true, false])
    second.emit('data', response(7))
    expect(observer.rows[2].responseAt).not.toBeNull()
    expect(observer.failure).toBeNull()
  })

  test('records a correlation reused on one socket and an unknown response', () => {
    const reused = new Frames(fakeSocket)
    const socket = reused.socket({ host: 'a', port: 1 })
    socket.write(request(78, 7))
    expect(socket.write(request(78, 7))).toBe(false)
    expect(() => reused.acquisition('a:1', 0)).toThrow('Correlation reused')

    const unknown = new Frames(fakeSocket)
    unknown.socket({ host: 'a', port: 1 }).emit('data', response(99))
    expect(unknown.failure.message).toContain('Response without written request')
  })

  test('forgets what a closed socket never answered', () => {
    const observer = new Frames(fakeSocket)
    const socket = observer.socket({ host: 'a', port: 1 })
    socket.write(request(78, 7))
    socket.emit('close')
    expect(observer.pending.size).toBe(0)
  })

  test('selects the latest answered ShareFetch of the leader endpoint', () => {
    const observer = new Frames(fakeSocket)
    const rows = [
      { endpoint: 'a', apiKey: 78, writeAt: 0, responseAt: 5 },
      { endpoint: 'a', apiKey: 78, writeAt: 10, responseAt: 15 },
      { endpoint: 'a', apiKey: 79, writeAt: 20, responseAt: 25 },
      { endpoint: 'b', apiKey: 78, writeAt: 21, responseAt: 30 },
      { endpoint: 'a', apiKey: 78, writeAt: 30, responseAt: null },
    ]
    observer.rows.push(...rows)
    expect(observer.acquisition('a', 12)).toBe(rows[0])
    expect(observer.acquisition('a', 40)).toBe(rows[1])
    expect(() => observer.acquisition('c', 40)).toThrow('No answered acquisition')
  })
})

describe('Share throughput acknowledgement matching', () => {
  const frame = { endpoint: 'a', apiKey: 78, writeAt: 0, responseAt: 1 }
  const batch = (offsets, partition = 0) => ({
    topic: 't',
    partition,
    messages: offsets.map((offset) => message(offset)),
    acquisitionLockTimeoutMs: LIMITS.lock,
  })
  const accept = (firstOffset, lastOffset, extra = {}) => ({
    topic: 't',
    partition: 0,
    firstOffset,
    lastOffset,
    type: 'accept',
    error: null,
    ...extra,
  })
  const logWith = (offsets) => {
    const log = new AcquisitionLog({ acquisition: () => frame }, new Map([[0, 'a']]))
    const call = log.enter(batch(offsets), 2)
    return { log, call }
  }

  test('settles a compact range once the handler has returned', () => {
    const { log, call } = logWith([5, 6, 7])
    log.returned(call, 3)
    log.acknowledge(accept('5', '7'), 4)
    expect(log.acknowledged).toBe(3)
    expect(log.delivered).toBe(3)
    expect(call.group).toMatchObject({ settled: 3, settledAt: 4 })
    expect(log.events).toEqual([
      { topic: 't', partition: 0, firstOffset: '5', lastOffset: '7', type: 'accept', at: 4 },
    ])
  })

  test('bounds a wide range by the delivered population without expanding it', () => {
    const { log, call } = logWith([5, 6, 7])
    log.returned(call, 3)
    expect(() => log.acknowledge(accept('0', '999999'), 4)).toThrow(
      'Unobserved acknowledgement range'
    )
    expect(() => log.acknowledge(accept('0', '99999999999999999999'), 4)).toThrow(
      'Unobserved acknowledgement range'
    )
  })

  test('keeps absolute offsets exact', () => {
    const { log, call } = logWith(['9007199254740993', '9007199254740994'])
    log.returned(call, 3)
    expect(() => log.acknowledge(accept('9007199254740992', '9007199254740992'), 4)).toThrow(
      'Repeated or premature'
    )
    log.acknowledge(accept('9007199254740993', '9007199254740994'), 4)
    expect(log.acknowledged).toBe(2)
  })

  test.each([
    ['a repeated result', 'Repeated or premature', (log) => log.acknowledge(accept('5', '5'), 5)],
    [
      'an error result',
      'Unexpected acknowledgement result',
      (log) => log.acknowledge(accept('6', '6', { error: new Error('x') }), 5),
    ],
    [
      'a rejected result',
      'Unexpected acknowledgement result',
      (log) => log.acknowledge(accept('6', '6', { type: 'reject' }), 5),
    ],
    [
      'a gap result',
      'Unexpected acknowledgement result',
      (log) => log.acknowledge(accept('6', '6', { type: 'gap' }), 5),
    ],
    [
      'a reversed range',
      'Reversed acknowledgement result',
      (log) => log.acknowledge(accept('6', '5'), 5),
    ],
  ])('refuses %s', (_, diagnostic, act) => {
    const { log, call } = logWith([5, 6])
    log.returned(call, 3)
    log.acknowledge(accept('5', '5'), 4)
    expect(() => act(log)).toThrow(diagnostic)
  })

  test('refuses a result before the handler returned', () => {
    const { log } = logWith([5])
    expect(() => log.acknowledge(accept('5', '5'), 4)).toThrow('Repeated or premature')
  })

  test('refuses a repeated offset and an acquisition spanning partitions', () => {
    const { log } = logWith([5])
    expect(() => log.enter(batch([5]), 3)).toThrow('Duplicate delivered offset')
    const leaders = new Map([
      [0, 'a'],
      [1, 'a'],
    ])
    const shared = new AcquisitionLog({ acquisition: () => frame }, leaders)
    shared.enter(batch([1], 0), 2)
    expect(() => shared.enter(batch([1], 1), 2)).toThrow('Acquisition spans partitions')
  })

  test('releases every retained call, entry and event', () => {
    const { log, call } = logWith([5])
    log.returned(call, 3)
    log.acknowledge(accept('5', '5'), 4)
    log.clear()
    expect([log.calls, log.groups.size, log.byPartition.size, log.events]).toEqual([[], 0, 0, []])
  })
})

describe('Share throughput driver', () => {
  afterEach(() => jest.restoreAllMocks())

  const drive = (arm = 'ordinary') => {
    const frames = new Frames(fakeSocket)
    const driver = new ShareThroughput(
      { arm, acquireMode: 'record_limit', maxRecords: 500, messages: 6 },
      { kafka: null, frames, resources: {}, trace: '' }
    )
    driver.log = new AcquisitionLog(frames, new Map([[0, 'a']]))
    return { driver, frames }
  }
  const batchOf = (first) => ({
    topic: 't',
    partition: 0,
    messages: [message(first), message(first + 1)],
    acquisitionLockTimeoutMs: LIMITS.lock,
  })
  const clock = (times) => {
    const queue = [...times]
    jest.spyOn(performance, 'now').mockImplementation(() => (queue.length ? queue.shift() : 1e9))
  }

  test('measures an ordinary arm between its first and last handler return', () => {
    const { driver } = drive()
    clock([0, 10, 20, 30, 50, 60])
    for (const first of [0, 2, 4]) driver.handle(batchOf(first))
    const expected = driver.calls.flatMap((call) =>
      call.messages.map((entry) => identity({ topic: 't', partition: 0, message: entry }))
    )
    expect(driver.drained(6)).toBe(true)
    expect(driver.evaluate(expected)).toEqual({
      delivered: { batches: 3, messages: 6 },
      metrics: { status: 'valid', recordsPerSecond: 80, p99: 30, fetchPer1000: 0, gapSamples: 2 },
    })
  })

  test('refuses an ordinary delivery that is not the corpus', () => {
    const { driver } = drive()
    clock([0, 10, 20, 30, 50, 60])
    for (const first of [0, 2, 4]) driver.handle(batchOf(first))
    const expected = driver.calls.flatMap((call) =>
      call.messages.map((entry) => identity({ topic: 't', partition: 0, message: entry }))
    )
    expected.pop()
    expect(() => driver.evaluate(expected)).toThrow()
  })

  test.each([
    [
      'a public fetch total that differs from the handler message sum',
      3,
      LIMITS.lock,
      'Public fetch count',
    ],
    [
      'a lock duration that is not the configured one',
      2,
      20,
      'Unexpected acquisition lock duration',
    ],
  ])('refuses a share delivery with %s', (_, fetched, lock, diagnostic) => {
    const { driver, frames } = drive('share')
    frames.rows.push({ endpoint: 'a', apiKey: 78, writeAt: 0, responseAt: 1 })
    clock([2, 3])
    driver.handle({ ...batchOf(0), acquisitionLockTimeoutMs: lock })
    driver.fetchEvents.push({ numberOfRecords: fetched })
    const expected = driver.log.calls.flatMap((call) =>
      call.messages.map((entry) => identity({ topic: 't', partition: 0, message: entry }))
    )
    expect(() => driver.evaluate(expected)).toThrow(diagnostic)
  })

  test('waits for terminal accepted results of every share record and surfaces faults', () => {
    const { driver, frames } = drive('share')
    frames.rows.push({ endpoint: 'a', apiKey: 78, writeAt: 0, responseAt: 1 })
    clock([2, 3, 4])
    driver.handle(batchOf(0))
    expect(driver.drained(2)).toBe(false)
    driver.log.acknowledge(
      { topic: 't', partition: 0, firstOffset: '0', lastOffset: '1', type: 'accept', error: null },
      4
    )
    expect(driver.drained(2)).toBe(true)
    frames.failure = new Error('observer fault')
    expect(() => driver.drained(2)).toThrow('observer fault')
  })

  test('writes an exclusive hashed trace and releases variable observer storage', () => {
    const { driver, frames } = drive()
    clock([0, 10])
    driver.handle(batchOf(0))
    frames.rows.push({ endpoint: 'a', apiKey: 78, writeAt: 0, responseAt: 1 })
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'share-trace-'))
    try {
      driver.trace = path.join(directory, 'trace.jsonl')
      const proof = { stage: 'quiet-verifier', status: 'quiet', delivered: [], windowMs: 5000 }
      driver.terminal.push(proof)
      const metrics = { status: 'valid', identities: ['x'] }
      const hash = driver.archive(metrics, { batches: 1, messages: 2 })
      const text = fs.readFileSync(driver.trace, 'utf8')
      expect(hash).toBe(crypto.createHash('sha256').update(text).digest('hex'))
      const rows = text
        .trim()
        .split('\n')
        .map((line) => JSON.parse(line))
      expect(rows.map((row) => row.kind)).toEqual([
        'summary',
        'frame',
        'callback',
        'terminal-proof',
        'measured',
      ])
      expect(rows[2].identities).toHaveLength(2)
      expect(rows[3]).toEqual({ kind: 'terminal-proof', ...proof })
      expect(metrics).toEqual({ status: 'valid' })
      expect([driver.calls, frames.rows, driver.log.events, driver.terminal]).toEqual([
        [],
        [],
        [],
        [],
      ])
      expect(() => driver.archive({ status: 'valid' }, { batches: 0, messages: 0 })).toThrow()
    } finally {
      fs.rmSync(directory, { recursive: true, force: true })
    }
  })

  test('keeps workload observers until disconnect has joined', async () => {
    const { driver } = drive('share')
    const steps = []
    driver.removers.push(() => steps.push('remove'))
    driver.consumer = {
      disconnect: async () => {
        steps.push('disconnect')
        expect(driver.removers).toHaveLength(1)
      },
    }
    await driver.disconnect(performance.now() + 10000)
    expect(steps).toEqual(['disconnect', 'remove'])
    expect(driver.closed).toBe(true)
  })

  test('runs one observation per driver and needs a collector', async () => {
    const { driver } = drive()
    const gc = globalThis.gc
    globalThis.gc = undefined
    await expect(driver.run({})).rejects.toThrow('--expose-gc')
    globalThis.gc = () => {}
    try {
      driver.used = true
      await expect(driver.run({})).rejects.toThrow('one observation')
    } finally {
      globalThis.gc = gc
    }
  })
})

describe('Share throughput terminal proof', () => {
  afterEach(() => {
    jest.restoreAllMocks()
    jest.useRealTimers()
  })

  /**
   * Public-client doubles retain the actual terminal and active-verifier helper behavior.
   * Monotonic time starts beyond the old fixed deadlines and advances with the fake timers.
   * @param {object} [options] Scripted verifier and post-verifier failures.
   * @returns {object} Driver, original admin observations and clock-relative deadlines, without broker I/O.
   */
  const fixture = ({ silent = false, replay = false, drainReplay = false, failedRead } = {}) => {
    jest.useFakeTimers({ doNotFake: ['performance'] })
    const startedAt = Date.now()
    jest.spyOn(performance, 'now').mockImplementation(() => 1000000.25 + (Date.now() - startedAt))
    const deadline = performance.now() + 100000
    const steps = []
    const frames = new Frames(fakeSocket)
    const ends = [0, 1, 2].map((partition) => ({ partition, high: '1' }))
    let verified = false
    const listing = [
      {
        topic: 't',
        topicId: '11111111-1111-1111-1111-111111111111',
        partitions: ends.map(({ partition }) => ({
          partition,
          startOffset: '0',
          lag: '0',
          errorCode: 0,
          leaderEpoch: 7,
        })),
      },
    ]
    const admin = {
      connect: async () => {
        steps.push('admin-connect')
      },
      disconnect: async () => {
        steps.push('admin-disconnect')
      },
      fetchTopicMetadata: async () => ({ topics: [{ name: 't', topicId: listing[0].topicId }] }),
      fetchTopicOffsets: async () => {
        steps.push('ends')
        if (verified && failedRead === 'ends') throw new Error('Post-verifier end unavailable')
        return ends.map((row) => ({ ...row }))
      },
      listShareGroupOffsets: async ({ groupId, topics }) => {
        expect(groupId).toBe(driver.groupId)
        steps.push(topics ? 'named' : 'all')
        if (verified && failedRead === 'listing') throw new Error('Post-verifier listing refused')
        return listing
      },
    }
    const kafka = {
      admin: () => admin,
      shareConsumer: ({ groupId }) => {
        expect(groupId).toBe(driver.groupId)
        const emitter = new EventEmitter()
        let timer
        let handler
        return {
          events: { REQUEST: 'request', CRASH: 'crash' },
          on: (name, listener) => {
            emitter.on(name, listener)
            return () => emitter.off(name, listener)
          },
          connect: async () => {
            steps.push('verifier-connect')
          },
          subscribe: async ({ topics }) => {
            expect(topics).toEqual(['t'])
          },
          assignment: () => [{ topic: 't', partitions: [0, 1, 2] }],
          run: async ({ eachMessage }) => {
            handler = eachMessage
            emitter.emit('request', { payload: { apiName: 'ShareFetch', sentAt: Date.now() - 1 } })
            if (!silent)
              timer = setInterval(() => {
                emitter.emit('request', { payload: { apiName: 'ShareFetch', sentAt: Date.now() } })
              }, 250)
            if (replay) await eachMessage({ topic: 't', partition: 0, message: message(0) })
          },
          disconnect: async () => {
            clearInterval(timer)
            if (drainReplay) await handler({ topic: 't', partition: 0, message: message(0) })
            verified = true
            steps.push('verifier-disconnect')
          },
        }
      },
    }
    const quiet = {
      shareSessions: 0,
      shareRecords: 0,
      shareRanges: 0,
      queueDepth: 0,
      heapUsed: 5000,
    }
    const driver = new ShareThroughput(
      { arm: 'share', acquireMode: 'record_limit' },
      {
        kafka,
        frames,
        trace: '',
        resources: {
          snapshot: async () => {
            steps.push('snapshot')
            return quiet
          },
        },
      }
    )
    driver.consumer = {
      disconnect: async () => {
        steps.push('workload-disconnect')
      },
    }
    return {
      driver,
      steps,
      ends,
      listing,
      startedAt,
      deadline,
      cleanupDeadline: deadline + LIMITS.cleanup,
    }
  }

  /**
   * @param {object} fixture Original group and wall-clock origin of the scripted verifier.
   * @param {boolean} [silent=false] Whether only the pre-window fetch completes.
   * @returns {object[]} Exact inherited verifier sub-steps for the five-second observation.
   */
  const verifierProgress = ({ driver, startedAt }, silent = false) => {
    const row = (step, elapsedMs = 0, fields = {}) => ({
      stage: 'quiet-verifier',
      status: 'progress',
      groupId: driver.groupId,
      topic: 't',
      step,
      elapsedMs,
      ...fields,
    })
    const assignment = [{ topic: 't', partitions: [0, 1, 2] }]
    return [
      row('connect start'),
      row('connected'),
      row('join start'),
      row('first fetch', 0, { payload: { apiName: 'ShareFetch', sentAt: startedAt - 1 } }),
      row('run resolved'),
      row('window start', 0, { assignment, fetches: 1, windowMs: 5000 }),
      row('window end', 5000, { assignment, fetches: silent ? 1 : 20, windowMs: 5000 }),
      row('disconnect start', 5000),
      row('disconnected', 5000),
    ]
  }

  test('captures ends before consumption and archives both terminal reads after an active quiet window', async () => {
    const setup = fixture()
    const { driver, steps, listing, deadline, cleanupDeadline } = setup
    jest.spyOn(driver, 'admit').mockResolvedValue()
    jest.spyOn(driver, 'consume').mockImplementation(async () => {
      steps.push('consume')
    })
    jest
      .spyOn(driver, 'evaluate')
      .mockReturnValue({ delivered: { batches: 1, messages: 3 }, metrics: { status: 'valid' } })
    const archive = jest.spyOn(driver, 'archive').mockImplementation(() => {
      steps.push('archive')
      return 'hash'
    })

    const observation = driver.observe(
      { topic: 't', expected: ['corpus'], leaders: new Map() },
      deadline,
      cleanupDeadline
    )
    const startedAt = performance.now()
    await Promise.all([
      expect(observation).resolves.toMatchObject({ status: 'valid', traceHash: 'hash' }),
      (async () => {
        await jest.advanceTimersByTimeAsync(4999)
        expect(performance.now() - startedAt).toBe(4999)
        expect(steps).toContain('verifier-connect')
        expect(steps).not.toContain('verifier-disconnect')
        expect(archive).not.toHaveBeenCalled()
        await jest.advanceTimersByTimeAsync(1)
      })(),
    ])
    expect(performance.now() - startedAt).toBe(5000)

    expect(
      steps.filter((step) =>
        [
          'ends',
          'consume',
          'named',
          'all',
          'workload-disconnect',
          'verifier-connect',
          'verifier-disconnect',
          'archive',
        ].includes(step)
      )
    ).toEqual([
      'ends',
      'consume',
      'named',
      'all',
      'ends',
      'workload-disconnect',
      'verifier-connect',
      'verifier-disconnect',
      'named',
      'all',
      'ends',
      'archive',
    ])
    expect(steps.at(-1)).toBe('snapshot')
    expect(archive).toHaveBeenCalledTimes(1)
    expect(driver.terminal).toEqual([
      expect.objectContaining({
        stage: 'log-end',
        ends: [0, 1, 2].map((partition) => ({ partition, high: '1' })),
      }),
      expect.objectContaining({
        stage: 'terminal',
        status: 'settled',
        last: expect.objectContaining({ named: listing, all: listing }),
      }),
      ...verifierProgress(setup),
      expect.objectContaining({
        stage: 'quiet-verifier',
        status: 'quiet',
        windowMs: 5000,
        delivered: [],
      }),
      expect.objectContaining({
        stage: 'quiet-verifier',
        status: 'settled',
        last: expect.objectContaining({ named: listing, all: listing }),
      }),
    ])
  })

  test.each([
    [{ replay: true }, 'Same-group verifier delivered'],
    [{ drainReplay: true }, 'Same-group verifier delivered'],
    [{ silent: true }, 'The assigned verifier completed no active fetch in its observation window'],
    [{ failedRead: 'ends' }, 'Post-verifier end unavailable'],
    [{ failedRead: 'listing' }, 'Post-verifier listing refused'],
  ])('rejects an incomplete terminal proof: %s', async (options, diagnostic) => {
    const setup = fixture(options)
    const { driver, steps, deadline } = setup
    await driver.captureEnds('t', deadline)
    await Promise.all([
      expect(driver.verifyTerminal('t', deadline)).rejects.toThrow(diagnostic),
      jest.advanceTimersByTimeAsync(5000),
    ])
    expect(steps).toContain('verifier-disconnect')
    expect(driver.terminal[1]).toMatchObject({ stage: 'terminal', status: 'settled' })
    const proof = driver.terminal.filter(({ stage }) => stage === 'quiet-verifier')
    if (options.replay || options.drainReplay) {
      expect(proof).toEqual([
        ...verifierProgress(setup),
        expect.objectContaining({
          status: 'failed',
          delivered: ['["t",0,"0","6b","76"]'],
        }),
      ])
    } else if (options.silent) {
      expect(proof).toEqual(verifierProgress(setup, true))
    } else {
      expect(proof).toEqual([
        ...verifierProgress(setup),
        expect.objectContaining({ status: 'quiet', delivered: [] }),
        expect.objectContaining({ status: 'failed' }),
      ])
    }
  })

  test.each([
    (ends) => {
      ends[0].high = null
    },
    (ends) => {
      ends[0].high = '01'
    },
    (ends) => {
      ends[0].partition = 1
    },
    (ends) => {
      ends.pop()
    },
  ])('refuses an invalid independent end before consumption', async (mutate) => {
    const { driver, ends, steps, deadline } = fixture()
    mutate(ends)
    await expect(driver.captureEnds('t', deadline)).rejects.toThrow('Invalid independent log ends')
    expect(steps).toEqual(['admin-connect', 'ends', 'admin-disconnect'])
    expect(driver.partitions).toBeNull()
    expect(driver.terminal).toEqual([])
  })
})

describe('Share throughput resources', () => {
  const quiet = { shareSessions: 0, shareRecords: 0, shareRanges: 0, queueDepth: 0, heapUsed: 5000 }

  test.each([
    [
      'a missing entry count',
      { ...quiet, shareRecords: undefined },
      'Missing resource measurement: shareRecords',
    ],
    [
      'a negative range count',
      { ...quiet, shareRanges: -1 },
      'Missing resource measurement: shareRanges',
    ],
    [
      'a fractional entry count',
      { ...quiet, shareRecords: 0.5 },
      'Missing resource measurement: shareRecords',
    ],
    ['a retained entry', { ...quiet, shareRecords: 1 }, 'Ledger is not empty at idle'],
    ['a retained range', { ...quiet, shareRanges: 1 }, 'Compact ranges remain at idle'],
  ])('refuses %s at idle', (_, snapshot, diagnostic) => {
    expect(() => assertIdle(snapshot, { share: true })).toThrow(diagnostic)
  })

  test('accepts an idle share arm and ignores share counters for the ordinary arm', () => {
    expect(() => assertIdle(quiet, { share: true })).not.toThrow()
    expect(() => assertIdle({}, { share: false })).not.toThrow()
  })

  test.each([
    ['a retained session', { shareSessions: 1 }, 'Retained share session'],
    ['a retained delivered entry', { shareRecords: 1 }, 'Retained delivered entry'],
    ['a retained compact range', { shareRanges: 1 }, 'Retained compact range'],
    ['a retained request', { queueDepth: 1 }, 'Retained request'],
    ['a heap above the ceiling', { heapUsed: 5000 + LIMITS.heap + 1 }, 'Retained heap ceiling'],
    ['a missing heap reading', { heapUsed: undefined }, 'Missing heap measurement'],
    ['a missing session count', { shareSessions: undefined }, 'Missing resource measurement'],
  ])('refuses %s after disconnect', (_, change, diagnostic) => {
    expect(() => assertReleased(quiet, { ...quiet, ...change }, { share: true })).toThrow(
      diagnostic
    )
  })

  test('reports the retained heap at and below its ceiling', () => {
    const after = (heapUsed) => ({ ...quiet, heapUsed })
    expect(assertReleased(quiet, after(5000 + LIMITS.heap), { share: true })).toBe(LIMITS.heap)
    expect(assertReleased(quiet, after(4000), { share: false })).toBe(-1000)
  })

  test('uses nearest-rank quantiles and refuses missing samples', () => {
    expect(quantile([7, 1, 5, 3, 2, 6, 4], 0.5)).toBe(4)
    const hundred = Array.from({ length: 100 }, (_, i) => i + 1)
    expect(quantile(hundred, 0.5)).toBe(50)
    expect(quantile(hundred, 1)).toBe(100)
    expect(() => quantile([], 0.5)).toThrow('Missing finite samples')
    expect(() => quantile([1, NaN], 0.5)).toThrow('Missing finite samples')
  })
})

describe('Share throughput sessions', () => {
  test('accepts identical repetitions and keeps medians of the seven retained observations', () => {
    const session = sessionOf(validObservation)
    session.arms.record_limit.a[0].recordsPerSecond = 1e6
    const result = evaluateSession(session)
    expect(result.status).toBe('accepted')
    expect(result.arms.record_limit).toEqual({
      recordsPerSecond: 100,
      p99: 10,
      fetchPer1000: 2,
      heapDelta: 0,
    })
  })

  test('voids a session whose identical trees differ by more than three percent', () => {
    const session = sessionOf(validObservation)
    for (const row of session.arms.record_limit.aprime) row.recordsPerSecond = 104
    expect(evaluateSession(session)).toMatchObject({
      status: 'void',
      arm: 'record_limit',
      metric: 'recordsPerSecond',
    })
    for (const row of session.arms.record_limit.aprime) row.recordsPerSecond = 102.9
    expect(evaluateSession(session).status).toBe('accepted')
  })

  test('voids a zero against a nonzero reading and accepts zero against zero', () => {
    const session = sessionOf({ ...validObservation, fetchPer1000: 0 })
    expect(evaluateSession(session).status).toBe('accepted')
    for (const row of session.arms.ordinary.aprime) row.fetchPer1000 = 1
    expect(evaluateSession(session)).toMatchObject({ status: 'void', arm: 'ordinary' })
  })

  test('refuses incomplete, unaccepted and over-ceiling observations', () => {
    const missing = sessionOf(validObservation)
    missing.arms.ordinary.a.pop()
    expect(() => evaluateSession(missing)).toThrow('one discard and seven')
    const unaccepted = sessionOf(validObservation)
    unaccepted.arms.ordinary.a[3].status = 'inconclusive'
    expect(() => evaluateSession(unaccepted)).toThrow('Unaccepted observation')
    const uncontrolled = sessionOf(validObservation)
    uncontrolled.arms.ordinary.a[3].controls = false
    expect(() => evaluateSession(uncontrolled)).toThrow('Unaccepted observation')
    const heavy = sessionOf(validObservation)
    heavy.arms.record_limit.aprime[7].heapDelta = LIMITS.heap + 1
    expect(() => evaluateSession(heavy)).toThrow('Retained heap ceiling')
    const partial = sessionOf(validObservation)
    delete partial.arms.ordinary
    expect(() => evaluateSession(partial)).toThrow('Expected every arm')
  })

  test('compares later throughput per arm against the baseline of the same key', () => {
    const session = sessionOf(validObservation)
    const baseline = (rate, key = 'tuple') => ({
      key,
      arms: Object.fromEntries(
        Object.keys(session.arms).map((arm) => [arm, { recordsPerSecond: rate }])
      ),
    })
    expect(evaluateSession(session, baseline(103)).status).toBe('accepted')
    expect(() => evaluateSession(session, baseline(104))).toThrow('regression')
    expect(() => evaluateSession(session, baseline(100, 'other'))).toThrow('comparability key')
    const borrowed = baseline(100)
    delete borrowed.arms.ordinary
    expect(() => evaluateSession(session, borrowed)).toThrow('Missing baseline: ordinary')
  })
})

describe('Share throughput corpus and arguments', () => {
  const argv = [
    '--messages',
    '200000',
    '--size',
    '100',
    '--partitions',
    '3',
    '--acquire-mode',
    'record_limit',
    '--max-records',
    '500',
    '--control',
    '1',
  ]

  test('produces explicit-partition batches of one full acquisition with derived identities', async () => {
    const next = [7, 7, 7]
    const sends = []
    const producer = {
      send: async ({ topic, acks, messages }) => {
        const partition = messages[0].partition
        sends.push({ topic, acks, partition, size: messages.length })
        const baseOffset = String(next[partition])
        next[partition] += messages.length
        return [{ partition, baseOffset }]
      },
    }
    const expected = await produceCorpus(producer, 'topic', { messages: 3000 })
    expect(sends).toHaveLength(6)
    expect(sends.every((send) => send.acks === -1 && send.size === 500)).toBe(true)
    expect(expected).toHaveLength(3000)
    expect(new Set(expected).size).toBe(3000)
    expect(expected[0]).toBe(
      identity({
        topic: 'topic',
        partition: 0,
        message: message('7', 'k000000', '0'.padStart(LIMITS.size, '0')),
      })
    )
    expect(JSON.parse(expected[1])[4]).toHaveLength(LIMITS.size * 2)
  })

  test('refuses a producer acknowledgement without a base offset', async () => {
    const producer = { send: async () => [{ partition: 0 }] }
    await expect(produceCorpus(producer, 'topic', { messages: 3 })).rejects.toThrow(
      'Missing producer offset'
    )
  })

  test('accepts explicit values and defaults to the share arm', () => {
    expect(parseArgs(argv)).toEqual({
      messages: 200000,
      size: 100,
      partitions: 3,
      acquireMode: 'record_limit',
      maxRecords: 500,
      control: 1,
      arm: 'share',
    })
    expect(parseArgs([...argv, '--arm', 'ordinary']).arm).toBe('ordinary')
  })

  test('accepts the separate frame control corpus only as a record_limit share run', () => {
    const frame = argv.map((value) => ({ 200000: '3000', 500: '1' })[value] ?? value)
    expect(parseArgs(frame)).toMatchObject({ messages: 3000, maxRecords: 1 })
    expect(() =>
      parseArgs(frame.map((v) => (v === 'record_limit' ? 'batch_optimized' : v)))
    ).toThrow()
    expect(() => parseArgs([...frame, '--arm', 'ordinary'])).toThrow()
    expect(() => parseArgs(argv.map((v) => (v === '500' ? '1' : v)))).toThrow()
  })

  test('refuses absent, duplicate, unknown and malformed arguments', () => {
    expect(() => parseArgs(argv.slice(0, -1))).toThrow()
    expect(() => parseArgs(argv.slice(0, -2))).toThrow('Missing --control')
    expect(() => parseArgs([...argv, '--size', '100'])).toThrow()
    expect(() => parseArgs([...argv, '--extra', '1'])).toThrow()
    expect(() => parseArgs(['messages', '200000', ...argv.slice(2)])).toThrow()
    expect(() => parseArgs([])).toThrow()
    for (const [from, to] of [
      ['500', '500.0'],
      ['200000', '200000.0'],
      ['100', '0100'],
      ['1', '2'],
      ['record_limit', 'other'],
    ])
      expect(() => parseArgs(argv.map((v) => (v === from ? to : v)))).toThrow()
    expect(() => parseArgs([...argv, '--arm', 'other'])).toThrow('Invalid arm')
  })

  test('splits a leading fixture action from the configuration', () => {
    expect(parseCommand(argv).phase).toBe('run')
    expect(parseCommand(['--prepare', ...argv])).toMatchObject({
      phase: 'prepare',
      config: { maxRecords: 500 },
    })
    expect(parseCommand(['--cleanup', ...argv]).phase).toBe('cleanup')
    expect(() => parseCommand(['--prepare'])).toThrow()
  })
})

describe('Share corpus topic cleanup', () => {
  afterEach(() => {
    jest.restoreAllMocks()
    jest.useRealTimers()
  })

  test('proves absence on every broker with a fractional remaining duration', async () => {
    jest.useFakeTimers({ doNotFake: ['performance'] })
    jest.spyOn(performance, 'now').mockReturnValue(100.25)
    const timeout = jest.spyOn(AbortSignal, 'timeout')
    const emitter = new EventEmitter()
    const brokers = ['a', 'b', 'c'].map((host) => ({ host, port: 9092 }))
    const visited = []
    const fixture = Object.create(CorpusFixture.prototype)
    fixture.admin = {
      events: { REQUEST: 'request' },
      on: (name, listener) => {
        emitter.on(name, listener)
        return () => emitter.off(name, listener)
      },
      deleteTopics: async () => {},
      describeCluster: async () => ({ brokers }),
      listGroups: async () => ({ groups: [] }),
      listTopics: async () => {
        const broker = `${brokers[visited.length].host}:9092`
        visited.push(broker)
        emitter.emit('request', { payload: { apiName: 'Metadata', broker } })
        return []
      },
    }

    const cleanup = fixture.removeTopic('owned', 1100)
    await jest.advanceTimersByTimeAsync(500)
    await cleanup

    expect(timeout).toHaveBeenCalledWith(999)
    expect(visited).toEqual(['a:9092', 'b:9092', 'c:9092'])
    expect(emitter.listenerCount('request')).toBe(0)
  })

  test('refuses to extend a submillisecond cleanup remainder', async () => {
    jest.spyOn(performance, 'now').mockReturnValue(100.25)
    const timeout = jest.spyOn(AbortSignal, 'timeout')
    const fixture = Object.create(CorpusFixture.prototype)
    fixture.admin = { deleteTopics: async () => {} }

    await expect(fixture.removeTopic('owned', 100.75)).rejects.toThrow(
      'Benchmark deadline exceeded'
    )
    expect(timeout).not.toHaveBeenCalled()
  })
})
