const crypto = require('crypto')
const { Kafka, CompressionTypes, CompressionCodecs } = require('../../../index')
const {
  KafkaJSDeliveryTimeout,
  KafkaJSProtocolError,
  KafkaJSRecordTooLargeError,
} = require('../../errors')
const Broker = require('../../broker')
const Accumulator = require('../state/accumulator')
const PartitionSequencer = require('../state/partitionSequencer')
const { RequestBudget } = require('../state/recordSize')
const { Schedule } = require('../../../testHelpers/schedule')
const { createScriptedCluster, producerScript } = require('../../../testHelpers/scriptedCluster')

const TOPIC = 'split-topic'
const CAP = 1048576
/** Short, fixed backoffs: a retried chunk waits 10 ms. */
const RETRY = { retries: 10, initialRetryTime: 10, maxRetryTime: 10, factor: 0 }
const PATHS = [
  ['the idempotent path', { idempotent: true }],
  ['the plain path', { idempotent: false }],
]

const idOf = (index) => `00000000-0000-4000-8000-${String(index + 1).padStart(12, '0')}`
const tick = () => new Promise((resolve) => setImmediate(resolve))

/** Waits, a turn at a time, for `predicate`, and fails the test when it never holds. */
const until = async (predicate, turns = 2000) => {
  for (let i = 0; i < turns && !predicate(); i += 1) await tick()
  expect(predicate()).toBe(true)
}

/**
 * A scripted cluster of `nodes` whose Metadata lists `topics` (`leaders[i]` leads partition i)
 * and whose Produce answers number records as a partition log does, from `offsets` on. `verdict`
 * picks an error code for one written partition batch; 0 accepts it. Every Produce frame the
 * client writes is measured.
 */
const scripted = ({ topics, nodes = 1, profile = 'default', offsets = {} }) => {
  const cluster = createScriptedCluster({
    nodes: Array.from({ length: nodes }, (_, id) => ({ id, host: `node${id}`, port: 9092 })),
    profile,
    schedule: new Schedule(),
  })
  producerScript(cluster)
  const frames = []
  const factory = cluster.socketFactory
  cluster.socketFactory = (options) => {
    const socket = factory(options)
    const write = socket.write
    socket.write = (buffer, ...rest) => {
      const frame = Buffer.from(buffer)
      if (frame.readInt16BE(4) === 0) frames.push(frame.length)
      return write(buffer, ...rest)
    }
    return socket
  }
  const names = new Map(topics.map(({ name }, index) => [idOf(index), name]))
  const next = new Map(Object.entries(offsets))
  const log = { written: [], requests: [], reverse: false, verdict: () => 0 }
  cluster.answer('Metadata', () => ({
    brokers: cluster.nodes.map(({ id, host, port }) => ({ nodeId: id, host, port, rack: null })),
    controllerId: 0,
    topics: topics.map(({ name, leaders }, index) => ({
      errorCode: 0,
      name,
      topicId: idOf(index),
      isInternal: false,
      topicAuthorizedOperations: -2147483648,
      partitions: leaders.map((leaderId, partitionIndex) => ({
        errorCode: 0,
        partitionIndex,
        leaderId,
        leaderEpoch: 5,
        replicas: [leaderId],
        isr: [leaderId],
        offlineReplicas: [],
      })),
    })),
  }))
  cluster.answer('Produce', ({ topics: requested }) => {
    log.requests.push(
      requested.flatMap(({ partitions, ...key }) =>
        partitions.map(
          ({ partition }) => `${key.name ?? key.topic ?? names.get(key.topicId)}/${partition}`
        )
      )
    )
    return {
      responses: requested.map(({ partitions, ...key }) => {
        const answered = partitions.map(({ partition, batches }) => {
          const topic = key.name ?? key.topic ?? names.get(key.topicId)
          const records = batches.reduce((sum, batch) => sum + batch.recordCount, 0)
          const entry = { topic, partition, records, batch: batches[0] }
          const errorCode = log.verdict(entry)
          log.written.push({ ...entry, errorCode })
          const slot = `${topic}/${partition}`
          const base = Number(next.get(slot) ?? 0)
          if (errorCode === 0) next.set(slot, base + records)
          return {
            partition,
            errorCode,
            baseOffset: errorCode === 0 ? String(base) : '-1',
            logAppendTime: '-1',
            logStartOffset: '0',
            leader: null,
          }
        })
        return { ...key, partitions: log.reverse ? answered.reverse() : answered }
      }),
      nodeEndpoints: [],
    }
  })
  return { cluster, frames, log }
}

describe('Producer > a send larger than one request', () => {
  let producer, cluster, saved

  beforeEach(() => {
    saved = { ...CompressionCodecs }
  })

  afterEach(async () => {
    if (producer) await producer.disconnect()
    if (cluster) cluster.assertClean()
    Object.assign(CompressionCodecs, saved)
    jest.restoreAllMocks()
    producer = cluster = null
  })

  /** Starts a producer over `scripted(setup)` and connects it. */
  const start = async (setup, options = {}, kafka = {}) => {
    const fixture = scripted(setup)
    cluster = fixture.cluster
    producer = new Kafka({
      ...cluster.kafkaOptions(),
      requestTimeout: 1000,
      enforceRequestTimeout: false,
      ...kafka,
    }).producer({ linger: 0, deliveryTimeout: 60000, retry: RETRY, ...options })
    await producer.connect()
    return fixture
  }

  /** Counts the Produce requests the producer builds, refused ones included. */
  const attempts = () => {
    const calls = []
    const produce = Broker.prototype.produce
    jest.spyOn(Broker.prototype, 'produce').mockImplementation(function (request) {
      calls.push(request)
      return produce.call(this, request)
    })
    return calls
  }

  const records = (count, bytes, prefix = 'r') =>
    Array.from({ length: count }, (_, i) => ({
      value: `${prefix}${i}:`.padEnd(bytes, 'x'),
      partition: 0,
    }))

  describe.each(PATHS)('on %s', (_, path) => {
    test('5 MiB of 100 KiB records for one partition go out in order, in requests within maxRequestSize', async () => {
      const { frames, log } = await start({ topics: [{ name: TOPIC, leaders: [0] }] }, path)

      const metadata = await producer.send({ topic: TOPIC, messages: records(52, 102400) })

      expect(frames.length).toBeGreaterThanOrEqual(5)
      expect(Math.max(...frames)).toBeLessThanOrEqual(CAP)
      expect(log.written.map(({ records: count }) => count)).toEqual([10, 10, 10, 10, 10, 2])
      if (path.idempotent) {
        expect(log.written.map(({ batch }) => batch.baseSequence)).toEqual([0, 10, 20, 30, 40, 50])
      }
      expect(metadata).toEqual([expect.objectContaining({ partition: 0, baseOffset: '0' })])
    })

    test('a delayed first chunk, a retried second one and independent partitions answered in reverse: every chunk once, the send resolves with its first record', async () => {
      const { log } = await start(
        { topics: [{ name: TOPIC, leaders: [0, 0] }], offsets: { [`${TOPIC}/0`]: 100 } },
        path
      )
      log.reverse = true
      let calls = 0
      let release
      const held = new Promise((resolve) => {
        release = resolve
      })
      const answer = cluster.answerers.get('Produce')
      cluster.answer('Produce', async (fields, record) => {
        calls += 1
        if (calls === 1) await held
        return answer(fields, record)
      })
      let seconds = 0
      log.verdict = ({ partition, batch, records: count }) =>
        partition === 0 &&
        count === 10 &&
        log.written.filter((w) => w.partition === 0).length === 1 &&
        seconds++ === 0
          ? 19
          : 0
      const messages = [...records(30, 102400), { value: 'other', partition: 1 }]

      const sent = producer.send({ topic: TOPIC, messages })
      await until(() => calls === 1)
      await tick()
      expect(log.written).toEqual([])
      release()
      const metadata = await sent

      const zero = log.written.filter(({ partition }) => partition === 0)
      expect(zero.map(({ records: count, errorCode }) => [count, errorCode])).toEqual([
        [10, 0],
        [10, 19],
        [10, 0],
        [10, 0],
      ])
      expect(log.written.filter(({ partition }) => partition === 1)).toHaveLength(1)
      expect(metadata).toEqual([
        expect.objectContaining({ partition: 0, baseOffset: '100' }),
        expect.objectContaining({ partition: 1, baseOffset: '0' }),
      ])
      expect(log.requests.every((request) => new Set(request).size === request.length)).toBe(true)
    })

    test('a middle chunk failing for good rejects the send with its own error, unwrapped, and the later chunks are still delivered', async () => {
      const { log } = await start({ topics: [{ name: TOPIC, leaders: [0] }] }, path)
      log.verdict = () => (log.written.length === 1 ? 87 : 0)

      const error = await producer
        .send({ topic: TOPIC, messages: records(30, 102400) })
        .catch((failure) => failure)

      expect(error).toBeInstanceOf(KafkaJSProtocolError)
      expect(error).toMatchObject({ type: 'INVALID_RECORD', code: 87 })
      expect(error.cause).toBeUndefined()
      expect('outcome' in error).toBe(false)
      expect(log.written.map(({ errorCode }) => errorCode)).toEqual([0, 87, 0])
    })

    test('sends sharing the batch that fails reject with the same error object', async () => {
      const { log } = await start(
        { topics: [{ name: TOPIC, leaders: [0] }] },
        {
          ...path,
          linger: 50,
          batchSize: 4 * CAP,
        }
      )
      log.verdict = ({ records: count }) => (count === 11 ? 87 : 0)

      const lone = producer.send({ topic: TOPIC, messages: records(1, 10) }).catch((e) => e)
      const split = producer.send({ topic: TOPIC, messages: records(12, 102400) }).catch((e) => e)
      const [a, b] = await Promise.all([lone, split])

      expect(log.written.map(({ records: count, errorCode }) => [count, errorCode])).toEqual([
        [11, 87],
        [2, 0],
      ])
      expect(a).toBeInstanceOf(KafkaJSProtocolError)
      expect(b).toBe(a)
    })

    test('3 000 partitions of 1 KiB in one send go out in packed requests, each within maxRequestSize', async () => {
      const calls = attempts()
      const { frames } = await start(
        { topics: [{ name: TOPIC, leaders: Array.from({ length: 3000 }, () => 0) }] },
        path
      )
      const messages = Array.from({ length: 3000 }, (_, partition) => ({
        value: 'x'.repeat(1024),
        partition,
      }))

      await producer.send({ topic: TOPIC, messages })

      expect(frames.length).toBeGreaterThanOrEqual(3)
      expect(Math.max(...frames)).toBeLessThanOrEqual(CAP)
      expect(frames.slice(0, -1).every((length) => length > 0.9 * CAP)).toBe(true)
      expect(calls).toHaveLength(frames.length)
    })

    test('incompressible records, long identifiers, several topics and a batchSize above maxRequestSize: no frame over the cap, no record cut', async () => {
      const topics = [
        { name: 't'.repeat(249), leaders: [0, 0, 0] },
        { name: 'second-topic', leaders: [0, 0] },
      ]
      const { frames, log } = await start(
        { topics, profile: 'default' },
        { ...path, batchSize: 4 * CAP },
        { clientId: 'c'.repeat(2000) }
      )
      const value = () => crypto.randomBytes(70000)
      const sends = []
      for (const codec of ['GZIP', 'Snappy', 'LZ4', 'ZSTD']) {
        for (const [index, { name, leaders }] of topics.entries()) {
          sends.push(
            producer.send({
              topic: name,
              compression: CompressionTypes[codec],
              messages: leaders.flatMap((_, partition) =>
                Array.from({ length: 20 + index }, () => ({ value: value(), partition }))
              ),
            })
          )
        }
      }
      await Promise.all(sends)

      expect(Math.max(...frames)).toBeLessThanOrEqual(CAP)
      expect(log.written.reduce((sum, { records: count }) => sum + count, 0)).toBe(4 * (60 + 42))
    })
  })

  test.each(PATHS)(
    'an expanding codec of its own: requests are divided until they fit, every record goes once, at most 2N - 1 size attempts, %s',
    async (_, path) => {
      const calls = attempts()
      CompressionCodecs[CompressionTypes.Snappy] = () => ({
        compress: async ({ buffer }) => Buffer.concat([buffer, Buffer.alloc(buffer.length)]),
        decompress: async (buffer) => buffer,
      })
      const { frames, log } = await start({ topics: [{ name: TOPIC, leaders: [0, 0, 0] }] }, path)
      const messages = [0, 1, 2].flatMap((partition) =>
        Array.from({ length: 8 }, (_, i) => ({
          value: `${partition}-${i}`.padEnd(40000, 'x'),
          partition,
        }))
      )

      await producer.send({ topic: TOPIC, compression: CompressionTypes.Snappy, messages })

      expect(Math.max(...frames)).toBeLessThanOrEqual(CAP)
      expect(log.written.reduce((sum, { records: count }) => sum + count, 0)).toBe(24)
      expect(calls.length).toBeLessThanOrEqual(2 * 24 - 1)
      expect(calls.length).toBeGreaterThan(frames.length)
      if (path.idempotent) {
        for (const partition of [0, 1, 2]) {
          const own = log.written.filter((w) => w.partition === partition)
          let expected = 0
          for (const { batch, records: count } of own) {
            expect(batch.baseSequence).toBe(expected)
            expected += count
          }
        }
      }
    }
  )

  test.each(PATHS)(
    'records no request can carry once compressed fail alone with the size error, and their siblings are delivered, %s',
    async (_, path) => {
      const calls = attempts()
      CompressionCodecs[CompressionTypes.Snappy] = () => ({
        compress: async ({ buffer }) =>
          buffer.includes('X') ? Buffer.alloc(2 * CAP) : Buffer.from(buffer),
        decompress: async (buffer) => buffer,
      })
      const { log } = await start({ topics: [{ name: TOPIC, leaders: [0] }] }, path)
      const messages = Array.from({ length: 6 }, (_, i) => ({
        value: (i % 3 === 1 ? 'X' : 'o').repeat(1000),
        partition: 0,
      }))

      const error = await producer
        .send({ topic: TOPIC, compression: CompressionTypes.Snappy, messages })
        .catch((failure) => failure)

      expect(error).toBeInstanceOf(KafkaJSRecordTooLargeError)
      expect(error).toMatchObject({ reason: 'maxRequestSize', scope: 'record', limit: CAP })
      expect(log.written.reduce((sum, { records: count }) => sum + count, 0)).toBe(4)
      expect(calls.length).toBeLessThanOrEqual(2 * 6 - 1)
    }
  )

  test.each(PATHS)(
    'transmitted batches that each fit alone are retried apart when their combined retry is refused, %s',
    async (_, path) => {
      let expand = false
      CompressionCodecs[CompressionTypes.Snappy] = () => ({
        compress: async ({ buffer }) =>
          expand ? Buffer.concat([buffer, buffer]) : Buffer.from(buffer),
        decompress: async (buffer) => buffer,
      })
      const { frames, log } = await start({ topics: [{ name: TOPIC, leaders: [0, 0] }] }, path)
      log.verdict = () => {
        expand = true
        return log.written.length < 2 ? 19 : 0
      }
      const messages = [0, 1].map((partition) => ({ value: 'v'.repeat(400000), partition }))

      const outcome = await producer
        .send({ topic: TOPIC, compression: CompressionTypes.Snappy, messages })
        .catch((failure) => failure)

      expect(outcome).toEqual([
        expect.objectContaining({ partition: 0 }),
        expect.objectContaining({ partition: 1 }),
      ])
      expect(Math.max(...frames)).toBeLessThanOrEqual(CAP)
      expect(log.written.map(({ errorCode }) => errorCode)).toEqual([19, 19, 0, 0])
    }
  )

  test('a transmitted batch whose retry would exceed the cap fails with the size error, no outcome and no rebase, and leaves its sequence unresolved', async () => {
    let compressions = 0
    CompressionCodecs[CompressionTypes.Snappy] = () => ({
      compress: async ({ buffer }) => {
        compressions += 1
        return compressions === 1 ? Buffer.from(buffer) : Buffer.alloc(CAP)
      },
      decompress: async (buffer) => buffer,
    })
    const { log } = await start({ topics: [{ name: TOPIC, leaders: [0] }] }, { idempotent: true })
    log.verdict = () => (log.written.length === 0 ? 19 : 0)

    const error = await producer
      .send({ topic: TOPIC, compression: CompressionTypes.Snappy, messages: records(3, 100) })
      .catch((failure) => failure)
    CompressionCodecs[CompressionTypes.Snappy] = saved[CompressionTypes.Snappy]
    await producer.send({
      topic: TOPIC,
      compression: CompressionTypes.Snappy,
      messages: records(1, 10),
    })

    expect(error).toBeInstanceOf(KafkaJSRecordTooLargeError)
    expect(error).toMatchObject({ reason: 'maxRequestSize', scope: 'send', topic: TOPIC })
    expect('outcome' in error).toBe(false)
    const [first, next] = [log.written[0].batch, log.written.at(-1).batch]
    expect([first.baseSequence, first.recordCount]).toEqual([0, 3])
    expect([next.producerEpoch, next.baseSequence]).toEqual([first.producerEpoch + 1, 0])
    expect(log.written).toHaveLength(2)
  })

  test('a split send with one delivered chunk and one never sent rejects with that chunk’s own unsent timeout', async () => {
    const { log } = await start(
      { topics: [{ name: TOPIC, leaders: [0, 1] }], nodes: 2 },
      { idempotent: true, deliveryTimeout: 600, retry: { ...RETRY, retries: 1000 } },
      { brokers: ['node0:9092'], requestTimeout: 500, connectionTimeout: 100 }
    )
    cluster.down(1)

    const error = await producer
      .send({ topic: TOPIC, messages: [...records(12, 102400), { value: 'away', partition: 1 }] })
      .catch((failure) => failure)

    expect(error).toBeInstanceOf(KafkaJSDeliveryTimeout)
    expect(error.outcome).toBe('unsent')
    expect(log.written.map(({ partition, records: count }) => [partition, count])).toEqual([
      [0, 10],
      [0, 2],
    ])
    cluster.up(1)
  })

  test('no batch grows past what one request carries, even with a batchSize above maxRequestSize', async () => {
    const calls = attempts()
    const { frames, log } = await start(
      { topics: [{ name: TOPIC, leaders: [0] }] },
      { idempotent: true, linger: 50, batchSize: 4 * CAP }
    )

    await Promise.all([
      producer.send({ topic: TOPIC, messages: records(6, 102400, 'a') }),
      producer.send({ topic: TOPIC, messages: records(6, 102400, 'b') }),
    ])

    expect(log.written.map(({ records: count }) => count)).toEqual([6, 6])
    expect(calls).toHaveLength(frames.length)
  })

  test('a transactional split send counts long topic, client and transaction identifiers through the transactional request path', async () => {
    const topic = 't'.repeat(249)
    const transactionalId = 'transaction-'.padEnd(2000, 'x')
    const { frames, log } = await start(
      { topics: [{ name: topic, leaders: [0, 0] }], profile: 'tv1' },
      { transactionalId, batchSize: 4 * CAP },
      { clientId: 'c'.repeat(2000) }
    )
    const transaction = await producer.transaction()
    const messages = [0, 1].flatMap((partition) =>
      Array.from({ length: 20 }, () => ({
        value: crypto.randomBytes(70000),
        partition,
      }))
    )
    await transaction.send({ topic, compression: CompressionTypes.GZIP, messages })
    await transaction.commit()
    expect(frames.length).toBeGreaterThan(2)
    expect(Math.max(...frames)).toBeLessThanOrEqual(CAP)
    expect(log.written.reduce((sum, { records }) => sum + records, 0)).toBe(40)
    const requests = cluster.requests().filter(({ api }) => api === 'Produce')
    expect(requests.every(({ fields }) => fields.transactionalId === transactionalId)).toBe(true)
    expect(
      requests.every(({ fields }) =>
        fields.topics.every((entry) => (entry.name ?? entry.topic) === topic)
      )
    ).toBe(true)
    expect(cluster.requests().some(({ api }) => api === 'AddPartitionsToTxn')).toBe(true)
    for (const partition of [0, 1]) {
      let sequence = 0
      for (const entry of log.written.filter((entry) => entry.partition === partition)) {
        expect(entry.batch.baseSequence).toBe(sequence)
        sequence += entry.records
      }
      expect(sequence).toBe(20)
    }
  })

  test('acks 0 returns [] for a split send', async () => {
    const { frames, log } = await start(
      { topics: [{ name: TOPIC, leaders: [0] }] },
      { idempotent: false }
    )

    const metadata = await producer.send({ topic: TOPIC, acks: 0, messages: records(25, 102400) })

    expect(metadata).toEqual([])
    await until(() => log.written.length === 3)
    expect(frames).toHaveLength(3)
    expect(Math.max(...frames)).toBeLessThanOrEqual(CAP)
    expect(log.written.map(({ records: count }) => count)).toEqual([10, 10, 5])
    expect(
      cluster
        .requests()
        .filter(({ api }) => api === 'Produce')
        .map(({ fields }) => fields.acks)
    ).toEqual([0, 0, 0])
  })
})

describe('Producer > the parts of a refused request', () => {
  const logger = { debug() {}, info() {}, warn() {}, error() {} }
  const gate = (budget) => ({
    now: Date.now(),
    producerIdValid: true,
    producerId: '1',
    producerEpoch: 0,
    isPartitionInTransaction: () => true,
    budget,
  })
  const batch = (partition, id, size = 10) => ({
    id,
    topic: TOPIC,
    partition,
    messages: [{ value: 'x'.repeat(size) }],
    acks: -1,
    timeout: 30000,
    compression: 0,
    firstSequence: null,
    producerId: null,
    producerEpoch: null,
    attempt: 0,
    settlers: [],
    backoffUntil: 0,
    createdAt: 0,
    sequenceReset: false,
  })
  const leaders = (partitions) => ({ [TOPIC]: { 0: partitions } })

  test('a part keeps its identity after a member fails, and goes out alone', () => {
    const sequencer = new PartitionSequencer({ logger })
    const [a, b, c, d] = [batch(0, 1), batch(1, 2), batch(2, 3), batch(3, 4)]
    sequencer.enqueue([a, b, c])
    expect(sequencer.drain(0, leaders([0, 1, 2]), gate())).toEqual([a, b, c])
    const [first, second] = [{}, {}]
    a.child = first
    b.child = c.child = second
    for (const refused of [a, b, c]) sequencer.restoreUnsent(refused)
    sequencer.enqueue([d])

    sequencer.fail(b)
    const drained = sequencer.drain(0, leaders([2, 3, 0]), gate())

    expect(drained).toEqual([c])
    expect(sequencer.drain(0, leaders([3, 0]), gate())).toEqual([d])
    expect(sequencer.drain(0, leaders([0]), gate())).toEqual([a])
    expect([a, c, d].map(({ firstSequence }) => firstSequence)).toEqual([0, 0, 0])
  })

  test.each(['backoff', 'transaction', 'broker movement'])(
    'retained membership survives %s without mixing a sibling or unrelated work',
    (condition) => {
      const sequencer = new PartitionSequencer({ logger })
      const [a, b, c, d] = [batch(0, 1), batch(1, 2), batch(2, 3), batch(3, 4)]
      sequencer.enqueue([a, b, c])
      sequencer.drain(0, leaders([0, 1, 2]), gate())
      a.child = {}
      b.child = c.child = {}
      const retained = c.child
      for (const member of [a, b, c]) sequencer.restoreUnsent(member)
      sequencer.enqueue([d])
      const g = gate()
      if (condition === 'backoff') b.backoffUntil = g.now + 100
      if (condition === 'transaction')
        g.isPartitionInTransaction = (_, partition) => partition !== 1
      const routing =
        condition === 'broker movement'
          ? { [TOPIC]: { 0: [0, 2, 3], 1: [1] } }
          : leaders([0, 1, 2, 3])
      expect(sequencer.drain(0, routing, g)).toEqual([a])
      expect(sequencer.drain(0, routing, g)).toEqual([c])
      expect(c.child).toBe(retained)
      if (condition === 'broker movement') expect(sequencer.drain(1, routing, gate())).toEqual([b])
      else {
        b.backoffUntil = 0
        expect(sequencer.drain(0, routing, gate())).toEqual([b])
      }
      expect(sequencer.drain(0, routing, gate())).toEqual([d])
    }
  )

  test('a detached child keeps its immutable count for sequence retirement and cannot return a range twice', () => {
    const sequencer = new PartitionSequencer({ logger })
    const child = batch(0, 1)
    child.messages = [{ value: 'a' }, { value: 'b' }]
    child.recordCount = 2
    sequencer.enqueue([child])
    sequencer.drain(0, leaders([0]), gate())
    expect(sequencer.nextSequence(TOPIC, 0)).toBe(2)
    child.messages = null
    sequencer.rebase(child)
    expect(sequencer.nextSequence(TOPIC, 0)).toBe(0)
    sequencer.fail(child)
    sequencer.fail(child)
    expect(sequencer.nextSequence(TOPIC, 0)).toBe(0)
  })

  test('a refused batch gives its sequences back and is numbered afresh at the head of its partition', () => {
    const sequencer = new PartitionSequencer({ logger })
    const [a, b] = [batch(0, 1), batch(0, 2)]
    sequencer.enqueue([a, b])
    expect(sequencer.drain(0, leaders([0]), gate())).toEqual([a])
    expect(sequencer.nextSequence(TOPIC, 0)).toBe(1)

    sequencer.restoreUnsent(a)

    expect(sequencer.nextSequence(TOPIC, 0)).toBe(0)
    expect(sequencer.entry(TOPIC, 0).queued).toEqual([a, b])
    expect([a.firstSequence, a.producerId, a.producerEpoch]).toEqual([null, null, null])
    expect(sequencer.drain(0, leaders([0]), gate())).toEqual([a])
    expect(a.firstSequence).toBe(0)
  })

  test('the drain keeps a request within the plan of maxRequestSize and always takes one batch', () => {
    const sequencer = new PartitionSequencer({ logger })
    const budget = new RequestBudget({ maxRequestSize: 1200, clientId: 'c' })
    const batches = [0, 1, 2, 3].map((partition) => batch(partition, partition, 300))
    sequencer.enqueue(batches)

    const requests = []
    for (
      let drained;
      (drained = sequencer.drain(0, leaders([0, 1, 2, 3]), gate(budget))).length > 0;
    ) {
      requests.push(drained.map(({ partition }) => partition))
      drained.forEach((one) => sequencer.complete(one, { baseOffset: '0' }))
    }
    const huge = batch(0, 9, 5000)
    sequencer.enqueue([huge])

    expect(requests).toEqual([
      [0, 1],
      [2, 3],
    ])
    expect(sequencer.drain(0, leaders([0]), gate(budget))).toEqual([huge])
  })
})

describe('Producer > the accumulator cuts a send into chunks', () => {
  // 1000 less the request's framing (33), the topic's (43), the partition's (16) and the batch
  // header (61): 847 bytes of records a batch
  const CAPACITY = 847
  const accumulatorOf = (sendPartitioned, options = {}) =>
    new Accumulator({
      linger: 0,
      batchSize: 16384,
      budget: new RequestBudget({ maxRequestSize: 1000, clientId: 'c' }),
      partition: async ({ records }) =>
        records.map(({ topic, messages }) => ({ topic, partition: 0, messages })),
      sendPartitioned,
      ...options,
    })
  const record = (i) => ({ value: `${i}`.padEnd(200, 'x') })

  test('resolves with the offset of the send’s first record whatever order the chunks answer in', async () => {
    const handed = []
    const accumulator = accumulatorOf(async ({ batches }) => {
      handed.push(...batches)
    })
    const sent = accumulator.append({
      acks: -1,
      topicMessages: [{ topic: TOPIC, messages: Array.from({ length: 12 }, (_, i) => record(i)) }],
    })
    await new Promise((resolve) => setImmediate(resolve))

    expect(handed.map(({ messages, wire }) => [messages.length, wire])).toEqual([
      [3, 675],
      [3, 675],
      [3, 675],
      [3, 675],
    ])
    let offset = 500
    for (const one of [...handed].reverse()) {
      one.settlers.forEach((settler) =>
        settler.resolve({ topicName: TOPIC, partition: 0, baseOffset: String(offset) })
      )
      offset -= 3
      one.retire()
    }
    expect(await sent).toEqual([{ topicName: TOPIC, partition: 0, baseOffset: '491' }])
  })

  test('refuses a record no request can carry before anything is charged or queued', async () => {
    const handed = []
    const accumulator = accumulatorOf(async ({ batches }) => handed.push(...batches))

    const error = await accumulator
      .append({
        acks: -1,
        topicMessages: [{ topic: TOPIC, messages: [{ value: 'x'.repeat(CAPACITY) }] }],
      })
      .catch((failure) => failure)

    expect(error).toBeInstanceOf(KafkaJSRecordTooLargeError)
    expect(error).toMatchObject({
      reason: 'maxRequestSize',
      scope: 'record',
      limit: 847,
      topic: TOPIC,
    })
    expect(error.size).toBe(847 + 25)
    expect(handed).toEqual([])
  })

  test('a batch never written is cut with its settlers: a straddling send gains a share, other settlers settle once both halves have', async () => {
    const handed = []
    const accumulator = accumulatorOf(async ({ batches }) => handed.push(...batches), {
      budget: new RequestBudget(),
      linger: 20,
    })
    const first = accumulator.append({
      acks: -1,
      topicMessages: [{ topic: TOPIC, messages: [record(0), record(1)] }],
    })
    const second = accumulator.append({
      acks: -1,
      topicMessages: [{ topic: TOPIC, messages: [record(2), record(3)] }],
    })
    await new Promise((resolve) => setTimeout(resolve, 40))
    const [batch] = handed
    const outside = { resolved: [], rejected: [] }
    const settlers = [
      ...batch.settlers,
      { resolve: (v) => outside.resolved.push(v), reject: (e) => outside.rejected.push(e) },
    ]
    const [head, tail] = Accumulator.cut({ ...batch, settlers }, 1)
    expect([head.messages.length, tail.messages.length]).toEqual([1, 3])
    head.settlers.forEach((settler) =>
      settler.resolve({ topicName: TOPIC, partition: 0, baseOffset: '10' })
    )
    expect(outside.resolved).toEqual([])
    const failure = new Error('tail failed')
    tail.settlers.forEach((settler) => settler.reject(failure))
    tail.settlers.forEach((settler) => settler.reject(new Error('again')))

    await expect(first).rejects.toBe(failure)
    await expect(second).rejects.toBe(failure)
    expect(outside).toEqual({ resolved: [], rejected: [failure] })
    head.retire()
    head.retire()
    tail.retire()
    tail.retire()
    batch.retire()
  })
})

describe('Producer > retained request children at their delivery owner', () => {
  beforeEach(() => jest.useFakeTimers({ now: 0, doNotFake: ['nextTick', 'setImmediate'] }))
  afterEach(() => {
    jest.restoreAllMocks()
    jest.useRealTimers()
  })

  test.each(
    PATHS.flatMap(([name, options]) =>
      ['expiry', 'broker movement', 'temporary ineligibility'].map((condition) => [
        name,
        condition,
        options,
      ])
    )
  )('%s keeps a surviving child separate through %s', async (_, condition, options) => {
    const createSendMessages = require('../sendMessages')
    const createEosManager = require('../eosManager')
    const createRetry = require('../../retry')
    const Idempotence = require('../idempotence')
    const { createFakeCluster } = require('./fixtures/fakeCluster')
    const { deferred, flushAll, watch, succeeded } = require('./fixtures/bufferHarness')
    const logger = { debug() {}, info() {}, warn() {}, error() {} }
    const cluster = createFakeCluster({ partitions: 4 })
    const eosManager = createEosManager({ logger, cluster, transactional: false, retry: RETRY })
    await eosManager.initProducerId()
    const lookup = deferred()
    let blocked = false
    let first = true
    let routeB = 0
    const writes = []
    const attempts = []
    const produce = (node) => async (request) => {
      const members = request.topicData.flatMap(({ partitions }) =>
        partitions.map(({ partition }) => partition)
      )
      attempts.push(members)
      if (first) {
        first = false
        if (condition === 'broker movement') routeB = 1
        if (condition === 'temporary ineligibility') routeB = -1
        blocked = true
        const refusal = request.guard(4097)
        if (refusal) throw refusal
      }
      const refusal = request.guard(64)
      if (refusal) throw refusal
      writes.push({ node, members })
      return succeeded(request)
    }
    const brokers = [0, 1].map((nodeId) => ({
      ...cluster.broker,
      nodeId,
      produce: produce(nodeId),
    }))
    cluster.findBroker.mockImplementation(async ({ nodeId }) => {
      if (blocked) await lookup.promise
      return brokers[Number(nodeId)]
    })
    cluster.findLeaderForPartitions = (_, ids) =>
      ids.reduce((leaders, id) => {
        const node = Number(id) === 1 ? routeB : 0
        if (node >= 0) (leaders[node] ??= []).push(Number(id))
        return leaders
      }, {})
    const sender = createSendMessages({
      logger,
      cluster,
      eosManager,
      retrier: createRetry(RETRY),
      retry: RETRY,
      idempotence: new Idempotence({ enabled: options.idempotent, explicit: true, logger }),
      deliveryTimeout: 1000,
      budget: new RequestBudget({ maxRequestSize: 4096 }),
      getConnectionStatus: () => 'connected',
    })
    const batches = [0, 1, 2].map((partition) => ({
      topic: TOPIC,
      partition,
      messages: [{ value: String(partition) }],
      createdAt: partition === 1 && condition === 'expiry' ? -500 : 0,
      settlers: [{ resolve: jest.fn(), reject: jest.fn() }],
    }))
    const sent = watch(
      sender.sendPartitioned({ acks: -1, timeout: 30000, compression: 0, batches })
    )
    await flushAll()
    expect(attempts[0]).toEqual([0, 1, 2])
    expect(writes).toEqual([])
    const other = watch(
      sender.sendPartitioned({
        acks: -1,
        timeout: 30000,
        compression: 0,
        batches: [
          {
            topic: TOPIC,
            partition: 3,
            messages: [{ value: 'unrelated' }],
            createdAt: 0,
            settlers: [],
          },
        ],
      })
    )
    if (condition === 'expiry') {
      await jest.advanceTimersByTimeAsync(500)
      expect(batches[1].settlers[0].reject).toHaveBeenCalledTimes(1)
      expect(batches[1].settlers[0].reject.mock.calls[0][0]).toMatchObject({
        name: 'KafkaJSDeliveryTimeout',
        outcome: 'unsent',
      })
    }
    if (condition === 'broker movement') routeB = 1
    if (condition === 'temporary ineligibility') routeB = -1
    blocked = false
    lookup.resolve()
    await flushAll()
    expect(writes.find(({ members }) => members.includes(2)).members).toEqual([2])
    expect(writes.find(({ members }) => members.includes(0)).members).toEqual([0])
    if (condition === 'temporary ineligibility') {
      expect(writes.some(({ members }) => members.includes(1))).toBe(false)
      routeB = 0
      await jest.advanceTimersByTimeAsync(50)
    }
    await flushAll()
    expect(other.state).toBe('fulfilled')
    expect(sent.state).toBe(condition === 'expiry' ? 'rejected' : 'fulfilled')
    expect(writes.filter(({ members }) => members.includes(2))).toHaveLength(1)
    if (condition === 'expiry')
      expect(writes.some(({ members }) => members.includes(1))).toBe(false)
    else
      expect(writes.find(({ members }) => members.includes(1))).toEqual({
        node: condition === 'broker movement' ? 1 : 0,
        members: [1],
      })
    expect(attempts.length).toBeLessThanOrEqual(7)
  })

  test('the first half of a batch cut on the idempotent path expires at the delivery deadline while no broker answers', async () => {
    const createSendMessages = require('../sendMessages')
    const createEosManager = require('../eosManager')
    const createRetry = require('../../retry')
    const Idempotence = require('../idempotence')
    const { createFakeCluster } = require('./fixtures/fakeCluster')
    const { deferred, flushAll, watch } = require('./fixtures/bufferHarness')
    const logger = { debug() {}, info() {}, warn() {}, error() {} }
    const cluster = createFakeCluster({ partitions: 1 })
    const eosManager = createEosManager({ logger, cluster, transactional: false, retry: RETRY })
    await eosManager.initProducerId()
    const lookup = deferred()
    let blocked = false
    const broker = {
      ...cluster.broker,
      nodeId: 0,
      produce: async (request) => {
        blocked = true
        const refusal = request.guard(4097)
        if (refusal) throw refusal
        throw new Error('written')
      },
    }
    cluster.findBroker.mockImplementation(async () => {
      if (blocked) await lookup.promise
      return broker
    })
    cluster.findLeaderForPartitions = () => ({ 0: [0] })
    const sender = createSendMessages({
      logger,
      cluster,
      eosManager,
      retrier: createRetry(RETRY),
      retry: RETRY,
      idempotence: new Idempotence({ enabled: true, explicit: true, logger }),
      deliveryTimeout: 1000,
      budget: new RequestBudget({ maxRequestSize: 4096 }),
      getConnectionStatus: () => 'connected',
    })
    const settler = { resolve: jest.fn(), reject: jest.fn() }
    const sent = watch(
      sender.sendPartitioned({
        acks: -1,
        timeout: 30000,
        compression: 0,
        batches: [
          {
            topic: TOPIC,
            partition: 0,
            messages: [{ value: 'a' }, { value: 'b' }],
            createdAt: 0,
            settlers: [settler],
          },
        ],
      })
    )
    await flushAll()
    await jest.advanceTimersByTimeAsync(1100)
    await flushAll()

    expect(settler.reject).toHaveBeenCalledTimes(1)
    expect(settler.reject.mock.calls[0][0]).toMatchObject({
      name: 'KafkaJSDeliveryTimeout',
      outcome: 'unsent',
    })
    expect(sent.state).toBe('rejected')
    lookup.resolve()
    await flushAll()
  })

  test('a written part ends at that write, so its members can share a request again', async () => {
    const createSendMessages = require('../sendMessages')
    const createEosManager = require('../eosManager')
    const createRetry = require('../../retry')
    const Idempotence = require('../idempotence')
    const { createErrorFromCode } = require('../../protocol/error')
    const { createFakeCluster } = require('./fixtures/fakeCluster')
    const { flushAll, watch, succeeded } = require('./fixtures/bufferHarness')
    const logger = { debug() {}, info() {}, warn() {}, error() {} }
    const cluster = createFakeCluster({ partitions: 2 })
    const eosManager = createEosManager({ logger, cluster, transactional: false, retry: RETRY })
    await eosManager.initProducerId()
    const attempts = []
    const broker = {
      ...cluster.broker,
      nodeId: 0,
      produce: async (request) => {
        attempts.push(
          request.topicData.flatMap(({ partitions }) =>
            partitions.map(({ partition }) => partition)
          )
        )
        const refusal = request.guard(attempts.length === 1 ? 4097 : 64)
        if (refusal) throw refusal
        if (attempts.length <= 3) throw createErrorFromCode(19)
        return succeeded(request)
      },
    }
    cluster.findBroker.mockImplementation(async () => broker)
    cluster.findLeaderForPartitions = (_, ids) => ({ 0: ids.map(Number) })
    const sender = createSendMessages({
      logger,
      cluster,
      eosManager,
      retrier: createRetry(RETRY),
      retry: RETRY,
      idempotence: new Idempotence({ enabled: true, explicit: true, logger }),
      deliveryTimeout: 1000,
      budget: new RequestBudget({ maxRequestSize: 4096 }),
      getConnectionStatus: () => 'connected',
    })
    const sent = watch(
      sender.sendPartitioned({
        acks: -1,
        timeout: 30000,
        compression: 0,
        batches: [0, 1].map((partition) => ({
          topic: TOPIC,
          partition,
          messages: [{ value: String(partition) }],
          createdAt: 0,
          settlers: [],
        })),
      })
    )
    await flushAll()
    await jest.advanceTimersByTimeAsync(50)
    await flushAll()

    expect(attempts).toEqual([[0, 1], [0], [1], [0, 1]])
    expect(sent.state).toBe('fulfilled')
  })
})
