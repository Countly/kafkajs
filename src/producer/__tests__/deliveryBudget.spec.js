const v8 = require('v8')
const vm = require('vm')
const { Kafka } = require('../../../index')
const Accumulator = require('../state/accumulator')
const createProducer = require('../index')
const { createFakeCluster } = require('./fixtures/fakeCluster')
const { createErrorFromCode } = require('../../protocol/error')
const { createLogger, LEVELS } = require('../../loggers')
const { KafkaJSDeliveryTimeout, KafkaJSTransactionAbortableError } = require('../../errors')
const { Schedule } = require('../../../testHelpers/schedule')
const { createScriptedCluster, producerScript } = require('../../../testHelpers/scriptedCluster')
const { TOPIC } = require('../../consumer/__tests__/fixtures/fetchHarness')

const MESSAGES = [{ key: 'key-0', value: 'value-0' }]

jest.setTimeout(10000)

const silentLogger = () => createLogger({ level: LEVELS.NOTHING, logCreator: () => () => {} })

const releases = []

/**
 * Holds a fixture answer until finally cleanup releases it.
 * @returns {Promise<object[]>} empty partition list at release
 */
const held = () => deferred().promise

/** A promise and the functions that settle it. */
const deferred = () => {
  const handle = {}
  handle.promise = new Promise((resolve, reject) => {
    handle.resolve = resolve
    handle.reject = reject
  })
  releases.push(() => handle.resolve([]))
  return handle
}

/** The result of a promise, read without waiting for it: `pending` until it settles. */
const watch = (promise) => {
  const watched = { state: 'pending', value: undefined }
  promise.then(
    (value) => Object.assign(watched, { state: 'fulfilled', value }),
    (value) => Object.assign(watched, { state: 'rejected', value })
  )
  return watched
}

/** Collects the garbage a spec made, as many passes as the weak references need to clear. */
const collect = async () => {
  v8.setFlagsFromString('--expose-gc')
  const gc = vm.runInNewContext('gc')
  for (let pass = 0; pass < 5; pass++) {
    await new Promise((resolve) => setImmediate(resolve))
    gc()
  }
}

describe('Accumulator > a send that has not reached a batch', () => {
  const resources = []
  beforeEach(() => {
    jest.useFakeTimers({ now: 0, doNotFake: ['nextTick', 'setImmediate'] })
  })

  afterEach(async () => {
    try {
      for (const { release } of resources) release()
      await jest.advanceTimersByTimeAsync(10000)
    } finally {
      try {
        for (const release of releases.splice(0)) release()
        await jest.advanceTimersByTimeAsync(10000)
        const closing = Promise.all(
          resources.splice(0).map(({ accumulator }) => accumulator.close())
        )
        await Promise.race([closing, new Promise((resolve) => setImmediate(resolve))])
      } finally {
        jest.useRealTimers()
      }
    }
  })

  /** An accumulator whose partition assignment is `assign`, and the batches it hands on. */
  const create = ({ assign, deadline = 1000, expired, linger = 0 } = {}) => {
    const handedOn = []
    const assigned = deferred()
    const partition = jest.fn(assign ?? (() => assigned.promise))
    const accumulator = new Accumulator({
      linger,
      batchSize: 16384,
      deadline,
      expired,
      partition,
      sendPartitioned: jest.fn(async (request) => {
        handedOn.push(request)
        request.batches.forEach(({ settlers }) =>
          settlers.forEach((settler) =>
            settler.resolve({ topicName: TOPIC, partition: 0, baseOffset: '0' })
          )
        )
      }),
    })
    resources.push({ accumulator, release: () => assigned.resolve([]) })
    return { accumulator, handedOn, partition }
  }

  const send = (accumulator, extra = {}) =>
    accumulator.append({
      acks: -1,
      topicMessages: [{ topic: TOPIC, messages: MESSAGES }],
      ...extra,
    })

  test('rejects at its deadline with KafkaJSDeliveryTimeout, unsent, while its partitions are still awaited', async () => {
    const { accumulator, handedOn } = create()
    const sent = watch(send(accumulator))

    await jest.advanceTimersByTimeAsync(999)
    expect(sent.state).toBe('pending')
    await jest.advanceTimersByTimeAsync(1)

    expect(sent.state).toBe('rejected')
    expect(sent.value).toBeInstanceOf(KafkaJSDeliveryTimeout)
    expect(sent.value.outcome).toBe('unsent')
    expect(handedOn).toEqual([])
  })

  test('counts its deadline from the call that began it, not from the accumulator', async () => {
    const { accumulator } = create()
    const sent = watch(send(accumulator, { startedAt: -400 }))

    await jest.advanceTimersByTimeAsync(599)
    expect(sent.state).toBe('pending')
    await jest.advanceTimersByTimeAsync(1)

    expect(sent.state).toBe('rejected')
  })

  test('with no time left at entry rejects at once, asks for no partitions and arms no timer', async () => {
    const { accumulator, partition } = create()

    const sent = watch(send(accumulator, { startedAt: -1000 }))
    await new Promise((resolve) => setImmediate(resolve))

    expect(sent.state).toBe('rejected')
    expect(sent.value).toBeInstanceOf(KafkaJSDeliveryTimeout)
    expect(sent.value.outcome).toBe('unsent')
    expect(partition).not.toHaveBeenCalled()
    expect(jest.getTimerCount()).toBe(0)
  })

  test('is joined to a batch when its partitions come in time, and its deadline timer goes', async () => {
    const assigned = deferred()
    const { accumulator, handedOn } = create({ assign: () => assigned.promise })
    const sent = watch(send(accumulator))
    expect(jest.getTimerCount()).toBe(1)

    assigned.resolve([{ topic: TOPIC, partition: 0, messages: MESSAGES }])
    await jest.advanceTimersByTimeAsync(0)

    expect(sent.state).toBe('fulfilled')
    expect(handedOn).toHaveLength(1)
    expect(jest.getTimerCount()).toBe(0)
  })

  test('is not joined when its partitions come late, whatever it was waiting for', async () => {
    const assigned = deferred()
    const { accumulator, handedOn } = create({ assign: () => assigned.promise })
    const sent = watch(send(accumulator))

    await jest.advanceTimersByTimeAsync(1000)
    assigned.resolve([{ topic: TOPIC, partition: 0, messages: MESSAGES }])
    await jest.advanceTimersByTimeAsync(10)

    expect(sent.state).toBe('rejected')
    expect(handedOn).toEqual([])
  })

  test('does not turn a late failure into a second outcome or an unhandled rejection', async () => {
    const assigned = deferred()
    const { accumulator } = create({ assign: () => assigned.promise })
    const unhandled = jest.fn()
    process.on('unhandledRejection', unhandled)
    try {
      const sent = watch(send(accumulator))

      await jest.advanceTimersByTimeAsync(1000)
      assigned.reject(new Error('late failure'))
      await jest.advanceTimersByTimeAsync(10)

      expect(sent.value).toBeInstanceOf(KafkaJSDeliveryTimeout)
      expect(unhandled).not.toHaveBeenCalled()
    } finally {
      process.off('unhandledRejection', unhandled)
    }
  })

  test('keeps one timer for any number of sends, and none once the last has settled', async () => {
    const { accumulator } = create()
    const sends = Array.from({ length: 50 }, () => watch(send(accumulator)))

    expect(jest.getTimerCount()).toBe(1)
    await jest.advanceTimersByTimeAsync(1000)

    expect(sends.every(({ state }) => state === 'rejected')).toBe(true)
    expect(jest.getTimerCount()).toBe(0)
  })

  test('settles each send at its own deadline and re-arms for the next', async () => {
    const { accumulator } = create({ deadline: 1000 })
    const first = watch(send(accumulator))
    await jest.advanceTimersByTimeAsync(300)
    const second = watch(send(accumulator))

    await jest.advanceTimersByTimeAsync(700)
    expect([first.state, second.state]).toEqual(['rejected', 'pending'])
    expect(jest.getTimerCount()).toBe(1)
    await jest.advanceTimersByTimeAsync(300)

    expect(second.state).toBe('rejected')
    expect(jest.getTimerCount()).toBe(0)
  })

  test('re-arms for an earlier deadline, and settles the earlier send first, when the clock stepped back', async () => {
    const { accumulator } = create({ deadline: 1000 })
    const later = watch(send(accumulator))
    const earlier = watch(send(accumulator, { startedAt: -500 }))

    await jest.advanceTimersByTimeAsync(500)

    expect([earlier.state, later.state]).toEqual(['rejected', 'pending'])
    await jest.advanceTimersByTimeAsync(500)
    expect(later.state).toBe('rejected')
  })

  test('is counted by close(), which does not wait for it past its deadline', async () => {
    const { accumulator } = create()
    send(accumulator).catch(() => {})
    const closed = watch(accumulator.close())

    await jest.advanceTimersByTimeAsync(999)
    expect(closed.state).toBe('pending')
    await jest.advanceTimersByTimeAsync(1)

    expect(closed.state).toBe('fulfilled')
  })

  test('is counted by close() until its records have joined a batch, which close() then waits for', async () => {
    const assigned = deferred()
    const { accumulator, handedOn } = create({ assign: () => assigned.promise, linger: 5000 })
    const sent = watch(send(accumulator))
    const closed = watch(accumulator.close())

    assigned.resolve([{ topic: TOPIC, partition: 0, messages: MESSAGES }])
    await jest.advanceTimersByTimeAsync(0)
    expect(closed.state).toBe('pending')
    await jest.advanceTimersByTimeAsync(5000)

    expect(handedOn).toHaveLength(1)
    expect(closed.state).toBe('fulfilled')
    expect(sent.state).toBe('fulfilled')
  })

  test('a late result of an expired send does not end the append of the send that follows', async () => {
    const answers = [deferred(), deferred()]
    const { accumulator } = create({ assign: () => answers.shift().promise, deadline: 100 })
    const first = answers[0]
    send(accumulator).catch(() => {})
    await jest.advanceTimersByTimeAsync(50)
    send(accumulator).catch(() => {})
    await jest.advanceTimersByTimeAsync(50)

    first.resolve([{ topic: TOPIC, partition: 0, messages: MESSAGES }])
    const closed = watch(accumulator.close())
    await jest.advanceTimersByTimeAsync(10)
    expect(closed.state).toBe('pending')
    await jest.advanceTimersByTimeAsync(40)

    expect(closed.state).toBe('fulfilled')
  })

  test('a partition callback that throws rejects the send with that error and ends its append', async () => {
    const failure = new Error('partition failure')
    const { accumulator } = create({
      assign: () => {
        throw failure
      },
    })

    await expect(send(accumulator)).rejects.toBe(failure)
    await expect(accumulator.close()).resolves.toBeUndefined()
  })

  test('records that cannot be joined reject the send and end its append', async () => {
    const { accumulator } = create({ assign: async () => undefined })

    await expect(send(accumulator)).rejects.toBeInstanceOf(TypeError)
    await expect(accumulator.close()).resolves.toBeUndefined()
  })

  test('is pinned by the wait for its partitions until that wait ends, not by its expiry', async () => {
    const assigned = deferred()
    let seen
    const { accumulator } = create({
      assign: (pending) => {
        seen = pending
        return assigned.promise
      },
    })
    send(accumulator).catch(() => {})
    expect(seen.pins).toBe(1)

    await jest.advanceTimersByTimeAsync(1000)
    expect(seen.pins).toBe(1)
    expect(seen.records).toBeNull()

    assigned.resolve([])
    await jest.advanceTimersByTimeAsync(0)
    expect(seen.pins).toBe(0)
  })

  test('hands the transaction the error it rejects with, so that it can be armed', async () => {
    const armed = jest.fn((error) => error)
    const { accumulator } = create({ expired: armed })

    send(accumulator).catch(() => {})
    await jest.advanceTimersByTimeAsync(1000)

    expect(armed).toHaveBeenCalledTimes(1)
    expect(armed.mock.calls[0][0]).toBeInstanceOf(KafkaJSDeliveryTimeout)
  })

  test('gives a batch no longer linger than its deadline leaves', async () => {
    const { accumulator, handedOn } = create({
      assign: async (pending) =>
        pending.records.map(({ topic, messages }) => ({ topic, partition: 0, messages })),
      deadline: 300,
      linger: 5000,
    })
    send(accumulator).catch(() => {})
    await jest.advanceTimersByTimeAsync(0)

    await jest.advanceTimersByTimeAsync(299)
    expect(handedOn).toHaveLength(0)
    await jest.advanceTimersByTimeAsync(1)

    expect(handedOn).toHaveLength(1)
  })

  test('keeps none of its records alive once it has expired, however long its wait goes on', async () => {
    const { accumulator } = create({ assign: held })
    const refs = []
    const sends = []
    const start = (count) => {
      for (let i = 0; i < count; i++) {
        const value = Buffer.alloc(1048576, i)
        refs.push(new WeakRef(value))
        sends.push(
          accumulator
            .append({ acks: -1, topicMessages: [{ topic: TOPIC, messages: [{ value }] }] })
            .catch((error) => error)
        )
      }
    }

    start(20)
    const settled = watch(Promise.all(sends))
    await jest.advanceTimersByTimeAsync(1000)
    expect(settled.state).toBe('fulfilled')
    expect(settled.value.every((error) => error instanceof KafkaJSDeliveryTimeout)).toBe(true)
    await collect()

    expect(refs.filter((ref) => ref.deref() !== undefined)).toHaveLength(0)
  })
})

describe('Producer > the delivery deadline through the public call', () => {
  let producer, cluster, pending
  const gates = []

  /** Kafka `requestTimeout: 1000` and `enforceRequestTimeout: false`, producer `linger: 0`, `deliveryTimeout: 1000` */
  const settings = {
    kafka: { requestTimeout: 1000, enforceRequestTimeout: false },
    producer: { linger: 0, deliveryTimeout: 1000 },
  }

  const until = async (predicate) => {
    for (let i = 0; i < 200 && !predicate(); i += 1) {
      await jest.advanceTimersByTimeAsync(1)
      await new Promise((resolve) => setImmediate(resolve))
    }
    expect(predicate()).toBe(true)
  }

  const produceRequests = () => cluster.requests().filter(({ api }) => api === 'Produce')

  const start = (schedule, { kafka = {}, producerOptions = {} } = {}) => {
    cluster = producerScript(
      createScriptedCluster({ nodes: [{ id: 0, host: 'node0', port: 9092 }], schedule })
    )
    producer = new Kafka({ ...cluster.kafkaOptions(), ...settings.kafka, ...kafka }).producer({
      idempotent: true,
      ...settings.producer,
      ...producerOptions,
    })
  }

  beforeEach(() => {
    jest.useFakeTimers({ now: 0, doNotFake: ['nextTick', 'setImmediate'] })
  })

  /** Lets the work a released gate set going finish, so that none runs after the test has ended. */
  const settleCluster = async () => {
    for (let i = 0; i < 50; i += 1) {
      await jest.advanceTimersByTimeAsync(20)
      await new Promise((resolve) => setImmediate(resolve))
    }
  }

  afterEach(async () => {
    try {
      for (const gate of gates.splice(0)) if (gate.settle) gate.release()
      await jest.advanceTimersByTimeAsync(10000)
      await settleCluster()
    } finally {
      try {
        await pending?.catch(() => {})
        if (producer) await producer.disconnect()
        if (cluster) cluster.assertClean()
      } finally {
        producer = cluster = pending = undefined
        jest.useRealTimers()
      }
    }
  })

  test('a producer id request held past the deadline settles the send at the deadline, unsent, and the id still arrives', async () => {
    const schedule = new Schedule()
    const gate = schedule.hold({ api: 'InitProducerId' })
    gates.push(gate)
    start(schedule)
    const connecting = producer.connect()
    await gate.arrived()

    try {
      pending = producer.send({ topic: TOPIC, messages: MESSAGES })
      const sent = watch(pending)
      await jest.advanceTimersByTimeAsync(999)
      expect(sent.state).toBe('pending')
      await jest.advanceTimersByTimeAsync(1)

      expect(sent.state).toBe('rejected')
      expect(sent.value).toBeInstanceOf(KafkaJSDeliveryTimeout)
      expect(sent.value.outcome).toBe('unsent')
      expect(gate.settle).not.toBeNull()

      gate.release()
      await connecting
      expect(produceRequests()).toHaveLength(0)

      pending = producer.send({ topic: TOPIC, messages: MESSAGES })
      await until(() => produceRequests().length === 1)
      await pending
    } finally {
      if (gate.settle) gate.release()
      await connecting
    }
  })

  test('keeps none of the records of sends that expired behind a held producer id request', async () => {
    const schedule = new Schedule()
    const gate = schedule.hold({ api: 'InitProducerId' })
    gates.push(gate)
    start(schedule)
    const connecting = producer.connect()
    await gate.arrived()

    const refs = []
    const sendAll = (count) => {
      const sends = []
      for (let i = 0; i < count; i += 1) {
        const value = Buffer.alloc(1048576, i)
        refs.push(new WeakRef(value))
        sends.push(producer.send({ topic: TOPIC, messages: [{ value }] }).catch((error) => error))
      }
      return Promise.all(sends)
    }

    try {
      const settled = watch(sendAll(20))
      await jest.advanceTimersByTimeAsync(1000)
      expect(settled.state).toBe('fulfilled')
      const outcomes = settled.value
      await collect()

      expect(outcomes.every((error) => error instanceof KafkaJSDeliveryTimeout)).toBe(true)
      expect(gate.settle).not.toBeNull()
      expect(refs.filter((ref) => ref.deref() !== undefined)).toHaveLength(0)
    } finally {
      gate.release()
      await connecting
    }
  })

  test('an initial metadata request held past the deadline settles the send at the deadline, unsent', async () => {
    const schedule = new Schedule()
    start(schedule)
    await producer.connect()
    const gate = schedule.hold({ api: 'Metadata' })
    gates.push(gate)

    pending = producer.send({ topic: TOPIC, messages: MESSAGES })
    const sent = watch(pending)
    await gate.arrived()
    await jest.advanceTimersByTimeAsync(1000)

    expect(sent.state).toBe('rejected')
    expect(sent.value.outcome).toBe('unsent')
    expect(gate.settle).not.toBeNull()
    gate.release()
    await jest.advanceTimersByTimeAsync(10)
    expect(produceRequests()).toHaveLength(0)
  })

  test('a metadata refresh held after a failed first answer settles the send at the deadline, unsent', async () => {
    const schedule = new Schedule()
    start(schedule)
    await producer.connect()
    schedule.once({ api: 'Metadata' }, createErrorFromCode(5))
    const gate = schedule.hold({ api: 'Metadata' })
    gates.push(gate)

    pending = producer.send({ topic: TOPIC, messages: MESSAGES })
    const sent = watch(pending)
    await until(() => gate.settle !== null)
    await jest.advanceTimersByTimeAsync(1000)

    expect(sent.state).toBe('rejected')
    expect(sent.value.outcome).toBe('unsent')
    gate.release()
    await jest.advanceTimersByTimeAsync(10)
    expect(produceRequests()).toHaveLength(0)
  })

  test('a batch kept waiting for linger by a deadline shorter than it reaches the send path at the deadline', async () => {
    const schedule = new Schedule()
    start(schedule, {
      producerOptions: { linger: 5000, deliveryTimeout: undefined, retry: { maxTotalTime: 1000 } },
    })
    await producer.connect()

    pending = producer.send({ topic: TOPIC, messages: MESSAGES })
    const sent = watch(pending)
    await jest.advanceTimersByTimeAsync(999)
    expect(sent.state).toBe('pending')
    await jest.advanceTimersByTimeAsync(1)
    await jest.advanceTimersByTimeAsync(10)

    expect(sent.state).toBe('rejected')
    expect(sent.value).toBeInstanceOf(KafkaJSDeliveryTimeout)
    expect(produceRequests()).toHaveLength(0)
  })

  test('with the request timeout enforced at the same moment, the deadline settles the send and the connection failure that follows is fenced', async () => {
    const schedule = new Schedule()
    const gate = schedule.hold({ api: 'Produce' })
    gates.push(gate)
    start(schedule, { kafka: { requestTimeout: 1000, enforceRequestTimeout: true } })
    await producer.connect()

    pending = producer.send({ topic: TOPIC, messages: MESSAGES })
    const sent = watch(pending)
    await until(() => gate.settle !== null)
    await jest.advanceTimersByTimeAsync(1000)
    await jest.advanceTimersByTimeAsync(50)

    expect(sent.state).toBe('rejected')
    expect(sent.value).toBeInstanceOf(KafkaJSDeliveryTimeout)
    if (gate.settle) gate.release()
    await jest.advanceTimersByTimeAsync(2000)
    expect(produceRequests()).toHaveLength(1)
  })
})

describe('Producer > the delivery deadline where the cluster itself is held', () => {
  beforeEach(() => {
    jest.useFakeTimers({ now: 0, doNotFake: ['nextTick', 'setImmediate'] })
  })

  const producers = []
  afterEach(async () => {
    try {
      for (const release of releases.splice(0)) release()
      const closing = Promise.all(producers.splice(0).map((producer) => producer.disconnect()))
      await jest.advanceTimersByTimeAsync(10000)
      await Promise.race([closing, new Promise((resolve) => setImmediate(resolve))])
    } finally {
      jest.useRealTimers()
    }
  })

  /**
   * Tracks a producer over a held fake cluster for finally cleanup.
   * @param {object} cluster fake cluster
   * @param {object} [options] producer overrides
   * @returns {object} producer under test
   */
  const producerOver = (cluster, options = {}) => {
    const producer = createProducer({
      cluster,
      logger: silentLogger(),
      linger: 0,
      deliveryTimeout: 1000,
      ...options,
    })
    producers.push(producer)
    return producer
  }

  test.each([
    ['the broker lookup', (cluster) => (cluster.findBroker = jest.fn(held))],
    [
      'the metadata refresh after a leader moved',
      (cluster) => {
        cluster.broker.produce.mockRejectedValue(createErrorFromCode(6))
        cluster.refreshMetadata = jest.fn(held)
      },
    ],
  ])('%s, held, does not hold the send past its deadline', async (_, hold) => {
    const cluster = createFakeCluster()
    hold(cluster)
    const producer = producerOver(cluster)
    await producer.connect()

    const sent = watch(producer.send({ topic: TOPIC, messages: MESSAGES }))
    await jest.advanceTimersByTimeAsync(999)
    expect(sent.state).toBe('pending')
    await jest.advanceTimersByTimeAsync(1)

    expect(sent.state).toBe('rejected')
    expect(sent.value).toBeInstanceOf(KafkaJSDeliveryTimeout)
  })

  test('a transaction whose send expires before it reaches a batch can only be aborted', async () => {
    const cluster = createFakeCluster()
    cluster.refreshMetadataIfNecessary = jest.fn(held)
    const producer = producerOver(cluster, { transactionalId: 'tx-a', idempotent: true })
    await producer.connect()
    const transaction = await producer.transaction()

    const sent = watch(transaction.send({ topic: TOPIC, messages: MESSAGES }))
    await jest.advanceTimersByTimeAsync(1000)

    expect(sent.value).toBeInstanceOf(KafkaJSDeliveryTimeout)
    await expect(transaction.commit()).rejects.toBeInstanceOf(KafkaJSTransactionAbortableError)
  })

  test('a late failure of an expired transaction send does not arm the transaction that follows', async () => {
    const cluster = createFakeCluster()
    cluster.broker.negotiatedVersion = () => 5
    cluster.broker.initProducerId.mockImplementation(async ({ producerEpoch = -1 }) => ({
      producerId: '1000',
      producerEpoch: producerEpoch + 1,
    }))
    const metadata = deferred()
    const producer = producerOver(cluster, { transactionalId: 'tx-a', idempotent: true })
    await producer.connect()
    const first = await producer.transaction()
    cluster.refreshMetadataIfNecessary.mockImplementationOnce(() => metadata.promise)
    const expired = watch(first.send({ topic: TOPIC, messages: MESSAGES }))
    await jest.advanceTimersByTimeAsync(1000)
    expect(expired.value).toBeInstanceOf(KafkaJSDeliveryTimeout)
    await first.abort()
    const second = await producer.transaction()

    metadata.reject(createErrorFromCode(29))
    await jest.advanceTimersByTimeAsync(10)
    const sending = second.send({ topic: TOPIC, messages: MESSAGES })
    watch(sending)
    await jest.advanceTimersByTimeAsync(10)

    await expect(sending).resolves.toBeDefined()
    await second.commit()
  })

  test('a late failure of an expired send refreshes no metadata and is not retried', async () => {
    const cluster = createFakeCluster()
    const metadata = deferred()
    cluster.refreshMetadataIfNecessary.mockImplementationOnce(() => metadata.promise)
    const producer = producerOver(cluster)
    await producer.connect()
    const sent = watch(producer.send({ topic: TOPIC, messages: MESSAGES }))
    await jest.advanceTimersByTimeAsync(1000)
    expect(sent.value).toBeInstanceOf(KafkaJSDeliveryTimeout)
    cluster.refreshMetadata.mockClear()

    metadata.reject(createErrorFromCode(7))
    await jest.advanceTimersByTimeAsync(10)

    expect(cluster.refreshMetadata).not.toHaveBeenCalled()
    expect(jest.getTimerCount()).toBe(0)
  })

  test('an expired target-topic wait does not start another metadata refresh', async () => {
    const cluster = createFakeCluster()
    const metadata = deferred()
    cluster.addMultipleTargetTopics.mockImplementationOnce(() => metadata.promise)
    const producer = producerOver(cluster)
    await producer.connect()
    const sent = watch(producer.send({ topic: TOPIC, messages: MESSAGES }))
    await jest.advanceTimersByTimeAsync(1000)
    expect(sent.value).toBeInstanceOf(KafkaJSDeliveryTimeout)
    cluster.refreshMetadataIfNecessary.mockClear()
    metadata.resolve([])
    await jest.advanceTimersByTimeAsync(1)
    expect(cluster.refreshMetadataIfNecessary).not.toHaveBeenCalled()
  })
})
