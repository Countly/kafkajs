const { Kafka } = require('../../../index')
const { KafkaJSDeliveryTimeout } = require('../../errors')
const { Schedule } = require('../../../testHelpers/schedule')
const { createScriptedCluster, producerScript } = require('../../../testHelpers/scriptedCluster')
const { WireOrder } = require('../../../testHelpers/wireOrder')
const { TOPIC } = require('../../consumer/__tests__/fixtures/fetchHarness')

describe('Idempotent producer delivery expiry', () => {
  let producer, cluster, gate, pending, observation

  beforeEach(async () => {
    jest.useFakeTimers({ doNotFake: ['nextTick', 'setImmediate'] })
    const schedule = new Schedule()
    gate = schedule.hold({ api: 'Produce' })
    cluster = producerScript(
      createScriptedCluster({
        nodes: [{ id: 0, host: 'node0', port: 9092 }],
        schedule,
      })
    )
    producer = new Kafka({
      ...cluster.kafkaOptions(),
      requestTimeout: 1000,
      enforceRequestTimeout: false,
    }).producer({ idempotent: true, linger: 0, deliveryTimeout: 1000 })
    await producer.connect()
    let state = 'pending'
    let error
    pending = producer
      .send({ topic: TOPIC, messages: [{ value: 'deadline', timestamp: 1000 }] })
      .then(
        () => {
          state = 'fulfilled'
        },
        (failure) => {
          if (!(failure instanceof KafkaJSDeliveryTimeout)) throw failure
          error = failure
          state = 'rejected'
        }
      )
    for (let i = 0; i < 100 && !gate.call; i += 1) {
      await jest.advanceTimersByTimeAsync(1)
      await new Promise((resolve) => setImmediate(resolve))
    }
    expect(gate.call).not.toBeNull()
    await gate.arrived()
    cluster.mark('answer held')
    await jest.advanceTimersByTimeAsync(1000)
    const atDeadline = state
    await jest.advanceTimersByTimeAsync(4000)
    observation = { atDeadline, atFiveSeconds: state, error: error?.name, outcome: error?.outcome }
    gate.release()
    await pending
    for (
      let i = 0;
      i < 100 &&
      cluster.timeline().filter(({ api, kind }) => api === 'Produce' && kind === 'response')
        .length === 0;
      i += 1
    ) {
      await jest.advanceTimersByTimeAsync(1)
      await new Promise((resolve) => setImmediate(resolve))
    }
    observation.afterLateSuccess = state
    await new Promise((resolve) => setImmediate(resolve))
    expect(cluster.requests().filter(({ api }) => api === 'Produce')).toHaveLength(1)
    WireOrder.of(cluster).assertSequencesInOrder()
  })

  afterEach(async () => {
    if (gate?.settle) gate.release()
    if (pending) await pending
    if (producer) await producer.disconnect()
    if (cluster) cluster.assertClean()
    jest.useRealTimers()
  })

  it('settles an on-wire send at its delivery deadline and ignores a late success', () => {
    expect(observation).toEqual({
      atDeadline: 'rejected',
      atFiveSeconds: 'rejected',
      error: 'KafkaJSDeliveryTimeout',
      outcome: 'unknown',
      afterLateSuccess: 'rejected',
    })
  })

  it('runs a complete held-answer scenario through the public producer', () => {
    const produce = cluster.timeline().filter(({ api }) => api === 'Produce')
    expect(produce.map(({ kind }) => kind)).toEqual(['request', 'response'])
  })
})

describe('Produce write-boundary expiry', () => {
  const { CompressionCodecs, CompressionTypes } = require('../../../index')
  const Connection = require('../../network/connection')
  const { readRequestHeader } = require('testHelpers')
  let producer, cluster, releaseEncoding, savedCodec, pending
  let gates = []

  /** A bounded pump; its assertion is setup, outside expected-failure inversion. */
  const until = async (predicate) => {
    for (let i = 0; i < 200 && !predicate(); i += 1) {
      await jest.advanceTimersByTimeAsync(1)
      await new Promise((resolve) => setImmediate(resolve))
    }
    expect(predicate()).toBe(true)
  }

  const start = async (idempotent, schedule, partitions = 1) => {
    cluster = producerScript(
      createScriptedCluster({
        nodes: [{ id: 0, host: 'node0', port: 9092 }],
        profile: 'tv1',
        schedule,
      }),
      { partitions }
    )
    producer = new Kafka({
      ...cluster.kafkaOptions(),
      requestTimeout: 1000,
      enforceRequestTimeout: false,
    }).producer({
      idempotent,
      linger: 0,
      deliveryTimeout: 1000,
      retry: { retries: 10, initialRetryTime: 10, maxRetryTime: 10, factor: 0 },
    })
    await producer.connect()
  }

  beforeEach(() => {
    jest.useFakeTimers({ now: 0, doNotFake: ['nextTick', 'setImmediate'] })
    savedCodec = CompressionCodecs[CompressionTypes.GZIP]
  })
  afterEach(async () => {
    const completion = pending?.catch((error) => {
      if (!(error instanceof KafkaJSDeliveryTimeout)) throw error
    })
    releaseEncoding?.()
    for (const gate of gates) if (gate.settle) gate.release()
    gates = []
    await jest.advanceTimersByTimeAsync(2000)
    if (completion) await completion
    await until(() => jest.getTimerCount() === 0)
    if (producer) {
      // An open transaction is aborted at close, which waits for its retries.
      let closed = false
      const closing = producer.disconnect().finally(() => {
        closed = true
      })
      // eslint-disable-next-line no-unmodified-loop-condition -- the close sets it while the loop awaits
      for (let i = 0; i < 200 && !closed; i += 1) {
        await jest.advanceTimersByTimeAsync(10)
        await new Promise((resolve) => setImmediate(resolve))
      }
      await closing
    }
    if (cluster) cluster.assertClean()
    CompressionCodecs[CompressionTypes.GZIP] = savedCodec
    jest.restoreAllMocks()
    jest.useRealTimers()
    producer = cluster = releaseEncoding = pending = null
  })

  it.each([true, false])(
    'refuses a never-written held encoder and settles unsent, idempotent %s',
    async (idempotent) => {
      await start(idempotent)
      const codec = savedCodec()
      let entered = false
      CompressionCodecs[CompressionTypes.GZIP] = () => ({
        ...codec,
        compress: async (...args) => {
          entered = true
          await new Promise((resolve) => {
            releaseEncoding = resolve
          })
          return require('zlib').gzipSync(args[0].buffer)
        },
      })
      let error
      pending = producer
        .send({ topic: TOPIC, compression: CompressionTypes.GZIP, messages: [{ value: 'held' }] })
        .catch((failure) => {
          error = failure
        })
      await until(() => entered)
      await jest.advanceTimersByTimeAsync(1000)
      expect(error).toBeInstanceOf(KafkaJSDeliveryTimeout)
      expect(error.outcome).toBe('unsent')
      expect(cluster.requests().filter(({ api }) => api === 'Produce')).toHaveLength(0)
      expect(jest.getTimerCount()).toBe(0)
      releaseEncoding()
      await until(() => jest.getTimerCount() === 0)
      for (let i = 0; i < 10; i += 1) await new Promise((resolve) => setImmediate(resolve))
      expect(cluster.requests().filter(({ api }) => api === 'Produce')).toHaveLength(0)
    }
  )

  it.each([true, false])(
    'revokes a held retry after an earlier written retriable answer, idempotent %s',
    async (idempotent) => {
      await start(idempotent)
      const Broker = require('../../broker')
      const forwarded = []
      const produce = Broker.prototype.produce
      jest.spyOn(Broker.prototype, 'produce').mockImplementation(function (request) {
        forwarded.push(request.guard)
        return produce.call(this, request)
      })
      cluster.answer('Produce', ({ topics }) => ({
        responses: topics.map(({ topic, partitions }) => ({
          topic,
          partitions: partitions.map(({ partition }) => ({
            partition,
            errorCode: 7,
            baseOffset: '-1',
            logAppendTime: '-1',
            logStartOffset: '0',
          })),
        })),
      }))
      const codec = savedCodec()
      let calls = 0
      CompressionCodecs[CompressionTypes.GZIP] = () => ({
        ...codec,
        compress: async (...args) => {
          calls += 1
          if (calls === 2)
            await new Promise((resolve) => {
              releaseEncoding = resolve
            })
          return require('zlib').gzipSync(args[0].buffer)
        },
      })
      let error
      pending = producer
        .send({ topic: TOPIC, compression: CompressionTypes.GZIP, messages: [{ value: 'retry' }] })
        .catch((failure) => {
          error = failure
        })
      await until(() => calls === 2)
      expect(cluster.requests().filter(({ api }) => api === 'Produce')).toHaveLength(1)
      await jest.advanceTimersByTimeAsync(1000)
      expect(error).toBeInstanceOf(KafkaJSDeliveryTimeout)
      expect(error).toMatchObject({ outcome: 'unknown', code: 7, type: 'REQUEST_TIMED_OUT' })
      expect(forwarded.at(-1)()).toBeInstanceOf(Error)
      expect(jest.getTimerCount()).toBe(0)
      releaseEncoding()
      for (let i = 0; i < 10; i += 1) await new Promise((resolve) => setImmediate(resolve))
      expect(cluster.requests().filter(({ api }) => api === 'Produce')).toHaveLength(1)
    }
  )

  it.each([true, false])(
    'checks the clock at transmission with the timer callback withheld, idempotent %s',
    async (idempotent) => {
      await start(idempotent)
      const codec = savedCodec()
      CompressionCodecs[CompressionTypes.GZIP] = () => ({
        ...codec,
        compress: async (...args) => {
          jest.setSystemTime(2000)
          return require('zlib').gzipSync(args[0].buffer)
        },
      })
      let error
      pending = producer
        .send({
          topic: TOPIC,
          compression: CompressionTypes.GZIP,
          messages: [{ value: 'clock step' }],
        })
        .catch((failure) => {
          error = failure
        })
      await until(() => error !== undefined)
      expect(error).toMatchObject({ name: 'KafkaJSDeliveryTimeout', outcome: 'unsent' })
      expect(cluster.requests().filter(({ api }) => api === 'Produce')).toHaveLength(0)
    }
  )

  it('passes the complete frame length only at the transmission guard', async () => {
    const observations = []
    const lengths = []
    const send = Connection.prototype.send
    jest.spyOn(Connection.prototype, 'send').mockImplementation(function (request) {
      if (request.request.apiKey !== 0 || !request.guard) return send.call(this, request)
      const guard = request.guard
      return send.call(this, {
        ...request,
        guard: (length) => {
          observations.push(length)
          return guard(length)
        },
      })
    })
    await start(true)
    const factory = cluster.socketFactory
    // Capture the socket already used by the producer through its connection send path.
    const original = Connection.prototype.send.getMockImplementation()
    Connection.prototype.send.mockImplementation(function (request) {
      if (request.request.apiKey === 0) {
        const write = this.socket.write.bind(this.socket)
        this.socket.write = (buffer, ...args) => {
          if (readRequestHeader(buffer).apiKey === 0) lengths.push(buffer.length)
          return write(buffer, ...args)
        }
      }
      return original.call(this, request)
    })
    let completed = false
    pending = producer.send({ topic: TOPIC, messages: [{ value: 'length' }] }).then(() => {
      completed = true
    })
    await until(() => completed)
    await pending
    expect(factory).toBeDefined()
    expect(lengths).toHaveLength(1)
    expect(observations.filter((value) => value !== undefined)).toEqual(lengths)
    expect(observations.some((value) => value === undefined)).toBe(true)
  })

  it('a written timeout never gives its sequence range back', async () => {
    const Sequencer = require('../state/partitionSequencer')
    const StateManager = require('../state')
    const rebase = jest.spyOn(Sequencer.prototype, 'rebase')
    const mark = jest.spyOn(Sequencer.prototype, 'markUnresolved')
    const fail = jest.spyOn(StateManager.prototype, 'fail')
    const complete = jest.spyOn(StateManager.prototype, 'complete')
    const schedule = new Schedule()
    const gate = schedule.hold({ api: 'Produce' })
    gates.push(gate)
    await start(true, schedule)
    let error
    pending = producer
      .send({ topic: TOPIC, messages: [{ value: 'first write' }] })
      .catch((failure) => {
        error = failure
      })
    await until(() => gate.call !== null)
    const wire = cluster.requests().find(({ api }) => api === 'Produce')
    expect(wire.fields.topics[0].partitions[0].batches[0]).toMatchObject({ baseSequence: 0 })
    await jest.advanceTimersByTimeAsync(1000)
    expect(error).toMatchObject({ name: 'KafkaJSDeliveryTimeout', outcome: 'unknown' })
    expect(mark).toHaveBeenCalledTimes(1)
    expect(rebase).not.toHaveBeenCalled()
    const batch = mark.mock.calls[0][0]
    expect(batch).toMatchObject({ firstSequence: 0, attempt: 0 })
    expect(fail.mock.contexts[0].sequencer.entry(TOPIC, 0)).toMatchObject({
      nextSequence: 1,
      unresolvedNextSequence: 1,
      inFlight: [],
    })
    gate.release()
    await until(() => complete.mock.calls.length === 1)
    expect(mark).toHaveBeenCalledTimes(1)
    expect(rebase).not.toHaveBeenCalled()
  })

  it.each(['tv1', 'tv2'])(
    'a transaction is abortable at its on-wire deadline before the answer returns, %s',
    async (profile) => {
      const StateManager = require('../state')
      const { ABORTABLE_ERROR } = require('../eosManager/transactionStates')
      const fail = jest.spyOn(StateManager.prototype, 'fail')
      const complete = jest.spyOn(StateManager.prototype, 'complete')
      const schedule = new Schedule()
      const gate = schedule.hold({ api: 'Produce' })
      gates.push(gate)
      cluster = producerScript(
        createScriptedCluster({
          nodes: [{ id: 0, host: 'node0', port: 9092 }],
          profile,
          schedule,
        })
      )
      producer = new Kafka({
        ...cluster.kafkaOptions(),
        requestTimeout: 1000,
        enforceRequestTimeout: false,
      }).producer({
        transactionalId: 'expiry-transaction',
        idempotent: true,
        linger: 0,
        deliveryTimeout: 1000,
      })
      await producer.connect()
      let tx
      const opening = producer.transaction().then((value) => {
        tx = value
      })
      await until(() => tx !== undefined)
      await opening
      let error
      pending = tx
        .send({ topic: TOPIC, messages: [{ value: 'transaction write' }] })
        .catch((failure) => {
          error = failure
        })
      await until(() => gate.call !== null)
      const request = cluster.requests().find(({ api }) => api === 'Produce')
      expect(request.fields.transactionalId).toBe('expiry-transaction')
      await jest.advanceTimersByTimeAsync(1000)
      expect(error).toMatchObject({ name: 'KafkaJSDeliveryTimeout', outcome: 'unknown' })
      expect(fail).toHaveBeenCalledTimes(1)
      const manager = fail.mock.contexts[0]
      expect(manager.state()).toBe(ABORTABLE_ERROR)
      gate.release()
      await until(() => complete.mock.calls.length === 1)
      expect(manager.state()).toBe(ABORTABLE_ERROR)
      expect(cluster.requests().filter(({ api }) => api === 'Produce')).toHaveLength(1)
    }
  )

  it.each([0, 46, 6, 87])(
    'reconciles a late partition answer %i without rerunning its verdict',
    async (code) => {
      const StateManager = require('../state')
      const fail = jest.spyOn(StateManager.prototype, 'fail')
      const complete = jest.spyOn(StateManager.prototype, 'complete')
      const schedule = new Schedule()
      const gate = schedule.hold({ api: 'Produce' })
      gates.push(gate)
      await start(true, schedule)
      let error
      pending = producer.send({ topic: TOPIC, messages: [{ value: 'late' }] }).catch((failure) => {
        error = failure
      })
      await until(() => gate.call !== null)
      await jest.advanceTimersByTimeAsync(1000)
      expect(error).toMatchObject({ name: 'KafkaJSDeliveryTimeout', outcome: 'unknown' })
      expect(fail).toHaveBeenCalledTimes(1)
      const manager = fail.mock.contexts[0]
      const before = manager.sequencer.entry(TOPIC, 0)
      expect(before).toMatchObject({ nextSequence: 1, unresolvedNextSequence: 1, inFlight: [] })
      gate.release({
        responses: [
          {
            topic: TOPIC,
            partitions: [
              {
                partition: 0,
                errorCode: code,
                baseOffset: code === 0 ? '80' : '-1',
                logAppendTime: '-1',
                logStartOffset: '0',
              },
            ],
          },
        ],
      })
      await until(() =>
        cluster.timeline().some(({ api, kind }) => api === 'Produce' && kind === 'response')
      )
      for (let i = 0; i < 10; i += 1) await new Promise((resolve) => setImmediate(resolve))
      expect(fail).toHaveBeenCalledTimes(1)
      if (code === 0 || code === 46) {
        expect(complete).toHaveBeenCalledTimes(1)
        expect(manager.sequencer.entry(TOPIC, 0)).toMatchObject({
          lastAckedSequence: 0,
          lastAckedOffset: code === 0 ? 80 : -1,
          inFlight: [],
        })
      } else {
        expect(complete).not.toHaveBeenCalled()
        expect(manager.sequencer.entry(TOPIC, 0)).toEqual(before)
      }
      let finished = false
      pending = producer.send({ topic: TOPIC, messages: [{ value: 'next' }] }).then(() => {
        finished = true
      })
      await until(() => finished)
      const last = cluster
        .requests()
        .filter(({ api }) => api === 'Produce')
        .at(-1).fields.topics[0].partitions[0].batches[0]
      expect(last).toMatchObject({
        producerEpoch: code === 0 || code === 46 ? 0 : 1,
        baseSequence: code === 0 || code === 46 ? 1 : 0,
      })
    }
  )

  it('keeps the broker identity of a late connection failure and changes no sequence state', async () => {
    const StateManager = require('../state')
    const { failureSourceOf, attachFailureSource } = require('../../broker/failureSource')
    const { KafkaJSConnectionError, KafkaJSConnectionClosedError } = require('../../errors')
    const Broker = require('../../broker')
    const retired = jest.spyOn(Broker.prototype, 'retire')
    const Cluster = require('../../cluster')
    const removed = jest.spyOn(Cluster.prototype, 'removeBroker')
    const produce = Broker.prototype.produce
    const fail = jest.spyOn(StateManager.prototype, 'fail')
    let injected
    jest.spyOn(Broker.prototype, 'produce').mockImplementation(async function (request) {
      try {
        return await produce.call(this, request)
      } catch (error) {
        const source = failureSourceOf(error)
        if (!source) throw error
        await source.broker.connect()
        const closed = new KafkaJSConnectionClosedError('older generation closed', {
          host: 'node0',
          port: 9092,
        })
        attachFailureSource(closed, source)
        injected = closed
        throw closed
      }
    })
    const schedule = new Schedule()
    const gate = schedule.hold({ api: 'Produce' })
    gates.push(gate)
    await start(true, schedule)
    let error
    pending = producer
      .send({ topic: TOPIC, messages: [{ value: 'lost answer' }] })
      .catch((failure) => {
        error = failure
      })
    await until(() => gate.call !== null)
    await jest.advanceTimersByTimeAsync(1000)
    expect(error.outcome).toBe('unknown')
    const manager = fail.mock.contexts[0]
    const before = manager.sequencer.entry(TOPIC, 0)
    const failure = new KafkaJSConnectionError('lost answer')
    gate.fail(failure)
    await until(() => injected !== undefined)
    await until(() => cluster.requests().every(({ answeredOrder }) => answeredOrder !== null))
    expect(fail).toHaveBeenCalledTimes(1)
    expect(manager.sequencer.entry(TOPIC, 0)).toEqual(before)
    expect(removed).toHaveBeenCalledWith({
      host: 'node0',
      port: 9092,
      source: failureSourceOf(injected),
    })
    expect(retired.mock.contexts).not.toContain(failureSourceOf(injected).broker)
    expect(failureSourceOf(injected).broker.connectionPool.generation).toBeGreaterThan(
      failureSourceOf(injected).generation
    )
    expect(failureSourceOf(injected)).toEqual({
      broker: expect.any(Object),
      generation: expect.any(Number),
    })
  })

  it('disposes and acknowledges an unrelated old-epoch partition across two epoch bumps', async () => {
    const StateManager = require('../state')
    const complete = jest.spyOn(StateManager.prototype, 'complete')
    const schedule = new Schedule()
    const gate = schedule.hold({
      api: 'Produce',
      match: ({ fields }) =>
        fields.topics.some(({ partitions }) => partitions.some(({ partition }) => partition === 1)),
    })
    gates.push(gate)
    await start(true, schedule, 2)
    let calls = 0
    cluster.answer('Produce', ({ topics }) => ({
      responses: topics.map(({ partitions, ...key }) => ({
        ...key,
        partitions: partitions.map(({ partition }) => ({
          partition,
          errorCode: partition === 0 && calls++ % 2 === 0 ? 45 : 0,
          baseOffset: '0',
          logAppendTime: '-1',
          logStartOffset: '0',
        })),
      })),
    }))
    let oldDone = false
    pending = producer
      .send({ topic: TOPIC, messages: [{ partition: 1, value: 'old epoch' }] })
      .then(() => {
        oldDone = true
      })
    await until(() => gate.call !== null)
    for (let i = 0; i < 2; i += 1) {
      let done = false
      const bump = producer
        .send({ topic: TOPIC, messages: [{ partition: 0, value: 'bump' }] })
        .then(() => {
          done = true
        })
      await until(() => done)
      await bump
    }
    gate.release({
      responses: [
        {
          topic: TOPIC,
          partitions: [
            {
              partition: 1,
              errorCode: 0,
              baseOffset: '80',
              logAppendTime: '-1',
              logStartOffset: '0',
            },
          ],
        },
      ],
    })
    await until(() => oldDone)
    const manager = complete.mock.contexts.at(-1)
    expect(manager.producerEpoch).toBe(2)
    expect(manager.sequencer.entry(TOPIC, 1)).toMatchObject({
      lastAckedSequence: 0,
      lastAckedOffset: 80,
      inFlight: [],
    })
    let nextDone = false
    pending = producer
      .send({ topic: TOPIC, messages: [{ partition: 1, value: 'new epoch' }] })
      .then(() => {
        nextDone = true
      })
    await until(() => nextDone)
    const batch = cluster
      .requests()
      .filter(({ api }) => api === 'Produce')
      .at(-1).fields.topics[0].partitions[0].batches[0]
    expect(batch).toMatchObject({ producerEpoch: 2, baseSequence: 0 })
  })
  it.each([
    [true, false],
    [false, false],
    [true, true],
    [false, true],
  ])(
    'refuses an expired pending-queue request, idempotent %s, earlier write %s',
    async (idempotent, writtenBefore) => {
      await start(idempotent)
      if (writtenBefore) {
        cluster.answer('Produce', ({ topics }) => ({
          responses: topics.map(({ partitions, ...key }) => ({
            ...key,
            partitions: partitions.map(({ partition }) => ({
              partition,
              errorCode: 7,
              baseOffset: '-1',
              logAppendTime: '-1',
              logStartOffset: '0',
            })),
          })),
        }))
      }
      const originalSend = Connection.prototype.send
      let count = 0
      let held
      let guard
      jest.spyOn(Connection.prototype, 'send').mockImplementation(function (request) {
        if (request.request.apiKey === 0 && ++count === (writtenBefore ? 2 : 1)) {
          held = this
          guard = request.guard
          this.requestQueue.throttledUntil = Date.now() + 2000
        }
        return originalSend.call(this, request)
      })
      let error
      pending = producer
        .send({ topic: TOPIC, messages: [{ value: 'pending' }] })
        .catch((failure) => {
          error = failure
        })
      await until(() => held !== undefined && held.requestQueue.pending.length > 0)
      await jest.advanceTimersByTimeAsync(1000)
      expect(error).toMatchObject({
        name: 'KafkaJSDeliveryTimeout',
        outcome: writtenBefore ? 'unknown' : 'unsent',
      })
      expect(guard()).toBeInstanceOf(Error)
      expect(cluster.requests().filter(({ api }) => api === 'Produce')).toHaveLength(
        writtenBefore ? 1 : 0
      )
      held.requestQueue.throttledUntil = 0
      held.requestQueue.checkPendingRequests()
      await jest.advanceTimersByTimeAsync(0)
      for (let i = 0; i < 10; i += 1) await new Promise((resolve) => setImmediate(resolve))
      expect(cluster.requests().filter(({ api }) => api === 'Produce')).toHaveLength(
        writtenBefore ? 1 : 0
      )
    }
  )
})

describe('deadline owners during held recovery and expired hand-off', () => {
  const { createFakeCluster } = require('./fixtures/fakeCluster')
  const { newLogger } = require('testHelpers')
  const { ABORTABLE_ERROR } = require('../eosManager/transactionStates')
  let releases, pending
  beforeEach(() => {
    jest.useFakeTimers({ now: 0, doNotFake: ['nextTick', 'setImmediate'] })
    releases = []
    pending = []
  })
  afterEach(async () => {
    releases.forEach((release) => release())
    await jest.advanceTimersByTimeAsync(2000)
    await Promise.all(pending)
    jest.restoreAllMocks()
    jest.useRealTimers()
  })
  const gate = () => {
    let release
    const promise = new Promise((resolve) => {
      release = resolve
    })
    releases.push(release)
    return { promise, release }
  }
  const response = (request, code = 0) => ({
    topics: request.topicData.map(({ topic, partitions }) => ({
      topicName: topic,
      partitions: partitions.map(({ partition }) => ({
        partition,
        errorCode: code,
        baseOffset: code ? '-1' : '0',
        logAppendTime: '-1',
        logStartOffset: '0',
      })),
    })),
  })
  /** Real producer state and drain owner; the fake broker only supplies terminal responses. */
  const owner = async (transactional, produce, factories = {}) => {
    const cluster = createFakeCluster({
      partitions: 2,
      produce,
      options: {
        requestTimeout: 1000,
        enforceRequestTimeout: false,
        linger: 0,
        deliveryTimeout: 1000,
      },
    })
    cluster.applyLeaderHint = jest.fn(async () => true)
    const logger = newLogger()
    const retry = { retries: 10, initialRetryTime: 10, maxRetryTime: 10, factor: 0 }
    const eosManager = (factories.eos || require('../eosManager'))({
      logger,
      cluster,
      transactional,
      transactionalId: transactional ? 'expiry-owner' : undefined,
      retry,
    })
    await eosManager.initProducerId()
    if (transactional) await eosManager.beginTransaction()
    const retrier = (factories.retry || require('../../retry'))(retry)
    const sender = (factories.sender || require('../sendMessages'))({
      logger,
      cluster,
      eosManager,
      retrier,
      retry,
      deliveryTimeout: 1000,
      idempotence: new (require('../idempotence'))({
        enabled: !factories.plain,
        explicit: false,
        logger,
      }),
      partitioner: () => 0,
      getConnectionStatus: () => 'connected',
    })
    const send = (partition, createdAt = 0) => {
      const settler = { resolve: jest.fn(), reject: jest.fn() }
      let error
      const work = sender
        .sendPartitioned({
          acks: -1,
          timeout: 1000,
          compression: 0,
          batches: [
            {
              topic: TOPIC,
              partition,
              createdAt,
              messages: [{ value: 'owner' }],
              settlers: [settler],
            },
          ],
        })
        .catch((failure) => {
          error = failure
        })
      pending.push(work)
      return { settler, work, error: () => error }
    }
    return { cluster, manager: eosManager.stateManager, send }
  }

  it.each([false, true])(
    'expires every queued delivery while a pass waits for a recovery hint, transactional %s',
    async (transactional) => {
      const answer = gate()
      const hint = gate()
      const harness = await owner(transactional, async (request) => {
        const refusal = request.guard?.(100)
        if (refusal) throw refusal
        if (request.topicData[0].partitions[0].partition === 1) {
          await answer.promise
          return response(request)
        }
        const result = response(request, 6)
        result.topics[0].partitions[0].currentLeader = { leaderId: 1, leaderEpoch: 2 }
        return result
      })
      harness.cluster.applyLeaderHint.mockImplementation(() => hint.promise)
      const written = harness.send(1)
      await jest.advanceTimersByTimeAsync(0)
      const retried = harness.send(0)
      await jest.advanceTimersByTimeAsync(0)
      expect(harness.cluster.applyLeaderHint).toHaveBeenCalledTimes(1)
      const queued = [harness.send(0), harness.send(1)]
      await jest.advanceTimersByTimeAsync(0)
      const writes = harness.cluster.broker.produce.mock.calls.length
      expect(writes).toBe(2)
      await jest.advanceTimersByTimeAsync(1000)
      expect(written.error()).toMatchObject({ name: 'KafkaJSDeliveryTimeout', outcome: 'unknown' })
      for (const delivery of queued) {
        expect(delivery.error()).toMatchObject({
          name: 'KafkaJSDeliveryTimeout',
          outcome: 'unsent',
        })
        expect(delivery.settler.reject).toHaveBeenCalledTimes(1)
        expect(delivery.settler.resolve).not.toHaveBeenCalled()
      }
      expect(retried.error()).toBeDefined()
      if (transactional) expect(harness.manager.state()).toBe(ABORTABLE_ERROR)
      expect(jest.getTimerCount()).toBe(0)
      hint.release()
      answer.release()
      await jest.advanceTimersByTimeAsync(0)
      await Promise.all(pending)
      await jest.advanceTimersByTimeAsync(1000)
      expect(harness.cluster.broker.produce).toHaveBeenCalledTimes(writes)
      expect(jest.getTimerCount()).toBe(0)
      for (const delivery of queued) {
        expect(delivery.settler.reject).toHaveBeenCalledTimes(1)
        expect(delivery.settler.resolve).not.toHaveBeenCalled()
      }
    }
  )

  it.each(['idempotent', 'transactional', 'plain'])(
    'a zero remaining hand-off budget creates neither retrier nor write, mode %s',
    async (mode) => {
      const transactional = mode === 'transactional'
      const realRetry = require('../../retry')
      const retryFactory = Object.assign(
        jest.fn((options) => realRetry(options)),
        realRetry
      )
      let factories
      jest.isolateModules(() => {
        jest.doMock('../../retry', () => retryFactory)
        factories = {
          sender: require('../sendMessages'),
          eos: require('../eosManager'),
          retry: require('../../retry'),
        }
      })
      jest.dontMock('../../retry')
      factories.plain = mode === 'plain'
      const harness = await owner(transactional, async (request) => response(request), factories)
      jest.setSystemTime(1000)
      retryFactory.mockClear()
      const scheduled = jest.spyOn(global, 'setTimeout')
      const delivery = harness.send(0)
      await jest.advanceTimersByTimeAsync(0)
      await delivery.work
      expect(delivery.error()).toMatchObject({ name: 'KafkaJSDeliveryTimeout', outcome: 'unsent' })
      expect(delivery.settler.reject).toHaveBeenCalledTimes(1)
      expect(harness.cluster.broker.produce).not.toHaveBeenCalled()
      expect(retryFactory).not.toHaveBeenCalled()
      expect(scheduled).not.toHaveBeenCalled()
      expect(jest.getTimerCount()).toBe(0)
      if (transactional) expect(harness.manager.state()).toBe(ABORTABLE_ERROR)
    }
  )
})
