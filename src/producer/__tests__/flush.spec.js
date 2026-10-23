const {
  bufferedProducer,
  deferred,
  succeeded,
  flushAll,
  watch,
} = require('./fixtures/bufferHarness')
const {
  KafkaJSTimeout,
  KafkaJSInvalidConfigurationError,
  KafkaJSNonRetriableError,
} = require('../../errors')

describe('producer flush snapshot', () => {
  beforeEach(() => jest.useFakeTimers({ now: 0, doNotFake: ['setImmediate'] }))
  afterEach(() => jest.useRealTimers())

  test('waits for earlier sends and flushes them while a later send stays unsettled', async () => {
    const { producer, cluster } = bufferedProducer({
      idempotent: false,
      linger: 60000,
      deliveryTimeout: 120000,
    })
    const gates = []
    cluster.broker.produce.mockImplementation(async (request) => {
      const held = deferred()
      gates.push(held)
      await held.promise
      return succeeded(request)
    })
    await producer.connect()
    const sends = [0, 1, 2].map((n) =>
      producer.send({ topic: 'topic' + n, messages: [{ value: 'earlier' }] })
    )
    const order = []
    sends.forEach((sent) => sent.then(() => order.push('send')))
    const flushed = Promise.resolve().then(() => producer.flush())
    flushed.then(() => order.push('flush'))
    const flush = watch(flushed)
    let later
    try {
      await flushAll()
      expect(cluster.broker.produce.mock.calls.length).toBeGreaterThan(0)
      expect(flush.state).toBe('pending')
      const earlier = gates.splice(0)
      later = watch(producer.send({ topic: 'later', messages: [{ value: 'later' }] }))
      await flushAll()
      earlier.forEach((held) => held.resolve())
      await flushAll()
      expect(flush.state).toBe('fulfilled')
      expect(later.state).toBe('pending')
      expect(order).toEqual(['send', 'send', 'send', 'flush'])
      await Promise.all(sends)
    } finally {
      const closing = producer.disconnect()
      await flushAll()
      for (let i = 0; i < 5; i += 1) {
        gates.splice(0).forEach((held) => held.resolve())
        await flushAll()
      }
      await closing
    }
  })

  test('a zero call timeout leaves earlier sends running and invalid options change nothing', async () => {
    const { producer, cluster } = bufferedProducer({
      idempotent: false,
      linger: 60000,
      deliveryTimeout: 120000,
    })
    const held = deferred()
    cluster.broker.produce.mockImplementation(async (request) => {
      await held.promise
      return succeeded(request)
    })
    await producer.connect()
    const send = watch(producer.send({ topic: 'topic', messages: [{ value: 'earlier' }] }))
    try {
      await flushAll()
      const invalid = watch(Promise.resolve().then(() => producer.flush({ timeout: -1 })))
      await flushAll()
      expect(invalid.value).toBeInstanceOf(KafkaJSInvalidConfigurationError)
      expect(cluster.broker.produce).not.toHaveBeenCalled()
      const zero = watch(Promise.resolve().then(() => producer.flush({ timeout: 0 })))
      await flushAll()
      expect(zero.value).toBeInstanceOf(KafkaJSTimeout)
      expect(send.state).toBe('pending')
      held.resolve()
      await producer.flush()
      await flushAll()
      expect(send.state).toBe('fulfilled')
      await expect(producer.flush({ timeout: 0 })).resolves.toBeUndefined()
    } finally {
      held.resolve()
      await flushAll()
      await producer.disconnect()
    }
  })
})

describe('producer flush semantics', () => {
  beforeEach(() => jest.useFakeTimers({ now: 0, doNotFake: ['setImmediate'] }))
  afterEach(() => jest.useRealTimers())

  /** A producer whose Produce answers wait for `release()`, with one lingering send pending. */
  const lingering = async (options = {}) => {
    const fixture = bufferedProducer({
      idempotent: false,
      linger: 60000,
      deliveryTimeout: 120000,
      ...options,
    })
    const held = deferred()
    fixture.cluster.broker.produce.mockImplementation(async (request) => {
      await held.promise
      return succeeded(request)
    })
    await fixture.producer.connect()
    const send = watch(fixture.producer.send({ topic: 'topic', messages: [{ value: 'a' }] }))
    return { ...fixture, send, held }
  }

  test.each([-1, 0.5, NaN, Infinity, 2147483648, '10', true, null, () => {}])(
    'a timeout of %p is refused before anything changes',
    async (timeout) => {
      const { producer, cluster, send, held } = await lingering()
      try {
        const timers = jest.getTimerCount()
        const refused = watch(Promise.resolve().then(() => producer.flush({ timeout })))
        await flushAll()
        expect(refused.value).toBeInstanceOf(KafkaJSInvalidConfigurationError)
        expect(cluster.broker.produce).not.toHaveBeenCalled()
        expect(jest.getTimerCount()).toBe(timers)
        expect(send.state).toBe('pending')
      } finally {
        held.resolve()
        await producer.flush()
        await producer.disconnect()
      }
    }
  )

  test.each([null, 5, 'soon', true, () => {}])(
    'an argument of %p that is not an object is refused, and one returns a rejection, not a throw',
    async (options) => {
      const { producer, cluster, send, held } = await lingering()
      try {
        let returned
        expect(() => {
          returned = producer.flush(options)
        }).not.toThrow()
        const refused = watch(returned)
        await flushAll()
        expect(refused.value).toBeInstanceOf(KafkaJSInvalidConfigurationError)
        expect(cluster.broker.produce).not.toHaveBeenCalled()
        expect(send.state).toBe('pending')
      } finally {
        held.resolve()
        await producer.flush()
        await producer.disconnect()
      }
    }
  )

  test.each([0, 2147483647])(
    'a timeout of %p is accepted, and resolves with nothing unsettled',
    async (timeout) => {
      const { producer } = bufferedProducer({ idempotent: false })
      await producer.connect()
      await expect(producer.flush({ timeout })).resolves.toBeUndefined()
      await expect(producer.flush({})).resolves.toBeUndefined()
      expect(jest.getTimerCount()).toBe(0)
      await producer.disconnect()
    }
  )

  test('a timeout rejects KafkaJSTimeout while the send runs on, and the timer is cleared', async () => {
    const { producer, send, held } = await lingering({ linger: 0, deliveryTimeout: 1000 })
    try {
      await flushAll()
      const flush = watch(producer.flush({ timeout: 10 }))
      await jest.advanceTimersByTimeAsync(10)
      await flushAll()
      expect(flush.value).toBeInstanceOf(KafkaJSTimeout)
      expect(send.state).toBe('pending')
      held.resolve()
      await flushAll()
      expect(send.state).toBe('fulfilled')
      await expect(producer.flush({ timeout: 10 })).resolves.toBeUndefined()
    } finally {
      held.resolve()
      await jest.advanceTimersByTimeAsync(2000)
      await producer.disconnect()
    }
    expect(jest.getTimerCount()).toBe(0)
  })

  test('a failed send does not reject flush, and its own promise carries the failure', async () => {
    const { producer, cluster } = bufferedProducer({
      idempotent: false,
      linger: 60000,
      deliveryTimeout: 120000,
    })
    cluster.broker.produce.mockRejectedValue(new KafkaJSNonRetriableError('refused'))
    await producer.connect()
    const send = watch(producer.send({ topic: 'topic', messages: [{ value: 'a' }] }))
    const flush = watch(Promise.resolve().then(() => producer.flush()))
    await flushAll()
    expect(send.state).toBe('rejected')
    expect(flush.state).toBe('fulfilled')
    await producer.disconnect()
  })

  test('two flushes keep their own snapshots and timers', async () => {
    const { producer, held } = await lingering({ linger: 0, deliveryTimeout: 60000 })
    try {
      await flushAll()
      const early = watch(producer.flush({ timeout: 10 }))
      const second = watch(producer.send({ topic: 'topic', messages: [{ value: 'b' }] }))
      const late = watch(producer.flush({ timeout: 100000 }))
      await jest.advanceTimersByTimeAsync(10)
      expect(early.value).toBeInstanceOf(KafkaJSTimeout)
      expect(late.state).toBe('pending')
      held.resolve()
      await flushAll()
      expect(late.state).toBe('fulfilled')
      expect(second.state).toBe('fulfilled')
    } finally {
      held.resolve()
      await producer.disconnect()
    }
  })

  test('flush covers the producer and its transaction together', async () => {
    const { producer, cluster } = bufferedProducer({
      transactionalId: 'flush-transaction',
      idempotent: true,
      linger: 60000,
      deliveryTimeout: 120000,
    })
    const { broker } = cluster
    Object.assign(broker, {
      finalizedFeatures: { 'transaction.version': { minVersionLevel: 0, maxVersionLevel: 2 } },
      finalizedFeaturesEpoch: 1,
      negotiatedVersion: () => 5,
    })
    broker.produce.mockImplementation(async (request) => succeeded(request))
    await producer.connect()
    const transaction = await producer.transaction()
    const own = watch(producer.send({ topic: 'own', messages: [{ value: 'a' }] }))
    const inside = watch(transaction.send({ topic: 'inside', messages: [{ value: 'b' }] }))
    const flush = watch(Promise.resolve().then(() => producer.flush()))
    await flushAll()
    expect([own.state, inside.state, flush.state]).toEqual(['fulfilled', 'fulfilled', 'fulfilled'])
    await transaction.commit()
    await producer.disconnect()
  })
})
