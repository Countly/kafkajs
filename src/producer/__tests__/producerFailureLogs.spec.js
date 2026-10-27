const Accumulator = require('../state/accumulator')
const { bufferedProducer } = require('./fixtures/bufferHarness')
const { createLogger, LEVELS } = require('../../loggers')

const TOPIC = 'topic-a'
const MESSAGES = [{ key: 'key-0', value: 'value-0' }]

/**
 * A producer over the fake cluster whose logger keeps every entry from warnings up.
 * @returns {{ producer: object, cluster: object, logged: () => object[] }} `logged` gives the
 *   entries' levels, messages and fields
 */
const loggingProducer = () => {
  const entries = []
  const logger = createLogger({
    level: LEVELS.WARN,
    logCreator: () => (entry) => entries.push(entry),
  })
  const { producer, cluster } = bufferedProducer({ logger })
  const logged = () => entries.map(({ level, log }) => ({ level, ...log }))
  return { producer, cluster, logged }
}

/** Lets the promise callbacks a listener's rejection queued run. */
const turn = () => new Promise((resolve) => setImmediate(resolve))

describe('Producer > what a failure inside the producer logs instead of throwing', () => {
  afterEach(() => {
    jest.restoreAllMocks()
  })

  test('a listener that rejects is logged as an error with its event name and stack, and the others still run', async () => {
    const { producer, logged } = loggingProducer()
    const failure = new Error('listener failed')
    const other = jest.fn()
    producer.on(producer.events.CONNECT, async () => {
      throw failure
    })
    producer.on(producer.events.CONNECT, other)

    await producer.connect()
    await turn()

    expect(other).toHaveBeenCalledTimes(1)
    expect(logged()).toEqual([
      expect.objectContaining({
        level: LEVELS.ERROR,
        message: 'Failed to execute listener: listener failed',
        eventName: producer.events.CONNECT,
        stack: failure.stack,
      }),
    ])
    await producer.disconnect()
  })

  test('a failure while delivering what is accumulated is logged as a warning, and the producer still disconnects', async () => {
    const { producer, cluster, logged } = loggingProducer()
    const disconnected = jest.fn()
    producer.on(producer.events.DISCONNECT, disconnected)
    await producer.connect()
    jest.spyOn(Accumulator.prototype, 'close').mockImplementation(() => {
      throw new Error('delivery broke')
    })

    await expect(producer.disconnect()).resolves.toBeUndefined()

    expect(logged()).toEqual([
      expect.objectContaining({
        level: LEVELS.WARN,
        message: 'The producer could not finish delivering while it was disconnecting',
        error: 'delivery broke',
      }),
    ])
    expect(cluster.disconnect).toHaveBeenCalledTimes(1)
    expect(disconnected).toHaveBeenCalledTimes(1)
    await expect(producer.send({ topic: TOPIC, messages: MESSAGES })).rejects.toThrow(
      'The producer is disconnected'
    )
  })

  test('a cluster that fails to disconnect is logged as a warning, and the producer still ends disconnected', async () => {
    const { producer, cluster, logged } = loggingProducer()
    const disconnected = jest.fn()
    producer.on(producer.events.DISCONNECT, disconnected)
    await producer.connect()
    cluster.disconnect.mockRejectedValue(new Error('sockets would not close'))

    await expect(producer.disconnect()).resolves.toBeUndefined()

    expect(logged()).toEqual([
      expect.objectContaining({
        level: LEVELS.WARN,
        message: 'Disconnecting the cluster failed',
        error: 'sockets would not close',
      }),
    ])
    expect(cluster.disconnect).toHaveBeenCalledTimes(1)
    expect(disconnected).toHaveBeenCalledTimes(1)
    await expect(producer.send({ topic: TOPIC, messages: MESSAGES })).rejects.toThrow(
      'The producer is disconnected'
    )
  })
})
