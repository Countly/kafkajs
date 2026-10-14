const createProducer = require('../index')
const { createFakeCluster } = require('./fixtures/fakeCluster')
const { newLogger } = require('testHelpers')

const TOPIC = 'topic-a'
const REQUEST_TIMED_OUT = 7
const MESSAGE_TOO_LARGE = 10
const RETRY = { retries: 3, initialRetryTime: 1, maxRetryTime: 5 }

const MODES = {
  idempotent: { linger: 0, retry: RETRY },
  'non-idempotent': { idempotent: false, linger: 0, retry: RETRY },
}

/**
 * A Produce answer that lists each topic's partitions in the reverse of the order the request
 * carried them. A partition's offset is `100 + partition`; `errors` maps a partition to the error
 * code it fails with, for as many of its answers as the list holds.
 * @param {{ [partition: number]: number[] }} [errors]
 */
const reversedBroker = (errors = {}) => {
  const failures = new Map(
    Object.entries(errors).map(([partition, codes]) => [+partition, [...codes]])
  )
  return async ({ topicData }) => ({
    topics: topicData.map(({ topic, partitions }) => ({
      topicName: topic,
      partitions: partitions
        .map(({ partition }) => {
          const errorCode = failures.get(partition)?.shift() ?? 0
          return {
            partition,
            errorCode,
            baseOffset: errorCode === 0 ? String(100 + partition) : '-1',
            logAppendTime: '-1',
            logStartOffset: '0',
          }
        })
        .reverse(),
    })),
  })
}

/** The partitions of every Produce request the broker received, in the order it listed them. */
const requestedPartitions = (cluster) =>
  cluster.broker.produce.mock.calls.map(([{ topicData }]) =>
    topicData.flatMap(({ partitions }) => partitions.map(({ partition }) => partition))
  )

describe.each(Object.keys(MODES))('Producer > a Produce answer in reverse order > %s', (mode) => {
  const start = async (errors) => {
    const cluster = createFakeCluster({ partitions: 2, produce: reversedBroker(errors) })
    const producer = createProducer({ cluster, logger: newLogger(), ...MODES[mode] })
    await producer.connect()
    // two sends of one tick share their request
    const send = (partition) =>
      producer.send({ topic: TOPIC, messages: [{ value: `v${partition}`, partition }] })
    return { cluster, producer, sends: [send(0), send(1)] }
  }

  test('each partition gets its own offset, from one request', async () => {
    const { cluster, producer, sends } = await start()

    const [first, second] = await Promise.all(sends)
    await producer.disconnect()

    expect(first).toEqual([expect.objectContaining({ partition: 0, baseOffset: '100' })])
    expect(second).toEqual([expect.objectContaining({ partition: 1, baseOffset: '101' })])
    expect(requestedPartitions(cluster)).toEqual([[0, 1]])
  })

  test('a partition that failed for good fails alone, and nothing is sent again', async () => {
    const { cluster, producer, sends } = await start({ 1: [MESSAGE_TOO_LARGE] })

    const [first, second] = await Promise.allSettled(sends)
    await producer.disconnect()

    expect(first).toMatchObject({
      status: 'fulfilled',
      value: [expect.objectContaining({ partition: 0, baseOffset: '100' })],
    })
    expect(second).toMatchObject({
      status: 'rejected',
      reason: expect.objectContaining({ type: 'MESSAGE_TOO_LARGE', partition: 1 }),
    })
    expect(requestedPartitions(cluster)).toEqual([[0, 1]])
  })

  test('a partition that failed retriably is sent again alone, and the other is not', async () => {
    const { cluster, producer, sends } = await start({ 0: [REQUEST_TIMED_OUT] })

    const [first, second] = await Promise.all(sends)
    await producer.disconnect()

    expect(first).toEqual([expect.objectContaining({ partition: 0, baseOffset: '100' })])
    expect(second).toEqual([expect.objectContaining({ partition: 1, baseOffset: '101' })])
    expect(requestedPartitions(cluster)).toEqual([[0, 1], [0]])
  })
})
