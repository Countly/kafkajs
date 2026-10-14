const createProducer = require('../index')
const { createFakeCluster } = require('./fixtures/fakeCluster')
const {
  KafkaJSConnectionClosedError,
  KafkaJSNumberOfRetriesExceeded,
  KafkaJSProtocolError,
} = require('../../errors')
const { newLogger } = require('testHelpers')

const MESSAGE_TOO_LARGE = 10
const INVALID_TIMESTAMP = 32

const options = {
  logger: newLogger(),
  idempotent: true,
  linger: 0,
  retry: { retries: 1, initialRetryTime: 1, maxRetryTime: 1, factor: 0 },
}

/**
 * The per-partition answer to a Produce, failing every partition with `errorCode` when given.
 * @param {{ topicData: object[] }} request
 * @param {number} [errorCode=0]
 */
const answer = ({ topicData }, errorCode = 0) => ({
  topics: topicData.map(({ topic, partitions }) => ({
    topicName: topic,
    partitions: partitions.map(({ partition }) => ({
      partition,
      errorCode,
      baseOffset: '0',
      logAppendTime: '-1',
      logStartOffset: '0',
    })),
  })),
})

/**
 * @param {{ producerId: string, producerEpoch: number, topicData: object[] }} request
 * @returns {string} `producerId/epoch/firstSequence` of the request's first batch
 */
const stampsOf = ({ producerId, producerEpoch, topicData }) =>
  `${producerId}/${producerEpoch}/${topicData[0].partitions[0].firstSequence}`

/**
 * A fake cluster whose broker de-duplicates by producer id, epoch and first sequence the way a
 * real one does, and loses the acknowledgement of the first `lost` Produce requests after
 * appending them.
 * @param {number} lost how many acknowledgements never arrive
 * @returns {{ cluster: object, written: string[], stamps: string[] }} what the broker appended,
 *   and the `producerId/epoch/firstSequence` of every request it received
 */
const createLossyCluster = (lost) => {
  const seen = new Set()
  const written = []
  const stamps = []
  let requests = 0

  const cluster = createFakeCluster({
    produce: async (request) => {
      for (const { partitions } of request.topicData) {
        for (const batch of partitions) {
          const stamp = `${request.producerId}/${request.producerEpoch}/${batch.firstSequence}`
          stamps.push(stamp)
          if (!seen.has(stamp)) {
            seen.add(stamp)
            written.push(...batch.messages.map(({ value }) => value))
          }
        }
      }

      if (++requests <= lost) {
        throw new KafkaJSConnectionClosedError('acknowledgement lost', { broker: 'fake' })
      }
      return answer(request)
    },
  })

  return { cluster, written, stamps }
}

describe('Producer > an idempotent send whose acknowledgement is lost', () => {
  test('the client retries the batch under the same producer id, epoch and sequence, so the broker writes it once', async () => {
    const { cluster, written, stamps } = createLossyCluster(1)
    const producer = createProducer({ ...options, cluster })
    await producer.connect()

    await producer.send({ topic: 'topic-a', messages: [{ value: 'event-1' }] })
    await producer.disconnect()

    expect(stamps).toEqual(['1000/0/0', '1000/0/0'])
    expect(written).toEqual(['event-1'])
  })

  test('after the retries run out the outcome is unknown, and sending the same records again writes them a second time', async () => {
    const { cluster, written, stamps } = createLossyCluster(2)
    const producer = createProducer({ ...options, cluster })
    await producer.connect()
    const request = { topic: 'topic-a', messages: [{ value: 'event-1' }] }

    await expect(producer.send(request)).rejects.toBeInstanceOf(KafkaJSNumberOfRetriesExceeded)
    await producer.send(request)
    await producer.disconnect()

    expect(stamps).toEqual(['1000/0/0', '1000/0/0', '1000/1/0'])
    expect(written).toEqual(['event-1', 'event-1'])
  })
})

describe('Producer > an idempotent send the broker rejects as a record problem', () => {
  test.each([
    ['MESSAGE_TOO_LARGE', MESSAGE_TOO_LARGE],
    ['INVALID_TIMESTAMP', INVALID_TIMESTAMP],
  ])(
    '%s fails the send once as invalidConfiguration, then the producer bumps its epoch and sends the corrected record',
    async (type, code) => {
      const stamps = []
      const cluster = createFakeCluster({
        produce: async (request) => {
          stamps.push(stampsOf(request))
          return answer(request, stamps.length === 1 ? code : 0)
        },
      })
      const producer = createProducer({ ...options, cluster })
      await producer.connect()

      const error = await producer
        .send({ topic: 'topic-a', messages: [{ value: 'too-large' }] })
        .catch((e) => e)

      expect(error).toBeInstanceOf(KafkaJSProtocolError)
      expect(error).toMatchObject({
        type,
        code,
        retriable: false,
        category: 'invalidConfiguration',
      })
      expect(stamps).toHaveLength(1)

      await expect(
        producer.send({ topic: 'topic-a', messages: [{ value: 'corrected' }] })
      ).resolves.toHaveLength(1)
      expect(stamps).toEqual(['1000/0/0', '1000/1/0'])
      await producer.disconnect()
    }
  )
})
