const createProducer = require('../index')
const { createFakeCluster } = require('./fixtures/fakeCluster')
const { newLogger } = require('testHelpers')

/** Valid Kafka topic names that are also properties every plain object inherits. */
const TOPICS = ['toString', 'constructor', '__proto__']

const MODES = {
  idempotent: () => ({ linger: 0, deliveryTimeout: 500, retry: { retries: 1 } }),
  'non-idempotent': () => ({ idempotent: false, linger: 0, retry: { retries: 0 } }),
}

const prototypeKeys = () => Reflect.ownKeys(Object.prototype)

describe.each(Object.keys(MODES))(
  'Producer > topic names that Object.prototype owns > %s',
  (mode) => {
    let before
    let cluster
    let producer

    beforeEach(async () => {
      before = prototypeKeys()
      cluster = createFakeCluster({ partitions: 2 })
      producer = createProducer({ cluster, logger: newLogger(), ...MODES[mode]() })
      await producer.connect()
    })

    afterEach(async () => {
      await producer.disconnect()
      for (const key of prototypeKeys()) {
        if (!before.includes(key)) {
          delete Object.prototype[key]
        }
      }
    })

    test.each(TOPICS)(
      'a send to the topic %s succeeds and leaves Object.prototype alone',
      async (topic) => {
        const result = await producer.send({ topic, messages: [{ value: 'x' }] })

        expect(result).toEqual([expect.objectContaining({ topicName: topic, errorCode: 0 })])
        expect(cluster.broker.produce).toHaveBeenCalledTimes(1)
        expect(prototypeKeys()).toEqual(before)
      }
    )

    test('one batch to all of them, and an ordinary topic, settles every topic', async () => {
      const topicMessages = [...TOPICS, 'topic-a'].map((topic) => ({
        topic,
        messages: [
          { value: 'x', partition: 1 },
          { value: 'y', partition: 0 },
        ],
      }))

      const result = await producer.sendBatch({ topicMessages })

      expect(result.map(({ topicName, partition }) => `${topicName}:${partition}`).sort()).toEqual(
        [...TOPICS, 'topic-a'].flatMap((topic) => [`${topic}:0`, `${topic}:1`]).sort()
      )
      expect(prototypeKeys()).toEqual(before)
    })
  }
)
