const { Kafka } = require('../../../index')
const Broker = require('../../broker')
const { KafkaJSProtocolError, KafkaJSNonRetriableError } = require('../../errors')
const { createConnectionPool, newLogger } = require('testHelpers')
const { createScriptedCluster, groupScript } = require('../../../testHelpers/scriptedCluster')
const { prepareRecordBatches, TOPIC } = require('./fixtures/fetchHarness')
const { waitForCondition } = require('./fixtures/liveScenario')

const OFFSET_METADATA_TOO_LARGE = 12
const METADATA = 'm'.repeat(4097)
const NODES = [{ id: 0, host: 'node0', port: 9092 }]

/** One OffsetCommit answer: each `[partition, errorCode]` pair of `topic`. */
const commitAnswer = (topic, partitions) => ({
  throttleTime: 0,
  topics: [
    {
      topic,
      partitions: partitions.map(([partition, errorCode]) => ({ partition, errorCode })),
    },
  ],
})

const commitsOf = (cluster) => cluster.requests().filter(({ api }) => api === 'OffsetCommit')

/** Whether an OffsetCommit is the one the test sent, carrying the large metadata. */
const carriesMetadata = ({ fields }) => fields?.topics[0].partitions[0].metadata === METADATA

describe('Consumer > a commit refused with OFFSET_METADATA_TOO_LARGE', () => {
  let cluster, broker, consumer

  beforeAll(() => prepareRecordBatches())

  afterEach(async () => {
    await broker?.disconnect()
    await consumer?.disconnect()
    cluster?.assertClean()
    cluster = broker = consumer = undefined
  })

  test('is a typed, non-retriable protocol error naming the refused partition', async () => {
    cluster = createScriptedCluster({ nodes: NODES })
    cluster.schedule.once(
      { api: 'OffsetCommit' },
      commitAnswer('topic-a', [
        [0, 0],
        [1, OFFSET_METADATA_TOO_LARGE],
      ])
    )
    broker = new Broker({
      connectionPool: createConnectionPool({
        host: 'node0',
        port: 9092,
        socketFactory: cluster.socketFactory,
      }),
      logger: newLogger(),
    })
    await broker.connect()

    const error = await broker
      .offsetCommit({
        groupId: 'group',
        groupGenerationId: 1,
        memberId: 'member',
        topics: [
          {
            topic: 'topic-a',
            partitions: [
              { partition: 0, offset: '1' },
              { partition: 1, offset: '2', metadata: METADATA },
            ],
          },
        ],
      })
      .catch((e) => e)

    expect(error).toBeInstanceOf(KafkaJSProtocolError)
    expect(error).toMatchObject({
      type: 'OFFSET_METADATA_TOO_LARGE',
      code: OFFSET_METADATA_TOO_LARGE,
      retriable: false,
      topic: 'topic-a',
      partition: 1,
    })
    expect(commitsOf(cluster).map(({ fields }) => fields.topics[0].partitions[1].metadata)).toEqual(
      [METADATA]
    )
  })

  test('ends consumer.commitOffsets at once, with the partition on its cause', async () => {
    cluster = groupScript(createScriptedCluster({ nodes: NODES }))
    consumer = new Kafka(cluster.kafkaOptions()).consumer({
      groupId: 'metadata-group',
      autoCommit: false,
      heartbeatInterval: 100,
      maxWaitTimeInMs: 20,
    })
    const handled = jest.fn()
    await consumer.connect()
    await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
    await consumer.run({ eachMessage: async () => handled() })
    expect(
      await waitForCondition(() => handled.mock.calls.length > 0, { attempts: 400, delay: 5 })
    ).toBe(true)
    cluster.schedule.once(
      { api: 'OffsetCommit', match: carriesMetadata },
      commitAnswer(TOPIC, [[0, OFFSET_METADATA_TOO_LARGE]])
    )

    const error = await consumer
      .commitOffsets([{ topic: TOPIC, partition: 0, offset: '1', metadata: METADATA }])
      .catch((e) => e)

    expect(error).toBeInstanceOf(KafkaJSNonRetriableError)
    expect(error).toMatchObject({
      type: 'OFFSET_METADATA_TOO_LARGE',
      code: OFFSET_METADATA_TOO_LARGE,
      retriable: false,
    })
    expect(error.cause).toBeInstanceOf(KafkaJSProtocolError)
    expect(error.cause).toMatchObject({ topic: TOPIC, partition: 0 })
    expect(commitsOf(cluster).filter(carriesMetadata)).toHaveLength(1)
  })
})
