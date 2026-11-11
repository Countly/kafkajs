const createAdmin = require('..')
const createProducer = require('../../producer')
const createConsumer = require('../../consumer')
const { KafkaJSDeleteTopicRecordsError, KafkaJSNonRetriableError } = require('../../errors')
const { createErrorFromCode } = require('../../protocol/error')
const { createCluster, newLogger } = require('testHelpers')
const { createScriptedCluster } = require('../../../testHelpers/scriptedCluster')

describe('legacy admin call patterns', () => {
  it.each([false, true])(
    'connects clients sharing a fresh cluster (consumer: %s)',
    async (withConsumer) => {
      const scripted = createScriptedCluster({
        nodes: [{ id: 0, host: 'node0', port: 9092 }],
        profile: 'storage',
      })
      const cluster = createCluster(scripted.kafkaOptions({ retry: { retries: 0 } }), [
        'node0:9092',
      ])
      const logger = newLogger()
      const clients = [
        createAdmin({ cluster, logger }),
        createProducer({ cluster, logger, idempotent: false }),
      ]
      if (withConsumer) clients.push(createConsumer({ cluster, logger, groupId: 'g' }))
      try {
        const outcomes = await Promise.allSettled(clients.map((client) => client.connect()))
        expect(outcomes).toEqual(clients.map(() => ({ status: 'fulfilled', value: undefined })))
        expect(cluster.brokerPool.seedBroker.isConnected()).toBe(true)
      } finally {
        for (const client of clients) await client.disconnect()
        scripted.assertClean()
      }
    }
  )

  it.each([1700000000123, '1700000000123', '123', '-1', '-2'])(
    'retains high-first fallback for timestamp %p',
    async (timestamp) => {
      const reads = []
      const cluster = {
        addTargetTopic: async () => {},
        refreshMetadataIfNecessary: async () => {},
        findTopicPartitionMetadata: () => [{ partitionId: 0 }],
        fetchTopicsOffset: async (topics, options) => {
          reads.push({ topics, options })
          return [
            {
              topic: 't',
              partitions: [{ partition: 0, offset: reads.length === 1 ? '10' : '-1' }],
            },
          ]
        },
      }
      const admin = createAdmin({ cluster, logger: newLogger(), retry: { retries: 0 } })
      await expect(admin.fetchTopicOffsetsByTimestamp('t', timestamp)).resolves.toEqual([
        { partition: 0, offset: '10' },
      ])
      expect(reads).toEqual([
        {
          topics: [{ topic: 't', fromBeginning: false, partitions: [{ partition: 0 }] }],
          options: { isolationLevel: 1 },
        },
        {
          topics: [{ topic: 't', fromTimestamp: timestamp, partitions: [{ partition: 0 }] }],
          options: { isolationLevel: 1 },
        },
      ])
    }
  )
})

describe('delete-records partial broker retries', () => {
  it('keeps successful brokers out of retries and reports only the refused partition', async () => {
    const topic = 't'
    const brokers = [0, 1].map((nodeId) => ({
      deleteRecords: jest.fn(async () => {
        if (nodeId === 1) {
          throw new KafkaJSDeleteTopicRecordsError({
            topic,
            partitions: [
              {
                partition: 1,
                offset: '5',
                error: createErrorFromCode(
                  brokers[1].deleteRecords.mock.calls.length === 1 ? 6 : 1
                ),
              },
            ],
          })
        }
      }),
    }))
    const cluster = {
      addTargetTopic: async () => {},
      refreshMetadataIfNecessary: async () => {},
      refreshMetadata: jest.fn(async () => {}),
      findTopicPartitionMetadata: () => [{ partitionId: 0 }, { partitionId: 1 }],
      fetchTopicsOffset: jest.fn(async () => [
        {
          topic,
          partitions: [
            { partition: 0, offset: '0' },
            { partition: 1, offset: '0' },
          ],
        },
      ]),
      findLeaderForPartitions: () => ({ 0: [0], 1: [1] }),
      findBroker: async ({ nodeId }) => brokers[Number(nodeId)],
    }
    const admin = createAdmin({
      cluster,
      logger: newLogger(),
      retry: { retries: 1, initialRetryTime: 1, factor: 0 },
    })
    const error = await admin
      .deleteTopicRecords({
        topic,
        partitions: [
          { partition: 0, offset: '5' },
          { partition: 1, offset: '5' },
        ],
      })
      .catch((error) => error)
    expect(error).toBeInstanceOf(KafkaJSNonRetriableError)
    expect(error.cause).toBeInstanceOf(KafkaJSDeleteTopicRecordsError)
    expect(error.cause.partitions).toEqual([
      { partition: 1, offset: '5', error: expect.objectContaining({ code: 1 }) },
    ])
    expect(brokers[0].deleteRecords).toHaveBeenCalledTimes(1)
    expect(brokers[1].deleteRecords).toHaveBeenCalledTimes(2)
    expect(cluster.refreshMetadata).toHaveBeenCalledTimes(1)
  })
})

describe('recorded broker version regressions', () => {
  it('selects ListOffsets v10 from the recorded 4.0.2 range', () => {
    const { requests, selectVersion } = require('../../protocol/requests')
    const apiKeys = require('../../protocol/requests/apiKeys')
    const ranges = require('../../../scripts/protocol/brokerRanges.json')
    const [minVersion, maxVersion] = ranges['4.0.2'].ListOffsets
    expect([minVersion, maxVersion]).toEqual([1, 10])
    expect(
      selectVersion(
        { [apiKeys.ListOffsets]: { minVersion, maxVersion } },
        apiKeys.ListOffsets,
        requests.ListOffsets
      )
    ).toBe(10)
  })

  it('decodes the successful reassignment v1 response with the broker policy', async () => {
    const response = require('../../protocol/requests/alterPartitionReassignments/v1/response')
    const golden = require('../../protocol/requests/alterPartitionReassignments/fixtures/java/alterPartitionReassignmentsResponse-v1-storage.json')
    const raw = Buffer.from(golden.hex, 'hex')
    raw.writeInt32BE(0, 0)
    raw.writeUInt8(1, 4)
    const decoded = await response.parse(await response.decode(raw))
    expect(decoded).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 0,
      allowReplicationFactorChange: true,
      errorCode: 0,
      responses: [{ topic: 't', partitions: [{ partition: 0, errorCode: 0 }] }],
    })
  })
})
