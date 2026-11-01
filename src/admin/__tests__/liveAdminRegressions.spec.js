const createAdmin = require('..')
const createProducer = require('../../producer')
const createConsumer = require('../../consumer')
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
