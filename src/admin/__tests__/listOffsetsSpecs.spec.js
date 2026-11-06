const createAdmin = require('..')
const Broker = require('../../broker')
const { lookup } = require('../../protocol/requests')
const { KafkaJSInvalidConfigurationError } = require('../../errors')
const logger = { namespace: () => logger, debug() {}, info() {}, warn() {}, error() {} }
const fixture = () => {
  const read = jest.fn(async (topics) =>
    topics.map((t) => ({
      topic: t.topic,
      partitions: t.partitions.map((p) => ({
        partition: p.partition,
        offset: t.fromTimestamp === undefined ? '100' : '-1',
      })),
    }))
  )
  const cluster = {
    addTargetTopic: jest.fn(async () => {}),
    refreshMetadataIfNecessary: async () => {},
    findTopicPartitionMetadata: () => [{ partitionId: 0 }],
    fetchTopicsOffset: read,
  }
  return { cluster, read, admin: createAdmin({ cluster, logger, retry: { retries: 0 } }) }
}
/** Creates a real Broker method harness with negotiated builders and an independent body reader.
 * @param {number} max broker maximum @returns {object} broker and request records
 */
const brokerFixture = (max = 11) => {
  const broker = Object.create(Broker.prototype)
  broker.versions = { 2: { minVersion: 1, maxVersion: max } }
  broker.lookupRequest = lookup(broker.versions)
  broker.lookupRequestWithMax = (key, definition, maximum) =>
    lookup(broker.versions, { [key]: maximum })(key, definition)
  const send = Object.getOwnPropertySymbols(Broker.prototype).find((s) =>
    String(s).includes('sendRequest')
  )
  const sent = []
  broker[send] = async ({ request }) => {
    sent.push({ version: request.apiVersion, body: (await request.encode()).buffer })
    return { responses: [] }
  }
  return { broker, sent }
}
describe('offset specifications', () => {
  it.each([
    ['maxTimestamp', -3, 7],
    ['earliestLocal', -4, 8],
    ['latestTiered', -5, 9],
    ['earliestPendingUpload', -6, 11],
  ])('sends %s losslessly and preserves a negative raw answer', async (spec, value, minVersion) => {
    const { admin, read } = fixture()
    expect(
      await admin.fetchTopicOffsetsByTimestamp('t', spec, { isolationLevel: 'read_uncommitted' })
    ).toEqual([{ partition: 0, offset: '-1' }])
    expect(read).toHaveBeenCalledTimes(1)
    expect(read.mock.calls[0]).toEqual([
      [{ topic: 't', fromTimestamp: value, partitions: [{ partition: 0 }] }],
      { isolationLevel: 0, minVersion, maxVersion: 11, timeout: 5000 },
    ])
  })
  it('retains numeric high-before-timestamp fallback and default isolation', async () => {
    const { admin, read } = fixture()
    expect(await admin.fetchTopicOffsetsByTimestamp('t', 123)).toEqual([
      { partition: 0, offset: '100' },
    ])
    expect(read).toHaveBeenCalledTimes(2)
    expect(read.mock.calls.map((x) => x[1])).toEqual([{ isolationLevel: 1 }, { isolationLevel: 1 }])
  })
  it.each(['MAX_TIMESTAMP', '', 'earliest', 'constructor', '123abc'])(
    'refuses unknown spec before discovery %p',
    async (spec) => {
      const { admin, cluster } = fixture()
      await expect(admin.fetchTopicOffsetsByTimestamp('t', spec)).rejects.toBeInstanceOf(
        KafkaJSInvalidConfigurationError
      )
      expect(cluster.addTargetTopic).not.toHaveBeenCalled()
    }
  )
  it('keeps the consumer and numeric request version at nine', async () => {
    const { broker, sent } = brokerFixture()
    await broker.listOffsets({ topics: [] })
    expect(sent[0].version).toBe(9)
    expect(sent[0].body.toString('hex')).toBe('ffffffff010100')
  })
  it('selects eleven only on the explicit admin path and carries timeout', async () => {
    const { broker, sent } = brokerFixture()
    await broker.listOffsets({ topics: [], minVersion: 11, maxVersion: 11, timeout: 2345 })
    expect(sent[0].version).toBe(11)
    expect(sent[0].body.toString('hex')).toBe('ffffffff01010000092900')
  })
  it.each([
    [6, 7],
    [7, 8],
    [8, 9],
    [10, 11],
  ])('refuses broker v%i below need v%i before send', async (max, minVersion) => {
    const { broker, sent } = brokerFixture(max)
    await expect(
      broker.listOffsets({ topics: [], minVersion, maxVersion: 11 })
    ).rejects.toMatchObject({ name: 'KafkaJSServerDoesNotSupportApiKey' })
    expect(sent).toEqual([])
  })
})

describe('offset advertised downgrade', () => {
  it('refuses an unrepresentable option on the wire-negotiated range', async () => {
    const { Kafka } = require('../../..')
    const { createScriptedCluster } = require('../../../testHelpers/scriptedCluster')
    const scripted = createScriptedCluster({
      nodes: [{ id: 0, host: 'node0', port: 9092 }],
      profile: 'storage',
      advertise: { ListOffsets: [10, 10] },
    })
    scripted.answer('Metadata', () => ({
      brokers: [{ nodeId: 0, host: 'node0', port: 9092, rack: null }],
      controllerId: 0,
      topics: [
        {
          errorCode: 0,
          name: 'test-topic',
          topicId: '00112233-4455-6677-8899-aabbccddeeff',
          isInternal: false,
          topicAuthorizedOperations: -2147483648,
          partitions: [
            {
              errorCode: 0,
              partitionIndex: 0,
              leaderId: 0,
              leaderEpoch: 1,
              replicas: [0],
              isr: [0],
              offlineReplicas: [],
            },
          ],
        },
      ],
    }))
    const admin = new Kafka(scripted.kafkaOptions({ retry: { retries: 0 } })).admin()
    try {
      await admin.connect()
      await expect(
        admin.fetchTopicOffsetsByTimestamp('test-topic', 'earliestPendingUpload')
      ).rejects.toMatchObject({ name: 'KafkaJSServerDoesNotSupportApiKey' })
      expect(scripted.requests().filter((r) => r.api === 'ListOffsets')).toHaveLength(0)
    } finally {
      await admin.disconnect()
      scripted.assertClean()
    }
  })
})
