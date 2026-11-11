const createAdmin = require('..')
const logger = { namespace: () => logger, debug() {}, info() {}, warn() {}, error() {} }
const methods = [
  ['createTopics', 'createTopics/v2/request', { topics: [{ topic: 't' }] }, { topicErrors: [] }],
  [
    'createPartitions',
    'createPartitions/v0/request',
    { topicPartitions: [{ topic: 't', count: 2 }] },
    { topicErrors: [] },
  ],
  [
    'alterConfigs',
    'alterConfigs/v0/request',
    { resources: [{ type: 2, name: 't', configEntries: [] }] },
    { resources: [] },
  ],
  [
    'incrementalAlterConfigs',
    'incrementalAlterConfigs/v0/request',
    { resources: [{ type: 2, name: 't', configEntries: [] }] },
    { resources: [{ resourceType: 2, resourceName: 't', errorCode: 0, errorMessage: null }] },
  ],
]
describe('validation-only admin requests', () => {
  it.each(methods)(
    '%s sends its flag without follow-up work',
    async (method, codec, options, response) => {
      const bytes = []
      const cache = new Map([['existing', 'unchanged']])
      const metadata = jest.fn(async () => ({ topicMetadata: [] }))
      const broker = {
        metadata,
        [method]: jest.fn(async (request) => {
          bytes.push((await require('../../protocol/requests/' + codec)(request).encode()).buffer)
          return response
        }),
      }
      const cluster = {
        refreshMetadata: jest.fn(async () => {}),
        findControllerBroker: async () => broker,
        metadataCache: cache,
      }
      const admin = createAdmin({ cluster, logger, retry: { retries: 0 } })
      await admin[method]({ ...options, validateOnly: true })
      expect(bytes).toHaveLength(1)
      expect(bytes[0][bytes[0].length - 1]).toBe(1)
      expect(broker[method]).toHaveBeenCalledTimes(1)
      expect(metadata).not.toHaveBeenCalled()
      expect([...cache]).toEqual([['existing', 'unchanged']])
    }
  )
})

const fs = require('node:fs')
const path = require('node:path')
const { readSpec } = require('../../../scripts/compat/parse')
const { kafkaTag } = require('../../../scripts/compat/config.json')
const messageDirectory = path.join(__dirname, '../../../scripts/compat/messages', kafkaTag)
const withValidation = {
  createTopics: ['CreateTopicsRequest', '1+'],
  createPartitions: ['CreatePartitionsRequest', '0+'],
  alterConfigs: ['AlterConfigsRequest', '0+'],
  incrementalAlterConfigs: ['IncrementalAlterConfigsRequest', '0+'],
  alterClientQuotas: ['AlterClientQuotasRequest', '0+'],
  updateFeatures: ['UpdateFeaturesRequest', '1+'],
}
const withoutValidation = {
  connect: [[], 'lifecycle'],
  disconnect: [[], 'lifecycle'],
  logger: [[], 'diagnostics'],
  on: [[], 'instrumentation'],
  deleteTopics: [['DeleteTopics'], 'no wire validation form'],
  deleteTopicRecords: [['DeleteRecords'], 'no wire validation form'],
  deleteGroups: [['DeleteGroups'], 'no wire validation form'],
  deleteOffsets: [['OffsetDelete'], 'no wire validation form'],
  setOffsets: [['OffsetCommit'], 'commit helper has no validation form'],
  resetOffsets: [
    ['OffsetCommit', 'ListOffsets', 'OffsetFetch'],
    'commit helper has no validation form',
  ],
  createAcls: [['CreateAcls'], 'no wire validation form'],
  deleteAcls: [['DeleteAcls'], 'no wire validation form'],
  removeMembersFromConsumerGroup: [['LeaveGroup'], 'no wire validation form'],
  alterPartitionReassignments: [['AlterPartitionReassignments'], 'no wire validation form'],
  electLeaders: [['ElectLeaders'], 'no wire validation form'],
  unregisterController: [['UnregisterController'], 'no wire validation form'],
  abortTransaction: [['WriteTxnMarkers'], 'no wire validation form'],
  fenceProducers: [['InitProducerId', 'FindCoordinator'], 'no wire validation form'],
  alterReplicaLogDirs: [['AlterReplicaLogDirs'], 'no wire validation form'],
  alterUserScramCredentials: [['AlterUserScramCredentials'], 'no wire validation form'],
  createDelegationToken: [['CreateDelegationToken'], 'no wire validation form'],
  renewDelegationToken: [['RenewDelegationToken'], 'no wire validation form'],
  expireDelegationToken: [['ExpireDelegationToken'], 'no wire validation form'],
  listTopics: [['Metadata'], 'read-only'],
  fetchTopicMetadata: [['Metadata'], 'read-only'],
  fetchOffsets: [['OffsetFetch'], 'read-only'],
  fetchOffsetsForGroups: [['OffsetFetch'], 'read-only'],
  fetchTopicOffsets: [['ListOffsets'], 'read-only'],
  fetchTopicOffsetsByTimestamp: [['ListOffsets'], 'read-only'],
  describeConfigs: [['DescribeConfigs'], 'read-only'],
  listGroups: [['ListGroups'], 'read-only'],
  describeGroups: [['DescribeGroups', 'ConsumerGroupDescribe'], 'read-only'],
  describeShareGroups: [['ShareGroupDescribe'], 'read-only'],
  listShareGroupOffsets: [['DescribeShareGroupOffsets'], 'read-only'],
  alterShareGroupOffsets: [['AlterShareGroupOffsets'], 'no wire validation form'],
  deleteShareGroupOffsets: [['DeleteShareGroupOffsets'], 'no wire validation form'],
  describeAcls: [['DescribeAcls'], 'read-only'],
  listPartitionReassignments: [['ListPartitionReassignments'], 'read-only'],
  describeProducers: [['DescribeProducers'], 'read-only'],
  describeTransactions: [['DescribeTransactions'], 'read-only'],
  listTransactions: [['ListTransactions'], 'read-only'],
  describeCluster: [['Metadata', 'DescribeCluster'], 'read-only'],
  describeFeatures: [['ApiVersions'], 'read-only'],
  listConfigResources: [['ListConfigResources'], 'read-only'],
  describeLogDirs: [['DescribeLogDirs'], 'read-only'],
  describeReplicaLogDirs: [['DescribeLogDirs'], 'read-only projection'],
  describeClientQuotas: [['DescribeClientQuotas'], 'read-only'],
  describeUserScramCredentials: [['DescribeUserScramCredentials'], 'read-only'],
  describeDelegationToken: [['DescribeDelegationToken'], 'read-only'],
}

describe('complete validation-only ledger', () => {
  test('exactly six wire requests carry a versioned validation flag', () => {
    const census = fs
      .readdirSync(messageDirectory)
      .filter((file) => file.endsWith('Request.json'))
      .map((file) => readSpec(path.join(messageDirectory, file)))
      .flatMap((spec) =>
        spec.fields
          .filter((field) => field.path.split('.').at(-1).toLowerCase() === 'validateonly')
          .map((field) => [spec.name, field.versions, field.type])
      )
    expect(census.sort()).toEqual(
      Object.values(withValidation)
        .map(([request, versions]) => [request, versions, 'bool'])
        .sort()
    )
  })
  test('every public admin method has a schema-backed disposition', () => {
    const admin = createAdmin({ cluster: {}, logger })
    const actual = Object.keys(admin)
      .filter((key) => typeof admin[key] === 'function')
      .sort()
    expect(actual).toEqual(
      [...Object.keys(withValidation), ...Object.keys(withoutValidation)].sort()
    )
    for (const [requests, reason] of Object.values(withoutValidation)) {
      expect(reason.length).toBeGreaterThan(0)
      for (const request of requests) {
        const spec = readSpec(path.join(messageDirectory, request + 'Request.json'))
        expect(
          spec.fields.some((field) => field.path.split('.').at(-1).toLowerCase() === 'validateonly')
        ).toBe(false)
      }
    }
  })
  test('quota validation-only sends one flag byte without cache or follow-up work', async () => {
    const bodies = []
    const cache = new Map([['existing', 'unchanged']])
    const refreshMetadata = jest.fn()
    const metadata = jest.fn()
    const broker = {
      alterClientQuotas: jest.fn(async (fields) => {
        bodies.push(
          (await require('../../protocol/requests/alterClientQuotas/v0/request')(fields).encode())
            .buffer
        )
        return {
          entries: fields.entries.map(({ entity }) => ({
            entity,
            errorCode: 0,
            errorMessage: null,
          })),
        }
      }),
    }
    const cluster = {
      findConnectedBroker: jest.fn(async () => broker),
      refreshMetadata,
      metadata,
      metadataCache: cache,
    }
    const admin = createAdmin({ cluster, logger, retry: { retries: 0 } })
    await admin.alterClientQuotas({
      entries: [
        {
          entity: [{ entityType: 'user', entityName: null }],
          ops: [{ key: 'producer_byte_rate', value: 1234.5 }],
        },
      ],
      validateOnly: true,
    })
    expect(bodies).toHaveLength(1)
    expect(bodies[0].at(-1)).toBe(1)
    expect(broker.alterClientQuotas).toHaveBeenCalledTimes(1)
    expect(refreshMetadata).not.toHaveBeenCalled()
    expect(metadata).not.toHaveBeenCalled()
    expect([...cache]).toEqual([['existing', 'unchanged']])
  })
})

test('updateFeatures sends validation flag without follow-up work', async () => {
  const bytes = []
  const cache = new Map([['metadata.version', 1]])
  const broker = {
    updateFeatures: jest.fn(async (fields) => {
      const featureUpdates = fields.featureUpdates.map((f) => ({ ...f, upgradeType: 1 }))
      bytes.push(
        (
          await require('../../protocol/requests/updateFeatures/v1/request')({
            ...fields,
            featureUpdates,
          }).encode()
        ).buffer
      )
      return { results: [{ feature: 'metadata.version', errorCode: 0, errorMessage: null }] }
    }),
  }
  const metadata = jest.fn(async () => ({ topicMetadata: [] }))
  const cluster = {
    refreshMetadataIfNecessary: jest.fn(async () => {}),
    refreshMetadata: jest.fn(async () => {}),
    findControllerBroker: async () => broker,
    metadata,
    metadataCache: cache,
  }
  const admin = createAdmin({ cluster, logger, retry: { retries: 0 } })
  await admin.updateFeatures({
    features: [{ name: 'metadata.version', maxVersionLevel: 25 }],
    validateOnly: true,
  })
  expect(bytes).toHaveLength(1)
  const fields = require('../../../testHelpers/wire/updateFeaturesV1').readRequest(bytes[0])
  expect(fields.validateOnly).toBe(true)
  expect(broker.updateFeatures).toHaveBeenCalledTimes(1)
  expect(metadata).not.toHaveBeenCalled()
  expect([...cache]).toEqual([['metadata.version', 1]])
})
