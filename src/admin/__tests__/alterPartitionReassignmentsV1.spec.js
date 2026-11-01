const createAdmin = require('..')
const {
  KafkaJSAggregateError,
  KafkaJSAlterPartitionReassignmentsError,
  KafkaJSInvalidConfigurationError,
} = require('../../errors')
const { createErrorFromCode } = require('../../protocol/error')
const logger = { namespace: () => logger, debug() {}, info() {}, warn() {}, error() {} }
const fixture = () => {
  const broker = { alterPartitionReassignments: jest.fn(async () => {}) }
  const cluster = { refreshMetadata: async () => {}, findControllerBroker: async () => broker }
  return {
    broker,
    admin: createAdmin({ cluster, logger, retry: { retries: 1, initialRetryTime: 1, factor: 0 } }),
  }
}
const topics = [
  {
    topic: 't',
    partitionAssignment: [
      { partition: 0, replicas: [1, 2] },
      { partition: 1, replicas: [1, 2] },
      { partition: 2, replicas: [1, 2] },
    ],
  },
]
describe('reassignment replication policy', () => {
  it.each([undefined, true, false])(
    'forwards default or explicit policy %p without changing the legacy shape',
    async (allowReplicationFactorChange) => {
      const { admin, broker } = fixture()
      expect(
        await admin.alterPartitionReassignments({ topics, allowReplicationFactorChange })
      ).toBeUndefined()
      expect(broker.alterPartitionReassignments).toHaveBeenCalledWith({
        topics,
        timeout: undefined,
        allowReplicationFactorChange: allowReplicationFactorChange ?? true,
      })
    }
  )
  it('retains terminal children, retries only retryable children, and never resends success', async () => {
    const { admin, broker } = fixture()
    const terminal = new KafkaJSAlterPartitionReassignmentsError(createErrorFromCode(39), 't', 0)
    const temporary = new KafkaJSAlterPartitionReassignmentsError(createErrorFromCode(7), 't', 1)
    broker.alterPartitionReassignments.mockRejectedValueOnce(
      new KafkaJSAggregateError('mixed', [terminal, temporary])
    )
    await expect(
      admin.alterPartitionReassignments({ topics, allowReplicationFactorChange: false })
    ).rejects.toMatchObject({ errors: [expect.objectContaining({ topic: 't', partition: 0 })] })
    expect(broker.alterPartitionReassignments.mock.calls[1][0]).toEqual({
      topics: [{ topic: 't', partitionAssignment: [{ partition: 1, replicas: [1, 2] }] }],
      timeout: undefined,
      allowReplicationFactorChange: false,
    })
    expect(topics[0].partitionAssignment).toHaveLength(3)
  })
  it.each([null, 0, 'false'])(
    'refuses invalid new policy %p before discovery',
    async (allowReplicationFactorChange) => {
      const { admin, broker } = fixture()
      await expect(
        admin.alterPartitionReassignments({ topics, allowReplicationFactorChange })
      ).rejects.toBeInstanceOf(KafkaJSInvalidConfigurationError)
      expect(broker.alterPartitionReassignments).not.toHaveBeenCalled()
    }
  )
})

describe('reassignment version capability', () => {
  it('refuses false on version zero before sending', async () => {
    const Broker = require('../../broker')
    const broker = Object.create(Broker.prototype)
    broker.versions = { 45: { minVersion: 0, maxVersion: 0 } }
    broker.lookupRequest = jest.fn()
    await expect(
      broker.alterPartitionReassignments({ topics, allowReplicationFactorChange: false })
    ).rejects.toMatchObject({
      name: 'KafkaJSServerDoesNotSupportApiKey',
      apiName: 'AlterPartitionReassignments',
      message: expect.stringMatching(/client implements v0-v1, broker supports v0-v0/),
    })
    expect(broker.lookupRequest).not.toHaveBeenCalled()
  })
})

describe('reassignment advertised downgrade', () => {
  it('refuses an unrepresentable option on the wire-negotiated range', async () => {
    const { Kafka } = require('../../..')
    const { createScriptedCluster } = require('../../../testHelpers/scriptedCluster')
    const scripted = createScriptedCluster({
      nodes: [{ id: 0, host: 'node0', port: 9092 }],
      profile: 'storage',
      advertise: { AlterPartitionReassignments: [0, 0] },
    })
    const admin = new Kafka(scripted.kafkaOptions({ retry: { retries: 0 } })).admin()
    try {
      await admin.connect()
      await expect(
        admin.alterPartitionReassignments({ topics, allowReplicationFactorChange: false })
      ).rejects.toMatchObject({ name: 'KafkaJSServerDoesNotSupportApiKey' })
      expect(
        scripted.requests().filter((r) => r.api === 'AlterPartitionReassignments')
      ).toHaveLength(0)
    } finally {
      await admin.disconnect()
      scripted.assertClean()
    }
  })
})

describe('reassignment response boolean', () => {
  it.each([
    [false, 0, '00000000000000000100'],
    [true, 0, '00000000010000000100'],
    [false, 41, '00000000000029000100'],
    [true, 41, '00000000010029000100'],
  ])(
    'decodes the schema golden flag %s code %i',
    async (allowReplicationFactorChange, errorCode, hex) => {
      const response = require('../../protocol/requests/alterPartitionReassignments/v1/response')
      const decoded = await response.decode(Buffer.from(hex, 'hex')).catch((error) => error)
      expect(decoded).toEqual({
        throttleTime: 0,
        allowReplicationFactorChange,
        errorCode,
        responses: [],
      })
      if (errorCode)
        await expect(response.parse(decoded)).rejects.toMatchObject({ code: errorCode })
      else expect(await response.parse(decoded)).toEqual(decoded)
    }
  )
})
