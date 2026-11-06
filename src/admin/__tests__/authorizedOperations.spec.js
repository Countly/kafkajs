const { createGroupDescriptions } = require('../groupDescriptions')
const createAdmin = require('..')
const Metadata = require('../../protocol/requests/metadata')
const { createErrorFromCode } = require('../../protocol/error')
const logger = { namespace: () => logger, debug() {}, info() {}, warn() {}, error() {} }

describe('authorized operations', () => {
  it.each([false, true])(
    'reuses five located coordinators across classic fallback with requested=%p',
    async (requested) => {
      const targets = [0, 1].map((nodeId) => ({
        nodeId,
        consumerGroupDescribe: jest.fn(async ({ groupIds }) => ({
          groups: groupIds.map((groupId) => ({ groupId, errorCode: 69, errorMessage: 'classic' })),
        })),
        describeGroups: jest.fn(async ({ groupIds, includeAuthorizedOperations }) => ({
          groups: groupIds.map((groupId) => ({
            groupId,
            errorCode: 0,
            protocolType: 'consumer',
            members: [],
            authorizedOperations: includeAuthorizedOperations ? -2147483647 : -2147483648,
          })),
        })),
      }))
      const cluster = {
        findGroupCoordinator: jest.fn(async ({ groupId }) => targets[Number(groupId.slice(1)) % 2]),
      }
      const ids = ['g0', 'g1', 'g2', 'g3', 'g4']
      const { groups } = await createGroupDescriptions({
        cluster,
        logger,
        retry: { retries: 0 },
      }).describe(ids, { includeAuthorizedOperations: requested })
      expect(cluster.findGroupCoordinator).toHaveBeenCalledTimes(5)
      expect(groups).toHaveLength(5)
      expect(
        groups.every((row) => row.authorizedOperations === (requested ? -2147483647 : null))
      ).toBe(true)
      for (const target of targets) {
        expect(target.consumerGroupDescribe).toHaveBeenCalledWith(
          expect.objectContaining({ includeAuthorizedOperations: requested })
        )
        expect(target.describeGroups).toHaveBeenCalledWith(
          expect.objectContaining({ includeAuthorizedOperations: requested })
        )
      }
    }
  )

  it.each([false, true])('projects group masks only when requested=%p', async (requested) => {
    const masks = [undefined, -2147483648, 0, 8, -2147483647, 2147483647, -1]
    for (const classic of [false, true]) {
      const coordinator = {
        nodeId: 0,
        consumerGroupDescribe: jest.fn(async ({ groupIds }) => ({
          groups: groupIds.map((groupId, index) => ({
            groupId,
            errorCode: classic ? 69 : 0,
            members: [],
            authorizedOperations: masks[index],
          })),
        })),
        describeGroups: jest.fn(async ({ groupIds }) => ({
          groups: groupIds.map((groupId, index) => ({
            groupId,
            errorCode: 0,
            members: [],
            protocolType: 'consumer',
            authorizedOperations: masks[index],
          })),
        })),
      }
      const cluster = { findGroupCoordinator: jest.fn(async () => coordinator) }
      const descriptions = createGroupDescriptions({ cluster, logger })
      const ids = masks.map((_, index) => `g${index}`)
      const { groups } = await descriptions.describe(ids, {
        includeAuthorizedOperations: requested,
      })
      expect(groups.map(({ authorizedOperations }) => authorizedOperations)).toEqual(
        masks.map((mask) => (!requested || mask == null || mask === -2147483648 ? null : mask))
      )
      await descriptions.describe(ids)
      expect(cluster.findGroupCoordinator).toHaveBeenCalledTimes(ids.length * 2)
      expect(coordinator.consumerGroupDescribe).toHaveBeenLastCalledWith(
        expect.objectContaining({ includeAuthorizedOperations: false })
      )
    }
  })

  it.each([null, 1, 'true', {}])('rejects invalid flags %p before discovery', async (flag) => {
    const cluster = { findGroupCoordinator: jest.fn(), metadata: jest.fn() }
    await expect(
      createGroupDescriptions({ cluster, logger }).describe(['g'], {
        includeAuthorizedOperations: flag,
      })
    ).rejects.toMatchObject({ name: 'KafkaJSInvalidConfigurationError' })
    await expect(
      createAdmin({ cluster, logger }).fetchTopicMetadata({
        includeTopicAuthorizedOperations: flag,
      })
    ).rejects.toMatchObject({ name: 'KafkaJSInvalidConfigurationError' })
    expect(cluster.findGroupCoordinator).not.toHaveBeenCalled()
    expect(cluster.metadata).not.toHaveBeenCalled()
  })

  it('relocates a moved classic coordinator exactly once', async () => {
    const replacement = {
      nodeId: 1,
      describeGroups: jest.fn(async () => ({
        groups: [{ groupId: 'g', errorCode: 0, protocolType: 'consumer', members: [] }],
      })),
    }
    const original = {
      nodeId: 0,
      consumerGroupDescribe: async () => ({ groups: [{ groupId: 'g', errorCode: 69 }] }),
      describeGroups: jest.fn(async () => {
        throw createErrorFromCode(16)
      }),
    }
    const cluster = {
      findGroupCoordinator: jest
        .fn()
        .mockResolvedValueOnce(original)
        .mockResolvedValueOnce(replacement),
    }
    const { groups } = await createGroupDescriptions({
      cluster,
      logger,
      retry: { retries: 1, initialRetryTime: 1, factor: 0 },
    }).describe(['g'])
    expect(groups[0].groupId).toBe('g')
    expect(cluster.findGroupCoordinator).toHaveBeenCalledTimes(2)
    expect(original.describeGroups).toHaveBeenCalledTimes(1)
    expect(replacement.describeGroups).toHaveBeenCalledTimes(1)
  })

  it.each([undefined, -2147483648, 0, -2147483647, 2147483647])(
    'normalizes topic mask %p only at the absent sentinel',
    async (mask) => {
      const cluster = {
        metadata: jest.fn(async () => ({
          topicMetadata: [{ topic: 't', partitionMetadata: [], topicAuthorizedOperations: mask }],
        })),
      }
      const admin = createAdmin({ cluster, logger })
      const result = await admin.fetchTopicMetadata({ includeTopicAuthorizedOperations: true })
      expect(result.topics[0].authorizedOperations).toBe(
        mask == null || mask === -2147483648 ? null : mask
      )
      expect(cluster.metadata).toHaveBeenCalledWith({
        topics: [],
        includeTopicAuthorizedOperations: true,
      })
    }
  )

  it.each([8, 9, 10, 11, 12, 13])(
    'forwards the topic flag through Metadata v%i builders',
    async (version) => {
      const built = Metadata.protocol({ version })({
        topics: ['t'],
        includeTopicAuthorizedOperations: true,
      })
      const bytes = (await built.request.encode()).buffer
      const offset = version < 9 ? bytes.length - 1 : bytes.length - 2
      expect(bytes[offset]).toBe(1)
    }
  )

  it('keeps Metadata v7 encodable when the topic mask is requested', async () => {
    const built = Metadata.protocol({ version: 7 })({
      topics: ['t'],
      includeTopicAuthorizedOperations: true,
    })
    expect((await built.request.encode()).buffer.toString('hex')).toBe('0000000100017401')
  })

  it('keeps an explicit consumer operation and its timeout identity', async () => {
    const error = new Error('consumer operation retired')
    const operation = {
      check: jest.fn(() => {
        throw error
      }),
      wait: jest.fn(async (work) => {
        operation.check()
        return work()
      }),
      refusal: () => error,
      retry: jest.fn(),
      register: jest.fn(() => () => {}),
    }
    const cluster = { findGroupCoordinator: jest.fn() }
    await expect(
      createGroupDescriptions({ cluster, logger }).describe(['g'], { operation })
    ).rejects.toBe(error)
    expect(operation.wait).toHaveBeenCalled()
    expect(cluster.findGroupCoordinator).not.toHaveBeenCalled()
  })
  it('completes a topic-mask request on a Metadata-v7-only scripted cluster', async () => {
    const { Kafka } = require('../../..')
    const { createScriptedCluster } = require('../../../testHelpers/scriptedCluster')
    const scripted = createScriptedCluster({
      nodes: [{ id: 0, host: 'broker', port: 9092 }],
      advertise: { Metadata: [7, 7] },
    })
    scripted.answer('Metadata', () => ({
      brokers: scripted.nodes.map(({ id, host, port }) => ({ nodeId: id, host, port, rack: null })),
      topics: [{ name: 't', errorCode: 0, isInternal: false, partitions: [] }],
    }))
    const admin = new Kafka(scripted.kafkaOptions()).admin({ apiTimeout: 1000 })
    try {
      await admin.connect()
      const { topics } = await admin.fetchTopicMetadata({
        topics: ['t'],
        includeTopicAuthorizedOperations: true,
      })
      expect(topics[0]).toMatchObject({ name: 't', authorizedOperations: null })
      expect(scripted.requests().filter((row) => row.api === 'Metadata').length).toBeGreaterThan(0)
      expect(
        scripted
          .requests()
          .filter((row) => row.api === 'Metadata')
          .every((row) => row.version === 7)
      ).toBe(true)
      scripted.assertClean()
    } finally {
      await admin.disconnect()
      scripted.assertClean()
      expect(scripted.sockets().every((socket) => socket.destroyed)).toBe(true)
    }
  })
})
