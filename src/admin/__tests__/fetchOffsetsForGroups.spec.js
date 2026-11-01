const createAdmin = require('..')
const { KafkaJSInvariantViolation, KafkaJSInvalidConfigurationError } = require('../../errors')
const logger = { namespace: () => logger, debug() {}, info() {}, warn() {}, error() {} }
const retry = { retries: 1, initialRetryTime: 1, factor: 0 }
const groups = ['g0', 'g1', 'g2'].map((groupId) => ({
  groupId,
  topics: [{ topic: 't', partitions: [0] }],
}))
const row = (groupId, errorCode = 0) => ({
  groupId,
  errorCode,
  topics: errorCode
    ? []
    : [
        {
          topic: 't',
          partitions: [
            { partition: 0, offset: '10', metadata: null, committedLeaderEpoch: 3, errorCode: 0 },
          ],
        },
      ],
})
/** Builds a brokerless coordinator with the real negotiation contract.
 * @param {number} nodeId coordinator identity
 * @param {number} version selected OffsetFetch version
 * @returns {object} coordinator recording its requests
 */
const broker = (nodeId, version) => ({
  nodeId,
  lookupRequestWithMax: () => () => ({ request: { apiVersion: version } }),
  offsetFetch: jest.fn(async (request) =>
    request.groups
      ? { groups: request.groups.map((g) => row(g.groupId)) }
      : { ...row(request.groupId), responses: row(request.groupId).topics }
  ),
})
describe('fetch offsets for groups', () => {
  it.each([8, 9, 7])('batches three groups on two coordinators at version %i', async (version) => {
    const brokers = [broker(0, version), broker(1, version)]
    const cluster = {
      findGroupCoordinator: jest.fn(async ({ groupId }) => brokers[Number(groupId.slice(1)) % 2]),
    }
    const result = await createAdmin({ cluster, logger, retry }).fetchOffsetsForGroups({
      groups,
      requireStable: true,
    })
    expect(result.map((r) => r.groupId)).toEqual(['g0', 'g1', 'g2'])
    expect(brokers.reduce((n, b) => n + b.offsetFetch.mock.calls.length, 0)).toBe(
      version < 8 ? 3 : 2
    )
    for (const b of brokers)
      for (const [request] of b.offsetFetch.mock.calls) expect(request.requireStable).toBe(true)
  })
  it('returns a refused group beside the successful group', async () => {
    const b = broker(0, 9)
    b.offsetFetch.mockResolvedValue({ groups: [row('g0'), row('g1', 30)] })
    const result = await createAdmin({
      cluster: { findGroupCoordinator: async () => b },
      logger,
      retry,
    }).fetchOffsetsForGroups({ groups: groups.slice(0, 2) })
    expect(result.map((r) => [r.groupId, r.errorCode, r.error?.code ?? null])).toEqual([
      ['g0', 0, null],
      ['g1', 30, 30],
    ])
  })
  it('retries an unstable partition without resending its completed sibling', async () => {
    const b = broker(0, 9)
    b.offsetFetch
      .mockResolvedValueOnce({
        groups: [
          {
            groupId: 'g0',
            errorCode: 0,
            topics: [
              {
                topic: 't',
                partitions: [
                  { partition: 0, offset: '10', errorCode: 0 },
                  { partition: 1, offset: '-1', errorCode: 88 },
                ],
              },
            ],
          },
        ],
      })
      .mockResolvedValueOnce({
        groups: [
          {
            groupId: 'g0',
            errorCode: 0,
            topics: [{ topic: 't', partitions: [{ partition: 1, offset: '20', errorCode: 0 }] }],
          },
        ],
      })
    const result = await createAdmin({
      cluster: { findGroupCoordinator: async () => b },
      logger,
      retry,
    }).fetchOffsetsForGroups({
      groups: [{ groupId: 'g0', topics: [{ topic: 't', partitions: [0, 1] }] }],
      requireStable: true,
    })
    expect(b.offsetFetch.mock.calls[1][0].groups[0].topics).toEqual([
      { topic: 't', partitions: [{ partition: 1 }] },
    ])
    expect(result[0].topics[0].partitions.map((p) => p.offset)).toEqual(['10', '20'])
    expect(result[0].errorCode).toBe(0)
  })
  it('returns explicit empty selections without a lookup', async () => {
    const cluster = { findGroupCoordinator: jest.fn() }
    const result = await createAdmin({ cluster, logger, retry }).fetchOffsetsForGroups({
      groups: [{ groupId: 'g0', topics: [] }],
    })
    expect(result).toEqual([
      { groupId: 'g0', errorCode: 0, error: null, errorMessage: null, topics: [] },
    ])
    expect(cluster.findGroupCoordinator).not.toHaveBeenCalled()
  })
  it.each([[['g0', 'g0']], [['g0']], [['g0', 'foreign']]])(
    'rejects wrong response group identities %p',
    async (ids) => {
      const b = broker(0, 9)
      b.offsetFetch.mockResolvedValue({ groups: ids.map((id) => row(id)) })
      await expect(
        createAdmin({
          cluster: { findGroupCoordinator: async () => b },
          logger,
          retry,
        }).fetchOffsetsForGroups({ groups: groups.slice(0, 2) })
      ).rejects.toBeInstanceOf(KafkaJSInvariantViolation)
    }
  )
  it('refuses a nonboolean stable flag before coordinator work', async () => {
    const cluster = { findGroupCoordinator: jest.fn() }
    await expect(
      createAdmin({ cluster, logger, retry }).fetchOffsetsForGroups({
        groups,
        requireStable: 'true',
      })
    ).rejects.toBeInstanceOf(KafkaJSInvalidConfigurationError)
    expect(cluster.findGroupCoordinator).not.toHaveBeenCalled()
  })
})

describe('offset fetch wire seam', () => {
  it.each([9, 7])('sends the coordinator batches at wire version %i', async (version) => {
    const { Kafka } = require('../../..')
    const { createScriptedCluster } = require('../../../testHelpers/scriptedCluster')
    const nodes = [
      { id: 0, host: 'node0', port: 9092 },
      { id: 1, host: 'node1', port: 9093 },
    ]
    const scripted = createScriptedCluster({
      nodes,
      profile: 'offsets',
      advertise: { OffsetFetch: [version, version] },
    })
    scripted.answer('FindCoordinator', ({ coordinatorKeys }) => ({
      coordinators: coordinatorKeys.map((key) => {
        const node = nodes[Number(key.slice(1)) % 2]
        return {
          key,
          nodeId: node.id,
          host: node.host,
          port: node.port,
          errorCode: 0,
          errorMessage: null,
        }
      }),
    }))
    scripted.answer('OffsetFetch', (request) => {
      const groups = request.groups ?? [request]
      const results = groups.map((g) => ({
        groupId: g.groupId,
        errorCode: 0,
        topics: [
          {
            topic: 't',
            partitions: [
              { partition: 0, offset: '10', leaderEpoch: 3, metadata: null, errorCode: 0 },
            ],
          },
        ],
      }))
      return version < 8 ? results[0] : { groups: results }
    })
    const admin = new Kafka(scripted.kafkaOptions({ retry })).admin()
    try {
      await admin.connect()
      const result = await admin.fetchOffsetsForGroups({ groups, requireStable: true })
      expect(result.map((r) => r.groupId)).toEqual(['g0', 'g1', 'g2'])
      const requests = scripted.requests().filter((r) => r.api === 'OffsetFetch')
      expect(requests).toHaveLength(version < 8 ? 3 : 2)
      expect(requests.every((r) => r.version === version && r.fields.requireStable)).toBe(true)
      expect(requests.map((r) => r.node).sort()).toEqual(version < 8 ? [0, 0, 1] : [0, 1])
    } finally {
      await admin.disconnect()
      scripted.assertClean()
      expect(scripted.sockets().every((s) => s.destroyed)).toBe(true)
    }
  })
})

describe('offset result boundaries', () => {
  it.each([false, true])(
    'retains successful coordinator data when a peer fails with old=%p',
    async (old) => {
      const a = broker(0, old ? 7 : 9)
      const b = broker(1, old ? 7 : 9)
      b.offsetFetch.mockRejectedValue(new Error('transport unavailable'))
      const result = await createAdmin({
        cluster: { findGroupCoordinator: async ({ groupId }) => (groupId === 'g0' ? a : b) },
        logger,
        retry: { retries: 0 },
      }).fetchOffsetsForGroups({ groups: groups.slice(0, 2) })
      expect(result[0].topics[0].partitions[0].offset).toBe('10')
      expect(result[1].errorCode).toBe(-1)
      expect(result[1].error.message).toBe('transport unavailable')
    }
  )
  it.each([8, 9])('refuses wrong topic and partition sets at version %i', async (version) => {
    const build = require('../../protocol/requests/offsetFetch').protocol({ version })
    const pair = require('../../../testHelpers/wire/offsetFetchV9')
    const wanted = [
      { groupId: 'a', topics: [{ topic: 't', partitions: [{ partition: 0 }, { partition: 1 }] }] },
    ]
    const answers = [
      [{ topic: 't', partitions: [] }],
      [{ topic: 'other', partitions: [] }],
      [{ topic: 't', partitions: [{ partition: 0 }, { partition: 0 }] }],
      [{ topic: 't', partitions: [{ partition: 0 }, { partition: 2 }] }],
    ]
    for (const topics of answers) {
      const complete = topics.map((t) => ({
        ...t,
        partitions: t.partitions.map((p) => ({
          ...p,
          offset: '10',
          leaderEpoch: 3,
          metadata: null,
          errorCode: 0,
        })),
      }))
      await expect(
        build({ groups: wanted }).response.decode(
          pair.buildResponse({ groups: [{ groupId: 'a', errorCode: 0, topics: complete }] })
        )
      ).rejects.toBeInstanceOf(KafkaJSInvariantViolation)
    }
  })
  it('inherits a short API deadline and prevents the next coordinator lookup', async () => {
    const cluster = { findGroupCoordinator: jest.fn(() => new Promise(() => {})) }
    const admin = createAdmin({ cluster, logger, retry })
    await expect(admin.fetchOffsetsForGroups({ groups, apiTimeout: 10 })).rejects.toMatchObject({
      name: 'KafkaJSTimeout',
    })
    expect(cluster.findGroupCoordinator).toHaveBeenCalledTimes(1)
  })
})

describe('legacy single-group identity', () => {
  it('refuses a foreign group before its protocol refusal', async () => {
    const { Kafka } = require('../../..')
    const { createScriptedCluster } = require('../../../testHelpers/scriptedCluster')
    const scripted = createScriptedCluster({
      nodes: [{ id: 0, host: 'node0', port: 9092 }],
      profile: 'offsets',
    })
    scripted.answer('FindCoordinator', ({ coordinatorKeys }) => ({
      coordinators: coordinatorKeys.map((key) => ({
        key,
        nodeId: 0,
        host: 'node0',
        port: 9092,
        errorCode: 0,
        errorMessage: null,
      })),
    }))
    scripted.answer('OffsetFetch', () => ({ groupId: 'foreign', errorCode: 30, topics: [] }))
    const admin = new Kafka(scripted.kafkaOptions({ retry })).admin()
    try {
      await admin.connect()
      await expect(admin.fetchOffsets({ groupId: 'wanted' })).rejects.toMatchObject({
        name: 'KafkaJSInvariantViolation',
        message: expect.stringContaining('requested group "wanted" but received group "foreign"'),
      })
    } finally {
      await admin.disconnect()
      scripted.assertClean()
    }
  })
})

describe('before-work offset failures', () => {
  it.each(['authentication', 'capability'])(
    'throws the original wrapped %s refusal',
    async (kind) => {
      const {
        KafkaJSNonRetriableError,
        KafkaJSSASLAuthenticationError,
        KafkaJSServerDoesNotSupportApiKey,
      } = require('../../errors')
      const refusal =
        kind === 'authentication'
          ? new KafkaJSSASLAuthenticationError('authentication refused')
          : new KafkaJSServerDoesNotSupportApiKey('client v1-v9; broker absent', {
              apiKey: 9,
              apiName: 'OffsetFetch',
            })
      const wrapped = new KafkaJSNonRetriableError(refusal, { cause: refusal })
      const b = broker(0, 9)
      b.offsetFetch.mockRejectedValue(wrapped)
      const admin = createAdmin({
        cluster: { findGroupCoordinator: async () => b },
        logger,
        retry: { retries: 0 },
      })
      await expect(admin.fetchOffsetsForGroups({ groups: groups.slice(0, 1) })).rejects.toBe(
        refusal
      )
    }
  )
})

describe('offset routing isolation', () => {
  it.each(['authentication', 'capability'])(
    'keeps a %s route refusal call-level until work succeeds',
    async (kind) => {
      const {
        KafkaJSSASLAuthenticationError,
        KafkaJSServerDoesNotSupportApiKey,
      } = require('../../errors')
      const refusal =
        kind === 'authentication'
          ? new KafkaJSSASLAuthenticationError('authentication refused')
          : new KafkaJSServerDoesNotSupportApiKey('client v1-v9; broker absent', {
              apiKey: 9,
              apiName: 'OffsetFetch',
            })
      const route = jest
        .fn()
        .mockRejectedValueOnce(new Error('route unavailable'))
        .mockRejectedValueOnce(refusal)
      await expect(
        createAdmin({
          cluster: { findGroupCoordinator: route },
          logger,
          retry,
        }).fetchOffsetsForGroups({ groups: groups.slice(0, 1) })
      ).rejects.toBe(refusal)
      expect(route).toHaveBeenCalledTimes(2)
    }
  )
  it('sends healthy groups on both sides of an initial route failure', async () => {
    const healthy = broker(0, 9)
    const routeError = new Error('g1 route unavailable')
    const cluster = {
      findGroupCoordinator: jest.fn(async ({ groupId }) => {
        if (groupId === 'g1') throw routeError
        return healthy
      }),
    }
    const result = await createAdmin({
      cluster,
      logger,
      retry: { retries: 0 },
    }).fetchOffsetsForGroups({ groups })
    expect(result.map((g) => [g.groupId, g.errorCode])).toEqual([
      ['g0', 0],
      ['g1', -1],
      ['g2', 0],
    ])
    expect(result[0].topics[0].partitions[0].offset).toBe('10')
    expect(result[2].topics[0].partitions[0].offset).toBe('10')
    expect(result[1].error).toBe(routeError)
    expect(cluster.findGroupCoordinator.mock.calls.map(([g]) => g.groupId)).toEqual([
      'g0',
      'g1',
      'g2',
    ])
    expect(healthy.offsetFetch).toHaveBeenCalledTimes(1)
    expect(healthy.offsetFetch.mock.calls[0][0].groups.map((g) => g.groupId)).toEqual(['g0', 'g2'])
  })

  it('retains completed cells when a replacement route fails beside an unstable peer', async () => {
    const first = broker(0, 9)
    const routeError = new Error('replacement route unavailable')
    const part = (partition, errorCode) => ({
      partition,
      errorCode,
      offset: '10',
      metadata: null,
      committedLeaderEpoch: 3,
    })
    first.offsetFetch
      .mockResolvedValueOnce({
        groups: [
          row('g0', 16),
          {
            groupId: 'g1',
            errorCode: 0,
            topics: [{ topic: 't', partitions: [part(0, 0), part(1, 88)] }],
          },
          row('g2'),
        ],
      })
      .mockResolvedValueOnce({
        groups: [
          { groupId: 'g1', errorCode: 0, topics: [{ topic: 't', partitions: [part(1, 0)] }] },
        ],
      })
    let g0Routes = 0
    const cluster = {
      findGroupCoordinator: jest.fn(async ({ groupId }) => {
        if (groupId === 'g0' && ++g0Routes === 2) throw routeError
        return first
      }),
    }
    const selected = [
      groups[0],
      { groupId: 'g1', topics: [{ topic: 't', partitions: [0, 1] }] },
      groups[2],
    ]
    const result = await createAdmin({ cluster, logger, retry }).fetchOffsetsForGroups({
      groups: selected,
      requireStable: true,
    })
    expect(result.map((g) => [g.groupId, g.errorCode])).toEqual([
      ['g0', -1],
      ['g1', 0],
      ['g2', 0],
    ])
    expect(result[0].error).toBe(routeError)
    expect(result[1].topics[0].partitions.map((p) => [p.partition, p.offset, p.errorCode])).toEqual(
      [
        [0, '10', 0],
        [1, '10', 0],
      ]
    )
    expect(result[2].topics[0].partitions[0].offset).toBe('10')
    expect(cluster.findGroupCoordinator.mock.calls.map(([g]) => g.groupId)).toEqual([
      'g0',
      'g1',
      'g2',
      'g0',
    ])
    expect(first.offsetFetch).toHaveBeenCalledTimes(2)
    expect(first.offsetFetch.mock.calls[1][0].groups).toEqual([
      { groupId: 'g1', topics: [{ topic: 't', partitions: [{ partition: 1 }] }] },
    ])
  })
})

/** Builds a ready real Broker and records bytes from its selected production request encoder.
 * @param {number} version broker's sole advertised OffsetFetch version
 * @returns {object} broker, recorded bodies and route stub
 */
const encodedBroker = (version) => {
  const Broker = require('../../broker')
  const { lookup } = require('../../protocol/requests')
  const bodies = []
  const versions = { 9: { minVersion: version, maxVersion: version } }
  const pool = {
    host: 'fixture',
    port: 9092,
    generation: 0,
    sasl: null,
    pool: [],
    connectionTimeout: 1000,
    authenticationTimeout: 1000,
    setupPolicy: { timeoutCeiling: 1000, waitCeiling: 1000 },
    isConnected: () => true,
    isAuthenticated: () => true,
    setVersions() {},
    throwIfRetired() {},
    getConnectionByRequest: () => ({ isConnected: () => true, isAuthenticated: () => true }),
    async send(protocol) {
      bodies.push({
        version: protocol.request.apiVersion,
        body: (await protocol.request.encode()).buffer,
      })
      return { errorCode: 0, responses: row('g0').topics }
    },
  }
  const coordinator = new Broker({ connectionPool: pool, logger, versions, nodeId: 0 })
  coordinator.lookupRequest = lookup(versions)
  coordinator.versionsGeneration = 0
  return {
    coordinator,
    bodies,
    cluster: { findGroupCoordinator: jest.fn(async () => coordinator) },
  }
}

/** Checks classic or flexible selected request bytes independently of production decoders.
 * @param {object} recorded selected version and encoded body
 * @param {boolean} all all-topic null marker versus explicit topic/partition
 * @param {boolean} stable expected stable-read flag
 * @returns {void}
 */
const expectLegacyBody = ({ version, body }, all, stable = false) => {
  const Decoder = require('../../protocol/decoder')
  const decoder = new Decoder(body)
  expect(version).toBeGreaterThanOrEqual(2)
  expect(version).toBeLessThanOrEqual(7)
  const flexible = version >= 6
  expect(flexible ? decoder.readUVarIntString() : decoder.readString()).toBe('g0')
  if (all) {
    expect(flexible ? decoder.readUVarInt() : decoder.readInt32()).toBe(flexible ? 0 : -1)
  } else {
    expect(flexible ? decoder.readUVarInt() : decoder.readInt32()).toBe(flexible ? 2 : 1)
    expect(flexible ? decoder.readUVarIntString() : decoder.readString()).toBe('t')
    expect(flexible ? decoder.readUVarInt() : decoder.readInt32()).toBe(flexible ? 2 : 1)
    expect(decoder.readInt32()).toBe(0)
    if (flexible) decoder.readTaggedFields()
  }
  if (version === 7) expect(decoder.readBoolean()).toBe(stable)
  if (flexible) decoder.readTaggedFields()
  expect(decoder.canReadBytes(1)).toBe(false)
}

describe('legacy selected offset encoders', () => {
  it.each([2, 3, 4, 5, 6, 7])(
    'fetches omitted topics through the selected v%i encoder',
    async (version) => {
      const { cluster, bodies } = encodedBroker(version)
      const result = await createAdmin({
        cluster,
        logger,
        retry: { retries: 0 },
      }).fetchOffsetsForGroups({ groups: [{ groupId: 'g0' }] })
      expect(result[0].errorCode).toBe(0)
      expect(result[0].topics[0].partitions[0].offset).toBe('10')
      expect(bodies).toHaveLength(1)
      expect(bodies[0].version).toBe(version)
      expectLegacyBody(bodies[0], true)
    }
  )
  it.each([2, 3, 4, 5, 6, 7])(
    'keeps explicit empty selections off the v%i wire',
    async (version) => {
      const { cluster, bodies } = encodedBroker(version)
      const result = await createAdmin({
        cluster,
        logger,
        retry: { retries: 0 },
      }).fetchOffsetsForGroups({
        groups: [
          { groupId: 'g0', topics: [] },
          { groupId: 'g1', topics: [{ topic: 't', partitions: [] }] },
        ],
      })
      expect(result.map((g) => [g.groupId, g.errorCode, g.topics])).toEqual([
        ['g0', 0, []],
        ['g1', 0, []],
      ])
      expect(cluster.findGroupCoordinator).not.toHaveBeenCalled()
      expect(bodies).toHaveLength(0)
    }
  )
  it.each([2, 3, 4, 5, 6, 7])('preserves explicit partition bytes at v%i', async (version) => {
    const { cluster, bodies } = encodedBroker(version)
    const result = await createAdmin({
      cluster,
      logger,
      retry: { retries: 0 },
    }).fetchOffsetsForGroups({ groups: groups.slice(0, 1) })
    expect(result[0].topics[0].partitions[0].offset).toBe('10')
    expect(bodies).toHaveLength(1)
    expect(bodies[0].version).toBe(version)
    expectLegacyBody(bodies[0], false)
  })
  it.each([2, 3, 4, 5, 6])('refuses stable reads before sending v%i', async (version) => {
    const { cluster, bodies } = encodedBroker(version)
    await expect(
      createAdmin({ cluster, logger, retry: { retries: 0 } }).fetchOffsetsForGroups({
        groups: groups.slice(0, 1),
        requireStable: true,
      })
    ).rejects.toMatchObject({
      name: 'KafkaJSServerDoesNotSupportApiKey',
      message: expect.stringContaining('stable reads require v7+'),
    })
    expect(bodies).toHaveLength(0)
  })
  it('encodes stable reads on v7', async () => {
    const { cluster, bodies } = encodedBroker(7)
    const result = await createAdmin({
      cluster,
      logger,
      retry: { retries: 0 },
    }).fetchOffsetsForGroups({ groups: groups.slice(0, 1), requireStable: true })
    expect(result[0].errorCode).toBe(0)
    expectLegacyBody(bodies[0], false, true)
  })
  it('refuses an all-topic selection at v1 before encoding', async () => {
    const { cluster, bodies } = encodedBroker(1)
    await expect(
      createAdmin({ cluster, logger, retry: { retries: 0 } }).fetchOffsetsForGroups({
        groups: [{ groupId: 'g0' }],
      })
    ).rejects.toMatchObject({
      name: 'KafkaJSServerDoesNotSupportApiKey',
      message: expect.stringContaining('all-topic reads require v2+'),
    })
    expect(bodies).toHaveLength(0)
  })
})

describe('offset identity indexing', () => {
  it.each([8, 16, 32])('bounds explicit topic identity reads linearly for %i topics', (count) => {
    const { validateTopics } = require('../../protocol/requests/offsetFetch/responseIdentity')
    let reads = 0
    const topics = Array.from({ length: count }, (_, i) => ({
      get topic() {
        reads++
        return `topic-${i}`
      },
      partitions: [{ partition: 0 }],
    }))
    const actual = Array.from({ length: count }, (_, i) => ({
      topic: `topic-${i}`,
      partitions: [{ partition: 0 }],
    }))
    validateTopics({ errorCode: 0, topics: actual }, topics)
    expect(reads).toBeLessThanOrEqual(3 * count)
  })
  it.each([8, 16, 32])(
    'bounds explicit group identity reads linearly for %i groups',
    async (count) => {
      const build = require('../../protocol/requests/offsetFetch').protocol({ version: 9 })
      const pair = require('../../../testHelpers/wire/offsetFetchV9')
      let reads = 0
      const expected = Array.from({ length: count }, (_, i) => ({
        get groupId() {
          reads++
          return `group-${i}`
        },
        topics: [{ topic: 't', partitions: [{ partition: 0 }] }],
      }))
      const actual = Array.from({ length: count }, (_, i) => row(`group-${i}`))
      const protocol = build({ groups: expected })
      reads = 0
      const decoded = await protocol.response.decode(pair.buildResponse({ groups: actual }))
      expect(decoded.groups).toHaveLength(count)
      expect(reads).toBeLessThanOrEqual(3 * count)
    }
  )
})
describe('multi-group validation and exhaustion', () => {
  it.each([
    { groups: [] },
    { groups: [null] },
    { groups: [1] },
    { groups: [{ groupId: 'g\0' }] },
    { groups: [groups[0], groups[0]] },
    { groups, requireStable: null },
    { groups, requireStable: 1 },
    { groups: [groups[0], { groupId: 'bad', topics: [{ topic: 't', partitions: [-1] }] }] },
  ])('validates every item before discovery for %p', async (options) => {
    const cluster = { findGroupCoordinator: jest.fn() }
    await expect(
      createAdmin({ cluster, logger, retry }).fetchOffsetsForGroups(options)
    ).rejects.toMatchObject({ name: 'KafkaJSInvalidConfigurationError' })
    expect(cluster.findGroupCoordinator).not.toHaveBeenCalled()
  })
  it.each([14, 16])('retries only the unresolved group for code %i', async (code) => {
    const target = broker(0, 9)
    target.offsetFetch
      .mockResolvedValueOnce({ groups: [row('g0'), row('g1', code)] })
      .mockResolvedValueOnce({ groups: [row('g1')] })
    const cluster = { findGroupCoordinator: jest.fn(async () => target) }
    const result = await createAdmin({ cluster, logger, retry }).fetchOffsetsForGroups({
      groups: groups.slice(0, 2),
    })
    expect(result.map((g) => g.errorCode)).toEqual([0, 0])
    expect(cluster.findGroupCoordinator).toHaveBeenCalledTimes(code === 16 ? 3 : 2)
    expect(
      target.offsetFetch.mock.calls.map(([call]) => call.groups.map((g) => g.groupId))
    ).toEqual([['g0', 'g1'], ['g1']])
  })
  it.each([88, -1])(
    'broker group code %i is terminal, not the internal retry envelope',
    async (code) => {
      const target = broker(0, 9)
      target.offsetFetch.mockResolvedValue({
        groups: [{ groupId: 'g', errorCode: code, topics: [] }],
      })
      const admin = createAdmin({
        logger,
        cluster: { findGroupCoordinator: async () => target },
        retry,
      })
      const result = await admin.fetchOffsetsForGroups({ groups: [{ groupId: 'g' }] })
      expect(result[0].errorCode).toBe(code)
      expect(target.offsetFetch).toHaveBeenCalledTimes(1)
    }
  )
  it.each([false, true])(
    'keeps unstable partition data at zero retries with stable=%p',
    async (requireStable) => {
      const target = broker(0, 9)
      const answer = row('g0')
      answer.topics[0].partitions.push({
        ...answer.topics[0].partitions[0],
        partition: 1,
        errorCode: 88,
      })
      target.offsetFetch.mockResolvedValue({ groups: [answer] })
      const result = await createAdmin({
        logger,
        retry: { retries: 0 },
        cluster: { findGroupCoordinator: async () => target },
      }).fetchOffsetsForGroups({
        groups: [{ groupId: 'g0', topics: [{ topic: 't', partitions: [0, 1] }] }],
        requireStable,
      })
      expect(result[0].errorCode).toBe(0)
      expect(result[0].topics[0].partitions.map((p) => [p.offset, p.errorCode])).toEqual([
        ['10', 0],
        ['10', 88],
      ])
      expect(target.offsetFetch).toHaveBeenCalledTimes(1)
    }
  )
  it('retains a successful v7 group when its peer on the same coordinator fails', async () => {
    const target = broker(0, 7)
    const error = new Error('transport')
    target.offsetFetch.mockImplementation(async ({ groupId }) => {
      if (groupId === 'g1') throw error
      return { errorCode: 0, responses: row(groupId).topics }
    })
    const result = await createAdmin({
      logger,
      retry: { retries: 0 },
      cluster: { findGroupCoordinator: async () => target },
    }).fetchOffsetsForGroups({ groups: groups.slice(0, 2) })
    expect(result[0].topics[0].partitions[0].offset).toBe('10')
    expect(result[1]).toMatchObject({ errorCode: -1, error })
  })
  it('retains success when replacement discovery has a capability refusal', async () => {
    const target = broker(0, 9)
    target.offsetFetch.mockResolvedValue({ groups: [row('g0'), row('g1', 16)] })
    const error = new (require('../../errors').KafkaJSServerDoesNotSupportApiKey)('missing')
    const cluster = {
      findGroupCoordinator: jest
        .fn()
        .mockResolvedValueOnce(target)
        .mockResolvedValueOnce(target)
        .mockRejectedValue(error),
    }
    const result = await createAdmin({ logger, retry, cluster }).fetchOffsetsForGroups({
      groups: groups.slice(0, 2),
    })
    expect(result[0].topics[0].partitions[0].offset).toBe('10')
    expect(result[1]).toMatchObject({ errorCode: -1, error })
    expect(target.offsetFetch).toHaveBeenCalledTimes(1)
  })
  it('resolves omitted partitions from metadata while explicit empty partitions do no work', async () => {
    const target = broker(0, 9)
    const cluster = {
      findGroupCoordinator: jest.fn(async () => target),
      addTargetTopic: jest.fn(),
      refreshMetadataIfNecessary: jest.fn(),
      findTopicPartitionMetadata: () => [{ partitionId: 0 }],
    }
    const admin = createAdmin({ logger, retry, cluster })
    const result = await admin.fetchOffsetsForGroups({
      groups: [{ groupId: 'g0', topics: [{ topic: 't' }] }],
    })
    expect(result[0].topics[0].partitions[0].offset).toBe('10')
    expect(target.offsetFetch.mock.calls[0][0].groups[0].topics[0].partitions).toEqual([
      { partition: 0 },
    ])
    await admin.fetchOffsetsForGroups({
      groups: [{ groupId: 'g0', topics: [{ topic: 't', partitions: [] }] }],
    })
    expect(cluster.findGroupCoordinator).toHaveBeenCalledTimes(1)
  })
})

describe('second coordinator deadline', () => {
  it.each([false, true])(
    'publishes no partial result after expiry with timer withheld=%p',
    async (withheld) => {
      jest.useFakeTimers()
      let rejectHeld
      const held = new Promise((resolve, reject) => {
        rejectHeld = reject
      })
      const first = broker(0, 9)
      const second = broker(1, 9)
      let scope
      second.offsetFetch.mockImplementation(() => {
        scope = require('../deadline').currentDeadline()
        return held
      })
      const admin = createAdmin({
        logger,
        retry,
        cluster: {
          findGroupCoordinator: async ({ groupId }) => (groupId === 'g0' ? first : second),
        },
      })
      const result = admin
        .fetchOffsetsForGroups({ groups: groups.slice(0, 2), apiTimeout: 5 })
        .then(
          (value) => ({ value }),
          (error) => ({ error })
        )
      try {
        for (let i = 0; i < 60; i++) await Promise.resolve()
        expect(first.offsetFetch).toHaveBeenCalledTimes(1)
        expect(second.offsetFetch).toHaveBeenCalledTimes(1)
        if (withheld) {
          const now = jest.spyOn(performance, 'now').mockReturnValue(performance.now() + 6)
          rejectHeld(new Error('late refusal'))
          for (let i = 0; i < 60; i++) await Promise.resolve()
          now.mockRestore()
        } else {
          await jest.advanceTimersByTimeAsync(5)
          rejectHeld(new Error('late refusal'))
        }
        const outcome = await result
        expect(outcome.error).toBe(scope.timeout())
        expect(outcome.value).toBeUndefined()
        expect(scope.cleanups.size).toBe(0)
      } finally {
        jest.restoreAllMocks()
        expect(jest.getTimerCount()).toBe(0)
        jest.useRealTimers()
      }
    }
  )
})

describe('legacy admin stable option', () => {
  it.each([false, true])('forwards the stable read choice %p', async (requireStable) => {
    const target = { offsetFetch: jest.fn(async () => ({ responses: [] })) }
    const admin = createAdmin({
      logger,
      retry,
      cluster: { findGroupCoordinator: async () => target },
    })
    await admin.fetchOffsets({ groupId: 'g', topics: [], requireStable })
    expect(target.offsetFetch.mock.calls[0][0].requireStable ?? false).toBe(requireStable)
  })
  it.each([null, 'true', 1])(
    'refuses invalid legacy stable option %p before lookup',
    async (requireStable) => {
      const cluster = { findGroupCoordinator: jest.fn() }
      const admin = createAdmin({ logger, retry, cluster })
      await expect(admin.fetchOffsets({ groupId: 'g', requireStable })).rejects.toMatchObject({
        name: 'KafkaJSInvalidConfigurationError',
      })
      expect(cluster.findGroupCoordinator).not.toHaveBeenCalled()
    }
  )
})
