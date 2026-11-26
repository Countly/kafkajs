const { Kafka, logLevel, KafkaJSUnknownOutcomeError, KafkaJSTimeout } = require('../../..')
const Broker = require('../../broker')
const ConnectionPool = require('../../network/connectionPool')
const { createScriptedCluster } = require('../../../testHelpers/scriptedCluster')
const { Schedule } = require('../../../testHelpers/schedule')
const { runWithDeadline } = require('../deadline')
const { connectionOpts, newLogger } = require('../../../testHelpers')
// TEMPORARY DIAGNOSTIC: remove with testHelpers/diagTimers.js after one CI run.
const diag = require('../../../testHelpers/diagTimers')
const state = { scripted: null, logs: [], timers: null }
jest.setTimeout(15000)

const configs = {
  resources: [{ type: 2, name: 't', configEntries: [{ name: 'retention.ms', value: '10' }] }],
}
const incremental = {
  resources: configs.resources.map((resource) => ({
    ...resource,
    configEntries: resource.configEntries.map((entry) => ({ ...entry, operation: 0 })),
  })),
}
const acl = [
  {
    resourceType: 2,
    resourceName: 't',
    resourcePatternType: 3,
    principal: 'User:u',
    host: '*',
    operation: 3,
    permissionType: 3,
  },
]
const quota = {
  entity: [{ entityType: 'user', entityName: 'u' }],
  ops: [{ key: 'producer_byte_rate', value: 10 }],
}
const topicId = '00112233-4455-6677-8899-aabbccddeeff'
const node = (id) => ({ id, host: 'node' + id, port: 9092 })

/** Tests a public invocation over real brokers and fake sockets. */
const fixture = (nodes = [node(0)], options = {}) => {
  const schedule = new Schedule()
  const scripted = createScriptedCluster({ profile: 'destructiveAdmin', schedule, nodes })
  state.scripted = scripted
  scripted.answer('Metadata', () => ({
    brokers: nodes.map(({ id, host, port }) => ({ nodeId: id, host, port, rack: null })),
    controllerId: nodes[0].id,
    topics: [
      {
        name: 't',
        topicId,
        errorCode: 0,
        isInternal: false,
        topicAuthorizedOperations: -2147483648,
        partitions: nodes.map(({ id }) => ({
          partitionIndex: id,
          errorCode: 0,
          leaderId: id,
          leaderEpoch: 1,
          replicas: [id],
          isr: [id],
          offlineReplicas: [],
        })),
      },
    ],
  }))
  scripted.answer('ListOffsets', ({ topics }) => ({
    topics: topics.map(({ name, partitions }) => ({
      name,
      partitions: partitions.map(({ partitionIndex, timestamp }) => ({
        partitionIndex,
        errorCode: 0,
        timestamp: '0',
        offset: timestamp === '-2' ? '0' : '10',
        leaderEpoch: 1,
      })),
    })),
  }))
  const retry = { retries: 1, initialRetryTime: 1, maxRetryTime: 1, factor: 0 }
  const admin = new Kafka(
    scripted.kafkaOptions({
      retry,
      logLevel: logLevel.DEBUG,
      logCreator: diag.logInto(state.logs),
      ...options,
    })
  ).admin()
  return { admin, schedule, scripted }
}

/**
 * The real Broker method, so a spy wraps it and never a spy a timed-out test left behind.
 * @param {string} method Broker method name
 * @returns {Function} the unmocked method
 * @throws {Error} when the method is still mocked
 */
const unmocked = (method) => {
  const send = Broker.prototype[method]
  if (jest.isMockFunction(send)) throw new Error(`Broker.prototype.${method} is still mocked`)
  return send
}

describe('written admin mutations', () => {
  beforeEach(() => {
    jest.useFakeTimers({ doNotFake: ['nextTick', 'setImmediate'] })
    state.scripted = null
    state.logs.length = 0
    state.timers = diag.traceTimers()
  })
  afterEach(async () => {
    await diag.report(state)
    state.timers.restore()
    jest.restoreAllMocks()
    jest.useRealTimers()
  })

  it.each([
    [
      'DeleteTopics',
      'deleteTopics',
      { topics: ['t'] },
      { topics: [expect.objectContaining({ topic: 't' })] },
    ],
    ['DeleteGroups', 'deleteGroups', ['g'], ['g']],
    ['AlterConfigs', 'alterConfigs', configs, configs],
    ['IncrementalAlterConfigs', 'incrementalAlterConfigs', incremental, incremental],
    ['CreateAcls', 'createAcls', { acl }, { creations: acl }],
    ['DeleteAcls', 'deleteAcls', { filters: acl }, { filters: acl }],
    ['InitProducerId', 'fenceProducers', { transactionalIds: ['tx'] }, { transactionalId: 'tx' }],
    [
      'OffsetDelete',
      'deleteOffsets',
      { groupId: 'g', topics: [{ topic: 't', partitions: [0] }] },
      { groupId: 'g' },
    ],
    ['UnregisterController', 'unregisterController', { controllerId: 2 }, { controllerId: 2 }],
    [
      'UpdateFeatures',
      'updateFeatures',
      { features: [{ name: 'test.version', maxVersionLevel: 1 }] },
      { validateOnly: false },
    ],
    ['AlterClientQuotas', 'alterClientQuotas', { entries: [quota] }, { validateOnly: false }],
    ['ElectLeaders', 'electLeaders', {}, { electionType: 0 }],
    ['CreateDelegationToken', 'createDelegationToken', {}, {}],
    [
      'RenewDelegationToken',
      'renewDelegationToken',
      { hmac: Buffer.from([1]) },
      { hmac: Buffer.from([1]) },
    ],
    [
      'ExpireDelegationToken',
      'expireDelegationToken',
      { hmac: Buffer.from([1]) },
      { hmac: Buffer.from([1]) },
    ],
    [
      'DeleteRecords',
      'deleteTopicRecords',
      { topic: 't', partitions: [{ partition: 0, offset: '-1' }] },
      { topics: [{ topic: 't', partitions: [{ partition: 0, offset: '-1' }] }] },
    ],
    [
      'WriteTxnMarkers',
      'abortTransaction',
      { topic: 't', partition: 0, producerId: '5', producerEpoch: 0, coordinatorEpoch: 0 },
      { markers: [expect.objectContaining({ producerId: '5' })] },
    ],
    [
      'LeaveGroup',
      'removeMembersFromConsumerGroup',
      { groupId: 'g', members: [{ groupInstanceId: 'instance' }] },
      { groupId: 'g' },
    ],
    [
      'AlterReplicaLogDirs',
      'alterReplicaLogDirs',
      { replicas: [{ topic: 't', partition: 0, brokerId: 0, logDir: '/data/a' }] },
      { dirs: [{ path: '/data/a', topics: [{ topic: 't', partitions: [0] }] }] },
    ],
    [
      'AlterPartitionReassignments',
      'alterPartitionReassignments',
      { topics: [{ topic: 't', partitionAssignment: [{ partition: 0, replicas: [0] }] }] },
      {},
    ],
    [
      'AlterUserScramCredentials',
      'alterUserScramCredentials',
      { deletions: [{ user: 'u', mechanism: 'SCRAM-SHA-256' }] },
      {},
    ],
    [
      'AlterShareGroupOffsets',
      'alterShareGroupOffsets',
      { groupId: 'g', topics: [{ topic: 't', partitions: [{ partition: 0, offset: '5' }] }] },
      { groupId: 'g' },
    ],
    [
      'DeleteShareGroupOffsets',
      'deleteShareGroupOffsets',
      { groupId: 'g', topics: ['t'] },
      { groupId: 'g' },
    ],
  ])('%s reports a lost reply once', async (api, method, input, fields) => {
    const { admin, schedule, scripted } = fixture()
    const gate = schedule.hold({ api })
    try {
      await admin.connect()
      const result = admin[method](input)
      const error = result.catch((failure) => failure)
      await Promise.race([gate.arrived(), error])
      gate.fail(new Error('lost reply'))
      await jest.advanceTimersByTimeAsync(50)
      await expect(error).resolves.toBeInstanceOf(KafkaJSUnknownOutcomeError)
      expect((await error).requests).toEqual([expect.objectContaining({ api })])
      expect(scripted.requests().filter((frame) => frame.api === api)).toEqual([
        expect.objectContaining({
          fields: Array.isArray(fields) ? fields : expect.objectContaining(fields),
        }),
      ])
    } finally {
      await admin.disconnect()
      scripted.assertClean()
    }
  })

  it.each([35, 51, 15, 16])(
    'InitProducerId keeps broker code %i as an answered row',
    async (errorCode) => {
      const { admin, schedule, scripted } = fixture()
      schedule.once({ api: 'InitProducerId' }, { errorCode })
      try {
        await admin.connect()
        const result = admin.fenceProducers({ transactionalIds: ['tx'] })
        await jest.advanceTimersByTimeAsync(50)
        await expect(result).resolves.toMatchObject([{ transactionalId: 'tx', errorCode }])
        expect(scripted.requests().filter(({ api }) => api === 'InitProducerId')).toEqual([
          expect.objectContaining({ fields: expect.objectContaining({ transactionalId: 'tx' }) }),
        ])
      } finally {
        await admin.disconnect()
        scripted.assertClean()
      }
    }
  )

  it.each([3, 41, 89])(
    'DeleteTopics keeps broker code %i as an answered rejection',
    async (code) => {
      const { admin, schedule, scripted } = fixture()
      schedule.once(
        { api: 'DeleteTopics' },
        {
          topicErrors: [{ topic: 't', topicId, errorCode: code, errorMessage: null }],
        }
      )
      try {
        await admin.connect()
        const result = admin.deleteTopics({ topics: ['t'] }).catch((error) => error)
        await jest.advanceTimersByTimeAsync(50)
        await expect(result).resolves.toMatchObject({ code })
        expect(scripted.requests().filter(({ api }) => api === 'DeleteTopics')).toHaveLength(1)
      } finally {
        await admin.disconnect()
        scripted.assertClean()
      }
    }
  )

  it('IncrementalAlterConfigs returns a controller error for a mutation and retries validation', async () => {
    const { admin, schedule, scripted } = fixture()
    const api = 'IncrementalAlterConfigs'
    const refused = {
      resources: [{ resourceType: 2, resourceName: 't', errorCode: 41, errorMessage: null }],
    }
    schedule.once({ api }, refused)
    schedule.once({ api }, refused)
    try {
      await admin.connect()
      await expect(admin.incrementalAlterConfigs(incremental)).resolves.toEqual(refused)
      const result = admin.incrementalAlterConfigs({ ...incremental, validateOnly: true })
      await jest.advanceTimersByTimeAsync(50)
      await expect(result).resolves.toMatchObject({ resources: [{ errorCode: 0 }] })
      expect(
        scripted
          .requests()
          .filter((frame) => frame.api === api)
          .map((frame) => frame.fields)
      ).toEqual(
        [
          incremental,
          { ...incremental, validateOnly: true },
          { ...incremental, validateOnly: true },
        ].map((request) => ({ validateOnly: false, ...request }))
      )
    } finally {
      await admin.disconnect()
      scripted.assertClean()
    }
  })

  it('a malformed body after a write is an unknown outcome', async () => {
    const { admin, schedule, scripted } = fixture()
    schedule.once({ api: 'CreateAcls' }, Buffer.from([0]))
    try {
      await admin.connect()
      await expect(admin.createAcls({ acl })).rejects.toBeInstanceOf(KafkaJSUnknownOutcomeError)
      expect(scripted.requests().filter(({ api }) => api === 'CreateAcls')).toHaveLength(1)
    } finally {
      await admin.disconnect()
      scripted.assertClean()
    }
  })

  it('a deadline wins over a held mutation reply', async () => {
    const { admin, schedule, scripted } = fixture()
    const gate = schedule.hold({ api: 'CreateAcls' })
    try {
      await admin.connect()
      const result = admin.createAcls({ acl, apiTimeout: 10 }).catch((error) => error)
      await gate.arrived()
      await jest.advanceTimersByTimeAsync(10)
      await expect(result).resolves.toBeInstanceOf(KafkaJSTimeout)
      gate.release()
      expect(scripted.requests().filter(({ api }) => api === 'CreateAcls')).toHaveLength(1)
    } finally {
      await admin.disconnect()
      scripted.assertClean()
    }
  })

  it.each([
    ['two unresolved config writes are reported together', false, [0, 1]],
    ['a decoded config refusal cannot hide a sibling lost reply', true, [1]],
  ])('%s', async (_title, answered, unknownNodes) => {
    const { admin, schedule, scripted } = fixture([node(0), node(1)])
    const api = 'AlterConfigs'
    const gates = [schedule.hold({ api, node: 0 }), schedule.hold({ api, node: 1 })]
    try {
      await admin.connect()
      const resources = [0, 1].map((id) => ({
        type: 4,
        name: String(id),
        configEntries: [{ name: 'log.retention.ms', value: '10' }],
      }))
      const result = admin.alterConfigs({ resources }).catch((error) => error)
      await Promise.all(gates.map((gate) => gate.arrived()))
      if (answered)
        gates[0].release({
          resources: [{ resourceType: 4, resourceName: '0', errorCode: 41, errorMessage: null }],
        })
      else gates[0].fail(new Error('lost 0'))
      gates[1].fail(new Error('lost 1'))
      const error = await result
      expect(error).toBeInstanceOf(KafkaJSUnknownOutcomeError)
      expect(error.requests.map(({ nodeId }) => nodeId).sort()).toEqual(unknownNodes)
      expect(
        scripted
          .requests()
          .filter((frame) => frame.api === api)
          .map(({ node }) => node)
          .sort()
      ).toEqual([0, 1])
    } finally {
      await admin.disconnect()
      scripted.assertClean()
    }
  })

  it.each([
    [
      'a second CreateAcls frame in one admin call is refused before writing with the replay refusal',
      'CreateAcls',
    ],
    [
      'a second WriteTxnMarkers frame in one admin call is refused before writing',
      'WriteTxnMarkers',
    ],
  ])('%s', async (_title, api) => {
    const scripted = createScriptedCluster({ profile: 'destructiveAdmin', nodes: [node(0)] })
    const broker = new Broker({
      connectionPool: new ConnectionPool(
        connectionOpts({
          host: 'node0',
          port: 9092,
          socketFactory: scripted.socketFactory,
        })
      ),
      logger: newLogger(),
      nodeId: 0,
    })
    const send =
      api === 'CreateAcls'
        ? () => broker.createAcls({ acl })
        : () => broker.writeTxnMarkers({ markers: [] })
    try {
      await broker.connect()
      await runWithDeadline(api, 100, async () => {
        await send()
        await expect(send()).rejects.toThrow('an earlier request of this call')
      })
      expect(scripted.requests().filter((frame) => frame.api === api)).toHaveLength(1)
    } finally {
      await broker.disconnect()
      scripted.assertClean()
    }
  })

  it('reentrant public calls can write the same config and a different config', async () => {
    const api = 'AlterConfigs'
    let entered = false
    let started
    const nested = new Promise((resolve) => {
      started = resolve
    })
    const { admin, schedule, scripted } = fixture([node(0), node(1)], {
      logLevel: logLevel.DEBUG,
      logCreator:
        () =>
        ({ log }) => {
          if (entered || !log.message.startsWith('Response AlterConfigs(')) return
          entered = true
          const other = { resources: [{ ...configs.resources[0], name: 'other' }] }
          started(Promise.allSettled([admin.alterConfigs(configs), admin.alterConfigs(other)]))
        },
    })
    scripted.answer(api, ({ resources }) => ({
      resources: resources.map(({ type, name }) => ({
        resourceType: type,
        resourceName: name,
        errorCode: 0,
        errorMessage: null,
      })),
    }))
    const gate = schedule.hold({ api, node: 1 })
    try {
      await admin.connect()
      const pending = admin.alterConfigs({
        resources: [...configs.resources, { type: 4, name: '1', configEntries: [] }],
      })
      const outer = pending.catch((error) => error)
      await Promise.race([gate.arrived(), outer])
      const results = await Promise.race([nested, outer])
      gate.release()
      expect(results.map(({ status }) => status)).toEqual(['fulfilled', 'fulfilled'])
      await expect(pending).resolves.toMatchObject({ resources: expect.any(Array) })
      expect(
        scripted
          .requests()
          .filter((frame) => frame.api === api)
          .map((frame) => frame.fields.resources[0].name)
          .sort()
      ).toEqual(['1', 'other', 't', 't'])
    } finally {
      await admin.disconnect()
      scripted.assertClean()
    }
  })

  it('an unnamed transactional id after an answered fence is refused before writing', async () => {
    const { admin, scripted } = fixture()
    try {
      await admin.connect()
      await expect(admin.fenceProducers({ transactionalIds: ['good', '\ud800'] })).rejects.toThrow(
        'an earlier request of this call'
      )
      expect(scripted.requests().filter(({ api }) => api === 'InitProducerId')).toEqual([
        expect.objectContaining({ fields: expect.objectContaining({ transactionalId: 'good' }) }),
      ])
    } finally {
      await admin.disconnect()
      scripted.assertClean()
    }
  })

  it('only the unanswered fence is reported when a sibling got a broker code', async () => {
    const { admin, schedule, scripted } = fixture()
    const api = 'InitProducerId'
    schedule.once({ api }, { errorCode: 15 })
    const gate = schedule.hold({ api })
    try {
      await admin.connect()
      const result = admin
        .fenceProducers({ transactionalIds: ['answered', 'lost'] })
        .catch((error) => error)
      await Promise.race([gate.arrived(), result])
      gate.fail(new Error('lost reply'))
      await expect(result).resolves.toMatchObject({
        name: 'KafkaJSUnknownOutcomeError',
        requests: [{ api, apiVersion: 5, nodeId: 0, items: ['lost'] }],
      })
      expect(
        scripted
          .requests()
          .filter((frame) => frame.api === api)
          .map(({ fields }) => fields.transactionalId)
      ).toEqual(['answered', 'lost'])
    } finally {
      await admin.disconnect()
      scripted.assertClean()
    }
  })

  it('two DeleteRecords partition errors are reported in one aggregate', async () => {
    const { admin, scripted } = fixture([node(0), node(1)])
    const api = 'DeleteRecords'
    scripted.answer(api, ({ topics }) => ({
      topics: topics.map(({ topic, partitions }) => ({
        topic,
        partitions: partitions.map(({ partition }) => ({
          partition,
          lowWatermark: '0',
          errorCode: 6,
        })),
      })),
    }))
    try {
      await admin.connect()
      const partitions = [0, 1].map((partition) => ({ partition, offset: '-1' }))
      const error = await admin.deleteTopicRecords({ topic: 't', partitions }).catch((e) => e)
      expect(error).toMatchObject({ name: 'KafkaJSNonRetriableError', retriable: false })
      expect(error.cause).toMatchObject({ name: 'KafkaJSDeleteTopicRecordsError' })
      expect([...error.cause.partitions].sort((a, b) => a.partition - b.partition)).toEqual(
        [0, 1].map((partition) => ({
          partition,
          offset: '-1',
          error: expect.objectContaining({ code: 6 }),
        }))
      )
      expect(
        scripted
          .requests()
          .filter((frame) => frame.api === api)
          .map(({ node }) => node)
          .sort()
      ).toEqual([0, 1])
    } finally {
      await admin.disconnect()
      scripted.assertClean()
    }
  })

  it('unowned InitProducerId and LeaveGroup keep ordinary transport errors inside an admin scope', async () => {
    const { schedule, scripted } = fixture()
    const broker = new Broker({
      connectionPool: new ConnectionPool(
        connectionOpts({
          host: 'node0',
          port: 9092,
          socketFactory: scripted.socketFactory,
        })
      ),
      nodeId: 0,
      logger: newLogger(),
    })
    try {
      await broker.connect()
      await runWithDeadline('nested requests', 1000, async () => {
        for (const [api, send] of [
          [
            'InitProducerId',
            () => broker.initProducerId({ transactionalId: 'tx', transactionTimeout: 100 }),
          ],
          ['LeaveGroup', () => broker.leaveGroup({ groupId: 'g', memberId: 'member' })],
        ]) {
          const gate = schedule.hold({ api })
          const result = send().catch((error) => error)
          await Promise.race([gate.arrived(), result])
          gate.fail(new Error('lost reply'))
          expect(await result).toMatchObject({ retriable: true })
          expect(await result).not.toBeInstanceOf(KafkaJSUnknownOutcomeError)
          await send()
          expect(scripted.requests().filter((frame) => frame.api === api)).toHaveLength(2)
        }
      })
    } finally {
      await broker.disconnect()
      scripted.assertClean()
    }
  })

  it('a failed dial before the write leaves room for the retry to send once', async () => {
    const { scripted } = fixture()
    const broker = new Broker({
      connectionPool: new ConnectionPool(
        connectionOpts({
          host: 'node0',
          port: 9092,
          socketFactory: scripted.socketFactory,
        })
      ),
      nodeId: 0,
      logger: newLogger(),
    })
    try {
      await broker.connect()
      await broker.disconnect()
      scripted.down(0)
      await runWithDeadline('createAcls', 1000, async () => {
        await expect(broker.createAcls({ acl })).rejects.toMatchObject({ retriable: true })
        expect(scripted.requests().filter((frame) => frame.api === 'CreateAcls')).toEqual([])
        scripted.up(0)
        await broker.createAcls({ acl })
      })
      expect(scripted.requests().filter((frame) => frame.api === 'CreateAcls')).toEqual([
        expect.objectContaining({ fields: { creations: acl } }),
      ])
    } finally {
      await broker.disconnect()
      scripted.assertClean()
    }
  })

  it.each([
    ...[
      [0, false],
      [0, true],
      [1, false],
      [1, true],
    ].map(([id, lostFirst]) => [
      'AlterConfigs',
      id,
      lostFirst,
      'alterConfigs',
      'alterConfigs',
      2,
      'config',
    ]),
    ...[
      [0, false],
      [0, true],
      [1, false],
      [1, true],
    ].map(([id, lostFirst]) => [
      'IncrementalAlterConfigs',
      id,
      lostFirst,
      'incrementalAlterConfigs',
      'incrementalAlterConfigs',
      1,
      'config',
    ]),
    ...[
      [0, false],
      [0, true],
      [1, false],
      [1, true],
    ].map(([id, lostFirst]) => [
      'DeleteRecords',
      id,
      lostFirst,
      'deleteRecords',
      'deleteTopicRecords',
      2,
      'records',
    ]),
    ...[
      [0, false],
      [0, true],
      [1, false],
      [1, true],
    ].map(([id, lostFirst]) => [
      'DeleteGroups',
      id,
      lostFirst,
      'deleteGroups',
      'deleteGroups',
      3,
      'groups',
    ]),
    ...[
      [0, false],
      [0, true],
      [1, false],
      [1, true],
    ].map(([id, lostFirst]) => [
      'AlterReplicaLogDirs',
      id,
      lostFirst,
      'alterReplicaLogDirs',
      'alterReplicaLogDirs',
      2,
      'logDirs',
    ]),
  ])(
    '%s preserves a lost reply beside a pre-write failure on node %i (lost first %p)',
    async (api, failedNode, lostFirst, brokerMethod, adminMethod, apiVersion, family) => {
      const nodes = [node(0), node(1)]
      const { admin, schedule, scripted } = fixture(nodes, { retry: { retries: 0 } })
      const lostNode = 1 - failedNode
      if (family === 'groups') {
        scripted.answer('FindCoordinator', ({ coordinatorKeys }) => ({
          coordinators: coordinatorKeys.map((key) => {
            const { id, host, port } = nodes[Number(key)]
            return { key, nodeId: id, host, port, errorCode: 0, errorMessage: null }
          }),
        }))
      }
      if (family === 'logDirs') {
        const { AlterReplicaLogDirs } = require('../../protocol/requests/apiKeys')
        scripted.answer('ApiVersions', (_fields, { node }) => {
          const answer = scripted.apiVersionsAnswer()
          if (node !== failedNode) return answer
          return {
            ...answer,
            apiKeys: answer.apiKeys.filter(({ apiKey }) => apiKey !== AlterReplicaLogDirs),
          }
        })
      }

      const order = failedNode === 0 ? [lostNode, failedNode] : [failedNode, lostNode]
      const input = {
        config: {
          resources: order.map((id) => ({ type: 4, name: String(id), configEntries: [] })),
        },
        records: {
          topic: 't',
          partitions: order.map((partition) => ({ partition, offset: '-1' })),
        },
        groups: order.map(String),
        logDirs: {
          replicas: order.map((id) => ({
            brokerId: id,
            topic: 't',
            partition: id,
            logDir: '/data/a',
          })),
        },
      }
      const item = {
        config: { type: 4, name: String(lostNode) },
        records: { topic: 't', partition: lostNode },
        groups: String(lostNode),
        logDirs: { brokerId: lostNode, topic: 't', partition: lostNode },
      }
      const heldReply = schedule.hold({ api, node: lostNode })
      const send = unmocked(brokerMethod)
      let startDial
      const dialAllowed = new Promise((resolve) => {
        startDial = resolve
      })
      let dialFinished
      const failed = new Promise((resolve) => {
        dialFinished = resolve
      })
      const spy = jest
        .spyOn(Broker.prototype, brokerMethod)
        .mockImplementation(async function (options) {
          if (this.nodeId !== failedNode) return send.call(this, options)
          await dialAllowed
          try {
            if (family !== 'logDirs') {
              await this.disconnect()
              scripted.down(failedNode)
            }
            return await send.call(this, options)
          } finally {
            dialFinished()
          }
        })
      try {
        await admin.connect()
        const result = admin[adminMethod](input[family]).catch((error) => error)
        await Promise.race([heldReply.arrived(), result])
        if (lostFirst) {
          heldReply.fail(new Error('lost reply'))
          await new Promise(setImmediate)
        }
        startDial()
        await failed
        if (!lostFirst) heldReply.fail(new Error('lost reply'))
        const error = await result
        expect(error).toBeInstanceOf(KafkaJSUnknownOutcomeError)
        expect(error.requests).toEqual([
          { api, apiVersion, nodeId: lostNode, items: [item[family]] },
        ])
        expect(scripted.requests().filter((frame) => frame.api === api)).toEqual([
          expect.objectContaining({ node: lostNode }),
        ])
      } finally {
        startDial()
        if (heldReply.settle) heldReply.fail(new Error('test ended'))
        spy.mockRestore()
        await admin.disconnect()
        scripted.assertClean()
      }
    }
  )

  it.each(['lost', 'retriable'])(
    'setOffsets retries a written commit after a %s reply',
    async (kind) => {
      const { describeGroupAs } = require('./fixtures/describeGroupAs')
      const described = describeGroupAs()
      const { admin, schedule, scripted } = fixture()
      const api = 'OffsetCommit'
      const gate = schedule.hold({ api })
      scripted.answer(api, ({ topics }) => ({
        topics: topics.map(({ topic, partitions }) => ({
          topic,
          partitions: partitions.map(({ partition }) => ({ partition, errorCode: 0 })),
        })),
      }))
      try {
        await admin.connect()
        const input = { groupId: 'g', topic: 't', partitions: [{ partition: 0, offset: '5' }] }
        const result = admin.setOffsets(input).catch((error) => error)
        await Promise.race([gate.arrived(), result])
        if (kind === 'lost') gate.fail(new Error('lost reply'))
        else
          gate.release({
            topics: [{ topic: 't', partitions: [{ partition: 0, errorCode: 14 }] }],
          })
        await jest.advanceTimersByTimeAsync(50)
        await expect(result).resolves.toBeUndefined()
        expect(
          scripted
            .requests()
            .filter((frame) => frame.api === api)
            .map(({ fields }) => fields)
        ).toEqual(
          Array.from({ length: 2 }, () =>
            expect.objectContaining({
              groupId: 'g',
              topics: [
                {
                  topic: 't',
                  partitions: [expect.objectContaining({ partition: 0, offset: '5' })],
                },
              ],
            })
          )
        )
      } finally {
        described.restore()
        await admin.disconnect()
        scripted.assertClean()
      }
    }
  )

  it('a controller refusal for deleteTopics by id prevents a follow-up refresh', async () => {
    const { admin, schedule, scripted } = fixture()
    const api = 'DeleteTopics'
    schedule.once(
      { api },
      { topicErrors: [{ topic: null, topicId, errorCode: 41, errorMessage: null }] }
    )
    try {
      await admin.connect()
      const result = admin.deleteTopics({ topicIds: [topicId] }).catch((error) => error)
      await jest.advanceTimersByTimeAsync(50)
      await expect(result).resolves.toMatchObject({
        code: 41,
        topicErrors: [expect.objectContaining({ topicId, errorCode: 41 })],
      })
      const frames = scripted.requests()
      const written = frames.findIndex((frame) => frame.api === api)
      expect(written).toBeGreaterThanOrEqual(0)
      expect(frames.slice(written).map((frame) => frame.api)).toEqual([api])
    } finally {
      await admin.disconnect()
      scripted.assertClean()
    }
  })

  it('DeleteRecords retries only the unwritten leader after its sibling answers', async () => {
    const { admin, schedule, scripted } = fixture([node(0), node(1)])
    const api = 'DeleteRecords'
    const answer = ({ topics }) => ({
      topics: topics.map(({ topic, partitions }) => ({
        topic,
        partitions: partitions.map(({ partition }) => ({
          partition,
          lowWatermark: '5',
          errorCode: 0,
        })),
      })),
    })
    scripted.answer(api, answer)
    const gate = schedule.hold({ api, node: 0 })
    const send = unmocked('deleteRecords')
    let ready
    const firstDialFinished = new Promise((resolve) => {
      ready = resolve
    })
    let failFirst = true
    const spy = jest
      .spyOn(Broker.prototype, 'deleteRecords')
      .mockImplementation(async function (options) {
        if (this.nodeId !== 1 || !failFirst) return send.call(this, options)
        failFirst = false
        await this.disconnect()
        scripted.down(1)
        try {
          return await send.call(this, options)
        } finally {
          scripted.up(1)
          ready()
        }
      })
    try {
      await admin.connect()
      const partitions = [0, 1].map((partition) => ({ partition, offset: '5' }))
      const result = admin.deleteTopicRecords({ topic: 't', partitions }).catch((error) => error)
      await Promise.race([Promise.all([gate.arrived(), firstDialFinished]), result])
      await jest.advanceTimersByTimeAsync(50)
      expect(
        scripted
          .requests()
          .filter((frame) => frame.api === api)
          .map(({ node }) => node)
      ).toEqual([0])
      gate.release()
      await jest.advanceTimersByTimeAsync(50)
      await expect(result).resolves.toBeUndefined()
      expect(
        scripted
          .requests()
          .filter((frame) => frame.api === api)
          .map(({ node }) => node)
      ).toEqual([0, 1])
    } finally {
      if (gate.settle) gate.fail(new Error('test ended'))
      spy.mockRestore()
      await admin.disconnect()
      scripted.assertClean()
    }
  })

  it('DeleteRecords sends the unwritten leader after its sibling refuses and reports both refusals', async () => {
    const { admin, schedule, scripted } = fixture([node(0), node(1)])
    const api = 'DeleteRecords'
    scripted.answer(api, ({ topics }) => ({
      topics: topics.map(({ topic, partitions }) => ({
        topic,
        partitions: partitions.map(({ partition }) => ({
          partition,
          lowWatermark: '0',
          errorCode: 6,
        })),
      })),
    }))
    const gate = schedule.hold({ api, node: 0 })
    const send = unmocked('deleteRecords')
    let ready
    const firstDialFinished = new Promise((resolve) => {
      ready = resolve
    })
    let failFirst = true
    const spy = jest
      .spyOn(Broker.prototype, 'deleteRecords')
      .mockImplementation(async function (options) {
        if (this.nodeId !== 1 || !failFirst) return send.call(this, options)
        failFirst = false
        await this.disconnect()
        scripted.down(1)
        try {
          return await send.call(this, options)
        } finally {
          scripted.up(1)
          ready()
        }
      })
    try {
      await admin.connect()
      const partitions = [0, 1].map((partition) => ({ partition, offset: '5' }))
      const result = admin.deleteTopicRecords({ topic: 't', partitions }).catch((error) => error)
      await Promise.race([Promise.all([gate.arrived(), firstDialFinished]), result])
      await jest.advanceTimersByTimeAsync(50)
      gate.release()
      await jest.advanceTimersByTimeAsync(50)
      const error = await result
      expect(error).toMatchObject({ name: 'KafkaJSNonRetriableError', retriable: false })
      expect(error.cause).toMatchObject({ name: 'KafkaJSDeleteTopicRecordsError' })
      expect([...error.cause.partitions].sort((a, b) => a.partition - b.partition)).toEqual(
        [0, 1].map((partition) => ({
          partition,
          offset: '5',
          error: expect.objectContaining({ code: 6 }),
        }))
      )
      expect(
        scripted
          .requests()
          .filter((frame) => frame.api === api)
          .map(({ node }) => node)
      ).toEqual([0, 1])
    } finally {
      if (gate.settle) gate.fail(new Error('test ended'))
      spy.mockRestore()
      await admin.disconnect()
      scripted.assertClean()
    }
  })
})
