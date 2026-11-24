const { Kafka, KafkaJSUnknownOutcomeError, KafkaJSTimeout } = require('../../..')
const Broker = require('../../broker')
const ConnectionPool = require('../../network/connectionPool')
const { createScriptedCluster } = require('../../../testHelpers/scriptedCluster')
const { Schedule } = require('../../../testHelpers/schedule')
const { runWithDeadline } = require('../deadline')
const { connectionOpts, newLogger } = require('../../../testHelpers')

const configs = {
  resources: [{ type: 2, name: 't', configEntries: [{ name: 'retention.ms', value: '10' }] }],
}
const incremental = {
  resources: configs.resources.map((resource) => ({
    ...resource,
    configEntries: resource.configEntries.map((entry) => ({ ...entry, operation: 0 })),
  })),
}
const acl = [{
  resourceType: 2,
  resourceName: 't',
  resourcePatternType: 3,
  principal: 'User:u',
  host: '*',
  operation: 3,
  permissionType: 3,
}]
const quota = {
  entity: [{ entityType: 'user', entityName: 'u' }],
  ops: [{ key: 'producer_byte_rate', value: 10 }],
}
const topicId = '00112233-4455-6677-8899-aabbccddeeff'
const node = (id) => ({ id, host: 'node' + id, port: 9092 })

/** Tests a public invocation over real brokers and fake sockets. */
const fixture = (nodes = [node(0)]) => {
  const schedule = new Schedule()
  const scripted = createScriptedCluster({ profile: 'destructiveAdmin', schedule, nodes })
  const retry = { retries: 1, initialRetryTime: 1, maxRetryTime: 1, factor: 0 }
  const admin = new Kafka(scripted.kafkaOptions({ retry })).admin()
  return { admin, schedule, scripted }
}

describe('written admin mutations', () => {
  beforeEach(() => jest.useFakeTimers({ doNotFake: ['nextTick', 'setImmediate'] }))
  afterEach(() => jest.useRealTimers())

  it.each([
    ['DeleteTopics', 'deleteTopics', { topics: ['t'] }, { topics: [{ name: 't' }] }],
    ['DeleteGroups', 'deleteGroups', ['g'], ['g']],
    ['AlterConfigs', 'alterConfigs', configs, configs],
    ['IncrementalAlterConfigs', 'incrementalAlterConfigs', incremental, incremental],
    ['CreateAcls', 'createAcls', { acl }, { creations: acl }],
    ['DeleteAcls', 'deleteAcls', { filters: acl }, { filters: acl }],
    ['InitProducerId', 'fenceProducers', { transactionalIds: ['tx'] }, { transactionalId: 'tx' }],
    ['OffsetDelete', 'deleteOffsets', { groupId: 'g', topics: [{ topic: 't', partitions: [0] }] }, { groupId: 'g' }],
    ['UnregisterController', 'unregisterController', { controllerId: 2 }, { controllerId: 2 }],
    ['UpdateFeatures', 'updateFeatures', { features: [{ name: 'test.version', maxVersionLevel: 1 }] }, { validateOnly: false }],
    ['AlterClientQuotas', 'alterClientQuotas', { entries: [quota] }, { validateOnly: false }],
    ['ElectLeaders', 'electLeaders', {}, { electionType: 0 }],
    ['CreateDelegationToken', 'createDelegationToken', {}, {}],
  ])('%s reports a lost reply once', async (api, method, input, fields) => {
    const { admin, schedule, scripted } = fixture()
    const gate = schedule.hold({ api })
    try {
      await admin.connect()
      const result = admin[method](input)
      const error = result.catch((failure) => failure)
      await gate.arrived()
      gate.fail(new Error('lost reply'))
      await jest.advanceTimersByTimeAsync(50)
      await expect(error).resolves.toBeInstanceOf(KafkaJSUnknownOutcomeError)
      expect((await error).requests).toEqual([
        expect.objectContaining({ api, nodeId: 0 }),
      ])
      expect(scripted.requests().filter((frame) => frame.api === api)).toEqual([
        expect.objectContaining({ fields: expect.objectContaining(fields) }),
      ])
    } finally {
      await admin.disconnect()
      scripted.assertClean()
    }
  })

  it.each([35, 51, 15, 16])('InitProducerId keeps broker code %i as an answered row', async (errorCode) => {
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
  })

  it.each([3, 41, 89])('DeleteTopics keeps broker code %i as an answered rejection', async (code) => {
    const { admin, schedule, scripted } = fixture()
    schedule.once({ api: 'DeleteTopics' }, {
      responses: [{ name: 't', topicId, errorCode: code, errorMessage: null }],
    })
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
  })

  it('IncrementalAlterConfigs returns a controller error for a mutation and retries validation', async () => {
    const { admin, schedule, scripted } = fixture()
    const api = 'IncrementalAlterConfigs'
    const refused = { resources: [{ resourceType: 2, resourceName: 't', errorCode: 41, errorMessage: null }] }
    schedule.once({ api }, refused)
    schedule.once({ api }, refused)
    try {
      await admin.connect()
      await expect(admin.incrementalAlterConfigs(incremental)).resolves.toEqual(refused)
      const result = admin.incrementalAlterConfigs({ ...incremental, validateOnly: true })
      await jest.advanceTimersByTimeAsync(50)
      await expect(result).resolves.toMatchObject({ resources: [{ errorCode: 0 }] })
      expect(scripted.requests().filter((frame) => frame.api === api).map((frame) => frame.fields))
        .toEqual([incremental, { ...incremental, validateOnly: true }, { ...incremental, validateOnly: true }]
          .map((request) => ({ validateOnly: false, ...request })))
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
      if (answered) gates[0].release({
        resources: [{ resourceType: 4, resourceName: '0', errorCode: 41, errorMessage: null }],
      })
      else gates[0].fail(new Error('lost 0'))
      gates[1].fail(new Error('lost 1'))
      const error = await result
      expect(error).toBeInstanceOf(KafkaJSUnknownOutcomeError)
      expect(error.requests.map(({ nodeId }) => nodeId).sort()).toEqual(unknownNodes)
      expect(scripted.requests().filter((frame) => frame.api === api).map(({ node }) => node).sort()).toEqual([0, 1])
    } finally {
      await admin.disconnect()
      scripted.assertClean()
    }
  })

  it.each([
    ['a second CreateAcls frame in one admin call is refused before writing with the replay refusal', 'CreateAcls'],
    ['a second WriteTxnMarkers frame in one admin call is refused before writing', 'WriteTxnMarkers'],
  ])('%s', async (_title, api) => {
    const scripted = createScriptedCluster({ profile: 'destructiveAdmin', nodes: [node(0)] })
    const broker = new Broker({
      connectionPool: new ConnectionPool(connectionOpts({
        host: 'node0', port: 9092, socketFactory: scripted.socketFactory,
      })),
      logger: newLogger(),
      nodeId: 0,
    })
    const send = api === 'CreateAcls'
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
})
