const createAdmin = require('../index')
const ConfigResourceTypes = require('../../protocol/configResourceTypes')
const AlterConfigOpTypes = require('../../protocol/alterConfigOpTypes')
const apiKeys = require('../../protocol/requests/apiKeys')
const { createGroupApiBroker } = require('../../broker/__tests__/fixtures/groupApiHarness')
const { newLogger } = require('testHelpers')

const { TOPIC, BROKER, BROKER_LOGGER, GROUP, CLIENT_METRICS } = ConfigResourceTypes

/**
 * @param {string} id the broker's name in the assertions
 * @returns {object} a broker whose three config methods answer every resource they get with a success
 */
const stubBroker = (id) => {
  const answer = async ({ resources }) => ({
    resources: resources.map(({ type, name }) => ({
      errorCode: 0,
      errorMessage: null,
      resourceType: type,
      resourceName: name,
      configEntries: [],
    })),
  })

  return {
    id,
    describeConfigs: jest.fn(answer),
    alterConfigs: jest.fn(answer),
    incrementalAlterConfigs: jest.fn(answer),
  }
}

/**
 * An admin over stub brokers: node 1 and node 2, and the default broker that stands for the
 * controller. A node the cluster does not know rejects like the real `findBroker`.
 */
const adminOverStubs = () => {
  const nodes = { 1: stubBroker('node 1'), 2: stubBroker('node 2') }
  const controller = stubBroker('default')
  const cluster = {
    refreshMetadata: jest.fn(async () => {}),
    findControllerBroker: jest.fn(async () => controller),
    findBroker: jest.fn(async ({ nodeId }) => {
      if (!nodes[nodeId]) {
        throw new Error(`Broker ${JSON.stringify(nodeId)} not found`)
      }
      return nodes[nodeId]
    }),
  }
  const admin = createAdmin({ logger: newLogger(), retry: { retries: 0 }, cluster })

  return { admin, cluster, nodes, controller }
}

/** The resource each of the three methods is given for one `type` and `name`. */
const RESOURCES = {
  describeConfigs: ({ type, name }) => ({ type, name }),
  alterConfigs: ({ type, name }) => ({
    type,
    name,
    configEntries: [{ name: 'key', value: 'value' }],
  }),
  incrementalAlterConfigs: ({ type, name }) => ({
    type,
    name,
    configEntries: [{ name: 'key', operation: AlterConfigOpTypes.SET, value: 'value' }],
  }),
}

/** How each of the three methods is called with one resource. */
const CALLS = {
  describeConfigs: (admin, resource) => admin.describeConfigs({ resources: [resource] }),
  alterConfigs: (admin, resource) => admin.alterConfigs({ resources: [resource] }),
  incrementalAlterConfigs: (admin, resource) =>
    admin.incrementalAlterConfigs({ resources: [resource] }),
}

/** Calls `method` with a resource of that `type` and `name`, and returns what it resolves. */
const callWith = (method, admin, type, name) =>
  CALLS[method](admin, RESOURCES[method]({ type, name }))

describe('Admin > configuration resources > routing', () => {
  describe.each(Object.keys(CALLS))('%s', (method) => {
    test.each([
      [BROKER, '1', 'node 1'],
      [BROKER, '2', 'node 2'],
      [BROKER, '', 'default'],
      [BROKER_LOGGER, '1', 'node 1'],
      [TOPIC, 'topic', 'default'],
      [GROUP, 'group', 'default'],
      [CLIENT_METRICS, 'subscription', 'default'],
    ])('resource type %d named %j goes to the %s broker', async (type, name, target) => {
      const { admin, cluster, nodes, controller } = adminOverStubs()
      const brokers = [...Object.values(nodes), controller]

      await callWith(method, admin, type, name)

      const called = brokers.filter((broker) => broker[method].mock.calls.length > 0)
      expect(called.map(({ id }) => id)).toEqual([target])
      expect(called[0][method]).toHaveBeenCalledWith(
        expect.objectContaining({ resources: [expect.objectContaining({ type, name })] })
      )
      expect(cluster.findBroker).toHaveBeenCalledTimes(target === 'default' ? 0 : 1)
    })

    test('one request per broker when node-routed and default resources are mixed', async () => {
      const { admin, nodes, controller } = adminOverStubs()
      const call = {
        describeConfigs: () =>
          admin.describeConfigs({
            resources: [
              { type: TOPIC, name: 'topic' },
              { type: BROKER, name: '2' },
              { type: BROKER, name: '' },
            ],
          }),
        alterConfigs: () =>
          admin.alterConfigs({
            resources: [
              { type: TOPIC, name: 'topic', configEntries: [] },
              { type: BROKER, name: '2', configEntries: [] },
              { type: BROKER, name: '', configEntries: [] },
            ],
          }),
        incrementalAlterConfigs: () =>
          admin.incrementalAlterConfigs({
            resources: [
              { type: TOPIC, name: 'topic', configEntries: [] },
              { type: BROKER, name: '2', configEntries: [] },
              { type: BROKER, name: '', configEntries: [] },
            ],
          }),
      }[method]

      await call()

      expect(nodes[2][method]).toHaveBeenCalledTimes(1)
      expect(controller[method]).toHaveBeenCalledTimes(1)
      expect(controller[method].mock.calls[0][0].resources.map(({ name }) => name)).toEqual([
        'topic',
        '',
      ])
    })
  })
})

describe('Admin > configuration resources > validation of the resource name', () => {
  describe.each(Object.keys(CALLS))('%s', (method) => {
    test('accepts the empty name of the cluster-default broker config', async () => {
      const { admin } = adminOverStubs()

      await expect(callWith(method, admin, BROKER, '')).resolves.toBeDefined()
    })

    test.each([[TOPIC], [BROKER_LOGGER], [GROUP], [CLIENT_METRICS]])(
      'still rejects an empty name for resource type %d with the message it always had',
      async (type) => {
        const { admin, cluster } = adminOverStubs()
        const resource = RESOURCES[method]({ type, name: '' })

        await expect(CALLS[method](admin, resource)).rejects.toMatchObject({
          name: 'KafkaJSNonRetriableError',
          message: `Invalid resource name : ${JSON.stringify(resource)}`,
        })
        expect(cluster.refreshMetadata).not.toHaveBeenCalled()
      }
    )

    test.each([[null], [undefined], [4], [{}]])(
      'still rejects the name %j of a BROKER resource',
      async (name) => {
        const { admin, cluster } = adminOverStubs()
        const resource = RESOURCES[method]({ type: BROKER, name })

        await expect(CALLS[method](admin, resource)).rejects.toMatchObject({
          name: 'KafkaJSNonRetriableError',
          message: `Invalid resource name ${name}: ${JSON.stringify(resource)}`,
        })
        expect(cluster.refreshMetadata).not.toHaveBeenCalled()
      }
    )
  })
})

describe('Admin > describeConfigs and alterConfigs > messages that must not change', () => {
  const OLD_METHODS = {
    describeConfigs: (admin, resources) => admin.describeConfigs({ resources }),
    alterConfigs: (admin, resources) => admin.alterConfigs({ resources }),
  }

  describe.each(Object.keys(OLD_METHODS))('%s', (method) => {
    test.each([
      [null, 'Invalid resources array null'],
      ['not-an-array', 'Invalid resources array not-an-array'],
      [[], 'Resources array cannot be empty'],
      [[{ type: 1999 }], 'Invalid resource type 1999: {"type":1999}'],
      [[{ type: TOPIC, name: null }], 'Invalid resource name null: {"type":2,"name":null}'],
      [[{ type: TOPIC, name: 123 }], 'Invalid resource name 123: {"type":2,"name":123}'],
    ])('rejects %j with %j', async (resources, message) => {
      const { admin, cluster } = adminOverStubs()

      await expect(OLD_METHODS[method](admin, resources)).rejects.toMatchObject({
        name: 'KafkaJSNonRetriableError',
        message,
      })
      expect(cluster.refreshMetadata).not.toHaveBeenCalled()
    })
  })
})

describe('Admin > describeConfigs > includeDocumentation', () => {
  let harness

  const vector = (kind, version, caseId) =>
    require(
      `../../protocol/requests/describeConfigs/fixtures/java/describeConfigs${kind}-v${version}-${caseId}.json`
    )

  /** An admin over one real broker, on a fake socket, that offers DescribeConfigs up to v4. */
  const adminOverBroker = () => {
    harness = createGroupApiBroker({
      versions: { [apiKeys.DescribeConfigs]: { minVersion: 1, maxVersion: 4 } },
      respond: () => Buffer.from(vector('Response', 4, 'entries').hex, 'hex'),
    })

    return createAdmin({
      logger: newLogger(),
      retry: { retries: 0 },
      cluster: {
        refreshMetadata: async () => {},
        findControllerBroker: async () => harness.broker,
      },
    })
  }

  const resources = [{ type: TOPIC, name: 'test-topic' }]

  afterEach(async () => {
    await harness.pool.destroy()
  })

  test('sends the flag when it is set', async () => {
    const admin = adminOverBroker()

    await admin.describeConfigs({ resources, includeSynonyms: true, includeDocumentation: true })

    expect(harness.requests[0].apiVersion).toEqual(4)
    expect(harness.requests[0].body.toString('hex')).toEqual(
      vector('Request', 4, 'includeDocumentation').hex
    )
  })

  test.each([[undefined], [false]])(
    'does not ask for documentation when it is %s',
    async (flag) => {
      const admin = adminOverBroker()

      await admin.describeConfigs({
        resources,
        includeSynonyms: true,
        includeDocumentation: flag,
      })

      expect(harness.requests[0].body.toString('hex')).toEqual(vector('Request', 4, 'allKeys').hex)
    }
  )

  test('keeps every key an entry had and adds configType and documentation', async () => {
    const admin = adminOverBroker()

    const { resources: described } = await admin.describeConfigs({
      resources,
      includeSynonyms: true,
      includeDocumentation: true,
    })

    expect(described[0].configEntries).toEqual([
      {
        configName: 'retention.ms',
        configValue: '60000',
        readOnly: false,
        isDefault: false,
        configSource: 1,
        isSensitive: false,
        configSynonyms: [
          { configName: 'retention.ms', configValue: '60000', configSource: 1 },
          { configName: 'log.retention.ms', configValue: null, configSource: 5 },
        ],
        configType: 5,
        documentation: 'The retention time',
      },
      {
        configName: 'cleanup.policy',
        configValue: 'delete',
        readOnly: false,
        isDefault: true,
        configSource: 5,
        isSensitive: false,
        configSynonyms: [],
        configType: 7,
        documentation: null,
      },
    ])
  })
})
