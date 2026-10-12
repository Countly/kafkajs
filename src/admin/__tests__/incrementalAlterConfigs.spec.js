const createAdmin = require('../index')
const ConfigResourceTypes = require('../../protocol/configResourceTypes')
const AlterConfigOpTypes = require('../../protocol/alterConfigOpTypes')
const apiKeys = require('../../protocol/requests/apiKeys')
const { createErrorFromCode } = require('../../protocol/error')
const { KafkaJSConnectionError } = require('../../errors')
const { createGroupApiBroker } = require('../../broker/__tests__/fixtures/groupApiHarness')
const { newLogger } = require('testHelpers')

const { TOPIC, BROKER, BROKER_LOGGER, GROUP } = ConfigResourceTypes
const { SET, DELETE, APPEND, SUBTRACT } = AlterConfigOpTypes
const INVALID_CONFIG = 40
const NOT_CONTROLLER = 41

/** @returns {{ type: number, name: string, configEntries: object[] }} a valid TOPIC resource */
const topicResource = (overrides = {}) => ({
  type: TOPIC,
  name: 'test-topic',
  configEntries: [{ name: 'retention.ms', operation: SET, value: '60000' }],
  ...overrides,
})

/**
 * A result the broker would answer for `resource`.
 * @param {{ type: number, name: string }} resource
 * @param {object} [overrides]
 */
const resultFor = ({ type, name }, overrides = {}) => ({
  errorCode: 0,
  errorMessage: null,
  resourceType: type,
  resourceName: name,
  ...overrides,
})

/**
 * An admin over stub brokers, `answer` deciding what a broker says for the resources it is given.
 * Retries are set explicitly: the retrier's defaults depend on the run mode, and the cases that
 * count attempts must not.
 * @param {(request: { resources: object[], validateOnly: boolean }) => Promise<object>} [answer]
 * @param {object} [retry]
 */
const adminOverStubs = (answer, retry = { retries: 2, initialRetryTime: 1, maxRetryTime: 1 }) => {
  const respond =
    answer ||
    (async ({ resources }) => ({ throttleTime: 0, resources: resources.map((r) => resultFor(r)) }))
  const makeBroker = () => ({ incrementalAlterConfigs: jest.fn(respond) })
  const nodes = { 1: makeBroker(), 2: makeBroker() }
  const controller = makeBroker()
  const cluster = {
    refreshMetadata: jest.fn(async () => {}),
    findControllerBroker: jest.fn(async () => controller),
    findBroker: jest.fn(async ({ nodeId }) => nodes[nodeId]),
  }

  return { admin: createAdmin({ logger: newLogger(), retry, cluster }), cluster, nodes, controller }
}

describe('Admin > incrementalAlterConfigs > validation', () => {
  // Each case is refused before anything is sent: no metadata refresh and no request.
  test.each([
    ['resources is null', null, /^Invalid resources array null$/],
    ['resources is not an array', 'topic', /^Invalid resources array topic$/],
    ['resources is empty', [], /^Resources array cannot be empty$/],
    ['a resource is null', [null], /^Invalid resources array, the entry at index 0 is null$/],
    ['a resource type is unknown', [topicResource({ type: 1999 })], /^Invalid resource type 1999/],
    ['a resource name is null', [topicResource({ name: null })], /^Invalid resource name null/],
    ['a resource name is a number', [topicResource({ name: 7 })], /^Invalid resource name 7/],
    ['a TOPIC name is empty', [topicResource({ name: '' })], /^Invalid resource name : /],
    [
      'a GROUP name is empty',
      [topicResource({ type: GROUP, name: '' })],
      /^Invalid resource name : /,
    ],
    [
      'a BROKER_LOGGER name is empty',
      [topicResource({ type: BROKER_LOGGER, name: '' })],
      /^Invalid resource name : /,
    ],
    [
      'configEntries is not an array',
      [topicResource({ configEntries: 'retention.ms' })],
      /^Invalid resource configEntries 'retention\.ms': /,
    ],
    [
      'a config entry is null',
      [topicResource({ configEntries: [null] })],
      /^Invalid config entry null/,
    ],
    [
      'a config name is not a string',
      [topicResource({ configEntries: [{ name: 5, operation: SET, value: '1' }] })],
      /^Invalid config entry name 5/,
    ],
    [
      'the same resource appears twice',
      [topicResource(), topicResource()],
      /^Invalid resources array, it cannot have multiple entries for the same resource/,
    ],
    [
      'a config name repeats with the same operation',
      [
        topicResource({
          configEntries: [
            { name: 'cleanup.policy', operation: APPEND, value: 'compact' },
            { name: 'cleanup.policy', operation: APPEND, value: 'delete' },
          ],
        }),
      ],
      /^Invalid resource configEntries, it cannot have multiple entries for the config cleanup\.policy/,
    ],
    [
      'a config name repeats with another operation',
      [
        topicResource({
          configEntries: [
            { name: 'cleanup.policy', operation: SUBTRACT, value: 'delete' },
            { name: 'cleanup.policy', operation: APPEND, value: 'compact' },
          ],
        }),
      ],
      /^Invalid resource configEntries, it cannot have multiple entries for the config cleanup\.policy/,
    ],
  ])('rejects when %s', async (_, resources, message) => {
    const { admin, cluster, nodes, controller } = adminOverStubs()

    await expect(admin.incrementalAlterConfigs({ resources })).rejects.toMatchObject({
      name: 'KafkaJSNonRetriableError',
      message: expect.stringMatching(message),
    })

    expect(cluster.refreshMetadata).not.toHaveBeenCalled()
    expect(controller.incrementalAlterConfigs).not.toHaveBeenCalled()
    expect(nodes[1].incrementalAlterConfigs).not.toHaveBeenCalled()
  })

  test.each([
    ['is missing', undefined],
    ['is null', null],
    ['is a numeric string', '1'],
    ['is fractional', 1.5],
    ['is below the range', -1],
    ['is above the range', 4],
    ['is the name of an operation', 'SET'],
    ['is a boolean', true],
  ])('rejects an operation that %s', async (_, operation) => {
    const { admin, cluster } = adminOverStubs()
    const resources = [topicResource({ configEntries: [{ name: 'retention.ms', operation }] })]

    await expect(admin.incrementalAlterConfigs({ resources })).rejects.toMatchObject({
      name: 'KafkaJSNonRetriableError',
      message: expect.stringMatching(/^Invalid config operation .* for retention\.ms/),
    })
    expect(cluster.refreshMetadata).not.toHaveBeenCalled()
  })

  describe.each([
    ['SET', SET],
    ['APPEND', APPEND],
    ['SUBTRACT', SUBTRACT],
  ])('%s needs a string value', (_, operation) => {
    test.each([
      ['missing', undefined],
      ['null', null],
      ['a number', 60000],
      ['a boolean', true],
      ['an array', ['compact']],
      ['a bigint', 1n],
    ])('rejects a value that is %s, naming the config', async (__, value) => {
      const { admin, cluster } = adminOverStubs()
      const resources = [
        topicResource({ configEntries: [{ name: 'retention.ms', operation, value }] }),
      ]

      await expect(admin.incrementalAlterConfigs({ resources })).rejects.toMatchObject({
        name: 'KafkaJSNonRetriableError',
        message: expect.stringMatching(/^Invalid config value for retention\.ms: /),
      })
      expect(cluster.refreshMetadata).not.toHaveBeenCalled()
    })

    test('accepts the empty string, which is a value', async () => {
      const { admin, controller } = adminOverStubs()
      const resources = [
        topicResource({ configEntries: [{ name: 'cleanup.policy', operation, value: '' }] }),
      ]

      await admin.incrementalAlterConfigs({ resources })

      expect(controller.incrementalAlterConfigs).toHaveBeenCalledTimes(1)
    })
  })

  test.each([
    ['missing', undefined],
    ['null', null],
    ['a string', 'ignored'],
    ['a number', 60000],
    ['an object', {}],
  ])('accepts a DELETE whose value is %s and sends the entry on', async (_, value) => {
    const { admin, controller } = adminOverStubs()
    const entry = { name: 'retention.ms', operation: DELETE, value }

    await admin.incrementalAlterConfigs({
      resources: [topicResource({ configEntries: [entry] })],
    })

    expect(controller.incrementalAlterConfigs).toHaveBeenCalledWith({
      resources: [topicResource({ configEntries: [entry] })],
      validateOnly: false,
    })
  })

  test('accepts a resource with no config entries', async () => {
    const { admin, controller } = adminOverStubs()

    await admin.incrementalAlterConfigs({ resources: [topicResource({ configEntries: [] })] })

    expect(controller.incrementalAlterConfigs).toHaveBeenCalledTimes(1)
  })
})

describe('Admin > incrementalAlterConfigs > results', () => {
  test('resolves one result per resource, in the order the resources were given', async () => {
    const { admin, nodes, controller } = adminOverStubs()
    const resources = [
      topicResource({ name: 'first' }),
      topicResource({ type: BROKER, name: '2' }),
      topicResource({ type: BROKER, name: '' }),
      topicResource({ type: BROKER, name: '1' }),
      topicResource({ name: 'last' }),
    ]

    const response = await admin.incrementalAlterConfigs({ resources })

    expect(response).toEqual({
      resources: resources.map(({ type, name }) => resultFor({ type, name })),
    })
    expect(controller.incrementalAlterConfigs).toHaveBeenCalledTimes(1)
    expect(
      controller.incrementalAlterConfigs.mock.calls[0][0].resources.map((r) => r.name)
    ).toEqual(['first', '', 'last'])
    expect(nodes[1].incrementalAlterConfigs).toHaveBeenCalledTimes(1)
    expect(nodes[2].incrementalAlterConfigs).toHaveBeenCalledTimes(1)
  })

  test('resolves a failed resource beside a good one, with its code and message', async () => {
    const answer = async ({ resources }) => ({
      resources: resources.map((resource) =>
        resource.name === 'bad-topic'
          ? resultFor(resource, { errorCode: INVALID_CONFIG, errorMessage: 'not a list' })
          : resultFor(resource)
      ),
    })
    const { admin, controller } = adminOverStubs(answer)
    const resources = [topicResource({ name: 'bad-topic' }), topicResource({ name: 'good-topic' })]

    await expect(admin.incrementalAlterConfigs({ resources })).resolves.toEqual({
      resources: [
        resultFor(resources[0], { errorCode: INVALID_CONFIG, errorMessage: 'not a list' }),
        resultFor(resources[1]),
      ],
    })
    expect(controller.incrementalAlterConfigs).toHaveBeenCalledTimes(1)
  })

  test.each([[true], [false], [undefined]])(
    'sends validateOnly %s as a boolean, false by default',
    async (validateOnly) => {
      const { admin, controller } = adminOverStubs()

      await admin.incrementalAlterConfigs({ resources: [topicResource()], validateOnly })

      expect(controller.incrementalAlterConfigs.mock.calls[0][0].validateOnly).toBe(
        validateOnly === true
      )
    }
  )

  test('answers UNKNOWN for a resource the broker left out, never a success', async () => {
    const answer = async ({ resources }) => ({ resources: [resultFor(resources[0])] })
    const { admin } = adminOverStubs(answer)
    const resources = [topicResource({ name: 'answered' }), topicResource({ name: 'forgotten' })]

    const response = await admin.incrementalAlterConfigs({ resources })

    expect(response.resources).toEqual([
      resultFor(resources[0]),
      {
        errorCode: -1,
        errorMessage: 'The broker did not answer for this resource',
        resourceType: TOPIC,
        resourceName: 'forgotten',
      },
    ])
  })

  test.each([[29], [INVALID_CONFIG], [42], [44]])(
    'returns code %d as a result and never retries it',
    async (errorCode) => {
      const answer = async ({ resources }) => ({
        resources: resources.map((resource) => resultFor(resource, { errorCode })),
      })
      const { admin, controller } = adminOverStubs(answer)

      const response = await admin.incrementalAlterConfigs({ resources: [topicResource()] })

      expect(response.resources[0].errorCode).toBe(errorCode)
      expect(controller.incrementalAlterConfigs).toHaveBeenCalledTimes(1)
    }
  )
})

describe('Admin > incrementalAlterConfigs > failures', () => {
  test('rejects with a transport error as it is, after one attempt', async () => {
    const failure = new KafkaJSConnectionError('Connection error: ECONNRESET')
    const { admin, controller } = adminOverStubs(async () => {
      throw failure
    })

    await expect(admin.incrementalAlterConfigs({ resources: [topicResource()] })).rejects.toBe(
      failure
    )
    expect(controller.incrementalAlterConfigs).toHaveBeenCalledTimes(1)
  })

  test('sends the whole call again when a resource answers NOT_CONTROLLER, as Java does', async () => {
    let attempt = 0
    const answer = async ({ resources }) => {
      attempt += 1
      return {
        resources: resources.map((resource) =>
          resultFor(resource, { errorCode: attempt === 1 ? NOT_CONTROLLER : 0 })
        ),
      }
    }
    const { admin, controller, cluster } = adminOverStubs(answer)

    const response = await admin.incrementalAlterConfigs({ resources: [topicResource()] })

    expect(response.resources[0].errorCode).toBe(0)
    expect(controller.incrementalAlterConfigs).toHaveBeenCalledTimes(2)
    expect(cluster.refreshMetadata).toHaveBeenCalledTimes(2)
  })

  test('gives up with KafkaJSNumberOfRetriesExceeded when NOT_CONTROLLER persists', async () => {
    const answer = async ({ resources }) => ({
      resources: resources.map((resource) => resultFor(resource, { errorCode: NOT_CONTROLLER })),
    })
    const { admin, controller } = adminOverStubs(answer, {
      retries: 1,
      initialRetryTime: 1,
      maxRetryTime: 1,
    })

    await expect(
      admin.incrementalAlterConfigs({ resources: [topicResource()] })
    ).rejects.toMatchObject({ name: 'KafkaJSNumberOfRetriesExceeded', retriable: false })
    expect(controller.incrementalAlterConfigs).toHaveBeenCalledTimes(2)
  })

  test('rejects with the error of a NOT_CONTROLLER thrown by the broker as well', async () => {
    const { admin, controller } = adminOverStubs(
      async () => {
        throw createErrorFromCode(NOT_CONTROLLER)
      },
      { retries: 1, initialRetryTime: 1, maxRetryTime: 1 }
    )

    await expect(
      admin.incrementalAlterConfigs({ resources: [topicResource()] })
    ).rejects.toMatchObject({ name: 'KafkaJSNumberOfRetriesExceeded' })
    expect(controller.incrementalAlterConfigs).toHaveBeenCalledTimes(2)
  })
})

describe('Admin > incrementalAlterConfigs > on the wire', () => {
  let harness

  const vector = (kind, caseId) =>
    require(
      `../../protocol/requests/incrementalAlterConfigs/fixtures/java/incrementalAlterConfigs${kind}-v1-${caseId}.json`
    )

  /** An admin over one real broker, on a fake socket, that offers IncrementalAlterConfigs up to v1. */
  const adminOverBroker = (responseHex) => {
    harness = createGroupApiBroker({
      versions: { [apiKeys.IncrementalAlterConfigs]: { minVersion: 0, maxVersion: 1 } },
      respond: () => Buffer.from(responseHex, 'hex'),
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

  afterEach(async () => {
    await harness.pool.destroy()
  })

  test('writes the value of a DELETE as null, whatever value the caller passed', async () => {
    const admin = adminOverBroker(vector('Response', 'ok').hex)
    const resources = [
      topicResource({ configEntries: [{ name: 'retention.ms', operation: DELETE, value: '1' }] }),
    ]

    await admin.incrementalAlterConfigs({ resources })

    expect(harness.requests[0].apiVersion).toEqual(1)
    expect(harness.requests[0].body.toString('hex')).toEqual(vector('Request', 'delete').hex)
  })

  test('writes the empty string as the value of a SET, never the null that removes the config', async () => {
    const admin = adminOverBroker(vector('Response', 'ok').hex)
    const resources = [
      topicResource({ configEntries: [{ name: 'retention.ms', operation: SET, value: '' }] }),
    ]

    await admin.incrementalAlterConfigs({ resources })

    // the vector's value "60000" (length byte 06) is the empty compact string (length byte 01)
    expect(harness.requests[0].body.toString('hex')).toEqual(
      vector('Request', 'set').hex.replace('063630303030', '01')
    )
  })

  test('resolves the per-resource results the broker answered', async () => {
    const admin = adminOverBroker(vector('Response', 'error').hex)
    const resources = [topicResource(), topicResource({ name: 'other-topic' })]

    await expect(admin.incrementalAlterConfigs({ resources })).resolves.toEqual({
      resources: [
        resultFor({ type: TOPIC, name: 'test-topic' }),
        resultFor(
          { type: TOPIC, name: 'other-topic' },
          {
            errorCode: INVALID_CONFIG,
            errorMessage: 'Invalid value compact,foo for configuration cleanup.policy',
          }
        ),
      ],
    })
  })
})
