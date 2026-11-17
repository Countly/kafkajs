const createAdmin = require('../index')
const createProducer = require('../../producer/index')

const {
  secureRandom,
  createCluster,
  newLogger,
  sslConnectionOpts,
  saslConnectionOpts,
  saslBrokers,
} = require('testHelpers')
const {
  clusterAclPort,
  closeEach,
  waitForAclBindings,
} = require('../../../testHelpers/aclPropagation')
const { KafkaJSProtocolError } = require('../../errors')

const ACL_RESOURCE_TYPES = require('../../protocol/aclResourceTypes')
const ACL_OPERATION_TYPES = require('../../protocol/aclOperationTypes')
const ACL_PERMISSION_TYPES = require('../../protocol/aclPermissionTypes')
const RESOURCE_PATTERN_TYPES = require('../../protocol/resourcePatternTypes')

const createSASLClientForUser =
  (createClient) =>
  ({ username, password }) => {
    const saslConnectionOpts = () => {
      return Object.assign(sslConnectionOpts(), {
        port: 9094,
        sasl: {
          mechanism: 'plain',
          username,
          password,
        },
      })
    }

    const client = createClient({
      logger: newLogger(),
      cluster: createCluster(
        {
          ...saslConnectionOpts(),
          metadataMaxAge: 50,
        },
        saslBrokers()
      ),
    })

    return client
  }

const createSASLAdminClientForUser = createSASLClientForUser(createAdmin)
const createSASLProducerClientForUser = createSASLClientForUser(createProducer)

/**
 * @param {string} resourceName topic whose ACLs to read
 * @returns {object} DescribeAcls filter matching every literal ACL on that topic
 */
const topicAclFilter = (resourceName) => ({
  resourceType: ACL_RESOURCE_TYPES.TOPIC,
  resourceName,
  resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
  principal: null,
  host: null,
  operation: ACL_OPERATION_TYPES.ANY,
  permissionType: ACL_PERMISSION_TYPES.ANY,
})

/**
 * Follows a client error's `cause` chain to the broker's protocol error.
 * @param {Error} error rejection of a client call
 * @returns {Error} the first KafkaJSProtocolError in the chain, else the last error
 */
const protocolCause = (error) => {
  let cause = error
  while (!(cause instanceof KafkaJSProtocolError) && cause?.cause) cause = cause.cause
  return cause
}

/**
 * @param {Promise<any>} call client call expected to be refused
 * @returns {Promise<void>}
 * @throws {Error} the call resolves or fails with anything but TOPIC_AUTHORIZATION_FAILED (29)
 */
const expectTopicAuthorizationFailed = async (call) => {
  const cause = protocolCause(
    await call.then(
      (value) => ({ resolved: value }),
      (e) => e
    )
  )
  expect(cause).toBeInstanceOf(KafkaJSProtocolError)
  expect(cause).toMatchObject({ type: 'TOPIC_AUTHORIZATION_FAILED', code: 29 })
}

describe('Admin', () => {
  let admin
  let aclPort

  /** @returns {Promise<object>} per-broker ACL port with the `test` admin's credentials */
  const connectAclPort = async () => {
    const { ssl, sasl } = saslConnectionOpts()
    aclPort = await clusterAclPort({ brokers: saslBrokers(), ssl, sasl })
    return aclPort
  }

  afterEach(async () => {
    const port = aclPort
    aclPort = null
    await closeEach(admin && (() => admin.disconnect()), port && (() => port.close()))
  })

  describe('createAcls', () => {
    test('throws an error if the acl array is invalid', async () => {
      admin = createSASLAdminClientForUser({ username: 'test', password: 'testtest' })
      await admin.connect()

      await expect(admin.createAcls({ acl: 'this-is-not-an-array' })).rejects.toHaveProperty(
        'message',
        'Invalid ACL array this-is-not-an-array'
      )
    })

    test('throws an error if the resource name is invalid', async () => {
      admin = createSASLAdminClientForUser({ username: 'test', password: 'testtest' })
      await admin.connect()

      const ACLEntry = {
        resourceType: ACL_RESOURCE_TYPES.TOPIC,
        resourceName: 123,
        resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
        principal: 'User:foo',
        host: '*',
        operation: ACL_OPERATION_TYPES.ALL,
        permissionType: ACL_PERMISSION_TYPES.DENY,
      }

      await expect(admin.createAcls({ acl: [ACLEntry] })).rejects.toHaveProperty(
        'message',
        'Invalid ACL array, the resourceNames have to be a valid string'
      )
    })

    test('throws an error if the principal name is invalid', async () => {
      admin = createSASLAdminClientForUser({ username: 'test', password: 'testtest' })
      await admin.connect()

      const ACLEntry = {
        resourceType: ACL_RESOURCE_TYPES.TOPIC,
        resourceName: 'foo',
        resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
        principal: 123,
        host: '*',
        operation: ACL_OPERATION_TYPES.ALL,
        permissionType: ACL_PERMISSION_TYPES.DENY,
      }

      await expect(admin.createAcls({ acl: [ACLEntry] })).rejects.toHaveProperty(
        'message',
        'Invalid ACL array, the principals have to be a valid string'
      )
    })

    test('throws an error if the host name is invalid', async () => {
      admin = createSASLAdminClientForUser({ username: 'test', password: 'testtest' })
      await admin.connect()

      const ACLEntry = {
        resourceType: ACL_RESOURCE_TYPES.TOPIC,
        resourceName: 'foo',
        resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
        principal: 'User:foo',
        host: 123,
        operation: ACL_OPERATION_TYPES.ALL,
        permissionType: ACL_PERMISSION_TYPES.DENY,
      }

      await expect(admin.createAcls({ acl: [ACLEntry] })).rejects.toHaveProperty(
        'message',
        'Invalid ACL array, the hosts have to be a valid string'
      )
    })

    test('throws an error if there are invalid resource types', async () => {
      admin = createSASLAdminClientForUser({ username: 'test', password: 'testtest' })
      await admin.connect()

      const ACLEntry = {
        resourceType: 123,
        resourceName: 'foo',
        resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
        principal: 'User:foo',
        host: '*',
        operation: ACL_OPERATION_TYPES.ALL,
        permissionType: ACL_PERMISSION_TYPES.DENY,
      }

      await expect(admin.createAcls({ acl: [ACLEntry] })).rejects.toHaveProperty(
        'message',
        `Invalid resource type 123: ${JSON.stringify(ACLEntry)}`
      )
    })

    test('throws an error if there are invalid resource pattern types', async () => {
      admin = createSASLAdminClientForUser({ username: 'test', password: 'testtest' })
      await admin.connect()

      const ACLEntry = {
        resourceType: ACL_RESOURCE_TYPES.TOPIC,
        resourceName: 'foo',
        resourcePatternType: 123,
        principal: 'User:foo',
        host: '*',
        operation: ACL_OPERATION_TYPES.ALL,
        permissionType: ACL_PERMISSION_TYPES.DENY,
      }

      await expect(admin.createAcls({ acl: [ACLEntry] })).rejects.toHaveProperty(
        'message',
        `Invalid resource pattern type 123: ${JSON.stringify(ACLEntry)}`
      )
    })

    test('throws an error if there are invalid permission types', async () => {
      admin = createSASLAdminClientForUser({ username: 'test', password: 'testtest' })
      await admin.connect()

      const ACLEntry = {
        resourceType: ACL_RESOURCE_TYPES.TOPIC,
        resourceName: 'foo',
        resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
        principal: 'User:foo',
        host: '*',
        operation: ACL_OPERATION_TYPES.ALL,
        permissionType: 123,
      }

      await expect(admin.createAcls({ acl: [ACLEntry] })).rejects.toHaveProperty(
        'message',
        `Invalid permission type 123: ${JSON.stringify(ACLEntry)}`
      )
    })

    test('throws an error if there are invalid operation types', async () => {
      admin = createSASLAdminClientForUser({ username: 'test', password: 'testtest' })
      await admin.connect()

      const ACLEntry = {
        resourceType: ACL_RESOURCE_TYPES.TOPIC,
        resourceName: 'foo',
        resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
        principal: 'User:foo',
        host: '*',
        operation: 123,
        permissionType: ACL_PERMISSION_TYPES.DENY,
      }

      await expect(admin.createAcls({ acl: [ACLEntry] })).rejects.toHaveProperty(
        'message',
        `Invalid operation type 123: ${JSON.stringify(ACLEntry)}`
      )
    })

    test('checks topic access', async () => {
      const topicName = `test-topic-${secureRandom()}`

      admin = createSASLAdminClientForUser({ username: 'test', password: 'testtest' })

      await admin.connect()
      await admin.createTopics({
        waitForLeaders: true,
        topics: [{ topic: topicName, numPartitions: 1, replicationFactor: 2 }],
      })

      await expect(
        admin.createAcls({
          acl: [
            {
              resourceType: ACL_RESOURCE_TYPES.TOPIC,
              resourceName: topicName,
              resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
              principal: 'User:bob',
              host: '*',
              operation: ACL_OPERATION_TYPES.ALL,
              permissionType: ACL_PERMISSION_TYPES.DENY,
            },
            {
              resourceType: ACL_RESOURCE_TYPES.TOPIC,
              resourceName: topicName,
              resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
              principal: 'User:alice',
              host: '*',
              operation: ACL_OPERATION_TYPES.ALL,
              permissionType: ACL_PERMISSION_TYPES.ALLOW,
            },
          ],
        })
      ).resolves.toEqual(true)
      await waitForAclBindings({
        port: await connectAclPort(),
        filter: topicAclFilter(topicName),
        expected: [
          {
            resourceType: ACL_RESOURCE_TYPES.TOPIC,
            resourceName: topicName,
            resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
            principal: 'User:bob',
            host: '*',
            operation: ACL_OPERATION_TYPES.ALL,
            permissionType: ACL_PERMISSION_TYPES.DENY,
          },
          {
            resourceType: ACL_RESOURCE_TYPES.TOPIC,
            resourceName: topicName,
            resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
            principal: 'User:alice',
            host: '*',
            operation: ACL_OPERATION_TYPES.ALL,
            permissionType: ACL_PERMISSION_TYPES.ALLOW,
          },
        ],
      })

      await admin.disconnect()
      admin = createSASLAdminClientForUser({ username: 'bob', password: 'bobbob' })
      await admin.connect()

      await expectTopicAuthorizationFailed(admin.fetchTopicMetadata({ topics: [topicName] }))

      await admin.disconnect()
      admin = createSASLAdminClientForUser({ username: 'alice', password: 'alicealice' })
      await admin.connect()

      await expect(admin.fetchTopicMetadata({ topics: [topicName] })).resolves.toBeTruthy()
    })

    test('can produce to allowed topic after failing to produce to not-allowed topic', async () => {
      const allowedTopic = `allowed-${secureRandom()}`
      const notAllowedTopic = `disallowed-${secureRandom()}`

      admin = createSASLAdminClientForUser({ username: 'test', password: 'testtest' })

      await admin.connect()
      await admin.createTopics({
        waitForLeaders: true,
        topics: [allowedTopic, notAllowedTopic].map((topic) => ({ topic, numPartitions: 1 })),
      })
      const denied = {
        resourceType: ACL_RESOURCE_TYPES.TOPIC,
        resourceName: notAllowedTopic,
        resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
        principal: 'User:bob',
        host: '*',
        operation: ACL_OPERATION_TYPES.WRITE,
        permissionType: ACL_PERMISSION_TYPES.DENY,
      }
      const allowed = {
        ...denied,
        resourceName: allowedTopic,
        permissionType: ACL_PERMISSION_TYPES.ALLOW,
      }
      await expect(admin.createAcls({ acl: [denied, allowed] })).resolves.toEqual(true)
      const port = await connectAclPort()
      for (const binding of [denied, allowed]) {
        await waitForAclBindings({
          port,
          filter: topicAclFilter(binding.resourceName),
          expected: [binding],
        })
      }

      await admin.disconnect()
      const producer = createSASLProducerClientForUser({ username: 'bob', password: 'bobbob' })
      await producer.connect()

      await expect(
        producer.send({ topic: allowedTopic, messages: [{ value: 'hello' }] })
      ).resolves.not.toBeUndefined()
      await expectTopicAuthorizationFailed(
        producer.send({ topic: notAllowedTopic, messages: [{ value: 'whoops' }] })
      )
      await expect(
        producer.send({ topic: allowedTopic, messages: [{ value: 'world' }] })
      ).resolves.not.toBeUndefined()

      await producer.disconnect()
    })
  })
})
