const createAdmin = require('../index')

const {
  secureRandom,
  createCluster,
  newLogger,
  saslConnectionOpts,
  saslBrokers,
} = require('testHelpers')
const {
  clusterAclPort,
  closeEach,
  waitForAclBindings,
} = require('../../../testHelpers/aclPropagation')

const ACL_RESOURCE_TYPES = require('../../protocol/aclResourceTypes')
const ACL_OPERATION_TYPES = require('../../protocol/aclOperationTypes')
const ACL_PERMISSION_TYPES = require('../../protocol/aclPermissionTypes')
const RESOURCE_PATTERN_TYPES = require('../../protocol/resourcePatternTypes')

describe('Admin', () => {
  let admin
  let aclPort

  beforeEach(async () => {
    admin = createAdmin({
      logger: newLogger(),
      cluster: createCluster(
        {
          ...saslConnectionOpts(),
          metadataMaxAge: 50,
        },
        saslBrokers()
      ),
    })

    await admin.connect()
  })

  afterEach(async () => {
    const port = aclPort
    aclPort = null
    await closeEach(admin && (() => admin.disconnect()), port && (() => port.close()))
  })

  describe('deleteAcls', () => {
    test('throws an error if the acl filter array is invalid', async () => {
      await expect(admin.deleteAcls({ filters: 'this-is-not-an-array' })).rejects.toHaveProperty(
        'message',
        'Invalid ACL Filter array this-is-not-an-array'
      )
    })

    test('throws an error if the resource name is invalid', async () => {
      const ACLFilter = {
        resourceType: ACL_RESOURCE_TYPES.TOPIC,
        resourceName: 123,
        resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
        principal: 'User:foo',
        host: '*',
        operation: ACL_OPERATION_TYPES.ALL,
        permissionType: ACL_PERMISSION_TYPES.DENY,
      }

      await expect(admin.deleteAcls({ filters: [ACLFilter] })).rejects.toHaveProperty(
        'message',
        'Invalid ACL Filter array, the resourceNames have to be a valid string'
      )
    })

    test('throws an error if the principal name is invalid', async () => {
      const ACLFilter = {
        resourceType: ACL_RESOURCE_TYPES.TOPIC,
        resourceName: 'foo',
        resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
        principal: 123,
        host: '*',
        operation: ACL_OPERATION_TYPES.ALL,
        permissionType: ACL_PERMISSION_TYPES.DENY,
      }

      await expect(admin.deleteAcls({ filters: [ACLFilter] })).rejects.toHaveProperty(
        'message',
        'Invalid ACL Filter array, the principals have to be a valid string'
      )
    })

    test('throws an error if the host name is invalid', async () => {
      const ACLFilter = {
        resourceType: ACL_RESOURCE_TYPES.TOPIC,
        resourceName: 'foo',
        resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
        principal: 'User:foo',
        host: 123,
        operation: ACL_OPERATION_TYPES.ALL,
        permissionType: ACL_PERMISSION_TYPES.DENY,
      }

      await expect(admin.deleteAcls({ filters: [ACLFilter] })).rejects.toHaveProperty(
        'message',
        'Invalid ACL Filter array, the hosts have to be a valid string'
      )
    })

    test('throws an error if there are invalid resource types', async () => {
      const ACLFilter = {
        resourceType: 123,
        resourceName: 'foo',
        resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
        principal: 'User:foo',
        host: '*',
        operation: ACL_OPERATION_TYPES.ALL,
        permissionType: ACL_PERMISSION_TYPES.DENY,
      }

      await expect(admin.deleteAcls({ filters: [ACLFilter] })).rejects.toHaveProperty(
        'message',
        `Invalid resource type 123: ${JSON.stringify(ACLFilter)}`
      )
    })

    test('throws an error if there are invalid resource pattern types', async () => {
      const ACLFilter = {
        resourceType: ACL_RESOURCE_TYPES.TOPIC,
        resourceName: 'foo',
        resourcePatternType: 123,
        principal: 'User:foo',
        host: '*',
        operation: ACL_OPERATION_TYPES.ALL,
        permissionType: ACL_PERMISSION_TYPES.DENY,
      }

      await expect(admin.deleteAcls({ filters: [ACLFilter] })).rejects.toHaveProperty(
        'message',
        `Invalid resource pattern type 123: ${JSON.stringify(ACLFilter)}`
      )
    })

    test('throws an error if there are invalid permission types', async () => {
      const ACLFilter = {
        resourceType: ACL_RESOURCE_TYPES.TOPIC,
        resourceName: 'foo',
        resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
        principal: 'User:foo',
        host: '*',
        operation: ACL_OPERATION_TYPES.ALL,
        permissionType: 123,
      }

      await expect(admin.deleteAcls({ filters: [ACLFilter] })).rejects.toHaveProperty(
        'message',
        `Invalid permission type 123: ${JSON.stringify(ACLFilter)}`
      )
    })

    test('throws an error if there are invalid operation types', async () => {
      const ACLFilter = {
        resourceType: ACL_RESOURCE_TYPES.TOPIC,
        resourceName: 'foo',
        resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
        principal: 'User:foo',
        host: '*',
        operation: 123,
        permissionType: ACL_PERMISSION_TYPES.DENY,
      }

      await expect(admin.deleteAcls({ filters: [ACLFilter] })).rejects.toHaveProperty(
        'message',
        `Invalid operation type 123: ${JSON.stringify(ACLFilter)}`
      )
    })

    test('applies and deletes acl', async () => {
      const topicName = `test-topic-${secureRandom()}`

      await admin.createTopics({
        waitForLeaders: true,
        topics: [{ topic: topicName, numPartitions: 1, replicationFactor: 2 }],
      })

      const bob = {
        resourceType: ACL_RESOURCE_TYPES.TOPIC,
        resourceName: topicName,
        resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
        principal: 'User:bob',
        host: '*',
        operation: ACL_OPERATION_TYPES.ALL,
        permissionType: ACL_PERMISSION_TYPES.DENY,
      }
      const alice = { ...bob, principal: 'User:alice', permissionType: ACL_PERMISSION_TYPES.ALLOW }
      await expect(admin.createAcls({ acl: [bob, alice] })).resolves.toEqual(true)

      // Each broker's authorizer applies the controller's ACL records on its own schedule, so an
      // empty answer after the delete proves nothing until every broker has served the create.
      const { ssl, sasl } = saslConnectionOpts()
      aclPort = await clusterAclPort({ brokers: saslBrokers(), ssl, sasl })
      const topicFilter = {
        resourceType: ACL_RESOURCE_TYPES.TOPIC,
        resourceName: topicName,
        resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
        operation: ACL_OPERATION_TYPES.ANY,
        permissionType: ACL_PERMISSION_TYPES.ANY,
      }
      await waitForAclBindings({ port: aclPort, filter: topicFilter, expected: [bob, alice] })

      await expect(
        admin.deleteAcls({
          filters: [
            {
              resourceName: topicName,
              resourceType: ACL_RESOURCE_TYPES.TOPIC,
              host: '*',
              permissionType: ACL_PERMISSION_TYPES.ALLOW,
              operation: ACL_OPERATION_TYPES.ANY,
              resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
            },
          ],
        })
      ).resolves.toMatchObject({
        filterResponses: [
          {
            errorCode: 0,
            errorMessage: null,
            matchingAcls: [
              {
                errorCode: 0,
                errorMessage: null,
                resourceType: ACL_RESOURCE_TYPES.TOPIC,
                resourceName: topicName,
                resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
                principal: 'User:alice',
                host: '*',
                operation: ACL_OPERATION_TYPES.ALL,
                permissionType: ACL_PERMISSION_TYPES.ALLOW,
              },
            ],
          },
        ],
      })

      await waitForAclBindings({ port: aclPort, filter: topicFilter, expected: [bob] })
      await expect(
        admin.describeAcls({
          ...topicFilter,
          host: '*',
          permissionType: ACL_PERMISSION_TYPES.ALLOW,
        })
      ).resolves.toEqual({ resources: [] })
    })
  })
})
