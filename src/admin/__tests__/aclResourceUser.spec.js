const createAdmin = require('../index')
const apiKeys = require('../../protocol/requests/apiKeys')
const { AclResourceTypes } = require('../../../index')
const { createGroupApiBroker } = require('../../broker/__tests__/fixtures/groupApiHarness')
const { newLogger } = require('testHelpers')

const DIR = '../../protocol/requests/'
const vector = (api, kind, caseId) =>
  require(`${DIR}${api}/fixtures/java/${api}${kind}-v3-${caseId}.json`)

describe('Admin > ACLs on a USER resource', () => {
  let harness

  /**
   * An admin over one real broker on a fake socket that offers one ACL API up to v3 and answers
   * every request with `hex`. The harness serves a single request, and each case sends one.
   */
  const adminOver = (apiKey, hex) => {
    harness = createGroupApiBroker({
      versions: { [apiKey]: { minVersion: 1, maxVersion: 3 } },
      respond: () => Buffer.from(hex, 'hex'),
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
    if (harness) {
      await harness.pool.destroy()
      harness = null
    }
  })

  test('USER is resource type 7, next to the types a 3.x broker knows', () => {
    expect(AclResourceTypes.USER).toBe(7)
    expect(AclResourceTypes.DELEGATION_TOKEN).toBe(6)
  })

  test('createAcls accepts it and sends it at v3', async () => {
    const admin = adminOver(apiKeys.CreateAcls, vector('createAcls', 'Response', 'ok').hex)
    const { hex, fields } = vector('createAcls', 'Request', 'userResource')

    await expect(admin.createAcls({ acl: fields.creations })).resolves.toBe(true)
    expect(harness.requests[0]).toMatchObject({ apiKey: 30, apiVersion: 3 })
    expect(harness.requests[0].body.toString('hex')).toEqual(hex)
  })

  test('describeAcls accepts it and sends it at v3', async () => {
    const response = vector('describeAcls', 'Response', 'userResource')
    const admin = adminOver(apiKeys.DescribeAcls, response.hex)
    const { hex, fields } = vector('describeAcls', 'Request', 'userResource')

    await expect(admin.describeAcls(fields)).resolves.toEqual({
      resources: response.fields.resources,
    })
    expect(harness.requests[0]).toMatchObject({ apiKey: 29, apiVersion: 3 })
    expect(harness.requests[0].body.toString('hex')).toEqual(hex)
  })

  test('deleteAcls accepts it and sends it at v3', async () => {
    const response = vector('deleteAcls', 'Response', 'userResource')
    const admin = adminOver(apiKeys.DeleteAcls, response.hex)
    const { hex, fields } = vector('deleteAcls', 'Request', 'userResource')

    await expect(admin.deleteAcls({ filters: fields.filters })).resolves.toEqual({
      filterResponses: response.fields.filterResponses,
    })
    expect(harness.requests[0]).toMatchObject({ apiKey: 31, apiVersion: 3 })
    expect(harness.requests[0].body.toString('hex')).toEqual(hex)
  })

  test('the three validators still refuse the type after it', async () => {
    const admin = createAdmin({ logger: newLogger(), cluster: {} })
    const { fields: creation } = vector('createAcls', 'Request', 'userResource')
    const { fields: description } = vector('describeAcls', 'Request', 'userResource')
    const { fields: deletion } = vector('deleteAcls', 'Request', 'userResource')

    await expect(
      admin.createAcls({ acl: [{ ...creation.creations[0], resourceType: 8 }] })
    ).rejects.toThrow('Invalid resource type 8')
    await expect(admin.describeAcls({ ...description, resourceType: 8 })).rejects.toThrow(
      'Invalid resource type 8'
    )
    await expect(
      admin.deleteAcls({ filters: [{ ...deletion.filters[0], resourceType: 8 }] })
    ).rejects.toThrow('Invalid resource type 8')
  })
})
