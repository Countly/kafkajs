const createAdmin = require('../../../../admin')
const { createGroupApiBroker } = require('../../../../broker/__tests__/fixtures/groupApiHarness')
const { KafkaJSPartialMessageError, KafkaJSProtocolError } = require('../../../../errors')
const { newLogger } = require('testHelpers')
const { decode, parse } = require('./response')

const vector = (caseId) => require(`../fixtures/java/createAclsResponse-v3-${caseId}.json`)

describe('Protocol > Requests > CreateAcls > v3 response', () => {
  test.each(['ok', 'error'])('decodes the Apache 4.3.1 %s vector', async (caseId) => {
    const { hex, fields } = vector(caseId)

    expect(await decode(Buffer.from(hex, 'hex'))).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: fields.throttleTime,
      creationResponses: fields.creationResponses,
    })
  })

  test('parse throws the error of the first failed creation', async () => {
    const failed = await decode(Buffer.from(vector('error').hex, 'hex'))

    await expect(parse(failed)).rejects.toMatchObject({ type: 'INVALID_REQUEST', code: 42 })
  })

  test('refuses a null result array, which confirms no creation', async () => {
    const nullResults = Buffer.from('000000000000', 'hex')

    await expect(decode(nullResults)).rejects.toThrow(KafkaJSPartialMessageError)
    await expect(decode(nullResults)).rejects.toThrow('CreateAclsResponse.Results')
  })

  describe('through the admin client', () => {
    const acl = [
      {
        principal: 'User:alice',
        host: '*',
        resourceName: 'orders',
        resourceType: 2,
        resourcePatternType: 3,
        operation: 3,
        permissionType: 2,
      },
    ]

    const createAclsAnswered = async (hex) => {
      const { broker, requests, pool } = createGroupApiBroker({
        versions: { 30: { minVersion: 1, maxVersion: 3 } },
        respond: () => Buffer.from(hex, 'hex'),
      })
      const admin = createAdmin({
        logger: newLogger(),
        retry: { retries: 0 },
        cluster: { refreshMetadata: async () => {}, findControllerBroker: async () => broker },
      })

      try {
        const settled = await admin.createAcls({ acl }).then(
          (value) => ({ value }),
          (error) => ({ error })
        )
        return { ...settled, version: requests[0].apiVersion }
      } finally {
        await pool.destroy()
      }
    }

    test('resolves true when the broker confirms the creation', async () => {
      await expect(createAclsAnswered('00000000020000000000')).resolves.toEqual({
        value: true,
        version: 3,
      })
    })

    test('rejects with the error of a creation the broker refused', async () => {
      const { error, version } = await createAclsAnswered('0000000002001d000000')

      expect(version).toBe(3)
      expect(error).toBeInstanceOf(KafkaJSProtocolError)
    })

    test('rejects a null result array instead of confirming the creation', async () => {
      const { error, value, version } = await createAclsAnswered('000000000000')

      expect(version).toBe(3)
      expect(value).toBeUndefined()
      expect(error).toBeInstanceOf(KafkaJSPartialMessageError)
    })
  })
})
