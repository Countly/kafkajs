const { createGroupApiBroker } = require('../../../broker/__tests__/fixtures/groupApiHarness')
const { KafkaJSInvariantViolation } = require('../../../errors')
const apiKeys = require('../apiKeys')
const responseV8 = require('./v8/response')
const responseV9 = require('./v9/response')

const CASES = ['groupMatch', 'groupMismatch', 'twoGroups', 'noGroups']
const CODECS = { 8: responseV8, 9: responseV9 }

/**
 * @param {number} version
 * @param {string} name one of `CASES`
 * @returns {{ hex: string, fields: object }} the vector the Apache 4.3.1 codec wrote
 */
const load = (version, name) =>
  require(`./fixtures/java/offsetFetchResponse-v${version}-${name}.json`)

/** @returns {object} the partition the single-group vectors carry, as the response codec decodes it */
const committed = () => ({
  topic: 'test-topic',
  partitions: [
    { partition: 0, offset: '10', committedLeaderEpoch: 3, metadata: 'read-check', errorCode: 0 },
  ],
})

/**
 * @param {number} version
 * @param {string} name vector case
 * @returns {{ broker: object, requests: object[], pool: object }} a broker answering with the vector
 */
const brokerAnswering = (version, name) =>
  createGroupApiBroker({
    versions: { [apiKeys.OffsetFetch]: { minVersion: version, maxVersion: version } },
    respond: () => Buffer.from(load(version, name).hex, 'hex'),
  })

describe('Protocol > Requests > OffsetFetch > Java identity vectors', () => {
  let harness
  afterEach(async () => {
    if (harness) await harness.pool.destroy()
    harness = null
  })

  describe.each([8, 9])('version %i', (version) => {
    test.each(['groupMatch', 'groupMismatch'])(
      'decodes the %s vector with its group and non-null metadata',
      async (name) => {
        const { fields, hex } = load(version, name)
        const data = await CODECS[version].decode(Buffer.from(hex, 'hex'))

        expect(data.groupId).toBe(fields.groupId)
        expect(data.clientSideThrottleTime).toBe(fields.throttleTime)
        expect(data.responses).toEqual([committed()])
      }
    )

    test.each(['twoGroups', 'noGroups'])(
      'refuses the %s vector, since one group is asked about',
      async (name) => {
        await expect(
          CODECS[version].decode(Buffer.from(load(version, name).hex, 'hex'))
        ).rejects.toBeInstanceOf(KafkaJSInvariantViolation)
      }
    )

    test('hands the matching vector to the caller that asked for that group', async () => {
      harness = brokerAnswering(version, 'groupMatch')
      const response = await harness.broker.offsetFetch({
        groupId: 'test-group',
        topics: [{ topic: 'test-topic', partitions: [{ partition: 0 }] }],
      })

      expect(response.groupId).toBe('test-group')
      expect(response.responses).toEqual([committed()])
    })

    test('refuses the mismatching vector naming both groups', async () => {
      harness = brokerAnswering(version, 'groupMismatch')
      const error = await harness.broker
        .offsetFetch({
          groupId: 'test-group',
          topics: [{ topic: 'test-topic', partitions: [{ partition: 0 }] }],
        })
        .catch((failure) => failure)

      expect(error).toBeInstanceOf(KafkaJSInvariantViolation)
      expect(error.message).toContain('test-group')
      expect(error.message).toContain('other-group')
    })

    test('accepts the mismatching vector for the group it names', async () => {
      harness = brokerAnswering(version, 'groupMismatch')
      const response = await harness.broker.offsetFetch({
        groupId: 'other-group',
        topics: [{ topic: 'test-topic', partitions: [{ partition: 0 }] }],
      })

      expect(response.groupId).toBe('other-group')
    })

    test.each(['twoGroups', 'noGroups'])(
      'refuses the %s vector whichever group was asked about',
      async (name) => {
        for (const groupId of ['test-group', 'other-group']) {
          harness = brokerAnswering(version, name)
          await expect(harness.broker.offsetFetch({ groupId, topics: [] })).rejects.toBeInstanceOf(
            KafkaJSInvariantViolation
          )
          await harness.pool.destroy()
          harness = null
        }
      }
    )
  })

  test('shares one layout between the two versions: the bytes are equal case by case', () => {
    for (const name of CASES) expect(load(8, name).hex).toBe(load(9, name).hex)
  })

  test.each(CASES)('keeps the %s fields in step with the bytes the generator wrote', (name) => {
    const { fields, hex } = load(9, name)
    const groups = fields.groups ?? [fields]

    for (const { groupId } of groups) expect(hex).toContain(Buffer.from(groupId).toString('hex'))
    expect(hex.startsWith(fields.throttleTime.toString(16).padStart(8, '0'))).toBe(true)
  })
})
