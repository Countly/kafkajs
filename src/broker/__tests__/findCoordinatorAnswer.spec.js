const { createGroupApiBroker } = require('./fixtures/groupApiHarness')
const apiKeys = require('../../protocol/requests/apiKeys')
const COORDINATOR_TYPES = require('../../protocol/coordinatorTypes')
const Encoder = require('../../protocol/encoder')
const findCoordinatorV6 = require('../../../testHelpers/wire/findCoordinatorV6')

const GROUP = 'group-A'
const NODE = { nodeId: 1, host: 'broker-1', port: 9092 }
const NO_NODE = { nodeId: -1, host: '', port: -1 }

/**
 * @param {object} [fields] over a coordinator for `GROUP` at `NODE` with no error
 * @returns {{ key: string|null, nodeId: number, host: string|null, port: number, errorCode: number,
 *   errorMessage: null }} one coordinator entry
 */
const coordinator = (fields = {}) => ({
  key: GROUP,
  ...NODE,
  errorCode: 0,
  errorMessage: null,
  ...fields,
})

/**
 * The bytes of an answer in the layout of `version`: v3 carries one coordinator and no key, v4 to v6
 * carry a list of keyed entries.
 * @param {number} version
 * @param {object} fields over `coordinator()`
 * @returns {Buffer}
 */
const answer = (version, fields) => {
  const entry = coordinator(fields)
  if (version >= 4) {
    return findCoordinatorV6.buildResponse({ throttleTime: 0, coordinators: [entry] })
  }

  return new Encoder()
    .writeInt32(0)
    .writeInt16(entry.errorCode)
    .writeUVarIntString(null)
    .writeInt32(entry.nodeId)
    .writeUVarIntString(entry.host)
    .writeInt32(entry.port)
    .writeTaggedFields().buffer
}

describe('Broker > findGroupCoordinator answer', () => {
  const harnesses = []

  afterEach(async () => {
    await Promise.all(harnesses.splice(0).map(({ pool }) => pool.destroy()))
  })

  /**
   * One lookup answered by a real broker over a real connection, at `version`.
   * @param {number} version
   * @param {object} [fields] over `coordinator()`
   * @param {{ groupId?: string, coordinatorType?: number }} [asked]
   * @returns {Promise<object>}
   */
  const lookUp = (
    version,
    fields = {},
    { groupId = GROUP, coordinatorType = COORDINATOR_TYPES.GROUP } = {}
  ) => {
    const harness = createGroupApiBroker({
      versions: { [apiKeys.GroupCoordinator]: { minVersion: 0, maxVersion: version } },
      respond: () => answer(version, fields),
    })
    harnesses.push(harness)
    return harness.broker.findGroupCoordinator({ groupId, coordinatorType })
  }

  /** @returns {number[]} the FindCoordinator versions the latest broker was asked in */
  const versionsSent = () => harnesses[harnesses.length - 1].requests.map((r) => r.apiVersion)

  test.each([
    ['another key', 'group-B'],
    ['a null key', null],
  ])('a v4 to v6 answer for %s is refused retriably', async (_, key) => {
    for (const version of [4, 5, 6]) {
      await expect(lookUp(version, { key })).rejects.toMatchObject({
        name: 'KafkaJSError',
        retriable: true,
        message: expect.stringMatching(/^FindCoordinator response is for key .*group-A/),
      })
      expect(versionsSent()).toEqual([version])
    }
  })

  test.each([
    ['a node id of -1', { nodeId: -1 }],
    ['an empty host', { host: '' }],
    ['a null host', { host: null }],
    ['a port of -1', { port: -1 }],
    ['a port of 0', { port: 0 }],
    ['a port above 65535', { port: 65536 }],
  ])('a success with %s is refused retriably at v3, v4 and v6', async (_, fields) => {
    for (const version of [3, 4, 6]) {
      await expect(lookUp(version, fields)).rejects.toMatchObject({
        name: 'KafkaJSError',
        retriable: true,
        message: expect.stringMatching(/^FindCoordinator response names an unusable coordinator/),
      })
      expect(versionsSent()).toEqual([version])
    }
  })

  test('an answer without a key at v3 and an answer that echoes the key at v4 to v6 resolve with a usable node', async () => {
    for (const version of [3, 4, 5, 6]) {
      const found = await lookUp(version)

      expect(found.coordinator).toEqual(NODE)
    }
  })

  test('the smallest and the largest usable node resolve', async () => {
    for (const fields of [
      { nodeId: 0, port: 1 },
      { nodeId: 2147483647, port: 65535 },
    ]) {
      for (const version of [3, 4, 6]) {
        const found = await lookUp(version, fields)

        expect(found.coordinator).toEqual({ ...NODE, ...fields })
      }
    }
  })

  test('a transaction coordinator lookup that echoes its transactional id resolves', async () => {
    const found = await lookUp(
      4,
      { key: 'txn-a' },
      { groupId: 'txn-a', coordinatorType: COORDINATOR_TYPES.TRANSACTION }
    )

    expect(found.coordinator).toEqual(NODE)
  })

  test('an error answer with no node, the key echoed at v4 to v6, is reported as its protocol error, not as a coverage error', async () => {
    for (const version of [3, 4, 6]) {
      await expect(lookUp(version, { ...NO_NODE, errorCode: 15 })).rejects.toMatchObject({
        name: 'KafkaJSProtocolError',
        type: 'GROUP_COORDINATOR_NOT_AVAILABLE',
        code: 15,
      })
    }
  })
})
