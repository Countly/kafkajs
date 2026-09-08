const { createGroupApiBroker } = require('./fixtures/groupApiHarness')
const apiKeys = require('../../protocol/requests/apiKeys')
const { requests } = require('../../protocol/requests')

const DIR = '../../protocol/requests/describeGroups/fixtures/java/'
const described = (version) =>
  require(`${DIR}describeGroupsResponse-v${version}-describedGroup.json`)
const NOT_FOUND = require(`${DIR}describeGroupsResponse-v6-groupIdNotFound.json`)

const build = (maxVersion, hex = described(maxVersion).hex) =>
  createGroupApiBroker({
    versions: { [apiKeys.DescribeGroups]: { minVersion: 0, maxVersion } },
    respond: () => Buffer.from(hex, 'hex'),
  })

/**
 * The row's `ErrorCode` sits at a fixed offset in the flexible body: ThrottleTimeMs is four
 * bytes, the groups compact-array count one. Rewriting it in place makes an error frame out of a
 * golden one without adding two more vectors, and the returned frame is asserted to still decode.
 * @param {number} version 5 or 6
 * @param {number} errorCode
 * @returns {Buffer} a copy of the golden body carrying that group-level code
 */
const withGroupError = (version, errorCode) => {
  const framed = Buffer.from(described(version).hex, 'hex')
  framed.writeInt16BE(errorCode, 5)
  return framed
}

describe('Broker > describeGroups versions', () => {
  let harness

  afterEach(async () => {
    if (harness) {
      await harness.pool.destroy()
      harness = null
    }
  })

  test('sends the v6 codec bytes, with IncludeAuthorizedOperations on the wire', async () => {
    harness = build(6)

    await harness.broker.describeGroups({
      groupIds: ['test-group'],
      includeAuthorizedOperations: true,
    })

    const expected = await requests.DescribeGroups.protocol({ version: 6 })({
      groupIds: ['test-group'],
      includeAuthorizedOperations: true,
    }).request.encode()

    expect(harness.requests[0].apiVersion).toEqual(6)
    expect(harness.requests[0].body).toEqual(expected.buffer)
  })

  test('sends the same bytes at v2 as before this change', async () => {
    // The regression guard for every existing caller: v2 is what this client negotiated before
    // v3-v6 existed, and the new option must not reach the wire there.
    harness = build(2, described(3).hex)

    await harness.broker.describeGroups({
      groupIds: ['test-group'],
      includeAuthorizedOperations: true,
    })

    const expected = await requests.DescribeGroups.protocol({ version: 2 })({
      groupIds: ['test-group'],
    }).request.encode()

    expect(harness.requests[0].apiVersion).toEqual(2)
    expect(harness.requests[0].body).toEqual(expected.buffer)
  })

  test('resolves a GROUP_ID_NOT_FOUND row at v6, keeping the row shape callers have always read', async () => {
    harness = build(6, NOT_FOUND.hex)

    const response = await harness.broker.describeGroups({ groupIds: ['test-group'] })

    expect(response.groups[0]).toMatchObject({
      errorCode: 69,
      errorMessage: NOT_FOUND.fields.groups[0].errorMessage,
      groupId: 'test-group',
      state: 'Dead',
      protocol: '',
      protocolType: '',
      members: [],
    })
  })

  test('rejects a retriable coordinator code at v6, so the caller retries as it always has', async () => {
    // COORDINATOR_NOT_AVAILABLE (15). Both callers of this request wrap it in a retrier and
    // depend on the throw; a row would be a silent behaviour change on a public API.
    harness = build(6, withGroupError(6, 15).toString('hex'))

    await expect(harness.broker.describeGroups({ groupIds: ['test-group'] })).rejects.toThrow(
      'The group coordinator is not available'
    )
  })

  test('rejects a group error at v5, where the contract predates per-group reporting', async () => {
    harness = build(5, withGroupError(5, 15).toString('hex'))

    await expect(harness.broker.describeGroups({ groupIds: ['test-group'] })).rejects.toThrow(
      'The group coordinator is not available'
    )
  })

  test('rejects GROUP_ID_NOT_FOUND at v5, which is what v6 changes', async () => {
    harness = build(5, withGroupError(5, 69).toString('hex'))

    await expect(harness.broker.describeGroups({ groupIds: ['test-group'] })).rejects.toThrow()
  })

  test('carries the new fields at v6 and every 2.2.4 field with them', async () => {
    harness = build(6)

    const response = await harness.broker.describeGroups({ groupIds: ['test-group'] })
    const [group] = response.groups

    expect(group).toMatchObject({
      errorCode: 0,
      errorMessage: null,
      groupId: 'test-group',
      state: 'Stable',
      protocolType: 'consumer',
      protocol: 'RoundRobinAssigner',
      authorizedOperations: -2147483648,
    })
    expect(group.members[0]).toMatchObject({
      memberId: 'test-member',
      groupInstanceId: 'test-instance',
      clientId: 'test-client',
      clientHost: '/127.0.0.1',
    })
  })

  test('the mutated error frames differ from the goldens only in the group error code', async () => {
    // The fixed offset the two rejection cases rely on, asserted rather than assumed: decoding a
    // mutated frame reproduces the golden row with one field changed.
    const golden = await requests.DescribeGroups.protocol({ version: 6 })().response.decode(
      Buffer.from(described(6).hex, 'hex')
    )
    const mutated = await requests.DescribeGroups.protocol({ version: 6 })().response.decode(
      withGroupError(6, 15)
    )

    expect(mutated).toEqual({
      ...golden,
      groups: [{ ...golden.groups[0], errorCode: 15 }],
    })
  })
})
