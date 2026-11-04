const fs = require('fs')
const path = require('path')
const assert = require('assert')
const Broker = require('../../broker')
const { MemberMetadata } = require('../assignerProtocol')
const { KafkaJSProtocolError } = require('../../errors')
const { createConnectionPool, newLogger, secureRandom } = require('testHelpers')
const { readBrokerVersion } = require('../../../testHelpers/brokerVersion')
const { writeEvidence } = require('../../../testHelpers/e2eEvidence')

/**
 * Selects an independently reviewed expectation; a missing key is a failed acceptance.
 * @param {object} fixture immutable reviewed capture table
 * @param {string} version broker-reported version
 * @returns {object} the literal expectation
 * @throws {Error} when the version has no reviewed entry
 */
const expectedFor = (fixture, version) => {
  if (!Object.hasOwn(fixture, version))
    throw new Error(`No reviewed completing-rebalance capture for ${version}`)
  return fixture[version]
}

/** @param {object} actual @param {object} fixture @param {string} version @returns {void} @throws {Error} on disagreement */
const compare = (actual, fixture, version) =>
  assert.deepStrictEqual(actual, expectedFor(fixture, version))

describe('Completing rebalance capture oracle', () => {
  test('selects the observed broker version independently', () => {
    const fixture = {
      '4.3.1': { errorCode: 27, type: 'REBALANCE_IN_PROGRESS' },
      '4.2.2': { errorCode: 0, type: null },
    }
    compare({ errorCode: 27, type: 'REBALANCE_IN_PROGRESS' }, fixture, '4.3.1')
    compare({ errorCode: 0, type: null }, fixture, '4.2.2')
    expect(fixture['4.3.1'].errorCode).toBe(27)
  })
  test('missing versions fail and a prototype property cannot supply an expectation', () => {
    expect(() => expectedFor({}, '4.1.2')).toThrow('No reviewed')
    expect(() => expectedFor({}, 'constructor')).toThrow('No reviewed')
  })
  test('the answer cannot become its own expectation', () => {
    const fixture = { '4.3.1': { errorCode: 27, type: 'REBALANCE_IN_PROGRESS' } }
    expect(() => compare({ errorCode: 0, type: null }, fixture, '4.3.1')).toThrow()
  })
})

const live = process.env.COMPLETING_REBALANCE_E2E === '1' ? test : test.skip
live('raw classic follower heartbeat is pinned during the proven sync window', async () => {
  const groupId = `e2e-p9a-completing-${secureRandom()}`
  const seed = new Broker({ connectionPool: createConnectionPool(), logger: newLogger() })
  const members = []
  const joined = []
  let syncRequests = 0
  const original = Broker.prototype.syncGroup
  const sync = jest.spyOn(Broker.prototype, 'syncGroup').mockImplementation(function (...args) {
    syncRequests++
    return original.apply(this, args)
  })
  try {
    await seed.connect()
    const { coordinator } = await seed.findGroupCoordinator({ groupId })
    for (let index = 0; index < 2; index++) {
      const member = new Broker({
        connectionPool: createConnectionPool(coordinator),
        logger: newLogger(),
      })
      members.push(member)
      await member.connect()
    }
    const join = (member, memberId) =>
      member.joinGroup({
        groupId,
        memberId,
        sessionTimeout: 30000,
        rebalanceTimeout: 30000,
        groupProtocols: [
          { name: 'capture', metadata: MemberMetadata.encode({ version: 1, topics: [] }) },
        ],
      })
    // With a zero initial rebalance delay a lone first join completes at once, so two concurrent
    // first joins can land in different generations. A known member is always awaited: the first
    // member joins alone, then rejoins while the second joins.
    const { memberId: firstId } = await join(members[0])
    const responses = await Promise.all([join(members[0], firstId), join(members[1])])
    joined.push(...responses)
    expect(responses[0].generationId).toBe(responses[1].generationId)
    expect(responses[0].memberId).not.toBe(responses[1].memberId)
    const followerIndex = responses.findIndex((response) => response.memberId !== response.leaderId)
    expect(followerIndex).toBeGreaterThanOrEqual(0)
    const description = await members[0].describeGroups({ groupIds: [groupId] })
    expect(description.groups[0].state).toBe('CompletingRebalance')
    expect(syncRequests).toBe(0)
    const follower = responses[followerIndex]
    let actual
    try {
      const response = await members[followerIndex].heartbeat({
        groupId,
        groupGenerationId: follower.generationId,
        memberId: follower.memberId,
      })
      actual = { errorCode: response.errorCode, type: null }
    } catch (error) {
      if (!(error instanceof KafkaJSProtocolError)) throw error
      actual = { errorCode: error.code, type: error.type }
    }
    expect(syncRequests).toBe(0)
    const version = readBrokerVersion()
    if (process.env.COMPLETING_REBALANCE_CAPTURE === '1') {
      const candidate = path.resolve('.tmp/e2e', `completing-rebalance-candidate-${version}.json`)
      fs.mkdirSync(path.dirname(candidate), { recursive: true })
      fs.writeFileSync(candidate, JSON.stringify({ [version]: actual }, null, 2) + '\n')
    } else {
      const fixture = JSON.parse(
        fs.readFileSync(path.join(__dirname, 'fixtures/completingRebalance.json'), 'utf8')
      )
      compare(actual, fixture, version)
    }
    writeEvidence('completing-rebalance', {
      groupId,
      generationId: follower.generationId,
      window: 'CompletingRebalance',
      leaderSyncRequests: syncRequests,
      actual,
      capture: process.env.COMPLETING_REBALANCE_CAPTURE === '1',
    })
  } finally {
    for (let index = 0; index < joined.length; index++)
      await members[index].leaveGroup({ groupId, memberId: joined[index].memberId }).catch(() => {})
    await Promise.all(members.map((member) => member.disconnect()))
    await seed.disconnect()
    sync.mockRestore()
  }
})
