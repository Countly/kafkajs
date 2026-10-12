const apiKeys = require('../apiKeys')
const { lookup, requests } = require('../index')
const { versions, protocol } = require('./index')

const NETWORK_DELAY = 5000

const build = (version, overrides = {}) =>
  protocol({ version })({
    groupId: 'test-group',
    generationId: 1,
    memberId: 'member-1',
    groupAssignment: [],
    ...overrides,
  })

describe('Protocol > Requests > SyncGroup', () => {
  versions.forEach((version) => {
    describe(`v${version}`, () => {
      it('caps the request timeout at 60 s plus the network delay', () => {
        const { requestTimeout } = build(version, {
          rebalanceTimeout: 120000,
          sessionTimeout: 60000,
        })
        expect(requestTimeout).toEqual(65000)
      })

      it('falls back to the session timeout when there is no rebalance timeout', () => {
        const { requestTimeout } = build(version, { sessionTimeout: 60000 })
        expect(requestTimeout).toEqual(60000 + NETWORK_DELAY)
      })

      it('caps a MAX_SAFE_INTEGER rebalance timeout rather than overflowing', () => {
        const { requestTimeout } = build(version, { rebalanceTimeout: Number.MAX_SAFE_INTEGER })
        expect(requestTimeout).toEqual(65000)
      })

      it('leaves the timeout unset when neither is supplied (client default applies)', () => {
        expect(build(version).requestTimeout).toBeUndefined()
      })
    })
  })

  it('offers v4 and v5, and a broker offering v0-5 is asked at v5', () => {
    const brokerVersions = { [apiKeys.SyncGroup]: { minVersion: 0, maxVersion: 5 } }
    const { request } = lookup(brokerVersions)(apiKeys.SyncGroup, requests.SyncGroup)({
      groupId: 'test-group',
      generationId: 1,
      memberId: 'member-1',
      groupAssignment: [],
    })

    expect(versions).toEqual(['0', '1', '2', '3', '4', '5'])
    expect(request.apiVersion).toEqual(5)
  })
})
