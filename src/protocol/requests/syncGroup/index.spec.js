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
      it('derives the request timeout from the rebalance timeout', () => {
        const { requestTimeout } = build(version, {
          rebalanceTimeout: 120000,
          sessionTimeout: 60000,
        })
        expect(requestTimeout).toEqual(120000 + NETWORK_DELAY)
      })

      it('falls back to the session timeout when there is no rebalance timeout', () => {
        const { requestTimeout } = build(version, { sessionTimeout: 60000 })
        expect(requestTimeout).toEqual(60000 + NETWORK_DELAY)
      })

      it('does not overflow past MAX_SAFE_INTEGER', () => {
        const { requestTimeout } = build(version, { rebalanceTimeout: Number.MAX_SAFE_INTEGER })
        expect(requestTimeout).toEqual(Number.MAX_SAFE_INTEGER)
      })

      it('leaves the timeout unset when neither is supplied (client default applies)', () => {
        expect(build(version).requestTimeout).toBeUndefined()
      })
    })
  })
})
