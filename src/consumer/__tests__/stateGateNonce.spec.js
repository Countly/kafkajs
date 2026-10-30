const { secureRandom } = require('testHelpers')
const { gateNonce } = require('./fixtures/gateNonce')

const GUARD_NONCE = /^[a-zA-Z0-9_-]{1,64}$/

describe('Consumer > state gate nonce', () => {
  it('stays within the guard limit for real secureRandom ids', () => {
    for (let i = 0; i < 50; i++) {
      const id = secureRandom()
      const nonce = gateNonce(id)
      const control = gateNonce(id, 'c')
      expect(nonce).toMatch(GUARD_NONCE)
      expect(control).toMatch(GUARD_NONCE)
      expect(control).not.toEqual(nonce)
    }
  })

  it('is unique per id', () => {
    expect(gateNonce(secureRandom())).not.toEqual(gateNonce(secureRandom()))
  })

  it('rejects the old unbounded form', () => {
    expect(`n${secureRandom()}`).not.toMatch(GUARD_NONCE)
  })
})
