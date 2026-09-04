const ApiVersions = require('./index')
const ApiVersionsV0 = ApiVersions.protocol({ version: 0 })
const ApiVersionsV1 = ApiVersions.protocol({ version: 1 })
const ApiVersionsV2 = ApiVersions.protocol({ version: 2 })
const ApiVersionsV3 = ApiVersions.protocol({ version: 3 })
const ApiVersionsV4 = ApiVersions.protocol({ version: 4 })

describe('Protocol > Requests > ApiVersions', () => {
  describe('V0', () => {
    it('logs response errors', () => {
      const protocolV0 = ApiVersionsV0()
      expect(protocolV0.logResponseError).toBe(true)
    })
  })

  describe('V1+', () => {
    it('does not log response errors', () => {
      const protocolV1 = ApiVersionsV1()
      const protocolV2 = ApiVersionsV2()
      const protocolV3 = ApiVersionsV3()
      const protocolV4 = ApiVersionsV4()

      expect(protocolV1.logResponseError).toBe(false)
      expect(protocolV2.logResponseError).toBe(false)
      expect(protocolV3.logResponseError).toBe(false)
      expect(protocolV4.logResponseError).toBe(false)
    })
  })
})
