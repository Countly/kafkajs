const ApiVersions = require('./index')
const ApiVersionsV0 = ApiVersions.protocol({ version: 0 })
const ApiVersionsV1 = ApiVersions.protocol({ version: 1 })
const ApiVersionsV2 = ApiVersions.protocol({ version: 2 })
const ApiVersionsV3 = ApiVersions.protocol({ version: 3 })
const ApiVersionsV4 = ApiVersions.protocol({ version: 4 })
const ApiVersionsV5 = ApiVersions.protocol({ version: 5 })

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
      const protocolV5 = ApiVersionsV5()

      expect(protocolV1.logResponseError).toBe(false)
      expect(protocolV2.logResponseError).toBe(false)
      expect(protocolV3.logResponseError).toBe(false)
      expect(protocolV4.logResponseError).toBe(false)
      expect(protocolV5.logResponseError).toBe(false)
    })
  })

  describe('versions', () => {
    it('offers v0 to v5', () => {
      expect(ApiVersions.versions).toEqual(['0', '1', '2', '3', '4', '5'])
    })

    it('hands the identity to v5 only', async () => {
      const identity = { clusterId: 'cluster-A', nodeId: 2 }
      const bytesOf = async (version, ...args) =>
        (await ApiVersions.protocol({ version })(...args).request.encode()).buffer.toString('hex')

      expect(await bytesOf(5, identity)).not.toEqual(await bytesOf(5))
      for (const version of [3, 4]) {
        expect(await bytesOf(version, identity)).toEqual(await bytesOf(version))
      }
    })
  })
})
