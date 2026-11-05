const ApiVersions = require('./index')
const Encoder = require('../../encoder')
const { CLIENT_SOFTWARE_NAME, clientSoftwareVersion } = require('../../clientSoftware')
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

      const software = new Encoder()
        .writeUVarIntString(CLIENT_SOFTWARE_NAME)
        .writeUVarIntString(clientSoftwareVersion()).buffer

      // v5 writes the pair after the software: the cluster id as a compact string, the node as an
      // int32, then the empty tag buffer; without one it writes the null cluster id and node -1.
      expect(await bytesOf(5, identity)).toEqual(
        software.toString('hex') + '0a636c75737465722d41' + '00000002' + '00'
      )
      expect(await bytesOf(5)).toEqual(software.toString('hex') + '00' + 'ffffffff' + '00')
      for (const version of [3, 4]) {
        expect(await bytesOf(version, identity)).toEqual(await bytesOf(version))
        expect(await bytesOf(version, identity)).toEqual(software.toString('hex') + '00')
      }
    })
  })
})
