const RequestV5Protocol = require('./request')

/** One Apache 4.4.0 request vector: the bytes Java wrote and the fields it wrote them from. */
const vector = (name) => require(`../fixtures/java/apiVersionsRequest-v5-${name}.json`)

describe('Protocol > Requests > ApiVersions > v5', () => {
  test.each(['defaults', 'known', 'nonAscii'])(
    'encodes the Apache 4.4.0 %s vector byte for byte',
    async (name) => {
      const { hex, fields } = vector(name)
      const { buffer } = await RequestV5Protocol(fields).encode()

      expect(buffer.toString('hex')).toEqual(hex)
    }
  )

  test('names the ApiVersions key at version 5', () => {
    expect(RequestV5Protocol()).toMatchObject({ apiKey: 18, apiVersion: 5, apiName: 'ApiVersions' })
  })

  test('writes the intended cluster and node as a pair after the client software', async () => {
    const { buffer } = await RequestV5Protocol({
      clientSoftwareName: 'n',
      clientSoftwareVersion: 'v',
      clusterId: 'cluster-A',
      nodeId: 2,
    }).encode()

    // 02 6e: "n"; 02 76: "v"; 0a + "cluster-A"; int32 2; empty tag buffer
    expect(buffer.toString('hex')).toEqual('026e0276' + '0a636c75737465722d41' + '00000002' + '00')
  })

  test.each([
    ['a cluster id and no node', 'cluster-A', -1],
    ['a cluster id and a node that is not set', 'cluster-A', null],
    ['a cluster id and a negative node', 'cluster-A', -3],
    ['a cluster id and a node that is not an integer', 'cluster-A', 1.5],
    ['a node and no cluster id', null, 2],
    ['a node and a cluster id that is not set', undefined, 2],
    ['no identity at all', undefined, undefined],
  ])('writes the defaults for both halves when it has %s', async (_what, clusterId, nodeId) => {
    const options = { clientSoftwareName: 'n', clientSoftwareVersion: 'v' }
    const { buffer } = await RequestV5Protocol({ ...options, clusterId, nodeId }).encode()

    // 00: a null cluster id; ffffffff: node -1 (the defaults); empty tag buffer
    expect(buffer.toString('hex')).toEqual('026e0276' + '00' + 'ffffffff' + '00')
  })

  test('writes node 0, which is a node, as itself', async () => {
    const { buffer } = await RequestV5Protocol({
      clientSoftwareName: 'n',
      clientSoftwareVersion: 'v',
      clusterId: 'c',
      nodeId: 0,
    }).encode()

    expect(buffer.toString('hex')).toEqual('026e0276' + '0263' + '00000000' + '00')
  })

  test('defaults to this package identity and no cluster or node', async () => {
    const { CLIENT_SOFTWARE_NAME, clientSoftwareVersion } = require('../../../clientSoftware')
    const { buffer } = await RequestV5Protocol().encode()
    const Decoder = require('../../../decoder')
    const decoder = new Decoder(buffer)

    expect(decoder.readUVarIntString()).toEqual(CLIENT_SOFTWARE_NAME)
    expect(decoder.readUVarIntString()).toEqual(clientSoftwareVersion())
    expect(decoder.readUVarIntString()).toBeNull()
    expect(decoder.readInt32()).toEqual(-1)
    expect(decoder.readTaggedFields().size).toEqual(0)
    expect(decoder.canReadBytes(1)).toBe(false)
  })
})
