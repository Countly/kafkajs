const { requests, lookup } = require('./index')
const apiKeys = require('./apiKeys')
const { isFlexible, requestHeaderVersion, responseHeaderVersion } = require('../headers')
const { KafkaJSServerDoesNotSupportApiKey } = require('../../errors')
const rows = [
  ['ShareGroupHeartbeat', 76, 1],
  ['ShareGroupDescribe', 77, 1],
  ['ShareFetch', 78, 2],
  ['ShareAcknowledge', 79, 2],
  ['DescribeShareGroupOffsets', 90, 1],
  ['AlterShareGroupOffsets', 91, 0],
  ['DeleteShareGroupOffsets', 92, 0],
]

describe('released share codec versions', () => {
  test.each(rows)('%s selects only its released version', (name, key, version) => {
    expect(apiKeys[name]).toBe(key)
    expect(requests[name].versions.map(Number)).toEqual([version])
    const factory = lookup({ [key]: { minVersion: version, maxVersion: version } })(
      key,
      requests[name]
    )
    expect(factory().request).toMatchObject({ apiKey: key, apiName: name, apiVersion: version })
    expect(isFlexible(key, 0)).toBe(true)
    expect(requestHeaderVersion(key, version)).toBe(2)
    expect(responseHeaderVersion(key, version)).toBe(1)
  })
  test.each(rows)('%s refuses an unadvertised key by name', (name, key) => {
    let error
    try {
      lookup({})(key, requests[name])
    } catch (caught) {
      error = caught
    }
    expect(error).toBeInstanceOf(KafkaJSServerDoesNotSupportApiKey)
    expect(error).toMatchObject({ apiKey: key, apiName: name, retriable: false })
  })
  test.each([
    ['ShareFetch', 78, 1],
    ['ShareAcknowledge', 79, 1],
    ['DescribeShareGroupOffsets', 90, 0],
  ])('%s refuses preview-only range', (name, key, version) => {
    expect(() =>
      lookup({ [key]: { minVersion: version, maxVersion: version } })(key, requests[name])
    ).toThrow(KafkaJSServerDoesNotSupportApiKey)
  })
})

describe('released share request header bytes', () => {
  const encodeRequest = require('../request')
  const vectors = [
    [
      'AlterShareGroupOffsets',
      91,
      0,
      'alterShareGroupOffsets',
      'alterShareGroupOffsetsRequest-v0-offsets.json',
    ],
    [
      'DeleteShareGroupOffsets',
      92,
      0,
      'deleteShareGroupOffsets',
      'deleteShareGroupOffsetsRequest-v0-topics.json',
    ],
    [
      'DescribeShareGroupOffsets',
      90,
      1,
      'describeShareGroupOffsets',
      'describeShareGroupOffsetsRequest-v1-emptyTopics.json',
    ],
    ['ShareAcknowledge', 79, 2, 'shareAcknowledge', 'shareAcknowledgeRequest-v2-final.json'],
    ['ShareFetch', 78, 2, 'shareFetch', 'shareFetchRequest-v2-close.json'],
    [
      'ShareGroupDescribe',
      77,
      1,
      'shareGroupDescribe',
      'shareGroupDescribeRequest-v1-authorized.json',
    ],
    [
      'ShareGroupHeartbeat',
      76,
      1,
      'shareGroupHeartbeat',
      'shareGroupHeartbeatRequest-v1-emptyTopics.json',
    ],
  ]
  test.each(vectors)(
    '%s uses the flexible header and classic client ID',
    async (name, key, version, directory, file) => {
      const vector = require('./' + directory + '/fixtures/java/' + file)
      const body = Buffer.from(vector.hex, 'hex')
      const header = Buffer.alloc(16)
      header.writeInt32BE(12 + body.length, 0)
      header.writeInt16BE(key, 4)
      header.writeInt16BE(version, 6)
      header.writeInt32BE(123, 8)
      header.writeInt16BE(1, 12)
      header[14] = 120
      header[15] = 0
      const { request } = requests[name].protocol({ version })(vector.fields)
      const frame = await encodeRequest({ correlationId: 123, clientId: 'x', request })
      expect(frame.buffer).toEqual(Buffer.concat([header, body]))
    }
  )
})
