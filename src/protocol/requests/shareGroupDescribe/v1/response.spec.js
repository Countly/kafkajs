const { plain } = require('../../../__tests__/fixtures/fuzzResponseVectors')
const fs = require('fs')
const path = require('path')
const codec = require('./response')
const { KafkaJSPartialMessageError, KafkaJSInvalidVarIntError } = require('../../../../errors')

const files = [
  'shareGroupDescribeResponse-v1-mixed.json',
  'shareGroupDescribeResponse-v1-unknownTags.json',
]

describe('independent Java response bytes', () => {
  test.each(files)('decodes %s to its literal fields', async (name) => {
    const vector = JSON.parse(
      fs.readFileSync(path.join(__dirname, '../fixtures/java', name), 'utf8')
    )
    const decoded = await codec.decode(Buffer.from(vector.hex, 'hex'))
    expect(decoded).toMatchObject({
      throttleTime: 0,
      clientSideThrottleTime: vector.fields.throttleTime,
    })
    expect(plain(decoded)).toEqual(vector.fields)
    expect(await codec.parse(decoded)).toBe(decoded)
  })
  test.each(files)('rejects every truncated envelope of %s', async (name) => {
    const vector = JSON.parse(
      fs.readFileSync(path.join(__dirname, '../fixtures/java', name), 'utf8')
    )
    const bytes = Buffer.from(vector.hex, 'hex')
    for (let length = 0; length < bytes.length; length++) {
      await expect(codec.decode(bytes.subarray(0, length))).rejects.toBeInstanceOf(
        KafkaJSPartialMessageError
      )
    }
  })
  test('rejects a trailing byte after a valid response', async () => {
    const vector = JSON.parse(
      fs.readFileSync(path.join(__dirname, '../fixtures/java', files[0]), 'utf8')
    )
    await expect(
      codec.decode(Buffer.concat([Buffer.from(vector.hex, 'hex'), Buffer.from([0])]))
    ).rejects.toBeInstanceOf(KafkaJSPartialMessageError)
  })
})

test('rejects null for a required array', async () => {
  const bytes = Buffer.from(
    require('../fixtures/java/shareGroupDescribeResponse-v1-mixed.json').hex,
    'hex'
  )
  bytes[4] = 0
  await expect(codec.decode(bytes)).rejects.toBeInstanceOf(KafkaJSPartialMessageError)
})

test('rejects a compact length beyond unsigned int32', async () => {
  const bytes = Buffer.from(
    require('../fixtures/java/shareGroupDescribeResponse-v1-mixed.json').hex,
    'hex'
  )
  const malformed = Buffer.concat([
    bytes.subarray(0, 4),
    Buffer.from('ffffffff1f', 'hex'),
    bytes.subarray(5),
  ])
  await expect(codec.decode(malformed)).rejects.toBeInstanceOf(KafkaJSInvalidVarIntError)
})

test('refuses null for a required group ID', async () => {
  const vector = require('../fixtures/java/shareGroupDescribeResponse-v1-mixed.json')
  const bytes = Buffer.from(vector.hex, 'hex')
  bytes[8] = 0
  await expect(codec.decode(bytes)).rejects.toBeInstanceOf(KafkaJSPartialMessageError)
})
