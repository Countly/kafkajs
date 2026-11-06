const { plain } = require('../../../__tests__/fixtures/fuzzResponseVectors')
const { expectRefusal } = require('../../../__tests__/fixtures/shareSpecKit')
const fs = require('fs')
const path = require('path')
const codec = require('./response')
const { KafkaJSPartialMessageError, KafkaJSInvalidVarIntError } = require('../../../../errors')

const files = [
  'alterShareGroupOffsetsResponse-v0-mixed.json',
  'alterShareGroupOffsetsResponse-v0-error.json',
  'alterShareGroupOffsetsResponse-v0-unknownTags.json',
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
  test.each(files)('rejects a trailing byte after %s', async (name) => {
    const vector = JSON.parse(
      fs.readFileSync(path.join(__dirname, '../fixtures/java', name), 'utf8')
    )
    await expectRefusal(
      codec.decode(Buffer.concat([Buffer.from(vector.hex, 'hex'), Buffer.from([0])])),
      KafkaJSPartialMessageError,
      'Trailing share response bytes'
    )
  })
})

test('rejects null for a required array', async () => {
  const bytes = Buffer.from(
    require('../fixtures/java/alterShareGroupOffsetsResponse-v0-mixed.json').hex,
    'hex'
  )
  bytes[7] = 0
  await expectRefusal(codec.decode(bytes), KafkaJSPartialMessageError, 'Null required array')
})

test('rejects a compact length beyond unsigned int32', async () => {
  const bytes = Buffer.from(
    require('../fixtures/java/alterShareGroupOffsetsResponse-v0-mixed.json').hex,
    'hex'
  )
  const malformed = Buffer.concat([
    bytes.subarray(0, 7),
    Buffer.from('ffffffff1f', 'hex'),
    bytes.subarray(8),
  ])
  await expectRefusal(
    codec.decode(malformed),
    KafkaJSInvalidVarIntError,
    'Unsigned VarInt exceeds int32'
  )
})
