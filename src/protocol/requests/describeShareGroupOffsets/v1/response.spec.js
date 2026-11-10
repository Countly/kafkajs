const { plain } = require('../../../__tests__/fixtures/fuzzResponseVectors')
const { expectRefusal } = require('../../../__tests__/fixtures/shareSpecKit')
const fs = require('fs')
const path = require('path')
const codec = require('./response')
const { KafkaJSPartialMessageError, KafkaJSInvalidVarIntError } = require('../../../../errors')

const files = [
  'describeShareGroupOffsetsResponse-v1-knownAndUnknownLag.json',
  'describeShareGroupOffsetsResponse-v1-unknownTags.json',
]

describe('independent Java response bytes', () => {
  test.each(files)('decodes %s to its literal fields', async (name) => {
    const vector = JSON.parse(
      fs.readFileSync(path.join(__dirname, '../fixtures/java', name), 'utf8')
    )
    const decoded = await codec.decode(Buffer.from(vector.hex, 'hex'))
    for (const group of decoded.groups) {
      for (const topic of group.topics) {
        for (const partition of topic.partitions) {
          expect(typeof partition.startOffset).toBe('string')
          expect(typeof partition.lag).toBe('string')
          expect(typeof partition.leaderEpoch).toBe('number')
        }
      }
    }
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
    require('../fixtures/java/describeShareGroupOffsetsResponse-v1-knownAndUnknownLag.json').hex,
    'hex'
  )
  bytes[4] = 0
  await expectRefusal(codec.decode(bytes), KafkaJSPartialMessageError, 'Null required array')
})

test('rejects a compact length beyond unsigned int32', async () => {
  const bytes = Buffer.from(
    require('../fixtures/java/describeShareGroupOffsetsResponse-v1-knownAndUnknownLag.json').hex,
    'hex'
  )
  const malformed = Buffer.concat([
    bytes.subarray(0, 4),
    Buffer.from('ffffffff1f', 'hex'),
    bytes.subarray(5),
  ])
  await expectRefusal(
    codec.decode(malformed),
    KafkaJSInvalidVarIntError,
    'Unsigned VarInt exceeds int32'
  )
})

describe('the unknown -1 and the offset domain', () => {
  const vector = require('../fixtures/java/describeShareGroupOffsetsResponse-v1-knownAndUnknownLag.json')
  // Partition 0 of the Java body: its StartOffset int64 starts at byte 45 and its Lag int64 at 57.
  const FIELDS = [
    ['startOffset', 45, 9007199254740993n],
    ['lag', 57, 9223372036854775807n],
  ]

  /** The Java body with one partition-0 int64 overwritten, after proving what it held. */
  const patched = (at, was, value) => {
    const bytes = Buffer.from(vector.hex, 'hex')
    expect(bytes.readBigInt64BE(at)).toBe(was)
    bytes.writeBigInt64BE(value, at)
    return bytes
  }

  test.each(FIELDS)('keeps -1 in %s as the exact decimal string', async (field, at, was) => {
    const expected = structuredClone(vector.fields)
    expected.groups[0].topics[0].partitions[0][field] = '-1'
    const decoded = await codec.decode(patched(at, was, -1n))
    expect(plain(decoded)).toEqual(expected)
  })

  test.each(FIELDS)('refuses -2 in %s as an invalid offset', async (_, at, was) => {
    await expectRefusal(
      codec.decode(patched(at, was, -2n)),
      KafkaJSPartialMessageError,
      'Invalid offset or lag'
    )
  })
})
