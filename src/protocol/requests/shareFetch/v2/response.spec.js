const { plain } = require('../../../__tests__/fixtures/fuzzResponseVectors')
const fs = require('fs')
const path = require('path')
const codec = require('./response')
const {
  KafkaJSPartialMessageError,
  KafkaJSInvalidVarIntError,
  KafkaJSCorruptRecordError,
} = require('../../../../errors')

const files = [
  'shareFetchResponse-v2-recordsAndErrors.json',
  'shareFetchResponse-v2-empty.json',
  'shareFetchResponse-v2-error.json',
  'shareFetchResponse-v2-unknownTags.json',
]

/** @param {*} value @returns {*} Stable literal record fields while retaining every enclosing response field. */
const literal = (value) => {
  if (Array.isArray(value)) return value.map(literal)
  if (value && typeof value === 'object') {
    if (Object.hasOwn(value, 'batchContext'))
      return {
        offset: value.offset,
        timestamp: value.timestamp,
        key: value.key?.toString('hex') ?? null,
        value: value.value?.toString('hex') ?? null,
        headers: value.headers,
      }
    return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, literal(item)]))
  }
  return value
}

describe('independent Java response bytes', () => {
  test.each(files)('decodes %s to its literal fields', async (name) => {
    const vector = JSON.parse(
      fs.readFileSync(path.join(__dirname, '../fixtures/java', name), 'utf8')
    )
    const decoded = await codec.decode(Buffer.from(vector.hex, 'hex'))
    for (const topic of decoded.responses) {
      for (const partition of topic.partitions) {
        for (const range of partition.acquiredRecords) {
          expect(typeof range.firstOffset).toBe('string')
          expect(typeof range.lastOffset).toBe('string')
        }
        for (const message of partition.messages) expect(typeof message.offset).toBe('string')
      }
    }
    expect(decoded).toMatchObject({
      throttleTime: 0,
      clientSideThrottleTime: vector.fields.throttleTime,
    })
    expect(literal(plain(decoded))).toEqual(vector.fields)
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
    require('../fixtures/java/shareFetchResponse-v2-recordsAndErrors.json').hex,
    'hex'
  )
  bytes[11] = 0
  await expect(codec.decode(bytes)).rejects.toBeInstanceOf(KafkaJSPartialMessageError)
})

test('rejects a compact length beyond unsigned int32', async () => {
  const bytes = Buffer.from(
    require('../fixtures/java/shareFetchResponse-v2-recordsAndErrors.json').hex,
    'hex'
  )
  const malformed = Buffer.concat([
    bytes.subarray(0, 11),
    Buffer.from('ffffffff1f', 'hex'),
    bytes.subarray(12),
  ])
  await expect(codec.decode(malformed)).rejects.toBeInstanceOf(KafkaJSInvalidVarIntError)
})

test('refuses a negative acquired offset', async () => {
  const bytes = Buffer.from(
    require('../fixtures/java/shareFetchResponse-v2-recordsAndErrors.json').hex,
    'hex'
  )
  Buffer.from('ffffffffffffffff', 'hex').copy(bytes, 120)
  await expect(codec.decode(bytes)).rejects.toBeInstanceOf(KafkaJSPartialMessageError)
})

test('refuses a reversed acquired range', async () => {
  const bytes = Buffer.from(
    require('../fixtures/java/shareFetchResponse-v2-recordsAndErrors.json').hex,
    'hex'
  )
  Buffer.from('0020000000000000', 'hex').copy(bytes, 128)
  await expect(codec.decode(bytes)).rejects.toBeInstanceOf(KafkaJSPartialMessageError)
})

test('refuses a compact Records length larger than its envelope', async () => {
  const vector = require('../fixtures/java/shareFetchResponse-v2-recordsAndErrors.json')
  // Bytes 48 and 191 hold the two partitions' Records lengths.
  for (const [offset, length] of [
    [191, '7f'],
    [48, 'ffffffff0f'],
  ]) {
    const bytes = Buffer.from(vector.hex, 'hex')
    Buffer.from(length, 'hex').copy(bytes, offset)
    const error = await codec.decode(bytes).catch((caught) => caught)
    expect(error).toBeInstanceOf(KafkaJSPartialMessageError)
    expect(error.message).toContain('compact Records')
  }
})

test('preserves the inherited typed valid-prefix marker', async () => {
  const vector = require('../fixtures/java/shareFetchResponse-v2-recordsAndErrors.json')
  const original = Buffer.from(vector.hex, 'hex')
  const first = original.subarray(49, 119)
  const second = Buffer.from(first)
  second.writeBigInt64BE(9007199254740994n, 0)
  second.writeInt32BE(2, 57)
  const bytes = Buffer.concat([
    original.subarray(0, 48),
    Buffer.from('8d01', 'hex'),
    first,
    second,
    original.subarray(119),
  ])
  const result = await codec.decode(bytes, { checkCrcs: false })
  const partition = result.responses[0].partitions[0]
  expect(partition.messages).toHaveLength(1)
  expect(partition.messages[0].offset).toBe('9007199254740993')
  expect(partition.corruption).toMatchObject({ reason: 'count', offset: '9007199254740994' })
  expect(partition.corruption.error).toBeInstanceOf(
    require('../../../../errors').KafkaJSCorruptRecordError
  )
  expect(partition.corruption).not.toHaveProperty('nextOffset')
  expect(partition.acquiredRecords).toEqual(
    vector.fields.responses[0].partitions[0].acquiredRecords
  )
  expect(result.nodeEndpoints).toEqual(vector.fields.nodeEndpoints)
})

test('halts a partition at a checksum mismatch and keeps the fields that follow it', async () => {
  const vector = require('../fixtures/java/shareFetchResponse-v2-recordsAndErrors.json')
  const bytes = Buffer.from(vector.hex, 'hex')
  // The record batch spans bytes 49 to 118; its CRC field starts at byte 66.
  bytes[66] ^= 0xff
  const result = await codec.decode(bytes, { checkCrcs: true })
  const [first, second] = result.responses[0].partitions
  const [expectedFirst, expectedSecond] = vector.fields.responses[0].partitions
  expect(first.messages).toEqual([])
  expect(first.corruption).toMatchObject({ reason: 'checksum', offset: '9007199254740993' })
  expect(first.corruption.error).toBeInstanceOf(KafkaJSCorruptRecordError)
  expect(first.corruption).not.toHaveProperty('nextOffset')
  expect(first.corruption.error).not.toHaveProperty('nextOffset')
  expect(first.acquiredRecords).toEqual(expectedFirst.acquiredRecords)
  expect(literal(plain(second))).toEqual(expectedSecond)
  expect(result.nodeEndpoints).toEqual(vector.fields.nodeEndpoints)
})

test('refuses null for required Records', async () => {
  const vector = require('../fixtures/java/shareFetchResponse-v2-recordsAndErrors.json')
  const bytes = Buffer.from(vector.hex, 'hex')
  bytes[48] = 0
  await expect(codec.decode(bytes)).rejects.toBeInstanceOf(KafkaJSPartialMessageError)
})
