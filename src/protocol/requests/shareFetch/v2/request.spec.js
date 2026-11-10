const fs = require('fs')
const path = require('path')
const codec = require('./request')
const { KafkaJSNonRetriableError } = require('../../../../errors')
const { expectRefusal } = require('../../../__tests__/fixtures/shareSpecKit')

const files = [
  'shareFetchRequest-v2-open.json',
  'shareFetchRequest-v2-incremental.json',
  'shareFetchRequest-v2-renew.json',
  'shareFetchRequest-v2-close.json',
]

describe('independent Java request bytes', () => {
  test.each(files)('encodes %s from its literal fields', async (name) => {
    const vector = JSON.parse(
      fs.readFileSync(path.join(__dirname, '../fixtures/java', name), 'utf8')
    )
    const descriptor = codec(vector.fields)
    expect(descriptor).toMatchObject({ apiKey: 78, apiName: 'ShareFetch', apiVersion: 2 })
    expect((await descriptor.encode()).buffer.toString('hex')).toBe(vector.hex)
  })
  test('exposes descriptor metadata without encoding missing arguments', () => {
    expect(codec()).toMatchObject({ apiKey: 78, apiVersion: 2 })
  })
})

describe('exact offset domains', () => {
  test.each(['-1', '9223372036854775808', '9007199254740993x', '01', 9007199254740992, null])(
    'refuses invalid offset %s with a typed error',
    async (value) => {
      const fields = {
        groupId: 'share-group',
        memberId: 'share-member',
        shareSessionEpoch: 1,
        maxWaitMs: 500,
        minBytes: 0,
        maxBytes: 2147483647,
        maxRecords: 5,
        batchSize: 5,
        shareAcquireMode: 1,
        isRenewAck: false,
        topics: [
          {
            topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
            partitions: [
              {
                partitionIndex: 0,
                acknowledgementBatches: [
                  {
                    firstOffset: '9007199254740993',
                    lastOffset: '9007199254740995',
                    acknowledgeTypes: [1],
                  },
                  {
                    firstOffset: '9007199254740997',
                    lastOffset: '9007199254741000',
                    acknowledgeTypes: [1, 0, 2, 3],
                  },
                  {
                    firstOffset: '9223372036854775807',
                    lastOffset: '9223372036854775807',
                    acknowledgeTypes: [3],
                  },
                ],
              },
            ],
          },
        ],
        forgottenTopicsData: [],
      }
      fields.topics[0].partitions[0].acknowledgementBatches[0].firstOffset = value
      await expect(codec(fields).encode()).rejects.toBeInstanceOf(KafkaJSNonRetriableError)
    }
  )
})

test.each([-129, 128, 1.5, '1'])('refuses invalid acquire mode %j', async (shareAcquireMode) => {
  await expect(
    codec({ groupId: 'g', memberId: 'm', shareAcquireMode }).encode()
  ).rejects.toBeInstanceOf(KafkaJSNonRetriableError)
})

describe('bigint and numeric offsets', () => {
  const source = require('../fixtures/java/shareFetchRequest-v2-incremental.json')
  // The first acknowledgement batch's FirstOffset int64 inside the Java body.
  const FIRST_OFFSET_AT = 74

  const encodeWithFirstOffset = async (firstOffset) => {
    const fields = structuredClone(source.fields)
    fields.topics[0].partitions[0].acknowledgementBatches[0].firstOffset = firstOffset
    return (await codec(fields).encode()).buffer.toString('hex')
  }

  test('encodes an exact bigint offset to the Java bytes', async () => {
    expect(await encodeWithFirstOffset(9007199254740993n)).toBe(source.hex)
  })

  test.each([1, Number.MAX_SAFE_INTEGER])(
    'encodes the safe number %s as the Java bytes with that int64 patched in',
    async (offset) => {
      const bytes = Buffer.from(source.hex, 'hex')
      expect(bytes.readBigInt64BE(FIRST_OFFSET_AT)).toBe(9007199254740993n)
      bytes.writeBigInt64BE(BigInt(offset), FIRST_OFFSET_AT)
      expect(await encodeWithFirstOffset(offset)).toBe(bytes.toString('hex'))
    }
  )
})

describe('topic IDs', () => {
  const INVALID = [
    ['missing', undefined],
    ['null', null],
    ['malformed', 'not-a-uuid'],
    ['unhyphenated', '0102030405060708090a0b0c0d0e0f10'],
    ['numeric', 1],
  ]

  test.each(INVALID)('refuses a %s topic ID in topics', async (_, topicId) => {
    await expectRefusal(
      codec({ topics: [{ topicId, partitions: [] }] }).encode(),
      KafkaJSNonRetriableError,
      'Expected canonical UUID string'
    )
  })

  test.each(INVALID)('refuses a %s topic ID in forgottenTopicsData', async (_, topicId) => {
    await expectRefusal(
      codec({ forgottenTopicsData: [{ topicId, partitions: [] }] }).encode(),
      KafkaJSNonRetriableError,
      'Expected canonical UUID string'
    )
  })
})
