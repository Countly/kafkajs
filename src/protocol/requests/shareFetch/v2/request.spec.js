const fs = require('fs')
const path = require('path')
const codec = require('./request')
const { KafkaJSNonRetriableError } = require('../../../../errors')

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

test('accepts exact bigint and safe numeric offsets', async () => {
  const source = require('../fixtures/java/shareFetchRequest-v2-incremental.json')
  const fields = structuredClone(source.fields)
  fields.topics[0].partitions[0].acknowledgementBatches[0].firstOffset = 9007199254740993n
  const expected = await codec(source.fields).encode()
  expect((await codec(fields).encode()).buffer).toEqual(expected.buffer)
  const batch = fields.topics[0].partitions[0].acknowledgementBatches[0]
  batch.firstOffset = 1
  batch.lastOffset = '1'
  const numeric = await codec(fields).encode()
  batch.firstOffset = '1'
  expect((await codec(fields).encode()).buffer).toEqual(numeric.buffer)
})
