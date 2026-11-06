const fs = require('fs')
const path = require('path')
const codec = require('./request')
const { KafkaJSNonRetriableError } = require('../../../../errors')
const { expectRefusal } = require('../../../__tests__/fixtures/shareSpecKit')

const files = [
  'shareAcknowledgeRequest-v2-renew.json',
  'shareAcknowledgeRequest-v2-final.json',
  'shareAcknowledgeRequest-v2-mixed10.json',
  'shareAcknowledgeRequest-v2-mixed11.json',
  'shareAcknowledgeRequest-v2-uniform1.json',
  'shareAcknowledgeRequest-v2-uniform2.json',
  'shareAcknowledgeRequest-v2-uniform10.json',
  'shareAcknowledgeRequest-v2-uniform11.json',
  'shareAcknowledgeRequest-v2-uniform64.json',
]

describe('independent Java request bytes', () => {
  test.each(files)('encodes %s from its literal fields', async (name) => {
    const vector = JSON.parse(
      fs.readFileSync(path.join(__dirname, '../fixtures/java', name), 'utf8')
    )
    const descriptor = codec(vector.fields)
    expect(descriptor).toMatchObject({ apiKey: 79, apiName: 'ShareAcknowledge', apiVersion: 2 })
    expect((await descriptor.encode()).buffer.toString('hex')).toBe(vector.hex)
  })
  test('exposes descriptor metadata without encoding missing arguments', () => {
    expect(codec()).toMatchObject({ apiKey: 79, apiVersion: 2 })
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
        isRenewAck: true,
        topics: [
          {
            topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
            partitions: [
              {
                partitionIndex: 0,
                acknowledgementBatches: [
                  {
                    firstOffset: '9007199254740993',
                    lastOffset: '9007199254740994',
                    acknowledgeTypes: [4],
                  },
                ],
              },
            ],
          },
        ],
      }
      fields.topics[0].partitions[0].acknowledgementBatches[0].firstOffset = value
      await expect(codec(fields).encode()).rejects.toBeInstanceOf(KafkaJSNonRetriableError)
    }
  )
})

describe('topic IDs', () => {
  test.each([
    ['missing', undefined],
    ['null', null],
    ['malformed', 'not-a-uuid'],
    ['unhyphenated', '0102030405060708090a0b0c0d0e0f10'],
    ['numeric', 1],
  ])('refuses a %s topic ID', async (_, topicId) => {
    await expectRefusal(
      codec({ topics: [{ topicId, partitions: [] }] }).encode(),
      KafkaJSNonRetriableError,
      'Expected canonical UUID string'
    )
  })
})
