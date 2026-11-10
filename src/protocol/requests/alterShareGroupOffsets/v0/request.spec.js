const fs = require('fs')
const path = require('path')
const codec = require('./request')
const { KafkaJSNonRetriableError } = require('../../../../errors')

const files = ['alterShareGroupOffsetsRequest-v0-offsets.json']

describe('independent Java request bytes', () => {
  test.each(files)('encodes %s from its literal fields', async (name) => {
    const vector = JSON.parse(
      fs.readFileSync(path.join(__dirname, '../fixtures/java', name), 'utf8')
    )
    const descriptor = codec(vector.fields)
    expect(descriptor).toMatchObject({
      apiKey: 91,
      apiName: 'AlterShareGroupOffsets',
      apiVersion: 0,
    })
    expect((await descriptor.encode()).buffer.toString('hex')).toBe(vector.hex)
  })
  test('exposes descriptor metadata without encoding missing arguments', () => {
    expect(codec()).toMatchObject({ apiKey: 91, apiVersion: 0 })
  })
})

describe('exact offset domains', () => {
  test.each(['-1', '9223372036854775808', '9007199254740993x', '01', 9007199254740992, null])(
    'refuses invalid offset %s with a typed error',
    async (value) => {
      const fields = {
        groupId: 'share-group',
        topics: [
          {
            topicName: 'alpha',
            partitions: [
              { partitionIndex: 0, startOffset: '9007199254740993' },
              { partitionIndex: 2, startOffset: '9223372036854775807' },
            ],
          },
        ],
      }
      fields.topics[0].partitions[0].startOffset = value
      await expect(codec(fields).encode()).rejects.toBeInstanceOf(KafkaJSNonRetriableError)
    }
  )
})
