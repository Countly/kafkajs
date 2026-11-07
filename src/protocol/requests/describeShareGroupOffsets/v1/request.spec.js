const fs = require('fs')
const path = require('path')
const codec = require('./request')

const files = [
  'describeShareGroupOffsetsRequest-v1-nullTopics.json',
  'describeShareGroupOffsetsRequest-v1-emptyTopics.json',
  'describeShareGroupOffsetsRequest-v1-namedTopics.json',
]

describe('independent Java request bytes', () => {
  test.each(files)('encodes %s from its literal fields', async (name) => {
    const vector = JSON.parse(
      fs.readFileSync(path.join(__dirname, '../fixtures/java', name), 'utf8')
    )
    const descriptor = codec(vector.fields)
    expect(descriptor).toMatchObject({
      apiKey: 90,
      apiName: 'DescribeShareGroupOffsets',
      apiVersion: 1,
    })
    expect((await descriptor.encode()).buffer.toString('hex')).toBe(vector.hex)
  })
  test('exposes descriptor metadata without encoding missing arguments', () => {
    expect(codec()).toMatchObject({ apiKey: 90, apiVersion: 1 })
  })
})
