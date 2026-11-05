const { KafkaJSNonRetriableError } = require('../../../../errors')
const fs = require('fs')
const path = require('path')
const codec = require('./request')

const files = [
  'shareGroupDescribeRequest-v1-basic.json',
  'shareGroupDescribeRequest-v1-authorized.json',
]

describe('independent Java request bytes', () => {
  test.each(files)('encodes %s from its literal fields', async (name) => {
    const vector = JSON.parse(
      fs.readFileSync(path.join(__dirname, '../fixtures/java', name), 'utf8')
    )
    const descriptor = codec(vector.fields)
    expect(descriptor).toMatchObject({ apiKey: 77, apiName: 'ShareGroupDescribe', apiVersion: 1 })
    expect((await descriptor.encode()).buffer.toString('hex')).toBe(vector.hex)
  })
  test('exposes descriptor metadata without encoding missing arguments', () => {
    expect(codec()).toMatchObject({ apiKey: 77, apiVersion: 1 })
  })
})

test.each([0, null, 'true'])(
  'refuses invalid authorization flag %j',
  async (includeAuthorizedOperations) => {
    await expect(
      codec({ groupIds: ['g'], includeAuthorizedOperations }).encode()
    ).rejects.toBeInstanceOf(KafkaJSNonRetriableError)
  }
)
