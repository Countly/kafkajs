const fs = require('fs')
const path = require('path')
const codec = require('./request')
const { KafkaJSNonRetriableError } = require('../../../../errors')

const files = [
  'shareGroupHeartbeatRequest-v1-join.json',
  'shareGroupHeartbeatRequest-v1-steady.json',
  'shareGroupHeartbeatRequest-v1-emptyTopics.json',
  'shareGroupHeartbeatRequest-v1-leave.json',
]

describe('independent Java request bytes', () => {
  test.each(files)('encodes %s from its literal fields', async (name) => {
    const vector = JSON.parse(
      fs.readFileSync(path.join(__dirname, '../fixtures/java', name), 'utf8')
    )
    const descriptor = codec(vector.fields)
    expect(descriptor).toMatchObject({ apiKey: 76, apiName: 'ShareGroupHeartbeat', apiVersion: 1 })
    expect((await descriptor.encode()).buffer.toString('hex')).toBe(vector.hex)
  })
  test('exposes descriptor metadata without encoding missing arguments', () => {
    expect(codec()).toMatchObject({ apiKey: 76, apiVersion: 1 })
  })
})

test.each([-2147483649, 2147483648, 1.5, '7', null])(
  'refuses invalid member epoch %s',
  async (memberEpoch) => {
    await expect(
      codec({ groupId: 'g', memberId: 'm', memberEpoch }).encode()
    ).rejects.toBeInstanceOf(KafkaJSNonRetriableError)
  }
)

test.each([
  { groupId: null },
  { rackId: 3 },
  { subscribedTopicNames: {} },
  { subscribedTopicNames: [2] },
])('refuses malformed string and topic fields %j', async (fields) => {
  await expect(
    codec({ groupId: 'g', memberId: 'm', memberEpoch: 0, ...fields }).encode()
  ).rejects.toBeInstanceOf(KafkaJSNonRetriableError)
})
