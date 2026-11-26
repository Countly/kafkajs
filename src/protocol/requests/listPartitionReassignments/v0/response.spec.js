const Encoder = require('../../../encoder')
const { KafkaJSPartialMessageError } = require('../../../../errors')
const { decode, parse } = require('./response')

/**
 * Encodes a one-topic, one-partition reply. A list or name given as `null` is written as the
 * null value; a list given as `undefined` falls back to a valid one.
 * @param {object} [fields] `name`, `topics`, `partitions`, `replicas`, `adding` and `removing`
 * @returns {Buffer} the response body
 */
const reply = (fields = {}) => {
  const {
    name = 'topic-a',
    topics,
    partitions,
    replicas = [2],
    adding = [],
    removing = [],
  } = fields
  const partition = new Encoder()
    .writeInt32(0)
    .writeUVarIntArray(replicas, 'int32')
    .writeUVarIntArray(adding, 'int32')
    .writeUVarIntArray(removing, 'int32')
    .writeTaggedFields()
  const topic = new Encoder()
    .writeUVarIntString(name)
    .writeUVarIntArray(partitions === undefined ? [partition] : partitions)
    .writeTaggedFields()

  return new Encoder()
    .writeInt32(0)
    .writeInt16(0)
    .writeUVarIntString(null)
    .writeUVarIntArray(topics === undefined ? [topic] : topics)
    .writeTaggedFields().buffer
}

describe('Protocol > Requests > ListPartitionReassignments > v0 required fields', () => {
  test.each([
    ['Topics', { topics: null }],
    ['Topics.Partitions', { partitions: null }],
    ['Topics.Partitions.Replicas', { replicas: null }],
    ['Topics.Partitions.AddingReplicas', { adding: null }],
    ['Topics.Partitions.RemovingReplicas', { removing: null }],
  ])('refuses a null %s list', async (field, fields) => {
    const error = await decode(reply(fields)).then(
      () => null,
      (rejection) => rejection
    )

    expect(error).toBeInstanceOf(KafkaJSPartialMessageError)
    expect(error.message).toContain(`ListPartitionReassignmentsResponse.${field} is a null array`)
  })

  test('refuses a null topic name', async () => {
    const error = await decode(reply({ name: null })).then(
      () => null,
      (rejection) => rejection
    )

    expect(error).toBeInstanceOf(KafkaJSPartialMessageError)
    expect(error.message).toContain('ListPartitionReassignmentsResponse.Topics.Name is null')
  })

  test('keeps empty lists as empty arrays (control)', async () => {
    const empty = { partition: 0, replicas: [], addingReplicas: [], removingReplicas: [] }

    expect(await decode(reply({ replicas: [], adding: [], removing: [] }))).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 0,
      errorCode: 0,
      topics: [{ name: 'topic-a', partitions: [empty] }],
    })
    expect((await decode(reply({ partitions: [] }))).topics).toEqual([
      { name: 'topic-a', partitions: [] },
    ])
    expect((await decode(reply({ topics: [] }))).topics).toEqual([])
  })

  test('keeps an empty topic name as an empty string, never as null', async () => {
    expect((await decode(reply({ name: '' }))).topics[0].name).toBe('')
  })
})

describe('Protocol > Requests > AlterPartitionReassignments > v0', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v0_response.json')))
    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 0,
      errorCode: 0,
      topics: [
        {
          name: 'test-topic-1f131dd7f83b8d72a447-33298-d13ec602-1a34-41c8-b59e-0657aef3ad25',
          partitions: [
            { partition: 0, replicas: [2, 1, 0], addingReplicas: [1], removingReplicas: [0] },
          ],
        },
      ],
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })

  test('decodes a captured Kafka 4.3.1 response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v0_response_kafka431.json')))

    expect(data.errorCode).toEqual(0)
    expect(Array.isArray(data.topics)).toBe(true)
  })
})
