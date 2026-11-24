const Encoder = require('../../../encoder')
const { KafkaJSPartialMessageError } = require('../../../../errors')
const { decode, parse } = require('./response')
const { decode: decodeV3 } = require('../v3/response')

const vector = (caseId) => require(`../fixtures/java/createPartitionsResponse-v2-${caseId}.json`)

describe('Protocol > Requests > CreatePartitions > v2 response', () => {
  test.each(['ok', 'error'])('decodes the Apache 4.3.1 %s vector', async (caseId) => {
    const { hex, fields } = vector(caseId)

    expect(await decode(Buffer.from(hex, 'hex'))).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: fields.throttleTime,
      topicErrors: fields.topicErrors,
    })
  })

  test('parse resolves a clean response and throws the error of the first failed topic', async () => {
    const clean = await decode(Buffer.from(vector('ok').hex, 'hex'))
    const failed = await decode(Buffer.from(vector('error').hex, 'hex'))

    await expect(parse(clean)).resolves.toBe(clean)
    await expect(parse(failed)).rejects.toMatchObject({ type: 'INVALID_PARTITIONS', code: 37 })
  })

  test('sorts the topics by name as v0 does', async () => {
    const body = new Encoder()
      .writeInt32(0)
      .writeUVarIntArray(
        ['b', 'a'].map((name) =>
          new Encoder()
            .writeUVarIntString(name)
            .writeInt16(0)
            .writeUVarIntString(null)
            .writeTaggedFields()
        )
      )
      .writeTaggedFields().buffer

    expect((await decode(body)).topicErrors.map(({ topic }) => topic)).toEqual(['a', 'b'])
  })

  test('keeps an empty name and message as empty strings, never as null', async () => {
    const body = new Encoder()
      .writeInt32(25)
      .writeUVarIntArray([
        new Encoder()
          .writeUVarIntString('')
          .writeInt16(0)
          .writeUVarIntString('')
          .writeTaggedFields(),
      ])
      .writeTaggedFields().buffer

    expect(await decode(body)).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 25,
      topicErrors: [{ topic: '', errorCode: 0, errorMessage: '' }],
    })
  })

  test('refuses a null result list', async () => {
    const body = Buffer.from('000000000000', 'hex')

    for (const codec of [decode, decodeV3]) {
      const error = await codec(body).then(
        () => null,
        (rejection) => rejection
      )

      expect(error).toBeInstanceOf(KafkaJSPartialMessageError)
      expect(error.message).toContain('CreatePartitionsResponse.Results')
    }
  })

  test('keeps an empty result list (control)', async () => {
    const body = Buffer.from('000000000100', 'hex')

    for (const codec of [decode, decodeV3]) {
      expect(await codec(body)).toEqual({
        throttleTime: 0,
        clientSideThrottleTime: 0,
        topicErrors: [],
      })
    }
  })
})
