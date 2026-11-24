const { KafkaJSPartialMessageError } = require('../../../../errors')
const { decode, parse } = require('./response')
const { decode: decodeV5 } = require('../v5/response')
const { decode: decodeV6 } = require('../v6/response')

const vector = (caseId) => require(`../fixtures/java/deleteTopicsResponse-v4-${caseId}.json`)
const byName = (a, b) => a.topic.localeCompare(b.topic)

describe('Protocol > Requests > DeleteTopics > v4 response', () => {
  test.each(['ok', 'error', 'deletionDisabled'])(
    'decodes the Apache 4.3.1 %s vector, topics sorted by name',
    async (caseId) => {
      const { hex, fields } = vector(caseId)

      expect(await decode(Buffer.from(hex, 'hex'))).toEqual({
        throttleTime: 0,
        clientSideThrottleTime: fields.throttleTime,
        topicErrors: [...fields.topicErrors].sort(byName),
      })
    }
  )

  test('parse resolves a clean response and throws the error of the first failed topic', async () => {
    const clean = await decode(Buffer.from(vector('ok').hex, 'hex'))
    const disabled = await decode(Buffer.from(vector('deletionDisabled').hex, 'hex'))

    await expect(parse(clean)).resolves.toBe(clean)
    await expect(parse(disabled)).rejects.toMatchObject({ type: 'TOPIC_DELETION_DISABLED' })
  })

  test('refuses a null result list', async () => {
    const body = Buffer.from('000000000000', 'hex')

    for (const codec of [decode, decodeV5, decodeV6]) {
      const error = await codec(body).then(
        () => null,
        (rejection) => rejection
      )

      expect(error).toBeInstanceOf(KafkaJSPartialMessageError)
      expect(error.message).toContain('DeleteTopicsResponse.Responses')
    }
  })

  test('keeps an empty result list (control)', async () => {
    const body = Buffer.from('000000000100', 'hex')

    for (const codec of [decode, decodeV5, decodeV6]) {
      expect(await codec(body)).toEqual({
        throttleTime: 0,
        clientSideThrottleTime: 0,
        topicErrors: [],
      })
    }
  })
})
