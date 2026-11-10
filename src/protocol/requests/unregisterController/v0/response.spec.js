const response = require('./response')
const wire = require('../../../../../testHelpers/wire/unregisterControllerV0')
const golden = require('../fixtures/v0_response.json')
const { KafkaJSPartialMessageError, KafkaJSProtocolError } = require('../../../../errors')

const decoded = (hex) => response.decode(Buffer.from(hex, 'hex'))
const tag = (n) => ({ tag: n, value: Buffer.from([0x78]) })

describe('Protocol > Requests > UnregisterController > v0 response', () => {
  test('decodes success with null message byte for byte', async () => {
    const { hex } = golden
    expect(wire.buildResponse({ throttleTimeMs: 25 }).toString('hex')).toBe(hex)
    expect(await decoded(hex)).toEqual({
      throttleTimeMs: 25,
      throttleTime: 0,
      clientSideThrottleTime: 25,
      errorCode: 0,
      errorMessage: null,
    })
  })

  test('decodes code 136 with null and custom messages', async () => {
    const cases = [
      ['0000001900880000', null],
      ['0000001900880461626300', 'abc'],
    ]
    for (const [hex, errorMessage] of cases) {
      const fields = { throttleTimeMs: 25, errorCode: 136, errorMessage }
      expect(wire.buildResponse(fields).toString('hex')).toBe(hex)
      expect(await decoded(hex)).toMatchObject({ errorCode: 136, errorMessage })
    }
  })

  test('preserves null empty and custom messages', async () => {
    const cases = [
      ['0000000000880000', null],
      ['0000000000880100', ''],
      ['0000000000880461626300', 'abc'],
      ['00000000008806c3a9e282ac00', 'é€'],
    ]
    for (const [hex, errorMessage] of cases) {
      expect(wire.buildResponse({ errorCode: 136, errorMessage }).toString('hex')).toBe(hex)
      expect((await decoded(hex)).errorMessage).toBe(errorMessage)
    }
  })

  test('rejects truncation trailing bytes and malformed tags', async () => {
    const valid = wire.buildResponse({ errorCode: 136, errorMessage: 'abc' })
    for (let length = 0; length < valid.length; length++)
      await expect(response.decode(valid.subarray(0, length))).rejects.toBeInstanceOf(
        KafkaJSPartialMessageError
      )
    await expect(response.decode(Buffer.concat([valid, Buffer.from([0])]))).rejects.toBeInstanceOf(
      KafkaJSPartialMessageError
    )
    const malformed = [
      wire.buildResponse({ tags: [tag(2), tag(1)] }),
      wire.buildResponse({ tags: [tag(1), tag(1)] }),
      wire.buildResponse({ tags: [tag(1)] }).subarray(0, -1),
    ]
    for (const bytes of malformed)
      await expect(response.decode(bytes)).rejects.toBeInstanceOf(KafkaJSPartialMessageError)
  })

  test('skips an unknown tagged field without shifting known fields', async () => {
    const bytes = wire.buildResponse({ throttleTimeMs: 5, errorCode: 136, tags: [tag(1)] })
    expect(bytes.subarray(-4).toString('hex')).toBe('01010178')
    expect(await response.decode(bytes)).toMatchObject({
      throttleTimeMs: 5,
      errorCode: 136,
      errorMessage: null,
    })
  })

  test('applies response throttle to the client', async () => {
    const body = await decoded(golden.hex)
    expect(body.throttleTime).toBe(0)
    expect(body.clientSideThrottleTime).toBe(25)
    expect(await response.parse(body)).toBe(body)
  })

  test('parse throws the typed protocol error carrying the broker message', async () => {
    const error = await response.parse(await decoded('0000000000880461626300')).catch((e) => e)
    expect(error).toBeInstanceOf(KafkaJSProtocolError)
    expect(error).toMatchObject({
      type: 'CONTROLLER_ID_NOT_REGISTERED',
      code: 136,
      retriable: false,
      brokerErrorMessage: 'abc',
    })
    expect(error.message).toContain('abc')
    const bare = await response.parse(await decoded('0000000000880000')).catch((e) => e)
    expect(bare.brokerErrorMessage).toBeNull()
    expect(bare.message).toBe('The given controller ID was not registered.')
    const empty = await response.parse(await decoded('0000000000880100')).catch((e) => e)
    expect(empty.brokerErrorMessage).toBe('')
  })
})
