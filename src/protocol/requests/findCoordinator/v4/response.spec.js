const { decode, parse } = require('./response')

describe('Protocol > Requests > FindCoordinator > v4', () => {
  test('flattens the single-entry Coordinators array into the shape every caller reads', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v4_response.json')))

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 0,
      errorCode: 0,
      errorMessage: null,
      coordinator: { nodeId: 1, host: '127.0.0.1', port: 9092 },
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })

  test('a coordinator-level error is thrown by parse', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v4_response_error.json')))
    expect(data.errorCode).toEqual(15)
    await expect(parse(data)).rejects.toMatchObject({ code: 15 })
  })

  test('a response with anything other than exactly one coordinator is rejected', async () => {
    const Encoder = require('../../../encoder')
    const empty = new Encoder().writeInt32(0).writeUVarIntArray([]).writeTaggedFields().buffer
    await expect(decode(empty)).rejects.toThrow(/exactly one coordinator/)
  })

  test('TRANSACTION_ABORTABLE surfaces unchanged', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v4_response.json')))
    data.errorCode = 120
    await expect(parse(data)).rejects.toMatchObject({
      type: 'TRANSACTION_ABORTABLE',
      code: 120,
      retriable: false,
    })
  })
})
