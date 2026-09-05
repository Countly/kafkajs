const responseV4 = require('../v4/response')
const responseV6 = require('./response')

describe('Protocol > Requests > FindCoordinator > v6', () => {
  test('reuses v4 decode/parse byte-for-byte', async () => {
    expect(responseV6.decode).toBe(responseV4.decode)
    expect(responseV6.parse).toBe(responseV4.parse)

    const data = await responseV6.decode(Buffer.from(require('../fixtures/v4_response.json')))
    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 0,
      errorCode: 0,
      errorMessage: null,
      coordinator: { nodeId: 1, host: '127.0.0.1', port: 9092 },
    })

    await expect(responseV6.parse(data)).resolves.toBeTruthy()
  })

  test('a coordinator-level error is thrown by parse', async () => {
    const data = await responseV6.decode(Buffer.from(require('../fixtures/v4_response_error.json')))
    expect(data.errorCode).toEqual(15)
    await expect(responseV6.parse(data)).rejects.toMatchObject({ code: 15 })
  })
})
