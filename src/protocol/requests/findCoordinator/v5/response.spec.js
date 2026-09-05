const responseV4 = require('../v4/response')
const responseV5 = require('./response')

describe('Protocol > Requests > FindCoordinator > v5', () => {
  test('reuses v4 decode/parse byte-for-byte', async () => {
    expect(responseV5.decode).toBe(responseV4.decode)
    expect(responseV5.parse).toBe(responseV4.parse)

    const data = await responseV5.decode(Buffer.from(require('../fixtures/v4_response.json')))
    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 0,
      errorCode: 0,
      errorMessage: null,
      coordinator: { nodeId: 1, host: '127.0.0.1', port: 9092 },
    })

    await expect(responseV5.parse(data)).resolves.toBeTruthy()
  })

  test('TRANSACTION_ABORTABLE surfaces unchanged', async () => {
    const data = await responseV5.decode(Buffer.from(require('../fixtures/v4_response.json')))
    data.errorCode = 120
    await expect(responseV5.parse(data)).rejects.toMatchObject({
      type: 'TRANSACTION_ABORTABLE',
      code: 120,
      retriable: false,
    })
  })
})
