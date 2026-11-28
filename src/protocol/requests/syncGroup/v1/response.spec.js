const Encoder = require('../../../encoder')
const { decode, parse } = require('./response')

describe('Protocol > Requests > SyncGroup > v1', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v1_response.json')))
    expect(data).toEqual({
      throttleTime: 0,
      errorCode: 0,
      memberAssignment: Buffer.from(require('../fixtures/v1_memberAssignment.json')),
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })

  test('rejects UNSUPPORTED_VERSION before reading the absent assignment bytes', async () => {
    const buffer = new Encoder().writeInt32(0).writeInt16(35).buffer

    await expect(decode(buffer)).rejects.toMatchObject({ type: 'UNSUPPORTED_VERSION', code: 35 })
  })
})
