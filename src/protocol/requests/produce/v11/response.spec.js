const { decode, parse } = require('./response')

describe('Protocol > Requests > Produce > v11', () => {
  test('decodes identically to v10 — v11 adds no response fields', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v11_response.json')))
    expect(data.topics[0].partitions[0]).toEqual({
      partition: 0,
      errorCode: 0,
      baseOffset: '100',
      logAppendTime: '-1',
      logStartOffset: '0',
      recordErrors: [],
      errorMessage: null,
      currentLeader: null,
    })
    expect(data.nodeEndpoints).toEqual([])
    await expect(parse(data)).resolves.toBeTruthy()
  })

  test('TRANSACTION_ABORTABLE surfaces as the non-retriable protocol error it already is', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v11_response.json')))
    data.topics[0].partitions[0].errorCode = 120
    await expect(parse(data)).rejects.toMatchObject({
      type: 'TRANSACTION_ABORTABLE',
      code: 120,
      retriable: false,
    })
  })
})
