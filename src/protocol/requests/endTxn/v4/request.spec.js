const RequestV4Protocol = require('./request')
const { protocol } = require('../index')

const vector = (version, caseId) =>
  require(`../fixtures/java/endTxnRequest-v${version}-${caseId}.json`)

describe('Protocol > Requests > EndTxn > v4', () => {
  test.each(['commit', 'abort'])(
    'encodes the Apache 4.3.1 %s vector byte for byte',
    async (caseId) => {
      const { hex, fields } = vector(4, caseId)
      const { buffer } = await RequestV4Protocol(fields).encode()

      expect(buffer.toString('hex')).toEqual(hex)
    }
  )

  test('is the v3 body: v4 only lets the broker answer TRANSACTION_ABORTABLE', async () => {
    const { hex, fields } = vector(4, 'commit')
    const v3 = await require('../v3/request')(fields).encode()

    expect(v3.buffer.toString('hex')).toEqual(hex)
    expect(vector(3, 'commit').hex).toEqual(hex)
  })

  test('announces version 4 and is reachable through the version map', async () => {
    const { hex, fields } = vector(4, 'abort')
    const { request } = protocol({ version: 4 })(fields)

    expect(request).toMatchObject({ apiName: 'EndTxn', apiVersion: 4 })
    expect((await request.encode()).buffer.toString('hex')).toEqual(hex)
  })
})
