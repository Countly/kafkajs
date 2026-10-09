const RequestV5Protocol = require('./request')
const { protocol, versions } = require('../index')

const vector = (version, caseId) =>
  require(`../fixtures/java/endTxnRequest-v${version}-${caseId}.json`)

describe('Protocol > Requests > EndTxn > v5', () => {
  test.each(['commit', 'abort'])(
    'encodes the Apache 4.3.1 %s vector byte for byte',
    async (caseId) => {
      const { hex, fields } = vector(5, caseId)
      const { buffer } = await RequestV5Protocol(fields).encode()

      expect(buffer.toString('hex')).toEqual(hex)
    }
  )

  test('is the v3 body: only the response grows at v5', async () => {
    const { hex, fields } = vector(5, 'abort')
    const v3 = await require('../v3/request')(fields).encode()

    expect(v3.buffer.toString('hex')).toEqual(hex)
    expect(vector(3, 'abort').hex).toEqual(hex)
  })

  test.each(['commit', 'abort'])(
    'announces version 5 and is reachable through the version map as %s',
    async (caseId) => {
      const { hex, fields } = vector(5, caseId)
      const { request } = protocol({ version: 5 })(fields)

      expect(request).toMatchObject({ apiName: 'EndTxn', apiVersion: 5 })
      expect((await request.encode()).buffer.toString('hex')).toEqual(hex)
      expect(versions).toEqual(['0', '1', '2', '3', '4', '5'])
    }
  )
})
