const { decodeGroup, decode, parse } = require('./response')
const Encoder = require('../../../encoder')
const Decoder = require('../../../decoder')
const { createErrorFromCode } = require('../../../error')

describe('Protocol > Requests > ListGroups > v0', () => {
  test('reports the two fields it cannot carry as empty strings', async () => {
    // v0-v2 share this decoder, and none of them has GroupState or GroupType. Reporting them as
    // `''` rather than leaving them undefined is what lets one caller read one row shape at every
    // negotiated version - the admin layer lower-cases `groupType`, and `undefined` has no case.
    const row = new Encoder().writeString('test-group').writeString('consumer')

    expect(decodeGroup(new Decoder(row.buffer))).toEqual({
      groupId: 'test-group',
      protocolType: 'consumer',
      groupState: '',
      groupType: '',
    })
  })

  test('decodes an error code and a non-compact group array', async () => {
    const data = await decode(
      Buffer.from('0000' + '00000001' + '000a746573742d67726f7570' + '0008636f6e73756d6572', 'hex')
    )

    expect(data).toEqual({
      errorCode: 0,
      groups: [{ groupId: 'test-group', protocolType: 'consumer', groupState: '', groupType: '' }],
    })
  })

  test('throws on a top-level error code, which is what the retrier acts on', async () => {
    // COORDINATOR_LOAD_IN_PROGRESS and COORDINATOR_NOT_AVAILABLE arrive here, and the shipped
    // retrier turns a thrown retriable protocol error into a retry - so `parse` must throw.
    await expect(parse({ errorCode: 14, groups: [] })).rejects.toThrow(createErrorFromCode(14))
    await expect(parse({ errorCode: 0, groups: [] })).resolves.toEqual({
      errorCode: 0,
      groups: [],
    })
  })
})
