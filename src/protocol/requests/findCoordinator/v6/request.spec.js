const RequestV4Protocol = require('../v4/request')
const RequestV6Protocol = require('./request')

describe('Protocol > Requests > FindCoordinator > v6', () => {
  test('apiVersion is 6', () => {
    expect(RequestV6Protocol({ coordinatorKey: 'g', coordinatorType: 0 }).apiVersion).toEqual(6)
  })

  test('reuses v4 encoding byte-for-byte', async () => {
    const options = { coordinatorKey: 'test-group', coordinatorType: 1 }
    const v4 = await RequestV4Protocol(options).encode()
    const v6 = await RequestV6Protocol(options).encode()
    expect(v6.buffer).toEqual(v4.buffer)
  })
})
