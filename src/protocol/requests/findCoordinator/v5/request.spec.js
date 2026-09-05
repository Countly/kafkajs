const RequestV4Protocol = require('../v4/request')
const RequestV5Protocol = require('./request')

describe('Protocol > Requests > FindCoordinator > v5', () => {
  test('apiVersion is 5', () => {
    expect(RequestV5Protocol({ coordinatorKey: 'g', coordinatorType: 0 }).apiVersion).toEqual(5)
  })

  test('reuses v4 encoding byte-for-byte', async () => {
    const options = { coordinatorKey: 'test-group', coordinatorType: 1 }
    const v4 = await RequestV4Protocol(options).encode()
    const v5 = await RequestV5Protocol(options).encode()
    expect(v5.buffer).toEqual(v4.buffer)
  })
})
