const request = require('./request')
const wire = require('../../../../../testHelpers/wire/unregisterControllerV0')
const { requestHeaderVersion, responseHeaderVersion } = require('../../../headers')

describe('Protocol > Requests > UnregisterController > v0', () => {
  test('encodes controller seven byte for byte', async () => {
    const descriptor = request({ controllerId: 7 })
    expect(descriptor).toMatchObject({ apiKey: 94, apiVersion: 0, apiName: 'UnregisterController' })
    expect((await descriptor.encode()).buffer.toString('hex')).toBe('0000000700')
    expect(requestHeaderVersion(94, 0)).toBe(2)
    expect(responseHeaderVersion(94, 0)).toBe(1)
  })

  test('reads the golden controller id and consumes every byte', async () => {
    const golden = Buffer.from('0000000700', 'hex')
    expect(wire.readRequest(golden)).toEqual({ controllerId: 7 })
    expect((await request(wire.readRequest(golden)).encode()).buffer).toEqual(golden)
    expect(() => wire.readRequest(Buffer.concat([golden, Buffer.from([0])]))).toThrow()
    expect(() => wire.readRequest(golden.subarray(0, 4))).toThrow()
    const maxInt32 = Buffer.from('7fffffff00', 'hex')
    expect((await request({ controllerId: 2147483647 }).encode()).buffer).toEqual(maxInt32)
    expect(wire.readRequest(maxInt32)).toEqual({ controllerId: 2147483647 })
  })
})
