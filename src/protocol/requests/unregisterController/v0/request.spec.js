const request = require('./request')
const wire = require('../../../../../testHelpers/wire/unregisterControllerV0')
const golden = require('../fixtures/v0_request.json')
const { requestHeaderVersion, responseHeaderVersion } = require('../../../headers')

describe('Protocol > Requests > UnregisterController > v0', () => {
  test('encodes controller seven byte for byte', async () => {
    const descriptor = request({ controllerId: 7 })
    expect(descriptor).toMatchObject({ apiKey: 94, apiVersion: 0, apiName: 'UnregisterController' })
    expect((await descriptor.encode()).buffer.toString('hex')).toBe(golden.hex)
    expect(requestHeaderVersion(94, 0)).toBe(2)
    expect(responseHeaderVersion(94, 0)).toBe(1)
  })

  test('reads the golden controller id and consumes every byte', async () => {
    const bytes = Buffer.from(golden.hex, 'hex')
    expect(wire.readRequest(bytes)).toEqual({ controllerId: 7 })
    expect((await request(wire.readRequest(bytes)).encode()).buffer).toEqual(bytes)
    expect(() => wire.readRequest(Buffer.concat([bytes, Buffer.from([0])]))).toThrow()
    expect(() => wire.readRequest(bytes.subarray(0, 4))).toThrow()
    const maxInt32 = Buffer.from('7fffffff00', 'hex')
    expect((await request({ controllerId: 2147483647 }).encode()).buffer).toEqual(maxInt32)
    expect(wire.readRequest(maxInt32)).toEqual({ controllerId: 2147483647 })
  })
})
