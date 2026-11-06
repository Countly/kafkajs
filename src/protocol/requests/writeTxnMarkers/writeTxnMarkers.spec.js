const fs = require('fs')
const path = require('path')
const layout = require('./layout.json')
const { requestHeaderVersion, responseHeaderVersion } = require('../../headers')
/** Checks one independent vector against the production codec. @param {object} vector fields/bytes @returns {Promise<void>} assertions @throws {Error} codec mismatch. */
const check = async (vector) => {
  const direction = vector.message.endsWith('Request') ? 'request' : 'response'
  const codec = require(`./v${vector.version}/${direction}`)
  if (direction === 'request') {
    const request = codec(vector.fields)
    expect((await request.encode()).buffer.toString('hex')).toBe(vector.hex)
    expect(requestHeaderVersion(request.apiKey, request.apiVersion)).toBe(2)
    expect(responseHeaderVersion(request.apiKey, request.apiVersion)).toBe(1)
  } else {
    const decoded = await codec.decode(Buffer.from(vector.hex, 'hex'))
    const expected = { ...vector.fields }
    if ('throttleTime' in expected) {
      expected.clientSideThrottleTime = expected.throttleTime
      expected.throttleTime = 0
    }
    expect(decoded).toEqual(expected)
    await expect(codec.decode(Buffer.from(vector.hex, 'hex').subarray(0, -1))).rejects.toThrow()
    await expect(codec.decode(Buffer.from(vector.hex + 'ff', 'hex'))).rejects.toThrow()
  }
}
test.each(layout)('layout $message v$version $case', check)
test('Java vectors independently match every prescribed case', async () => {
  const directory = path.join(__dirname, 'fixtures/java')
  const java = fs
    .readdirSync(directory)
    .filter((file) => file.endsWith('.json'))
    .map((file) => require(path.join(directory, file)))
  for (const vector of layout) {
    const independent = java.find(
      (other) =>
        other.message === vector.message &&
        other.version === vector.version &&
        other.case === vector.case
    )
    expect(independent).toEqual(vector)
    await check(independent)
  }
})

test('layout pairs preserve every independent layout field', () => {
  for (const vector of layout) {
    const pair = require('../../../../testHelpers/wire/writeTxnMarkersV' + vector.version)
    if (vector.message.endsWith('Request')) {
      expect(pair.readRequest(Buffer.from(vector.hex, 'hex'))).toEqual(vector.fields)
      expect(() => pair.readRequest(Buffer.from(vector.hex + 'ff', 'hex'))).toThrow(
        'its tag buffer'
      )
      expect(() => pair.readRequest(Buffer.from(vector.hex, 'hex').subarray(0, -1))).toThrow()
    } else expect(pair.buildResponse(vector.fields).toString('hex')).toBe(vector.hex)
  }
})
