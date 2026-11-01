const fs = require('node:fs')
const path = require('node:path')
const expected = require('../../../../testHelpers/wire/expected').securityAdmin
/** Revives synthetic fixture byte fields without changing other wire values.
 * @param {unknown} value JSON value
 * @returns {unknown} value with byte markers as Buffers
 */
const revive = (value) =>
  Array.isArray(value)
    ? value.map(revive)
    : value && typeof value === 'object'
      ? value.$bytes
        ? Buffer.from(value.$bytes, 'hex')
        : Object.fromEntries(Object.entries(value).map(([key, entry]) => [key, revive(entry)]))
      : value
describe('RenewDelegationToken codec', () => {
  test.each([1, 2])('literal v%s request and response bytes', async (version) => {
    const literal = expected['renewDelegationTokenV' + version]
    const request = require('./v' + version + '/request')(literal.request)
    const response = require('./v' + version + '/response')
    expect((await request.encode()).buffer.toString('hex')).toEqual(literal.requestHex)
    const decoded = await response.decode(Buffer.from(literal.responseHex, 'hex'))
    expect(decoded).toMatchObject(literal.response)
    expect(await response.parse(decoded)).toMatchObject(literal.response)
    expect(decoded.clientSideThrottleTime).toBe(literal.response.throttleTimeMs)
    await expect(
      response.decode(Buffer.from(literal.responseHex, 'hex').subarray(0, -1))
    ).rejects.toThrow()
    await expect(
      response.decode(Buffer.concat([Buffer.from(literal.responseHex, 'hex'), Buffer.from([0])]))
    ).rejects.toThrow()
  })
  test.each([1, 2])('Java v%s request and response goldens', async (version) => {
    const directory = path.join(__dirname, 'fixtures/java')
    const files = fs.existsSync(directory)
      ? fs
          .readdirSync(directory)
          .filter(
            (file) =>
              file.startsWith('renewDelegationToken') &&
              file.includes('-v' + version + '-security') &&
              file.endsWith('.json')
          )
      : []
    expect(files.length).toBe(2)
    for (const file of files) {
      const vector = JSON.parse(fs.readFileSync(path.join(directory, file), 'utf8'))
      const fields = revive(vector.fields)
      if (vector.message.endsWith('Request'))
        expect(
          (await require('./v' + version + '/request')(fields).encode()).buffer.toString('hex')
        ).toBe(vector.hex)
      else
        expect(
          await require('./v' + version + '/response').decode(Buffer.from(vector.hex, 'hex'))
        ).toMatchObject(fields)
    }
  })
})
