const fs = require('node:fs')
const path = require('node:path')
const expected = require('../../../../testHelpers/wire/expected').securityAdmin
const { KafkaJSPartialMessageError } = require('../../../errors')
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
describe('DescribeClientQuotas codec', () => {
  test.each([0, 1])('literal v%s request and response bytes', async (version) => {
    const literal = expected['describeClientQuotasV' + version]
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
  describe('v1 quota key written as bytes', () => {
    /**
     * A v1 body with one entry: entity `user` named null, one value of 1.0 under `keyBytes`.
     * @param {string} keyBytes the compact string hex of the value key: `00` is null, `02 61` is "a"
     * @returns {Buffer}
     */
    const bodyWithKey = (keyBytes) =>
      Buffer.from(
        [
          '00000000', // ThrottleTimeMs
          '0000', // ErrorCode
          '00', // ErrorMessage, null
          '02', // Entries, one
          '02', // Entity, one
          '0575736572', // EntityType "user"
          '00', // EntityName, null
          '00', // Entity tags
          '02', // Values, one
          keyBytes,
          '3ff0000000000000', // Value 1.0
          '00', // Values tags
          '00', // Entries tags
          '00', // body tags
        ].join(''),
        'hex'
      )

    test('refuses a null quota key written as bytes', async () => {
      const error = await require('./v1/response')
        .decode(bodyWithKey('00'))
        .then(
          () => null,
          (rejection) => rejection
        )

      expect(error).toBeInstanceOf(KafkaJSPartialMessageError)
      expect(error.message).toContain('DescribeClientQuotasResponse.Entries.Values.Key')
    })

    test('decodes a quota key written as bytes', async () => {
      const decoded = await require('./v1/response').decode(bodyWithKey('0261'))

      expect(decoded.entries).toEqual([
        {
          entity: [{ entityType: 'user', entityName: null }],
          values: [{ key: 'a', value: 1 }],
        },
      ])
    })
  })
  test.each([0, 1])('Java v%s request and response goldens', async (version) => {
    const directory = path.join(__dirname, 'fixtures/java')
    const files = fs.existsSync(directory)
      ? fs
          .readdirSync(directory)
          .filter(
            (file) =>
              file.startsWith('describeClientQuotas') &&
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
