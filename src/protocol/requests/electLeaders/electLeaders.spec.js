const fs = require('fs')
const path = require('path')
const layout = require('./layout.json')
const { requestHeaderVersion, responseHeaderVersion } = require('../../headers')
/** Verifies independent literal bytes in both directions. @param {object} vector case @returns {Promise<void>} assertions @throws {Error} mismatch */
const check = async (vector) => {
  const direction = vector.message.endsWith('Request') ? 'request' : 'response'
  const codec = require(`./v${vector.version}/${direction}`)
  if (direction === 'request') {
    const r = codec(vector.fields)
    expect((await r.encode()).buffer.toString('hex')).toBe(vector.hex)
    const flexible = vector.version >= 2
    expect(requestHeaderVersion(r.apiKey, r.apiVersion)).toBe(flexible ? 2 : 1)
    expect(responseHeaderVersion(r.apiKey, r.apiVersion)).toBe(flexible ? 1 : 0)
  } else {
    expect(await codec.decode(Buffer.from(vector.hex, 'hex'))).toEqual({
      ...vector.fields,
      throttleTime: 0,
      clientSideThrottleTime: 17,
    })
    await expect(codec.decode(Buffer.from(vector.hex + 'ff', 'hex'))).rejects.toThrow()
    await expect(codec.decode(Buffer.from(vector.hex, 'hex').subarray(0, -1))).rejects.toThrow()
  }
}
test.each(layout)('layout $message v$version $case', check)
test('layout pair directions match independent fields', () => {
  for (const vector of layout) {
    const pair = require('../../../../testHelpers/wire/electLeadersV' + vector.version)
    if (vector.message.endsWith('Request')) {
      expect(pair.readRequest(Buffer.from(vector.hex, 'hex'))).toEqual(vector.fields)
      expect(() => pair.readRequest(Buffer.from(vector.hex + 'ff', 'hex'))).toThrow()
      expect(() => pair.readRequest(Buffer.from(vector.hex, 'hex').subarray(0, -1))).toThrow()
    } else expect(pair.buildResponse(vector.fields).toString('hex')).toBe(vector.hex)
  }
})
test('Java vectors independently match every prescribed case', async () => {
  const dir = path.join(__dirname, 'fixtures/java')
  const vectors = fs
    .readdirSync(dir)
    .filter((f) => f.endsWith('.json'))
    .map((f) => JSON.parse(fs.readFileSync(path.join(dir, f), 'utf8')))
  for (const vector of layout) {
    const java = vectors.find(
      (v) => v.message === vector.message && v.version === vector.version && v.case === vector.case
    )
    expect(java).toEqual(vector)
    await check(java)
  }
})

test('layout unknown tags are skipped without shifting known fields', async () => {
  const vector = layout.find((v) => v.message.endsWith('Response') && v.version === 2)
  const codec = require(`./v${vector.version}/response`)
  const extended = vector.hex.slice(0, -2) + '01020178'
  expect(await codec.decode(Buffer.from(extended, 'hex'))).toEqual({
    ...vector.fields,
    throttleTime: 0,
    clientSideThrottleTime: 17,
  })
})

test('layout nonnullable classic result arrays reject the null sentinel', async () => {
  const vector = layout.find((v) => v.message.endsWith('Response') && v.version === 0)
  const body = Buffer.from(vector.hex, 'hex')
  body.writeInt32BE(-1, 4)
  await expect(require('./v0/response').decode(body)).rejects.toThrow()
})

test('layout required strings reject a null wire sentinel', async () => {
  const vector = layout.find((v) => v.message.endsWith('Response') && v.version === 2)
  const value = JSON.parse(JSON.stringify(vector.fields))
  value.replicaElectionResults[0].topic = null
  const bytes = require('../../../../testHelpers/wire/electLeadersV2').buildResponse(value)
  await expect(require('./v2/response').decode(bytes)).rejects.toThrow('must not be null')
})
