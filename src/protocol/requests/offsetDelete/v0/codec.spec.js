const fs = require('node:fs')
const path = require('node:path')
const request = require('./request')
const response = require('./response')
/** Reads a committed independently generated vector; absence is a failing gate.
 * @param {string} name generated fixture basename
 * @returns {object} independent bytes and generator fields
 * @throws {Error} missing or invalid fixture
 */
const vector = (name) =>
  JSON.parse(fs.readFileSync(path.join(__dirname, '../fixtures/java', name), 'utf8'))
describe('OffsetDelete independent Java codecs', () => {
  it('encodes the Java request and decodes its mixed response', async () => {
    const sent = vector('offsetDeleteRequest-v0-basic.json')
    const received = vector('offsetDeleteResponse-v0-mixed.json')
    expect((await request(sent.fields).encode()).buffer.toString('hex')).toBe(sent.hex)
    const decoded = await response.decode(Buffer.from(received.hex, 'hex'))
    expect(decoded).toEqual({
      errorCode: 0,
      throttleTime: 0,
      clientSideThrottleTime: 17,
      topics: received.fields.topics,
    })
    expect(await response.parse(decoded)).toBe(decoded)
  })
})
