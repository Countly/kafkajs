const Decoder = require('../../src/protocol/decoder')
const vector = require('../../src/protocol/requests/pushTelemetry/fixtures/java/pushTelemetryResponse-v0-ok.json')

module.exports = {
  api: 'PushTelemetry',
  apiKey: 72,
  version: 0,
  /**
   * Patches only throttle and error in the independent seven-byte Java response template.
   * @param {{throttleTime?: number, errorCode?: number}} [fields] scalar response overrides
   * @returns {Buffer} a fresh complete v0 response body
   * @throws {RangeError} for a scalar outside its signed wire range
   */
  buildResponse(fields = {}) {
    const value = { ...vector.fields, ...fields }
    const body = Buffer.from(vector.hex, 'hex')
    body.writeInt32BE(value.throttleTime, 0)
    body.writeInt16BE(value.errorCode, 4)
    return body
  },
  /**
   * Reads a complete Push request; payload bytes are represented as hex in fixture fields.
   * @param {Buffer} body request bytes after the flexible header
   * @returns {object} UUID, signed subscription, final flag, signed codec and Metrics hex
   * @throws {Error} on null Metrics, truncation, invalid tags or trailing bytes
   */
  readRequest(body) {
    const decoder = new Decoder(body)
    const request = {
      clientInstanceId: decoder.readUuid(),
      subscriptionId: decoder.readInt32(),
      terminating: decoder.readBoolean(),
      compressionType: decoder.readInt8(),
    }
    const metrics = decoder.readUVarIntBytes()
    if (metrics === null) throw new Error('PushTelemetry Metrics cannot be null')
    request.metrics = metrics.toString('hex')
    decoder.readTaggedFields()
    if (decoder.canReadBytes(1))
      throw new Error('PushTelemetry v0 request has a byte after its tag buffer')
    return request
  },
}
