const Encoder = require('../../src/protocol/encoder')
const Decoder = require('../../src/protocol/decoder')
const vector = require('../../src/protocol/requests/getTelemetrySubscriptions/fixtures/java/getTelemetrySubscriptionsResponse-v0-basic.json')

module.exports = {
  api: 'GetTelemetrySubscriptions',
  apiKey: 71,
  version: 0,
  /**
   * Starts from independent response bytes and replaces only the telemetry fields named here.
   * Variable codec/prefix arrays use their v0 layout, never a production response encoder.
   * @param {object} [fields] finite subscription, error and throttle overrides
   * @returns {Buffer} a complete flexible v0 response body
   * @throws {Error} on null arrays/elements or invalid scalar values
   */
  buildResponse(fields = {}) {
    const value = { ...vector.fields, ...fields }
    if (
      !Array.isArray(value.acceptedCompressionTypes) ||
      !Array.isArray(value.requestedMetrics) ||
      value.requestedMetrics.some((prefix) => typeof prefix !== 'string')
    ) {
      throw new Error('Telemetry arrays and prefixes cannot be null')
    }
    const head = Buffer.from(vector.hex, 'hex').subarray(0, 26)
    head.writeInt32BE(value.throttleTime, 0)
    head.writeInt16BE(value.errorCode, 4)
    new Encoder().writeUuid(value.clientInstanceId).buffer.copy(head, 6)
    head.writeInt32BE(value.subscriptionId, 22)
    const tail = new Encoder().writeUVarInt(value.acceptedCompressionTypes.length + 1)
    for (const codec of value.acceptedCompressionTypes) tail.writeInt8(codec)
    tail
      .writeInt32(value.pushIntervalMs)
      .writeInt32(value.telemetryMaxBytes)
      .writeBoolean(value.deltaTemporality)
      .writeUVarInt(value.requestedMetrics.length + 1)
    for (const prefix of value.requestedMetrics) tail.writeUVarIntString(prefix)
    return Buffer.concat([head, tail.writeTaggedFields().buffer])
  },
  /**
   * Reads one complete v0 Get body with inherited UUID and tag bounds.
   * @param {Buffer} body header-free request bytes
   * @returns {{clientInstanceId: string|null}} canonical UUID or the all-zero sentinel
   * @throws {Error} on truncation, invalid tags or trailing bytes
   */
  readRequest(body) {
    const decoder = new Decoder(body)
    const request = { clientInstanceId: decoder.readUuid() }
    decoder.readTaggedFields()
    if (decoder.canReadBytes(1))
      throw new Error('GetTelemetrySubscriptions v0 request has a byte after its tag buffer')
    return request
  },
}
