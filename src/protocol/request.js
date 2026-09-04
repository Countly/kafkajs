const Encoder = require('./encoder')
const { requestHeaderVersion, REQUEST_HEADER_V2 } = require('./headers')

/**
 * Frames one request: a 4-byte size, the request header, then the codec's body.
 *
 * Header v1 is `api_key api_version correlation_id client_id`; header v2 appends an (always
 * empty, from this client) tagged-field buffer. `client_id` is a classic INT16-prefixed
 * NULLABLE_STRING in both — RequestHeader.json pins that field to the old-style prefix so an
 * older broker can parse the header of any ApiVersionsRequest.
 * @see https://kafka.apache.org/43/design/protocol
 *
 * @param {object} options
 * @param {number} options.correlationId
 * @param {string} options.clientId
 * @param {{ apiKey: number, apiVersion: number, encode: () => Promise<Encoder> }} options.request
 * @returns {Promise<Encoder>} the complete, size-prefixed frame
 */
module.exports = async ({ correlationId, clientId, request: { apiKey, apiVersion, encode } }) => {
  const payload = await encode()
  const requestPayload = new Encoder()
    .writeInt16(apiKey)
    .writeInt16(apiVersion)
    .writeInt32(correlationId)
    .writeString(clientId)

  if (requestHeaderVersion(apiKey, apiVersion) === REQUEST_HEADER_V2) {
    requestPayload.writeTaggedFields()
  }

  requestPayload.writeEncoder(payload)

  return new Encoder().writeInt32(requestPayload.size()).writeEncoder(requestPayload)
}
