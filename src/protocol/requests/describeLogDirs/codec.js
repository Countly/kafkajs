const { KafkaJSInvariantViolation } = require('../../../errors')
const Encoder = require('../../encoder')
const Decoder = require('../../decoder')

/** Builds a versioned broker-local directory selection; null topics means all.
 *
 * @param {number} version protocol version 1..5
 *
 * @param {object} options topics, or null
 *
 * @returns {object} request encoder
 *
 * @throws {Error} invalid encoding input
 */
const request = (version, { topics = null } = {}) => ({
  apiKey: 35,
  apiVersion: version,
  apiName: 'DescribeLogDirs',
  encode: async () => {
    const flexible = version >= 2
    const array = flexible ? 'writeUVarIntArray' : 'writeArray'
    const string = flexible ? 'writeUVarIntString' : 'writeString'
    if (!flexible && topics === null) return new Encoder().writeInt32(-1)
    const e = new Encoder()[array](
      topics === null
        ? null
        : topics.map(({ topic, partitions }) => {
            const t = new Encoder()
              [string](topic)
              [array](partitions.map((p) => new Encoder().writeInt32(p)))
            return flexible ? t.writeTaggedFields() : t
          })
    )
    return flexible ? e.writeTaggedFields() : e
  },
})

/** Reads directory data without throwing broker-level or directory-level protocol errors.
 *
 * @param {number} version protocol version 1..5
 *
 * @param {Buffer} bytes response body
 *
 * @returns {Promise<object>} normalized directories, decimal sizes, version-absent null fields
 *
 * @throws {Error} truncated, null required arrays or surplus bytes
 */
const decode = async (version, bytes) => {
  const d = new Decoder(bytes)
  const flexible = version >= 2
  const array = flexible ? 'readUVarIntArray' : 'readArray'
  const string = flexible ? 'readUVarIntString' : 'readString'
  const tags = (x) => {
    if (flexible) x.readTaggedFields()
  }
  const required = (x, read) => Decoder.requireArray(x[array](read), 'DescribeLogDirs')
  const throttle = d.readInt32()
  const errorCode = version >= 3 ? d.readInt16() : 0
  const logDirs = required(d, (x) => {
    const errorCode = x.readInt16()
    const path = x[string]()
    const topics = required(x, (t) => {
      const topic = t[string]()
      const partitions = required(t, (p) => {
        const row = {
          partition: p.readInt32(),
          size: p.readInt64().toString(),
          offsetLag: p.readInt64().toString(),
          isFuture: p.readBoolean(),
        }
        tags(p)
        return row
      })
      tags(t)
      return { topic, partitions }
    })
    const totalBytes = version >= 4 ? x.readInt64().toString() : null
    const usableBytes = version >= 4 ? x.readInt64().toString() : null
    const isCordoned = version >= 5 ? x.readBoolean() : null
    tags(x)
    return { errorCode, path, topics, totalBytes, usableBytes, isCordoned }
  })
  tags(d)
  if (d.canReadBytes(1))
    throw new KafkaJSInvariantViolation('DescribeLogDirs response has trailing bytes')
  return { throttleTime: 0, clientSideThrottleTime: throttle, errorCode, logDirs }
}
module.exports = { request, decode }
