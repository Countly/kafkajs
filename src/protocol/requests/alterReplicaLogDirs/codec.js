const { KafkaJSInvariantViolation } = require('../../../errors')
const Encoder = require('../../encoder')
const Decoder = require('../../decoder')

/** Encodes one broker's moves, grouped by directory and topic.
 *
 * @param {number} version protocol version 1 or 2
 *
 * @param {object} options dirs [{path,topics:[{topic,partitions:number[]}]}]
 *
 * @returns {object} request encoder
 *
 * @throws {Error} invalid encoding input
 */
const request = (version, { dirs }) => ({
  apiKey: 34,
  apiVersion: version,
  apiName: 'AlterReplicaLogDirs',
  encode: async () => {
    const flexible = version >= 2
    const array = flexible ? 'writeUVarIntArray' : 'writeArray'
    const string = flexible ? 'writeUVarIntString' : 'writeString'
    const tags = (e) => (flexible ? e.writeTaggedFields() : e)
    return tags(
      new Encoder()[array](
        dirs.map(({ path, topics }) =>
          tags(
            new Encoder()
              [string](path)
              [array](
                topics.map(({ topic, partitions }) =>
                  tags(
                    new Encoder()
                      [string](topic)
                      [array](partitions.map((p) => new Encoder().writeInt32(p)))
                  )
                )
              )
          )
        )
      )
    )
  },
})

/** Reads replica errors as data; a storage refusal is never a whole-call parser throw.
 *
 * @param {number} version protocol version 1 or 2
 *
 * @param {Buffer} bytes response body
 *
 * @returns {Promise<object>} topics with per-partition numeric codes
 *
 * @throws {Error} invalid framing or null required arrays
 */
const decode = async (version, bytes) => {
  const d = new Decoder(bytes)
  const flexible = version >= 2
  const array = flexible ? 'readUVarIntArray' : 'readArray'
  const string = flexible ? 'readUVarIntString' : 'readString'
  const tags = (x) => {
    if (flexible) x.readTaggedFields()
  }
  const required = (x, read) => Decoder.requireArray(x[array](read), 'AlterReplicaLogDirs')
  const throttle = d.readInt32()
  const topics = required(d, (t) => {
    const topic = t[string]()
    const partitions = required(t, (p) => {
      const row = { partition: p.readInt32(), errorCode: p.readInt16() }
      tags(p)
      return row
    })
    tags(t)
    return { topic, partitions }
  })
  tags(d)
  if (d.canReadBytes(1))
    throw new KafkaJSInvariantViolation('AlterReplicaLogDirs response has trailing bytes')
  return { throttleTime: 0, clientSideThrottleTime: throttle, topics }
}
module.exports = { request, decode }
