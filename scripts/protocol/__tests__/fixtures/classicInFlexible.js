const Encoder = require('../../../../src/protocol/encoder')

/**
 * A Heartbeat v4 request that declares the right schema and writes the wrong bytes: classic
 * strings and no tag buffer, where the definition is flexible from v4. Two declaration tables
 * agree on it; only its output gives it away.
 * @param {{ groupId: string, generationId: number, memberId: string, groupInstanceId: string|null }} options
 */
module.exports = ({ groupId, generationId, memberId, groupInstanceId }) => ({
  apiKey: 12,
  apiVersion: 4,
  apiName: 'Heartbeat',
  encode: async () =>
    new Encoder()
      .writeString(groupId)
      .writeInt32(generationId)
      .writeString(memberId)
      .writeString(groupInstanceId),
})

module.exports.schema = {
  api: 'Heartbeat',
  version: 4,
  direction: 'request',
  fields: [
    { name: 'GroupId', type: 'string', versions: '0+' },
    { name: 'GenerationId', type: 'int32', versions: '0+' },
    { name: 'MemberId', type: 'string', versions: '0+' },
    { name: 'GroupInstanceId', type: 'string', versions: '3+', nullable: true },
  ],
}
