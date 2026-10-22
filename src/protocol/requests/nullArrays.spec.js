const Encoder = require('../encoder')
const { KafkaJSPartialMessageError } = require('../../errors')

const TOPICS = ['Topics', 'Topics.Partitions']
const GROUPS = ['Groups', 'Groups.Topics', 'Groups.Topics.Partitions']
const PRODUCED = [
  'Responses',
  'Responses.PartitionResponses',
  'Responses.PartitionResponses.RecordErrors',
]
const FETCHED = ['Responses', 'Responses.Partitions']

/**
 * The compact response arrays the message definition does not allow to be null: each codec, with
 * the path of every such array it refuses a null for, as its own `schema` names them, and the
 * arrays its body must hold an element in to decode at all (an OffsetFetch answer is for exactly
 * one group).
 */
const CODECS = [
  [
    'addPartitionsToTxn/v3',
    ['ResultsByTopicV3AndBelow', 'ResultsByTopicV3AndBelow.ResultsByPartition'],
  ],
  ['alterConfigs/v2', ['Responses']],
  ['alterPartitionReassignments/v0', ['Responses', 'Responses.Partitions']],
  ['createAcls/v2', ['Results']],
  ['createAcls/v3', ['Results']],
  ['deleteAcls/v2', ['FilterResults', 'FilterResults.MatchingAcls']],
  ['deleteAcls/v3', ['FilterResults', 'FilterResults.MatchingAcls']],
  ['deleteRecords/v2', ['Topics.Partitions']],
  ['describeConfigs/v4', ['Results']],
  ['describeGroups/v5', ['Groups', 'Groups.Members']],
  ['describeGroups/v6', ['Groups', 'Groups.Members']],
  ['listOffsets/v6', TOPICS],
  ['listOffsets/v9', TOPICS],
  ['offsetCommit/v8', TOPICS],
  ['offsetCommit/v9', TOPICS],
  ['offsetCommit/v10', TOPICS],
  ['offsetFetch/v6', TOPICS],
  ['offsetFetch/v7', TOPICS],
  ['offsetFetch/v8', GROUPS, ['Groups']],
  ['offsetFetch/v9', GROUPS, ['Groups']],
  ['offsetFetch/v10', GROUPS, ['Groups']],
  ['produce/v9', PRODUCED],
  ['produce/v10', PRODUCED],
  ['produce/v12', PRODUCED],
  ['produce/v13', PRODUCED],
  ['txnOffsetCommit/v3', TOPICS],
  ['txnOffsetCommit/v5', TOPICS],
  ['fetch/v12', FETCHED],
  ['fetch/v13', FETCHED],
  ['fetch/v16', FETCHED],
  ['fetch/v18', FETCHED],
]

/**
 * @param {string} versions a schema version range: `N`, `N+` or `N-M`
 * @param {number} version
 * @returns {boolean} whether `version` is in the range
 */
const covers = (versions, version) => {
  const [, first, rest] = /^(\d+)(\+|-\d+)?$/.exec(versions)
  if (rest === undefined) return version === Number(first)
  return version >= Number(first) && (rest === '+' || version <= Number(rest.slice(1)))
}

const SCALARS = {
  int8: (encoder) => encoder.writeInt8(0),
  int16: (encoder) => encoder.writeInt16(0),
  int32: (encoder) => encoder.writeInt32(0),
  int64: (encoder) => encoder.writeInt64(0),
  bool: (encoder) => encoder.writeBoolean(false),
  uuid: (encoder) => encoder.writeUuid(null),
  string: (encoder) => encoder.writeUVarIntString('x'),
  bytes: (encoder) => encoder.writeUVarIntBytes(Buffer.alloc(0)),
  records: (encoder) => encoder.writeUVarIntBytes(Buffer.alloc(0)),
}

/**
 * Writes a compact-framed response body field by field from a codec's schema, with every array
 * empty, so that it decodes. Only the arrays in `filled` and on the way to `nullPath` hold an
 * element, and the array at `nullPath` is written as null.
 * @param {{ fields: object[], version: number }} schema
 * @param {string} [nullPath] the schema name of the array to write as null
 * @param {string[]} [filled] the schema names of arrays to write with one element
 * @returns {Buffer}
 */
const bodyOf = ({ fields, version }, nullPath, filled = []) => {
  const writeStruct = (encoder, prefix) => {
    for (const field of fields) {
      const own = field.name.startsWith(prefix) && !field.name.slice(prefix.length).includes('.')
      if (own && field.tag === undefined && covers(field.versions, version)) {
        writeField(encoder, field)
      }
    }
    return encoder.writeTaggedFields()
  }

  const writeField = (encoder, { name, type }) => {
    if (!type.startsWith('[]')) return SCALARS[type](encoder)
    if (name === nullPath) return encoder.writeUVarInt(0)
    const holdsOne = filled.includes(name) || (nullPath ?? '').startsWith(`${name}.`)
    return encoder.writeUVarIntArray(holdsOne ? [writeStruct(new Encoder(), `${name}.`)] : [])
  }

  return writeStruct(new Encoder(), '').buffer
}

/**
 * @param {*} codec a response module, or the factory deleteRecords exports
 * @returns {{ decode: Function, schema: object }}
 */
const responseOf = (codec) => ({
  decode: typeof codec === 'function' ? codec({ topics: [] }).decode : codec.decode,
  schema: codec.schema,
})

describe('Protocol > Requests > a null array in a response that does not allow one', () => {
  describe.each(CODECS)('%s', (file, paths, filled) => {
    const { decode, schema } = responseOf(require(`./${file}/response`))

    test('decodes a body with every array empty', async () => {
      await expect(decode(bodyOf(schema, undefined, filled))).resolves.toBeDefined()
    })

    test.each(paths)('refuses a null %s, naming it', async (path) => {
      const error = await (async () => decode(bodyOf(schema, path, filled)))().then(
        () => null,
        (rejection) => rejection
      )

      expect(error).toBeInstanceOf(KafkaJSPartialMessageError)
      expect(error.message).toContain(`${schema.api}Response.${path}`)
    })
  })
})
