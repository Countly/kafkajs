const fs = require('node:fs')
const path = require('node:path')
const Encoder = require('../../../../src/protocol/encoder')
const { parseMessage, parseVersionRange } = require('../../../compat/parse')

/** @param {string} range Kafka version range. @param {number} version Selected version. @returns {boolean} Membership. */
const includes = (range, version) => {
  const parsed = parseVersionRange(range)
  return parsed && version >= parsed.min && (parsed.max === null || version <= parsed.max)
}

/**
 * Encodes scripted external replies from the pinned Kafka definitions, independently of decoders.
 * Values use upstream field names. Omitted nonnullable fields use zero/empty defaults.
 * @param {string} api Kafka API name.
 * @param {number} version Selected wire version.
 * @param {object} values Literal scripted result.
 * @returns {Buffer} Complete response body.
 * @throws {Error} For unsupported field types or definitions.
 */
const reply = (api, version, values = {}) => {
  const spec = parseMessage(
    fs.readFileSync(
      path.join(__dirname, '../../../compat/messages/4.3.1', `${api}Response.json`),
      'utf8'
    )
  )
  const flexible = includes(spec.flexibleVersions, version)
  const structures = new Map((spec.commonStructs ?? []).map((s) => [s.name, s.fields]))
  const encode = (fields, data) => {
    const out = new Encoder()
    for (const field of fields) {
      if (!includes(field.versions, version) || includes(field.taggedVersions, version)) continue
      const value = data[field.name]
      const type = field.type
      if (type.startsWith('[]')) {
        const items = value ?? []
        const scalar = type.slice(2)
        const entries = items.map((item) =>
          field.fields || structures.has(scalar)
            ? encode(field.fields ?? structures.get(scalar), item)
            : primitive(scalar, item)
        )
        if (flexible) out.writeUVarIntArray(entries)
        else out.writeArray(entries)
      } else if (field.fields || structures.has(type))
        out.writeEncoder(encode(field.fields ?? structures.get(type), value ?? {}))
      else out.writeEncoder(primitive(type, value))
    }
    if (flexible) out.writeTaggedFields()
    return out
  }
  const primitive = (type, value) => {
    const out = new Encoder()
    if (type === 'string')
      return flexible ? out.writeUVarIntString(value ?? '') : out.writeString(value ?? '')
    if (type === 'bytes' || type === 'records')
      return flexible ? out.writeUVarIntBytes(value ?? null) : out.writeBytes(value ?? null)
    if (type === 'uuid') return out.writeUuid(value ?? '00000000-0000-0000-0000-000000000000')
    if (type === 'bool') return out.writeBoolean(value ?? false)
    const method = {
      int8: 'writeInt8',
      int16: 'writeInt16',
      int32: 'writeInt32',
      int64: 'writeInt64',
    }[type]
    if (!method) throw new Error(`Unsupported scripted reply type: ${type}`)
    return out[method](value ?? 0)
  }
  return encode(spec.fields, values).buffer
}

module.exports = { reply }
