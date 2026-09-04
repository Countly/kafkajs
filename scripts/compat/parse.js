'use strict'

/**
 * Parser for the Apache Kafka message-definition dialect
 * (`clients/src/main/resources/common/message/*.json`). It is JSON plus `//` line
 * comments outside string literals. Only the keys this inventory needs are normalized:
 *
 *   apiKey                 int, absent for `"type": "header"` files
 *   type                   "request" | "response" | "header" (others are broker-internal)
 *   validVersions          closed range, "N" or "N-M" — the versions a broker accepts
 *   flexibleVersions       "N+" (KIP-482 compact encoding + tag buffers from N) or "none"
 *   deprecatedVersions     optional range clients should stop using
 *   latestVersionUnstable  optional bool; the top version is not advertised by default
 *   fields[]               ordered wire fields; nested via `fields`, or by struct name
 *                          resolved from `commonStructs`
 *   field.versions         "0+" / "3+" / "0-2"; defaults to `taggedVersions` when absent
 *   field.tag/taggedVersions  KIP-482 optional field, only legal in flexible versions
 *
 * Zero dependencies by design — see docs/Testing.md and `npm run compat:generate`.
 */

const fs = require('fs')

const stripComments = (source) => {
  let out = ''
  let inString = false
  for (let i = 0; i < source.length; i++) {
    const c = source[i]
    if (inString) {
      out += c
      if (c === '\\') {
        out += source[++i]
      } else if (c === '"') {
        inString = false
      }
    } else if (c === '"') {
      inString = true
      out += c
    } else if (c === '/' && source[i + 1] === '/') {
      while (i < source.length && source[i] !== '\n') i++
      out += '\n'
    } else {
      out += c
    }
  }
  return out
}

const parseMessage = (source) => JSON.parse(stripComments(source))
const readSpec = (file) => parseSpec(fs.readFileSync(file, 'utf8'))

const parseVersionRange = (spec) => {
  if (spec == null || spec === 'none') return null
  const text = String(spec).trim()
  const open = /^(\d+)\+$/.exec(text)
  if (open) return { min: Number(open[1]), max: null }
  const closed = /^(\d+)-(\d+)$/.exec(text)
  if (closed) return { min: Number(closed[1]), max: Number(closed[2]) }
  const single = /^(\d+)$/.exec(text)
  if (single) return { min: Number(single[1]), max: Number(single[1]) }
  throw new Error(`unparsable version range: ${JSON.stringify(spec)}`)
}

const parseValidVersions = (spec) => {
  const range = parseVersionRange(spec)
  if (range === null || range.max === null) {
    throw new Error(`validVersions must be a closed range, got ${JSON.stringify(spec)}`)
  }
  return range
}

const parseFlexibleVersions = (spec) => {
  const range = parseVersionRange(spec)
  return range === null ? null : range.min
}

const expandVersions = (range) =>
  range === null ? [] : Array.from({ length: range.max - range.min + 1 }, (_, i) => range.min + i)

const structNameOf = (type) => {
  const text = String(type)
  return text.indexOf('[]') === 0 ? text.slice(2) : text
}

const orNull = (value) => (value === undefined ? null : value)

const flattenFields = (fields, commonStructs, prefix, seen) => {
  const structs = commonStructs || {}
  const path = prefix || ''
  const visited = seen || []
  const rows = []
  for (const field of fields || []) {
    const row = {
      path: path + field.name,
      type: field.type,
      versions: field.versions === undefined ? orNull(field.taggedVersions) : field.versions,
      nullableVersions: orNull(field.nullableVersions),
      tag: orNull(field.tag),
      taggedVersions: orNull(field.taggedVersions),
      about: field.about === undefined ? '' : field.about,
    }
    rows.push(row)
    const structName = structNameOf(field.type)
    const nested = field.fields || (structs[structName] && structs[structName].fields)
    if (nested && visited.indexOf(structName) === -1) {
      rows.push(...flattenFields(nested, structs, `${row.path}.`, visited.concat(structName)))
    }
  }
  return rows
}

const parseSpec = (source) => {
  const raw = typeof source === 'string' ? parseMessage(source) : source
  const commonStructs = {}
  for (const struct of raw.commonStructs || []) commonStructs[struct.name] = struct
  const validVersions = raw.validVersions ? parseValidVersions(raw.validVersions) : null
  return {
    name: raw.name,
    type: raw.type,
    apiKey: orNull(raw.apiKey),
    listeners: raw.listeners || null,
    validVersions,
    flexibleFrom: parseFlexibleVersions(raw.flexibleVersions),
    deprecatedVersions: orNull(raw.deprecatedVersions),
    latestVersionUnstable: raw.latestVersionUnstable === true,
    fields: flattenFields(raw.fields, commonStructs),
  }
}

module.exports = {
  stripComments,
  parseMessage,
  readSpec,
  parseVersionRange,
  parseValidVersions,
  parseFlexibleVersions,
  expandVersions,
  flattenFields,
  parseSpec,
}
