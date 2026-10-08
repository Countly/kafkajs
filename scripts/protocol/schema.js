'use strict'

/**
 * Validates every codec's declared `schema` export against the committed Apache
 * message-definition snapshot (`scripts/compat/messages/<tag>/`). A codec earns machine
 * validation by exporting `schema` (see `src/protocol/requests/apiVersions/v0/response.js`
 * for the shape); codecs without one are reported as unvalidated so coverage can grow API by
 * API instead of a big-bang migration.
 *
 * Zero dependencies beyond `scripts/compat/parse` and `src/protocol/headers`, CommonJS,
 * Node >= 22 — same conventions as `scripts/protocol/table.js`.
 */

const fs = require('fs')
const path = require('path')
const { readSpec, parseVersionRange } = require('../compat/parse')
const { kafkaTag } = require('./table')
const { isFlexible } = require('../../src/protocol/headers')

const COMPAT_ROOT = path.join(__dirname, '..', 'compat')
const REPO_ROOT = path.join(__dirname, '..', '..')
const REQUESTS_ROOT = path.join(REPO_ROOT, 'src', 'protocol', 'requests')
const VERSION_DIR = /^v\d+$/

/**
 * @param {string} absolutePath
 * @returns {string} `absolutePath` relative to the repo root, with POSIX separators
 */
const toRepoRelative = (absolutePath) =>
  path.relative(REPO_ROOT, absolutePath).split(path.sep).join('/')

/**
 * @param {{min: number, max: number|null}|null} range
 * @param {number} version
 * @returns {boolean} whether `version` falls inside the closed (or open-ended) range
 */
const rangeContains = (range, version) =>
  range !== null && version >= range.min && (range.max === null || version <= range.max)

/**
 * @param {string} api Apache message name without `Request`/`Response`, e.g. `ApiVersions`
 * @param {'request'|'response'} direction
 * @returns {string} absolute path to the committed message-definition JSON
 */
const specFile = (api, direction) =>
  path.join(
    COMPAT_ROOT,
    'messages',
    kafkaTag(),
    `${api}${direction === 'request' ? 'Request' : 'Response'}.json`
  )

/**
 * Walks `src/protocol/requests/*<code>/v*</code>/`, requiring `request.js` and `response.js`
 * where present. Requiring a codec must be side-effect free; a module whose `require` throws
 * is still reported, with `requireError` set instead of `schema`.
 * @returns {{file: string, dir: string, direction: 'request'|'response', module: object|null, schema: object|null, requireError: Error|null}[]}
 *          sorted by `file`, so the report is stable
 */
const discover = () => {
  const codecs = []

  for (const apiName of fs.readdirSync(REQUESTS_ROOT).sort()) {
    const apiDir = path.join(REQUESTS_ROOT, apiName)
    if (!fs.statSync(apiDir).isDirectory()) continue

    for (const versionName of fs.readdirSync(apiDir).sort()) {
      if (!VERSION_DIR.test(versionName)) continue
      const versionDir = path.join(apiDir, versionName)

      for (const direction of ['request', 'response']) {
        const file = path.join(versionDir, `${direction}.js`)
        if (!fs.existsSync(file)) continue

        let mod = null
        let schema = null
        let requireError = null
        try {
          mod = require(file)
          schema = mod && mod.schema !== undefined ? mod.schema : null
        } catch (error) {
          requireError = error
        }

        codecs.push({
          file: toRepoRelative(file),
          dir: toRepoRelative(versionDir),
          direction,
          module: mod,
          schema,
          requireError,
        })
      }
    }
  }

  return codecs.sort((a, b) => (a.file < b.file ? -1 : a.file > b.file ? 1 : 0))
}

/**
 * Resolves the field list a codec must declare at `version`, from an already-parsed spec (see
 * `scripts/compat/parse#parseSpec`): the fields in JSON order, filtered to those alive at
 * `version`, each carrying `nullable`/`tag` only when they apply at that version.
 *
 * A field declared only as tagged arrives from `flattenFields` with `versions: null` (its own
 * `versions` key is absent or explicitly `null`, so nothing but `taggedVersions` says when it is
 * alive); such a row is range-tested and reported using its `taggedVersions` instead, never
 * dropped silently by a `null` range.
 *
 * A field nested under a field that is not alive at `version` is not alive either, whatever its
 * own range says: a common struct's fields carry the struct's range even where the struct is only
 * reachable through a version-limited parent (AddPartitionsToTxnRequest's `Transactions.Topics`,
 * v4+, reuses the `0+` topic struct of `V3AndBelowTopics`). `flattenFields` emits every parent
 * before its children, so one pass decides it.
 * @param {{fields: object[]}} spec a parsed message spec
 * @param {number} version the concrete API version to resolve fields for
 * @returns {{name: string, type: string, versions: string, nullable?: true, tag?: number}[]}
 */
const fieldsFromSpec = (spec, version) => {
  const rows = []
  const alive = new Set()

  for (const field of spec.fields) {
    const parent = field.path.slice(0, Math.max(0, field.path.lastIndexOf('.')))
    if (parent !== '' && !alive.has(parent)) continue

    const versions = field.versions === null ? field.taggedVersions : field.versions
    if (!rangeContains(parseVersionRange(versions), version)) continue

    alive.add(field.path)
    const row = { name: field.path, type: field.type, versions }
    if (rangeContains(parseVersionRange(field.nullableVersions), version)) row.nullable = true
    if (field.tag !== null) row.tag = field.tag
    rows.push(row)
  }

  return rows
}

/**
 * Resolves the field list a codec must declare for `(api, direction, version)`, reading the
 * message definition from the committed snapshot. See `fieldsFromSpec` for the filtering rules.
 * @param {string} api Apache message name without `Request`/`Response`, e.g. `ApiVersions`
 * @param {'request'|'response'} direction
 * @param {number} version the concrete API version to resolve fields for
 * @returns {{name: string, type: string, versions: string, nullable?: true, tag?: number}[]}
 */
const expectedFields = (api, direction, version) =>
  fieldsFromSpec(readSpec(specFile(api, direction)), version)

/**
 * @param {{name: string, type: string, versions: string, nullable?: true, tag?: number}} row
 * @returns {string} a canonical key so two rows can be compared for equality with `===`
 */
const canonicalKey = (row) =>
  `${row.name}|${row.type}|${row.versions}|${row.nullable === true}|${
    row.tag === undefined || row.tag === null ? '' : row.tag
  }`

/**
 * Compares a codec's declared `schema.fields` against the expected field list, positionally
 * and strictly: an extra row, a missing row, or a row differing in `name`, `type`, `versions`,
 * `nullable` or `tag` at the same index is one `S1` failure naming the codec file, the index,
 * and both sides.
 * @param {{file: string, schema: {fields: object[]}}} codec
 * @param {{name: string, type: string, versions: string, nullable?: true, tag?: number}[]} expected
 * @returns {string[]} `S1 …` failure messages, empty when the lists match
 */
const compare = (codec, expected) => {
  const actual = codec.schema.fields
  const failures = []
  const length = Math.max(actual.length, expected.length)

  for (let index = 0; index < length; index++) {
    const actualRow = actual[index]
    const expectedRow = expected[index]

    if (actualRow === undefined) {
      failures.push(
        `S1 ${codec.file} field ${index}: missing row, expected ${JSON.stringify(expectedRow)}`
      )
    } else if (expectedRow === undefined) {
      failures.push(`S1 ${codec.file} field ${index}: extra row ${JSON.stringify(actualRow)}`)
    } else if (canonicalKey(actualRow) !== canonicalKey(expectedRow)) {
      failures.push(
        `S1 ${codec.file} field ${index}: expected ${JSON.stringify(
          expectedRow
        )}, got ${JSON.stringify(actualRow)}`
      )
    }
  }

  return failures
}

/**
 * Validates one discovered codec against the message-definition snapshot: that its declared
 * `api`/`version` exist and agree with `direction`, that its field list matches (`compare`),
 * and that its implied flexibility (compact encodings, tagged-field sections) matches the
 * message JSON's `flexibleVersions` as seen through `isFlexible`.
 * @param {{file: string, direction: 'request'|'response', schema: object}} codec a codec whose `schema` is not null
 * @param {string[]} failures accumulator
 */
const checkCodec = (codec, failures) => {
  const { schema } = codec
  const direction = schema.direction === 'request' ? 'request' : 'response'
  const file = specFile(schema.api, direction)

  if (!fs.existsSync(file)) {
    failures.push(
      `S3 ${codec.file} declares api '${schema.api}' but ${toRepoRelative(file)} does not exist`
    )
    return
  }

  const spec = readSpec(file)

  if (!rangeContains(spec.validVersions, schema.version)) {
    failures.push(
      `S3 ${codec.file} v${schema.version} is outside ${schema.api}'s validVersions ` +
        `${spec.validVersions.min}-${spec.validVersions.max}`
    )
    return
  }

  if (schema.direction !== codec.direction) {
    failures.push(
      `S3 ${codec.file} declares direction '${schema.direction}' but the file is ${codec.direction}.js`
    )
    return
  }

  failures.push(...compare(codec, expectedFields(schema.api, schema.direction, schema.version)))

  const specFlexible = spec.flexibleFrom !== null && schema.version >= spec.flexibleFrom
  const codecFlexible = isFlexible(spec.apiKey, schema.version)
  if (specFlexible !== codecFlexible) {
    failures.push(
      `S1 ${codec.file} flexibility mismatch: ${schema.api}${
        direction === 'request' ? 'Request' : 'Response'
      } v${schema.version} flexible=${specFlexible} per the message JSON, ` +
        `but isFlexible(${spec.apiKey}, ${schema.version})=${codecFlexible}`
    )
  }
}

/**
 * Discovers every codec (`discover`) and validates each one that declares a `schema`
 * (`checkCodec`); codecs without one are collected and reported as a single unvalidated-count
 * line so coverage can be tracked as it grows API by API. A codec whose `require` failed is
 * reported once, as `S3`, and counted as unvalidated.
 * @param {string[]} failures accumulator, appended to in place
 * @param {string[]} reports accumulator, appended to in place
 */
const checkSchemas = (failures, reports) => {
  const codecs = discover()
  const unvalidated = []

  for (const codec of codecs) {
    if (codec.requireError) {
      failures.push(`S3 ${codec.file} could not be required: ${codec.requireError.message}`)
      unvalidated.push(codec.file)
      continue
    }

    if (codec.schema == null) {
      unvalidated.push(codec.file)
      continue
    }

    checkCodec(codec, failures)
  }

  reports.push(
    `S4 ${unvalidated.length} of ${codecs.length} codecs are unvalidated: ${unvalidated
      .sort()
      .join(', ')}`
  )
}

module.exports = { discover, expectedFields, compare, checkSchemas, fieldsFromSpec }
