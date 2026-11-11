'use strict'

/**
 * Projects independent Kafka broker tool output into the capture representation read by
 * `testHelpers/brokerCapture.js`.
 *
 * Three raw outputs of one cluster state are the oracle: `kafka-broker-api-versions.sh`,
 * `kafka-features.sh describe`, and the one JSON line printed by `wireFeatures.js`. Nothing is read
 * from the client or its codecs, and nothing is defaulted: a field kept in the capture was observed
 * by one of the sources, and the sources are cross-checked where they overlap.
 */
const crypto = require('crypto')

/** The raw files of one capture, by role. */
const RAW_FILES = Object.freeze({
  apiVersions: 'broker-api-versions.txt',
  features: 'kafka-features-describe.txt',
  wire: 'wire-apiversions-v4.jsonl',
})

const MAX_INT16 = 32767
/** The envelope fields every capture needs, in the order the capture file lists them. */
const REQUIRED_ENVELOPE = [
  'correlation',
  'errorCode',
  'apiKeyCount',
  'throttle',
  'shareApiRanges',
  'supportedFeatures',
  'finalizedFeaturesEpoch',
  'finalizedFeatures',
]
/** Envelope fields kept only when the wire line carried them. */
const OPTIONAL_ENVELOPE = ['zkMigrationReady', 'tags']

const BLOCK_HEADER = /^(\S+) \(id: -?\d+ .*\) -> \($/
const API_LINE =
  /^\t?[A-Za-z0-9]+\((\d+)\): (?:UNSUPPORTED|(\d+)(?: to (\d+))?(?: \[(?:usable: \d+|unusable: node too (?:new|old))\])?),?$/
const FEATURE_LABELS = [
  'Feature',
  'SupportedMinVersion',
  'SupportedMaxVersion',
  'FinalizedVersionLevel',
  'Epoch',
]
const TEXT = (value) => typeof value === 'string' && /\S/.test(value)
const PROVENANCE_FIELDS = [
  ['source', TEXT],
  ['brokerVersion', (value) => /^\d+\.\d+\.\d+$/.test(value)],
  ['brokerCommitId', (value) => /^[0-9a-f]{7,40}$/.test(value)],
  ['imageDigest', (value) => /^[\w][\w./-]*@sha256:[0-9a-f]{64}$/.test(value)],
  ['candidateSha', (value) => /^[0-9a-f]{40}$/.test(value)],
  ['featureState', TEXT],
]

/** @param {unknown} value @returns {boolean} whether it is a plain object */
const isObject = (value) => value !== null && typeof value === 'object' && !Array.isArray(value)

/** @param {string|Buffer} data @returns {string} the SHA-256 digest, hex */
const sha256 = (data) => crypto.createHash('sha256').update(data).digest('hex')

/**
 * Parses `kafka-broker-api-versions.sh` output: one block per broker, each API as
 * `Name(key): min to max [usable: n]`, a single number when the endpoints are equal, or
 * `UNSUPPORTED`. Only supported keys are kept, by numeric key.
 * @param {string} text the raw output
 * @returns {{ node: string, ranges: Record<string, number[]> }[]} the blocks, in order
 * @throws {Error} on a broker `ERROR:` line, a malformed or duplicate API line, an endpoint out of
 *   int16 range or a minimum above its maximum, an unterminated block, or no block at all
 */
const parseBrokerApiVersions = (text) => {
  const blocks = []
  let current = null
  for (const [index, raw] of text.split('\n').entries()) {
    const line = raw.replace(/\r$/, '')
    if (current === null) {
      if (line.trim() === '') continue
      if (line.includes(' -> ERROR: ')) throw new Error(`broker answered ERROR: ${line}`)
      const header = BLOCK_HEADER.exec(line)
      if (!header)
        throw new Error(`malformed broker header at line ${index + 1}: ${JSON.stringify(line)}`)
      current = { node: header[1], ranges: {} }
    } else if (line === ')') {
      blocks.push(current)
      current = null
    } else {
      const match = API_LINE.exec(line)
      if (!match) {
        throw new Error(
          `malformed API line ${index + 1} of ${current.node}: ${JSON.stringify(line)}`
        )
      }
      if (match[2] === undefined) continue
      const [key, min, max] = [match[1], match[2], match[3] ?? match[2]].map(Number)
      if (min > MAX_INT16 || max > MAX_INT16 || key > MAX_INT16) {
        throw new Error(`API ${key} of ${current.node} has an endpoint out of range`)
      }
      if (min > max)
        throw new Error(`API ${key} of ${current.node} has a minimum above its maximum`)
      if (Object.hasOwn(current.ranges, key)) {
        throw new Error(`API ${key} is listed twice in ${current.node}`)
      }
      current.ranges[key] = [min, max]
    }
  }
  if (current) throw new Error(`unterminated broker block for ${current.node}`)
  if (blocks.length === 0) throw new Error('no broker block in the API versions output')
  return blocks
}

/**
 * @param {{ node: string, ranges: Record<string, number[]> }[]} blocks parsed broker blocks
 * @returns {Record<string, number[]>} the ranges every block states
 * @throws {Error} when a later block differs from the first
 */
const agreedRanges = (blocks) => {
  const [first, ...rest] = blocks
  const expected = JSON.stringify(first.ranges)
  for (const block of rest) {
    if (JSON.stringify(block.ranges) !== expected) {
      throw new Error(`broker blocks disagree: ${block.node} differs from ${first.node}`)
    }
  }
  return first.ranges
}

/**
 * Parses `kafka-features.sh describe` rows:
 * `Feature: <name>  SupportedMinVersion: <n>  SupportedMaxVersion: <n>  FinalizedVersionLevel: <n>  Epoch: <n>`.
 * Values stay strings: `metadata.version` prints symbolic levels.
 * @param {string} text the raw output
 * @returns {{ name: string, min: string, max: string, finalized: string, epoch: string }[]}
 * @throws {Error} on a malformed row, rows printing different epochs, or no rows
 */
const parseFeaturesDescribe = (text) => {
  const rows = []
  for (const raw of text.split('\n')) {
    const line = raw.trim()
    if (line === '') continue
    const parts = line.split(/\s{2,}/)
    if (
      parts.length !== 5 ||
      !parts.every((part, i) => part.startsWith(`${FEATURE_LABELS[i]}: `))
    ) {
      throw new Error(`malformed feature row: ${JSON.stringify(line)}`)
    }
    const [name, min, max, finalized, epoch] = parts.map((part, i) =>
      part.slice(FEATURE_LABELS[i].length + 2)
    )
    rows.push({ name, min, max, finalized, epoch })
  }
  if (rows.length === 0) throw new Error('no feature rows in the feature tool output')
  if (new Set(rows.map(({ epoch }) => epoch)).size !== 1) {
    throw new Error('the feature tool printed different epochs')
  }
  return rows
}

/**
 * @param {string} text the raw wire capture
 * @returns {object} its one JSON line
 * @throws {Error} unless the text holds exactly one non-empty line that is a JSON object
 */
const parseWireLine = (text) => {
  const lines = text.split('\n').filter((line) => line.trim() !== '')
  if (lines.length !== 1) {
    throw new Error(`the wire capture must hold exactly one JSON line, found ${lines.length}`)
  }
  const value = JSON.parse(lines[0])
  if (!isObject(value)) throw new Error('the wire capture line is not a JSON object')
  return value
}

/**
 * Checks the tool outputs against the wire line where they overlap: the API ranges, the supported
 * ranges of numeric features and the finalized level of every feature the tool lists. Features the
 * tool does not list are not checked; `metadata.version` prints symbolic levels and is compared by
 * name only.
 * @param {Record<string, number[]>} apiRanges the agreed ranges of the tool blocks
 * @param {{ name: string, min: string, max: string, finalized: string, epoch: string }[]} rows
 * @param {object} wire the wire line
 * @returns {void}
 * @throws {Error} naming the first disagreement
 */
const crossCheck = (apiRanges, rows, wire) => {
  if (!isObject(wire.apiRanges))
    throw new Error('the wire line carries no apiRanges to cross-check')
  const wireRanges = wire.apiRanges
  for (const key of new Set([...Object.keys(apiRanges), ...Object.keys(wireRanges)])) {
    if (JSON.stringify(apiRanges[key]) !== JSON.stringify(wireRanges[key])) {
      throw new Error(
        `API ${key} range disagree: tool ${JSON.stringify(apiRanges[key])}, wire ${JSON.stringify(wireRanges[key])}`
      )
    }
  }

  const supported = new Map(
    (wire.supportedFeatures ?? []).map((feature) => [feature.name, feature])
  )
  const finalized = new Map(
    (wire.finalizedFeatures ?? []).map((feature) => [feature.name, feature])
  )
  for (const row of rows) {
    const feature = supported.get(row.name)
    if (!feature)
      throw new Error(`feature ${row.name} is listed by the tool but not supported on the wire`)
    for (const [label, tool, onWire] of [
      ['min', row.min, feature.min],
      ['max', row.max, feature.max],
    ]) {
      if (/^\d+$/.test(tool) && Number(tool) !== onWire) {
        throw new Error(
          `feature ${row.name} supported ${label} ${tool} (tool) disagree with ${onWire} (wire)`
        )
      }
    }
    const level = finalized.get(row.name)?.maxVersionLevel ?? 0
    if (/^\d+$/.test(row.finalized) && Number(row.finalized) !== level) {
      throw new Error(
        `feature ${row.name} finalized level ${row.finalized} (tool) disagree with wire ${level}`
      )
    }
  }
}

/**
 * Checks a provenance record and the digests of the raw outputs it names.
 * @param {object} provenance the record
 * @param {Record<string, string>} raws the raw text of each output, by file name
 * @returns {object} the record in canonical field order
 * @throws {Error} on a missing or malformed field, a missing capture entry, or a digest that does
 *   not match its raw text
 */
const verifyProvenance = (provenance, raws) => {
  if (!isObject(provenance)) throw new Error('missing provenance')
  for (const [field, valid] of PROVENANCE_FIELDS) {
    if (!valid(provenance[field])) throw new Error(`provenance ${field} is missing or malformed`)
  }

  const entries = Object.values(RAW_FILES).map((file) => {
    const entry = Array.isArray(provenance.captures)
      ? provenance.captures.find((capture) => capture?.file === file)
      : undefined
    if (!entry || provenance.captures.length !== 3) {
      throw new Error('provenance must name all three raw outputs')
    }
    if (!/^[0-9a-f]{64}$/.test(entry.sha256)) {
      throw new Error(`provenance digest of ${file} is malformed`)
    }
    if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/.test(entry.capturedOn)) {
      throw new Error(`provenance capture time of ${file} is not a UTC timestamp`)
    }
    if (!TEXT(entry.howProduced)) {
      throw new Error(`provenance does not say how ${file} was produced`)
    }
    if (sha256(raws[file]) !== entry.sha256) throw new Error(`raw digest mismatch for ${file}`)
    return {
      file,
      sha256: entry.sha256,
      capturedOn: entry.capturedOn,
      howProduced: entry.howProduced,
    }
  })

  return {
    ...Object.fromEntries(PROVENANCE_FIELDS.map(([field]) => [field, provenance[field]])),
    captures: entries,
  }
}

/**
 * Projects the three raw outputs of one cluster state into a capture.
 * @param {object} input
 * @param {string} input.apiVersionsText raw `kafka-broker-api-versions.sh` output
 * @param {string} input.wireFeaturesLine raw one-line output of `wireFeatures.js`
 * @param {string} input.featuresText raw `kafka-features.sh describe` output
 * @param {object} input.provenance source, broker version, commit and image digest, candidate SHA,
 *   feature state and, per raw output, its file name, SHA-256, UTC capture time and method
 * @returns {{ provenance: object, apiRanges: Record<string, number[]>, features: object }} the
 *   capture `readBrokerCapture` reads: numeric API ranges from the tool and the feature envelope
 *   from the wire line; the finalized-features epoch falls back to the tool's when the wire line
 *   lacks it
 * @throws {Error} when provenance or a digest is wrong, the sources disagree where they overlap, a
 *   required envelope field is missing from every source, or `apiKeyCount` differs from the ranges
 */
const projectBrokerTools = ({ apiVersionsText, wireFeaturesLine, featuresText, provenance }) => {
  const checked = verifyProvenance(provenance, {
    [RAW_FILES.apiVersions]: apiVersionsText,
    [RAW_FILES.features]: featuresText,
    [RAW_FILES.wire]: wireFeaturesLine,
  })
  const apiRanges = agreedRanges(parseBrokerApiVersions(apiVersionsText))
  const rows = parseFeaturesDescribe(featuresText)
  const wire = parseWireLine(wireFeaturesLine)

  const numeric = (value) => (typeof value === 'string' && /^\d+$/.test(value) ? value : undefined)
  const toolEpoch = numeric(rows[0].epoch)
  const wireEpoch = numeric(wire.finalizedFeaturesEpoch)
  if (toolEpoch && wireEpoch && toolEpoch !== wireEpoch) {
    throw new Error(`finalized-features epoch disagree: tool ${toolEpoch}, wire ${wireEpoch}`)
  }

  const envelope = { ...wire, finalizedFeaturesEpoch: wireEpoch ?? toolEpoch }
  for (const field of REQUIRED_ENVELOPE) {
    if (envelope[field] === undefined) {
      throw new Error(`required feature envelope field ${field} is missing from every source`)
    }
  }
  crossCheck(apiRanges, rows, wire)
  if (envelope.apiKeyCount !== Object.keys(apiRanges).length) {
    throw new Error(
      `apiKeyCount ${envelope.apiKeyCount} disagree with ${Object.keys(apiRanges).length} projected API keys`
    )
  }

  const features = {}
  for (const field of [...REQUIRED_ENVELOPE, ...OPTIONAL_ENVELOPE]) {
    if (envelope[field] !== undefined) features[field] = envelope[field]
  }

  return { provenance: checked, apiRanges, features }
}

module.exports = {
  RAW_FILES,
  projectBrokerTools,
  parseBrokerApiVersions,
  parseFeaturesDescribe,
  sha256,
}
