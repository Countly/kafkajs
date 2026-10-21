'use strict'

/**
 * The census of the wire codecs. Every `(api, version, direction)` file under
 * `src/protocol/requests/` is classified on two independent axes:
 *
 * - wire-negotiation reachability, from what `selectVersion` returns against the offered ranges of
 *   every supported broker line (`brokerRanges.json`): `reachable`; `capped-only`, selected only
 *   under one of the internal ceilings of `CAPS`; `below-floor`, under the minimum every line
 *   offers; or `dead`, never selected;
 * - implementation-import reachability, from a static scan of the `require` calls of the codec
 *   files: whether a reachable or capped-only codec imports the file, directly or through others.
 *
 * Nothing is deleted unless it is below-floor and import-unreachable (`deletable`). Every reachable
 * or capped-only pair has a `schema` export and a named golden assertion, or an individually
 * approved row of `schemaWaivers.json`; a pair with neither is a failure, never a note.
 *
 * Zero dependencies beyond the repository's own request tables, CommonJS, Node >= 22.
 */

const fs = require('fs')
const path = require('path')
const { requests, selectVersion } = require('../../src/protocol/requests')
const apiKeys = require('../../src/protocol/requests/apiKeys')
const { KafkaJSServerDoesNotSupportApiKey } = require('../../src/errors')
const { messageOfDirectory, keyOfMessage } = require('./apiNames')

const REPO_ROOT = path.join(__dirname, '..', '..')
const REQUESTS = 'src/protocol/requests'
const BROKER = 'src/broker/index.js'
const WAIVER_KINDS = ['exclusion', 'alias']
const APPROVED_BY = /^\d{4}-\d{2}-\d{2}, \S.*$/
// a review is named by its date and role: no path, no phase, package, ruling or review label
const NOT_PORTABLE =
  /[/\\]|Phase [0-9]|Task [0-9]|\b9[A-F]\b|\bW[0-9]{2}[ab]?\b|\bD9[0-9]\b|\bR[0-9]{1,2}\b|R3-|\bRS-[A-Z][0-9]|Amendment|§|\.md\b/

/**
 * The internal ceilings a request is sent under. A version is `capped-only` when only one of these
 * selects it. `token` is the text of the line that holds the ceiling, so its `file:line` is found
 * where it is today instead of being written down and left to drift.
 * @type {{ api: string, max: number, file: string, token: string, reason: string }[]}
 */
const CAPS = [
  {
    api: 'Produce',
    max: 11,
    file: BROKER,
    token: 'const PRODUCE_TOPIC_NAMES_MAX_VERSION = 11',
    reason: 'a produce request that carries no topic ids is keyed by name',
  },
  {
    api: 'Produce',
    max: 11,
    file: BROKER,
    token: '[apiKeys.Produce]: 11,',
    reason: 'a transactional produce under transaction version 1 stays at v11',
  },
  {
    api: 'Produce',
    max: 12,
    file: BROKER,
    token: 'const TRANSACTION_V2_PRODUCE_VERSION = 12',
    reason: 'a transactional produce under transaction version 2 is sent at exactly v12',
  },
  {
    api: 'Fetch',
    max: 12,
    file: BROKER,
    token: 'const FETCH_TOPIC_NAMES_MAX_VERSION = 12',
    reason: 'a fetch request that supplies no topic ids is keyed by name',
  },
  {
    api: 'OffsetCommit',
    max: 9,
    file: BROKER,
    token: 'const OFFSETS_TOPIC_NAMES_MAX_VERSION = 9',
    reason: 'an offset commit that carries no topic ids is keyed by name',
  },
  {
    api: 'OffsetFetch',
    max: 9,
    file: BROKER,
    token: 'const OFFSETS_TOPIC_NAMES_MAX_VERSION = 9',
    reason: 'an offset fetch that carries no topic ids is keyed by name',
  },
  {
    api: 'SyncGroup',
    max: 4,
    file: BROKER,
    token: 'const SYNC_GROUP_NO_PROTOCOL_MAX_VERSION = 4',
    reason: 'from v5 a coordinator refuses a request that names only one of protocol type and name',
  },
  {
    api: 'EndTxn',
    max: 4,
    file: BROKER,
    token: '[apiKeys.EndTxn]: 4,',
    reason: 'a transaction under transaction version 1 stops before the second part of KIP-890',
  },
  {
    api: 'TxnOffsetCommit',
    max: 4,
    file: BROKER,
    token: '[apiKeys.TxnOffsetCommit]: 4,',
    reason: 'a transaction under transaction version 1 stops before the second part of KIP-890',
  },
]

/**
 * @param {string} root
 * @param {string} file a repository-relative path
 * @param {string} token text of one line
 * @returns {number|null} the 1-based number of the first line of `file` that contains `token`
 */
const lineOf = (root, file, token) => {
  const lines = fs.readFileSync(path.join(root, file), 'utf8').split('\n')
  const index = lines.findIndex((line) => line.includes(token))
  return index === -1 ? null : index + 1
}

/**
 * @param {string} source
 * @returns {string[]} the relative module paths the source requires with a string literal
 */
const requiresOf = (source) =>
  [...source.matchAll(/require\(\s*['"](\.{1,2}\/[^'"]+)['"]\s*\)/g)].map((match) => match[1])

/** The census of the codecs of one request table. */
class CodecInventory {
  /**
   * @param {object} options
   * @param {string} [options.root] the repository root
   * @param {Record<string, { versions: string[] }>} [options.definitions] the request table, by
   *   `apiKeys.js` name
   * @param {Record<string, Record<string, [number, number]>>} options.ranges the offered range of
   *   every API on every supported broker line
   * @param {typeof CAPS} [options.caps]
   * @param {{ file: string, dir: string, direction: string, schema: object|null,
   *   module: object|null }[]} options.codecs from `schema.discover`
   * @param {object[]} [options.waivers] the parsed `schemaWaivers.json`
   * @param {import('./dispositions').FixtureDispositions|null} [options.dispositions]
   */
  constructor({
    root = REPO_ROOT,
    definitions = requests,
    ranges,
    caps = CAPS,
    codecs,
    waivers = [],
    dispositions = null,
  }) {
    this.root = root
    this.definitions = definitions
    this.ranges = ranges
    this.caps = caps
    this.waivers = waivers
    this.dispositions = dispositions
    this.wire = new Map()
    this.rows = codecs.map((codec) => this.rowOf(codec))
    this.imports = this.scanImports()
    this.markImportReachable()
  }

  /**
   * What `selectVersion` makes of every supported line, per `apiKeys.js` name: the versions it
   * returns with no ceiling, the ones it returns under a ceiling, and the lowest minimum offered.
   * @param {string} key an `apiKeys.js` name
   * @returns {{ reachable: Set<number>, capped: Set<number>, minimums: number[] }}
   */
  wireOf(key) {
    if (this.wire.has(key)) return this.wire.get(key)

    const found = { reachable: new Set(), capped: new Set(), minimums: [] }
    const definition = this.definitions[key]
    const apiKey = apiKeys[key]
    for (const line of Object.values(this.ranges)) {
      const offered = line[key]
      if (!offered || !definition) continue

      found.minimums.push(offered[0])
      const table = { [apiKey]: { apiKey, minVersion: offered[0], maxVersion: offered[1] } }
      const select = (maxVersions) => {
        try {
          return selectVersion(table, apiKey, definition, maxVersions)
        } catch (error) {
          if (error instanceof KafkaJSServerDoesNotSupportApiKey) return null
          throw error
        }
      }

      const plain = select({})
      if (plain !== null) found.reachable.add(plain)
      for (const cap of this.caps.filter(({ api }) => api === key)) {
        const capped = select({ [apiKey]: cap.max })
        if (capped !== null) found.capped.add(capped)
      }
    }

    this.wire.set(key, found)
    return found
  }

  /**
   * @param {{ file: string, dir: string, direction: string, schema: object|null }} codec
   * @returns {object} one row of the census
   */
  rowOf(codec) {
    const parts = codec.dir.split('/')
    const directory = parts[parts.length - 2]
    const version = Number(parts[parts.length - 1].slice(1))
    const api = messageOfDirectory(directory)
    const key = keyOfMessage(api)
    const { reachable, capped, minimums } = this.wireOf(key)
    const wire = reachable.has(version)
      ? 'reachable'
      : capped.has(version)
        ? 'capped-only'
        : minimums.length > 0 && minimums.every((minimum) => version < minimum)
          ? 'below-floor'
          : 'dead'

    return {
      api,
      directory,
      version,
      direction: codec.direction,
      file: codec.file,
      wire,
      importReachable: false,
      hasSchema: codec.schema != null,
      codec,
    }
  }

  /**
   * Every non-spec source file under a `vN` directory, and the files each one requires.
   * @returns {Map<string, { required: string[], dangling: string[] }>} by repository-relative path
   */
  scanImports() {
    const units = new Map()
    const requestsRoot = path.join(this.root, REQUESTS)
    const walk = (directory) => {
      for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
        const full = path.join(directory, entry.name)
        if (entry.isDirectory()) {
          if (entry.name !== '__tests__' && entry.name !== 'fixtures') walk(full)
        } else if (entry.name.endsWith('.js') && !entry.name.endsWith('.spec.js')) {
          const rel = path.relative(this.root, full).split(path.sep).join('/')
          if (/\/v\d+\//.test(rel)) units.set(rel, this.requiredBy(full))
        }
      }
    }
    walk(requestsRoot)
    return units
  }

  /**
   * @param {string} file absolute path of a source file
   * @returns {{ required: string[], dangling: string[] }} the files under `requests/` it requires,
   *   and the relative requires that resolve to no file
   */
  requiredBy(file) {
    const required = []
    const dangling = []
    for (const request of requiresOf(fs.readFileSync(file, 'utf8'))) {
      const base = path.resolve(path.dirname(file), request)
      const target = [base, `${base}.js`, path.join(base, 'index.js')].find(
        (candidate) => fs.existsSync(candidate) && fs.statSync(candidate).isFile()
      )
      if (target === undefined) dangling.push(request)
      else {
        const rel = path.relative(this.root, target).split(path.sep).join('/')
        if (rel.startsWith(`${REQUESTS}/`)) required.push(rel)
      }
    }
    return { required, dangling }
  }

  /**
   * Sets `importReachable` on every row a reachable or capped-only codec requires, directly or
   * through other files.
   * @returns {void}
   */
  markImportReachable() {
    const seen = new Set()
    const queue = this.rows
      .filter(({ wire }) => wire === 'reachable' || wire === 'capped-only')
      .map(({ file }) => file)
    while (queue.length > 0) {
      const file = queue.pop()
      for (const next of (this.imports.get(file) || { required: [] }).required) {
        if (!seen.has(next)) {
          seen.add(next)
          queue.push(next)
        }
      }
    }
    for (const row of this.rows) row.importReachable = seen.has(row.file)
  }

  /** @returns {object[]} the rows a reachable or capped-only wire pair stands on */
  get selected() {
    return this.rows.filter(({ wire }) => wire === 'reachable' || wire === 'capped-only')
  }

  /** @returns {object[]} the rows that may be deleted: below-floor and imported by no retained codec */
  get deletable() {
    return this.rows.filter(
      ({ wire, importReachable }) => wire === 'below-floor' && !importReachable
    )
  }

  /**
   * @param {object} row
   * @returns {object|undefined} the waiver of the row, if it has one
   */
  waiverOf(row) {
    return this.waivers.find(
      (waiver) =>
        waiver.api === row.api &&
        waiver.version === row.version &&
        waiver.direction === row.direction
    )
  }

  /**
   * @param {object} row
   * @returns {{ family: string, spec: string, test: string, fixtures: object[] }[]} the named
   *   golden assertions under the row
   */
  goldensOf(row) {
    return this.dispositions
      ? this.dispositions.goldensFor(row.directory, row.version, row.direction)
      : []
  }

  /** @returns {object[]} selected rows with no `schema` export and no waiver */
  get schemaGaps() {
    return this.selected.filter((row) => !row.hasSchema && !this.waiverOf(row))
  }

  /** @returns {object[]} selected rows with no golden assertion and no waiver */
  get goldenGaps() {
    return this.selected.filter((row) => this.goldensOf(row).length === 0 && !this.waiverOf(row))
  }

  /**
   * Checks every waiver: complete, approved in a portable form, naming a selected pair that still
   * needs it, and for an alias the same function object with definitions that differ in nothing the
   * importing codec does not itself decode.
   * @param {(alias: object, row: object) => string|null} aliasProblem
   * @param {string[]} failures appended to in place
   * @returns {void}
   */
  checkWaivers(aliasProblem, failures) {
    const seen = new Set()
    for (const waiver of this.waivers) {
      const label = `schemaWaivers.json ${waiver.api} v${waiver.version} ${waiver.direction}`
      for (const field of ['api', 'direction', 'kind', 'reason', 'approvedBy', 'evidence']) {
        if (typeof waiver[field] !== 'string' || waiver[field].trim() === '') {
          failures.push(`I3 ${label} has no ${field}`)
        }
      }
      if (!Number.isInteger(waiver.version)) failures.push(`I3 ${label} has no version`)
      if (!WAIVER_KINDS.includes(waiver.kind))
        failures.push(`I3 ${label} kind '${waiver.kind}' is not exclusion or alias`)
      if (
        typeof waiver.approvedBy === 'string' &&
        (!APPROVED_BY.test(waiver.approvedBy) || NOT_PORTABLE.test(waiver.approvedBy))
      ) {
        failures.push(
          `I3 ${label} approvedBy must be a review date and role ("2026-10-19, release reviewer"), with no path or label`
        )
      }
      if (seen.has(label)) failures.push(`I3 ${label} appears twice`)
      seen.add(label)

      const row = this.selected.find(
        (candidate) =>
          candidate.api === waiver.api &&
          candidate.version === waiver.version &&
          candidate.direction === waiver.direction
      )
      if (row === undefined) {
        failures.push(`I3 ${label} is not a reachable or capped-only pair`)
      } else if (row.hasSchema && this.goldensOf(row).length > 0) {
        failures.push(`I3 ${label} is not needed: the pair has a schema and a golden`)
      } else if (waiver.kind === 'alias') {
        const problem = aliasProblem(waiver, row)
        if (problem) failures.push(`I3 ${label}: ${problem}`)
      }
    }
  }

  /**
   * Checks the census and appends its figures.
   * @param {object} options
   * @param {(alias: object, row: object) => string|null} options.aliasProblem
   * @param {string[]} failures appended to in place
   * @param {string[]} reports appended to in place
   * @returns {void}
   */
  check({ aliasProblem }, failures, reports) {
    for (const row of this.schemaGaps) {
      failures.push(
        `I1 ${row.file} (${row.api} v${row.version} ${row.direction}, ${row.wire}) has no schema export and no waiver`
      )
    }
    for (const row of this.goldenGaps) {
      failures.push(
        `I2 ${row.file} (${row.api} v${row.version} ${row.direction}, ${row.wire}) has no named golden assertion and no waiver`
      )
    }
    this.checkWaivers(aliasProblem, failures)

    for (const [file, { dangling }] of this.imports) {
      for (const request of dangling)
        failures.push(`I4 ${file} requires ${request}, which does not exist`)
    }

    for (const cap of this.caps) {
      if (lineOf(this.root, cap.file, cap.token) === null) {
        failures.push(
          `I5 the ${cap.api} ceiling v${cap.max} is not at '${cap.token}' in ${cap.file}`
        )
      }
    }

    reports.push(this.summary())
    reports.push(
      `I7 ceilings: ${this.caps
        .map(
          (cap) => `${cap.api} v${cap.max} ${cap.file}:${lineOf(this.root, cap.file, cap.token)}`
        )
        .join(', ')}`
    )
    reports.push(
      `I8 waivers: ${this.waivers.length}${this.waivers.map((w) => ` ${w.api}/v${w.version}/${w.direction}`).join('')}; deletable: ${this.deletable.length}`
    )
  }

  /** @returns {string} the counts, as one report line */
  summary() {
    const count = (wire) => this.rows.filter((row) => row.wire === wire).length
    const imported = this.rows.filter(
      (row) => row.importReachable && row.wire !== 'reachable' && row.wire !== 'capped-only'
    )
    return (
      `I6 ${this.rows.length} codec files: ${count('reachable')} reachable, ${count('capped-only')} capped-only, ` +
      `${count('below-floor')} below-floor, ${count('dead')} dead; ${imported.length} not selected but imported by a selected codec; ` +
      `${this.schemaGaps.length} schema gaps, ${this.goldenGaps.length} golden gaps`
    )
  }
}

/**
 * Why an alias waiver is not valid, or null. An alias names the version it stands in for (`aliasOf`)
 * and holds only when both codecs export the same function object and the message definitions of
 * the two versions list the same fields, so the shared code reads both.
 * @param {CodecInventory} inventory
 * @returns {(waiver: object, row: object) => string|null}
 */
const aliasChecker = (inventory) => (waiver, row) => {
  const other = inventory.rows.find(
    (candidate) =>
      candidate.api === row.api &&
      candidate.version === waiver.aliasOf &&
      candidate.direction === row.direction
  )
  if (!Number.isInteger(waiver.aliasOf) || other === undefined) {
    return `aliasOf must name another version of the same API and direction`
  }

  const mine = row.codec.module
  const theirs = other.codec.module
  const shared =
    row.direction === 'response'
      ? mine.decode === theirs.decode && mine.parse === theirs.parse
      : mine === theirs
  if (!shared) return `v${row.version} and v${other.version} do not export the same function object`

  const { expectedFields } = require('./schema')
  const list = (version) =>
    expectedFields(row.api, row.direction, version).map(({ versions, ...field }) =>
      JSON.stringify(field)
    )
  const differing = list(row.version).filter((field) => !list(other.version).includes(field))
  const missing = list(other.version).filter((field) => !list(row.version).includes(field))
  return differing.length + missing.length === 0
    ? null
    : `the definitions of v${row.version} and v${other.version} differ in ${[...differing, ...missing].join(', ')}`
}

/**
 * Runs the census, the fixture records and the walk of every bound vector for the repository.
 * @param {string[]} failures appended to in place
 * @param {string[]} reports appended to in place
 * @param {string} [root] the repository root
 * @returns {CodecInventory} the census, for callers that want its rows
 */
const checkInventory = (failures, reports, root = REPO_ROOT) => {
  const { discover, checkEncoding } = require('./schema')
  const { discoverFixtures } = require('./fixtureFamilies')
  const { FixtureDispositions, validateCaptureRoutes } = require('./dispositions')
  const { CAPTURE_LIST } = require('./capture')
  const read = (name) => JSON.parse(fs.readFileSync(path.join(__dirname, name), 'utf8'))

  const fixtures = discoverFixtures(root)
  const dispositions = new FixtureDispositions({
    root,
    fixtures,
    entries: read('fixtureDispositions.json'),
  })
  const inventory = new CodecInventory({
    root,
    ranges: read('brokerRanges.json'),
    codecs: discover(),
    waivers: read('schemaWaivers.json'),
    dispositions,
  })

  inventory.check({ aliasProblem: aliasChecker(inventory) }, failures, reports)
  dispositions.validate(failures)
  validateCaptureRoutes(
    { entries: read('captureRoutes.json'), captureList: CAPTURE_LIST, fixtures },
    failures
  )

  for (const row of inventory.selected.filter(({ hasSchema }) => hasSchema)) {
    const vectors = inventory
      .goldensOf(row)
      .flatMap(({ fixtures: bound }) => bound)
      .map((fixture) => ({ name: fixture.file, bytes: fixture.bytes }))
    checkEncoding(row.codec, vectors, failures)
  }

  return inventory
}

module.exports = {
  CodecInventory,
  CAPS,
  lineOf,
  requiresOf,
  aliasChecker,
  checkInventory,
  REPO_ROOT,
}
