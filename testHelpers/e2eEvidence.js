const fs = require('fs')
const path = require('path')
const { LANES, currentLane } = require('./lane')
const { readBrokerVersion } = require('./brokerVersion')

/** Where live scenarios leave what they measured: under the repository root, ignored by git and npm. */
const EVIDENCE_DIR = path.join(__dirname, '..', '.tmp', 'e2e')

/** A file stem, never a path. */
const EVIDENCE_NAME = /^[a-z0-9]+(?:-[a-z0-9]+)*$/

/**
 * The fields every evidence line carries, which `scripts/evidence/verify.js` requires: when it was
 * written, and what it was written against.
 * @type {readonly string[]}
 */
const STAMP_FIELDS = Object.freeze([
  'at',
  'brokerVersion',
  'wireVersions',
  'node',
  'config',
  'tree',
  'lane',
])

/** The tree stamp of a run nobody froze: it matches no commit, so no verifier accepts it. */
const UNFROZEN_TREE = 'unfrozen'

/** Keys whose values are credentials, whatever the client option is called. */
const SECRET_KEY =
  /pass|secret|token|credential|authoriz|private|^(ca|cert|key|pfx)$|apikey|accesskeyid/i

/**
 * What a line says about the run that wrote it.
 * @typedef {object} EvidenceRun
 * @property {string} brokerVersion the version the running broker reports, `major.minor.patch`
 * @property {string} node `process.version`
 * @property {string} tree what was tested: the commit hash a frozen run sets in `KAFKAJS_TREE`
 * @property {'fake'|'docker'} lane
 * @property {Record<string, number>} [wireVersions] the version negotiated for each API the
 *   scenario used; a line may carry its own instead
 * @property {object} [config] the client options of the scenario, credentials left out
 */

/** @type {EvidenceRun|null} what a test installed with `useEvidenceRun` */
let installed = null
/** @type {EvidenceRun|null} what a live scenario read from its environment, once */
let discovered = null

/**
 * The evidence file a live scenario writes to.
 * @param {string} name the scenario's evidence name, e.g. `idempotent-pipelining`
 * @returns {string} the absolute path of `.tmp/e2e/<name>.jsonl`
 * @throws {Error} when `name` is not lower-case kebab-case, so it can never leave the directory
 */
const evidencePath = (name) => {
  if (typeof name !== 'string' || !EVIDENCE_NAME.test(name)) {
    throw new Error(`Evidence name must be lower-case kebab-case, got ${JSON.stringify(name)}`)
  }

  return path.join(EVIDENCE_DIR, `${name}.jsonl`)
}

/**
 * A copy of `value` that is safe to write to evidence: values under a credential-like key are
 * replaced by `[redacted]`, functions, binary data and cycles by a marker, bigints by strings.
 * @param {*} value
 * @param {WeakSet<object>} [ancestors] the objects being copied, to catch a cycle
 * @returns {*}
 */
const sanitiseConfig = (value, ancestors = new WeakSet()) => {
  if (typeof value === 'function') return '[function]'
  if (typeof value === 'bigint') return String(value)
  if (value === null || typeof value !== 'object') return value
  if (ArrayBuffer.isView(value) || value instanceof ArrayBuffer) return '[binary]'
  if (ancestors.has(value)) return '[circular]'

  ancestors.add(value)
  const copy = Array.isArray(value)
    ? value.map((item) => sanitiseConfig(item, ancestors))
    : Object.fromEntries(
        Object.entries(value).map(([key, item]) => [
          key,
          SECRET_KEY.test(key) ? '[redacted]' : sanitiseConfig(item, ancestors),
        ])
      )
  ancestors.delete(value)

  return copy
}

/**
 * @param {*} value
 * @returns {boolean} whether `value` is a non-null, non-array object
 */
const isPlainObject = (value) =>
  value !== null && typeof value === 'object' && !Array.isArray(value)

/**
 * @param {EvidenceRun} context
 * @returns {EvidenceRun} the context, if it is complete
 * @throws {Error} naming the first field that is missing or malformed
 */
const completeRun = (context) => {
  const checks = {
    brokerVersion: (value) => typeof value === 'string' && /^\d+\.\d+\.\d+$/.test(value),
    node: (value) => typeof value === 'string' && /^v\d+\.\d+\.\d+/.test(value),
    tree: (value) => typeof value === 'string' && value.length > 0,
    lane: (value) => LANES.includes(value),
    wireVersions: (value) => value === undefined || isPlainObject(value),
    config: (value) => value === undefined || isPlainObject(value),
  }

  for (const [field, valid] of Object.entries(checks)) {
    if (!valid(context?.[field])) {
      throw new Error(
        `Evidence run context has no valid ${field}: ${JSON.stringify(context?.[field])}`
      )
    }
  }

  return { ...context }
}

/**
 * Sets the identity every later evidence line is stamped with, in place of the one a live scenario
 * reads from its environment. A unit test calls it with literal values, so it needs no Docker, no
 * Git and no environment; call it with no argument to go back to the environment.
 * @param {EvidenceRun} [context]
 * @returns {void}
 * @throws {Error} when the context is incomplete: a line is never written with a guessed stamp
 */
const useEvidenceRun = (context) => {
  installed = context === undefined ? null : completeRun(context)
}

/**
 * The run a live scenario belongs to, read once per process: the broker's own version (the lane's
 * `KAFKA_VERSION` is only a check on it), this Node, `KAFKAJS_TREE` and the lane.
 * @returns {EvidenceRun}
 * @throws {Error} when the broker cannot be asked, or is not the version the lane asked for
 */
const discoverRun = () => {
  if (!discovered) {
    const brokerVersion = readBrokerVersion()
    const asked = process.env.KAFKA_VERSION
    if (asked && asked !== brokerVersion) {
      throw new Error(
        `The broker reports ${brokerVersion} but KAFKA_VERSION=${asked}: this run is evidence of neither`
      )
    }

    discovered = completeRun({
      brokerVersion,
      node: process.version,
      tree: process.env.KAFKAJS_TREE || UNFROZEN_TREE,
      lane: currentLane(),
    })
  }

  return discovered
}

/**
 * Appends one record to a scenario's evidence file as a JSON line stamped with the time it was
 * written and with the run it belongs to (`brokerVersion`, `node`, `tree`, `lane`, and the
 * sanitised `config` and `wireVersions`), creating `.tmp/e2e/` on first use. Synchronous, so a
 * record written just before an assertion fails is on disk when jest reports it: a passing suite's
 * console output is not kept, which is why live scenarios write here and never to the console.
 *
 * The record's own keys win over `at` and may carry `wireVersions` and `config`; the identity
 * stamps always come from the run, so a record cannot claim another tree or broker. A `toJSON` of
 * the record is dropped and never called: it could rewrite the stamped line, credentials
 * included, as it is serialised.
 * @param {string} name the scenario's evidence name
 * @param {object} record what was measured
 * @returns {void}
 * @throws {Error} on an invalid name (see `evidencePath`), or when the run's identity cannot be
 *   established (see `useEvidenceRun`, `discoverRun`); nothing is written then
 */
const writeEvidence = (name, record) => {
  const file = evidencePath(name)
  const run = installed || discoverRun()
  const { toJSON: _dropped, ...fields } = record
  const line = {
    at: new Date().toISOString(),
    ...fields,
    wireVersions: fields.wireVersions ?? run.wireVersions ?? {},
    config: sanitiseConfig(fields.config ?? run.config ?? {}),
    brokerVersion: run.brokerVersion,
    node: run.node,
    tree: run.tree,
    lane: run.lane,
  }

  fs.mkdirSync(EVIDENCE_DIR, { recursive: true })
  fs.appendFileSync(file, `${JSON.stringify(line)}\n`)
}

/**
 * The version each API was sent at, from the `REQUEST` events a client emitted: the value for a
 * line's `wireVersions`.
 * @param {Array<{ apiName: string, apiVersion: number }>} requests the event payloads
 * @returns {Record<string, number>} the highest version seen for each API name
 */
const wireVersionsOf = (requests) => {
  const versions = {}
  for (const { apiName, apiVersion } of requests) {
    versions[apiName] = Math.max(versions[apiName] ?? apiVersion, apiVersion)
  }

  return versions
}

/**
 * Every line of a scenario's evidence file, oldest first.
 * @param {string} name the scenario's evidence name
 * @returns {object[]} the parsed lines; none when the file does not exist
 * @throws {Error} on an invalid name, or a line that is not JSON
 */
const evidenceLines = (name) => {
  const file = evidencePath(name)

  return fs.existsSync(file)
    ? fs
        .readFileSync(file, 'utf8')
        .split('\n')
        .filter(Boolean)
        .map((line) => JSON.parse(line))
    : []
}

module.exports = {
  STAMP_FIELDS,
  evidencePath,
  evidenceLines,
  writeEvidence,
  useEvidenceRun,
  wireVersionsOf,
  sanitiseConfig,
}
