const fs = require('fs')
const path = require('path')

/** Where live scenarios leave what they measured: under the repository root, ignored by git and npm. */
const EVIDENCE_DIR = path.join(__dirname, '..', '.tmp', 'e2e')

/** A file stem, never a path. */
const EVIDENCE_NAME = /^[a-z0-9]+(?:-[a-z0-9]+)*$/

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
 * Appends one record to a scenario's evidence file as a JSON line stamped with the time it was
 * written, creating `.tmp/e2e/` on first use. Synchronous, so a record written just before an
 * assertion fails is on disk when jest reports it: a passing suite's console output is not kept,
 * which is why live scenarios write here and never to the console.
 * @param {string} name the scenario's evidence name
 * @param {object} record what was measured; its own keys win over the `at` stamp
 * @returns {void}
 * @throws {Error} on an invalid name, see `evidencePath`
 */
const writeEvidence = (name, record) => {
  const file = evidencePath(name)
  fs.mkdirSync(EVIDENCE_DIR, { recursive: true })
  fs.appendFileSync(file, `${JSON.stringify({ at: new Date().toISOString(), ...record })}\n`)
}

module.exports = { evidencePath, writeEvidence }
