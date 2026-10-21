'use strict'

/**
 * Checks the two committed records about the wire fixtures against the files on disk:
 *
 * - `fixtureDispositions.json`, one entry per fixture family: where its bytes came from, what kind
 *   of evidence they are, what is done with them, and the named golden assertions in the codec
 *   specs that bind them to the codecs they are evidence for;
 * - `captureRoutes.json`, one entry per message `scripts/protocol/capture.js` records from a live
 *   broker, and one per capture that has no route, with the reason.
 *
 * Zero dependencies, CommonJS, Node >= 22.
 */

const fs = require('fs')
const path = require('path')
const { EVIDENCE_OF_SET } = require('./fixtureFamilies')

const SPECS = path.join('src', 'protocol', 'requests')
const DISPOSITIONS = ['retain', 'replace', 'remove']

/**
 * The spec a golden names: its `spec`, or, for a golden of one version, the spec beside that
 * version's codec.
 * @param {{ family: string }} entry
 * @param {{ direction: string, versions: number[], spec?: string }} golden
 * @returns {string|undefined} a path under `src/protocol/requests/`, undefined when it cannot be known
 */
const specOf = ({ family }, { direction, versions, spec }) =>
  spec ||
  (versions.length === 1
    ? `${family.split('/')[0]}/v${versions[0]}/${direction}.spec.js`
    : undefined)
const DIRECTIONS = ['request', 'response']

/**
 * @param {*} value
 * @returns {boolean} whether it is a non-empty string
 */
const text = (value) => typeof value === 'string' && value.trim().length > 0

/** The fixture dispositions and what they bind. */
class FixtureDispositions {
  /**
   * @param {object} options
   * @param {string} options.root the repository root
   * @param {ReturnType<import('./fixtureFamilies').discoverFixtures>} options.fixtures every
   *   fixture on disk
   * @param {object[]} options.entries the parsed `fixtureDispositions.json`
   */
  constructor({ root, fixtures, entries }) {
    this.root = root
    this.fixtures = fixtures
    this.entries = entries
  }

  /**
   * The fixtures a golden entry stands on: those of its family bound to one of its versions in its
   * direction and holding bytes.
   * @param {object} entry the disposition
   * @param {object} golden one of its `goldens`
   * @param {number} [version] one version, instead of all of `golden.versions`
   * @returns {object[]} fixtures from `discoverFixtures`
   */
  fixturesOf(entry, golden, version) {
    const versions = version === undefined ? golden.versions : [version]
    return this.fixtures.filter(
      (fixture) =>
        fixture.family === entry.family &&
        fixture.direction === golden.direction &&
        versions.includes(fixture.version) &&
        fixture.bytes !== null
    )
  }

  /**
   * @param {string} directory the API's directory under `requests/`
   * @param {number} version
   * @param {'request'|'response'} direction
   * @returns {{ family: string, spec: string, test: string, fixtures: object[] }[]} the golden
   *   assertions that name this codec and have a fixture under them
   */
  goldensFor(directory, version, direction) {
    return this.entries
      .filter((entry) => entry.disposition === 'retain' && entry.family.startsWith(`${directory}/`))
      .flatMap((entry) =>
        (entry.goldens || [])
          .filter((golden) => golden.direction === direction && golden.versions.includes(version))
          .map((golden) => ({
            family: entry.family,
            spec: specOf(entry, golden),
            test: golden.test,
            fixtures: this.fixturesOf(entry, golden, version),
          }))
      )
      .filter((golden) => golden.fixtures.length > 0)
  }

  /**
   * @param {string} spec a path under `src/protocol/requests/`
   * @returns {string|null} the text of the spec, or null when it does not exist
   */
  specText(spec) {
    const file = path.join(this.root, SPECS, spec)
    return fs.existsSync(file) ? fs.readFileSync(file, 'utf8') : null
  }

  /**
   * Checks every entry and the whole against the disk.
   * @param {string[]} failures appended to in place
   * @returns {void}
   */
  validate(failures) {
    const seen = new Set()
    for (const entry of this.entries) {
      const where = `fixtureDispositions.json ${entry.family}`
      if (seen.has(entry.family)) failures.push(`F1 ${where} appears twice`)
      seen.add(entry.family)
      this.validateEntry(entry, where, failures)
    }

    const onDisk = new Set(this.fixtures.map(({ family }) => family))
    for (const family of onDisk) {
      if (!seen.has(family))
        failures.push(`F1 fixtureDispositions.json has no entry for the fixture family ${family}`)
    }
    for (const entry of this.entries) {
      if (!onDisk.has(entry.family) && !entry.removed) {
        failures.push(
          `F1 fixtureDispositions.json ${entry.family} names a family with no fixture on disk`
        )
      }
    }
  }

  /**
   * @param {object} entry
   * @param {string} where
   * @param {string[]} failures
   */
  validateEntry(entry, where, failures) {
    for (const key of ['family', 'route', 'evidenceKind', 'disposition', 'reason']) {
      if (!text(entry[key])) failures.push(`F2 ${where} has no ${key}`)
    }
    if (!DISPOSITIONS.includes(entry.disposition)) {
      failures.push(
        `F2 ${where} disposition '${entry.disposition}' is not one of ${DISPOSITIONS.join(', ')}`
      )
    }

    const set = String(entry.family).split('/')[1]
    const expected = EVIDENCE_OF_SET[set]
    if (set !== 'hand' && expected !== undefined && entry.evidenceKind !== expected) {
      failures.push(
        `F2 ${where} evidenceKind '${entry.evidenceKind}' is not '${expected}', which its files are`
      )
    }

    if (entry.disposition === 'remove') {
      if (!text(entry.removed))
        failures.push(`F2 ${where} is removed and names no file in 'removed'`)
      else if (fs.existsSync(path.join(this.root, entry.removed))) {
        failures.push(`F5 ${where} is dispositioned 'remove' but ${entry.removed} is still on disk`)
      }
    }

    for (const golden of entry.goldens || []) this.validateGolden(entry, golden, where, failures)
  }

  /**
   * @param {object} entry
   * @param {{ direction: string, versions: number[], spec?: string, test: string }} golden
   * @param {string} where
   * @param {string[]} failures
   */
  validateGolden(entry, golden, where, failures) {
    const label = `${where} golden ${golden.direction} v${(golden.versions || []).join(',')}`
    if (
      !DIRECTIONS.includes(golden.direction) ||
      !Array.isArray(golden.versions) ||
      golden.versions.length === 0
    ) {
      failures.push(`F3 ${label} needs a direction and a list of versions`)
      return
    }

    const spec = specOf(entry, golden)
    const source = spec ? this.specText(spec) : null
    if (!spec) failures.push(`F3 ${label} has several versions and names no spec`)
    else if (source === null) failures.push(`F3 ${label}: ${spec} does not exist`)
    else if (!text(golden.test) || !source.includes(golden.test)) {
      failures.push(`F3 ${label}: ${spec} has no test named '${golden.test}'`)
    } else if (!source.includes('fixtures/')) {
      failures.push(`F3 ${label}: ${spec} reads no fixture`)
    }

    for (const version of golden.versions) {
      if (this.fixturesOf(entry, golden, version).length === 0) {
        failures.push(`F3 ${label}: the family has no ${golden.direction} fixture for v${version}`)
      }
    }
  }
}

/**
 * Checks `captureRoutes.json` against the list `scripts/protocol/capture.js` records.
 * @param {object} options
 * @param {object[]} options.entries the parsed `captureRoutes.json`
 * @param {{ dir: string, apiVersion: number, directions: string[] }[]} options.captureList
 *   `CAPTURE_LIST` of `capture.js`
 * @param {ReturnType<import('./fixtureFamilies').discoverFixtures>} options.fixtures
 * @param {string[]} failures appended to in place
 * @returns {void}
 */
const validateCaptureRoutes = ({ entries, captureList, fixtures }, failures) => {
  const key = ({ api, version, direction }) => `${api} v${version} ${direction}`
  const routed = entries.filter(({ disposition }) => disposition === 'captured')
  const listed = captureList.flatMap(({ dir, apiVersion, directions }) =>
    directions.map((direction) => ({ api: dir, version: apiVersion, direction }))
  )

  for (const row of listed) {
    if (!routed.some((entry) => key(entry) === key(row))) {
      failures.push(`F4 captureRoutes.json has no entry for ${key(row)}, which capture.js records`)
    }
  }
  for (const entry of routed) {
    if (!listed.some((row) => key(row) === key(entry))) {
      failures.push(`F4 captureRoutes.json ${key(entry)} is not a row of capture.js`)
    }
    const held = fixtures.some(
      (fixture) =>
        fixture.directory === entry.api &&
        fixture.version === entry.version &&
        fixture.direction === entry.direction
    )
    if (!held) failures.push(`F4 captureRoutes.json ${key(entry)} has no fixture on disk`)
  }

  for (const entry of entries) {
    for (const field of ['api', 'route', 'evidenceKind', 'disposition', 'reason']) {
      if (!text(entry[field])) failures.push(`F4 captureRoutes.json ${key(entry)} has no ${field}`)
    }
    if (!['captured', 'no-route'].includes(entry.disposition)) {
      failures.push(
        `F4 captureRoutes.json ${key(entry)} disposition '${entry.disposition}' is not captured or no-route`
      )
    }
    if (entry.direction === 'request' && !text(entry.decodeSide)) {
      failures.push(
        `F4 captureRoutes.json ${key(entry)} is a request and names no decodeSide check`
      )
    }
  }
}

module.exports = { FixtureDispositions, validateCaptureRoutes }
