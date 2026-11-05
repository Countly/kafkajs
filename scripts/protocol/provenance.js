'use strict'

/**
 * Where each API's evidence came from, recorded in `scripts/protocol/provenance.json` and checked
 * against the files it describes: the Apache release the message definitions were taken at, the
 * SHA-256 of each message definition, and, for every Java vector, the `kafka-clients` jar that
 * wrote it. The jar of a vector is read from `scripts/protocol/javaVectorSources.json`, one record
 * per fixture, never from the version a generator script downloads by default, so moving a pin
 * cannot reattribute a vector that an older jar wrote.
 * `npm run protocol:generate` writes the file; `npm run protocol:check` fails when it differs from
 * what the repository holds.
 *
 * Zero dependencies, CommonJS, Node >= 22.
 */

const crypto = require('crypto')
const fs = require('fs')
const path = require('path')
const { requests } = require('../../src/protocol/requests')
const { kafkaTag } = require('./table')
const { messageOfKey, directoryOfMessage } = require('./apiNames')
const { discoverFixtures } = require('./fixtureFamilies')

const REPO_ROOT = path.join(__dirname, '..', '..')
const OUTPUT = path.join(__dirname, 'provenance.json')
const OUTPUT_RELATIVE = 'scripts/protocol/provenance.json'
const SOURCES_RELATIVE = 'scripts/protocol/javaVectorSources.json'
const SHA256_HEX = /^[0-9a-f]{64}$/
const VERSION = /^[0-9]+(\.[0-9]+)+$/

/**
 * The only jar version whose SHA-256 may stay null: the one the vectors older than the
 * `javaVectorSources.json` records were generated with, whose hash was never kept. Every other jar
 * must name its hash.
 */
const UNHASHED_JARS = new Set(['4.3.1'])

/**
 * @typedef {object} VectorSource
 * @property {string} javaClientsJar the `kafka-clients` version that wrote the fixture
 * @property {string|null} javaClientsJarSha256 the SHA-256 of that jar, null only for a jar in
 *   `UNHASHED_JARS`
 * @property {string} fixtureSha256 the SHA-256 of the fixture file's bytes
 */

/**
 * @param {string} file
 * @returns {string} the SHA-256 of the file's bytes, in hex
 */
const sha256 = (file) => crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex')

/**
 * @param {string} root the repository root
 * @returns {ReturnType<typeof discoverFixtures>} the Java vectors on disk
 */
const javaFixtures = (root) =>
  discoverFixtures(root).filter(({ set }) => set === 'java' || set === 'javaBuffer')

/**
 * Reads the source records and checks them against the Java vectors on disk.
 * @param {string} root the repository root
 * @returns {{ records: Record<string, VectorSource>, problems: string[] }} the records (empty when
 *   the file is missing or unreadable) and every way they disagree with the repository: a record
 *   missing for a vector, one for a file that is no vector, one written twice, one with a fixture
 *   hash the file no longer has, and one whose jar or hashes are malformed or inconsistent
 */
const readSources = (root) => {
  const file = path.join(root, SOURCES_RELATIVE)
  if (!fs.existsSync(file)) return { records: {}, problems: [`${SOURCES_RELATIVE} is missing`] }

  const text = fs.readFileSync(file, 'utf8')
  let records
  try {
    records = JSON.parse(text)
  } catch (error) {
    return { records: {}, problems: [`${SOURCES_RELATIVE} is not JSON: ${error.message}`] }
  }

  const problems = []
  const keys = [...text.matchAll(/^ {2}"([^"]+)": \{/gm)].map(([, key]) => key)
  for (const key of new Set(keys.filter((candidate, at) => keys.indexOf(candidate) !== at))) {
    problems.push(`duplicate source record for ${key}`)
  }

  const onDisk = new Set(javaFixtures(root).map(({ file: fixture }) => fixture))
  for (const fixture of [...onDisk].sort()) {
    if (!Object.hasOwn(records, fixture)) problems.push(`no source record for ${fixture}`)
  }

  const hashOfJar = new Map()
  for (const [fixture, record] of Object.entries(records)) {
    if (!onDisk.has(fixture)) {
      problems.push(`source record for ${fixture} names no Java vector`)
      continue
    }
    const { javaClientsJar: jar, javaClientsJarSha256: jarSha, fixtureSha256 } = record
    if (typeof jar !== 'string' || !VERSION.test(jar)) {
      problems.push(`source record for ${fixture} names no jar version`)
    } else if (jarSha === null && !UNHASHED_JARS.has(jar)) {
      problems.push(`source record for ${fixture} names no SHA-256 of its ${jar} jar`)
    }
    if (jarSha !== null && !SHA256_HEX.test(jarSha)) {
      problems.push(`source record for ${fixture} has a malformed jar SHA-256`)
    } else if (
      jarSha !== null &&
      hashOfJar.get(jar) !== undefined &&
      hashOfJar.get(jar) !== jarSha
    ) {
      problems.push(`source records name two SHA-256 values for the ${jar} jar`)
    } else if (jarSha !== null) {
      hashOfJar.set(jar, jarSha)
    }
    if (fixtureSha256 !== sha256(path.join(root, fixture))) {
      problems.push(`source record for ${fixture} holds a stale fixture SHA-256`)
    }
  }

  return { records, problems: [...new Set(problems)] }
}

/**
 * @param {string} [root] the repository root
 * @returns {Record<string, { kafkaTag: string, messageSha256: { Request: string, Response: string },
 *   javaClientsJar: string|null, javaVectors: ({ file: string } & VectorSource)[] }>} one entry per
 *   API of the request table whose message definition the snapshot holds, implemented or not, by
 *   Apache message name. `javaVectors` is the source record of each of the API's Java vectors, by
 *   file; `javaClientsJar` is their one jar version, or null when they have none or differ.
 * @throws {Error} when the source records disagree with the Java vectors on disk
 */
const compute = (root = REPO_ROOT) => {
  const tag = kafkaTag()
  const { records, problems } = readSources(root)
  if (problems.length > 0) throw new Error(problems.join('; '))

  const vectorsOf = new Map()
  for (const { directory, file } of javaFixtures(root)) {
    vectorsOf.set(directory, [...(vectorsOf.get(directory) || []), { file, ...records[file] }])
  }
  const messages = path.join(root, 'scripts', 'compat', 'messages', tag)

  return Object.fromEntries(
    Object.keys(requests)
      .map(messageOfKey)
      .filter((message) => fs.existsSync(path.join(messages, `${message}Request.json`)))
      .sort()
      .map((message) => {
        const javaVectors = (vectorsOf.get(directoryOfMessage(message)) || []).sort((a, b) =>
          a.file < b.file ? -1 : a.file > b.file ? 1 : 0
        )
        const jars = new Set(javaVectors.map(({ javaClientsJar }) => javaClientsJar))
        return [
          message,
          {
            kafkaTag: tag,
            messageSha256: {
              Request: sha256(path.join(messages, `${message}Request.json`)),
              Response: sha256(path.join(messages, `${message}Response.json`)),
            },
            javaClientsJar: jars.size === 1 ? [...jars][0] : null,
            javaVectors,
          },
        ]
      })
  )
}

/**
 * @param {ReturnType<typeof compute>} provenance
 * @returns {string} the text of `provenance.json`
 */
const render = (provenance) => `${JSON.stringify(provenance, null, 2)}\n`

/**
 * Compares the committed file with what the repository holds now.
 * @param {string[]} failures appended to in place
 * @param {string} [root] the repository root
 * @param {string} [file] the committed file
 * @returns {void}
 */
const checkProvenance = (failures, root = REPO_ROOT, file = OUTPUT) => {
  const { problems } = readSources(root)
  if (problems.length > 0) {
    for (const problem of problems) failures.push(`P3 ${problem}`)
    return
  }

  let expected
  try {
    expected = compute(root)
  } catch (error) {
    failures.push(`P1 provenance cannot be computed: ${error.message}`)
    return
  }

  const actual = fs.existsSync(file) ? fs.readFileSync(file, 'utf8') : null
  if (actual === null) {
    failures.push(`P2 ${OUTPUT_RELATIVE} is missing - run \`npm run protocol:generate\``)
    return
  }

  const committed = JSON.parse(actual)
  const apis = new Set([...Object.keys(committed), ...Object.keys(expected)])
  for (const api of [...apis].sort()) {
    if (JSON.stringify(committed[api]) !== JSON.stringify(expected[api])) {
      failures.push(`P2 ${OUTPUT_RELATIVE} differs for ${api} - run \`npm run protocol:generate\``)
    }
  }
  if (actual !== render(expected) && failures.every((failure) => !failure.startsWith('P2 '))) {
    failures.push(`P2 ${OUTPUT_RELATIVE} is not in the form \`npm run protocol:generate\` writes`)
  }
}

/**
 * Writes `provenance.json`.
 * @param {string} [file] where to write it; the committed file by default
 * @returns {string} the repository path of the committed file
 * @throws {Error} when the source records disagree with the Java vectors on disk
 */
const writeProvenance = (file = OUTPUT) => {
  fs.writeFileSync(file, render(compute()))
  return OUTPUT_RELATIVE
}

module.exports = {
  compute,
  render,
  readSources,
  checkProvenance,
  writeProvenance,
  OUTPUT_RELATIVE,
  SOURCES_RELATIVE,
}
