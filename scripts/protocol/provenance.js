'use strict'

/**
 * Where each API's evidence came from, recorded in `scripts/protocol/provenance.json` and checked
 * against the files it describes: the Apache release the message definitions were taken at, the
 * SHA-256 of each message definition, and the `kafka-clients` jar the Java vectors were written by.
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
const GENERATORS = ['run.sh', 'runAssignors.sh']
const JAR_PIN = /KAFKA_CLIENTS_VERSION="\$\{KAFKA_CLIENTS_VERSION:-([0-9][0-9.]*)\}"/

/**
 * @param {string} file
 * @returns {string} the SHA-256 of the file's bytes, in hex
 */
const sha256 = (file) => crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex')

/**
 * The jar version the two Java generators download by default.
 * @param {string} [root] the repository root
 * @returns {string} the version
 * @throws {Error} when a generator pins none, or the two pin different ones
 */
const pinnedJar = (root = REPO_ROOT) => {
  const pins = GENERATORS.map((name) => {
    const found = JAR_PIN.exec(
      fs.readFileSync(path.join(root, 'scripts', 'protocol', 'javaVectors', name), 'utf8')
    )
    if (!found) throw new Error(`${name} pins no kafka-clients version`)
    return found[1]
  })
  if (new Set(pins).size !== 1) {
    throw new Error(`the Java generators pin different kafka-clients versions: ${pins.join(', ')}`)
  }
  return pins[0]
}

/**
 * @param {string} [root] the repository root
 * @returns {Record<string, { kafkaTag: string, messageSha256: { Request: string, Response: string },
 *   javaClientsJar: string|null }>} one entry per API of the request table whose message
 *   definition the snapshot holds, implemented or not, by Apache message name
 */
const compute = (root = REPO_ROOT) => {
  const tag = kafkaTag()
  const jar = pinnedJar(root)
  const withVectors = new Set(
    discoverFixtures(root)
      .filter(({ set }) => set === 'java' || set === 'javaBuffer')
      .map(({ directory }) => directory)
  )
  const messages = path.join(root, 'scripts', 'compat', 'messages', tag)

  return Object.fromEntries(
    Object.keys(requests)
      .map(messageOfKey)
      .filter((message) => fs.existsSync(path.join(messages, `${message}Request.json`)))
      .sort()
      .map((message) => [
        message,
        {
          kafkaTag: tag,
          messageSha256: {
            Request: sha256(path.join(messages, `${message}Request.json`)),
            Response: sha256(path.join(messages, `${message}Response.json`)),
          },
          javaClientsJar: withVectors.has(directoryOfMessage(message)) ? jar : null,
        },
      ])
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
 */
const writeProvenance = (file = OUTPUT) => {
  fs.writeFileSync(file, render(compute()))
  return OUTPUT_RELATIVE
}

module.exports = { compute, render, pinnedJar, checkProvenance, writeProvenance, OUTPUT_RELATIVE }
