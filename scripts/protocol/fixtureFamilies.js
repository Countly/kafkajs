'use strict'

/**
 * The wire fixtures under `src/protocol/requests/<api>/fixtures/`, found on disk, bound to the
 * `(api, version, direction)` they hold bytes for, and grouped into families. A family is the
 * fixtures of one API that came from one place, which is what a disposition is written for.
 *
 * Zero dependencies, CommonJS, Node >= 22.
 */

const fs = require('fs')
const path = require('path')

const REQUESTS = path.join('src', 'protocol', 'requests')

/** The families, and what each one is evidence of. */
const EVIDENCE_OF_SET = {
  java: 'java-vector',
  javaBuffer: 'java-vector',
  capture: 'capture',
  hand: 'hand',
}

/**
 * @param {string} rel a fixture path inside its `fixtures` directory
 * @returns {'java'|'javaBuffer'|'capture'|'hand'} the family of the fixture: the structured Java
 *   vectors, the older Java vectors kept as bytes, a broker capture, or anything else
 */
const setOf = (rel) => {
  if (rel.startsWith(`java${path.sep}`) || rel.startsWith('java/')) return 'java'
  if (/_java\.json$/.test(rel)) return 'javaBuffer'
  if (/_kafka4\d\d\.json$/.test(rel)) return 'capture'
  return 'hand'
}

/**
 * @param {string} directory
 * @returns {string[]} every file under it, as paths relative to it, in name order
 */
const listFiles = (directory) =>
  fs
    .readdirSync(directory, { withFileTypes: true })
    .flatMap((entry) =>
      entry.isDirectory()
        ? listFiles(path.join(directory, entry.name)).map((rel) => path.join(entry.name, rel))
        : [entry.name]
    )
    .sort()

/**
 * Binds a fixture to the message it holds. A structured vector names its message and version; any
 * other fixture names its version and direction in its file name (`v4_response_java.json`).
 * @param {string} rel
 * @param {object} json the parsed fixture
 * @returns {{ version: number, direction: 'request'|'response' }|null} null when it binds to none
 */
const bindingOf = (rel, json) => {
  if (typeof json.message === 'string' && Number.isInteger(json.version)) {
    return {
      version: json.version,
      direction: /Request$/.test(json.message) ? 'request' : 'response',
    }
  }

  const named = /^v(\d+)_.*?(request|response)/i.exec(path.basename(rel))
  return named ? { version: Number(named[1]), direction: named[2].toLowerCase() } : null
}

/**
 * @param {object} json a parsed fixture
 * @returns {Buffer|null} the message body it holds: `hex` of a structured vector, `data` of a
 *   `{ type: 'Buffer' }` one; null for a fixture that holds neither
 */
const bytesOf = (json) => {
  if (typeof json.hex === 'string') return Buffer.from(json.hex, 'hex')
  if (json.type === 'Buffer' && Array.isArray(json.data)) return Buffer.from(json.data)
  return null
}

/**
 * Every fixture of every API under `root`, with its family and binding.
 * @param {string} root the repository root
 * @returns {{ directory: string, rel: string, file: string, family: string, set: string,
 *   evidenceKind: string, version: number|null, direction: string|null, bytes: Buffer|null }[]}
 */
const discoverFixtures = (root) => {
  const requests = path.join(root, REQUESTS)
  return fs
    .readdirSync(requests)
    .sort()
    .filter((directory) => fs.existsSync(path.join(requests, directory, 'fixtures')))
    .flatMap((directory) => {
      const fixtures = path.join(requests, directory, 'fixtures')
      return listFiles(fixtures)
        .filter((rel) => rel.endsWith('.json'))
        .map((rel) => {
          const json = JSON.parse(fs.readFileSync(path.join(fixtures, rel), 'utf8'))
          const set = setOf(rel)
          return {
            directory,
            rel: rel.split(path.sep).join('/'),
            file: path.join(REQUESTS, directory, 'fixtures', rel).split(path.sep).join('/'),
            family: `${directory}/${set}`,
            set,
            evidenceKind: EVIDENCE_OF_SET[set],
            ...(bindingOf(rel, json) || { version: null, direction: null }),
            bytes: bytesOf(json),
          }
        })
    })
}

module.exports = { EVIDENCE_OF_SET, setOf, bindingOf, bytesOf, discoverFixtures, listFiles }
