'use strict'

/**
 * Builds `src/protocol/flexibleVersions.js` from the committed Apache message-definition
 * snapshot (`scripts/compat/messages/<tag>/`). A message declares `flexibleVersions` — the
 * version range that uses KIP-482 compact encodings and tagged-field sections — and the
 * request and response of one API always agree on it. That single number per API key is all
 * the runtime needs to pick a header version.
 *
 * Zero dependencies, CommonJS, Node >= 22 — same conventions as scripts/compat/.
 */

const fs = require('fs')
const path = require('path')
const { readSpec } = require('../compat/parse')

const COMPAT_ROOT = path.join(__dirname, '..', 'compat')
const REPO_ROOT = path.join(__dirname, '..', '..')
const OUTPUT_RELATIVE = 'src/protocol/flexibleVersions.js'
const OUTPUT = path.join(REPO_ROOT, OUTPUT_RELATIVE)

/** @returns {string} the Kafka tag the committed snapshot was taken at */
const kafkaTag = () =>
  JSON.parse(fs.readFileSync(path.join(COMPAT_ROOT, 'config.json'), 'utf8')).kafkaTag

/**
 * @param {string} tag
 * @returns {{ apiKey: number, apiName: string, flexibleFrom: number|null }[]} sorted by apiKey
 */
const collect = (tag) => {
  const dir = path.join(COMPAT_ROOT, 'messages', tag)
  const rows = []
  for (const file of fs.readdirSync(dir).sort()) {
    if (!file.endsWith('Request.json')) continue
    const request = readSpec(path.join(dir, file))
    if (request.apiKey === null) continue
    const response = readSpec(path.join(dir, file.replace(/Request\.json$/, 'Response.json')))
    if (request.flexibleFrom !== response.flexibleFrom) {
      throw new Error(
        `${request.name}: request flexibleVersions ${request.flexibleFrom} does not match response ${response.flexibleFrom}`
      )
    }
    rows.push({
      apiKey: request.apiKey,
      apiName: request.name.replace(/Request$/, ''),
      flexibleFrom: request.flexibleFrom,
    })
  }
  return rows.sort((a, b) => a.apiKey - b.apiKey)
}

/**
 * @param {string} tag
 * @param {{ apiKey: number, apiName: string, flexibleFrom: number|null }[]} rows
 * @returns {string} the full text of src/protocol/flexibleVersions.js
 */
const render = (tag, rows) => {
  const entries = rows
    .map(
      (row) =>
        `  ${row.apiKey}: ${row.flexibleFrom === null ? 'null' : row.flexibleFrom}, // ${row.apiName}`
    )
    .join('\n')

  return `/**
 * The first flexible (KIP-482) version of each API, keyed by API key, or \`null\` when the API
 * has no flexible version at all. A flexible version uses compact strings/bytes/arrays and
 * ends every structure with a tagged-field section; it also selects request header v2 and
 * response header v1.
 *
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-482%3A+The+Kafka+Protocol+should+Support+Optional+Tagged+Fields
 * @see https://kafka.apache.org/43/design/protocol
 *
 * Generated from the Apache Kafka ${tag} message definitions
 * (clients/src/main/resources/common/message/<Name>Request.json).
 * Do not edit by hand — run \`npm run protocol:generate\`; \`npm run protocol:check\` fails on drift.
 *
 * @type {{ [apiKey: number]: number | null }}
 */
module.exports = {
${entries}
}
`
}

module.exports = { kafkaTag, collect, render, OUTPUT, OUTPUT_RELATIVE }
