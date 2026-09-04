#!/usr/bin/env node
'use strict'

/**
 * Protocol codec tooling.
 *
 *   npm run protocol:generate   rewrite src/protocol/flexibleVersions.js from the snapshot
 *   npm run protocol:check      regenerate in memory, diff against the committed file,
 *                               report and exit 1 on any failure (--json for machine output)
 *   node scripts/protocol/index.js scaffold <ApiName> <version>
 *                               emit a hand-finishable codec skeleton for one API version
 *
 * The snapshot is the one scripts/compat already commits, pinned by scripts/compat/config.json.
 */

const fs = require('fs')
const { kafkaTag, collect, render, OUTPUT, OUTPUT_RELATIVE } = require('./table')

/** @returns {number} process exit code */
const generate = () => {
  const tag = kafkaTag()
  fs.writeFileSync(OUTPUT, render(tag, collect(tag)))
  console.log(`wrote ${OUTPUT_RELATIVE}`)
  return 0
}

/**
 * @param {string[]} failures accumulator
 */
const checkTable = (failures) => {
  const tag = kafkaTag()
  const expected = render(tag, collect(tag))
  const actual = fs.existsSync(OUTPUT) ? fs.readFileSync(OUTPUT, 'utf8') : null
  if (actual !== expected) {
    failures.push(`S2 ${OUTPUT_RELATIVE} is out of date — run \`npm run protocol:generate\``)
  }
}

/**
 * @param {{ json: boolean }} options
 * @returns {number} process exit code
 */
const check = (options) => {
  const failures = []
  const reports = []
  checkTable(failures)
  return finish(failures, reports, options)
}

const finish = (failures, reports, options) => {
  if (options.json) {
    console.log(JSON.stringify({ ok: failures.length === 0, failures, reports }, null, 2))
  } else {
    for (const report of reports) console.log(`  report  ${report}`)
    for (const failure of failures) console.error(`  FAIL    ${failure}`)
    console.log(
      failures.length === 0
        ? `protocol:check passed (${reports.length} reports)`
        : `protocol:check failed (${failures.length} failures, ${reports.length} reports)`
    )
  }
  return failures.length === 0 ? 0 : 1
}

const main = () => {
  const args = process.argv.slice(2)
  const command = args[0]
  const options = { json: args.indexOf('--json') !== -1 }
  if (command === 'generate') return generate()
  if (command === 'check') return check(options)
  if (command === 'scaffold') {
    const [, apiName, version] = args
    if (apiName === undefined || version === undefined) {
      console.error('usage: node scripts/protocol/index.js scaffold <ApiName> <version>')
      return 1
    }
    for (const file of require('./scaffold').write(apiName, Number(version))) {
      console.log(`wrote ${file}`)
    }
    return 0
  }
  console.error(
    'usage: node scripts/protocol/index.js generate | check [--json] | scaffold <ApiName> <version>'
  )
  return 1
}

if (require.main === module) {
  process.exit(main())
}

module.exports = { generate, check }
