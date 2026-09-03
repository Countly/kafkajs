#!/usr/bin/env node
'use strict'

/**
 * Compatibility inventory CLI.
 *
 *   yarn compat:generate   write docs/KafkaCompatibility.md + docs/kafka-compatibility.json
 *   yarn compat:check      regenerate in memory, diff against the committed files, run the
 *                          drift checks, print a report, exit 1 on any failure (--json for
 *                          machine output)
 *   yarn compat:fetch      refresh the committed snapshot from the pinned tag (network)
 *
 * Nothing here reads or writes anything under src/ — this is inventory only.
 */

const fs = require('fs')
const path = require('path')
const { ROOT, REPO_ROOT, readJson, buildModel, toJsonArtifact, serializeJson } = require('./build')
const { render } = require('./render')
const { formatVersions } = require('./gaps')
const { messageFiles } = require('./fetch')

const outputPaths = () => {
  const config = readJson('config.json')
  return {
    markdown: path.join(REPO_ROOT, config.outputs.markdown),
    json: path.join(REPO_ROOT, config.outputs.json),
    markdownRelative: config.outputs.markdown,
    jsonRelative: config.outputs.json,
  }
}

const generate = () => {
  const model = buildModel()
  const paths = outputPaths()
  fs.writeFileSync(paths.markdown, render(model))
  fs.writeFileSync(paths.json, serializeJson(toJsonArtifact(model)))
  console.log(`wrote ${paths.markdownRelative}`)
  console.log(`wrote ${paths.jsonRelative}`)
  return 0
}

/** Snapshot completeness (D4), checked before buildModel so a missing file is a clear message. */
const checkSnapshot = (trackedApis, tag, failures) => {
  const dir = path.join(ROOT, 'messages', tag)
  for (const name of messageFiles(trackedApis.tracked)) {
    if (!fs.existsSync(path.join(dir, name))) {
      failures.push(`D4 missing snapshot file: scripts/compat/messages/${tag}/${name}`)
    }
  }
}

const checkBelowFloor = (model, failures) => {
  const waived = new Map(model.waivers.map((waiver) => [waiver.api, waiver]))
  for (const api of model.apis) {
    const waiver = waived.get(api.kafkaName)
    const allowed = waiver === undefined ? [] : waiver.versions
    const unwaived = api.belowFloor.filter((version) => allowed.indexOf(version) === -1)
    if (unwaived.length > 0) {
      failures.push(
        `D2 ${api.kafkaName} implements v${formatVersions(unwaived)} below the Kafka ${
          model.kafkaTag
        } floor v${api.validVersions[0]} with no waiver in scripts/compat/waivers.json`
      )
    }
    if (waiver !== undefined) {
      const stale = waiver.versions.filter((version) => api.belowFloor.indexOf(version) === -1)
      if (stale.length > 0) {
        failures.push(
          `D2 stale waiver: ${api.kafkaName} v${formatVersions(
            stale
          )} is no longer implemented below the floor — remove it from scripts/compat/waivers.json`
        )
      }
    }
  }
  const known = new Set(model.apis.map((api) => api.kafkaName))
  for (const waiver of model.waivers) {
    if (!known.has(waiver.api)) {
      failures.push(`D2 waiver for unknown API "${waiver.api}" in scripts/compat/waivers.json`)
    }
  }
}

const checkNotes = (model, notes, failures) => {
  const known = new Set(model.apis.map((api) => api.kafkaName))
  for (const name of Object.keys(notes.apis)) {
    if (!known.has(name))
      failures.push(`D4 orphan note for unknown API "${name}" in scripts/compat/notes.json`)
  }
}

const checkErrors = (model, failures, reports) => {
  const errors = model.errors
  for (const error of errors.unknownToKafka) {
    failures.push(
      `F1 src/protocol/error.js defines code ${error.code} (${error.type}), which Kafka ${model.kafkaTag} does not`
    )
  }
  for (const mismatch of errors.mismatches) {
    failures.push(
      `F1 code ${mismatch.code}: this client calls it ${mismatch.forkType}, Kafka ${model.kafkaTag} calls it ${mismatch.kafkaType} — fix it, or record it in notes.json errorAliases`
    )
  }
  for (const stale of errors.staleAliases) {
    failures.push(
      `F1 stale alias ${stale.forkType} -> ${stale.kafkaType} in notes.json: it no longer matches the snapshot`
    )
  }
  reports.push(
    `F1 ${errors.missingInFork.length} error codes defined by Kafka ${model.kafkaTag} are missing here`
  )
}

const check = (options) => {
  const failures = []
  const reports = []
  const config = readJson('config.json')
  const trackedApis = readJson('tracked-apis.json')
  checkSnapshot(trackedApis, config.kafkaTag, failures)
  if (failures.length > 0) return finish(null, failures, reports, options)

  const model = buildModel()
  const paths = outputPaths()

  const expectedMarkdown = render(model)
  const expectedJson = serializeJson(toJsonArtifact(model))
  const actualMarkdown = fs.existsSync(paths.markdown)
    ? fs.readFileSync(paths.markdown, 'utf8')
    : null
  const actualJson = fs.existsSync(paths.json) ? fs.readFileSync(paths.json, 'utf8') : null
  if (actualMarkdown !== expectedMarkdown) {
    failures.push(`D1 ${paths.markdownRelative} is out of date — run \`yarn compat:generate\``)
  }
  if (actualJson !== expectedJson) {
    failures.push(`D1 ${paths.jsonRelative} is out of date — run \`yarn compat:generate\``)
  }

  checkBelowFloor(model, failures)
  checkNotes(model, readJson('notes.json'), failures)
  checkErrors(model, failures, reports)

  for (const api of model.apis) {
    if (api.gap === 'unknown') {
      failures.push(
        `D4 ${api.kafkaName} has a codec directory but an empty versions array — lookup() can never select it`
      )
    }
    if (api.gap.indexOf('ahead:') === 0) {
      failures.push(
        `D4 ${api.kafkaName} implements v${formatVersions(api.forkVersions)}, above the Kafka ${
          model.kafkaTag
        } maximum v${api.validVersions[1]}`
      )
    }
  }

  for (const finding of model.findings) {
    if (finding.level === 'error') failures.push(`D4 ${finding.message}`)
    else reports.push(`D4 ${finding.message}`)
  }

  for (const api of model.apis.filter((a) => a.onFloor)) {
    reports.push(
      `D3 floor watch: ${api.kafkaName} v${Math.max.apply(
        null,
        api.forkVersions
      )} sits on the Kafka ${model.kafkaTag} minimum`
    )
  }

  return finish(model, failures, reports, options)
}

const finish = (model, failures, reports, options) => {
  if (options.json) {
    console.log(
      JSON.stringify(
        {
          ok: failures.length === 0,
          kafkaTag: model === null ? null : model.kafkaTag,
          failures,
          reports,
        },
        null,
        2
      )
    )
  } else {
    for (const report of reports) console.log(`  report  ${report}`)
    for (const failure of failures) console.error(`  FAIL    ${failure}`)
    console.log(
      failures.length === 0
        ? `compat:check passed (${reports.length} reports)`
        : `compat:check failed (${failures.length} failures, ${reports.length} reports)`
    )
  }
  return failures.length === 0 ? 0 : 1
}

const main = async () => {
  const args = process.argv.slice(2)
  const command = args[0]
  const options = { json: args.indexOf('--json') !== -1 }
  if (command === 'generate') return generate()
  if (command === 'check') return check(options)
  if (command === 'fetch') {
    await require('./fetch').run()
    return 0
  }
  console.error('usage: node scripts/compat generate | check [--json] | fetch')
  return 1
}

if (require.main === module) {
  main().then(
    (code) => process.exit(code),
    (error) => {
      console.error(error.stack)
      process.exit(1)
    }
  )
}

module.exports = { generate, check }
