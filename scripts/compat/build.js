'use strict'

/**
 * Assembles the single model both outputs are rendered from (spec B1, B2).
 * Pure with respect to time and environment: the only inputs are the committed JSON
 * files, the committed message snapshot, and the fork's own request definitions.
 */

const fs = require('fs')
const path = require('path')
const parse = require('./parse')
const { classify } = require('./gaps')
const { readFork } = require('./forkVersions')
const { readSnapshot, compareErrorTables } = require('./errors')

const ROOT = __dirname
const REPO_ROOT = path.join(__dirname, '..', '..')

const readJson = name => JSON.parse(fs.readFileSync(path.join(ROOT, name), 'utf8'))

/** Plain byte-wise string comparator: deterministic across locales/environments, unlike localeCompare. */
const asciiCompare = (a, b) => (a < b ? -1 : a > b ? 1 : 0)

const buildModel = () => {
  const config = readJson('config.json')
  const trackedApis = readJson('tracked-apis.json')
  const notes = readJson('notes.json')
  const waiverFile = readJson('waivers.json')
  const tag = config.kafkaTag
  const messagesDir = path.join(ROOT, 'messages', tag)

  const fork = readFork(trackedApis)

  const apis = trackedApis.tracked.map(entry => {
    const spec = parse.readSpec(path.join(messagesDir, `${entry.kafkaName}Request.json`))
    const forkInfo = fork.apis[entry.kafkaName]
    const note = notes.apis[entry.kafkaName]
    const gap = classify({
      validVersions: spec.validVersions,
      forkVersions: forkInfo.forkVersions,
      hasCodec: forkInfo.hasCodec,
      stub: forkInfo.stub,
      planned: entry.planned,
    })
    return {
      kafkaName: entry.kafkaName,
      apiKey: entry.apiKey,
      forkName: entry.forkName,
      apiKeysName: entry.apiKeysName,
      planned: entry.planned,
      validVersions: [spec.validVersions.min, spec.validVersions.max],
      flexibleFrom: spec.flexibleFrom,
      deprecatedVersions: spec.deprecatedVersions,
      latestVersionUnstable: spec.latestVersionUnstable,
      forkVersions: forkInfo.forkVersions,
      gap: gap.gap,
      onFloor: gap.onFloor,
      belowFloor: gap.belowFloor,
      notes: note === undefined ? '' : note.note,
    }
  })
  apis.sort((a, b) => a.apiKey - b.apiKey)

  const byName = new Map(apis.map(api => [api.kafkaName, api]))
  const waivers = waiverFile.waivers
    .map(waiver => {
      const api = byName.get(waiver.api)
      return {
        api: waiver.api,
        versions: waiver.versions.slice().sort((a, b) => a - b),
        kafkaMin: api === undefined ? null : api.validVersions[0],
        reason: waiver.reason,
        until: waiver.until,
      }
    })
    .sort((a, b) => asciiCompare(a.api, b.api))

  const forkErrorCodes = require(path.join(REPO_ROOT, 'src', 'protocol', 'error')).errorCodes
  const errors = compareErrorTables({
    snapshot: readSnapshot(tag, ROOT),
    forkErrorCodes,
    aliases: notes.errorAliases,
  })

  return {
    kafkaTag: tag,
    schemaVersion: config.schemaVersion,
    tool: 'scripts/compat',
    apis,
    untracked: trackedApis.untracked.slice().sort((a, b) => a.apiKey - b.apiKey),
    waivers,
    crossCutting: notes.crossCutting,
    errors,
    findings: fork.findings,
  }
}

/** The committed machine-readable artifact (B2): the model minus the transient findings. */
const toJsonArtifact = model => ({
  generatedFrom: { kafkaTag: model.kafkaTag, tool: model.tool, schemaVersion: model.schemaVersion },
  apis: model.apis,
  waivers: model.waivers,
  crossCutting: model.crossCutting,
  errors: model.errors,
  untracked: model.untracked,
})

const serializeJson = artifact => `${JSON.stringify(artifact, null, 2)}\n`

module.exports = {
  ROOT,
  REPO_ROOT,
  readJson,
  asciiCompare,
  buildModel,
  toJsonArtifact,
  serializeJson,
}
