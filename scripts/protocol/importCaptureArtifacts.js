#!/usr/bin/env node
'use strict'

/**
 * Turns the artifact of one broker capture job into `scripts/protocol/captures/<version>/`.
 *
 * The artifact holds the three raw outputs (`broker-api-versions.txt`, `kafka-features-describe.txt`,
 * `wire-apiversions-v4.jsonl`), the broker's startup identity lines (`broker-identity.txt`), the
 * image digest (`image-digest.txt`), `capture-meta.json`, and the `SHA256SUMS` the job wrote. The
 * import checks every digest, reads the broker version and commit off the identity lines, projects
 * the capture with `captureBrokerTools.js`, writes `apiVersions.json` and copies the three raw
 * files byte for byte. Nothing is written until the artifact has been verified and projected.
 *
 * With `--tables <dir>` it also merges the client tables the job recorded (`brokerRanges.json` and
 * `brokerFeatures.json`) into `scripts/protocol/`, but only when they equal the independent capture.
 *
 * With `--dry-run` it makes every check against scratch copies and writes nothing to the repository.
 *
 * Usage: node scripts/protocol/importCaptureArtifacts.js --artifact <dir> --version <x.y.z> [--tables <dir>] [--dry-run]
 */
const fs = require('fs')
const os = require('os')
const path = require('path')
const { RAW_FILES, projectBrokerTools, sha256 } = require('./captureBrokerTools')
const { readBrokerCapture } = require('../../testHelpers/brokerCapture')

const CAPTURES_DIR = path.join(__dirname, 'captures')
const RANGES_FILE = path.join(__dirname, 'brokerRanges.json')
const FEATURES_FILE = path.join(__dirname, 'brokerFeatures.json')
const IDENTITY_FILE = 'broker-identity.txt'
const IMAGE_FILE = 'image-digest.txt'
const META_FILE = 'capture-meta.json'
const SUMS_FILE = 'SHA256SUMS'
const SIGNED_FILES = [...Object.values(RAW_FILES), IDENTITY_FILE, IMAGE_FILE, META_FILE]
const USAGE =
  'Usage: node scripts/protocol/importCaptureArtifacts.js --artifact <dir> --version <x.y.z> [--tables <dir>] [--dry-run]'

/**
 * Checks the artifact's files against the digests its job recorded.
 * @param {string} artifactDir
 * @returns {Map<string, string>} the recorded SHA-256 of each file
 * @throws {Error} on a malformed line, an unlisted required file or a digest that differs
 */
const verifyArtifact = (artifactDir) => {
  const sums = new Map()
  for (const line of fs.readFileSync(path.join(artifactDir, SUMS_FILE), 'utf8').split('\n')) {
    if (line === '') continue
    const match = /^([0-9a-f]{64}) {2}(\S+)$/.exec(line)
    if (!match) throw new Error(`malformed ${SUMS_FILE} line: ${JSON.stringify(line)}`)
    sums.set(match[2], match[1])
  }
  for (const file of SIGNED_FILES) {
    if (!sums.has(file)) throw new Error(`${SUMS_FILE} does not list ${file}`)
    if (sha256(fs.readFileSync(path.join(artifactDir, file))) !== sums.get(file)) {
      throw new Error(`artifact digest mismatch for ${file}`)
    }
  }
  return sums
}

/**
 * Reads the broker version and commit id off its startup log lines.
 * @param {string} text the `Kafka version:` and `Kafka commitId:` lines of the broker log
 * @returns {{ brokerVersion: string, brokerCommitId: string }}
 * @throws {Error} unless the text states exactly one version and exactly one commit id
 */
const readIdentity = (text) => {
  const only = (pattern, what) => {
    const found = new Set([...text.matchAll(pattern)].map((match) => match[1]))
    if (found.size !== 1) {
      throw new Error(`the broker identity log must state exactly one ${what}, found ${found.size}`)
    }
    return [...found][0]
  }

  return {
    brokerVersion: only(/Kafka version: (\d+\.\d+\.\d+)(?=\s|$)/g, 'version'),
    brokerCommitId: only(/Kafka commitId: ([0-9a-f]{7,40})(?=\s|$)/g, 'commit id'),
  }
}

/**
 * @param {object} value
 * @returns {string} JSON with two-space indent and number arrays on one line, as the committed
 *   captures are written
 */
const toJson = (value) =>
  `${JSON.stringify(value, null, 2).replace(
    /\[\n\s*(-?\d+(?:,\n\s*-?\d+)*)\n\s*\]/g,
    (_, list) => `[${list.replace(/,\n\s*/g, ', ')}]`
  )}\n`

/**
 * Verifies and projects one capture artifact and writes `<outDir>/<version>/`.
 * @param {object} options
 * @param {string} options.artifactDir the downloaded artifact directory
 * @param {string} options.version the broker version the job was meant to capture
 * @param {string} [options.outDir] where capture directories live; the committed captures by default
 * @returns {{ capture: object, target: string }} the projected capture and the directory written
 * @throws {Error} when the version is malformed or its capture exists, the artifact fails its
 *   digests, the broker reports another version, or the projection rejects the evidence
 */
const importCapture = ({ artifactDir, version, outDir = CAPTURES_DIR }) => {
  if (!/^\d+\.\d+\.\d+$/.test(version)) throw new Error('the version must be major.minor.patch')
  const target = path.join(outDir, version)
  if (fs.existsSync(target)) throw new Error(`${target} already exists`)

  const sums = verifyArtifact(artifactDir)
  const read = (file) => fs.readFileSync(path.join(artifactDir, file), 'utf8')
  const meta = JSON.parse(read(META_FILE))
  const identity = readIdentity(read(IDENTITY_FILE))
  if (identity.brokerVersion !== version) {
    throw new Error(`the broker reports ${identity.brokerVersion}, not ${version}`)
  }

  const capture = projectBrokerTools({
    apiVersionsText: read(RAW_FILES.apiVersions),
    featuresText: read(RAW_FILES.features),
    wireFeaturesLine: read(RAW_FILES.wire),
    provenance: {
      source: meta.source,
      ...identity,
      imageDigest: read(IMAGE_FILE).trim(),
      candidateSha: meta.candidateSha,
      featureState: meta.featureState,
      captures: (meta.captures ?? []).map((entry) => ({ ...entry, sha256: sums.get(entry.file) })),
    },
  })

  fs.mkdirSync(target, { recursive: true })
  fs.writeFileSync(path.join(target, 'apiVersions.json'), toJson(capture))
  for (const file of Object.values(RAW_FILES)) {
    fs.copyFileSync(path.join(artifactDir, file), path.join(target, file))
  }
  return { capture, target }
}

/**
 * Merges the client tables a capture job recorded into the committed ones, when they equal what
 * the independent capture shows: every API range, and the finalized `share.version` (0 when the
 * feature is supported but not finalized, no key when it is not supported).
 * @param {object} options
 * @param {string} options.tablesDir holds the job's `brokerRanges.json` and `brokerFeatures.json`
 * @param {string} options.version
 * @param {{ apiRanges: object, features: object }} options.capture the projected capture
 * @param {string} [options.rangesFile] the committed ranges table
 * @param {string} [options.featuresFile] the committed features table
 * @returns {void}
 * @throws {Error} when the job's tables lack the version or differ from the capture; nothing is
 *   written then
 */
const mergeClientTables = ({
  tablesDir,
  version,
  capture,
  rangesFile = RANGES_FILE,
  featuresFile = FEATURES_FILE,
}) => {
  const { apiNames, rangeOf, recordRanges, SHARE_VERSION } = require('../e2e/apiVersionsProbe')
  const entry = (file) => JSON.parse(fs.readFileSync(path.join(tablesDir, file), 'utf8'))[version]
  const ranges = entry('brokerRanges.json')
  const features = entry('brokerFeatures.json')
  if (!ranges || !features) throw new Error(`the client tables hold no ${version} entry`)

  const keys = Object.keys(capture.apiRanges)
  if (Object.keys(ranges).length !== keys.length) {
    throw new Error(
      `the client range table lists ${Object.keys(ranges).length} APIs, the capture ${keys.length}`
    )
  }
  const names = apiNames()
  for (const key of keys) {
    const recorded = rangeOf(ranges, Number(key), names)
    if (JSON.stringify(recorded) !== JSON.stringify(capture.apiRanges[key])) {
      throw new Error(
        `API ${key}: the client table ${JSON.stringify(recorded)} differs from the capture ${JSON.stringify(capture.apiRanges[key])}`
      )
    }
  }

  const { supportedFeatures, finalizedFeatures } = capture.features
  const finalized = finalizedFeatures.find(({ name }) => name === SHARE_VERSION)
  const expected = finalized
    ? { [SHARE_VERSION]: finalized.maxVersionLevel }
    : supportedFeatures.some(({ name }) => name === SHARE_VERSION)
      ? { [SHARE_VERSION]: 0 }
      : {}
  if (JSON.stringify(features) !== JSON.stringify(expected)) {
    throw new Error(
      `the client features ${JSON.stringify(features)} differ from the captured ${JSON.stringify(expected)} (share.version)`
    )
  }

  recordRanges(rangesFile, version, ranges)
  recordRanges(featuresFile, version, features)
}

/**
 * @param {string} version
 * @param {{ apiRanges: Record<string, number[]> }} capture
 * @param {string} verb what was done with the capture
 * @returns {string} one line naming the API count and whether key 94 (UnregisterController) is offered
 */
const summary = (version, capture, verb) =>
  `${verb} ${version}: ${Object.keys(capture.apiRanges).length} API ranges, key 94 ` +
  (capture.apiRanges[94] ? `offered ${JSON.stringify(capture.apiRanges[94])}` : 'not offered')

/**
 * Imports one artifact. A real run validates the written capture with the strict reader and removes
 * a capture the reader refuses; a dry run does every check in a scratch directory and writes nothing
 * to the repository.
 * @param {string[]} argv the arguments after the script name
 * @returns {void}
 * @throws {Error} with the usage for missing arguments, else what the steps throw
 */
const main = (argv) => {
  const option = (name) => (argv.includes(name) ? argv[argv.indexOf(name) + 1] : undefined)
  const [artifactDir, version, tablesDir] = [
    option('--artifact'),
    option('--version'),
    option('--tables'),
  ]
  if (!artifactDir || !version) throw new Error(USAGE)

  if (argv.includes('--dry-run')) {
    const scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'capture-dry-run-'))
    try {
      const { capture } = importCapture({ artifactDir, version, outDir: scratch })
      readBrokerCapture(version, scratch)
      if (tablesDir) {
        const [rangesFile, featuresFile] = [RANGES_FILE, FEATURES_FILE].map((file) => {
          const copy = path.join(scratch, path.basename(file))
          fs.copyFileSync(file, copy)
          return copy
        })
        mergeClientTables({ tablesDir, version, capture, rangesFile, featuresFile })
      }
      console.log(summary(version, capture, 'dry run of'))
    } finally {
      fs.rmSync(scratch, { recursive: true, force: true })
    }
    return
  }

  const { capture, target } = importCapture({ artifactDir, version })
  try {
    readBrokerCapture(version)
    if (tablesDir) mergeClientTables({ tablesDir, version, capture })
  } catch (error) {
    fs.rmSync(target, { recursive: true, force: true })
    throw error
  }
  console.log(summary(version, capture, `imported into ${path.relative(process.cwd(), target)}:`))
}

if (require.main === module) {
  try {
    main(process.argv.slice(2))
  } catch (error) {
    console.error(error.message)
    process.exit(1)
  }
}

module.exports = { importCapture, mergeClientTables, readIdentity, verifyArtifact, main }
