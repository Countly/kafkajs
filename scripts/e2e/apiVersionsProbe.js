#!/usr/bin/env node
'use strict'

/**
 * ApiVersions probe: the version range a broker offers for each API the admin and classic-group
 * codecs target.
 *
 * Usage inside a guard-owned fixture: node scripts/e2e/apiVersionsProbe.js [--all | --record=<file>] [--record-features=<file>]
 * Run this probe only inside a fixture owned by the reviewed live guard.
 * The fixture owner selects the broker version and records evidence before teardown.
 * Prints one JSON line, and appends it to `.tmp/e2e/api-versions-probe.jsonl`:
 *   {"event":"api-versions","kafkaVersion":"4.3.1","versions":{"Metadata":[0,13],…}}
 * where each value is `[minVersion, maxVersion]`, or `null` for an API the broker does not list.
 *
 * With `--all` the line lists every API the broker offers, not just the probed ones: named as the
 * client names it when it knows the API, else as the Kafka 4.3.1 message definitions do, else
 * `ApiKey<n>`. The version is the one the running broker reports, never the `KAFKA_VERSION` asked
 * for (the two must agree, or the probe stops). With `--record=<file>` it also merges that entry
 * into a JSON table `{ "<version>": { "<ApiName>": [min, max], … } }`, keeping the other entries:
 * that is how `scripts/protocol/brokerRanges.json` is written. `--record` needs its file as
 * `--record=<file>`; `--record`, `--record=` and `--record <file>` print a usage line and exit 1.
 *
 * With `--record-features=<file>` (which implies `--all`) it also reads the broker's finalized
 * `share.version` off the same answer, prints and appends a second line
 * `{"event":"finalized-features","kafkaVersion":"4.3.1","features":{"share.version":1}}` and merges
 * `features` into that file the same way: that is how `scripts/protocol/brokerFeatures.json` is
 * written. The file needs naming as `--record` does.
 *
 * Development-only. It reads one ApiVersions v4 answer from `localhost:9092` and creates nothing on
 * the cluster. A 4.1 broker hides an unstable last version from ApiVersions, so OffsetCommit and
 * OffsetFetch report a maximum of 9 there and 10 from 4.2.
 * @see https://kafka.apache.org/43/design/protocol
 * @see https://github.com/apache/kafka/blob/4.1.2/clients/src/main/java/org/apache/kafka/common/protocol/ApiKeys.java
 */
const fs = require('fs')
const path = require('path')
const { writeEvidence } = require('../../testHelpers/e2eEvidence')
const { readBrokerVersion } = require('../../testHelpers/brokerVersion')
const apiKeys = require('../../src/protocol/requests/apiKeys')
const { readSpec } = require('../compat/parse')
const Connection = require('../../src/network/connection')
const defaultSocketFactory = require('../../src/network/socketFactory')
const ApiVersions = require('../../src/protocol/requests/apiVersions')
const { createLogger, LEVELS } = require('../../src/loggers')
const LoggerConsole = require('../../src/loggers/console')

/**
 * The probed APIs and their protocol keys, in the order the line prints them. Kept here, not read
 * from `apiKeys.js`, because DescribeTopicPartitions (75) is not declared there until its codec
 * lands.
 * @see https://kafka.apache.org/43/design/protocol
 * @type {Readonly<Record<string, number>>}
 */
const PROBED_APIS = Object.freeze({
  Metadata: 3,
  OffsetCommit: 8,
  OffsetFetch: 9,
  JoinGroup: 11,
  SyncGroup: 14,
  Heartbeat: 12,
  LeaveGroup: 13,
  CreateTopics: 19,
  DeleteTopics: 20,
  CreatePartitions: 37,
  DeleteRecords: 21,
  DeleteGroups: 42,
  DescribeAcls: 29,
  CreateAcls: 30,
  DeleteAcls: 31,
  DescribeConfigs: 32,
  AlterConfigs: 33,
  IncrementalAlterConfigs: 44,
  DescribeTopicPartitions: 75,
})

/**
 * @param {{ apiKey: number, minVersion: number, maxVersion: number }[]} apiVersions the `apiVersions`
 *   of a decoded ApiVersions response
 * @returns {Record<string, [number, number]|null>} `[min, max]` per probed API; `null` when the
 *   broker lists none
 */
const versionsOf = (apiVersions) => {
  const ranges = new Map(
    apiVersions.map(({ apiKey, minVersion, maxVersion }) => [apiKey, [minVersion, maxVersion]])
  )

  return Object.fromEntries(
    Object.entries(PROBED_APIS).map(([name, apiKey]) => [name, ranges.get(apiKey) ?? null])
  )
}

/**
 * The Kafka 4.3.1 message definitions the repository pins; a key with neither a client name nor a
 * definition is recorded as `ApiKey<n>`.
 */
const MESSAGES = path.join(__dirname, '..', 'compat', 'messages', '4.3.1')

/**
 * The name of each API key: the client's own name for the ones it declares (`GroupCoordinator`
 * for key 10, as `apiKeys.js` has it), else the name in the pinned message definitions.
 * @returns {Map<number, string>}
 */
const apiNames = () => {
  const names = new Map(
    fs
      .readdirSync(MESSAGES)
      .filter((file) => file.endsWith('Request.json'))
      .map((file) => [readSpec(path.join(MESSAGES, file)).apiKey, file.replace('Request.json', '')])
  )
  for (const [name, apiKey] of Object.entries(apiKeys)) {
    names.set(apiKey, name)
  }

  return names
}

/**
 * Every API a broker offers, by name, in the order of its key.
 * @param {{ apiKey: number, minVersion: number, maxVersion: number }[]} apiVersions the
 *   `apiVersions` of a decoded ApiVersions response
 * @param {Map<number, string>} [names] key to name; `ApiKey<n>` for a key it lacks
 * @returns {Record<string, [number, number]>} `[minVersion, maxVersion]` per API
 */
const allVersionsOf = (apiVersions, names = apiNames()) =>
  Object.fromEntries(
    [...apiVersions]
      .sort((a, b) => a.apiKey - b.apiKey)
      .map(({ apiKey, minVersion, maxVersion }) => [
        names.get(apiKey) ?? `ApiKey${apiKey}`,
        [minVersion, maxVersion],
      ])
  )

/**
 * The range one API has in a recorded entry, by the name it was recorded under: the client's name
 * for the key now, else `ApiKey<n>`. A key the client named after the entry was recorded is still
 * found under its `ApiKey<n>`, so read a recorded table through this, never by name.
 * @param {Record<string, [number, number]>} entry one version's entry of `brokerRanges.json`
 * @param {number} apiKey the protocol key
 * @param {Map<number, string>} [names] key to name, as `apiNames` gives it
 * @returns {[number, number]|undefined} `[minVersion, maxVersion]`; `undefined` when the entry
 *   holds the key under neither name
 */
const rangeOf = (entry, apiKey, names = apiNames()) =>
  entry[names.get(apiKey)] ?? entry[`ApiKey${apiKey}`]

/**
 * The name of the share-group feature whose finalized level gates share groups (KIP-932).
 * @see https://github.com/apache/kafka/blob/4.4.0/server-common/src/main/java/org/apache/kafka/server/common/ShareVersion.java
 */
const SHARE_VERSION = 'share.version'

/**
 * The finalized `share.version` of an ApiVersions answer, as `brokerFeatures.json` records it: the
 * finalized `maxVersionLevel` when the broker finalized the feature, `0` when it supports the
 * feature without finalizing it (on the wire, level 0 is the absence of a finalized entry), and no
 * key when the broker neither supports nor finalized it. A level is never inferred from the API
 * ranges, and nothing is read from the epoch except that it is present.
 * @param {{ supportedFeatures?: { name: string }[], finalizedFeatures?: { name: string, maxVersionLevel: number }[], finalizedFeaturesEpoch?: string }} answer
 *   a decoded ApiVersions v3+ response; the epoch is the int64 decimal string the codec gives
 * @returns {{ 'share.version'?: number }}
 * @throws {Error} when the answer carries no finalized-features epoch (-1): such a broker has told
 *   nothing about its features, so no level may be recorded for it
 */
const featuresOf = ({
  supportedFeatures = [],
  finalizedFeatures = [],
  finalizedFeaturesEpoch = '-1',
}) => {
  if (BigInt(finalizedFeaturesEpoch) < 0n) {
    throw new Error('The broker sent no finalized features (epoch -1): nothing to record')
  }

  const finalized = finalizedFeatures.find(({ name }) => name === SHARE_VERSION)
  if (finalized) {
    return { [SHARE_VERSION]: finalized.maxVersionLevel }
  }

  return supportedFeatures.some(({ name }) => name === SHARE_VERSION) ? { [SHARE_VERSION]: 0 } : {}
}

/**
 * Writes one broker version's entry into a JSON table, creating the file when it is missing,
 * replacing that version's entry when it is there and leaving the others as they are. Entries are
 * ordered by version. It writes `brokerRanges.json` and `brokerFeatures.json`.
 * @param {string} file path of the JSON table, `{ "<version>": { … }, … }`
 * @param {string} version `major.minor.patch`, as the broker reports it
 * @param {object} versions the version's entry: `[minVersion, maxVersion]` per API name, as
 *   `allVersionsOf` gives it, or the features `featuresOf` gives
 * @returns {void}
 * @throws {Error} when `version` is not `major.minor.patch`
 */
const recordRanges = (file, version, versions) => {
  if (!/^\d+\.\d+\.\d+$/.test(version)) {
    throw new Error(`A broker version is major.minor.patch, got ${JSON.stringify(version)}`)
  }

  const table = fs.existsSync(file) ? JSON.parse(fs.readFileSync(file, 'utf8')) : {}
  table[version] = versions
  const order = (a, b) => a.localeCompare(b, undefined, { numeric: true })
  const sorted = Object.fromEntries(
    Object.keys(table)
      .sort(order)
      .map((key) => [key, table[key]])
  )
  fs.writeFileSync(file, `${JSON.stringify(sorted, null, 2)}\n`)
}

/**
 * A run on the wrong broker is no evidence of the right one: the version the running broker
 * reports must be the one the lane asked for, when it asked for one.
 * @param {string} reported the version the running broker reports
 * @param {string|undefined} requested `KAFKA_VERSION`, when set
 * @returns {void}
 * @throws {Error} when `requested` is set and differs from `reported`
 */
const assertSameVersion = (reported, requested) => {
  if (requested && requested !== reported) {
    throw new Error(`The broker reports ${reported} but KAFKA_VERSION=${requested}: stopping`)
  }
}

/**
 * One ApiVersions v4 round trip over a fresh connection, to `localhost:9092` unless told otherwise.
 * @param {{ host?: string, port?: number }} [address] the broker's plaintext listener
 * @returns {Promise<{ apiVersions: object[] }>} the decoded response
 * @throws {Error} when the broker cannot be reached or answers with an error code
 */
const readApiVersions = async ({ host = 'localhost', port = 9092 } = {}) => {
  const connection = new Connection({
    host,
    port,
    logger: createLogger({ level: LEVELS.NOTHING, logCreator: LoggerConsole }),
    socketFactory: defaultSocketFactory(),
    clientId: 'api-versions-probe',
    connectionTimeout: 3000,
    requestTimeout: 10000,
  })

  try {
    await connection.connect()
    return await connection.send(ApiVersions.protocol({ version: 4 })())
  } finally {
    await connection.disconnect()
  }
}

const USAGE =
  'Usage: node scripts/e2e/apiVersionsProbe.js [--all | --record=<file>] [--record-features=<file>]'

/**
 * The file a `<flag>=<file>` argument names.
 * @param {string[]} argv the arguments after the script name
 * @param {string} flag e.g. `--record`
 * @returns {string|undefined} the file; `''` for the flag without one (`<flag>` or `<flag>=`);
 *   `undefined` when the flag is absent
 */
const fileArgument = (argv, flag) =>
  argv.find((arg) => arg === flag || arg.startsWith(`${flag}=`))?.slice(flag.length + 1)

const DEFAULT_SOURCES = Object.freeze({ readApiVersions, readBrokerVersion, writeEvidence })

/**
 * Runs the probe: reads one ApiVersions answer, prints the line, appends it to the evidence file
 * and, with `--record=<file>`, merges it into that table; with `--record-features=<file>` it does
 * the same with the finalized features of that answer. `--all` and both record flags list every
 * API under the version the running broker reports; plain mode lists the probed APIs under
 * `KAFKA_VERSION` (default 4.3.1) and never asks the broker for its version.
 * @param {string[]} argv the arguments after the script name
 * @param {object} [sources] what the probe reads and writes; each defaults to the live one
 * @param {() => Promise<object>} [sources.readApiVersions] the broker's decoded answer
 * @param {() => string} [sources.readBrokerVersion] the version the running broker reports
 * @param {(name: string, record: object) => void} [sources.writeEvidence] the evidence writer
 * @returns {Promise<void>}
 * @throws {Error} with the usage when a record flag has no file, before anything is read; when
 *   the broker is not the version `KAFKA_VERSION` asks for, and when `--record-features` meets an
 *   answer without finalized features, before anything is written
 */
const main = async (argv, sources = {}) => {
  const io = { ...DEFAULT_SOURCES, ...sources }
  const record = fileArgument(argv, '--record')
  const recordFeatures = fileArgument(argv, '--record-features')
  if (record === '' || recordFeatures === '') {
    throw new Error(USAGE)
  }

  const all = argv.includes('--all') || record !== undefined || recordFeatures !== undefined
  const answer = await io.readApiVersions()
  const kafkaVersion = all ? io.readBrokerVersion() : process.env.KAFKA_VERSION || '4.3.1'

  if (all) {
    assertSameVersion(kafkaVersion, process.env.KAFKA_VERSION)
  }

  const versions = all ? allVersionsOf(answer.apiVersions) : versionsOf(answer.apiVersions)
  const line = { event: 'api-versions', kafkaVersion, versions }
  const featureLine = recordFeatures && {
    event: 'finalized-features',
    kafkaVersion,
    features: featuresOf(answer),
  }
  io.writeEvidence('api-versions-probe', line)
  if (record) {
    recordRanges(path.resolve(record), kafkaVersion, versions)
  }

  console.log(JSON.stringify(line))
  if (featureLine) {
    io.writeEvidence('api-versions-probe', featureLine)
    recordRanges(path.resolve(recordFeatures), kafkaVersion, featureLine.features)
    console.log(JSON.stringify(featureLine))
  }
}

if (require.main === module) {
  main(process.argv.slice(2)).then(
    () => process.exit(0),
    (e) => {
      console.error(e)
      process.exit(1)
    }
  )
}

module.exports = {
  PROBED_APIS,
  versionsOf,
  apiNames,
  allVersionsOf,
  rangeOf,
  SHARE_VERSION,
  featuresOf,
  recordRanges,
  readApiVersions,
  assertSameVersion,
  main,
}
