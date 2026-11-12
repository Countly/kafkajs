#!/usr/bin/env node
'use strict'

/**
 * Transaction-version probe for the TV1 test arm.
 *
 * Usage: node scripts/e2e/transactionVersionProbe.js   (npm run probe:transaction-version)
 * Prints one JSON line, and appends it to `.tmp/e2e/transaction-version-probe.jsonl`, when the
 * committed arm pins the level:
 *   {"event":"transaction-version","kafkaVersion":"4.3.1","mechanism":"downgrade",
 *    "formatFlag":"pinned"|"not-pinned"|"failed","finalizedFeaturesEpoch":<n>,"maxVersionLevel":1}
 *
 * Development-only. It brings `docker-compose.transaction-v1.yml` up from scratch and asserts that
 * the finalized feature `transaction.version` ends at level 1. That arm pins it by a runtime
 * downgrade - a one-shot service runs `kafka-features.sh downgrade --feature transaction.version=1`
 * once the brokers answer - so the level is polled, not read once. Every reading is this client's
 * own ApiVersions v4 response (the `FinalizedFeatures` tagged field), never a CLI's output.
 *
 * It then runs a second arm as a finding that never decides the exit code: the base cluster
 * (`docker-compose.yml`) with each node's storage re-formatted by
 * `kafka-storage.sh format --feature transaction.version=1`. `formatFlag` reports whether that
 * pinned the level (`pinned`), left it elsewhere (`not-pinned`) or never came up (`failed`).
 *
 * It drives Docker itself, so it is never wrapped in `npm run test:kafka`, and it leaves no cluster
 * running. Exit codes: 0 when the committed arm reports level 1; 1 otherwise.
 * @see https://github.com/apache/kafka/blob/4.3/tools/src/main/java/org/apache/kafka/tools/FeatureCommand.java
 * @see https://github.com/apache/kafka/blob/4.3/core/src/main/scala/kafka/tools/StorageTool.scala
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-890%3A+Transactions+Server-Side+Defense
 */
const fs = require('fs')
const os = require('os')
const path = require('path')
const { execFileSync } = require('child_process')
const { writeEvidence } = require('../../testHelpers/e2eEvidence')
const Connection = require('../../src/network/connection')
const defaultSocketFactory = require('../../src/network/socketFactory')
const ApiVersions = require('../../src/protocol/requests/apiVersions')
const { createLogger, LEVELS } = require('../../src/loggers')
const LoggerConsole = require('../../src/loggers/console')

const ROOT = path.resolve(__dirname, '..', '..')
const BASE_COMPOSE = 'docker-compose.yml'
const TV1_COMPOSE = 'docker-compose.transaction-v1.yml'
const WAIT_FOR_KAFKA = path.join(ROOT, 'scripts', 'waitForKafka.js')
const KAFKA_SERVICES = ['kafka1', 'kafka2', 'kafka3']
const FEATURE = 'transaction.version'
const PINNED_LEVEL = 1
const FORMAT_DEADLINE_MS = 30000
const ARM_DEADLINE_MS = 60000
const POLL_INTERVAL_MS = 1000

/**
 * What each node of the storage-format finding arm runs instead of the image's entry point. The
 * image formats storage itself (KafkaDockerWrapper setup) with the release's default features -
 * level 2 on 4.x - and its launch step tolerates a directory that is already formatted. So the
 * node runs that setup once to write its configuration, removes the default-formatted log
 * directory, formats it again with the level pinned and hands over to the image's own entry point.
 * `$$` is compose's escape for a literal `$`, so bash sees `$KAFKA_LOG_DIRS` and `$CLUSTER_ID`.
 * @see https://github.com/apache/kafka/blob/4.3/docker/jvm/launch
 * @see https://github.com/apache/kafka/blob/4.3/core/src/main/scala/kafka/docker/KafkaDockerWrapper.scala
 */
const FORMAT_SCRIPT = [
  'set -e',
  '/opt/kafka/bin/kafka-run-class.sh kafka.docker.KafkaDockerWrapper setup --default-configs-dir /etc/kafka/docker --mounted-configs-dir /mnt/shared/config --final-configs-dir /opt/kafka/config',
  'rm -rf "$$KAFKA_LOG_DIRS"',
  '/opt/kafka/bin/kafka-storage.sh format --cluster-id "$$CLUSTER_ID" --feature transaction.version=1 -c /opt/kafka/config/server.properties',
  'exec /etc/kafka/docker/run',
].join('\n')

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

/**
 * The compose override that turns the base cluster into the storage-format finding arm. JSON is
 * valid YAML, so it is written as JSON and layered on `docker-compose.yml` with a second `-f`.
 * @returns {string}
 */
const formatArmOverride = () =>
  JSON.stringify(
    {
      services: Object.fromEntries(
        KAFKA_SERVICES.map((service) => [service, { command: ['bash', '-c', FORMAT_SCRIPT] }])
      ),
    },
    null,
    2
  )

/**
 * Reads the finalized `transaction.version` level, and the epoch of the features it belongs to, off
 * a decoded ApiVersions response.
 * @param {{ finalizedFeaturesEpoch: string, finalizedFeatures: {name: string, maxVersionLevel: number}[] }} response
 *   a decoded ApiVersions v3+ response
 * @returns {{ finalizedFeaturesEpoch: number, maxVersionLevel: number|null }} `maxVersionLevel` is
 *   null when the broker finalized no `transaction.version`
 */
const transactionVersionOf = ({ finalizedFeaturesEpoch, finalizedFeatures }) => {
  const feature = (finalizedFeatures || []).find(({ name }) => name === FEATURE)
  return {
    finalizedFeaturesEpoch: Number(finalizedFeaturesEpoch),
    maxVersionLevel: feature ? feature.maxVersionLevel : null,
  }
}

/**
 * @param {{ finalizedFeaturesEpoch: number, maxVersionLevel: number|null }|null} reading
 * @returns {boolean} whether the features are valid and the level is the pinned one
 */
const isPinned = (reading) =>
  reading != null && reading.finalizedFeaturesEpoch >= 0 && reading.maxVersionLevel === PINNED_LEVEL

/**
 * @param {{ finalizedFeaturesEpoch: number, maxVersionLevel: number|null }|null} reading the
 *   finding arm's last reading, null when the arm never answered
 * @returns {'pinned'|'not-pinned'|'failed'}
 */
const formatFlagOf = (reading) => {
  if (reading == null) {
    return 'failed'
  }

  return isPinned(reading) ? 'pinned' : 'not-pinned'
}

/**
 * Runs `docker compose` over the given files from the repository root; its output goes to stderr
 * so stdout carries only the probe's own line.
 * @param {string[]} files compose files, layered in order
 * @param {string[]} args
 * @returns {void}
 * @throws {Error} when docker compose exits non-zero
 */
const compose = (files, args) => {
  execFileSync('docker', ['compose', ...files.flatMap((file) => ['-f', file]), ...args], {
    cwd: ROOT,
    stdio: ['ignore', 2, 2],
  })
}

/**
 * Waits for the three brokers with the harness's own script, which also warms the cluster.
 * @returns {void}
 * @throws {Error} when a broker never answers within the script's deadline
 */
const waitForBrokers = () => {
  execFileSync(process.execPath, [WAIT_FOR_KAFKA], {
    cwd: ROOT,
    stdio: ['ignore', 2, 2],
    env: { ...process.env, COMPOSE_FILE: BASE_COMPOSE },
  })
}

/**
 * One ApiVersions v4 round trip to `localhost:9092` over a fresh connection.
 * @returns {Promise<{ finalizedFeaturesEpoch: number, maxVersionLevel: number|null }>}
 */
const readTransactionVersion = async () => {
  const connection = new Connection({
    host: 'localhost',
    port: 9092,
    logger: createLogger({ level: LEVELS.NOTHING, logCreator: LoggerConsole }),
    socketFactory: defaultSocketFactory(),
    clientId: 'transaction-version-probe',
    connectionTimeout: 3000,
    requestTimeout: 10000,
  })

  try {
    await connection.connect()
    return transactionVersionOf(await connection.send(ApiVersions.protocol({ version: 4 })()))
  } finally {
    await connection.disconnect()
  }
}

/**
 * Polls until the level is pinned or the deadline passes. A broker that is still loading its
 * metadata answers with no features yet, and the committed arm's downgrade lands after the
 * brokers first answer, which is why one reading is not enough.
 * @param {number} deadlineMs
 * @returns {Promise<{ finalizedFeaturesEpoch: number, maxVersionLevel: number|null }|null>} the last reading
 */
const awaitPinnedLevel = async (deadlineMs) => {
  const deadline = Date.now() + deadlineMs
  let reading = null

  for (;;) {
    try {
      reading = await readTransactionVersion()
    } catch (e) {
      process.stderr.write(`ApiVersions read failed: ${e.message}\n`)
    }

    if (isPinned(reading) || Date.now() >= deadline) {
      return reading
    }

    await sleep(POLL_INTERVAL_MS)
  }
}

/**
 * Brings one arm up from scratch, waits for the brokers, polls the level and tears the arm down.
 * A failure anywhere is a reading of `null`, never a thrown probe.
 * @param {string} label for stderr
 * @param {string[]} files compose files for this arm
 * @param {number} deadlineMs how long the level may take to appear
 * @returns {Promise<{ finalizedFeaturesEpoch: number, maxVersionLevel: number|null }|null>}
 */
const runArm = async (label, files, deadlineMs) => {
  try {
    compose(files, ['down', '--remove-orphans'])
    compose(files, ['up', '-d', '--force-recreate'])
    waitForBrokers()
    const reading = await awaitPinnedLevel(deadlineMs)
    process.stderr.write(`${label}: ${JSON.stringify(reading)}\n`)
    return reading
  } catch (e) {
    process.stderr.write(`${label} failed: ${e.message}\n`)
    return null
  } finally {
    try {
      compose(files, ['down', '--remove-orphans'])
    } catch (e) {
      process.stderr.write(`${label}: docker compose down failed: ${e.message}\n`)
    }
  }
}

/**
 * @returns {Promise<number>} the process exit code
 */
const main = async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'kafkajs-tv-probe-'))
  const override = path.join(dir, 'format-override.yml')
  fs.writeFileSync(override, formatArmOverride())

  try {
    const arm = await runArm('committed arm', [TV1_COMPOSE], ARM_DEADLINE_MS)
    const formatFlag = formatFlagOf(
      await runArm('format finding', [BASE_COMPOSE, override], FORMAT_DEADLINE_MS)
    )
    process.stderr.write(`storage-format flag: ${formatFlag}\n`)

    if (!isPinned(arm)) {
      process.stderr.write(`${TV1_COMPOSE} did not pin ${FEATURE} at level ${PINNED_LEVEL}\n`)
      return 1
    }

    const line = {
      event: 'transaction-version',
      kafkaVersion: process.env.KAFKA_VERSION || '4.4.0',
      mechanism: 'downgrade',
      formatFlag,
      finalizedFeaturesEpoch: arm.finalizedFeaturesEpoch,
      maxVersionLevel: arm.maxVersionLevel,
    }
    writeEvidence('transaction-version-probe', line)
    console.log(JSON.stringify(line))
    return 0
  } finally {
    fs.rmSync(dir, { recursive: true, force: true })
  }
}

if (require.main === module) {
  main().then(
    (code) => process.exit(code),
    (e) => {
      console.error(e)
      process.exit(1)
    }
  )
}

module.exports = { transactionVersionOf }
