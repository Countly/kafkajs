#!/usr/bin/env node
'use strict'

/**
 * ApiVersions probe: the version range a broker offers for each API the admin and classic-group
 * codecs target.
 *
 * Usage: npm run probe:api-versions
 *   (= npm run test:kafka -- "node scripts/e2e/apiVersionsProbe.js"; `KAFKA_VERSION=4.2.2` or
 *   `4.1.2` selects the cluster image, default 4.3.1)
 * Prints one JSON line, and appends it to `.tmp/e2e/api-versions-probe.jsonl`:
 *   {"event":"api-versions","kafkaVersion":"4.3.1","versions":{"Metadata":[0,13],…}}
 * where each value is `[minVersion, maxVersion]`, or `null` for an API the broker does not list.
 *
 * Development-only. It reads one ApiVersions v4 answer from `localhost:9092` and creates nothing on
 * the cluster. A 4.1 broker hides an unstable last version from ApiVersions, so OffsetCommit and
 * OffsetFetch report a maximum of 9 there and 10 from 4.2.
 * @see https://kafka.apache.org/43/design/protocol
 * @see https://github.com/apache/kafka/blob/4.1.2/clients/src/main/java/org/apache/kafka/common/protocol/ApiKeys.java
 */
const { writeEvidence } = require('../../testHelpers/e2eEvidence')
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
 * One ApiVersions v4 round trip to `localhost:9092` over a fresh connection.
 * @returns {Promise<{ apiVersions: object[] }>} the decoded response
 * @throws {Error} when the broker cannot be reached or answers with an error code
 */
const readApiVersions = async () => {
  const connection = new Connection({
    host: 'localhost',
    port: 9092,
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

/** @returns {Promise<void>} */
const main = async () => {
  const { apiVersions } = await readApiVersions()
  const line = {
    event: 'api-versions',
    kafkaVersion: process.env.KAFKA_VERSION || '4.3.1',
    versions: versionsOf(apiVersions),
  }
  writeEvidence('api-versions-probe', line)
  console.log(JSON.stringify(line))
}

if (require.main === module) {
  main().then(
    () => process.exit(0),
    (e) => {
      console.error(e)
      process.exit(1)
    }
  )
}

module.exports = { PROBED_APIS, versionsOf }
