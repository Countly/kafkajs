#!/usr/bin/env node
'use strict'

/**
 * Idempotent-producer authorization probe for the ACL test arm.
 *
 * Usage: npm run probe:idempotent-authz
 *   (= COMPOSE_FILE=docker-compose.acl.yml npm run test:kafka -- "node scripts/e2e/idempotentAuthzProbe.js")
 * Prints one JSON line, and appends it to `.tmp/e2e/idempotent-authz-probe.jsonl`:
 *   {"event":"idempotent-authz","restrictedInitProducerIdCode":<n>,"nonIdempotentConnectCode":<n>,
 *    "nonIdempotentSendCode":<n>,"afterTopicWriteGrantCode":<n>}
 *
 * Development-only. It measures what the authorization scenario asserts, on the cluster it runs
 * against: the code an idempotent producer's InitProducerId gets for a principal holding no ACL
 * at all; the code a non-idempotent producer's connect() gets for the same principal - 0 is the
 * expected answer, and since that principal is refused InitProducerId it also proves none was
 * sent; the code a non-idempotent send then gets; and the code InitProducerId gets once that
 * principal holds WRITE on one topic. From Kafka 2.8 a broker authorizes InitProducerId by
 * IDEMPOTENT_WRITE on the cluster OR WRITE on any topic, so the grant alone is expected to turn
 * the first answer into success. A code of 0 means the call succeeded. The grant is deleted again
 * with the topic, so a reused cluster starts the next run with the principal holding no ACL.
 * @see https://github.com/apache/kafka/blob/4.3/core/src/main/scala/kafka/server/KafkaApis.scala
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-679%3A+Producer+will+enable+the+strongest+delivery+guarantee+by+default
 */
const fs = require('fs')
const path = require('path')
const { writeEvidence } = require('../../testHelpers/e2eEvidence')
const {
  Kafka,
  logLevel,
  AclResourceTypes,
  AclOperationTypes,
  AclPermissionTypes,
  ResourcePatternTypes,
} = require('../../index')

const ROOT = path.resolve(__dirname, '..', '..')
const SUPER_USER_BROKERS = ['localhost:9092', 'localhost:9095', 'localhost:9098']
const SASL_BROKERS = ['localhost:9094', 'localhost:9097', 'localhost:9100']
const RESTRICTED_PRINCIPAL = 'User:bob'
const RESTRICTED_SASL = { mechanism: 'plain', username: 'bob', password: 'bobbob' }
const GRANT_DEADLINE_MS = 30000
const POLL_INTERVAL_MS = 500

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

/**
 * The broker error code an error carries, walking the cause chain: this client's retrier and
 * producer wrap protocol errors, and the wrapper may or may not copy the code.
 * @param {Error} error
 * @returns {number|null} the first numeric `code` found, or null when there is none
 */
const errorCodeOf = (error) => {
  const seen = new Set()

  for (let current = error; current != null && !seen.has(current); current = current.cause) {
    if (typeof current.code === 'number') {
      return current.code
    }

    seen.add(current)
  }

  return null
}

/**
 * @returns {Kafka} a client authenticating as the restricted principal over SASL_SSL
 */
const restrictedClient = () =>
  new Kafka({
    clientId: 'idempotent-authz-probe-restricted',
    brokers: SASL_BROKERS,
    ssl: {
      servername: 'localhost',
      rejectUnauthorized: false,
      ca: [fs.readFileSync(path.join(ROOT, 'testHelpers', 'certs', 'cert-signed'), 'utf-8')],
    },
    sasl: RESTRICTED_SASL,
    retry: { retries: 2, initialRetryTime: 100, maxRetryTime: 500 },
    logLevel: logLevel.NOTHING,
  })

/**
 * Connects an idempotent producer, which issues InitProducerId inside `connect()`.
 * @param {Kafka} kafka
 * @returns {Promise<number|null>} 0 on success, else the refusal's code
 */
const initProducerIdCode = async (kafka) => {
  const producer = kafka.producer({ idempotent: true })
  try {
    await producer.connect()
    return 0
  } catch (e) {
    return errorCodeOf(e)
  } finally {
    await producer.disconnect().catch(() => {})
  }
}

/**
 * Connects a non-idempotent producer, which issues no InitProducerId inside `connect()`.
 * @param {Kafka} kafka
 * @returns {Promise<number|null>} 0 on success, else the refusal's code
 */
const nonIdempotentConnectCode = async (kafka) => {
  const producer = kafka.producer({ idempotent: false })
  try {
    await producer.connect()
    return 0
  } catch (e) {
    return errorCodeOf(e)
  } finally {
    await producer.disconnect().catch(() => {})
  }
}

/**
 * Sends one record from a connected non-idempotent producer. The `connect()` is measured on its
 * own by `nonIdempotentConnectCode`, so a failure there is a broken run and is thrown, not coded.
 * @param {Kafka} kafka
 * @param {string} topic
 * @returns {Promise<number|null>} 0 on success, else the refusal's code
 */
const nonIdempotentSendCode = async (kafka, topic) => {
  const producer = kafka.producer({ idempotent: false })
  try {
    await producer.connect()
    try {
      await producer.send({ topic, messages: [{ value: 'probe' }] })
      return 0
    } catch (e) {
      return errorCodeOf(e)
    }
  } finally {
    await producer.disconnect().catch(() => {})
  }
}

/**
 * Re-asks until the expected code appears or the deadline passes: an ACL reaches every broker
 * through the metadata log, so the first attempt after a grant can still be refused.
 * @param {() => Promise<number|null>} attempt
 * @param {number} expected
 * @param {number} deadlineMs
 * @returns {Promise<number|null>} the last code observed
 */
const awaitCode = async (attempt, expected, deadlineMs) => {
  const deadline = Date.now() + deadlineMs
  for (;;) {
    const code = await attempt()
    if (code === expected || Date.now() >= deadline) {
      return code
    }
    await sleep(POLL_INTERVAL_MS)
  }
}

/**
 * @returns {Promise<void>}
 * @throws {Error} when a measurement carried no code at all - a broken run, not an answer
 */
const main = async () => {
  const topic = `idempotent-authz-probe-${Date.now()}`
  const topicWriteAcl = {
    resourceType: AclResourceTypes.TOPIC,
    resourceName: topic,
    resourcePatternType: ResourcePatternTypes.LITERAL,
    principal: RESTRICTED_PRINCIPAL,
    host: '*',
    operation: AclOperationTypes.WRITE,
    permissionType: AclPermissionTypes.ALLOW,
  }
  const admin = new Kafka({
    clientId: 'idempotent-authz-probe-admin',
    brokers: SUPER_USER_BROKERS,
    logLevel: logLevel.NOTHING,
  }).admin()

  await admin.connect()
  try {
    await admin.createTopics({
      topics: [{ topic, numPartitions: 1, replicationFactor: 1 }],
      waitForLeaders: true,
    })

    const restricted = restrictedClient()
    const restrictedInitProducerIdCode = await initProducerIdCode(restricted)
    const connectCode = await nonIdempotentConnectCode(restricted)
    const sendCode = await nonIdempotentSendCode(restricted, topic)

    await admin.createAcls({ acl: [topicWriteAcl] })

    const afterTopicWriteGrantCode = await awaitCode(
      () => initProducerIdCode(restricted),
      0,
      GRANT_DEADLINE_MS
    )

    const codes = {
      restrictedInitProducerIdCode,
      nonIdempotentConnectCode: connectCode,
      nonIdempotentSendCode: sendCode,
      afterTopicWriteGrantCode,
    }
    if (Object.values(codes).some((code) => code === null)) {
      throw new Error(`a measurement carried no error code: ${JSON.stringify(codes)}`)
    }

    const line = { event: 'idempotent-authz', ...codes }
    writeEvidence('idempotent-authz-probe', line)
    console.log(JSON.stringify(line))
  } finally {
    await admin
      .deleteAcls({ filters: [topicWriteAcl] })
      .catch((e) => process.stderr.write(`could not delete the topic WRITE ACL: ${e.message}\n`))
    await admin.deleteTopics({ topics: [topic] }).catch(() => {})
    await admin.disconnect()
  }
}

if (require.main === module) {
  main().catch((e) => {
    console.error(e)
    process.exit(1)
  })
}

module.exports = { errorCodeOf }
