const fs = require('fs')
const path = require('path')
const {
  Kafka,
  logLevel,
  AclResourceTypes,
  AclOperationTypes,
  AclPermissionTypes,
  ResourcePatternTypes,
} = require('../../../index')
const { KafkaJSInvalidConfigurationError } = require('../../errors')
const { errorCodeOf } = require('../../../scripts/e2e/idempotentAuthzProbe')
const { plainTextBrokers, saslBrokers, secureRandom, writeEvidence } = require('testHelpers')
const { sleep, withTimeout } = require('../../consumer/__tests__/fixtures/liveScenario')

// Gated: it needs the ACL cluster arm. Run alone: `npm run test:e2e:idempotent-authz`.
const describeAuthz = process.env.IDEMPOTENT_AUTHZ_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'idempotent-authz'
// Named so each wait says how long it is prepared to be wrong for.
const SCENARIO_BUDGET_MS = 60000
const HOOK_BUDGET_MS = 60000
const DISCONNECT_BUDGET_MS = 15000
// An ACL reaches every broker through the metadata log, so a grant can take a moment to apply.
const GRANT_DEADLINE_MS = 30000
const GRANT_POLL_MS = 500
const CLUSTER_AUTHORIZATION_FAILED = 31
const TOPIC_AUTHORIZATION_FAILED = 29
// The principal docker-compose.acl.yml grants nothing: no IDEMPOTENT_WRITE, no topic WRITE.
const RESTRICTED_PRINCIPAL = 'User:bob'
const RESTRICTED_SASL = { mechanism: 'plain', username: 'bob', password: 'bobbob' }

/**
 * Live proof of the idempotent default's authorization rule on a cluster that enforces ACLs. A
 * broker allows `InitProducerId` to a principal with IDEMPOTENT_WRITE on the cluster or, from Kafka
 * 2.8, WRITE on any topic (KIP-679); a principal with neither cannot connect a default producer and
 * is told every remedy, while `idempotent: false` connects without asking for a producer id.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-679%3A+Producer+will+enable+the+strongest+delivery+guarantee+by+default
 * @see https://github.com/apache/kafka/blob/4.3/core/src/main/scala/kafka/server/KafkaApis.scala
 */
describeAuthz('Producer > the idempotent default on a cluster that enforces ACLs (live)', () => {
  let admin
  let restricted
  let clients
  let topic
  let topicWriteAcl

  beforeAll(async () => {
    admin = new Kafka({
      clientId: `e2e-p7-idempotent-authz-admin-${secureRandom()}`,
      brokers: plainTextBrokers(),
      logLevel: logLevel.NOTHING,
    }).admin()
    await admin.connect()
    // A run that stopped before its afterAll can leave a grant behind, and WRITE on any topic is
    // enough for InitProducerId: start from a principal that holds nothing.
    await admin.deleteAcls({
      filters: [
        {
          resourceType: AclResourceTypes.ANY,
          resourcePatternType: ResourcePatternTypes.ANY,
          principal: RESTRICTED_PRINCIPAL,
          operation: AclOperationTypes.ANY,
          permissionType: AclPermissionTypes.ANY,
        },
      ],
    })
    topic = `e2e-p7-idempotent-authz-${secureRandom()}`
    await admin.createTopics({
      topics: [{ topic, numPartitions: 1, replicationFactor: 1 }],
      waitForLeaders: true,
    })
    topicWriteAcl = {
      resourceType: AclResourceTypes.TOPIC,
      resourceName: topic,
      resourcePatternType: ResourcePatternTypes.LITERAL,
      principal: RESTRICTED_PRINCIPAL,
      host: '*',
      operation: AclOperationTypes.WRITE,
      permissionType: AclPermissionTypes.ALLOW,
    }
    restricted = new Kafka({
      clientId: `e2e-p7-idempotent-authz-${secureRandom()}`,
      brokers: saslBrokers(),
      ssl: {
        servername: 'localhost',
        rejectUnauthorized: false,
        ca: [
          fs.readFileSync(
            path.join(__dirname, '..', '..', '..', 'testHelpers', 'certs', 'cert-signed'),
            'utf-8'
          ),
        ],
      },
      sasl: RESTRICTED_SASL,
      // The refusal never clears, so the budget only bounds a failing attempt: 3 tries, ~0.3 s in all.
      retry: { retries: 2, initialRetryTime: 100, multiplier: 2, maxRetryTime: 500 },
      logLevel: logLevel.NOTHING,
    })
  }, HOOK_BUDGET_MS)

  afterAll(async () => {
    await admin.deleteAcls({ filters: [topicWriteAcl] }).catch(() => {})
    await admin.deleteTopics({ topics: [topic] }).catch(() => {})
    await admin.disconnect()
  }, HOOK_BUDGET_MS)

  beforeEach(() => {
    clients = []
  })

  afterEach(async () => {
    for (const client of clients.reverse()) {
      await withTimeout(
        client.disconnect().catch(() => {}),
        DISCONNECT_BUDGET_MS
      )
    }
  }, HOOK_BUDGET_MS)

  /** A producer of the restricted principal, disconnected after the test, with the requests it sent. */
  const restrictedProducer = (options) => {
    const producer = restricted.producer(options)
    clients.push(producer)
    const requests = []
    producer.on(producer.events.REQUEST, ({ payload }) => requests.push(payload.apiName))
    return { producer, requests }
  }

  test(
    'a default producer cannot connect, and the error names all three remedies',
    async () => {
      const { producer } = restrictedProducer()

      const error = await producer.connect().catch((e) => e)

      writeEvidence(EVIDENCE, {
        scenario: 'default-producer',
        name: error?.name,
        category: error?.category,
        code: errorCodeOf(error),
        message: error.message,
      })
      expect(error).toBeInstanceOf(KafkaJSInvalidConfigurationError)
      expect(error.category).toBe('invalidConfiguration')
      expect(errorCodeOf(error)).toBe(CLUSTER_AUTHORIZATION_FAILED)
      expect(error.message).toContain('IDEMPOTENT_WRITE')
      expect(error.message).toContain('WRITE on any topic')
      expect(error.message).toContain('idempotent: false')
    },
    SCENARIO_BUDGET_MS
  )

  test(
    'an idempotent: false producer connects without asking for a producer id, and its send is refused for the topic',
    async () => {
      const { producer, requests } = restrictedProducer({ idempotent: false })

      await producer.connect()
      const error = await producer.send({ topic, messages: [{ value: 'refused' }] }).catch((e) => e)

      writeEvidence(EVIDENCE, {
        scenario: 'non-idempotent-producer',
        requests: [...new Set(requests)],
        sendCode: errorCodeOf(error),
      })
      expect(requests).not.toContain('InitProducerId')
      expect(errorCodeOf(error)).toBe(TOPIC_AUTHORIZATION_FAILED)
    },
    SCENARIO_BUDGET_MS
  )

  test(
    'WRITE on one topic is enough for a default producer to connect',
    async () => {
      await admin.createAcls({ acl: [topicWriteAcl] })

      const deadline = Date.now() + GRANT_DEADLINE_MS
      let attempts = 0
      let connected = false
      while (!connected && Date.now() < deadline) {
        attempts += 1
        const { producer } = restrictedProducer()
        connected = await producer.connect().then(
          () => true,
          () => false
        )
        if (!connected) {
          await sleep(GRANT_POLL_MS)
        }
      }

      writeEvidence(EVIDENCE, { scenario: 'topic-write-grant', connected, attempts })
      expect(connected).toBe(true)
    },
    SCENARIO_BUDGET_MS
  )
})
