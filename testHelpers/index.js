/* eslint-disable jest/valid-describe-callback */
// Disabled to allow for higher order test functions where
// `callback` is parameterized instead of a static function
const fs = require('fs')
const { execSync } = require('child_process')
const { randomUUID } = require('crypto')
const semver = require('semver')
const crypto = require('crypto')
const Cluster = require('../src/cluster')
const Broker = require('../src/broker')
const waitFor = require('../src/utils/waitFor')
const { pollUntil } = require('../src/producer/__tests__/fixtures/bounded')
const connectionBuilder = require('../src/cluster/connectionPoolBuilder')
const ConnectionPool = require('../src/network/connectionPool')
const defaultSocketFactory = require('../src/network/socketFactory')
// Under KAFKAJS_TEST_CHAOS=1 every helper connection goes through the unarmed chaos wrapper; a
// scenario arms its sockets through `connectionOpts().socketFactory.sockets`.
const socketFactory =
  process.env.KAFKAJS_TEST_CHAOS === '1'
    ? require('./chaosSocket').createChaosSocketFactory(defaultSocketFactory())
    : defaultSocketFactory()

const {
  createLogger,
  LEVELS: { NOTHING },
} = require('../src/loggers')

const LoggerConsole = require('../src/loggers/console')
const { Kafka } = require('../index')
const {
  createFakeSocketFactory,
  socketCounts,
  respondWithFrame,
  readRequestHeader,
} = require('./fakeSocket')
const {
  evidencePath,
  evidenceLines,
  writeEvidence,
  useEvidenceRun,
  wireVersionsOf,
} = require('./e2eEvidence')
const {
  execInKafka1,
  compose,
  stopBroker,
  startBroker,
  reassignPreferredReplica,
  reassignmentIsComplete,
  waitForReassignmentToComplete,
  triggerPreferredElection,
  triggerUncleanElection,
} = require('./leaderElection')

const newLogger = (opts = {}) =>
  createLogger(Object.assign({ level: NOTHING, logCreator: LoggerConsole }, opts))

const getHost = () => 'localhost'
const secureRandom = (length = 10) =>
  `${crypto.randomBytes(length).toString('hex')}-${process.pid}-${randomUUID()}`

// Unsecured JWT (alg "none") accepted by the test broker's OAUTHBEARER unsecured validator.
const unsecuredJwt = (claims) => {
  const encode = (obj) => Buffer.from(JSON.stringify(obj)).toString('base64url')
  const now = Math.floor(Date.now() / 1000)
  return `${encode({ alg: 'none', typ: 'JWT' })}.${encode({
    iat: now,
    exp: now + 3600,
    ...claims,
  })}.`
}

const plainTextBrokers = (host = getHost()) => [`${host}:9092`, `${host}:9095`, `${host}:9098`]
const sslBrokers = (host = getHost()) => [`${host}:9093`, `${host}:9096`, `${host}:9099`]
const saslBrokers = (host = getHost()) => [`${host}:9094`, `${host}:9097`, `${host}:9100`]

const connectionOpts = (opts = {}) => ({
  socketFactory,
  clientId: `test-${secureRandom()}`,
  connectionTimeout: 3000,
  requestTimeout: 30000,
  logger: newLogger(),
  host: getHost(),
  port: 9092,
  ...opts,
})

const sslConnectionOpts = () =>
  Object.assign(connectionOpts(), {
    port: 9093,
    ssl: {
      servername: 'localhost',
      rejectUnauthorized: false,
      ca: [fs.readFileSync('./testHelpers/certs/cert-signed', 'utf-8')],
    },
  })

const saslConnectionOpts = () =>
  Object.assign(sslConnectionOpts(), {
    port: 9094,
    sasl: {
      mechanism: 'plain',
      username: 'test',
      password: 'testtest',
    },
  })

const saslWrongConnectionOpts = () =>
  Object.assign(sslConnectionOpts(), {
    port: 9094,
    sasl: {
      mechanism: 'plain',
      username: 'wrong',
      password: 'wrong',
    },
  })

const saslSCRAM256ConnectionOpts = () =>
  Object.assign(sslConnectionOpts(), {
    port: 9094,
    sasl: {
      mechanism: 'scram-sha-256',
      username: 'testscram',
      password: 'testtestscram=256',
    },
  })

const saslSCRAM256WrongConnectionOpts = () =>
  Object.assign(sslConnectionOpts(), {
    port: 9094,
    sasl: {
      mechanism: 'scram-sha-256',
      username: 'wrong',
      password: 'wrong',
    },
  })

const saslSCRAM512ConnectionOpts = () =>
  Object.assign(sslConnectionOpts(), {
    port: 9094,
    sasl: {
      mechanism: 'scram-sha-512',
      username: 'testscram',
      password: 'testtestscram=512',
    },
  })

const saslSCRAM512WrongConnectionOpts = () =>
  Object.assign(sslConnectionOpts(), {
    port: 9094,
    sasl: {
      mechanism: 'scram-sha-512',
      username: 'wrong',
      password: 'wrong',
    },
  })

const saslOAuthBearerConnectionOpts = () =>
  Object.assign(sslConnectionOpts(), {
    port: 9094,
    sasl: {
      mechanism: 'oauthbearer',
      oauthBearerProvider: () => {
        const token = unsecuredJwt({ sub: 'test' })

        return {
          value: token,
        }
      },
    },
  })

/**
 * List of the possible SASL setups.
 * OAUTHBEARER must be enabled as a special case.
 */
const saslEntries = []
if (process.env.OAUTHBEARER_ENABLED !== '1') {
  saslEntries.push({
    name: 'PLAIN',
    opts: saslConnectionOpts,
    wrongOpts: saslWrongConnectionOpts,
    expectedErr: /SASL PLAIN authentication failed/,
  })

  saslEntries.push({
    name: 'SCRAM 256',
    opts: saslSCRAM256ConnectionOpts,
    wrongOpts: saslSCRAM256WrongConnectionOpts,
    expectedErr: /SASL SCRAM SHA256 authentication failed/,
  })

  saslEntries.push({
    name: 'SCRAM 512',
    opts: saslSCRAM512ConnectionOpts,
    wrongOpts: saslSCRAM512WrongConnectionOpts,
    expectedErr: /SASL SCRAM SHA512 authentication failed/,
  })
} else {
  saslEntries.push({
    name: 'OAUTHBEARER',
    opts: saslOAuthBearerConnectionOpts,
  })
}

const createConnectionPool = (opts = {}) =>
  new ConnectionPool(Object.assign(connectionOpts(), opts))

const createConnectionBuilder = (opts = {}, brokers = plainTextBrokers()) => {
  return connectionBuilder({
    socketFactory,
    logger: newLogger(),
    brokers,
    connectionTimeout: 1000,
    ...connectionOpts(),
    ...opts,
  })
}

const createCluster = (opts = {}, brokers = plainTextBrokers()) =>
  new Cluster(Object.assign(connectionOpts(), opts, { brokers }))

const createModPartitioner =
  () =>
  ({ partitionMetadata, message }) => {
    const numPartitions = partitionMetadata.length
    const key = parseInt(message.key.replace(/[^\d]/g, ''), 10)
    return ((key || 0) % 3) % numPartitions
  }

const testWaitFor = async (fn, opts = {}) => waitFor(fn, { ignoreTimeout: true, ...opts })

// KRaft commits CreateTopics on the controller and propagates the metadata asynchronously, so a
// broker that has not caught up yet answers UNKNOWN_TOPIC_OR_PARTITION (Kafka 4.x) or
// LEADER_NOT_AVAILABLE (Kafka <= 3.x). Test setup retries both so a spec never races the controller.
const TOPIC_NOT_READY_ERRORS = ['UNKNOWN_TOPIC_OR_PARTITION', 'LEADER_NOT_AVAILABLE']

/**
 * @param {import("../types").KafkaJSError} errorType
 * @param {() => Promise<T>} fn
 * @returns {Promise<T>}
 * @template T
 */
const retryProtocol = (errorType, fn) =>
  waitFor(
    async () => {
      try {
        return await fn()
      } catch (e) {
        // Also retry the KRaft topic-propagation errors: any request against a broker that has not
        // caught up with the controller yet can answer these regardless of what `errorType` is.
        if (e.type !== errorType && !TOPIC_NOT_READY_ERRORS.includes(e.type)) {
          throw e
        }
        return false
      }
    },
    { ignoreTimeout: true }
  )

const waitForMessages = (buffer, { number = 1, delay = 50 } = {}) =>
  waitFor(() => (buffer.length >= number ? buffer : false), { delay, ignoreTimeout: true })

const waitForNextEvent = (consumer, eventName, { maxWait = 10000 } = {}) =>
  new Promise((resolve, reject) => {
    const timeoutId = setTimeout(
      () => reject(new Error(`Timeout waiting for '${eventName}'`)),
      maxWait
    )
    consumer.on(eventName, (event) => {
      clearTimeout(timeoutId)
      resolve(event)
    })
    consumer.on(consumer.events.CRASH, (event) => {
      clearTimeout(timeoutId)
      reject(event.payload.error)
    })
  })

const waitForConsumerToJoinGroup = (consumer, { maxWait = 10000, label = '' } = {}) =>
  new Promise((resolve, reject) => {
    const timeoutId = setTimeout(() => {
      consumer.disconnect().then(() => {
        reject(new Error(`Timeout ${label}`.trim()))
      })
    }, maxWait)
    consumer.on(consumer.events.GROUP_JOIN, (event) => {
      clearTimeout(timeoutId)
      resolve(event)
    })
    consumer.on(consumer.events.CRASH, (event) => {
      clearTimeout(timeoutId)
      consumer.disconnect().then(() => {
        reject(event.payload.error)
      })
    })
  })

const CREATE_TOPIC_ATTEMPTS = 10
const CREATE_TOPIC_RETRY_MS = 200
const TOPIC_PROPAGATION_DELAY_MS = 100
// Bounds both waits of createTopic(): the controller's leader wait inside admin.createTopics and
// the all-brokers metadata wait after it. 10 s timed out in setup on a 3-node KRaft cluster under
// host load (load average about 7 on 12 cores). A timeout is not retried, so a hook spends at most
// two of these, 60 s, inside the 90 s test timeout. The same value is the CreateTopics request's
// wire timeoutMs, a third bound that only a controller slower than 30 s to commit would reach.
const TOPIC_PROPAGATION_MAX_WAIT_MS = 30000

// One connection per broker per test file, not per createTopic() call: reconnecting on every
// fixture topic floods the brokers with handshakes. `testHelpers/setup.js` closes them in afterAll.
let propagationBrokers = null

const getPropagationBrokers = () => {
  if (!propagationBrokers) {
    propagationBrokers = plainTextBrokers().map((address) => {
      const [host, port] = address.split(':')
      return new Broker({
        // No idle reaper: these sockets live for the whole file, and its re-armed timer would
        // count against a case that tracks the timers it leaves behind.
        connectionPool: createConnectionPool({ host, port: Number(port), connectionsMaxIdle: 0 }),
        logger: newLogger(),
        allowAutoTopicCreation: false,
      })
    })
  }

  return propagationBrokers
}

const disconnectPropagationBrokers = async () => {
  const brokers = propagationBrokers
  propagationBrokers = null
  brokers && (await Promise.all(brokers.map((broker) => broker.disconnect())))
}

/**
 * Waits until *every* broker serves metadata for `topic` with a leader for each partition.
 *
 * `admin.createTopics({ waitForLeaders: true })` only checks the controller, but tests connect to
 * an arbitrary broker, so on a slow runner they can still hit one that has not caught up.
 *
 * @param {string} topic
 * @param {number} partitions
 * @param {object} [options]
 * @param {AbortSignal} [options.signal] cancels reads and polling under the same 30-second bound
 * @param {object[]} [options.brokers] caller-owned brokers; otherwise uses the file's shared brokers
 * @returns {Promise<void>}
 * @throws {Error} permanent metadata error, cancellation or propagation timeout
 */
const waitForTopicPropagation = async (topic, partitions, { signal, brokers } = {}) => {
  signal?.throwIfAborted()
  const servesTopic = async (broker, scope) => {
    scope?.throwIfAborted()
    const options = scope ? { guard: () => (scope.aborted ? scope.reason : null) } : undefined
    let response
    try {
      await broker.connect(options)
      scope?.throwIfAborted()
      response = await broker.metadata([topic], options)
    } catch (e) {
      scope?.throwIfAborted()
      if (TOPIC_NOT_READY_ERRORS.includes(e.type)) {
        return false
      }
      throw e
    }

    const topicMetadata = response.topicMetadata.find(({ topic: name }) => name === topic)
    return Boolean(
      topicMetadata &&
      topicMetadata.partitionMetadata.length === partitions &&
      topicMetadata.partitionMetadata.every(({ leader }) => leader >= 0)
    )
  }

  const targets = brokers || getPropagationBrokers()
  const check = async (scope) =>
    (await Promise.all(targets.map((broker) => servesTopic(broker, scope)))).every(Boolean)
  const label = `Timed out waiting for all brokers to serve metadata for topic "${topic}"`
  if (signal) {
    await pollUntil(signal, TOPIC_PROPAGATION_MAX_WAIT_MS, check, {
      delayMs: TOPIC_PROPAGATION_DELAY_MS,
      label,
    })
  } else {
    await waitFor(() => check(), {
      delay: TOPIC_PROPAGATION_DELAY_MS,
      maxWait: TOPIC_PROPAGATION_MAX_WAIT_MS,
      timeoutMessage: label,
    })
  }
}

const createTopic = async ({ topic, partitions = 1, replicas = 1, config = [] }) => {
  const kafka = new Kafka({ clientId: 'testHelpers', brokers: [`${getHost()}:9092`] })
  const admin = kafka.admin()

  try {
    await admin.connect()

    for (let attempt = 1; ; attempt++) {
      try {
        await admin.createTopics({
          waitForLeaders: true,
          timeout: TOPIC_PROPAGATION_MAX_WAIT_MS,
          topics: [
            {
              topic,
              numPartitions: partitions,
              replicationFactor: replicas,
              configEntries: config,
            },
          ],
        })
        await waitForTopicPropagation(topic, partitions)
        return
      } catch (e) {
        if (!TOPIC_NOT_READY_ERRORS.includes(e.type) || attempt >= CREATE_TOPIC_ATTEMPTS) {
          throw e
        }

        await new Promise((resolve) => setTimeout(resolve, CREATE_TOPIC_RETRY_MS))
      }
    }
  } finally {
    admin && (await admin.disconnect())
  }
}

const addPartitions = async ({ topic, partitions }) => {
  const cmd = `TOPIC=${topic} PARTITIONS=${partitions} ./scripts/addPartitions.sh`
  const cluster = createCluster()

  await cluster.connect()
  await cluster.addTargetTopic(topic)

  execSync(cmd, { stdio: ['ignore', 'pipe', 'pipe'], maxBuffer: 10 * 1024 * 1024 })

  waitFor(async () => {
    await cluster.refreshMetadata()
    const partitionMetadata = cluster.findTopicPartitionMetadata(topic)
    return partitionMetadata.length === partitions
  })
}

const testIfKafkaVersion = (version, versionComparator) => {
  const scopedTest = (description, callback, testFn = test) => {
    return versionComparator(semver.coerce(process.env.KAFKA_VERSION), semver.coerce(version))
      ? testFn(description, callback)
      : test.skip(description, callback)
  }

  scopedTest.only = (description, callback) => scopedTest(description, callback, test.only)

  return scopedTest
}

const testIfKafkaVersionLTE = (version) => testIfKafkaVersion(version, semver.lte)
const testIfKafkaVersionGTE = (version) => testIfKafkaVersion(version, semver.gte)

const testIfKafkaAtMost_0_10 = testIfKafkaVersionLTE('0.10')
const testIfKafkaAtLeast_0_11 = testIfKafkaVersionGTE('0.11')
const testIfKafkaAtLeast_1_1_0 = testIfKafkaVersionGTE('1.1')

const flakyTest = (description, callback, testFn = test) =>
  testFn(`[flaky] ${description}`, callback)
flakyTest.skip = (description, callback) => flakyTest(description, callback, test.skip)
flakyTest.only = (description, callback) => flakyTest(description, callback, test.only)
const describeIfEnv =
  (key, value) =>
  (description, callback, describeFn = describe) => {
    return value === process.env[key]
      ? describeFn(description, callback)
      : describe.skip(description, callback)
  }

const describeIfNotEnv =
  (key, value) =>
  (description, callback, describeFn = describe) => {
    return value !== process.env[key]
      ? describeFn(description, callback)
      : describe.skip(description, callback)
  }

/**
 * Conditional describes for SASL OAUTHBEARER.
 * OAUTHBEARER must be enabled as a special case as current Kafka impl
 * doesn't allow it to be enabled aside of other SASL mechanisms.
 */
const describeIfOauthbearerEnabled = describeIfEnv('OAUTHBEARER_ENABLED', '1')
const describeIfOauthbearerDisabled = describeIfNotEnv('OAUTHBEARER_ENABLED', '1')

const unsupportedVersionResponse = () => Buffer.from({ type: 'Buffer', data: [0, 35, 0, 0, 0, 0] })
const unsupportedVersionResponseWithTimeout = () =>
  Buffer.from({ type: 'Buffer', data: [0, 0, 0, 0, 0, 35] })

const generateMessages = (options) => {
  const { prefix, number = 100 } = options || {}
  const prefixOrEmpty = prefix ? `-${prefix}` : ''

  return Array(number)
    .fill()
    .map((v, i) => {
      const value = secureRandom()
      return {
        key: `key${prefixOrEmpty}-${i}-${value}`,
        value: `value${prefixOrEmpty}-${i}-${value}`,
      }
    })
}

module.exports = {
  secureRandom,
  connectionOpts,
  sslConnectionOpts,
  saslConnectionOpts,
  saslSCRAM256ConnectionOpts,
  saslSCRAM512ConnectionOpts,
  saslOAuthBearerConnectionOpts,
  saslEntries,
  createConnectionPool,
  createConnectionBuilder,
  createCluster,
  createModPartitioner,
  plainTextBrokers,
  sslBrokers,
  saslBrokers,
  newLogger,
  retryProtocol,
  createTopic,
  waitForTopicPropagation,
  disconnectPropagationBrokers,
  waitFor: testWaitFor,
  waitForMessages,
  waitForNextEvent,
  waitForConsumerToJoinGroup,
  testIfKafkaAtMost_0_10,
  testIfKafkaAtLeast_0_11,
  testIfKafkaAtLeast_1_1_0,
  flakyTest,
  describeIfOauthbearerEnabled,
  describeIfOauthbearerDisabled,
  addPartitions,
  unsupportedVersionResponse,
  generateMessages,
  unsupportedVersionResponseWithTimeout,
  createFakeSocketFactory,
  socketCounts,
  respondWithFrame,
  readRequestHeader,
  evidencePath,
  evidenceLines,
  writeEvidence,
  useEvidenceRun,
  wireVersionsOf,
  execInKafka1,
  compose,
  stopBroker,
  startBroker,
  reassignPreferredReplica,
  reassignmentIsComplete,
  waitForReassignmentToComplete,
  triggerPreferredElection,
  triggerUncleanElection,
}

// Loaded on first use, so a spec that never schedules or probes does not pay for requiring them.
const LAZY = {
  Gate: './schedule',
  Schedule: './schedule',
  interleavings: './schedule',
  mulberry32: './schedule',
  createResourceProbe: './resourceProbe',
  lanes: './lane',
  currentLane: './lane',
}
for (const [name, file] of Object.entries(LAZY)) {
  Object.defineProperty(module.exports, name, { enumerable: true, get: () => require(file)[name] })
}
