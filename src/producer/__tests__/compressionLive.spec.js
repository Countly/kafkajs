const path = require('path')
const { spawnSync } = require('child_process')
const { Kafka, CompressionTypes, logLevel } = require('../../../index')
const Broker = require('../../broker')
const compression = require('../../protocol/message/compression')
const lz4 = require('../../protocol/message/compression/lz4')
const { compressedBatch } = require('../../protocol/message/compression/fixtures/compressedBatch')
const {
  assertBaselineArtifact,
  assertBaselineDiagnostic,
  assertFinalArtifact,
  expectedRecords,
  loadCapture,
  publicRecord,
  restamp,
  sha256,
} = require('../../protocol/message/compression/fixtures/librdkafkaCapture')
const {
  validateCapture,
  verifyStoredBody,
} = require('../../protocol/message/compression/fixtures/rawRecordOracle')
const { readLog } = require('../../../testHelpers/rawLog')
const {
  createConnectionPool,
  createTopic,
  newLogger,
  plainTextBrokers,
  secureRandom,
  wireVersionsOf,
  writeEvidence,
} = require('testHelpers')
const { sleep, waitForCondition } = require('../../consumer/__tests__/fixtures/liveScenario')
const {
  WireTap,
  createLiveSockets,
  rawProduceRequest,
  repliesOf,
  scanBatches,
} = require('./fixtures/liveWire')
const {
  CleanupStack,
  ProducerProbe,
  describeError,
  leaderOf,
  readData,
  topicConfig,
} = require('./fixtures/liveProbe')
const {
  CASE_JOIN_MS,
  NEVER,
  bounded,
  caseTimeout,
  liveCase,
  liveRun,
} = require('./fixtures/bounded')

// Gated: it produces and consumes through every built-in codec, with the Java client's help. Run
// alone: `npm run test:e2e:compression`.
const describeCompression = process.env.COMPRESSION_E2E === '1' ? describe : describe.skip

const ROOT = path.join(__dirname, '..', '..', '..')
const RUN_SH = path.join(ROOT, 'scripts', 'e2e', 'javaConsumer', 'run.sh')

const EVIDENCE = 'compression'
const CASE_BUDGET_MS = 120000
const HOOK_BUDGET_MS = 120000
const QUICK_BUDGET_MS = 5000
const REQUEST_TIMEOUT_MS = 10000
const LINGER_MS = 5
const DELIVERY_TIMEOUT_MS = 30000
const MAX_REQUEST_SIZE = 1048576
const DISCONNECT_BUDGET_MS = 15000
const JAVA_READ_MS = 10000
const JAVA_PRODUCE_SECONDS = 60
const JAVA_READ_SECONDS = 30
const QUIET_MS = 1500
const WAIT_BUDGET = { attempts: 120, delay: 250 } // 30 s
const CORPUS_SIZE = 20
const SUFFIX = 'abcdefgh'.repeat(512) // 4096 compressible bytes

/** The codecs of the matrix; LZ4 runs only where its optional binding is installed. */
const CODECS = Object.freeze([
  { name: 'gzip', type: CompressionTypes.GZIP, id: 1, supported: true },
  { name: 'snappy', type: CompressionTypes.Snappy, id: 2, supported: true },
  { name: 'lz4', type: CompressionTypes.LZ4, id: 3, supported: lz4.isSupported },
  { name: 'zstd', type: CompressionTypes.ZSTD, id: 4, supported: true },
])

/** The settings every JS producer here declares, on top of the codec each send names. */
const SETTINGS = Object.freeze({
  requestTimeout: REQUEST_TIMEOUT_MS,
  enforceRequestTimeout: true,
  idempotent: true,
  linger: LINGER_MS,
  deliveryTimeout: DELIVERY_TIMEOUT_MS,
  acks: -1,
})

const REPLAY = Object.freeze({
  log: 'every corpus record exactly once',
  offsets: 'only explicitly handled commits; none in these cases',
  handlers: 'exactly once within each uninterrupted case',
})

/**
 * The ordered UTF-8 corpus of one source: twenty pairs carrying a 4096-byte compressible suffix,
 * and for the JS-to-JS case a null and an empty key.
 * @param {string} source the prefix that tells this corpus from every other
 * @param {boolean} [special=false] adds the null-key and empty-key records
 * @returns {Array<{ key: string|null, value: string }>}
 */
const corpusOf = (source, special = false) => [
  ...Array.from({ length: CORPUS_SIZE }, (_, index) => ({
    key: `${source}-${index}`,
    value: `${source}-${index}-héllo-☃-${SUFFIX}`,
  })),
  ...(special
    ? [
        { key: null, value: `${source}-null-key-${SUFFIX}` },
        { key: '', value: `${source}-empty-key-${SUFFIX}` },
      ]
    : []),
]

/**
 * @param {Buffer|null} bytes a key or value as the consumer was given it
 * @returns {string|null}
 */
const textOf = (bytes) => (bytes === null ? null : bytes.toString())

/**
 * One scenario's resources: topic names, clients and a cleanup that closes them together.
 */
class Rig {
  /**
   * @param {string} name the direction, for names
   * @param {{ name: string }} codec
   */
  constructor(name, codec) {
    this.id = secureRandom()
    this.codec = codec
    this.brokers = plainTextBrokers()
    this.clientId = `e2e-p9b-compression-${name}-${codec.name}-${this.id}`
    this.topic = `e2e-p9b-compression-${name}-${codec.name}-${this.id}`
    this.cleanup = new CleanupStack()
    this.requests = []
    this.sockets = createLiveSockets()
  }

  /** @returns {object} the options of every client a rig opens */
  get clientOptions() {
    return { brokers: this.brokers, logLevel: logLevel.NOTHING }
  }

  /**
   * Creates the one-partition topic and the admin client.
   * @returns {Promise<void>}
   */
  async start() {
    await createTopic({ topic: this.topic, partitions: 1, replicas: 3 })
    this.admin = new Kafka(this.clientOptions).admin()
    await this.admin.connect()
    this.cleanup.add('admin', () => this.admin.disconnect())
  }

  /**
   * Connects a JS producer with the declared settings and installs the admission probe on it.
   * @returns {Promise<void>}
   */
  async startProducer() {
    const kafka = new Kafka({
      ...this.clientOptions,
      clientId: this.clientId,
      socketFactory: this.sockets.socketFactory,
      requestTimeout: REQUEST_TIMEOUT_MS,
      enforceRequestTimeout: true,
    })
    this.producer = kafka.producer({
      idempotent: true,
      linger: LINGER_MS,
      deliveryTimeout: DELIVERY_TIMEOUT_MS,
      maxRequestSize: MAX_REQUEST_SIZE,
    })
    this.producer.on(this.producer.events.REQUEST, ({ payload }) => this.requests.push(payload))
    this.probe = new ProducerProbe({
      producer: this.producer,
      frames: this.sockets.frames,
      clientId: this.clientId,
      maxRequestSize: MAX_REQUEST_SIZE,
    }).install()
    this.cleanup.add('admission probe', () => this.probe.restore())
    this.cleanup.add('resources after disconnect', () => this.probe.settled())
    await this.producer.connect()
    this.cleanup.add('producer', () => this.producer.disconnect({ timeout: DISCONNECT_BUDGET_MS }))
  }

  /**
   * Sends the corpus with the rig's codec, as the first send of the producer.
   * @param {Array<{ key: string|null, value: string }>} pairs
   * @param {AbortSignal} scope the case's, which fences the discovery of the batch owners
   * @returns {Promise<void>}
   */
  async send(pairs, scope) {
    await this.probe.capture(
      () =>
        this.producer.send({
          topic: this.topic,
          messages: pairs,
          acks: -1,
          compression: this.codec.type,
        }),
      { signal: scope }
    )
  }

  /**
   * Reads the topic through a JS consumer with manual commits.
   * @param {number} count how many records to wait for
   * @returns {Promise<Array<{ key: string|null, value: string, offset: string }>>} what it was
   *   given, after it stayed quiet: more than `count` is visible as an extra
   */
  async consume(count) {
    const received = []
    const consumer = new Kafka(this.clientOptions).consumer({
      groupId: `e2e-p9b-compression-reader-${this.id}`,
      maxWaitTimeInMs: 500,
    })
    this.cleanup.add('consumer', () => consumer.disconnect())
    await consumer.connect()
    await consumer.subscribe({ topic: this.topic, fromBeginning: true })
    await consumer.run({
      autoCommit: false,
      eachMessage: async ({ message }) => {
        received.push({
          key: textOf(message.key),
          value: textOf(message.value),
          offset: message.offset,
        })
      },
    })
    await waitForCondition(() => received.length >= count, WAIT_BUDGET)
    await sleep(QUIET_MS)
    await consumer.disconnect()
    return received
  }

  /**
   * @returns {Promise<string>} the offset the broker holds for the reader's group, '-1' when none
   */
  async committed() {
    const [{ partitions }] = await this.admin.fetchOffsets({
      groupId: `e2e-p9b-compression-reader-${this.id}`,
      topics: [this.topic],
    })
    return String(partitions[0].offset)
  }

  /**
   * The record batches the broker stored, with the compression their attributes name.
   * @returns {Promise<Array<{ codec: string, records: number, baseOffset: string }>>}
   */
  async storedBatches() {
    const tap = new WireTap()
    await readLog({ brokers: this.brokers, topic: this.topic, open: tap.open })
    return tap
      .batches()
      .filter((batch) => !batch.control)
      .map(({ codecName, count, baseOffset }) => ({ codec: codecName, records: count, baseOffset }))
  }

  /**
   * Writes the case's evidence line.
   * @param {string} direction
   * @param {object} fields
   * @returns {void}
   */
  record(direction, fields) {
    writeEvidence(EVIDENCE, {
      scenario: 'compression',
      case: direction,
      codec: this.codec.name,
      config: { ...SETTINGS, compression: this.codec.name },
      replay: REPLAY,
      wireVersions: wireVersionsOf(this.requests),
      wire: { frames: this.sockets.frames.trace(), replies: repliesOf(this.requests) },
      ...fields,
    })
  }

  /**
   * Notes that the codec's optional binding is absent: reported, not counted as evidence.
   * @param {string} direction
   * @returns {boolean} true when the case has nothing to run
   */
  absent(direction) {
    if (this.codec.supported) return false
    this.record(direction, { skipped: `the ${this.codec.name} binding is not installed` })
    expect(this.codec.supported).toBe(false)
    return true
  }

  /**
   * Closes the rig after the case, whatever it did.
   * @template T
   * @param {() => Promise<T>} work
   * @returns {Promise<T>}
   */
  run(work) {
    return this.cleanup.run(work)
  }
}

/**
 * Runs the Java member in the foreground, offline, on the controller's cached classpath.
 * @param {string[]} args `run.sh` arguments
 * @param {number} seconds the child's own deadline
 * @param {string} label what the child does, for a failure message
 * @returns {{ events: object[], status: number }} the JSON lines it printed and its exit code
 * @throws {Error} when it did not exit 0 or printed an error line: a missing cached class or jar
 *   is a prerequisite failure, never a skip
 */
const runJava = (args, seconds, label) => {
  const result = spawnSync('timeout', [`${seconds}s`, RUN_SH, ...args], {
    cwd: ROOT,
    encoding: 'utf8',
    maxBuffer: 64 * 1024 * 1024,
    env: {
      ...process.env,
      JAVA_E2E_OFFLINE: '1',
      KAFKA_CLIENTS_VERSION: process.env.KAFKA_CLIENTS_VERSION || '4.3.1',
      JAVA_TOOL_OPTIONS:
        '-Dfile.encoding=UTF-8 -Dstdout.encoding=UTF-8 -Dsun.stdout.encoding=UTF-8',
    },
  })
  const events = (result.stdout || '')
    .split('\n')
    .filter((line) => line.startsWith('{'))
    .flatMap((line) => {
      try {
        return [JSON.parse(line)]
      } catch {
        return []
      }
    })
  const errors = events.filter(({ event }) => event === 'error')
  if (result.status !== 0 || errors.length > 0) {
    throw new Error(
      `The Java ${label} exited ${result.status} with ${JSON.stringify(errors)}: ` +
        `${String(result.stderr).slice(-1500)}`
    )
  }
  return { events, status: result.status }
}

/**
 * @param {Array<{ key: string|null, value: string }>} pairs
 * @param {Array<{ key: string|null, value: string, offset: string }>} received
 * @returns {boolean} whether `received` is `pairs` in order, key by key and value by value, at
 *   distinct ascending offsets
 */
const sameCorpus = (pairs, received) =>
  received.length === pairs.length &&
  received.every(
    (record, index) =>
      record.key === pairs[index].key &&
      record.value === pairs[index].value &&
      (index === 0 || BigInt(record.offset) > BigInt(received[index - 1].offset))
  )

/**
 * Live proof that each built-in codec interoperates, in the three directions the Java client
 * gives: this client's records read back by this client and by the Java client, and the Java
 * client's by this client. Each case reads the broker's own batch attributes to show the requested
 * codec was used, and compares keys, values, order and count, never the count alone. The
 * authentic librdkafka Snappy capture is replayed raw, its compressed record bytes untouched, and
 * read through the public consumer; it is a prerequisite that is never synthesized.
 *
 * Replay sets: every corpus record once in the log; offsets only as explicitly handled, none here;
 * handler delivery exactly once within each uninterrupted case.
 * @see https://kafka.apache.org/43/documentation/#producerconfigs_compression.type
 */
describeCompression('Producer > every codec across clients (live)', () => {
  test(
    'the declared producer settings are accepted without connecting',
    liveCase(QUICK_BUDGET_MS, () => {
      const kafka = new Kafka({
        brokers: plainTextBrokers(),
        logLevel: logLevel.NOTHING,
        requestTimeout: REQUEST_TIMEOUT_MS,
        enforceRequestTimeout: true,
      })

      expect(DELIVERY_TIMEOUT_MS).toBeGreaterThanOrEqual(LINGER_MS + REQUEST_TIMEOUT_MS)
      expect(() =>
        kafka.producer({
          idempotent: true,
          linger: LINGER_MS,
          deliveryTimeout: DELIVERY_TIMEOUT_MS,
        })
      ).not.toThrow()
    }),
    caseTimeout(QUICK_BUDGET_MS)
  )

  describe.each(CODECS.map((codec) => [codec.name, codec]))('%s', (_, codec) => {
    test(
      'JS producer to JS consumer: keys, values, order and count, and the broker holds the codec',
      liveCase(CASE_BUDGET_MS, async (scope) => {
        const rig = new Rig('js-js', codec)
        if (rig.absent('js-to-js')) return
        const pairs = corpusOf(`js-${codec.name}`, true)
        await rig.run(async () => {
          await rig.start()
          await rig.startProducer()
          await rig.send(pairs, scope)
          const received = await rig.consume(pairs.length)
          const stored = await rig.storedBatches()
          const raw = await readData(rig.brokers, rig.topic)
          const committed = await rig.committed()
          const closing = await rig.probe.closing({
            topic: `e2e-p9b-compression-probe-js-js-${codec.name}-${rig.id}`,
            budgetMs: DELIVERY_TIMEOUT_MS,
          })

          rig.record('js-to-js', {
            received: received.length,
            stored,
            raw: raw.length,
            committed,
            closing,
          })

          expect(sameCorpus(pairs, received)).toBe(true)
          expect(raw).toHaveLength(pairs.length)
          expect(stored.length).toBeGreaterThanOrEqual(1)
          expect(stored.every(({ codec: used }) => used === codec.name)).toBe(true)
          expect(stored.reduce((sum, { records }) => sum + records, 0)).toBe(pairs.length)
          expect(committed).toBe('-1')
        })
      }),
      caseTimeout(CASE_BUDGET_MS)
    )

    test(
      'JS producer to the Java read_committed consumer: every key and value, in order, once',
      liveCase(CASE_BUDGET_MS, async (scope) => {
        const rig = new Rig('js-java', codec)
        if (rig.absent('js-to-java')) return
        const pairs = corpusOf(`js-${codec.name}`).map(({ key, value }) => ({ key, value }))
        await rig.run(async () => {
          await rig.start()
          await rig.startProducer()
          await rig.send(pairs, scope)
          const stored = await rig.storedBatches()
          const java = runJava(
            [
              '--role',
              'read-committed-consumer',
              rig.brokers.join(','),
              `e2e-p9b-compression-java-reader-${rig.id}`,
              rig.topic,
              String(JAVA_READ_MS),
            ],
            JAVA_READ_SECONDS,
            'consumer'
          )
          const records = java.events.filter(({ event }) => event === 'record')
          const done = java.events.find(({ event }) => event === 'done')
          const closing = await rig.probe.closing({
            topic: `e2e-p9b-compression-probe-js-java-${codec.name}-${rig.id}`,
            budgetMs: DELIVERY_TIMEOUT_MS,
          })

          rig.record('js-to-java', {
            stored,
            java: { status: java.status, done, records: records.length },
            closing,
          })

          expect(stored.every(({ codec: used }) => used === codec.name)).toBe(true)
          expect(done).toMatchObject({ event: 'done', records: pairs.length })
          expect(records.map(({ key, value }) => ({ key, value }))).toEqual(pairs)
          expect(
            new Set(records.map(({ partition, offset }) => `${partition}:${offset}`)).size
          ).toBe(pairs.length)
        })
      }),
      caseTimeout(CASE_BUDGET_MS)
    )

    test(
      'Java compressed producer to JS consumer: every key and value, in order, once',
      liveCase(CASE_BUDGET_MS, async () => {
        const rig = new Rig('java-js', codec)
        if (rig.absent('java-to-js')) return
        const source = `java-${codec.name}`
        const pairs = Array.from({ length: CORPUS_SIZE }, (_, index) => ({
          key: `${source}-${index}`,
          value: `${source}-${index}`,
        }))
        await rig.run(async () => {
          await rig.start()
          const java = runJava(
            [
              '--role',
              'compressed-producer',
              '--compression',
              codec.name,
              rig.brokers.join(','),
              source,
              rig.topic,
              String(CORPUS_SIZE),
            ],
            JAVA_PRODUCE_SECONDS,
            'producer'
          )
          const done = java.events.find(({ event }) => event === 'done')
          const received = await rig.consume(pairs.length)
          const stored = await rig.storedBatches()

          rig.record('java-to-js', {
            java: { status: java.status, done },
            received: received.length,
            stored,
          })

          expect(done).toEqual({ event: 'done', records: CORPUS_SIZE, compression: codec.name })
          expect(sameCorpus(pairs, received)).toBe(true)
          expect(stored.length).toBeGreaterThanOrEqual(1)
          expect(stored.every(({ codec: used }) => used === codec.name)).toBe(true)
        })
      }),
      caseTimeout(CASE_BUDGET_MS)
    )
  })

  describe('the authentic librdkafka Snappy capture', () => {
    const outcome = { records: [], error: null }
    let expected

    // The baseline stage sets COMPRESSION_BASELINE_DIAGNOSTIC and COMPRESSION_BASELINE_ARTIFACT and
    // runs in the no-built-in artifact; the final stage sets neither. Setup failures stay outside the test.
    beforeAll(async () => {
      liveRun.assertClean()
      return bounded(
        NEVER,
        HOOK_BUDGET_MS - CASE_JOIN_MS,
        async () => {
          const capture = loadCapture()
          const validated = validateCapture(capture)
          const { manifest } = capture
          const pinned = process.env.COMPRESSION_BASELINE_DIAGNOSTIC
          const artifactVersion = process.env.COMPRESSION_BASELINE_ARTIFACT
          if (pinned && !artifactVersion) {
            throw new Error('A baseline stage names its artifact in COMPRESSION_BASELINE_ARTIFACT')
          }
          const artifact = pinned
            ? assertBaselineArtifact({ root: ROOT, version: artifactVersion, compression })
            : null
          if (!pinned) assertFinalArtifact({ compression })
          const id = secureRandom()
          const brokers = plainTextBrokers()
          const topic = `e2e-p9b-compression-librdkafka-${id}`
          const controlTopic = `e2e-p9b-compression-librdkafka-control-${id}`
          const cleanup = new CleanupStack()
          const control = []

          await cleanup.run(async () => {
            await createTopic({
              topic,
              partitions: 1,
              replicas: 3,
              config: [{ name: 'message.timestamp.type', value: 'CreateTime' }],
            })
            expect(await topicConfig(brokers, topic, 'message.timestamp.type')).toBe('CreateTime')
            await createTopic({ topic: controlTopic, partitions: 1, replicas: 3 })

            const admin = new Kafka({ brokers, logLevel: logLevel.NOTHING }).admin()
            await admin.connect()
            cleanup.add('admin', () => admin.disconnect())

            /** Writes one raw batch to the partition's leader and returns the offset it was given. */
            const replay = async (bytes, target) => {
              const leader = await leaderOf(admin, target)
              const broker = new Broker({
                connectionPool: createConnectionPool({ host: leader.host, port: leader.port }),
                logger: newLogger(),
              })
              await broker.connect()
              cleanup.add(`raw producer of ${target}`, () => broker.disconnect())
              const protocolRequest = rawProduceRequest(broker, bytes, target, 0)
              const connection = await broker.connectionPool.getConnectionByRequest(protocolRequest)
              const response = await broker.connectionPool.send(
                protocolRequest,
                connection,
                null,
                true
              )
              const [{ errorCode, baseOffset }] = response.topics[0].partitions
              expect(errorCode).toBe(0)
              return BigInt(baseOffset)
            }

            const { bytes, delta } = restamp(capture.batch)
            const [replayed] = scanBatches(bytes)
            expect(replayed.body.equals(validated.body)).toBe(true)
            const controlBytes = restamp(await compressedBatch({ codec: 'gzip', records: 1 })).bytes

            const tap = new WireTap()
            const consumer = new Kafka({
              brokers,
              logLevel: logLevel.NOTHING,
              socketFactory: tap.socketFactory,
            }).consumer({ groupId: `e2e-p9b-compression-librdkafka-${id}`, maxWaitTimeInMs: 500 })
            cleanup.add('consumer', () => consumer.disconnect())
            const failed = (event) => {
              outcome.error ??= event.payload.error
            }
            consumer.on(consumer.events.CRASH, failed)
            consumer.on(consumer.events.PARTITION_HALTED, failed)
            await consumer.connect()
            await consumer.subscribe({ topics: [topic, controlTopic], fromBeginning: true })
            await consumer.run({
              autoCommit: false,
              eachMessage: async ({ topic: from, message }) => {
                if (from === controlTopic) control.push(textOf(message.value))
                else outcome.records.push(publicRecord(message))
              },
            })

            await replay(controlBytes, controlTopic)
            await waitForCondition(() => control.length >= 1 || outcome.error !== null, WAIT_BUDGET)
            if (control.length !== 1)
              throw new Error('The non-Snappy control was not delivered once')

            const assigned = await replay(bytes, topic)
            await waitForCondition(
              () =>
                outcome.records.length >= Number(manifest.batch.recordCount) ||
                outcome.error !== null,
              WAIT_BUDGET
            )
            await sleep(QUIET_MS)
            await consumer.disconnect()

            const stored = tap
              .batches()
              .find((batch) => BigInt(batch.baseOffset) === assigned && batch.codec === 2)
            expected = expectedRecords(manifest, assigned)
            writeEvidence(EVIDENCE, {
              scenario: 'compression',
              case: 'authentic-librdkafka-snappy',
              codec: 'snappy',
              config: { producer: manifest.producer, message_timestamp_type: 'CreateTime' },
              replay: REPLAY,
              capture: {
                sha256: manifest.sha256,
                compressedSha256: manifest.compressedSha256,
                recordDataSha256: manifest.recordDataSha256,
                provenance: manifest.provenance,
              },
              stage: pinned ? { baseline: artifact, pinned: JSON.parse(pinned) } : { final: true },
              control,
              envelopeDelta: delta,
              assignedBaseOffset: String(assigned),
              stored: stored && {
                codec: stored.codecName,
                records: stored.count,
                bodySha256: sha256(stored.body),
                firstTimestamp: stored.firstTimestamp,
                lastOffsetDelta: stored.lastOffsetDelta,
              },
              delivered: outcome.records.length,
              error: describeError(outcome.error),
            })

            expect(control).toEqual(['v0'])
            expect(stored).toBeDefined()
            expect(stored.codec).toBe(2)
            expect(stored.firstTimestamp).toBe(manifest.batch.baseTimestamp)
            expect(String(stored.count)).toBe(String(manifest.batch.recordCount))
            expect(String(stored.lastOffsetDelta)).toBe(String(manifest.batch.lastOffsetDelta))
            const snappy = pinned ? null : compression.lookupCodec(compression.Types.Snappy)
            const decompressed = await snappy?.decompress(stored.body, {
              maxOutputLength: validated.data.length,
            })
            verifyStoredBody(validated, stored.body, decompressed)

            if (pinned) assertBaselineDiagnostic(outcome.error, pinned)
            else if (outcome.error !== null) throw outcome.error
          })
        },
        { label: 'The setup' }
      )
    }, HOOK_BUDGET_MS)

    test(
      'is delivered to the public consumer with the original records',
      liveCase(QUICK_BUDGET_MS, () => {
        expect(outcome.records).toEqual(expected)
      }),
      caseTimeout(QUICK_BUDGET_MS)
    )
  })
})
