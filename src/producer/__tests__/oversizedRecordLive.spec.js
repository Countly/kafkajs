const { Kafka, logLevel } = require('../../../index')
const ISOLATION_LEVEL = require('../../protocol/isolationLevel')
const apiKeys = require('../../protocol/requests/apiKeys')
const {
  KafkaJSInvalidConfigurationError,
  KafkaJSRecordTooLargeError,
  KafkaJSTransactionAbortableError,
} = require('../../errors')
const {
  createTopic,
  plainTextBrokers,
  secureRandom,
  writeEvidence,
  wireVersionsOf,
} = require('testHelpers')
const { sleep, waitForCondition } = require('../../consumer/__tests__/fixtures/liveScenario')
const { createLiveSockets, repliesOf } = require('./fixtures/liveWire')
const {
  CleanupStack,
  ProducerProbe,
  describeError,
  readData,
  readUntilCovered,
  retryCoordinator,
  settle,
  topicConfig,
} = require('./fixtures/liveProbe')
const { caseTimeout, liveCase } = require('./fixtures/bounded')

// Gated: it sends records no request, or no topic, can take. Run alone:
// `npm run test:e2e:oversized-record`.
const describeOversized = process.env.OVERSIZED_RECORD_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'oversized-record'
const CASE_BUDGET_MS = 120000
const REQUEST_TIMEOUT_MS = 1000
const DELIVERY_TIMEOUT_MS = 10000
const DISCONNECT_BUDGET_MS = 15000
const MAX_REQUEST_SIZE = 1048576
const SPLIT_REQUEST_SIZE = 262144
const SPLIT_BATCH_SIZE = 5242880
const SPLIT_RECORDS = 50
const SPLIT_RECORD_BYTES = 104857
const OVER_REQUEST_BYTES = 1048577
const TOPIC_LIMIT = 100000
const OVER_TOPIC_BYTES = 200000
const SMALL_SENDS = 10
const SETTLE_MS = 1000
const APPEAR_BUDGET = { attempts: 120, delay: 250 } // 30 s
const RETRY = { retries: 10, initialRetryTime: 100, maxRetryTime: 1000, factor: 0 }

/** The five combinations a size case runs under; explicit idempotence and transactions need -1. */
const ARMS = Object.freeze([
  { kind: 'plain', acks: 0 },
  { kind: 'plain', acks: 1 },
  { kind: 'plain', acks: -1 },
  { kind: 'idempotent', acks: -1 },
  { kind: 'transactional', acks: -1 },
])

/** The four explicit combinations whose acks the producer refuses. */
const INVALID_ARMS = Object.freeze([
  { kind: 'idempotent', acks: 0 },
  { kind: 'idempotent', acks: 1 },
  { kind: 'transactional', acks: 0 },
  { kind: 'transactional', acks: 1 },
])

const REPLAY = Object.freeze({
  log: 'the refused record is absent; every delivered record is present once and in order',
  offsets: 'n/a',
  handlers: 'n/a',
})

/** @param {{ kind: string, acks: number }} arm @returns {string} the title suffix of a case */
const titleOf = ({ kind, acks }) => `${kind} producer, acks ${acks}`

/**
 * The producer, its sockets and every resource one case holds, closed together.
 */
class Scenario {
  /**
   * @param {object} options
   * @param {{ kind: string, acks: number }} options.arm
   * @param {number} [options.maxRequestSize]
   * @param {number} [options.batchSize]
   */
  constructor({ arm, maxRequestSize = MAX_REQUEST_SIZE, batchSize }) {
    this.arm = arm
    this.id = secureRandom()
    this.brokers = plainTextBrokers()
    this.clientId = `e2e-p9b-oversized-record-${this.id}`
    this.transactionalId =
      arm.kind === 'transactional' ? `e2e-p9b-oversized-record-txn-${this.id}` : undefined
    this.maxRequestSize = maxRequestSize
    this.batchSize = batchSize
    this.sockets = createLiveSockets()
    this.cleanup = new CleanupStack()
    this.requests = []
  }

  /** The declared settings, for the evidence. */
  get config() {
    return {
      requestTimeout: REQUEST_TIMEOUT_MS,
      enforceRequestTimeout: true,
      linger: 0,
      deliveryTimeout: DELIVERY_TIMEOUT_MS,
      maxRequestSize: this.maxRequestSize,
      batchSize: this.batchSize,
      retry: RETRY,
      ...this.arm,
    }
  }

  /**
   * @param {string} name what the topic is for
   * @returns {string} a topic name no other case shares
   */
  topic(name) {
    return `e2e-p9b-oversized-record-${name}-${this.id}`
  }

  /**
   * Opens the admin client and the producer and installs the admission probe.
   * @returns {Promise<void>}
   */
  async start() {
    const { kind } = this.arm
    const kafka = new Kafka({
      clientId: this.clientId,
      brokers: this.brokers,
      logLevel: logLevel.NOTHING,
      socketFactory: this.sockets.socketFactory,
      requestTimeout: REQUEST_TIMEOUT_MS,
      enforceRequestTimeout: true,
    })
    this.producer = kafka.producer({
      ...(kind === 'plain' && { idempotent: false }),
      ...(kind === 'idempotent' && { idempotent: true }),
      ...(kind === 'transactional' && { transactionalId: this.transactionalId }),
      linger: 0,
      deliveryTimeout: DELIVERY_TIMEOUT_MS,
      maxRequestSize: this.maxRequestSize,
      batchSize: this.batchSize,
      retry: RETRY,
    })
    this.producer.on(this.producer.events.REQUEST, ({ payload }) => this.requests.push(payload))
    this.admin = new Kafka({ brokers: this.brokers, logLevel: logLevel.NOTHING }).admin()
    this.probe = new ProducerProbe({
      producer: this.producer,
      frames: this.sockets.frames,
      clientId: this.clientId,
      transactionalId: this.transactionalId,
      maxRequestSize: this.maxRequestSize,
    }).install()
    this.cleanup.add('admission probe', () => this.probe.restore())
    this.cleanup.add('resources after disconnect', () => this.probe.settled())
    await this.admin.connect()
    this.cleanup.add('admin', () => this.admin.disconnect())
    await this.producer.connect()
    this.cleanup.add('producer', () => this.producer.disconnect({ timeout: DISCONNECT_BUDGET_MS }))
  }

  /**
   * @returns {Promise<object>} what sends this case's records: the producer, or a new transaction
   */
  begin() {
    return this.arm.kind === 'transactional' ? this.producer.transaction() : this.producer
  }

  /**
   * Runs the closing admission probe on this case's producer, in a fresh transaction when it is
   * transactional.
   * @returns {Promise<object>} what the probe observed
   */
  async closing() {
    const topic = this.topic('probe')
    if (this.arm.kind !== 'transactional') {
      return this.probe.closing({ topic, budgetMs: DELIVERY_TIMEOUT_MS })
    }
    const transaction = await this.producer.transaction()
    return this.probe.closing({
      sender: transaction,
      topic,
      finish: () => transaction.commit(),
      isolationLevel: ISOLATION_LEVEL.READ_COMMITTED,
      budgetMs: DELIVERY_TIMEOUT_MS,
    })
  }

  /**
   * Writes the case's evidence line.
   * @param {string} name the part of the scenario
   * @param {object} fields what was measured
   * @returns {void}
   */
  record(name, fields) {
    writeEvidence(EVIDENCE, {
      scenario: 'oversized-record',
      case: name,
      config: this.config,
      replay: REPLAY,
      wireVersions: wireVersionsOf(this.requests),
      wire: { frames: this.sockets.frames.trace(), replies: repliesOf(this.requests) },
      ...fields,
    })
  }

  /**
   * Runs the case, then closes everything it opened; the case's own failure is the one thrown.
   * @template T
   * @param {() => Promise<T>} work
   * @returns {Promise<T>}
   */
  run(work) {
    return this.cleanup.run(async () => {
      await this.start()
      return work()
    })
  }

  /**
   * What a transaction armed with a refusal says to everything that follows, and that the group
   * offsets it staged never reached the broker.
   * @param {object} transaction the armed transaction
   * @param {Error} refusal the error its send was refused with
   * @param {string} topic
   * @param {AbortSignal} signal the case's scope; the offsets read waits for the group's
   *   coordinator, which can still move on a fresh cluster, under it
   * @returns {Promise<{ armed: Error, offset: string }>}
   */
  async expectArmed(transaction, refusal, topic, signal) {
    const groupId = `e2e-p9b-oversized-record-group-${this.id}`
    const later = []
    later.push(await settle(transaction.send({ topic, messages: [{ value: 'late' }], acks: -1 })))
    later.push(
      await settle(
        transaction.sendBatch({
          topicMessages: [{ topic, messages: [{ value: 'late' }] }],
          acks: -1,
        })
      )
    )
    later.push(
      await settle(
        transaction.sendOffsets({
          consumerGroupId: groupId,
          topics: [{ topic, partitions: [{ partition: 0, offset: '1' }] }],
        })
      )
    )
    later.push(await settle(transaction.commit()))
    const [armed] = later.map(({ error }) => error)
    expect(armed).toBeInstanceOf(KafkaJSTransactionAbortableError)
    expect(armed.cause).toBe(refusal)
    for (const { error } of later) expect(error).toBe(armed)
    await transaction.abort()
    const [{ partitions }] = await retryCoordinator(signal, () =>
      this.admin.fetchOffsets({ groupId, topics: [topic] })
    )
    return { armed, offset: partitions[0].offset }
  }
}

/**
 * @param {object[]} records
 * @returns {string[]} the records' values as text
 */
const valuesOf = (records) => records.map(({ value }) => String(value))

/**
 * Reads a topic until `done` holds of what the log holds, as the broker's leaders hold it.
 * @param {Scenario} scenario
 * @param {string} topic
 * @param {(records: object[]) => boolean} done
 * @param {boolean} [committed=false]
 * @returns {Promise<object[]>} the last read, whether or not `done` held
 */
const readUntil = async (scenario, topic, done, committed = false) => {
  let records = []
  await waitForCondition(async () => {
    records = await readData(scenario.brokers, topic, { committed })
    return done(records)
  }, APPEAR_BUDGET)
  return records
}

/**
 * Live proof of what a producer does with a record that is too large for its own request limit,
 * for a request it must split, for the topic's own limit, and for acks an idempotent or
 * transactional producer refuses. A record no request can carry is refused before it is written;
 * a send above one request is split into ordered requests that each fit; a record above the
 * topic's `max.message.bytes` is the broker's error, not the client's; and a transaction refused
 * either way can only be aborted, then a new one commits.
 *
 * Replay sets: the physical log holds the refused record never and every delivered record once, in
 * order; committed offsets and handler deliveries are not part of this scenario.
 * @see https://kafka.apache.org/43/configuration/topic-configs/#topicconfigs_max.message.bytes
 */
describeOversized('Producer > records above a size limit (live)', () => {
  test.each(ARMS.map((arm) => [titleOf(arm), arm]))(
    'a record no request can carry is refused before it is written, and the next send works: %s',
    liveCase(CASE_BUDGET_MS, async (scope, _, arm) => {
      const scenario = new Scenario({ arm })
      await scenario.run(async () => {
        const topic = scenario.topic('local')
        await createTopic({ topic, partitions: 1, replicas: 3 })
        const sender = await scenario.begin()
        await scenario.probe.capture(
          () => sender.send({ topic, messages: [{ value: 'valid-before' }], acks: arm.acks }),
          { signal: scope }
        )
        const mark = scenario.sockets.frames.mark()
        const refusal = await settle(
          sender.send({
            topic,
            messages: [{ value: Buffer.alloc(OVER_REQUEST_BYTES) }],
            acks: arm.acks,
          })
        )
        const written = scenario.sockets.frames.since(apiKeys.Produce, mark).length

        let armedAfter = null
        let committed = null
        if (arm.kind === 'transactional') {
          armedAfter = await scenario.expectArmed(sender, refusal.error, topic, scope)
          const next = await scenario.producer.transaction()
          await next.send({ topic, messages: [{ value: 'valid-after' }], acks: -1 })
          await next.commit()
          committed = valuesOf(
            await readUntilCovered(
              scope,
              () => readData(scenario.brokers, topic, { committed: true }),
              (records) => records.length >= 1
            )
          )
        } else {
          await sender.send({ topic, messages: [{ value: 'valid-after' }], acks: arm.acks })
        }
        const log = await readUntil(
          scenario,
          topic,
          (records) =>
            records.length >= 2 && records.some(({ value }) => `${value}` === 'valid-after')
        )
        const values = valuesOf(log)
        const closing = await scenario.closing()

        scenario.record('local-refusal', {
          written,
          refusal: describeError(refusal.error),
          armedAfter: armedAfter && {
            armed: describeError(armedAfter.armed),
            groupOffset: armedAfter.offset,
          },
          committed,
          values,
          largest: Math.max(...log.map(({ value }) => value.length)),
          closing,
        })

        expect(written).toBe(0)
        expect(refusal.error).toBeInstanceOf(KafkaJSRecordTooLargeError)
        expect(refusal.error).toMatchObject({
          reason: 'maxRequestSize',
          scope: 'record',
          type: 'MESSAGE_TOO_LARGE',
          code: 10,
        })
        expect(values).toEqual(['valid-before', 'valid-after'])
        if (arm.kind === 'transactional') {
          expect(armedAfter.offset).toBe('-1')
          expect(committed).toEqual(['valid-after'])
        }
      })
    }),
    caseTimeout(CASE_BUDGET_MS)
  )

  test.each(ARMS.map((arm) => [titleOf(arm), arm]))(
    'a send above one request is split into ordered requests that each fit: %s',
    liveCase(CASE_BUDGET_MS, async (scope, _, arm) => {
      const scenario = new Scenario({
        arm,
        maxRequestSize: SPLIT_REQUEST_SIZE,
        batchSize: SPLIT_BATCH_SIZE,
      })
      await scenario.run(async () => {
        const topic = scenario.topic('split')
        await createTopic({ topic, partitions: 1, replicas: 3 })
        const sender = await scenario.begin()
        await scenario.probe.capture(
          () => sender.send({ topic, messages: [{ value: 'warm' }], acks: arm.acks }),
          { signal: scope }
        )
        const ids = Array.from(
          { length: SPLIT_RECORDS },
          (_, index) => `split-${String(index).padStart(3, '0')}:`
        )
        const messages = ids.map((id) => {
          const value = Buffer.alloc(SPLIT_RECORD_BYTES)
          value.write(id)
          return { value }
        })
        const mark = scenario.sockets.frames.mark()
        const sent = await settle(sender.send({ topic, messages, acks: arm.acks }))
        const frames = scenario.sockets.frames.since(apiKeys.Produce, mark)
        if (arm.kind === 'transactional') await sender.commit()

        const isSplit = ({ value }) => value.length === SPLIT_RECORD_BYTES
        const log = await readUntil(
          scenario,
          topic,
          (records) => records.filter(isSplit).length >= SPLIT_RECORDS,
          arm.kind === 'transactional'
        )
        const delivered = log.filter(isSplit)
        const metadata = sent.value ?? []
        const closing = await scenario.closing()

        scenario.record('split-send', {
          sent: describeError(sent.error),
          frames: { count: frames.length, largest: Math.max(...frames.map(({ bytes }) => bytes)) },
          firstOffset: delivered[0]?.offset,
          metadata: metadata.map(({ partition, baseOffset }) => ({ partition, baseOffset })),
          delivered: delivered.length,
          closing,
        })

        expect(sent.error).toBeNull()
        expect(frames.length).toBeGreaterThanOrEqual(
          Math.floor((SPLIT_RECORDS * SPLIT_RECORD_BYTES) / SPLIT_REQUEST_SIZE)
        )
        for (const frame of frames) expect(frame.bytes).toBeLessThanOrEqual(SPLIT_REQUEST_SIZE)
        expect(delivered.map(({ value }) => value.toString('latin1', 0, ids[0].length))).toEqual(
          ids
        )
        if (arm.acks === 0) {
          expect(metadata).toEqual([])
        } else {
          expect(metadata[0].baseOffset).toBe(delivered[0].offset)
        }
      })
    }),
    caseTimeout(CASE_BUDGET_MS)
  )

  test.each(ARMS.map((arm) => [titleOf(arm), arm]))(
    'a record above the topic limit is the broker error, not a client refusal: %s',
    liveCase(CASE_BUDGET_MS, async (scope, _, arm) => {
      const scenario = new Scenario({ arm })
      await scenario.run(async () => {
        const topic = scenario.topic('broker')
        await createTopic({
          topic,
          partitions: 1,
          replicas: 3,
          config: [{ name: 'max.message.bytes', value: String(TOPIC_LIMIT) }],
        })
        const readBack = await topicConfig(scenario.brokers, topic, 'max.message.bytes')
        const sender = await scenario.begin()
        await scenario.probe.capture(
          () => sender.send({ topic, messages: [{ value: 'valid-before' }], acks: arm.acks }),
          { signal: scope }
        )
        const refusal = await settle(
          sender.send({
            topic,
            messages: [{ value: Buffer.alloc(OVER_TOPIC_BYTES) }],
            acks: arm.acks,
          })
        )
        if (arm.acks === 0) await sleep(SETTLE_MS)

        let armedAfter = null
        let active = sender
        if (arm.kind === 'transactional') {
          armedAfter = await scenario.expectArmed(sender, refusal.error, topic, scope)
          active = await scenario.producer.transaction()
        }
        for (let index = 0; index < SMALL_SENDS; index++) {
          await active.send({ topic, messages: [{ value: `small-${index}` }], acks: arm.acks })
        }
        if (arm.kind === 'transactional') await active.commit()

        const isSmall = ({ value }) => String(value).startsWith('small-')
        const log = await readUntil(
          scenario,
          topic,
          (records) => records.filter(isSmall).length >= SMALL_SENDS,
          arm.kind === 'transactional'
        )
        const everything = await readData(scenario.brokers, topic)
        const closing = await scenario.closing()

        scenario.record('topic-limit', {
          readBack,
          refusal: describeError(refusal.error),
          armedAfter: armedAfter && {
            armed: describeError(armedAfter.armed),
            groupOffset: armedAfter.offset,
          },
          small: valuesOf(log.filter(isSmall)),
          largest: Math.max(...everything.map(({ value }) => value.length)),
          closing,
        })

        expect(readBack).toBe(String(TOPIC_LIMIT))
        if (arm.acks === 0) {
          expect(refusal.error).toBeNull()
        } else {
          expect(refusal.error).toMatchObject({
            name: 'KafkaJSProtocolError',
            type: 'MESSAGE_TOO_LARGE',
            code: 10,
            retriable: false,
          })
          expect(refusal.error).not.toHaveProperty('outcome')
        }
        expect(valuesOf(log.filter(isSmall))).toEqual(
          Array.from({ length: SMALL_SENDS }, (_, index) => `small-${index}`)
        )
        expect(everything.every(({ value }) => value.length < OVER_TOPIC_BYTES)).toBe(true)
        if (arm.kind === 'transactional') expect(armedAfter.offset).toBe('-1')
      })
    }),
    caseTimeout(CASE_BUDGET_MS)
  )

  test.each(INVALID_ARMS.map((arm) => [titleOf(arm), arm]))(
    'acks other than -1 are a typed configuration error before anything is written: %s',
    liveCase(CASE_BUDGET_MS, async (scope, _, arm) => {
      const scenario = new Scenario({ arm })
      await scenario.run(async () => {
        const topic = scenario.topic('acks')
        await createTopic({ topic, partitions: 1, replicas: 3 })
        const sender = await scenario.begin()
        await scenario.probe.capture(
          () => sender.send({ topic, messages: [{ value: 'valid' }], acks: -1 }),
          { signal: scope }
        )
        const mark = scenario.sockets.frames.mark()
        const refusal = await settle(
          sender.send({ topic, messages: [{ value: 'invalid' }], acks: arm.acks })
        )
        const written = scenario.sockets.frames.since(apiKeys.Produce, mark).length
        if (arm.kind === 'transactional') await sender.abort()
        const log = await readUntil(scenario, topic, (records) => records.length >= 1)
        const closing = await scenario.closing()

        scenario.record('invalid-acks', {
          written,
          refusal: describeError(refusal.error),
          values: valuesOf(log),
          closing,
        })

        expect(refusal.error).toBeInstanceOf(KafkaJSInvalidConfigurationError)
        expect(written).toBe(0)
        expect(valuesOf(log)).not.toContain('invalid')
      })
    }),
    caseTimeout(CASE_BUDGET_MS)
  )
})
