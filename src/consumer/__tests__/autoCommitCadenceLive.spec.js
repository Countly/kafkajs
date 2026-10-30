const { Kafka, logLevel } = require('../../../index')
const { writeEvidence } = require('testHelpers')
const { CleanupStack, describeError } = require('../../producer/__tests__/fixtures/liveProbe')
const {
  NEVER,
  STOP_GRACE_MS,
  bounded,
  caseTimeout,
  delay,
  liveCase,
  pollUntil,
} = require('../../producer/__tests__/fixtures/bounded')
const { FaultWire, LiveTopic, STEP_MS, CLEANUP_BUDGET_MS } = require('./fixtures/closeScenario')

// Gated: it needs a real cluster. Run alone: `npm run test:e2e:auto-commit`.
const GATED = process.env.AUTO_COMMIT_E2E === '1'
const describeCadence = GATED ? describe : describe.skip

const EVIDENCE = 'auto-commit'
const CASE_BUDGET_MS = 90000
const INTERVAL_MS = 5000
const QUIET_WINDOW_MS = 30000
const WINDOW_RECORDS = 600
const OFFER_EVERY_MS = 1000
const OFFERS = QUIET_WINDOW_MS / OFFER_EVERY_MS
const MAX_COMMIT_FRAMES = Math.ceil(QUIET_WINDOW_MS / INTERVAL_MS) + 2
const MAX_WAIT_MS = 500

/**
 * One classic consumer over a fault wire that counts its commit frames. It delivers whole batches
 * and resolves every record.
 */
class CadenceMember {
  /**
   * @param {object} options
   * @param {AbortSignal} options.scope
   * @param {CleanupStack} options.cleanup
   * @param {LiveTopic} options.live
   * @param {string} options.name
   * @param {object} options.run the run options under test
   */
  constructor({ scope, cleanup, live, name, run }) {
    this.scope = scope
    this.live = live
    this.runOptions = run
    this.wire = new FaultWire()
    this.consumer = new Kafka({
      clientId: `e2e-p9c-cadence-${live.id}-${name}`,
      brokers: live.brokers,
      logLevel: logLevel.NOTHING,
      socketFactory: this.wire.factory,
    }).consumer({ groupId: live.groupId, maxWaitTimeInMs: MAX_WAIT_MS })
    /** @type {Array<{topic: string, partition: number, value: string}>} */
    this.delivered = []
    this.batches = 0
    cleanup.add(`consumer ${name}`, () =>
      bounded(NEVER, STOP_GRACE_MS * 4, () => this.consumer.disconnect({ timeout: 0 }))
    )
  }

  /** @returns {Promise<void>} once the run has started */
  async start() {
    const { scope, live, consumer } = this
    await bounded(scope, STEP_MS, () => consumer.connect(), { label: 'The consumer connect' })
    await bounded(
      scope,
      STEP_MS,
      () => consumer.subscribe({ topic: live.topic, fromBeginning: true }),
      { label: 'The consumer subscribe' }
    )
    await bounded(
      scope,
      STEP_MS,
      () =>
        consumer.run({
          ...this.runOptions,
          eachBatch: async ({ batch, resolveOffset, heartbeat }) => {
            this.batches += 1
            for (const message of batch.messages) {
              this.delivered.push({
                topic: batch.topic,
                partition: batch.partition,
                value: message.value.toString(),
              })
              resolveOffset(message.offset)
            }
            await heartbeat()
          },
        }),
      { label: 'The consumer run' }
    )
  }

  /** @param {number} count @returns {Promise<void>} once that many records were delivered */
  async delivery(count) {
    await pollUntil(this.scope, STEP_MS, () => this.delivered.length >= count, {
      label: 'The deliveries',
      delayMs: 100,
    })
  }
}

/**
 * Runs one case under its cleanup stack and writes its evidence row in a `finally`, with the
 * log, broker and handler sets kept apart.
 * @param {AbortSignal} scope
 * @param {string} title
 * @param {object} config what the row records
 * @param {{count: number, committed?: number}} corpus the records to produce and the group's
 *   committed offset before the case
 * @param {(ctx: {cleanup: CleanupStack, live: LiveTopic, observed: object}) => Promise<void>} body
 * @returns {Promise<void>}
 */
const runCase = async (scope, title, config, corpus, body) => {
  const cleanup = new CleanupStack()
  const live = new LiveTopic({ scope, cleanup, prefix: 'cadence' })
  const observed = { stage: 'starting' }
  await cleanup.run(
    async () => {
      try {
        await live.prepare(corpus)
        observed.brokerBefore = await live.brokerOffset()
        observed.stage = 'body'
        await body({ cleanup, live, observed })
        observed.stage = 'passed'
      } catch (error) {
        observed.error = describeError(error)
        throw error
      } finally {
        writeEvidence(EVIDENCE, {
          scenario: 'auto-commit',
          case: title,
          config: { ...config, budgetMs: CASE_BUDGET_MS },
          replay: {
            log: 'acknowledged records by topic, partition and value',
            offsets: 'committed offsets read independently of the client under test',
            handlers: 'delivered records; a replay starts at the last confirmed commit',
          },
          acknowledged: live.produced.length,
          observed,
        })
      }
    },
    { budgetMs: CLEANUP_BUDGET_MS * 3 }
  )
}

/**
 * @param {LiveTopic} live
 * @param {number} from first record number
 * @param {number} count how many records
 * @returns {Promise<void>} once the broker acknowledged them
 */
const offer = async (live, from, count) => {
  const producer = live.producer
  const messages = Array.from({ length: count }, (_, index) => ({
    partition: 0,
    value: `value-${from + index}`,
  }))
  await live.step('The offer', () => producer.send({ topic: live.topic, messages }))
  for (const { value } of messages) live.produced.push({ topic: live.topic, partition: 0, value })
}

describeCadence('Consumer > automatic commit cadence (live)', () => {
  const add = (title, body) =>
    test(title, liveCase(CASE_BUDGET_MS, body), caseTimeout(CASE_BUDGET_MS))

  add(
    'a configured interval commits only at processing opportunities inside the quiet window',
    (scope) =>
      runCase(
        scope,
        'a configured interval commits only at processing opportunities inside the quiet window',
        { autoCommit: true, autoCommitInterval: INTERVAL_MS, records: WINDOW_RECORDS },
        { count: 1 },
        async ({ cleanup, live, observed }) => {
          const member = new CadenceMember({
            scope,
            cleanup,
            live,
            name: 'one',
            run: { autoCommit: true, autoCommitInterval: INTERVAL_MS },
          })
          await member.start()
          await member.delivery(1)
          const perOffer = WINDOW_RECORDS / OFFERS
          const framesAtStart = member.wire.count('OffsetCommit')
          const batchesAtStart = member.batches
          const frameTimes = []
          let seen = framesAtStart
          const startedAt = performance.now()
          for (let index = 0; index < OFFERS; index++) {
            await offer(live, 1 + index * perOffer, perOffer)
            await delay(scope, OFFER_EVERY_MS)
            const now = member.wire.count('OffsetCommit')
            if (now > seen) frameTimes.push(Math.round(performance.now() - startedAt))
            seen = now
          }
          await member.delivery(1 + WINDOW_RECORDS)
          const inWindow = member.wire.count('OffsetCommit') - framesAtStart
          observed.windowMs = Math.round(performance.now() - startedAt)
          observed.commitFramesInWindow = inWindow
          observed.commitFrameTimesMs = frameTimes
          observed.opportunities = member.batches - batchesAtStart
          expect(member.delivered).toHaveLength(1 + WINDOW_RECORDS)
          expect(new Set(member.delivered.map(({ value }) => value)).size).toBe(1 + WINDOW_RECORDS)
          expect(observed.opportunities).toBeGreaterThanOrEqual(2)
          expect(inWindow).toBeGreaterThanOrEqual(2)
          expect(inWindow).toBeLessThanOrEqual(MAX_COMMIT_FRAMES)
        }
      )
  )

  add(
    'a below-interval batch followed by idle commits only at the next processing opportunity',
    (scope) =>
      runCase(
        scope,
        'a below-interval batch followed by idle commits only at the next processing opportunity',
        { autoCommit: true, autoCommitInterval: INTERVAL_MS },
        { count: 3 },
        async ({ cleanup, live, observed }) => {
          const member = new CadenceMember({
            scope,
            cleanup,
            live,
            name: 'one',
            run: { autoCommit: true, autoCommitInterval: INTERVAL_MS },
          })
          await member.start()
          await member.delivery(3)
          const framesAfterBatch = member.wire.count('OffsetCommit')
          const before = await live.brokerOffset()
          await delay(scope, INTERVAL_MS + 2000)
          observed.idleFrames = member.wire.count('OffsetCommit') - framesAfterBatch
          observed.brokerAfterIdle = await live.brokerOffset()
          expect(observed.idleFrames).toBe(0)
          expect(observed.brokerAfterIdle).toBe(before)
          await offer(live, 3, 1)
          await member.delivery(4)
          await pollUntil(scope, STEP_MS, async () => (await live.brokerOffset()) === 4, {
            label: 'The next opportunity commit',
          })
          observed.brokerAfter = 4
        }
      )
  )

  add('a graceful close commits the processed prefix confirmed by an independent read', (scope) =>
    runCase(
      scope,
      'a graceful close commits the processed prefix confirmed by an independent read',
      { autoCommit: true, autoCommitInterval: 600000 },
      { count: 30 },
      async ({ cleanup, live, observed }) => {
        const member = new CadenceMember({
          scope,
          cleanup,
          live,
          name: 'one',
          run: { autoCommit: true, autoCommitInterval: 600000 },
        })
        await member.start()
        await member.delivery(30)
        expect(await live.brokerOffset()).toBe(-1)
        await bounded(scope, STEP_MS, () => member.consumer.disconnect(), {
          label: 'The graceful close',
        })
        observed.brokerAfter = await live.brokerOffset()
        expect(observed.brokerAfter).toBe(30)
      }
    )
  )

  add('a connection loss replays exactly the suffix after the last confirmed commit', (scope) =>
    runCase(
      scope,
      'a connection loss replays exactly the suffix after the last confirmed commit',
      { autoCommit: true, autoCommitInterval: 600000, committed: 10 },
      { count: 30, committed: 10 },
      async ({ cleanup, live, observed }) => {
        const first = new CadenceMember({
          scope,
          cleanup,
          live,
          name: 'first',
          run: { autoCommit: true, autoCommitInterval: 600000 },
        })
        await first.start()
        await first.delivery(20)
        await bounded(scope, STEP_MS, () => first.consumer.disconnect({ timeout: 0 }), {
          label: 'The forced local close',
        })
        observed.brokerAfterLoss = await live.brokerOffset()
        expect(observed.brokerAfterLoss).toBe(10)
        const second = new CadenceMember({
          scope,
          cleanup,
          live,
          name: 'second',
          run: { autoCommit: true, autoCommitInterval: 600000 },
        })
        await second.start()
        await second.delivery(20)
        const replayed = second.delivered.map(({ value }) => value)
        observed.replayed = replayed.length
        expect(replayed.slice(0, 20)).toEqual(
          Array.from({ length: 20 }, (_, index) => `value-${10 + index}`)
        )
        expect(replayed).toHaveLength(20)
      }
    )
  )
})
