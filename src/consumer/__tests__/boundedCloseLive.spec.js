const { Kafka, logLevel } = require('../../../index')
const { roundRobin, cooperativeSticky } = require('../assigners')
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

// Gated: it needs a real cluster. Run alone: `npm run test:e2e:bounded-close`.
const GATED = process.env.BOUNDED_CLOSE_E2E === '1'
const describeClose = GATED ? describe : describe.skip

const EVIDENCE = 'bounded-close'
const CASE_BUDGET_MS = 90000
const CLOSE_TIMEOUT_MS = 3000
const TEARDOWN_ALLOWANCE_MS = 2000
const REQUEST_TIMEOUT_MS = 5000
const SESSION_TIMEOUT_MS = 10000
const MAX_WAIT_MS = 500
const COMMITTED_BEFORE = 5
const RECORDS = 20
/** Quiet time after a forced close in which no frame may be written by the retired run. */
const SETTLE_MS = 1500
const COORDINATOR_APIS = ['OffsetCommit', 'LeaveGroup', 'Heartbeat', 'ConsumerGroupHeartbeat']

const ARMS = {
  eager: { label: 'classic eager', protocol: 'classic', assigner: roundRobin },
  cooperative: { label: 'classic cooperative', protocol: 'classic', assigner: cooperativeSticky },
  consumer: { label: 'consumer protocol', protocol: 'consumer' },
}

/**
 * The consumer options an arm gives: the classic session timeout goes to a classic arm only.
 * @param {object} arm
 * @param {string} groupId
 * @returns {object}
 */
const consumerOptions = (arm, groupId) => ({
  groupId,
  maxWaitTimeInMs: MAX_WAIT_MS,
  ...(arm.protocol === 'consumer'
    ? { groupProtocol: 'consumer' }
    : { partitionAssigners: [arm.assigner], sessionTimeout: SESSION_TIMEOUT_MS }),
})

/**
 * One consumer over a fault wire: it resolves every record, commits only at its final attempt
 * (the interval never elapses), and can hold a handler or a close callback.
 */
class ClosingMember {
  /**
   * @param {object} options
   * @param {AbortSignal} options.scope
   * @param {CleanupStack} options.cleanup
   * @param {LiveTopic} options.live
   * @param {object} options.arm
   * @param {boolean} options.enforce whether request timeouts are enforced by the transport
   * @param {string} options.name the member label
   */
  constructor({ scope, cleanup, live, arm, enforce, name }) {
    this.scope = scope
    this.live = live
    this.arm = arm
    this.wire = new FaultWire()
    this.kafka = new Kafka({
      clientId: `e2e-p9c-close-${live.id}-${name}`,
      brokers: live.brokers,
      logLevel: logLevel.NOTHING,
      requestTimeout: REQUEST_TIMEOUT_MS,
      enforceRequestTimeout: enforce,
      socketFactory: this.wire.factory,
    })
    this.consumer = this.kafka.consumer(consumerOptions(arm, live.groupId))
    /** @type {Array<{topic: string, partition: number, value: string}>} */
    this.delivered = []
    /** @type {Function[]} resolvers of every handler gate and hook gate the member opened */
    this.handlerGates = []
    this.hookGates = []
    this.handlerHeld = null
    this.hookHeld = null
    this.detached = []
    cleanup.add(`consumer ${name}`, () =>
      bounded(NEVER, STOP_GRACE_MS * 4, () => this.consumer.disconnect({ timeout: 0 }))
    )
    cleanup.add(`held callbacks ${name}`, () => {
      this.releaseHandler()
      this.releaseHook()
    })
  }

  /** @returns {void} lets every held handler go on */
  releaseHandler() {
    for (const open of this.handlerGates) open()
  }

  /** @returns {void} lets every held close callback go on */
  releaseHook() {
    for (const open of this.hookGates) open()
  }

  /**
   * Connects, subscribes and runs. With `holdHandler` the handler enters once for the first record
   * and waits, after recording the closures a detached run would still hold; with `holdHook` the
   * revoked callback waits the same way, but only for owned partitions: every classic round also
   * revokes the empty assignment before the first join, and a held callback there would keep `run`
   * from resolving.
   * @param {object} [options]
   * @param {boolean} [options.holdHandler=false]
   * @param {boolean} [options.holdHook=false]
   * @returns {Promise<void>} once the run has started
   */
  async start({ holdHandler = false, holdHook = false } = {}) {
    const { scope, live, consumer } = this
    await bounded(scope, STEP_MS, () => consumer.connect(), { label: 'The consumer connect' })
    await bounded(
      scope,
      STEP_MS,
      () => consumer.subscribe({ topic: live.topic, fromBeginning: true }),
      {
        label: 'The consumer subscribe',
      }
    )
    const held = new Promise((resolve) => this.handlerGates.push(resolve))
    const hook = new Promise((resolve) => this.hookGates.push(resolve))
    let entered = false
    await bounded(
      scope,
      STEP_MS,
      () =>
        consumer.run({
          autoCommit: true,
          autoCommitInterval: 600000,
          rebalanceListener: holdHook
            ? {
                onPartitionsRevoked: async ({ partitions }) => {
                  if (partitions.length === 0) return
                  this.hookHeld = true
                  await hook
                  this.detached.push(
                    await consumer
                      .commitOffsets([{ topic: live.topic, partition: 0, offset: '1' }])
                      .then(
                        () => 'accepted',
                        (error) => error.type ?? error.name
                      )
                  )
                },
              }
            : undefined,
          eachMessage: async ({ topic, partition, message, heartbeat }) => {
            this.delivered.push({ topic, partition, value: message.value.toString() })
            if (holdHandler && !entered) {
              entered = true
              this.handlerHeld = true
              await held
              this.detached.push(
                await Promise.allSettled([
                  consumer.commitOffsets([{ topic, partition, offset: '99' }]),
                  heartbeat(),
                ]).then((results) => results.map(({ status }) => status))
              )
            }
          },
        }),
      { label: 'The consumer run' }
    )
  }

  /**
   * @param {number} count records to have been delivered
   * @returns {Promise<void>}
   */
  async delivery(count) {
    await pollUntil(this.scope, STEP_MS, () => this.delivered.length >= count, {
      label: 'The deliveries',
    })
  }

  /**
   * Closes with the product's own bound and measures what the caller saw.
   * @param {number} [timeout=CLOSE_TIMEOUT_MS]
   * @returns {Promise<number>} the monotonic milliseconds the close took
   */
  async close(timeout = CLOSE_TIMEOUT_MS) {
    const startedAt = performance.now()
    await bounded(
      this.scope,
      timeout + TEARDOWN_ALLOWANCE_MS,
      () => this.consumer.disconnect({ timeout }),
      { label: 'The bounded close' }
    )
    return performance.now() - startedAt
  }
}

/**
 * Runs one case under its cleanup stack. The evidence row is written in a `finally`, so a case
 * that fails or times out still records what it saw, and the primary failure stays the thrown
 * error. Three sets are kept apart: the log (what the broker acknowledged), the broker (committed
 * offsets read independently) and the handlers (what each run delivered).
 * @param {AbortSignal} scope
 * @param {string} title
 * @param {object} arm
 * @param {{ enforce?: boolean, committed?: number }} options
 * @param {(ctx: {cleanup: CleanupStack, live: LiveTopic, observed: object}) => Promise<void>} body
 * @returns {Promise<void>}
 */
const runCase = async (
  scope,
  title,
  arm,
  { enforce = true, committed = COMMITTED_BEFORE },
  body
) => {
  const cleanup = new CleanupStack()
  const live = new LiveTopic({ scope, cleanup, prefix: 'close' })
  const observed = { stage: 'starting', brokerBefore: null, brokerAfter: null, closeMs: null }
  await cleanup.run(
    async () => {
      try {
        await live.prepare({ count: RECORDS, committed })
        observed.brokerBefore = await live.brokerOffset()
        observed.stage = 'body'
        await body({ cleanup, live, observed })
        observed.stage = 'passed'
      } catch (error) {
        observed.error = describeError(error)
        throw error
      } finally {
        writeEvidence(EVIDENCE, {
          scenario: 'bounded-close',
          case: title,
          config: {
            protocol: arm.protocol,
            enforceRequestTimeout: enforce,
            requestTimeoutMs: REQUEST_TIMEOUT_MS,
            sessionTimeoutMs: arm.protocol === 'classic' ? SESSION_TIMEOUT_MS : null,
            closeTimeoutMs: CLOSE_TIMEOUT_MS,
            teardownAllowanceMs: TEARDOWN_ALLOWANCE_MS,
            autoCommit: true,
            budgetMs: CASE_BUDGET_MS,
          },
          replay: {
            log: 'acknowledged records by topic, partition and value',
            offsets: 'committed offsets read independently before and after the close',
            handlers: 'delivered records of each run; a replay starts at the last confirmed offset',
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
 * @param {ClosingMember} member
 * @returns {Record<string, number>} the coordinator frames the member wrote, stopped ones included
 */
const coordinatorFrames = (member) =>
  Object.fromEntries(COORDINATOR_APIS.map((api) => [api, member.wire.count(api)]))

/**
 * The blackhole arm: the member's coordinator requests never leave the process, the close is
 * bounded, and the retired run writes nothing after its deadline.
 * @param {string} title
 * @param {object} arm
 * @param {boolean} enforce
 * @returns {(scope: AbortSignal) => Promise<void>}
 */
const blackholeCase = (title, arm, enforce) => (scope) =>
  runCase(scope, title, arm, { enforce }, async ({ cleanup, live, observed }) => {
    const member = new ClosingMember({ scope, cleanup, live, arm, enforce, name: 'one' })
    await member.start()
    await member.delivery(RECORDS - COMMITTED_BEFORE)
    member.wire.hold(COORDINATOR_APIS)
    observed.closeMs = await member.close()
    observed.stopped = Object.fromEntries(
      COORDINATOR_APIS.map((api) => [api, member.wire.stoppedCount(api)])
    )
    expect(COORDINATOR_APIS.some((api) => member.wire.stoppedCount(api) > 0)).toBe(true)
    expect(observed.closeMs).toBeGreaterThanOrEqual(0)
    expect(observed.closeMs).toBeLessThan(CLOSE_TIMEOUT_MS + TEARDOWN_ALLOWANCE_MS)
    const framesAtClose = coordinatorFrames(member)
    await delay(scope, SETTLE_MS)
    expect(coordinatorFrames(member)).toEqual(framesAtClose)
    member.wire.release()
    observed.brokerAfter = await live.brokerOffset()
    expect(observed.brokerAfter).toBe(COMMITTED_BEFORE)
  })

/**
 * The handler-and-hook arm: a held handler, then a held close callback, each detached by a bounded
 * close; after the same visible group runs again on the same consumer, the released old callbacks
 * change nothing.
 * @param {string} title
 * @param {object} arm
 * @returns {(scope: AbortSignal) => Promise<void>}
 */
const detachedCase = (title, arm) => (scope) =>
  runCase(scope, title, arm, {}, async ({ cleanup, live, observed }) => {
    const member = new ClosingMember({ scope, cleanup, live, arm, enforce: true, name: 'one' })
    await member.start({ holdHandler: true })
    await pollUntil(scope, STEP_MS, () => member.handlerHeld === true, {
      label: 'The held handler',
    })
    observed.handlerCloseMs = await member.close()
    expect(observed.handlerCloseMs).toBeLessThan(CLOSE_TIMEOUT_MS + TEARDOWN_ALLOWANCE_MS)
    const deliveredBefore = member.delivered.length
    await member.start({ holdHook: true })
    await member.delivery(deliveredBefore + 1)
    member.releaseHandler()
    await pollUntil(scope, STEP_MS, () => member.detached.length >= 1, {
      label: 'The released handler',
    })
    expect(member.detached[0]).toEqual(['rejected', 'rejected'])
    await pollUntil(scope, STEP_MS, () => member.consumer.assignment().length > 0, {
      label: 'The replacement assignment',
    })
    observed.hookCloseMs = await member.close()
    expect(member.hookHeld).toBe(true)
    expect(observed.hookCloseMs).toBeLessThan(CLOSE_TIMEOUT_MS + TEARDOWN_ALLOWANCE_MS)
    await member.start()
    member.releaseHook()
    await pollUntil(scope, STEP_MS, () => member.detached.length >= 2, {
      label: 'The released hook',
    })
    expect(member.detached[1]).not.toBe('accepted')
    observed.detached = member.detached
    observed.brokerAfter = await live.brokerOffset()
    expect(observed.brokerAfter).toBeLessThanOrEqual(RECORDS)
    expect(observed.brokerAfter).not.toBe(99)
  })

/**
 * The unsent control: the final commit never leaves the process, so the broker keeps its offset.
 * @param {string} title
 * @param {object} arm
 * @returns {(scope: AbortSignal) => Promise<void>}
 */
const unsentCase = (title, arm) => (scope) =>
  runCase(scope, title, arm, {}, async ({ cleanup, live, observed }) => {
    const member = new ClosingMember({ scope, cleanup, live, arm, enforce: true, name: 'one' })
    await member.start()
    await member.delivery(RECORDS - COMMITTED_BEFORE)
    member.wire.hold(['OffsetCommit'])
    observed.closeMs = await member.close()
    observed.stopped = { OffsetCommit: member.wire.stoppedCount('OffsetCommit') }
    expect(member.wire.stoppedCount('OffsetCommit')).toBeGreaterThan(0)
    member.wire.release()
    observed.brokerAfter = await live.brokerOffset()
    expect(observed.brokerAfter).toBe(COMMITTED_BEFORE)
  })

/**
 * The written control: the final commit is on the wire and the broker applies it, but its answer
 * is lost, so the local close expires; the independent read after restoration sees the commit.
 * @param {string} title
 * @param {object} arm
 * @returns {(scope: AbortSignal) => Promise<void>}
 */
const writtenCase = (title, arm) => (scope) =>
  runCase(scope, title, arm, {}, async ({ cleanup, live, observed }) => {
    const member = new ClosingMember({ scope, cleanup, live, arm, enforce: true, name: 'one' })
    await member.start()
    await member.delivery(RECORDS - COMMITTED_BEFORE)
    const writtenBefore = member.wire.count('OffsetCommit')
    const stopLosing = member.wire.loseAnswers('OffsetCommit')
    observed.closeMs = await member.close()
    stopLosing()
    observed.written = member.wire.count('OffsetCommit') - writtenBefore
    expect(observed.written).toBeGreaterThan(0)
    expect(observed.closeMs).toBeLessThan(CLOSE_TIMEOUT_MS + TEARDOWN_ALLOWANCE_MS)
    await pollUntil(scope, STEP_MS, async () => (await live.brokerOffset()) === RECORDS, {
      label: 'The accepted commit',
    })
    observed.brokerAfter = RECORDS
  })

/**
 * The positive control: no fault, the graceful close commits the processed prefix and leaves once.
 * @param {string} title
 * @param {object} arm
 * @returns {(scope: AbortSignal) => Promise<void>}
 */
const gracefulCase = (title, arm) => (scope) =>
  runCase(scope, title, arm, {}, async ({ cleanup, live, observed }) => {
    const member = new ClosingMember({ scope, cleanup, live, arm, enforce: true, name: 'one' })
    await member.start()
    await member.delivery(RECORDS - COMMITTED_BEFORE)
    expect(await live.memberCount()).toBe(1)
    observed.closeMs = await member.close(10000)
    expect(observed.closeMs).toBeLessThan(10000 + TEARDOWN_ALLOWANCE_MS)
    observed.brokerAfter = await live.brokerOffset()
    expect(observed.brokerAfter).toBe(RECORDS)
    observed.leaves = member.wire.count('LeaveGroup')
    if (arm.protocol === 'classic') expect(observed.leaves).toBe(1)
    await pollUntil(scope, STEP_MS, async () => (await live.memberCount()) === 0, {
      label: 'The group without the member',
    })
  })

describeClose('Consumer > bounded close (live)', () => {
  const add = (title, body) =>
    test(title, liveCase(CASE_BUDGET_MS, body), caseTimeout(CASE_BUDGET_MS))

  add(
    'classic eager close fences a blackholed coordinator within the deadline',
    blackholeCase(
      'classic eager close fences a blackholed coordinator within the deadline',
      ARMS.eager,
      true
    )
  )
  add(
    'classic cooperative close fences a blackholed coordinator within the deadline',
    blackholeCase(
      'classic cooperative close fences a blackholed coordinator within the deadline',
      ARMS.cooperative,
      true
    )
  )
  add(
    'consumer protocol close fences a blackholed coordinator within the deadline',
    blackholeCase(
      'consumer protocol close fences a blackholed coordinator within the deadline',
      ARMS.consumer,
      true
    )
  )
  add(
    'classic eager close fences a blackholed coordinator with request timeout enforcement off',
    blackholeCase(
      'classic eager close fences a blackholed coordinator with request timeout enforcement off',
      ARMS.eager,
      false
    )
  )
  add(
    'consumer protocol close fences a blackholed coordinator with request timeout enforcement off',
    blackholeCase(
      'consumer protocol close fences a blackholed coordinator with request timeout enforcement off',
      ARMS.consumer,
      false
    )
  )
  add(
    'classic eager close detaches a held handler and hook without touching a replacement run',
    detachedCase(
      'classic eager close detaches a held handler and hook without touching a replacement run',
      ARMS.eager
    )
  )
  add(
    'classic cooperative close detaches a held handler and hook without touching a replacement run',
    detachedCase(
      'classic cooperative close detaches a held handler and hook without touching a replacement run',
      ARMS.cooperative
    )
  )
  add(
    'consumer protocol close detaches a held handler and hook without touching a replacement run',
    detachedCase(
      'consumer protocol close detaches a held handler and hook without touching a replacement run',
      ARMS.consumer
    )
  )
  add(
    'an unsent final commit leaves the broker offset unchanged',
    unsentCase('an unsent final commit leaves the broker offset unchanged', ARMS.eager)
  )
  add(
    'a written commit whose answer is lost is observed at the broker after restoration',
    writtenCase(
      'a written commit whose answer is lost is observed at the broker after restoration',
      ARMS.eager
    )
  )
  add(
    'classic eager graceful close commits the processed prefix and leaves once',
    gracefulCase(
      'classic eager graceful close commits the processed prefix and leaves once',
      ARMS.eager
    )
  )
  add(
    'classic cooperative graceful close commits the processed prefix and leaves once',
    gracefulCase(
      'classic cooperative graceful close commits the processed prefix and leaves once',
      ARMS.cooperative
    )
  )
  add(
    'consumer protocol graceful close commits the processed prefix and leaves once',
    gracefulCase(
      'consumer protocol graceful close commits the processed prefix and leaves once',
      ARMS.consumer
    )
  )
})
