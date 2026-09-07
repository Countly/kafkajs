const { Kafka, logLevel } = require('../../../index')
const { secureRandom, plainTextBrokers, createTopic } = require('testHelpers')

// Gated: each row parks a member for forty seconds and would race the other consumer specs. Run
// them alone: `npm run test:e2e:long-handler`.
const describeLongHandler = process.env.LONG_HANDLER_E2E === '1' ? describe : describe.skip

// Named so each wait says how long it is prepared to be wrong for.
const ROW_BUDGET_MS = 120000
const SESSION_TIMEOUT_MS = 10000
const HEARTBEAT_INTERVAL_MS = 3000
const GENEROUS_POLL_INTERVAL_MS = 120000
const TIGHT_POLL_INTERVAL_MS = 15000
const HANDLER_MS = 40000
const MAX_WAIT_TIME_MS = 1000
// The timer schedules the next tick from the completion of the last one, so a gap is one interval
// plus one round trip. Anything below the session timeout keeps the member alive.
const MAX_HEARTBEAT_GAP_MS = SESSION_TIMEOUT_MS - 2000
const JOIN_BUDGET = { attempts: 120, delay: 500 } // 60 s
const HANDLER_BUDGET = { attempts: 180, delay: 500 } // 90 s - the handler plus its scheduling
const DELIVERY_BUDGET = { attempts: 120, delay: 500 } // 60 s - records already on the broker
const DISCONNECT_BUDGET_MS = 15000

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

/**
 * Polls `predicate` on a plain interval until it is true, bounded rather than indefinite.
 * @param {() => boolean|Promise<boolean>} predicate
 * @param {{attempts: number, delay: number}} budget
 * @returns {Promise<boolean>} whether `predicate` became true within the attempt budget
 */
const waitForCondition = async (predicate, budget) => {
  for (let attempt = 0; attempt < budget.attempts; attempt++) {
    if (await predicate()) {
      return true
    }
    await sleep(budget.delay)
  }
  return false
}

/**
 * Awaits `promise` but gives up after `ms`, so a teardown can never outlive its hook budget.
 * @param {Promise<unknown>} promise
 * @param {number} ms
 * @returns {Promise<void>}
 */
const withTimeout = async (promise, ms) => {
  await Promise.race([promise, sleep(ms)])
}

/**
 * The largest gap between consecutive timestamps, and `null` for fewer than two of them.
 * @param {number[]} timestamps
 * @returns {number|null}
 */
const largestGap = (timestamps) =>
  timestamps.length < 2
    ? null
    : Math.max(...timestamps.slice(1).map((at, index) => at - timestamps[index]))

/**
 * Live confirmation of what a long handler costs, in both directions.
 *
 * A handler that **awaits** is not evicted: heartbeats run on a timer the runner owns, so they
 * keep going while the handler is parked, and only the poll deadline - which this row sets far
 * above the handler's duration - could end the membership.
 *
 * A handler that **blocks the event loop** still is. Heartbeats share the handler's event loop,
 * so a non-yielding loop starves the timer exactly as it starves everything else; the member's
 * session expires and even the poll guard cannot fire, because the guard is itself a timer. This
 * is the documented limit, asserted rather than described.
 *
 * @see https://kafka.apache.org/43/documentation.html#consumerconfigs_session.timeout.ms
 */
describeLongHandler('Consumer > long handler (live)', () => {
  let kafka
  let producer
  let consumer

  afterEach(async () => {
    consumer &&
      (await withTimeout(
        consumer.disconnect().catch(() => {}),
        DISCONNECT_BUDGET_MS
      ))
    producer &&
      (await withTimeout(
        producer.disconnect().catch(() => {}),
        DISCONNECT_BUDGET_MS
      ))
    consumer = producer = kafka = null
  }, 60000)

  /**
   * Creates the row's topic, produces one record to it and starts a consumer whose first delivery
   * runs `firstDelivery`; every later one returns immediately.
   * @param {object} options
   * @param {string} options.clientId
   * @param {number} options.maxPollInterval
   * @param {(observed: object) => Promise<void>|void} options.firstDelivery
   * @returns {Promise<{topic: string, groupId: string, observed: object}>}
   */
  const startScenario = async ({ clientId, maxPollInterval, firstDelivery }) => {
    const topic = `test-topic-${secureRandom()}`
    const groupId = `test-group-${secureRandom()}`
    await createTopic({ topic, partitions: 1 })

    kafka = new Kafka({ clientId, brokers: plainTextBrokers(), logLevel: logLevel.NOTHING })

    producer = kafka.producer()
    await producer.connect()
    await producer.send({ topic, messages: [{ partition: 0, key: 'k', value: 'v' }] })

    consumer = kafka.consumer({
      groupId,
      sessionTimeout: SESSION_TIMEOUT_MS,
      heartbeatInterval: HEARTBEAT_INTERVAL_MS,
      maxPollInterval,
      maxWaitTimeInMs: MAX_WAIT_TIME_MS,
    })

    const observed = {
      heartbeats: [],
      rebalancings: [],
      groupJoins: [],
      expiries: [],
      crashes: [],
      received: [],
      startedAt: null,
      finishedAt: null,
    }

    consumer.on(consumer.events.HEARTBEAT, ({ timestamp }) => observed.heartbeats.push(timestamp))
    consumer.on(consumer.events.REBALANCING, ({ timestamp, payload }) =>
      observed.rebalancings.push({ timestamp, payload })
    )
    consumer.on(consumer.events.GROUP_JOIN, ({ timestamp, payload }) =>
      observed.groupJoins.push({ timestamp, payload })
    )
    consumer.on(consumer.events.POLL_INTERVAL_EXCEEDED, ({ timestamp, payload }) =>
      observed.expiries.push({ timestamp, payload })
    )
    consumer.on(consumer.events.CRASH, ({ payload }) => observed.crashes.push(payload))

    await consumer.connect()
    await consumer.subscribe({ topic, fromBeginning: true })
    consumer
      .run({
        partitionsConsumedConcurrently: 1,
        eachMessage: async ({ partition, message }) => {
          observed.received.push(`${partition}:${message.offset}`)

          if (observed.startedAt == null) {
            observed.startedAt = Date.now()
            await firstDelivery(observed)
            observed.finishedAt = Date.now()
          }
        },
      })
      .catch((e) => observed.crashes.push({ error: e }))

    expect(await waitForCondition(() => observed.groupJoins.length > 0, JOIN_BUDGET)).toBe(true)
    return { topic, groupId, observed }
  }

  it(
    'keeps an awaiting handler in the group, heartbeating throughout',
    async () => {
      const { topic, groupId, observed } = await startScenario({
        clientId: 'long-handler-awaiting',
        maxPollInterval: GENEROUS_POLL_INTERVAL_MS,
        firstDelivery: () => sleep(HANDLER_MS),
      })

      const memberIdBefore = observed.groupJoins.at(-1).payload.memberId
      const rebalancingsBefore = observed.rebalancings.length

      expect(await waitForCondition(() => observed.finishedAt != null, HANDLER_BUDGET)).toBe(true)
      expect(observed.finishedAt - observed.startedAt).toBeGreaterThanOrEqual(HANDLER_MS)

      const duringHandler = observed.heartbeats.filter(
        (at) => at >= observed.startedAt && at <= observed.finishedAt
      )

      // Heartbeats keep going at roughly the configured interval while the handler is parked.
      expect(duringHandler.length).toBeGreaterThanOrEqual(
        Math.floor(HANDLER_MS / HEARTBEAT_INTERVAL_MS) - 3
      )
      expect(largestGap(duringHandler)).toBeLessThanOrEqual(MAX_HEARTBEAT_GAP_MS)

      // Not evicted: no rebalance was triggered, and the member id never changed.
      expect(observed.rebalancings.length).toEqual(rebalancingsBefore)
      expect(observed.expiries).toEqual([])
      expect(observed.groupJoins.at(-1).payload.memberId).toEqual(memberIdBefore)
      expect(observed.crashes).toEqual([])

      const deliveredBefore = observed.received.length
      await producer.send({ topic, messages: [{ partition: 0, key: 'k2', value: 'v2' }] })
      expect(
        await waitForCondition(() => observed.received.length > deliveredBefore, DELIVERY_BUDGET)
      ).toBe(true)

      console.log('long awaiting handler survived', {
        topic,
        groupId,
        handlerDurationMs: observed.finishedAt - observed.startedAt,
        heartbeatsDuringHandler: duringHandler.length,
        largestHeartbeatGapMs: largestGap(duringHandler),
        rebalancings: observed.rebalancings.length,
      })
    },
    ROW_BUDGET_MS
  )

  it(
    'documents the limit: a handler that blocks the event loop is evicted, and the poll guard cannot fire',
    async () => {
      const { topic, groupId, observed } = await startScenario({
        clientId: 'long-handler-busy-loop',
        maxPollInterval: TIGHT_POLL_INTERVAL_MS,
        firstDelivery: () => {
          const startedAt = performance.now()
          while (performance.now() - startedAt < HANDLER_MS) {
            // Deliberately non-yielding: nothing else on this event loop runs, heartbeat timer
            // and poll timer included.
          }
        },
      })

      // Read after a join wait that the busy loop may have delayed past its own end, so state
      // explicitly that no rejoin can have landed yet and this is the pre-loop member id.
      expect(observed.groupJoins.length).toEqual(1)
      const memberIdBefore = observed.groupJoins.at(-1).payload.memberId

      expect(await waitForCondition(() => observed.finishedAt != null, HANDLER_BUDGET)).toBe(true)
      expect(observed.finishedAt - observed.startedAt).toBeGreaterThanOrEqual(HANDLER_MS)

      // Nothing at all ran while the loop held the thread: no heartbeat went out, and the poll
      // guard - a timer like every other - could not fire either, though its deadline had passed.
      const heartbeatsDuringLoop = observed.heartbeats.filter(
        (at) => at > observed.startedAt && at < observed.finishedAt
      )
      const expiriesDuringLoop = observed.expiries.filter(
        ({ timestamp }) => timestamp > observed.startedAt && timestamp < observed.finishedAt
      )
      expect(heartbeatsDuringLoop).toEqual([])
      expect(expiriesDuringLoop).toEqual([])
      expect(observed.finishedAt - observed.startedAt).toBeGreaterThan(TIGHT_POLL_INTERVAL_MS)

      // The session expired while the thread was held, so the membership is gone: the consumer
      // rebalances and comes back under a new member id.
      const rebalanced = await waitForCondition(
        () => observed.rebalancings.some(({ timestamp }) => timestamp >= observed.finishedAt),
        DELIVERY_BUDGET
      )
      expect(rebalanced).toBe(true)

      const rejoined = await waitForCondition(
        () =>
          observed.groupJoins.some(
            ({ timestamp, payload }) =>
              timestamp >= observed.finishedAt && payload.memberId !== memberIdBefore
          ),
        DELIVERY_BUDGET
      )
      expect(rejoined).toBe(true)

      const deliveredBefore = observed.received.length
      await producer.send({ topic, messages: [{ partition: 0, key: 'k2', value: 'v2' }] })
      expect(
        await waitForCondition(() => observed.received.length > deliveredBefore, DELIVERY_BUDGET)
      ).toBe(true)

      console.log('busy-loop handler evicted', {
        topic,
        groupId,
        handlerDurationMs: observed.finishedAt - observed.startedAt,
        heartbeatsDuringLoop: heartbeatsDuringLoop.length,
        pollGuardFiredDuringLoop: expiriesDuringLoop.length,
        rebalanceReasonsAfterLoop: observed.rebalancings
          .filter(({ timestamp }) => timestamp >= observed.finishedAt)
          .map(({ payload }) => payload.reason),
        memberIdBefore,
        memberIdAfter: observed.groupJoins.at(-1).payload.memberId,
      })
    },
    ROW_BUDGET_MS
  )
})
