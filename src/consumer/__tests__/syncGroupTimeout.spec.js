const { performance: importedClock } = require('node:perf_hooks')
const { requests, lookup } = require('../../protocol/requests')
const apiKeys = require('../../protocol/requests/apiKeys')
const RequestQueue = require('../../network/requestQueue')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { newLogger } = require('testHelpers')
const { Kafka } = require('../../../index')
const { createScriptedCluster, groupScript } = require('../../../testHelpers/scriptedCluster')
const ConsumerGroup = require('../consumerGroup')
const { TOPIC } = require('./fixtures/fetchHarness')
const { WireTranscript } = require('./fixtures/rackWire')

const versions = {
  [apiKeys.JoinGroup]: { minVersion: 0, maxVersion: 5 },
  [apiKeys.SyncGroup]: { minVersion: 0, maxVersion: 3 },
}

const joinGroupTimeout = ({ rebalanceTimeout, sessionTimeout }) =>
  lookup(versions)(apiKeys.JoinGroup, requests.JoinGroup)({
    groupId: 'g',
    sessionTimeout,
    rebalanceTimeout,
    memberId: '',
    groupProtocols: [],
  }).requestTimeout

const syncGroupTimeout = ({ rebalanceTimeout, sessionTimeout }) =>
  lookup(versions)(apiKeys.SyncGroup, requests.SyncGroup)({
    groupId: 'g',
    generationId: 1,
    memberId: 'm',
    groupAssignment: [],
    sessionTimeout,
    rebalanceTimeout,
  }).requestTimeout

describe('Consumer > derived group request timeouts', () => {
  const SESSION_TIMEOUT = 30000

  it('derives 305000 / 305000 at the defaults', () => {
    expect(joinGroupTimeout({ rebalanceTimeout: 300000, sessionTimeout: SESSION_TIMEOUT })).toEqual(
      305000
    )
    expect(syncGroupTimeout({ rebalanceTimeout: 300000, sessionTimeout: SESSION_TIMEOUT })).toEqual(
      305000
    )
  })

  it('derives 125000 for a rebalanceTimeout of 120000', () => {
    expect(syncGroupTimeout({ rebalanceTimeout: 120000, sessionTimeout: SESSION_TIMEOUT })).toEqual(
      125000
    )
  })

  it('derives 35000 / 35000 for a maxPollInterval of 30000', () => {
    expect(joinGroupTimeout({ rebalanceTimeout: 30000, sessionTimeout: SESSION_TIMEOUT })).toEqual(
      35000
    )
    expect(syncGroupTimeout({ rebalanceTimeout: 30000, sessionTimeout: SESSION_TIMEOUT })).toEqual(
      35000
    )
  })

  it('keeps the falsy fallback for rebalanceTimeout 0', () => {
    // `||`, not `??`: a configured 0 must still fall back to sessionTimeout, exactly as it did
    // before this release. `??` would produce 5000 here.
    expect(joinGroupTimeout({ rebalanceTimeout: 0, sessionTimeout: SESSION_TIMEOUT })).toEqual(
      35000
    )
    expect(syncGroupTimeout({ rebalanceTimeout: 0, sessionTimeout: SESSION_TIMEOUT })).toEqual(
      35000
    )
  })

  it('falls back to the raw timeout when +5000 is not a safe integer', () => {
    expect(
      syncGroupTimeout({
        rebalanceTimeout: Number.MAX_SAFE_INTEGER,
        sessionTimeout: SESSION_TIMEOUT,
      })
    ).toEqual(Number.MAX_SAFE_INTEGER)
  })

  it('keeps the larger requestTimeout', () => {
    // Drives the real coalescing expression, `requestQueue/index.js:118-123`'s
    // `Math.max(defaultRequestTimeout, customRequestTimeout || 0)`, through the method that owns
    // it. Asserting on the constructor argument instead would compare 30000 with 25000 and could
    // never fail.
    const queue = new RequestQueue({
      instrumentationEmitter: new InstrumentationEventEmitter(),
      maxInFlightRequests: null,
      requestTimeout: 30000,
      enforceRequestTimeout: false,
      clientId: 'timeouts',
      broker: 'localhost:9092',
      logger: newLogger(),
    })

    const socketRequestFor = (customRequestTimeout) =>
      queue.createSocketRequest({
        entry: {
          apiKey: 14,
          apiName: 'SyncGroup',
          apiVersion: 3,
          correlationId: 1,
          resolve() {},
          reject() {},
        },
        expectResponse: true,
        requestTimeout: customRequestTimeout,
        sendRequest: () => {},
      })

    // A SyncGroup allowance below the client default is raised to it, never shortened...
    const derived = syncGroupTimeout({ rebalanceTimeout: 20000, sessionTimeout: SESSION_TIMEOUT })
    expect(derived).toEqual(25000)
    expect(socketRequestFor(derived).requestTimeout).toEqual(30000)

    // ...and the default allowance above it is honoured as it is.
    const atDefaults = syncGroupTimeout({
      rebalanceTimeout: 300000,
      sessionTimeout: SESSION_TIMEOUT,
    })
    expect(socketRequestFor(atDefaults).requestTimeout).toEqual(305000)
  })
})

/**
 * Puts the imported `node:perf_hooks` clock ten minutes ahead of real time, as in a Jest worker
 * that has already run many suites, so a client that reads it instead of the faked global clock
 * fails on every host.
 * Callers restore with `jest.restoreAllMocks()`.
 * @returns {void}
 */
const ageWorker = () => {
  const real = importedClock.now.bind(importedClock)
  jest.spyOn(importedClock, 'now').mockImplementation(() => real() + 600000)
}

/**
 * Advances fake time in 10 ms steps until the predicate holds.
 * @param {() => boolean} predicate
 * @param {string} label names the awaited boundary in the failure
 * @param {number} [limitMs=60000] the most fake time to spend
 * @returns {Promise<void>}
 * @throws {Error} when the predicate still fails after `limitMs`
 */
const settle = async (predicate, label, limitMs = 60000) => {
  for (let spent = 0; spent <= limitMs; spent += 10) {
    if (predicate()) return
    await jest.advanceTimersByTimeAsync(10)
  }
  throw new Error(`Did not settle: ${label}`)
}

/**
 * Awaits a promise while fake time advances, so timers it waits on can fire.
 * @param {Promise<*>} promise
 * @param {string} label
 * @returns {Promise<number>} the fake milliseconds it took
 */
const settled = async (promise, label) => {
  const start = Date.now()
  let done = false
  const watched = promise.finally(() => {
    done = true
  })
  await settle(() => done, label)
  await watched
  return Date.now() - start
}

describe('Consumer > a parked SyncGroup over the wire', () => {
  afterEach(() => {
    jest.restoreAllMocks()
    jest.useRealTimers()
  })

  /**
   * Runs one public consumer at the client default `requestTimeout` against a scripted
   * coordinator that parks the first SyncGroup, then closes it and checks no timer is left.
   * @param {object} options consumer options over the defaults
   * @param {(member: object) => Promise<void>} work schedule and assertions
   * @returns {Promise<void>}
   */
  const withParkedSync = async (options, work) => {
    ageWorker()
    jest.useFakeTimers({ doNotFake: ['setImmediate'] })
    const seam = groupScript(
      createScriptedCluster({ nodes: [{ id: 0, host: 'node', port: 9092 }] }),
      { batches: 0 }
    )
    const gate = seam.schedule.hold({ api: 'SyncGroup' })
    const consumer = new Kafka(seam.kafkaOptions({ requestTimeout: 30000 })).consumer({
      groupId: 'parked-sync',
      ...options,
    })
    const timeouts = []
    const crashes = []
    const joins = []
    const removers = [
      consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => joins.push(payload)),
      consumer.on(consumer.events.REQUEST_TIMEOUT, ({ payload }) => timeouts.push(payload)),
      consumer.on(consumer.events.CRASH, ({ payload }) => crashes.push(payload)),
    ]
    const syncs = () => seam.requests().filter(({ api }) => api === 'SyncGroup')
    let closed = false
    try {
      await consumer.connect()
      await consumer.subscribe({ topic: TOPIC })
      const running = consumer.run({ eachMessage: async () => {} })
      await settle(() => gate.call != null, 'parked SyncGroup')
      await work({ gate, consumer, running, timeouts, crashes, joins, syncs })
      closed = true
    } finally {
      if (gate.settle) gate.release()
      if (!closed) await settled(consumer.disconnect({ timeout: 0 }), 'cleanup close')
      removers.forEach((remove) => remove())
    }
    expect(jest.getTimerCount()).toBe(0)
  }

  it('a SyncGroup answer 70 s after send resolves at the defaults (fake time)', async () => {
    await withParkedSync(
      {},
      async ({ gate, consumer, running, timeouts, crashes, joins, syncs }) => {
        await jest.advanceTimersByTimeAsync(70000)
        expect(timeouts).toEqual([])
        gate.release()
        await settled(running, 'joined member')
        expect(joins).toHaveLength(1)
        expect(timeouts).toEqual([])
        expect(crashes).toEqual([])
        expect(syncs()).toHaveLength(1)
        await settled(consumer.disconnect(), 'close')
      }
    )
  })

  /**
   * Parks the only SyncGroup of a single-round consumer and checks that the round's own fence
   * closes at its deadline, not before and not with the later request allowance, then that the
   * consumer crashes with the deadline error once cleanup is done.
   * @param {object} options consumer options over the defaults
   * @param {number} budgetMs the round budget the options give
   * @returns {Promise<void>}
   */
  const roundEndsParkedSync = (options, budgetMs) => {
    const begin = ConsumerGroup.prototype.beginRebalanceRound
    const rounds = []
    jest.spyOn(ConsumerGroup.prototype, 'beginRebalanceRound').mockImplementation(function () {
      const round = begin.call(this)
      rounds.push({ round, at: Date.now() })
      return round
    })
    return withParkedSync(
      { ...options, retry: { retries: 0, restartOnFailure: async () => false } },
      async ({ gate, consumer, timeouts, crashes, joins, syncs }) => {
        expect(rounds).toHaveLength(1)
        const [{ round, at }] = rounds
        await jest.advanceTimersByTimeAsync(at + budgetMs - 1 - Date.now())
        expect(round.fence.closed).toBe(false)
        expect(crashes).toEqual([])
        expect(timeouts).toEqual([])

        await jest.advanceTimersByTimeAsync(1)
        expect(round.fence.closed).toBe(true)
        expect(round.fence.error).toMatchObject({
          type: 'JOIN_DEADLINE_EXCEEDED',
          message: `Timed out joining the group after ${budgetMs} ms`,
        })
        expect(gate.settle).not.toBeNull()
        expect(syncs()).toHaveLength(1)

        // The crash is published after cleanup, which may wait on the in-flight SyncGroup.
        await settle(() => crashes.length > 0, 'crash after the round deadline', 10000)
        expect(crashes[0].error).toMatchObject({ type: 'JOIN_DEADLINE_EXCEEDED' })
        expect(joins).toEqual([])
        expect(rounds).toHaveLength(1)
        await settled(consumer.disconnect(), 'close')
      }
    )
  }

  it('the round deadline still ends a SyncGroup that outlives the round (fake time)', async () => {
    // A 100 s round gives the SyncGroup a 105 s allowance: only the round can end it at 100 s.
    await roundEndsParkedSync({ maxPollInterval: 100000 }, 100000)
  })

  it('an unanswered SyncGroup at the defaults ends with the 300 s round, not its 305 s allowance (fake time)', async () => {
    await roundEndsParkedSync({}, 300000)
  })

  it('disconnect during a parked SyncGroup settles within the close bound (fake time)', async () => {
    await withParkedSync({}, async ({ consumer, timeouts, syncs }) => {
      await jest.advanceTimersByTimeAsync(70000)
      expect(timeouts).toEqual([])
      expect(syncs()).toHaveLength(1)
      const took = await settled(consumer.disconnect({ timeout: 5000 }), 'bounded close')
      expect(took).toBeLessThanOrEqual(5000)
    })
  })
})

describe('Consumer > JoinGroup attempts on the wire', () => {
  afterEach(() => {
    jest.restoreAllMocks()
    jest.useRealTimers()
  })

  /** @param {WireTranscript} wire @param {number} apiKey @returns {number} frames written */
  const written = (wire, apiKey) => wire.exchanges.filter((x) => x.apiKey === apiKey).length

  it('a JoinGroup retried inside one round is counted on the wire but adds no GROUP_JOIN (fake time)', async () => {
    ageWorker()
    jest.useFakeTimers({ doNotFake: ['setImmediate'] })
    const seam = groupScript(
      createScriptedCluster({ nodes: [{ id: 0, host: 'node', port: 9092 }] }),
      { batches: 0 }
    )
    seam.schedule.once({ api: 'JoinGroup' }, { errorCode: 27 })
    const wire = new WireTranscript(seam.socketFactory)
    const consumer = new Kafka(seam.kafkaOptions({ socketFactory: wire.socketFactory })).consumer({
      groupId: 'retried-join',
    })
    const joins = []
    const remove = consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => joins.push(payload))
    try {
      await consumer.connect()
      await consumer.subscribe({ topic: TOPIC })
      await settled(consumer.run({ eachMessage: async () => {} }), 'joined member')

      expect(joins).toHaveLength(1)
      expect(seam.requests().filter(({ api }) => api === 'JoinGroup')).toHaveLength(2)
      expect(written(wire, apiKeys.JoinGroup)).toBe(2)
      expect(written(wire, apiKeys.SyncGroup)).toBe(1)
    } finally {
      await settled(consumer.disconnect(), 'close')
      remove()
    }
    expect(jest.getTimerCount()).toBe(0)
  })
})
