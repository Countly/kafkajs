const { EventEmitter } = require('events')
const {
  ShareSoak,
  SharePriming,
  ShareSoakDriver,
  ShareSoakScreen,
  instrumentShare,
  main,
} = require('../soak')
const fixtures = require('../../../testHelpers/shareFixtures')
const helpers = require('../../../testHelpers')
const bounded = require('../../../src/producer/__tests__/fixtures/bounded')
const { CleanupStack } = require('../../../src/producer/__tests__/fixtures/liveProbe')

/**
 * Focuses on original cleanup stages, stack, probes and fixture proofs with scripted admin I/O.
 * Completion is bypassed here; complete-evidence receipt controls live in the owner spec.
 * @param {object} [options] injected workload and cleanup failures
 * @param {boolean} [options.exceptional] fail immediately after setup's baseline
 * @param {boolean} [options.cleanupFailure] fail deletion after a workload failure
 * @param {Function} [options.afterSetup] inject retained resources after the original baseline
 * @param {boolean} [options.screen] apply the original quiet-resource screen after cleanup
 * @param {boolean} [options.shortFailureBound=true] keep resource-negative polling short
 * @param {Error} [options.primary] original workload failure
 * @param {Error} [options.deletionError] original deletion failure
 * @returns {object} original owner, serializing archive, observations and owned admin
 * @throws {Error} when an original instrumentation source is unavailable
 */
const cleanupOwner = ({
  exceptional = false,
  cleanupFailure = false,
  afterSetup = () => {},
  screen = false,
  shortFailureBound = true,
  primary = new Error('workload failed'),
  deletionError = new Error('topic deletion failed'),
  Owner = ShareSoak,
} = {}) => {
  const rows = []
  const observations = []
  const events = new EventEmitter()
  const admin = {
    events: { REQUEST: 'request' },
    on: (name, listener) => {
      events.on(name, listener)
      return () => events.off(name, listener)
    },
    connect: jest.fn(async () => {}),
    disconnect: jest.fn(async () => {}),
    createTopics: jest.fn(async () => {}),
    deleteTopics: jest.fn(async () => {
      if (cleanupFailure) throw deletionError
    }),
    deleteGroups: jest.fn(async () => {}),
    listGroups: jest.fn(async () => ({ groups: [] })),
    describeShareGroups: jest.fn(async (groups) => ({
      groups: groups.map((groupId) => ({ groupId, errorCode: 69 })),
    })),
    describeCluster: jest.fn(async () => ({ brokers: [{ nodeId: 0, host: 'scripted', port: 1 }] })),
    fetchTopicMetadata: jest.fn(async ({ topics }) => ({
      topics: [{ name: topics[0], topicId: 'id', partitions: [{ partitionId: 0, leader: 0 }] }],
    })),
    listTopics: jest.fn(async () => {
      events.emit('request', { payload: { apiName: 'Metadata', broker: 'scripted:1' } })
      return []
    }),
  }
  const instrumented = instrumentShare(
    {
      brokers: ['scripted:1'],
      socketFactory: () => Object.assign(new EventEmitter(), { destroyed: false }),
    },
    { gc: null }
  )
  const shares = instrumented.shareResources
  const snapshot = shares.snapshot
  shares.snapshot = async () => {
    const reading = await snapshot()
    reading.heapUsed ??= process.memoryUsage().heapUsed
    observations.push(reading)
    return reading
  }
  const driver = new ShareSoakDriver({
    kafka: { admin: () => admin },
    brokers: ['scripted:1'],
    shares,
    tap: {},
    ports: {
      bounded,
      fixtures: {
        ...fixtures,
        setShareGroupConfigs: async () => {},
        resetShareGroupConfigs: async () => {},
      },
    },
  })
  driver.readConfiguration = async () => ({})
  driver.final = jest.fn(async () => ({}))
  const owner = new Owner({
    driver,
    kafka: {},
    shares,
    stack: new CleanupStack(),
    short: true,
    gate: new bounded.LiveRun(),
    evidence: {
      write: (row) => rows.push(JSON.parse(JSON.stringify(row))),
      read: () => rows,
    },
  })
  owner.soak = () => ({
    run: async () => {
      await afterSetup({ owner, driver, instrumented })
      if (exceptional) {
        if (shortFailureBound) owner.workDue = Math.min(owner.workDue, owner.clock.now() + 1000)
        throw primary
      }
      await owner.finish()
      const violations = []
      if (screen) ShareSoakScreen.quiet(driver.after, driver.baseline, 'teardown', violations)
      return { ok: violations.length === 0, violations, measured: {} }
    },
  })
  owner.prime = async () => {
    owner.priming = { status: 'passed', topic: 'primed', groupId: 'primed', topicId: 'primed' }
  }
  owner.complete = (resources) => resources
  return { owner, driver, admin, shares, rows, observations }
}

/**
 * Selects referenced bound allocations with their complete callers, excluding Jest's own timer.
 * @param {object} snapshot original same-turn probe result
 * @returns {object[]} referenced timers allocated by the original bounded helper
 */
const wrappers = (snapshot) =>
  snapshot.timerOwners.afterCollection.rows.filter(
    ({ type, hasRef, stack }) =>
      type === 'Timeout' && hasRef && stack.includes('/fixtures/bounded.js:')
  )

describe('share owner cleanup populations', () => {
  beforeEach(() => jest.spyOn(helpers, 'disconnectPropagationBrokers').mockResolvedValue())
  afterEach(() => {
    jest.restoreAllMocks()
    bounded.liveRun.reset()
  })

  test.each([false, true])(
    'matches original nested priming populations inside real Jest liveCase and outer cleanup, exceptional=%s',
    bounded.liveCase(30000, async (_scope, exceptional) => {
      await bounded.pollUntil(
        _scope,
        2000,
        // eslint-disable-next-line n/no-unsupported-features/node-builtins -- tests observe the original resource source on Node 22+
        () => !process.getActiveResourcesInfo().includes('ProcessWrap'),
        { delayMs: 10, label: 'Jest bootstrap children closing before resource ownership' }
      )
      await new Promise(setImmediate)
      const {
        owner: prime,
        driver,
        shares,
        rows,
      } = cleanupOwner({ Owner: SharePriming, exceptional, shortFailureBound: false })
      const parent = new ShareSoak({
        driver: {},
        kafka: {},
        shares,
        short: true,
        stack: new CleanupStack(),
        gate: prime.gate,
        evidence: { read: () => rows, write: (row) => rows.push(JSON.parse(JSON.stringify(row))) },
        createPriming: () => prime,
      })
      parent.prepare = jest.fn(async () => {})
      parent.soak = () => ({ run: async () => ({ ok: true, violations: [] }) })
      parent.complete = (resources) => resources
      const outer = new CleanupStack()
      outer.add('close the original two-scope owner', () => parent.close())
      let failure
      try {
        await outer.run(
          async () => {
            try {
              await parent.run()
            } catch (error) {
              failure = error
            }
          },
          { budgetMs: 20000 }
        )
        if (exceptional) {
          expect(failure?.message).toBe('workload failed')
          expect(failure.cleanupErrors).toBeUndefined()
          expect(parent.prepare).not.toHaveBeenCalled()
          expect(rows.some((r) => r.event === 'priming-complete')).toBe(false)
        } else {
          expect(failure).toBeUndefined()
          expect(parent.prepare).toHaveBeenCalledTimes(1)
          expect(parent.priming.status).toBe('passed')
        }
        expect(driver.after.activeResources).toEqual(driver.baseline.activeResources)
        expect(driver.after.activeResourcesBeforeCollection).toEqual(
          driver.baseline.activeResourcesBeforeCollection
        )
        expect(wrappers(driver.after)).toHaveLength(wrappers(driver.baseline).length)
        expect(wrappers(driver.after).length).toBeGreaterThan(4)
        expect(driver.after.timerOwners.afterCollection.dropped).toBe(0)
        expect(driver.after.timerOwners.afterCollection.unattributed).toEqual(
          driver.baseline.timerOwners.afterCollection.unattributed
        )
        expect(prime.stack.cleanups).toEqual([])
        expect(parent.closed).toBe(true)
      } finally {
        shares.restore()
      }
    }),
    bounded.caseTimeout(30000)
  )

  test.each([false, true])(
    'restores raw resources with individually observed wrapper owners, exceptional=%s',
    async (exceptional) => {
      const fixture = cleanupOwner({ exceptional })
      const { owner, driver, shares, observations } = fixture
      try {
        if (exceptional) await expect(owner.run()).rejects.toThrow('workload failed')
        else await owner.run()
        const workDue = owner.workDue
        await owner.close()
        expect(owner.workDue).toBe(workDue)
        expect(owner.cleanupDue).toBeLessThanOrEqual(workDue)
        expect(wrappers(driver.baseline)).toHaveLength(4)
        expect(wrappers(driver.after)).toHaveLength(4)
        expect(driver.after.activeResources).toEqual(driver.baseline.activeResources)
        expect(driver.after.activeResourcesBeforeCollection).toEqual(
          driver.baseline.activeResourcesBeforeCollection
        )
        for (const point of [driver.baseline, driver.after]) {
          const owners = wrappers(point)
          expect(new Set(owners.map(({ asyncId }) => asyncId)).size).toBe(4)
          const callers = owners.map(({ stack }) => {
            const frames = stack.split('\n')
            const caller =
              frames[frames.findIndex((frame) => frame.includes('/fixtures/bounded.js:')) + 1]
            if (/ShareSoak\.(?:bounded \[as (?:run|close)\]|run|close) /.test(caller))
              return 'envelope'
            if (/ShareSoak\.(?:bounded \[as within\]|within) /.test(caller)) return 'stage'
            if (/CleanupStack\.(?:bounded \[as closeAll\]|closeAll) /.test(caller)) return 'stack'
            if (caller.includes('/testHelpers/shareFixtures.js:')) return 'proof'
            return caller
          })
          expect(callers.sort()).toEqual(['envelope', 'proof', 'stack', 'stage'])
          expect(point.timerOwners.afterCollection.dropped).toBe(0)
        }
        expect(driver.final).toHaveBeenCalledTimes(exceptional ? 0 : 1)
        expect(observations.length).toBeGreaterThanOrEqual(4)
        expect(owner.gate.tainted).toBe(exceptional)
        expect(owner.closed).toBe(true)
        expect(owner.timing.stages.filter(({ name }) => name === 'cleanup').length).toBeGreaterThan(
          0
        )
      } finally {
        shares.restore()
      }
    }
  )

  test.each(['product timer', 'open socket'])(
    'rejects a surviving %s on exceptional owner cleanup',
    async (damage) => {
      let timer
      let socket
      const { owner, shares, rows, driver, observations } = cleanupOwner({
        exceptional: true,
        afterSetup: ({ instrumented }) => {
          if (damage.includes('timer')) timer = setTimeout(() => {}, 60000)
          else {
            socket = instrumented.socketFactory({ host: 'scripted', port: 1 })
          }
        },
      })
      try {
        await expect(owner.run()).rejects.toThrow('workload failed')
        const failure = await owner.close().catch((error) => error)
        expect(failure).toBeInstanceOf(AggregateError)
        expect(
          failure.errors.some(({ message }) => message.includes('prove the share soak cleanup'))
        ).toBe(true)
        expect(rows.filter(({ event }) => event === 'case-failed')).toHaveLength(2)
        expect(owner.gate.tainted).toBe(true)
        expect(owner.stack.cleanups).toEqual([])
        expect(wrappers(driver.baseline)).toHaveLength(4)
        expect(observations.length).toBeGreaterThan(3)
      } finally {
        clearTimeout(timer)
        if (socket) {
          socket.destroyed = true
          socket.removeAllListeners()
        }
        shares.restore()
      }
    }
  )

  test('screens a listener on an archived destroyed socket after normal cleanup', async () => {
    let socket
    const { owner, driver, shares } = cleanupOwner({
      screen: true,
      afterSetup: ({ instrumented }) => {
        socket = instrumented.socketFactory({ host: 'scripted', port: 1 })
        socket.on('retained', () => {})
        socket.destroyed = true
      },
    })
    try {
      const result = await owner.run()
      expect(driver.after.sockets.open).toBe(0)
      expect(driver.after.activeResources).toEqual(driver.baseline.activeResources)
      expect(result.ok).toBe(false)
      expect(result.violations).toContain('teardown: resources differ or a source is absent')
      expect(owner.gate.tainted).toBe(true)
      await owner.close()
    } finally {
      socket?.removeAllListeners()
      shares.restore()
    }
  })

  test('screens dropped timer allocations after normal cleanup even with equal raw counts', async () => {
    const timers = []
    const { owner, driver, shares } = cleanupOwner({
      screen: true,
      afterSetup: () => {
        for (let i = 0; i < 4097; i++) timers.push(setTimeout(() => {}, 60000))
        timers.forEach(clearTimeout)
      },
    })
    try {
      const result = await owner.run()
      expect(driver.after.activeResources).toEqual(driver.baseline.activeResources)
      expect(driver.after.timerOwners.afterCollection.dropped).toBeGreaterThan(0)
      expect(result.ok).toBe(false)
      expect(result.violations).toContain('teardown: resources differ or a source is absent')
      expect(owner.gate.tainted).toBe(true)
      await owner.close()
    } finally {
      timers.forEach(clearTimeout)
      shares.restore()
    }
  })

  test.each(['destroyed-socket listener', 'dropped inventory', 'unattributed inventory'])(
    'archives and returns a secondary %s failure on exceptional close at equal raw counts',
    async (damage) => {
      let socket
      const timers = []
      const primary = new Error('workload failed')
      const { owner, driver, shares, rows, admin } = cleanupOwner({
        exceptional: true,
        primary,
        afterSetup: ({ instrumented }) => {
          if (damage === 'destroyed-socket listener') {
            socket = instrumented.socketFactory({ host: 'scripted', port: 1 })
            socket.on('retained', () => {})
            socket.destroyed = true
          } else if (damage === 'dropped inventory') {
            for (let i = 0; i < 4097; i++) timers.push(setTimeout(() => {}, 60000))
            timers.forEach(clearTimeout)
          }
        },
      })
      if (damage === 'unattributed inventory') {
        const snapshot = shares.snapshot
        shares.snapshot = async () => {
          const reading = await snapshot()
          if (driver.baseline) {
            for (const owners of Object.values(reading.timerOwners)) {
              const index = owners.rows.findIndex(({ stack }) =>
                stack.includes('/fixtures/bounded.js:')
              )
              owners.rows.splice(index, 1)
              owners.unattributed.Timeout++
            }
          }
          return reading
        }
      }
      const restore = jest.spyOn(shares, 'restore')
      const quiet = jest.spyOn(ShareSoakScreen, 'quiet')
      try {
        await expect(owner.run()).rejects.toBe(primary)
        const failure = await owner.close().catch((error) => error)
        expect(failure).toBeInstanceOf(Error)
        expect(failure.message).toContain(
          damage === 'unattributed inventory'
            ? 'unattributed'
            : 'resources differ or a source is absent'
        )
        expect(driver.after.activeResources).toEqual(driver.baseline.activeResources)
        expect(driver.after.activeResourcesBeforeCollection).toEqual(
          driver.baseline.activeResourcesBeforeCollection
        )
        expect(driver.after.sockets.open).toBe(0)
        if (damage === 'destroyed-socket listener')
          expect(driver.after.namedListeners).not.toEqual(driver.baseline.namedListeners)
        if (damage === 'dropped inventory')
          expect(driver.after.timerOwners.afterCollection.dropped).toBeGreaterThan(0)
        if (damage === 'unattributed inventory') {
          expect(wrappers(driver.baseline)).toHaveLength(4)
          expect(wrappers(driver.after)).toHaveLength(3)
          expect(driver.after.timerOwners.beforeCollection.dropped).toBe(0)
          expect(driver.after.timerOwners.afterCollection.dropped).toBe(0)
          expect(driver.after.timerOwners.afterCollection.unattributed.Timeout).toBe(
            driver.baseline.timerOwners.afterCollection.unattributed.Timeout + 1
          )
        }
        expect(quiet).toHaveBeenCalledWith(
          driver.after,
          driver.baseline,
          'cleanup',
          expect.any(Array)
        )
        expect(quiet.mock.invocationCallOrder.at(-1)).toBeLessThan(
          restore.mock.invocationCallOrder[0]
        )
        expect(restore).toHaveBeenCalledTimes(1)
        expect(
          rows.filter(({ event }) => event === 'case-failed').map(({ error }) => error.message)
        ).toEqual([primary.message, failure.message.slice(0, 600)])
        expect(owner.gate.cause).toBe(primary)
        expect(owner.stack.cleanups).toEqual([])
        expect(admin.deleteGroups).toHaveBeenCalled()
        expect(admin.deleteTopics).toHaveBeenCalled()
        expect(admin.listTopics).toHaveBeenCalled()
        expect(owner.closed).toBe(true)
      } finally {
        socket?.removeAllListeners()
        timers.forEach(clearTimeout)
        shares.restore()
      }
    }
  )

  test.each([0, 1])(
    'exceptional close retains a partly spent original cleanup cutoff, late ms=%s',
    async (lateMs) => {
      let now = 100000
      const { owner, shares, rows } = cleanupOwner({
        exceptional: true,
        shortFailureBound: false,
        afterSetup: async ({ owner }) => {
          await owner.stage('cleanup', async () => {
            now += 12000
          })
        },
      })
      owner.clock = { now: () => now }
      try {
        await expect(owner.run()).rejects.toThrow('workload failed')
        const workDue = owner.workDue
        const cleanupDue = owner.cleanupDue
        const spent = owner.spent.cleanup
        now += 3000
        const entered = []
        owner.stack.add('consume the original remaining allowance', async () => {
          entered.push(now)
          now = cleanupDue + lateMs
        })
        const result = await owner.close().catch((error) => error)
        expect(entered).toEqual([115000])
        expect(owner.workDue).toBe(workDue)
        expect(owner.cleanupDue).toBe(cleanupDue)
        expect(cleanupDue).toBe(120000)
        expect(spent).toBe(12000)
        expect(owner.spent.cleanup).toBe(17000 + lateMs)
        expect(
          owner.timing.stages.filter(({ name }) => name === 'cleanup').map(({ due }) => due)
        ).toEqual([cleanupDue, cleanupDue])
        if (lateMs) {
          expect(result).toBeInstanceOf(Error)
          expect(result).toBeInstanceOf(AggregateError)
          expect(result.errors.map(({ message }) => message)).toEqual([
            'The shared cleanup deadline exceeded',
            'The shared cleanup deadline exceeded',
          ])
          expect(rows.filter(({ event }) => event === 'case-failed')).toHaveLength(3)
        } else expect(result).toBeUndefined()
        const next = jest.fn()
        await expect(owner.stage('cleanup', next)).rejects.toThrow('deadline exhausted')
        expect(next).not.toHaveBeenCalled()
      } finally {
        shares.restore()
      }
    }
  )

  test('keeps the primary error and archives joined cleanup errors through the acceptance entrypoint', async () => {
    const exposed = globalThis.gc
    globalThis.gc = () => {}
    const existing = new Error('earlier cleanup failed')
    const primary = Object.assign(new Error('workload failed'), { cleanupErrors: [existing] })
    const deletionError = new Error('topic deletion failed')
    const { owner, rows, shares, admin } = cleanupOwner({
      exceptional: true,
      cleanupFailure: true,
      primary,
      deletionError,
    })
    const close = jest.spyOn(owner, 'close')
    try {
      const failure = await main('share-consumer-short', { shareSoak: () => owner }).catch(
        (error) => error
      )
      expect(failure).toBe(primary)
      expect(close).toHaveBeenCalledTimes(1)
      const cleanup = await close.mock.results[0].value.catch((error) => error)
      expect(cleanup).toBeInstanceOf(AggregateError)
      expect(cleanup.errors).toHaveLength(1)
      expect(cleanup.errors[0].message).toBe('delete the share soak topic: topic deletion failed')
      expect(cleanup.errors[0].cause).toBe(deletionError)
      expect(failure.cleanupErrors).toEqual([existing, cleanup])
      expect(failure.cleanupErrors[0]).toBe(existing)
      expect(failure.cleanupErrors[1]).toBe(cleanup)
      expect(owner.gate.cause).toBe(primary)
      expect(
        rows
          .filter(({ event }) => event === 'case-failed')
          .map(({ error }) => error.message)
          .join('\n')
      ).toContain('topic deletion failed')
      expect(owner.gate.tainted).toBe(true)
      expect(admin.disconnect).toHaveBeenCalled()
      expect(owner.stack.cleanups).toEqual([])
      await owner.close()
    } finally {
      globalThis.gc = exposed
      shares.restore()
    }
  })
})
