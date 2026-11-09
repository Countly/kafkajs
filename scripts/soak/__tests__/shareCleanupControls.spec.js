const { EventEmitter } = require('events')
const { performance } = require('perf_hooks')
const { ShareSoak, ShareSoakDriver, ShareSoakScreen, instrumentShare, main } = require('../soak')
const fixtures = require('../../../testHelpers/shareFixtures')
const helpers = require('../../../testHelpers')
const bounded = require('../../../src/producer/__tests__/fixtures/bounded')
const { CleanupStack } = require('../../../src/producer/__tests__/fixtures/liveProbe')

/**
 * Scripts admin I/O only; original owner stages, cleanup stack, probes and fixture proofs run.
 * @param {object} [options] injected workload and cleanup failures
 * @param {boolean} [options.exceptional] fail immediately after setup's baseline
 * @param {boolean} [options.cleanupFailure] fail deletion after a workload failure
 * @param {Function} [options.afterSetup] inject retained resources after the original baseline
 * @param {boolean} [options.screen] apply the original quiet-resource screen after cleanup
 * @returns {object} original owner, serializing archive, observations and owned admin
 */
const cleanupOwner = ({
  exceptional = false,
  cleanupFailure = false,
  afterSetup = () => {},
  screen = false,
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
      if (cleanupFailure) throw new Error('topic deletion failed')
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
  const owner = new ShareSoak({
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
        owner.workDue = Math.min(owner.workDue, performance.now() + 1000)
        throw new Error('workload failed')
      }
      await owner.finish()
      const violations = []
      if (screen) ShareSoakScreen.quiet(driver.after, driver.baseline, 'teardown', violations)
      return { ok: violations.length === 0, violations, measured: {} }
    },
  })
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

  test.each(['product timer', 'unattributed timer', 'open socket'])(
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
            if (damage === 'retained listener') {
              socket.on('retained', () => {})
              socket.destroyed = true
            }
          }
        },
      })
      if (damage === 'unattributed timer') {
        const snapshot = shares.snapshot
        shares.snapshot = async () => {
          const reading = await snapshot()
          if (driver.baseline)
            for (const owners of Object.values(reading.timerOwners)) {
              owners.rows = owners.rows.filter(({ durationMs }) => durationMs !== 60000)
              owners.unattributed.Timeout++
            }
          return reading
        }
      }
      try {
        await expect(owner.run()).rejects.toThrow('workload failed')
        const failure = await owner.close().catch((error) => error)
        expect(failure).toBeInstanceOf(AggregateError)
        expect(
          failure.errors.some(({ message }) => message.includes('prove the share soak cleanup'))
        ).toBe(true)
        expect(rows.filter(({ event }) => event === 'case-failed')).toHaveLength(2)
        expect(rows.some(({ event }) => event === 'case-complete')).toBe(false)
        expect(owner.gate.tainted).toBe(true)
        expect(owner.stack.cleanups).toEqual([])
        expect(wrappers(driver.baseline)).toHaveLength(4)
        expect(observations.length).toBeGreaterThan(3)
        if (damage === 'unattributed timer')
          expect(
            observations.at(-1).timerOwners.afterCollection.unattributed.Timeout
          ).toBeGreaterThan(driver.baseline.timerOwners.afterCollection.unattributed.Timeout)
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

  test('fails the actual owner verdict for a listener on an archived destroyed socket', async () => {
    let socket
    const { owner, driver, shares, rows } = cleanupOwner({
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
      expect(rows.some(({ event }) => event === 'case-complete')).toBe(false)
      await owner.close()
    } finally {
      socket?.removeAllListeners()
      shares.restore()
    }
  })

  test('fails the actual owner resource verdict after dropped timer allocations even with equal raw counts', async () => {
    const timers = []
    const { owner, driver, shares, rows } = cleanupOwner({
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
      expect(rows.some(({ event }) => event === 'case-complete')).toBe(false)
      await owner.close()
    } finally {
      timers.forEach(clearTimeout)
      shares.restore()
    }
  })

  test('keeps the primary error and archives joined cleanup errors through the acceptance entrypoint', async () => {
    const exposed = globalThis.gc
    globalThis.gc = () => {}
    const { owner, rows, shares, admin } = cleanupOwner({ exceptional: true, cleanupFailure: true })
    try {
      const failure = await main('share-consumer-short', { shareSoak: () => owner }).catch(
        (error) => error
      )
      expect(failure.message).toBe('workload failed')
      expect(failure.cleanupErrors).toEqual(expect.arrayContaining([expect.any(Error)]))
      expect(
        rows
          .filter(({ event }) => event === 'case-failed')
          .map(({ error }) => error.message)
          .join('\n')
      ).toContain('topic deletion failed')
      expect(rows.some(({ event }) => event === 'case-complete')).toBe(false)
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
