const fs = require('fs')
const os = require('os')
const path = require('path')
const { EventEmitter } = require('events')
const v8 = require('v8')
const vm = require('vm')
const { createResourceProbe, compareSnapshots, ResourceRoles } = require('../resourceProbe')
const { createFakeSocketFactory, socketCounts } = require('../fakeSocket')
const {
  ARMS,
  KINDS,
  LIMITS,
  MemoryOutput,
  REGISTRY_PROBE,
  SCHEMA,
  foregroundCatalogue,
  judgeMemoryArm,
  maximumDescriptor,
  phaseCatalogue,
  registryCatalogue,
  registryEntry,
  registryFingerprint,
  validateResourceArtifacts,
} = require('../../scripts/e2e/telemetry/resourceArtifact')

/** A real full collection, without starting node with --expose-gc (the flag stays set for this process). */
const realGc = () => {
  v8.setFlagsFromString('--expose-gc')
  return vm.runInNewContext('gc')
}

describe('createResourceProbe', () => {
  test('reports only what it was given a source for', async () => {
    expect(await createResourceProbe().snapshot()).toEqual({})
    expect(await createResourceProbe({ queueDepth: () => 0 }).snapshot()).toEqual({ queueDepth: 0 })
  })

  describe('listeners', () => {
    test('counts the listeners of every event name of every emitter, once() included', async () => {
      const queue = new EventEmitter()
      const socket = new EventEmitter()
      queue.on('a', () => {})
      queue.on('a', () => {})
      queue.once('b', () => {})
      socket.on('close', () => {})
      const probe = createResourceProbe({ emitters: { queue, socket } })

      expect((await probe.snapshot()).listeners).toEqual({ queue: 3, socket: 1 })
    })

    test('sees a listener that was not removed, and one that was', async () => {
      const emitter = new EventEmitter()
      const probe = createResourceProbe({ emitters: { emitter } })
      const before = await probe.snapshot()

      const leaked = () => {}
      emitter.on('data', leaked)
      const during = await probe.snapshot()
      emitter.off('data', leaked)
      const after = await probe.snapshot()

      expect(during.listeners.emitter).toBe(before.listeners.emitter + 1)
      expect(after).toEqual(before)
    })
  })

  describe('sockets', () => {
    test('reports sockets created, destroyed and the open difference', async () => {
      const { factory, state } = createFakeSocketFactory()
      const probe = createResourceProbe({ sockets: () => socketCounts(state) })
      const open = (host) => factory({ host, port: 9092, onConnect: () => {} })

      const first = open('a')
      open('b')
      first.destroy()

      expect((await probe.snapshot()).sockets).toEqual({ created: 2, destroyed: 1, open: 1 })
    })

    test('sees a socket that was left open', async () => {
      const { factory, state } = createFakeSocketFactory()
      const probe = createResourceProbe({ sockets: () => socketCounts(state) })
      const before = await probe.snapshot()

      factory({ host: 'a', port: 9092, onConnect: () => {} })

      expect((await probe.snapshot()).sockets.open).toBe(before.sockets.open + 1)
    })
  })

  test('reads the queue depth, the ledger size and the obligation size when asked', async () => {
    const sizes = { queue: 4, ledger: 2, obligations: 1 }
    const probe = createResourceProbe({
      queueDepth: () => sizes.queue,
      ledgerSize: () => sizes.ledger,
      obligationSize: () => sizes.obligations,
    })

    expect(await probe.snapshot()).toEqual({ queueDepth: 4, ledgerSize: 2, obligationSize: 1 })
    sizes.ledger = 0
    expect((await probe.snapshot()).ledgerSize).toBe(0)
  })

  describe('active resources', () => {
    test('counts a real timer by its type, and not once it is cleared', async () => {
      const probe = createResourceProbe({ activeResources: true })
      const before = (await probe.snapshot()).activeResources.Timeout || 0

      const timer = setTimeout(() => {}, 60000)
      const during = (await probe.snapshot()).activeResources.Timeout
      clearTimeout(timer)
      const after = (await probe.snapshot()).activeResources.Timeout || 0

      expect(during).toBe(before + 1)
      expect(after).toBe(before)
    })

    test.each([
      ['modern', {}],
      ['legacy', { legacyFakeTimers: true }],
    ])('refuses to count under %s fake timers, which it cannot see', async (_name, options) => {
      jest.useFakeTimers(options)
      try {
        setTimeout(() => {}, 60000)

        await expect(createResourceProbe({ activeResources: true }).snapshot()).rejects.toThrow(
          'does not see fake timers'
        )
      } finally {
        jest.useRealTimers()
      }
    })

    test('counts a real timer again once legacy fake timers are removed', async () => {
      jest.useFakeTimers({ legacyFakeTimers: true })
      jest.useRealTimers()
      const probe = createResourceProbe({ activeResources: true })
      const before = (await probe.snapshot()).activeResources.Timeout || 0

      const timer = setTimeout(() => {}, 60000)
      try {
        expect((await probe.snapshot()).activeResources.Timeout).toBe(before + 1)
      } finally {
        clearTimeout(timer)
      }
    })
  })

  describe('heap', () => {
    test('reads resources atomically before heap collection and its finalizer gap', async () => {
      const socket = Object.assign(new EventEmitter(), { destroyed: false })
      socket.on('error', () => {})
      let depth = 1
      const probe = createResourceProbe({
        emitters: { socket },
        sockets: () => ({ created: 1, destroyed: Number(socket.destroyed) }),
        queueDepth: () => depth,
        heap: true,
        gc: () => {},
        pause: async () => {
          socket.destroyed = true
          socket.removeAllListeners()
          depth = 0
        },
      })

      expect(await probe.snapshot()).toMatchObject({
        listeners: { socket: 1 },
        sockets: { created: 1, destroyed: 0, open: 1 },
        queueDepth: 1,
      })
    })

    test.each([false, true])(
      'preserves the timer reading from before the collection gap, retained=%s',
      async (retained) => {
        const baseline = await createResourceProbe({ activeResources: true }).snapshot()
        const timer = setTimeout(() => {}, 60000)
        const probe = createResourceProbe({
          activeResources: true,
          heap: true,
          gc: () => {},
          pause: async () => {
            if (!retained) clearTimeout(timer)
          },
        })
        try {
          const snapshot = await probe.snapshot()
          expect(snapshot.activeResources.Timeout || 0).toBe(
            (baseline.activeResources.Timeout || 0) + 1
          )
        } finally {
          clearTimeout(timer)
        }
      }
    )

    test('collects twice, 100 ms apart, before it reads', async () => {
      const events = []
      const probe = createResourceProbe({
        heap: true,
        gc: () => events.push('gc'),
        pause: async (ms) => events.push(`pause ${ms}`),
      })

      const { heapUsed } = await probe.snapshot()

      expect(events).toEqual(['gc', 'pause 100', 'gc'])
      expect(heapUsed).toBeGreaterThan(0)
    })

    test('needs a gc function', () => {
      // null, not undefined: undefined takes the default, and an earlier spec in this process may
      // have exposed the real one
      expect(() => createResourceProbe({ heap: true, gc: null })).toThrow('--expose-gc')
    })

    /**
     * Keeps distinct JS objects observable across collection by reading every value after sampling.
     * @returns {Promise<void>}
     */
    test('sees memory that is retained and that is released, after a real collection', async () => {
      const probe = createResourceProbe({ heap: true, gc: realGc(), pause: async () => {} })
      const { heapUsed: before } = await probe.snapshot()

      let retained = Array.from({ length: 2_000_000 }, (_, index) => ({ index }))
      const { heapUsed: during } = await probe.snapshot()
      expect(retained).toHaveLength(2_000_000)
      expect(retained.reduce((sum, entry) => sum + entry.index, 0)).toBe(1_999_999_000_000)
      retained = null
      const { heapUsed: after } = await probe.snapshot()

      expect(during - before).toBeGreaterThan(10 * 1024 * 1024)
      expect(during - after).toBeGreaterThan(10 * 1024 * 1024)
    })
  })
})

describe('share resource sources', () => {
  test('reports actual sessions, records and compact ranges separately', async () => {
    const probe = createResourceProbe({
      shareState: () => ({ shareSessions: 2, shareRecords: 3, shareRanges: 1 }),
    })
    expect(await probe.snapshot()).toEqual({ shareSessions: 2, shareRecords: 3, shareRanges: 1 })
  })

  test('does not invent zero counts when the source is absent', async () => {
    expect(await createResourceProbe().snapshot()).toEqual({})
  })

  test.each([undefined, null, -1, 0.5, NaN, '1'])(
    'refuses a malformed share count %s',
    async (value) => {
      const probe = createResourceProbe({
        shareState: () => ({ shareSessions: 0, shareRecords: 0, shareRanges: value }),
      })
      await expect(probe.snapshot()).rejects.toThrow(
        'Missing share resource measurement: shareRanges'
      )
    }
  )

  test('refuses a source that returns no counts at all', async () => {
    const probe = createResourceProbe({ shareState: () => undefined })
    await expect(probe.snapshot()).rejects.toThrow(
      'Missing share resource measurement: shareSessions'
    )
  })

  test('keeps every other measurement beside the share counts', async () => {
    const probe = createResourceProbe({
      queueDepth: () => 4,
      shareState: () => ({ shareSessions: 0, shareRecords: 0, shareRanges: 0 }),
    })
    expect(await probe.snapshot()).toEqual({
      queueDepth: 4,
      shareSessions: 0,
      shareRecords: 0,
      shareRanges: 0,
    })
  })
})

describe('resource identity and settled memory sources', () => {
  test('shares weak identities only when probes explicitly share a scope', async () => {
    const identityScope = { issued: new WeakMap(), next: 0 }
    const owner = {}
    const first = createResourceProbe({ identityScope, identities: () => ({ owner: [owner] }) })
    const same = createResourceProbe({ identityScope, identities: () => ({ owner: [owner] }) })
    const other = createResourceProbe({ identityScope, identities: () => ({ owner: [{}] }) })
    const before = await first.snapshot()
    expect(compareSnapshots(before, await same.snapshot())).toEqual([])
    expect(compareSnapshots(before, await other.snapshot())).toEqual(['identities owner changed'])
    const independent = await createResourceProbe({
      identities: () => ({ owner: [{}] }),
    }).snapshot()
    expect(independent.identities.owner).toEqual([1])
  })

  test('detects replacement at the same resource count without retaining source objects', async () => {
    let socket = {}
    const probe = createResourceProbe({ identities: () => ({ sockets: [socket] }) })
    const first = await probe.snapshot()
    expect(first.identities.sockets).toHaveLength(1)
    expect((await probe.snapshot()).identities).toEqual(first.identities)
    socket = {}
    const replacement = await probe.snapshot()
    expect(replacement.identities.sockets).toHaveLength(1)
    expect(replacement.identities.sockets).not.toEqual(first.identities.sockets)
    expect(typeof replacement.identities.sockets[0]).toBe('number')
  })

  test('keeps multiplicity and event ownership while ignoring enumeration order', async () => {
    const fn = () => {}
    const other = () => {}
    let sources = { 'listener:data': [fn, other, fn], 'listener:close': [fn] }
    const probe = createResourceProbe({ identities: () => sources })
    const first = await probe.snapshot()
    expect(first.identities['listener:data']).toHaveLength(3)
    sources = { 'listener:close': [fn], 'listener:data': [fn, fn, other] }
    expect((await probe.snapshot()).identities).toEqual(first.identities)
    sources = { 'listener:close': [other], 'listener:data': [fn, fn, other] }
    expect((await probe.snapshot()).identities).not.toEqual(first.identities)
  })

  test('copies identity readings and refuses an unobservable primitive', async () => {
    const list = [{}]
    const probe = createResourceProbe({ identities: () => ({ jobs: list }) })
    const first = await probe.snapshot()
    list.length = 0
    expect(first.identities.jobs).toHaveLength(1)
    expect((await probe.snapshot()).identities.jobs).toEqual([])
    list.push(1)
    await expect(probe.snapshot()).rejects.toThrow('Resource identity must be an object')
  })

  test('reads heap and external together after the second collection', async () => {
    const events = []
    const readMemory = jest.spyOn(process, 'memoryUsage').mockImplementation(() => {
      events.push('memory')
      return { heapUsed: 11, external: 13, arrayBuffers: 7, rss: 101 }
    })
    const readUsage = jest.spyOn(process, 'resourceUsage').mockReturnValue({ maxRSS: 2 })
    const readStatus = jest
      .spyOn(fs, 'readFileSync')
      .mockReturnValue('Name:\tnode\nVmHWM:\t    4096 kB\nVmRSS:\t    2048 kB\n')
    try {
      const probe = createResourceProbe({
        memory: true,
        gc: () => events.push('gc'),
        pause: async (ms) => events.push(`pause ${ms}`),
      })
      expect((await probe.snapshot()).memory).toEqual({
        heapUsed: 11,
        external: 13,
        retainedBytes: 24,
        rss: 101,
        peakRss: 2048,
        rawMaxRss: 2048,
        highWaterRss: 4194304,
        highWaterSource: 'proc-self-status-vmhwm',
      })
      expect(events).toEqual(['gc', 'pause 100', 'gc', 'memory'])
    } finally {
      readMemory.mockRestore()
      readUsage.mockRestore()
      readStatus.mockRestore()
    }
  })

  test('reports no high water, and keeps the raw maximum apart, where the kernel gives none', async () => {
    const readUsage = jest.spyOn(process, 'resourceUsage').mockReturnValue({ maxRSS: 7 })
    const readStatus = jest.spyOn(fs, 'readFileSync').mockImplementation(() => {
      throw new Error('ENOENT')
    })
    try {
      const probe = createResourceProbe({ memory: true, gc: () => {}, pause: async () => {} })
      expect((await probe.snapshot()).memory).toMatchObject({
        peakRss: 7168,
        rawMaxRss: 7168,
        highWaterRss: null,
        highWaterSource: null,
      })
    } finally {
      readUsage.mockRestore()
      readStatus.mockRestore()
    }
  })

  test('requires gc for memory and preserves omitted measurements', async () => {
    expect(() => createResourceProbe({ memory: true, gc: null })).toThrow('--expose-gc')
    expect(await createResourceProbe().snapshot()).toEqual({})
  })
})

describe('emitter, event and listener registrations', () => {
  const take = async (emitters) =>
    (await createResourceProbe({ registrations: () => emitters }).snapshot()).registrations

  test('reports the emitter, each event and its raw listeners with multiplicity', async () => {
    const socket = new EventEmitter()
    const onClose = () => {}
    socket.on('close', onClose)
    socket.on('data', onClose)
    socket.on('data', onClose)
    socket.once('error', () => {})
    const probe = createResourceProbe({ registrations: () => ({ socket }) })
    const first = (await probe.snapshot()).registrations.socket

    expect(Object.keys(first.events).sort()).toEqual(['close', 'data', 'error'])
    expect(first.events.data).toHaveLength(2)
    expect(first.events.data[0]).toBe(first.events.data[1])
    expect(first.events.close[0]).toBe(first.events.data[0])
    expect((await probe.snapshot()).registrations.socket).toEqual(first)
    expect(await createResourceProbe().snapshot()).toEqual({})
  })

  test('changes when a listener is swapped at the same count', async () => {
    const socket = new EventEmitter()
    const kept = () => {}
    socket.on('data', kept)
    socket.on('data', () => {})
    const probe = createResourceProbe({ registrations: () => ({ socket }) })
    const before = await probe.snapshot()
    socket.removeAllListeners('data')
    socket.on('data', kept)
    socket.on('data', () => {})

    const after = await probe.snapshot()

    expect(after.registrations.socket.events.data).toHaveLength(2)
    expect(compareSnapshots(before, await probe.snapshot(), {})).toEqual([
      'registrations socket changed',
    ])
    expect(compareSnapshots(after, await probe.snapshot(), {})).toEqual([])
  })

  test('changes when the emitter is swapped for another with the same listeners', async () => {
    const listener = () => {}
    const a = new EventEmitter()
    const b = new EventEmitter()
    a.on('data', listener)
    b.on('data', listener)
    const probe = createResourceProbe({ registrations: () => ({ socket: current }) })
    let current = a
    const first = await probe.snapshot()
    current = b

    expect((await probe.snapshot()).registrations.socket.events).toEqual(
      first.registrations.socket.events
    )
    expect(compareSnapshots(first, await probe.snapshot(), {})).toEqual([
      'registrations socket changed',
    ])
  })

  test('sees a duplicate registration of one function', async () => {
    const emitter = new EventEmitter()
    const listener = () => {}
    emitter.on('data', listener)
    const probe = createResourceProbe({ registrations: () => ({ emitter }) })
    const before = await probe.snapshot()
    emitter.on('data', listener)

    expect(compareSnapshots(before, await probe.snapshot(), {})).toEqual([
      'registrations emitter changed',
    ])
  })

  test('refuses a source that is not an emitter', async () => {
    await expect(take({ socket: {} })).rejects.toThrow(TypeError)
  })
})

describe('resource roles and snapshot comparison', () => {
  test('gives a followed timer the role of its creator and the unknown role otherwise', async () => {
    const roles = new ResourceRoles({ types: ['Timeout'] }).start()
    const timers = []
    try {
      timers.push(roles.run('target', () => setTimeout(() => {}, 60000)))
      timers.push(roles.run('fixture', () => setTimeout(() => {}, 60000)))
      timers.push(setTimeout(() => {}, 60000))
      const read = roles.read()

      expect(Object.keys(read).sort()).toEqual(['fixture', 'target', 'unknown'])
      expect(read.target.Timeout).toHaveLength(1)
      expect(read.fixture.Timeout).toHaveLength(1)
      expect(read.unknown.Timeout).toHaveLength(1)
      expect(
        new Set([...read.target.Timeout, ...read.fixture.Timeout, ...read.unknown.Timeout]).size
      ).toBe(3)
    } finally {
      timers.forEach(clearTimeout)
      roles.stop()
    }
  })

  test('forgets a destroyed timer and every entry once stopped', async () => {
    const roles = new ResourceRoles({ types: ['Timeout'] }).start()
    try {
      const timer = roles.run('target', () => setTimeout(() => {}, 60000))
      expect(roles.read().target.Timeout).toHaveLength(1)
      clearTimeout(timer)
      await new Promise((resolve) => setImmediate(resolve))
      expect(roles.read()).toEqual({ unknown: {} })
    } finally {
      roles.stop()
    }
    expect(roles.read()).toEqual({ unknown: {} })
  })

  test('carries declared roles while empty so an absent role is a missing source', () => {
    const roles = new ResourceRoles({ types: ['Timeout'], roles: ['target', 'fixture'] })
    expect(roles.read()).toEqual({ target: {}, fixture: {}, unknown: {} })
    const empty = { resources: roles.read() }
    expect(compareSnapshots(empty, empty, { resources: ['target', 'fixture'] })).toEqual([])
    expect(
      compareSnapshots({ resources: {} }, { resources: {} }, { resources: ['target'] })
    ).toEqual(['missing resources source target'])
  })

  test('records which followed resource triggered another, and inherits its role', async () => {
    const roles = new ResourceRoles({ types: ['Timeout'] }).start()
    const timers = []
    try {
      const seen = await new Promise((resolve) => {
        timers.push(
          roles.run('target', () =>
            setTimeout(() => {
              timers.push(setTimeout(() => {}, 60000))
              resolve(roles.ancestry())
            }, 1)
          )
        )
      })
      const outer = seen.find((entry) => entry.role === 'target' && entry.parent === null)
      const inner = seen.find((entry) => entry.parent !== null)

      expect(outer).toMatchObject({ type: 'Timeout', role: 'target' })
      expect(inner).toMatchObject({ type: 'Timeout', role: 'target', parent: outer.id })
    } finally {
      timers.forEach(clearTimeout)
      roles.stop()
    }
  })

  const reading = (sockets, timers, extra = {}) => ({
    identities: { sockets },
    resources: { target: { Timeout: timers }, ...extra },
  })
  const required = { identities: ['sockets'], resources: ['target'] }

  test('passes an identical reading and fails each substitution it must notice', () => {
    const base = reading([1], [10])
    expect(compareSnapshots(base, reading([1], [10]), required)).toEqual([])
    expect(compareSnapshots(base, reading([2], [10]), required)).toEqual([
      'identities sockets changed',
    ])
    expect(compareSnapshots(base, reading([1], [11]), required)).toEqual([
      'resources target changed',
    ])
    expect(compareSnapshots(base, reading([1, 1], [10]), required)).toEqual([
      'identities sockets changed',
    ])
  })

  test('fails an omitted source even when everything present is equal', () => {
    expect(
      compareSnapshots({ identities: {} }, { identities: {} }, { identities: ['sockets'] })
    ).toEqual(['missing identities source sockets'])
    expect(
      compareSnapshots(reading([1], [10]), { identities: { sockets: [1] } }, required)
    ).toEqual(['missing resources source target'])
    expect(compareSnapshots({}, {}, {})).toEqual([])
  })

  test('fails an unknown handle that survives and allows one the baseline already had', () => {
    const base = reading([1], [10], { unknown: { Timeout: [5] } })
    expect(
      compareSnapshots(base, reading([1], [10], { unknown: { Timeout: [5] } }), required)
    ).toEqual([])
    expect(
      compareSnapshots(base, reading([1], [10], { unknown: { Timeout: [5, 6] } }), required)
    ).toEqual(['resources unknown changed', 'unknown Timeout resource survives'])
  })
})

describe('memory fixture module loading', () => {
  test('loads both directions of the declared core and telemetry protocols before any client', () => {
    const { preloadProtocols } = require('../../src/telemetry/__tests__/fixtures/memoryForeground')
    const load = jest.fn()
    preloadProtocols(load)

    for (const [api, version] of [
      ['apiVersions', 5],
      ['apiVersions', 4],
      ['metadata', 13],
      ['produce', 13],
      ['leaveGroup', 5],
      ['shareFetch', 2],
      ['shareAcknowledge', 2],
      ['getTelemetrySubscriptions', 0],
      ['pushTelemetry', 0],
    ]) {
      for (const direction of ['request', 'response'])
        expect(load).toHaveBeenCalledWith(
          expect.stringContaining(`/src/protocol/requests/${api}/v${version}/${direction}.js`)
        )
      expect(load).toHaveBeenCalledWith(
        expect.stringContaining(`/testHelpers/wire/${api}V${version}.js`)
      )
    }
    expect(load.mock.calls.every(([file]) => !file.endsWith('.spec.js'))).toBe(true)
  })
})

describe('native memory fixture result ownership', () => {
  test('returns only scalar facts after current and retired result scopes finish', async () => {
    const { nativePair } = require('../../src/telemetry/__tests__/fixtures/memoryForeground')
    const input = { points: [{}] }
    const payload = {
      compressionType: 4,
      rawBytes: 376306,
      transmittedBytes: 1505,
      includedIdentities: Array.from({ length: 257 }, () => ({})),
      metrics: Buffer.alloc(65536),
    }
    let epoch = 0
    const builder = {
      buildPayload: jest.fn(async (_snapshot, options) => {
        if (options.isCurrent()) {
          await Promise.resolve()
          return options.isCurrent() ? payload : null
        }
        return null
      }),
    }

    const facts = await nativePair(builder, input, { name: 'zstd', type: 4 }, () => epoch++)

    expect(epoch).toBe(1)
    expect(builder.buildPayload.mock.calls.map(([snapshot]) => snapshot)).toEqual([input, input])
    expect(facts).toEqual({
      codec: 'zstd',
      compressionType: 4,
      rawBytes: 376306,
      transmittedBytes: 1505,
      points: 257,
      retiredCompletion: true,
    })
    expect(Object.values(facts).every((value) => value === null || typeof value !== 'object')).toBe(
      true
    )
  })
})

describe('resource artifact validation', () => {
  const TREE = 'a'.repeat(40)
  const NODE = 'v26.7.0'
  const MIB = 1048576
  const UUID = '01020304-0506-0708-090a-0b0c0d0e0f10'
  const context = () => ({
    cpus: 4,
    load: [0.2, 0.2, 0.2],
    freeBytes: 4e9,
    totalBytes: 8e9,
    psi: { memorySomeAvg10: 0.1 },
  })

  /** One settled reading: heap is whatever makes heap plus external the retained bytes. */
  const settled = (at, rss, external, retained) => {
    const reading = {
      at,
      rss,
      heapUsed: retained - external,
      external,
      arrayBuffers: 1000,
      retained,
    }
    const checkpoints = [at - 150, at].map((completedAt) => ({
      ...reading,
      at: completedAt,
      admittedAt: completedAt - 100,
      observed: 2,
      engine: { bytecode: 100, code: 100 },
    }))
    return {
      ...reading,
      gc: { admittedAt: at - 250, completedAt: at, observed: 4, checkpoints },
    }
  }

  /** The high water, covered current RSS (MiB) and covered external bytes at each phase. */
  const PEAKS = {
    startup: [41, 41, 1e6],
    loaded: [75, 70, 3e6],
    'control:start': [75, 70, 3e6],
    'control:c0': [80, 74, 3e6],
    'control:c1': [80, 74, 3e6],
    'control:released': [80, 72, 3e6],
    start: [80, 72, 3e6],
    registered: [110, 100, 12e6],
    matched: [110, 80, 4e6],
    gzip: [120, 118, 20e6],
    held: [120, 85, 6e6],
    closed: [120, 76, 4e6],
    final: [120, 76, 4e6],
  }

  const mark = (phase, index) => {
    const key = /^(enabled|collector):/.test(phase) ? phase.split(':')[1] : phase
    const [high, covered, external] = PEAKS[key]
    return {
      phase,
      at: 100 + index * 100,
      rss: covered * MIB,
      vmhwm: high * MIB,
      maxRss: 357744640,
      covered: covered * MIB,
      coveredExternal: external,
      imports: phase === 'startup' ? null : { count: 400, late: [] },
    }
  }

  const edge = (at) => ({
    at,
    rss: 100 * MIB,
    heapUsed: 1e6,
    external: 10e6,
    arrayBuffers: 0,
    retained: 11e6,
  })

  /** The submission time of each job: inside the interval its codec mark covers. */
  const jobAt = (arm, id) =>
    100 +
    phaseCatalogue(arm, false).indexOf(`${arm === 'public' ? 'enabled' : 'collector'}:gzip`) * 100 -
    60 +
    id * 20

  const job = (id, retired, at) => ({
    id,
    codec: 'gzip',
    jobId: id * 10 + 1,
    inputId: id * 10 + 2,
    inputBytes: 350000,
    inputBackingId: id * 10 + 3,
    inputBackingBytes: 350000,
    epoch: 0,
    submitted: edge(at),
    settled: edge(at + 5),
    outcome: 'resolved',
    resultId: id * 10 + 4,
    resultBytes: 4000,
    resultBackingId: id * 10 + 5,
    resultBackingBytes: 4000,
    distinctBackingBytes: 354000,
    retired,
  })

  /** The receipt and reopen orders a public arm witnesses in its disconnect continuations. */
  const witnessed = (at, action) => [
    { action: 'disconnect', at },
    { action, at: at + 0.25 },
    { action: 'yield', at: at + 0.5 },
  ]
  const retainedRegistry = () => ({
    ...registryFingerprint(registryCatalogue(), UUID),
    ...REGISTRY_PROBE,
  })

  /** A passing record of one arm, built fresh so each test can break exactly one fact. */
  const record = (arm, kind = 'producer') => {
    const phases = phaseCatalogue(arm, false)
    const foreground = foregroundCatalogue(kind, 3)
    const roles = () => ({ target: {}, fixture: {}, unknown: {} })
    const childId = `child-${arm}-${kind}`
    const child = {
      childId,
      pid: 100,
      tree: TREE,
      runId: 'run-1',
      node: NODE,
      v8: '14.0',
      platform: 'linux',
      arch: 'x64',
    }
    const jobs = [job(1, false, jobAt(arm, 1)), job(2, true, jobAt(arm, 2))]
    const registry = arm === 'public' ? { registry: retainedRegistry() } : {}
    const disconnect = witnessed(6500, 'receipt')
    return {
      schema: SCHEMA,
      arm,
      kind,
      stamp: { tree: TREE, node: NODE, runId: 'run-1', platform: 'linux', arch: 'x64', at: 'now' },
      context: { before: context(), after: context() },
      exit: {
        code: 0,
        signal: null,
        timedOut: false,
        failure: null,
        stdoutBytes: 900,
        stderrBytes: 0,
        receipts: arm === 'public' ? 1 : 0,
        results: 1,
        receipt:
          arm === 'public'
            ? { receipt: 'disconnect', kind, uuid: UUID, childId, at: disconnect[1].at }
            : null,
        disconnectObservedAt: arm === 'public' ? 9000 : null,
        closedAfterMarkMs: 250,
        producer: { arm, kind, ...child },
      },
      readings: {
        manifest: {
          arm,
          kind,
          ...child,
          execArgv: ['--expose-gc', '--no-lazy', '--no-lazy-feedback-allocation'],
          instrumented: { inspector: false, coverage: false, jest: false },
          peakSource: { name: 'proc-self-status-vmhwm', units: 'bytes' },
          gc: { available: true, native: true },
          codecs: { gzip: true, zstd: false },
          fixture:
            arm === 'public'
              ? { descriptors: 128, descriptorBytes: 354304, uuid: UUID }
              : { nodes: 64, points: 257 },
          foreground,
          phases,
          sampleMs: 20,
        },
        startup: settled(10, 41 * MIB, 1e6, 2e6),
        loaded: settled(50, 70 * MIB, 3e6, 5e6),
        c0: settled(1000, 73 * MIB, 3e6, 6e6),
        c1: settled(2000, 74 * MIB, 3e6, 6.1e6),
        released: settled(3000, 72 * MIB, 3e6, 5.9e6),
        control: {
          enabled: false,
          api71: 0,
          api72: 0,
          nativeSubmissions: 0,
          probe: 'disabled',
          liveSockets: 0,
          liveRequests: 0,
          ops: foreground.ops,
        },
        matched: { ...settled(4000, 80 * MIB, 4e6, 30e6), ...registry },
        actives: [
          { phase: 'gzip', ...settled(5000, 118 * MIB, 20e6, 31e6), ...registry },
          { phase: 'held', ...settled(6000, 85 * MIB, 6e6, 31.5e6), ...registry },
        ],
        closed: { ...settled(7000, 76 * MIB, 4e6, 30.1e6), ...registry },
        marks: phases.map(mark),
        samples: {
          count: 20,
          windows: 2,
          pendingWindows: jobs.map((row) => ({
            begin: row.submitted.at,
            end: row.settled.at,
            count: 2,
            rss: 100 * MIB,
            external: 10e6,
          })),
          overflow: false,
          maxGapMs: 40,
          intervalMs: 20,
          peak: { rss: 100 * MIB, heapUsed: 1e6, external: 10e6, arrayBuffers: 0 },
        },
        native: { rows: jobs, maxLive: 1, live: 0, overflow: false },
        contexts: { control: context(), boundary: context(), final: context() },
        resources: {
          released: { control: {}, ...roles() },
          matched: roles(),
          closed: roles(),
        },
        ...(arm === 'public'
          ? {
              matchedState: { connected: false, descriptors: 128, uuid: UUID },
              enabled: {
                enabled: true,
                api71: 5,
                api72: 9,
                liveSockets: 0,
                liveRequests: 0,
                ops: foreground.ops,
              },
              pushes: { gzip: { accepted: 4, transmittedBytes: 4000 } },
              decoded: [{ codec: 'gzip', format: true, rawBytes: 350000, transmittedBytes: 4000 }],
              reopens: [{ codec: 'gzip', events: witnessed(4500, 'connect') }],
              disconnect,
              registry: registryCatalogue(),
              uuid: UUID,
              uuidHeld: UUID,
              descriptors: 128,
            }
          : {
              builds: [
                {
                  codec: 'gzip',
                  compressionType: 1,
                  rawBytes: 350000,
                  transmittedBytes: 4000,
                  points: 257,
                  retiredCompletion: true,
                },
              ],
            }),
      },
    }
  }

  /** Judges a record after a change, from a fresh passing one. */
  const judged = (arm, change, kind = 'producer') => {
    const broken = record(arm, kind)
    const checkpoints = [
      broken.readings.c0,
      broken.readings.c1,
      broken.readings.released,
      broken.readings.matched,
      ...broken.readings.actives,
      broken.readings.closed,
    ]
    const previous = checkpoints.map((reading) => ({ ...reading }))
    change(broken.readings, broken)
    // A scalar gate control changes the same final value in its otherwise unchanged GC history.
    checkpoints.forEach((reading, index) => {
      for (const point of reading.gc?.checkpoints ?? []) {
        if (point === null || typeof point !== 'object') continue
        for (const field of ['rss', 'heapUsed', 'external', 'arrayBuffers', 'retained'])
          if (point[field] === previous[index][field]) point[field] = reading[field]
      }
    })
    return judgeMemoryArm(broken)
  }
  const markOf = (r, phase) => r.marks.find((candidate) => candidate.phase === phase)
  /** Raises the high water from a phase onward, keeping it monotonic. */
  const raiseFrom = (r, phase, bytes) => {
    let reached = false
    for (const candidate of r.marks) {
      reached ||= candidate.phase === phase
      if (reached) candidate.vmhwm = bytes
    }
  }
  const B = 73 * MIB

  describe('diagnostic acceptance isolation and recorded boundary replay', () => {
    test.each([{ diagnostic: true }, { acceptanceEligible: false }])(
      'rejects diagnostic marker %p even in a schema-5 envelope',
      (marker) => {
        expect(judgeMemoryArm({ ...record('public'), ...marker })).toEqual({
          verdict: 'fail',
          problems: ['diagnostic records are ineligible for acceptance'],
        })
      }
    )

    test.each([262144, 262145, 8388608, 8388609])(
      'replays actual recorder bytes for retained boundary %s',
      (delta) => {
        const whole = record('public')
        const reading = delta < 8388608 ? whole.readings.closed : whole.readings.actives[0]
        reading.retained = whole.readings.matched.retained + delta
        reading.heapUsed = reading.retained - reading.external
        for (const point of reading.gc.checkpoints) {
          point.retained = reading.retained
          point.heapUsed = reading.heapUsed
        }
        const output = new MemoryOutput(
          { arm: 'public', kind: 'producer', childId: whole.readings.manifest.childId },
          () => 1000
        )
        output.write(
          Buffer.from(
            JSON.stringify({
              receipt: 'disconnect',
              kind: 'producer',
              uuid: whole.readings.uuid,
              childId: whole.readings.manifest.childId,
              at: whole.readings.disconnect[1].at,
            }) + '\n'
          )
        )
        output.write(Buffer.from(JSON.stringify({ readings: whole.readings }) + '\n'))
        const parsed = output.finish()
        expect(parsed.failure).toBeNull()
        const copy = JSON.parse(JSON.stringify({ ...whole, readings: parsed.readings }))
        expect(judgeMemoryArm(copy).verdict).toBe(
          delta === 262145 || delta === 8388609 ? 'fail' : 'pass'
        )
      }
    )
  })

  describe('a complete record', () => {
    test.each(ARMS)('passes for the %s arm', (arm) => {
      expect(judgeMemoryArm(record(arm))).toEqual({ verdict: 'pass', problems: [] })
    })

    test.each(KINDS)('passes for the %s kind', (kind) => {
      expect(judgeMemoryArm(record('public', kind))).toEqual({ verdict: 'pass', problems: [] })
    })
  })

  describe('the schema and the shape of the evidence', () => {
    test.each([1, 2, 3, 4])('rejects schema %s without frozen module evidence', (schema) => {
      const old = record('public')
      old.schema = schema
      expect(judgeMemoryArm(old)).toEqual({ verdict: 'fail', problems: ['unknown schema'] })
    })

    test('rejects the earlier schema and an aggregate-only record', () => {
      const old = {
        schema: 1,
        arm: 'public',
        kind: 'producer',
        stamp: { tree: TREE, node: NODE },
        context: { before: context(), after: context() },
        exit: record('public').exit,
        readings: {
          baseline: settled(1, 5e6, 1, 5e6),
          matched: settled(1, 6e6, 1, 6e6),
          held: settled(1, 7e6, 1, 7e6),
          closed: settled(1, 6e6, 1, 6e6),
          baselinePeakRss: 55000000,
          peakRss: 60000000,
          rssPeakSample: 59000000,
        },
      }

      expect(judgeMemoryArm(old)).toEqual({ verdict: 'fail', problems: ['unknown schema'] })
      const stripped = record('public')
      delete stripped.readings.marks
      expect(judgeMemoryArm(stripped)).toEqual({
        verdict: 'fail',
        problems: ['peak marks are missing: aggregate evidence is not accepted'],
      })
    })

    test.each([
      ['an unknown arm', (_r, whole) => (whole.arm = 'both'), 'unknown arm'],
      ['an unknown kind', (_r, whole) => (whole.kind = 'streams'), 'unknown kind'],
      ['no readings', (_r, whole) => (whole.readings = null), 'readings are missing'],
    ])('fails %s', (_name, change, problem) => {
      const broken = record('public')
      change(broken.readings, broken)
      expect(judgeMemoryArm(broken).problems).toContain(problem)
    })

    test.each([
      ['another kind', (r) => (r.manifest.kind = 'admin'), 'manifest is for another kind or arm'],
      [
        'an instrumented child',
        (r) => (r.manifest.instrumented.inspector = true),
        'child ran under an inspector, coverage or Jest',
      ],
      ['no exposed GC flag', (r) => (r.manifest.execArgv = []), 'child did not expose GC'],
      [
        'a replaced GC function',
        (r) => (r.manifest.gc.native = false),
        'GC function is not the engine collection',
      ],
      [
        'a changed phase list',
        (r) => r.manifest.phases.reverse(),
        'manifest phases are not the fixed catalogue',
      ],
      [
        'a missing mark',
        (r) => r.marks.splice(3, 1),
        'peak marks are not the fixed phase catalogue',
      ],
      [
        'marks in another order',
        (r) => r.marks.reverse(),
        'peak marks are not the fixed phase catalogue',
      ],
      [
        'a changed foreground catalogue',
        (r) => (r.manifest.foreground.ops[0].bytes = 1),
        'foreground catalogue is not the fixed one',
      ],
      [
        'a peak source that is not the selected one',
        (r) => (r.manifest.peakSource = { name: 'max-rss', units: 'bytes' }),
        'peak source is not the selected one',
      ],
      [
        'a peak source claimed without a reading',
        (r) => (markOf(r, 'enabled:held').vmhwm = null),
        'peak source is claimed but a mark has no reading',
      ],
      [
        'a decreasing authoritative peak',
        (r) => (markOf(r, 'enabled:held').vmhwm = 100 * MIB),
        'authoritative peak decreased',
      ],
      [
        'a decreasing raw maximum',
        (r) => (markOf(r, 'enabled:held').maxRss = 1),
        'raw maximum decreased',
      ],
      [
        'marks out of time order',
        (r) => (markOf(r, 'enabled:held').at = 1),
        'marks are out of order',
      ],
      [
        'an incomplete mark',
        (r) => delete markOf(r, 'enabled:held').covered,
        'enabled:held mark is incomplete',
      ],
      ['an incomplete reading', (r) => delete r.c0.rss, 'c0 reading is incomplete'],
      [
        'retained bytes that are not heap plus external',
        (r) => (r.matched.retained += 1),
        'matched retained bytes are not heap plus external',
      ],
      [
        'array buffers above external bytes',
        (r) => (r.closed.arrayBuffers = r.closed.external + 1),
        'closed array buffers exceed external bytes',
      ],
      [
        'a shortened collection pause',
        (r) => (r.c0.gc.admittedAt = r.c0.at - 99),
        'c0 collection pause was shortened',
      ],
      [
        'collection facts that do not end at the reading',
        (r) => (r.c1.gc.completedAt += 1),
        'c1 collection facts are incomplete',
      ],
      ['readings out of order', (r) => (r.c0.at = r.c1.at + 1), 'readings are out of order'],
      [
        'too few active readings',
        (r) => r.actives.pop(),
        'active readings are not one per codec and one held',
      ],
    ])('fails %s with its own diagnostic', (_name, change, problem) => {
      const result = judged('public', change)
      expect(result.verdict).toBe('fail')
      expect(result.problems).toContain(problem)
    })
  })

  describe('the child context', () => {
    test.each(['childId', 'pid', 'tree', 'runId', 'node', 'v8', 'platform', 'arch'])(
      'fails a manifest without its %s declaration',
      (key) => {
        const result = judged('public', (r) => delete r.manifest[key])
        expect(result.verdict).toBe('fail')
        expect(result.problems).toContain('child context declarations are incomplete')
      }
    )

    test.each([
      ['a child the parent did not spawn', (r, whole) => (whole.exit.producer.childId = 'other')],
      ['another process', (r, whole) => (whole.exit.producer.pid = 101)],
      ['another run in the stamp', (r, whole) => (whole.stamp.runId = 'run-2')],
      ['another build', (r) => (r.manifest.v8 = '13.9')],
      ['no producing process', (r, whole) => delete whole.exit.producer],
      ['a producer of another arm', (r, whole) => (whole.exit.producer.arm = 'collector')],
    ])('fails %s', (_name, change) => {
      expect(judged('public', change)).toEqual({
        verdict: 'fail',
        problems: ['child context differs from the producing process or stamp'],
      })
    })
  })

  describe('the child output catalogue', () => {
    const OTHER = '01020304-0506-0708-090a-0b0c0d0e0f11'
    const receipt = (overrides = {}) =>
      `${JSON.stringify({ receipt: 'disconnect', kind: 'producer', uuid: UUID, childId: 'child-1', at: 12.5, ...overrides })}\n`
    const readings = (extra = {}) =>
      `${JSON.stringify({ readings: { uuid: UUID, manifest: { gc: { available: true } }, ...extra } })}\n`
    /** Feeds lines to a reader whose clock moves 300 ms between the last byte and the close. */
    const read = (lines, { arm = 'public', bytewise = false } = {}) => {
      let now = 1000
      const output = new MemoryOutput({ arm, kind: 'producer', childId: 'child-1' }, () => now)
      const bytes = Buffer.from(lines.join(''))
      if (bytewise) for (const byte of bytes) output.write(Buffer.from([byte]))
      else output.write(bytes)
      now += 300
      return output.finish()
    }

    test('reads one receipt then one readings message, even split inside a character', () => {
      const result = read([receipt(), readings({ note: 'é' })], { bytewise: true })

      expect(result).toMatchObject({
        failure: null,
        receipts: 1,
        results: 1,
        receipt: { at: 12.5, childId: 'child-1' },
        disconnectObservedAt: 1000,
        closedAfterMarkMs: 300,
      })
      expect(result.readings.note).toBe('é')
    })

    test('times a child that owes no receipt from its readings', () => {
      expect(read([readings()], { arm: 'collector' })).toMatchObject({
        failure: null,
        receipts: 0,
        results: 1,
        closedAfterMarkMs: 300,
      })
    })

    test.each([
      [
        'a receipt of another kind',
        [receipt({ kind: 'admin' }), readings()],
        'public',
        'foreign disconnect receipt',
      ],
      [
        'a receipt of another child',
        [receipt({ childId: 'child-2' }), readings()],
        'public',
        'foreign disconnect receipt',
      ],
      [
        'a receipt without a UUID',
        [receipt({ uuid: 'x' }), readings()],
        'public',
        'foreign disconnect receipt',
      ],
      [
        'a receipt with an extra field',
        [receipt({ extra: 1 }), readings()],
        'public',
        'foreign disconnect receipt',
      ],
      [
        'a receipt from a child that owes none',
        [receipt(), readings()],
        'collector',
        'foreign disconnect receipt',
      ],
      [
        'a second receipt',
        [receipt(), receipt(), readings()],
        'public',
        'duplicate or late disconnect receipt',
      ],
      [
        'a receipt after the readings',
        [readings(), receipt()],
        'public',
        'duplicate or late disconnect receipt',
      ],
      [
        'a second readings message',
        [receipt(), readings(), readings()],
        'public',
        'duplicate readings message',
      ],
      [
        'a foreign message',
        [receipt(), '{"hello":1}\n', readings()],
        'public',
        'foreign output message',
      ],
      [
        'a malformed line',
        [receipt(), 'not json\n', readings()],
        'public',
        'unreadable output line',
      ],
      [
        'an unterminated line',
        [receipt(), readings().trim()],
        'public',
        'unterminated output line',
      ],
      ['no readings', [receipt()], 'public', 'missing readings message'],
      ['no receipt', [readings()], 'public', 'missing or unexpected disconnect receipt'],
      [
        'a receipt for another client',
        [receipt({ uuid: OTHER }), readings()],
        'public',
        'disconnect receipt UUID differs from the readings',
      ],
      [
        'output past its bound',
        [`${'x'.repeat(LIMITS.outputBytes + 1)}\n`],
        'public',
        'child output exceeded its bound',
      ],
    ])('fails %s', (_name, lines, arm, failure) => {
      expect(read(lines, { arm }).failure).toBe(failure)
    })
  })

  describe('the child and the host', () => {
    test.each([
      [
        'a child that exceeded its bound',
        (r, whole) => (whole.exit.timedOut = true),
        'child exceeded its bound',
      ],
      [
        'a nonzero exit',
        (r, whole) => (whole.exit.code = 1),
        'child did not exit with code 0 and no signal',
      ],
      [
        'a killed child',
        (r, whole) => (whole.exit.signal = 'SIGKILL'),
        'child did not exit with code 0 and no signal',
      ],
      ['stderr output', (r, whole) => (whole.exit.stderrBytes = 3), 'child wrote to stderr'],
      [
        'unbounded output',
        (r, whole) => (whole.exit.stdoutBytes = 65537),
        'child output exceeded its bound',
      ],
      [
        'a missing receipt',
        (r, whole) => (whole.exit.receipts = 0),
        'child sent the wrong number of disconnect receipts',
      ],
      [
        'a late close',
        (r, whole) => (whole.exit.closedAfterMarkMs = 10001),
        'child did not close within 10 s of its receipt',
      ],
      [
        'a negative close interval',
        (r, whole) => (whole.exit.closedAfterMarkMs = -1),
        'child did not close within 10 s of its receipt',
      ],
      [
        'an untimed close',
        (r, whole) => (whole.exit.closedAfterMarkMs = null),
        'child did not close within 10 s of its receipt',
      ],
      [
        'an unreadable output line',
        (r, whole) => (whole.exit.failure = 'unreadable output line'),
        'child output or spawn failed',
      ],
      [
        'no output verdict at all',
        (r, whole) => delete whole.exit.failure,
        'child output or spawn failed',
      ],
      [
        'a second readings message',
        (r, whole) => (whole.exit.results = 2),
        'child sent the wrong number of readings messages',
      ],
      [
        'incomplete host context',
        (r, whole) => delete whole.context.before.load,
        'host context is incomplete',
      ],
      [
        'a missing pressure field rather than an explicit null',
        (r, whole) => delete whole.context.after.psi,
        'host context is incomplete after cleanup',
      ],
      [
        'incomplete child context',
        (r) => delete r.contexts.boundary.freeBytes,
        'host context is incomplete at the control boundary',
      ],
    ])('fails %s', (_name, change, problem) => {
      const result = judged('public', change)
      expect(result.verdict).toBe('fail')
      expect(result.problems).toContain(problem)
    })

    test('accepts an explicitly unavailable pressure source', () => {
      const result = judged('public', (r, whole) => {
        whole.context.before.psi = null
        r.contexts.final.psi = null
      })
      expect(result).toEqual({ verdict: 'pass', problems: [] })
    })

    test.each([
      ['load', (c) => (c.load = [4, 1, 1]), 'host load was high'],
      ['free memory', (c) => (c.freeBytes = 1e6), 'free memory was low'],
      ['memory pressure', (c) => (c.psi = { memorySomeAvg10: 9 }), 'memory pressure was high'],
    ])('is inconclusive on a noisy host: %s, at every point it is read', (_name, change, text) => {
      for (const [where, label] of [
        [(r, whole) => whole.context.before, ''],
        [(r, whole) => whole.context.after, ' after cleanup'],
        [(r) => r.contexts.control, ' before the control'],
        [(r) => r.contexts.boundary, ' at the control boundary'],
        [(r) => r.contexts.final, ' at the end'],
      ])
        expect(judged('collector', (r, whole) => change(where(r, whole)), 'admin')).toEqual({
          verdict: 'inconclusive',
          problems: [`${text}${label}`],
        })
    })

    test('lets a failure take precedence over noise, and keeps both in the artifact', () => {
      const result = judged('public', (r, whole) => {
        whole.exit.code = 1
        whole.context.before.load = [4, 1, 1]
      })

      expect(result).toEqual({
        verdict: 'fail',
        problems: ['child did not exit with code 0 and no signal', 'host load was high'],
      })
    })
  })

  describe('the capabilities the numbers need', () => {
    test('is inconclusive, not a pass, without the real exposed GC', () => {
      const result = judged('public', (r, whole) => {
        r.manifest.gc = { available: false, native: false }
        whole.exit.receipts = 0
      })

      expect(result).toEqual({
        verdict: 'inconclusive',
        problems: ['real exposed GC is unavailable'],
      })
    })

    test('is inconclusive without a usable absolute peak source, whatever the readings say', () => {
      const result = judged('public', (r) => {
        r.manifest.peakSource = null
        for (const candidate of r.marks) candidate.vmhwm = null
      })

      expect(result).toEqual({
        verdict: 'inconclusive',
        problems: ['no usable absolute peak source'],
      })
    })

    test('is inconclusive when the high water does not cover a current reading', () => {
      const result = judged('public', (r) => {
        markOf(r, 'enabled:gzip').vmhwm = markOf(r, 'enabled:gzip').covered - 1
        for (const phase of ['enabled:held', 'enabled:closed', 'final'])
          markOf(r, phase).vmhwm = markOf(r, 'enabled:gzip').vmhwm
      })

      expect(result).toEqual({
        verdict: 'inconclusive',
        problems: ['peak source does not cover a current reading'],
      })
    })

    test('is inconclusive when the collections were not observed', () => {
      const result = judged('public', (r) => {
        r.c0.gc.observed = 1
        r.c0.gc.checkpoints[0].observed = 0
        r.c0.gc.checkpoints[1].observed = 1
      })

      expect(result).toEqual({
        verdict: 'inconclusive',
        problems: ['c0 collections were not observed'],
      })
    })
  })

  describe('collection convergence', () => {
    test('rejects a checkpoint without its successive post-GC readings', () => {
      const result = judged('public', (r) => delete r.closed.gc.checkpoints)
      expect(result.verdict).toBe('fail')
      expect(result.problems).toContain('closed collection checkpoints are incomplete')
    })

    test.each([null, 1, {}])('rejects a malformed collection checkpoint %p', (point) => {
      const result = judged('public', (r) => {
        r.closed.gc.checkpoints[0] = point
      })
      expect(result.verdict).toBe('fail')
      expect(result.problems).toContain('closed collection reading is incomplete')
    })

    test('does not let an unsettled control become an admitted reference', () => {
      const result = judged('public', (r) => {
        r.c0.gc.checkpoints[0].heapUsed -= 300000
        r.c0.gc.checkpoints[0].retained -= 300000
      })
      expect(result.verdict).toBe('inconclusive')
      expect(result.problems).toContain('c0 collections did not settle')
    })

    test('keeps earlier collection peaks even when the last reading is smaller', () => {
      const result = judged('public', (r) => {
        r.closed.gc.checkpoints[0].external += 70 * MIB
        r.closed.gc.checkpoints[0].retained += 70 * MIB
      })
      expect(result.verdict).toBe('fail')
      expect(result.problems).toContain(
        'external memory peak exceeds 64 MiB above the disabled control'
      )
    })

    test('does not discard an active retained violation found before convergence', () => {
      const result = judged('public', (r) => {
        r.actives[0].gc.checkpoints[0].heapUsed += 9 * MIB
        r.actives[0].gc.checkpoints[0].retained += 9 * MIB
      })
      expect(result.verdict).toBe('fail')
      expect(result.problems).toContain(
        'active retained bytes exceed the matched baseline by more than 8 MiB'
      )
    })

    test('rejects continued collection after an already converged pair', () => {
      const result = judged('public', (r) => {
        const point = r.closed.gc.checkpoints[0]
        r.closed.gc.checkpoints.unshift({
          ...point,
          at: point.at - 150,
          admittedAt: point.admittedAt - 150,
        })
        r.closed.gc.admittedAt -= 150
        r.closed.gc.observed += 2
      })
      expect(result.verdict).toBe('fail')
      expect(result.problems).toContain('closed collection continued after convergence')
    })

    test('rejects a child that can compile loaded functions lazily after its reference', () => {
      const result = judged('public', (r) => {
        r.manifest.execArgv = ['--expose-gc']
      })
      expect(result.verdict).toBe('fail')
      expect(result.problems).toContain('child did not declare eager JavaScript compilation')
    })
  })

  describe('recorded retained growth', () => {
    test.each([
      ['producer', 785472, 969696, 131072],
      ['consumer', 732000, 1343984, 130560],
      ['admin', 477456, 759880, 131072],
      ['shareConsumer', 820760, 1025512, 131072],
    ])(
      'preserves the public %s failure even with an unstable disabled control',
      (kind, controlHeap, heap, backing) => {
        const result = judged(
          'public',
          (r) => {
            r.c1.heapUsed = r.c0.heapUsed + controlHeap
            r.c1.retained = r.c1.heapUsed + r.c1.external
            r.closed.heapUsed = r.matched.heapUsed + heap
            r.closed.external = r.matched.external + backing
            r.closed.arrayBuffers = r.matched.arrayBuffers + backing
            r.closed.retained = r.closed.heapUsed + r.closed.external
            markOf(r, 'enabled:closed').coveredExternal = r.closed.external
          },
          kind
        )
        expect(result.verdict).toBe('fail')
        expect(result.problems).toContain('closed transient bytes exceed 256 KiB')
        expect(result.problems).toContain('control retained bytes are unstable')
      }
    )

    test.each([
      ['producer', 793544, 0, 300536, 65536],
      ['consumer', 734896, 0, 287296, 65536],
      ['admin', 479832, 0, 298128, 65536],
      ['shareConsumer', 824976, 4608, 283392, 131072],
    ])(
      'preserves the collector %s failure even with an unstable disabled control',
      (kind, controlHeap, controlExternal, heap, backing) => {
        const result = judged(
          'collector',
          (r) => {
            r.c1.heapUsed = r.c0.heapUsed + controlHeap
            r.c1.external = r.c0.external + controlExternal
            r.c1.retained = r.c1.heapUsed + r.c1.external
            markOf(r, 'control:c1').coveredExternal = r.c1.external
            r.closed.heapUsed = r.matched.heapUsed + heap
            r.closed.external = r.matched.external + backing
            r.closed.arrayBuffers = r.matched.arrayBuffers + backing
            r.closed.retained = r.closed.heapUsed + r.closed.external
            markOf(r, 'collector:closed').coveredExternal = r.closed.external
          },
          kind
        )
        expect(result.verdict).toBe('fail')
        expect(result.problems).toContain('closed transient bytes exceed 256 KiB')
        expect(result.problems).toContain('control retained bytes are unstable')
      }
    )
  })

  describe('the disabled control', () => {
    test.each([
      ['a telemetry Get', (r) => (r.control.api71 = 1)],
      ['a telemetry Push', (r) => (r.control.api72 = 1)],
      ['a native compression call', (r) => (r.control.nativeSubmissions = 1)],
      ['a client that is not disabled', (r) => (r.control.enabled = true)],
      ['a probe that did not refuse as disabled', (r) => (r.control.probe = 'resolved')],
    ])('fails a control that performed %s', (_name, change) => {
      const result = judged('public', change)
      expect(result.verdict).toBe('fail')
      expect(result.problems).toContain('control performed telemetry')
    })

    test.each([
      ['a socket left open', (r) => (r.control.liveSockets = 1), 'control was not released'],
      ['a request left pending', (r) => (r.control.liveRequests = 1), 'control was not released'],
      [
        'a handle that survives',
        (r) => (r.resources.released.control = { Timeout: [7] }),
        'control handles survive its release',
      ],
      [
        'no control role at all',
        (r) => delete r.resources.released.control,
        'control resource role is missing',
      ],
    ])('fails a control with %s', (_name, change, problem) => {
      const result = judged('public', change)
      expect(result.verdict).toBe('fail')
      expect(result.problems).toContain(problem)
    })

    test('fails a control whose foreground differs from the fixed catalogue, or from the enabled arm', () => {
      expect(judged('public', (r) => (r.control.ops = r.control.ops.slice(1))).problems).toContain(
        'control foreground differs from the fixed catalogue'
      )
      expect(
        judged('public', (r) => (r.enabled.ops = [...r.enabled.ops].reverse())).problems
      ).toContain('enabled foreground differs from the fixed catalogue')
      expect(
        judged('collector', (r) => (r.control.ops = foregroundCatalogue('admin', 3).ops)).problems
      ).toContain('control foreground differs from the fixed catalogue')
    })

    test('fails a mark that understates a reading, or the sampled peak', () => {
      expect(judged('public', (r) => (markOf(r, 'control:c0').covered = 1)).problems).toContain(
        'a peak mark understates a reading'
      )
      expect(
        judged('public', (r) => (markOf(r, 'enabled:gzip').coveredExternal = 1)).problems
      ).toContain('a peak mark understates a reading')
      expect(judged('public', (r) => (r.samples.peak.rss = 1000 * MIB)).problems).toContain(
        'a peak mark understates the sampled peak'
      )
      expect(judged('public', (r) => (r.samples.peak.external = 1e9)).problems).toContain(
        'a peak mark understates the sampled peak'
      )
      expect(judged('public', (r) => delete r.samples.maxGapMs).problems).toContain(
        'sampling facts are incomplete'
      )
    })
  })

  describe('the frozen module population', () => {
    test('keeps late protocol imports inconclusive instead of attributing them to telemetry', () => {
      expect(
        judged('public', (r) => {
          markOf(r, 'enabled:gzip').imports = { count: 401, late: ['wire/pushTelemetryV0.js'] }
        })
      ).toEqual({
        verdict: 'inconclusive',
        problems: ['modules loaded after the disabled reference'],
      })
    })

    test.each([
      ['no loaded population', (r) => (markOf(r, 'loaded').imports = null)],
      ['a negative count', (r) => (markOf(r, 'control:c0').imports.count = -1)],
      ['no late-module list', (r) => delete markOf(r, 'control:c0').imports.late],
      ['an empty module name', (r) => (markOf(r, 'control:c0').imports.late = [''])],
      ['duplicate names', (r) => (markOf(r, 'control:c0').imports.late = ['same', 'same'])],
    ])('fails malformed module facts: %s', (_name, change) => {
      expect(judged('public', change).verdict).toBe('fail')
    })

    test('fails missing module facts and never lets late imports erase a retained violation', () => {
      expect(judged('public', (r) => delete markOf(r, 'control:c0').imports).verdict).toBe('fail')
      const result = judged('public', (r) => {
        markOf(r, 'enabled:gzip').imports = { count: 401, late: ['wire/pushTelemetryV0.js'] }
        r.closed.heapUsed += LIMITS.closedTransient
        r.closed.retained += LIMITS.closedTransient
      })
      expect(result.verdict).toBe('fail')
      expect(result.problems).toEqual([
        'closed transient bytes exceed 256 KiB',
        'modules loaded after the disabled reference',
      ])
    })
  })

  describe('control admission, on both sides of every bound', () => {
    test.each([
      [
        'RSS',
        LIMITS.controlRss,
        (r, extra) => (r.c0.rss = r.c1.rss - extra),
        (r, extra) => (r.c1.rss = r.c0.rss - extra),
        'control RSS is unstable',
      ],
      [
        'retained bytes',
        LIMITS.controlRetained,
        (r, extra) => {
          r.c1.retained = r.c0.retained + extra
          r.c1.heapUsed = r.c1.retained - r.c1.external
        },
        (r, extra) => {
          r.c1.retained = r.c0.retained - extra
          r.c1.heapUsed = r.c1.retained - r.c1.external
        },
        'control retained bytes are unstable',
      ],
    ])(
      'admits a control whose %s differ by the bound in either direction and not by one more',
      (_name, bound, higher, lower, problem) => {
        for (const set of [higher, lower]) {
          expect(judged('public', (r) => set(r, bound))).toEqual({ verdict: 'pass', problems: [] })
          expect(judged('public', (r) => set(r, bound + 1))).toEqual({
            verdict: 'inconclusive',
            problems: [problem],
          })
        }
      }
    )

    test('admits a setup peak of 96 MiB and not a byte more, whatever follows', () => {
      const setup = (bytes) => (r) => {
        markOf(r, 'control:released').vmhwm = bytes
        raiseFrom(r, 'enabled:start', Math.max(bytes, 120 * MIB))
      }

      expect(LIMITS.setupPeak).toBe(100663296)
      expect(judged('public', setup(LIMITS.setupPeak))).toEqual({ verdict: 'pass', problems: [] })
      expect(judged('public', setup(LIMITS.setupPeak + 1))).toEqual({
        verdict: 'inconclusive',
        problems: ['setup exceeds the 96 MiB admission allowance'],
      })
    })

    test('does not let a subtraction rescue an inflated control', () => {
      const result = judged('public', (r) => {
        for (const reading of [r.c0, r.c1, r.released]) reading.rss = 140 * MIB
        for (const phase of ['control:c0', 'control:c1', 'control:released']) {
          markOf(r, phase).rss = 140 * MIB
          markOf(r, phase).covered = 140 * MIB
        }
        raiseFrom(r, 'control:c0', 140 * MIB)
      })

      expect(result).toEqual({
        verdict: 'inconclusive',
        problems: ['setup exceeds the 96 MiB admission allowance'],
      })
    })

    test('counts an earlier peak: a high water set before the enabled arm still dominates it', () => {
      const result = judged('public', (r) => {
        raiseFrom(r, 'enabled:registered', 150 * MIB)
      })

      expect(result).toEqual({
        verdict: 'fail',
        problems: ['telemetry RSS peak exceeds 64 MiB above the disabled control'],
      })
    })
  })

  describe('the incremental and absolute limits, on both sides', () => {
    test('allows the RSS peak to be exactly 64 MiB above the control and not a byte more', () => {
      const peak = (bytes) => (r) => raiseFrom(r, 'enabled:gzip', bytes)

      expect(judged('public', peak(B + LIMITS.rssDelta))).toEqual({ verdict: 'pass', problems: [] })
      expect(judged('public', peak(B + LIMITS.rssDelta + 1))).toEqual({
        verdict: 'fail',
        problems: ['telemetry RSS peak exceeds 64 MiB above the disabled control'],
      })
    })

    test('allows the whole arm to reach 160 MiB with a control of 96 MiB, and not a byte more', () => {
      const arm = (peak) => (r) => {
        for (const reading of [r.c0, r.c1, r.released]) reading.rss = LIMITS.setupPeak
        for (const phase of ['control:c0', 'control:c1', 'control:released']) {
          markOf(r, phase).rss = LIMITS.setupPeak
          markOf(r, phase).covered = LIMITS.setupPeak
        }
        raiseFrom(r, 'control:c0', LIMITS.setupPeak)
        raiseFrom(r, 'enabled:start', peak)
      }

      expect(LIMITS.absolutePeak).toBe(167772160)
      expect(judged('public', arm(LIMITS.setupPeak + LIMITS.rssDelta))).toEqual({
        verdict: 'pass',
        problems: [],
      })
      expect(judged('public', arm(LIMITS.setupPeak + LIMITS.rssDelta + 1)).problems).toEqual([
        'absolute current-image peak exceeds 160 MiB',
        'telemetry RSS peak exceeds 64 MiB above the disabled control',
      ])
    })

    test('judges the absolute backstop even when the control is inadmissible, and never passes it', () => {
      const inadmissible = (peak) => (r) => {
        markOf(r, 'control:released').vmhwm = LIMITS.setupPeak + 1
        raiseFrom(r, 'enabled:start', peak)
      }

      expect(judged('public', inadmissible(LIMITS.absolutePeak))).toEqual({
        verdict: 'inconclusive',
        problems: ['setup exceeds the 96 MiB admission allowance'],
      })
      expect(judged('public', inadmissible(LIMITS.absolutePeak + 1))).toEqual({
        verdict: 'fail',
        problems: [
          'absolute current-image peak exceeds 160 MiB',
          'setup exceeds the 96 MiB admission allowance',
        ],
      })
    })

    test('allows external memory to be exactly 64 MiB above the control and not a byte more', () => {
      const external = (bytes) => (r) => {
        markOf(r, 'enabled:gzip').coveredExternal = bytes
      }

      expect(judged('public', external(3e6 + LIMITS.externalDelta))).toEqual({
        verdict: 'pass',
        problems: [],
      })
      expect(judged('public', external(3e6 + LIMITS.externalDelta + 1))).toEqual({
        verdict: 'fail',
        problems: ['external memory peak exceeds 64 MiB above the disabled control'],
      })
    })

    test('takes the lower control reading as the baseline, never the higher or the control peak', () => {
      const reference = (r) => {
        r.c0.rss = 76 * MIB
        r.c1.rss = 74 * MIB
        markOf(r, 'control:c0').covered = 76 * MIB
        raiseFrom(r, 'enabled:gzip', 74 * MIB + LIMITS.rssDelta + 1)
      }

      expect(judged('public', reference).problems).toEqual([
        'telemetry RSS peak exceeds 64 MiB above the disabled control',
      ])
    })

    test.each([
      [
        'active retained bytes',
        LIMITS.retained,
        (r, extra) => {
          r.actives[0].retained = r.matched.retained + extra
          r.actives[0].heapUsed = r.actives[0].retained - r.actives[0].external
        },
        'active retained bytes exceed the matched baseline by more than 8 MiB',
      ],
      [
        'closed transient bytes',
        LIMITS.closedTransient,
        (r, extra) => {
          r.closed.retained = r.matched.retained + extra
          r.closed.heapUsed = r.closed.retained - r.closed.external
        },
        'closed transient bytes exceed 256 KiB',
      ],
    ])('allows %s exactly at the bound and fails one more', (_name, bound, set, problem) => {
      expect(judged('public', (r) => set(r, bound))).toEqual({ verdict: 'pass', problems: [] })
      expect(judged('public', (r) => set(r, bound + 1))).toEqual({
        verdict: 'fail',
        problems: [problem],
      })
    })

    test('keeps a retained violation as a failure on a noisy host and against an unstable control', () => {
      const result = judged('public', (r, whole) => {
        r.closed.retained = r.matched.retained + LIMITS.closedTransient + 1
        r.closed.heapUsed = r.closed.retained - r.closed.external
        r.c0.rss = r.c1.rss - LIMITS.controlRss - 1
        whole.context.before.load = [4, 1, 1]
      })

      expect(result).toEqual({
        verdict: 'fail',
        problems: [
          'closed transient bytes exceed 256 KiB',
          'host load was high',
          'control RSS is unstable',
        ],
      })
    })
  })

  describe('the native work', () => {
    test.each([
      ['an overflowed journal', (r) => (r.native.overflow = true), 'native journal overflowed'],
      ['work still pending', (r) => (r.native.live = 1), 'native work is still pending'],
      [
        'two jobs occupied at once',
        (r) => (r.native.maxLive = 2),
        'more than one native job was occupied at once',
      ],
      ['no journal', (r) => delete r.native, 'native journal is missing'],
      [
        'a job without issued identities',
        (r) => (r.native.rows[0].jobId = null),
        'gzip job 1 lacks issued identities',
      ],
      ['a job out of order', (r) => (r.native.rows[1].id = 1), 'gzip job 1 is out of order'],
      [
        'raw bytes out of range',
        (r) => (r.native.rows[0].inputBytes = LIMITS.rawBytes + 1),
        'gzip job 1 raw bytes are out of range',
      ],
      [
        'an uncompressed result',
        (r) => (r.native.rows[0].resultBytes = r.native.rows[0].inputBytes),
        'gzip job 1 payload was not compressed',
      ],
      [
        'a job that did not resolve',
        (r) => (r.native.rows[0].outcome = 'rejected'),
        'gzip job 1 did not resolve',
      ],
      [
        'a backing store counted twice',
        (r) => (r.native.rows[0].distinctBackingBytes += 4000),
        'gzip job 1 backing stores are not counted once each',
      ],
      [
        'a capacity below the bytes it holds',
        (r) => {
          r.native.rows[0].inputBackingBytes = 1
          r.native.rows[0].distinctBackingBytes = 4001
        },
        'gzip job 1 backing capacity is below its bytes',
      ],
      [
        'a job without a settlement reading',
        (r) => (r.native.rows[0].settled = null),
        'gzip job 1 lacks a reading at submission or settlement',
      ],
      [
        'a job that settled before it was submitted',
        (r) => (r.native.rows[0].settled.at = 1),
        'gzip job 1 settled before it was submitted',
      ],
      [
        'no retired completion',
        (r) => r.native.rows.pop(),
        'gzip native work lacks a current or a retired completion',
      ],
      [
        'work of another codec substituted for the codec the arm claims',
        (r) => r.native.rows.forEach((row) => (row.codec = 'zstd')),
        'gzip native work lacks a current or a retired completion',
      ],
      [
        'too few readings around the work',
        (r) => (r.samples.count = 3),
        'too few current readings around native work',
      ],
    ])('fails %s', (_name, change, problem) => {
      for (const arm of ARMS) {
        const result = judged(arm, change)
        expect(result.verdict).toBe('fail')
        expect(result.problems).toContain(problem)
      }
    })

    test('counts a backing store that the input and the result share once', () => {
      const result = judged('public', (r) => {
        for (const row of r.native.rows) {
          row.resultBackingId = row.inputBackingId
          row.inputBackingBytes = 354000
          row.resultBackingBytes = 354000
          row.distinctBackingBytes = 354000
        }
      })

      expect(result).toEqual({ verdict: 'pass', problems: [] })
    })

    test.each([
      [
        'a missing sampling period',
        (r) => delete r.manifest.sampleMs,
        'native sampling interval is not 20 ms',
      ],
      [
        'a changed sampling period',
        (r) => (r.samples.intervalMs = 40),
        'native sampling interval is not 20 ms',
      ],
      [
        'missing pending windows',
        (r) => delete r.samples.pendingWindows,
        'native pending windows are incomplete',
      ],
      [
        'a missing window count',
        (r) => delete r.samples.windows,
        'native pending windows are incomplete',
      ],
      [
        'undescribed windows',
        (r) => (r.samples.overflow = true),
        'native pending windows are incomplete',
      ],
      [
        'a window that ends before its job settles',
        (r) => (r.samples.pendingWindows[0].end -= 1),
        'gzip job 1 pending window does not cover its native work',
      ],
      [
        'a window without both edge readings',
        (r) => (r.samples.pendingWindows[0].count = 1),
        'gzip job 1 pending window does not cover its native work',
      ],
      [
        'a window above the sampled peak',
        (r) => (r.samples.pendingWindows[1].rss = 101 * MIB),
        'sampled peak understates a pending window',
      ],
      [
        'native retained bytes that are not heap plus external',
        (r) => (r.native.rows[0].submitted.retained += 1),
        'gzip job 1 retained bytes are not heap plus external',
      ],
      [
        'native array buffers above external bytes',
        (r) => (r.native.rows[1].settled.arrayBuffers = 11e6),
        'gzip job 2 array buffers exceed external bytes',
      ],
      [
        'a native reading before the loaded mark',
        (r) => (r.native.rows[0].submitted.at = 10),
        'gzip job 1 is outside chronological peak coverage',
      ],
      [
        'a mark that understates a native reading',
        (r) => (r.native.rows[0].settled.rss = 119 * MIB),
        'a peak mark understates a native reading',
      ],
    ])('fails %s', (_name, change, problem) => {
      for (const arm of ARMS) {
        const result = judged(arm, change)
        expect(result.verdict).toBe('fail')
        expect(result.problems).toContain(problem)
      }
    })

    test('folds a native reading into the external gate, whatever the marks say', () => {
      const raise = (r) => {
        const reading = r.native.rows[0].submitted
        reading.external = 104857600
        reading.retained = reading.heapUsed + reading.external
      }

      const understated = judged('public', raise)
      expect(understated.verdict).toBe('fail')
      expect(understated.problems).toEqual(
        expect.arrayContaining([
          'external memory peak exceeds 64 MiB above the disabled control',
          'a peak mark understates a native reading',
          'gzip job 1 pending window does not cover its native work',
        ])
      )
      expect(understated.problems).toHaveLength(3)
      expect(
        judged('public', (r) => {
          raise(r)
          markOf(r, 'enabled:gzip').coveredExternal = 104857600
          r.samples.pendingWindows[0].external = 104857600
          r.samples.peak.external = 104857600
        })
      ).toEqual({
        verdict: 'fail',
        problems: ['external memory peak exceeds 64 MiB above the disabled control'],
      })
    })
  })

  describe('the public client facts', () => {
    test.each([
      ['a changed UUID', (r) => (r.uuidHeld = 'x'), 'client instance id is missing or changed'],
      ['a missing UUID', (r) => (r.uuid = null), 'client instance id is missing or changed'],
      ['a short descriptor set', (r) => (r.descriptors = 127), 'descriptor count is not 128'],
      [
        'another descriptor population',
        (r) => (r.manifest.fixture.descriptorBytes = 1),
        'descriptor population is not the maximum',
      ],
      [
        'a matched baseline that is not the disconnected registry',
        (r) => (r.matchedState.connected = true),
        'matched baseline is not the disconnected registry',
      ],
      [
        'no telemetry request',
        (r) => (r.enabled.api72 = 0),
        'enabled client made no telemetry request',
      ],
      [
        'too few accepted pushes',
        (r) => (r.pushes.gzip.accepted = 2),
        'gzip accepted pushes are fewer than required',
      ],
      [
        'a reopen whose connect followed a fixture await',
        (r) =>
          (r.reopens[0].events = [
            { action: 'disconnect', at: 4500 },
            { action: 'yield', at: 4500.25 },
            { action: 'connect', at: 4500.5 },
          ]),
        'gzip has no immediate reopen from the close continuation',
      ],
      [
        'a reopen without its witnessed order',
        (r) => delete r.reopens[0].events,
        'gzip has no immediate reopen from the close continuation',
      ],
      [
        'a receipt sent after a fixture await',
        (r) =>
          (r.disconnect = [
            { action: 'disconnect', at: 6500 },
            { action: 'yield', at: 6500.25 },
            { action: 'receipt', at: 6500.5 },
          ]),
        'disconnect receipt is not at the public boundary',
      ],
      [
        'a receipt timed elsewhere',
        (r, whole) => (whole.exit.receipt.at = 7000),
        'disconnect receipt is not at the public boundary',
      ],
      [
        'a receipt of another kind',
        (r, whole) => (whole.exit.receipt.kind = 'admin'),
        'disconnect receipt is not at the public boundary',
      ],
      [
        'no recorded receipt',
        (r, whole) => (whole.exit.receipt = null),
        'disconnect receipt is not at the public boundary',
      ],
      [
        'a same-count substitution of one registration',
        (r) => {
          const metric = maximumDescriptor(5)
          r.registry[5] = registryEntry({ ...metric, name: `y${metric.name.slice(1)}` })
        },
        'retained registry fingerprint differs from successful public registrations',
      ],
      [
        'a registration charged below its metadata',
        (r) => (r.registry[0].charge -= 1),
        'retained registry fingerprint differs from successful public registrations',
      ],
      [
        'a checkpoint whose registered population changed',
        (r) => (r.actives[0].registry.hash = 'x'),
        'retained registry fingerprint differs from successful public registrations',
      ],
      [
        'a checkpoint where the client admitted a 129th descriptor',
        (r) => (r.closed.registry.overflow = null),
        'retained registry fingerprint differs from successful public registrations',
      ],
      [
        'a payload that is not in its codec format',
        (r) => (r.decoded[0].format = false),
        'gzip payload is not in its codec format',
      ],
      [
        'a decoded size that differs from the native input',
        (r) => (r.decoded[0].rawBytes = 1),
        'gzip decoded payload differs from the native evidence',
      ],
      [
        'a pushed size that is not a native result',
        (r) => (r.pushes.gzip.transmittedBytes = 1),
        'gzip decoded payload differs from the native evidence',
      ],
      [
        'resource roles that are missing',
        (r) => delete r.resources.closed,
        'resource roles are missing',
      ],
      [
        'a target handle that survives the close',
        (r) => (r.resources.closed.target = { Timeout: [9] }),
        'target handles survive the public close',
      ],
      [
        'an unknown handle that appears',
        (r) => (r.resources.closed.unknown = { Timeout: [9] }),
        'resource roles: unknown Timeout resource survives',
      ],
    ])('fails %s', (_name, change, problem) => {
      const result = judged('public', change)
      expect(result.verdict).toBe('fail')
      expect(result.problems).toContain(problem)
    })

    test('fails a descriptor a production client swapped for one of the same size', () => {
      const {
        DESCRIPTORS,
        MemoryForeground,
        descriptor,
      } = require('../../src/telemetry/__tests__/fixtures/memoryForeground')
      const foreground = new MemoryForeground('producer', { enabled: true }).build()
      try {
        for (let index = 0; index < DESCRIPTORS; index++) foreground.register(descriptor(index))
        const kept = foreground.fingerprint(UUID)
        expect(kept).toEqual(retainedRegistry())
        expect(judged('public', (r) => (r.closed.registry = kept))).toEqual({
          verdict: 'pass',
          problems: [],
        })

        const original = descriptor(5)
        const substitute = { ...original, name: `y${original.name.slice(1)}` }
        expect(registryEntry(substitute).charge).toBe(registryEntry(original).charge)
        foreground.client.unregisterMetricFromSubscription({
          name: original.name,
          attributes: original.attributes,
        })
        foreground.client.registerMetricForSubscription(substitute)
        const swapped = foreground.fingerprint(UUID)
        expect(swapped).toMatchObject({ descriptors: DESCRIPTORS - 1, overflow: 'limit' })
        const result = judged('public', (r) => (r.closed.registry = swapped))
        expect(result.verdict).toBe('fail')
        expect(result.problems).toContain(
          'retained registry fingerprint differs from successful public registrations'
        )
      } finally {
        foreground.release()
      }
    })
  })

  describe('the collector facts', () => {
    test.each([
      [
        'the wrong codecs',
        (r) => (r.builds[0].codec = 'zstd'),
        'native builds are not the expected codecs',
      ],
      [
        'another compression type',
        (r) => (r.builds[0].compressionType = 4),
        'gzip build used another codec',
      ],
      [
        'raw bytes out of range',
        (r) => (r.builds[0].rawBytes = 10),
        'gzip raw bytes are out of range',
      ],
      [
        'an uncompressed payload',
        (r) => (r.builds[0].transmittedBytes = 350000),
        'gzip payload was not compressed',
      ],
      [
        'fewer than 257 points',
        (r) => (r.builds[0].points = 256),
        'gzip collector build is incomplete',
      ],
      [
        'no retired completion',
        (r) => (r.builds[0].retiredCompletion = false),
        'gzip collector build is incomplete',
      ],
    ])('fails %s', (_name, change, problem) => {
      const result = judged('collector', change)
      expect(result.verdict).toBe('fail')
      expect(result.problems).toContain(problem)
    })
  })

  describe('a directory of artifacts', () => {
    let directory
    const write = (arm, kind, value = record(arm, kind)) =>
      fs.writeFileSync(path.join(directory, `resources-${arm}-${kind}.json`), JSON.stringify(value))
    const writeAll = () => ARMS.forEach((arm) => KINDS.forEach((kind) => write(arm, kind)))

    beforeEach(() => {
      directory = fs.mkdtempSync(path.join(os.tmpdir(), 'resource-artifacts-'))
    })
    afterEach(() => fs.rmSync(directory, { recursive: true, force: true }))

    test('accepts one stamped artifact for every arm and kind', () => {
      writeAll()
      const checked = validateResourceArtifacts(directory, { tree: TREE, node: NODE })
      expect(checked.results).toHaveLength(8)
      expect(checked.ok).toBe(true)
    })

    test('fails a missing kind even when the rest pass', () => {
      writeAll()
      fs.rmSync(path.join(directory, 'resources-public-admin.json'))
      const checked = validateResourceArtifacts(directory, { tree: TREE, node: NODE })
      expect(checked.ok).toBe(false)
      expect(checked.results.filter((result) => result.verdict !== 'pass')).toEqual([
        { arm: 'public', kind: 'admin', verdict: 'fail', problems: ['artifact is missing'] },
      ])
    })

    test('fails an artifact stamped for another tree, Node, arm or kind', () => {
      writeAll()
      for (const change of [
        (r) => (r.stamp.tree = 'b'.repeat(40)),
        (r) => (r.stamp.node = 'v22.0.0'),
        (r) => (r.readings.manifest.node = 'v22.0.0'),
        (r) => (r.kind = 'admin'),
      ]) {
        const forged = record('collector', 'producer')
        change(forged)
        write('collector', 'producer', forged)
        const checked = validateResourceArtifacts(directory, { tree: TREE, node: NODE })
        const result = checked.results.find((r) => r.arm === 'collector' && r.kind === 'producer')
        expect(result.verdict).toBe('fail')
        expect(result.problems[0]).toContain('not stamped for this tree, Node, arm and kind')
      }
    })

    test('fails an artifact that is not JSON', () => {
      writeAll()
      fs.writeFileSync(path.join(directory, 'resources-public-producer.json'), '{')
      const checked = validateResourceArtifacts(directory, { tree: TREE, node: NODE })
      expect(checked.results[0]).toMatchObject({
        verdict: 'fail',
        problems: [expect.stringContaining('artifact is not valid JSON')],
      })
    })

    test('fails two artifacts that claim one child, so a control cannot serve another arm', () => {
      writeAll()
      const borrowed = record('public', 'admin')
      borrowed.readings.manifest.childId = 'child-public-producer'
      write('public', 'admin', borrowed)

      const checked = validateResourceArtifacts(directory, { tree: TREE, node: NODE })

      expect(checked.ok).toBe(false)
      expect(
        checked.results
          .filter((result) => result.verdict !== 'pass')
          .map(({ arm, kind, problems }) => [arm, kind, problems[0]])
      ).toEqual([
        ['public', 'producer', 'artifact shares a child identity with another'],
        ['public', 'admin', 'artifact shares a child identity with another'],
      ])
    })

    test('fails an artifact without a child identity before checking for shared ones', () => {
      writeAll()
      const anonymous = record('public', 'admin')
      delete anonymous.readings.manifest.childId
      write('public', 'admin', anonymous)

      const checked = validateResourceArtifacts(directory, { tree: TREE, node: NODE })

      expect(
        checked.results
          .filter((result) => result.verdict !== 'pass')
          .map(({ arm, kind, problems }) => [arm, kind, problems[0]])
      ).toEqual([['public', 'admin', 'artifact has no child identity']])
    })

    test('fails the old schema in a directory even when everything else is in place', () => {
      writeAll()
      const old = record('public', 'consumer')
      old.schema = 1
      write('public', 'consumer', old)

      const checked = validateResourceArtifacts(directory, { tree: TREE, node: NODE })

      expect(
        checked.results.find((r) => r.arm === 'public' && r.kind === 'consumer')
      ).toMatchObject({
        verdict: 'fail',
        problems: ['unknown schema'],
      })
    })
  })
})

describe('telemetry timeout owner diagnostics', () => {
  test('reports a leaked timer creation stack and releases every diagnostic resource', async () => {
    // eslint-disable-next-line n/no-unsupported-features/node-builtins -- development-only async resource identity
    const { executionAsyncId } = require('async_hooks')
    const { TimeoutDiagnostics } = require('../timeoutDiagnostics')
    const roles = new ResourceRoles().start()
    const diagnostics = new TimeoutDiagnostics().start()
    const before = roles.read()
    const triggerAsyncId = executionAsyncId()
    const timer = setTimeout(() => {}, 60000)
    try {
      const report = diagnostics.read()
      const ids = roles.read().unknown.Timeout
      expect(report.live).toHaveLength(1)
      expect(report.live[0]).toMatchObject({
        id: ids[0],
        triggerAsyncId,
        createdAt: expect.any(Number),
        destroyedAt: null,
        stack: expect.stringContaining('testHelpers/__tests__/resourceProbe.spec.js:'),
      })
      expect(report.live[0].stack).not.toContain(path.resolve(__dirname, '../..'))
      expect(report.live[0].stack).not.toContain('node_modules')
      expect(report.created).toBe(1)
      clearTimeout(timer)
      await new Promise((resolve) => setImmediate(resolve))
      await new Promise((resolve) => setImmediate(resolve))
      expect(diagnostics.read()).toMatchObject({
        live: [],
        created: 1,
        destroyed: 1,
        lastDestroyedAt: expect.any(Number),
        dropped: 0,
      })
      diagnostics.stop()
      expect(roles.read()).toEqual(before)
      const later = setTimeout(() => {}, 60000)
      clearTimeout(later)
      expect(diagnostics.read()).toMatchObject({ live: [], created: 1 })
    } finally {
      clearTimeout(timer)
      diagnostics.stop()
      roles.stop()
    }
  })

  test('bounds live timer facts and reports dropped diagnostics', async () => {
    const { TimeoutDiagnostics } = require('../timeoutDiagnostics')
    const diagnostics = new TimeoutDiagnostics({ capacity: 1 }).start()
    const timers = [setTimeout(() => {}, 60000), setTimeout(() => {}, 60000)]
    try {
      expect(diagnostics.read()).toMatchObject({
        created: 2,
        dropped: 1,
        live: [expect.any(Object)],
      })
      for (const timer of timers) clearTimeout(timer)
      await new Promise((resolve) => setImmediate(resolve))
      await new Promise((resolve) => setImmediate(resolve))
      expect(diagnostics.read()).toMatchObject({ live: [], destroyed: 1 })
    } finally {
      for (const timer of timers) clearTimeout(timer)
      diagnostics.stop()
    }
  })
})
