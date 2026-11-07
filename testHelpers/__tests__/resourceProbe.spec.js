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
  judgeMemoryArm,
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
      })
      expect(events).toEqual(['gc', 'pause 100', 'gc', 'memory'])
    } finally {
      readMemory.mockRestore()
      readUsage.mockRestore()
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

describe('resource artifact validation', () => {
  const TREE = 'a'.repeat(40)
  const NODE = 'v26.7.0'
  const settled = (retained) => ({
    heapUsed: retained - 2000,
    external: 2000,
    arrayBuffers: 1000,
    retained,
    rss: 50000000,
  })
  const context = () => ({
    cpus: 4,
    load: [0.2, 0.2, 0.2],
    freeBytes: 4e9,
    totalBytes: 8e9,
    psi: { memorySomeAvg10: 0.1 },
  })

  /** A passing record of one arm, built fresh so each test can break exactly one fact. */
  const record = (arm, kind) => ({
    schema: 1,
    arm,
    kind,
    stamp: { tree: TREE, node: NODE },
    context: { before: context(), after: context() },
    exit: {
      code: 0,
      signal: null,
      timedOut: false,
      stdoutBytes: 900,
      stderrBytes: 0,
      receipts: arm === 'public' ? 1 : 0,
      closedAfterMarkMs: 250,
    },
    readings: {
      zstdSupported: false,
      uuid: '01020304-0506-0708-090a-0b0c0d0e0f10',
      uuidHeld: '01020304-0506-0708-090a-0b0c0d0e0f10',
      descriptors: 128,
      ...(arm === 'public' && {
        baselineResources: { target: {}, fixture: {}, unknown: {} },
        matchedResources: { target: {}, fixture: {}, unknown: {} },
        closedResources: { target: {}, fixture: {}, unknown: {} },
      }),
      baseline: settled(5000000),
      matched: settled(6000000),
      held: settled(7000000),
      closed: settled(6100000),
      baselinePeakRss: 55000000,
      peakRss: 60000000,
      rssPeakSample: 59000000,
      rssSamples: 20,
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
    },
  })

  test('passes a complete record of each arm', () => {
    for (const arm of ARMS)
      expect(judgeMemoryArm(record(arm, 'producer'))).toEqual({ verdict: 'pass', problems: [] })
  })

  test.each([
    [
      'a child that exceeded its bound',
      (r) => (r.exit.timedOut = true),
      'child exceeded its bound',
    ],
    ['a nonzero exit', (r) => (r.exit.code = 1), 'child did not exit with code 0 and no signal'],
    [
      'a killed child',
      (r) => (r.exit.signal = 'SIGKILL'),
      'child did not exit with code 0 and no signal',
    ],
    ['stderr output', (r) => (r.exit.stderrBytes = 3), 'child wrote to stderr'],
    [
      'a missing receipt',
      (r) => (r.exit.receipts = 0),
      'child sent the wrong number of disconnect receipts',
    ],
    [
      'a late close',
      (r) => (r.exit.closedAfterMarkMs = 10001),
      'child did not close within 10 s of its receipt',
    ],
    ['no readings', (r) => (r.readings = null), 'readings are missing'],
    [
      'retained bytes above the matched baseline',
      (r) => (r.readings.held = settled(6000000 + 8388609)),
      'held retained bytes exceed the matched baseline by more than 8 MiB',
    ],
    [
      'a transient that is not released',
      (r) => (r.readings.closed = settled(6000000 + 262145)),
      'closed transient bytes exceed 256 KiB',
    ],
    ['a peak above 64 MiB', (r) => (r.readings.peakRss = 67108865), 'peak RSS exceeds 64 MiB'],
    [
      'a peak below its own baseline',
      (r) => (r.readings.peakRss = 50000000),
      'lifetime peak is below baseline',
    ],
    [
      'retained bytes that are not heap plus external',
      (r) => (r.readings.held.retained += 1),
      'held retained bytes are not heap plus external',
    ],
    [
      'a changed UUID',
      (r) => (r.readings.uuidHeld = 'x'),
      'client instance id is missing or changed',
    ],
    [
      'a short descriptor set',
      (r) => (r.readings.descriptors = 127),
      'descriptor count is not 128',
    ],
    [
      'an uncompressed payload',
      (r) => (r.readings.builds[0].transmittedBytes = 350000),
      'gzip payload was not compressed',
    ],
    ['incomplete host context', (r) => delete r.context.before.load, 'host context is incomplete'],
  ])('fails %s with its own diagnostic', (_name, break_, problem) => {
    const broken = record('public', 'producer')
    break_(broken)
    const judged = judgeMemoryArm(broken)
    expect(judged.verdict).toBe('fail')
    expect(judged.problems).toContain(problem)
  })

  test('is inconclusive, not a pass, when the setup alone is over 64 MiB', () => {
    const noisy = record('public', 'producer')
    noisy.readings.baselinePeakRss = 68000000
    noisy.readings.peakRss = 69000000
    expect(judgeMemoryArm(noisy)).toEqual({
      verdict: 'inconclusive',
      problems: ['setup exceeds the 64 MiB RSS limit'],
    })
  })

  test.each([
    ['load', (c) => (c.load = [4, 1, 1]), 'host load was high'],
    ['free memory', (c) => (c.freeBytes = 1e6), 'free memory was low'],
    ['memory pressure', (c) => (c.psi = { memorySomeAvg10: 9 }), 'memory pressure was high'],
  ])('is inconclusive on a noisy host: %s', (_name, change, problem) => {
    const noisy = record('collector', 'admin')
    change(noisy.context.before)
    expect(judgeMemoryArm(noisy)).toEqual({ verdict: 'inconclusive', problems: [problem] })
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
        (r) => (r.kind = 'admin'),
      ]) {
        const forged = record('collector', 'producer')
        change(forged)
        write('collector', 'producer', forged)
        const checked = validateResourceArtifacts(directory, { tree: TREE, node: NODE })
        expect(
          checked.results.find((r) => r.arm === 'collector' && r.kind === 'producer')
        ).toMatchObject({
          verdict: 'fail',
          problems: [expect.stringContaining('not stamped for this tree')],
        })
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
  })
})
