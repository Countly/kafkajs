const fs = require('fs')
const {
  PEAK_SOURCE,
  WINDOW_CAPACITY,
  BoundaryWitness,
  ProcessMemory,
  PendingSampler,
  NativeJournal,
  readProcStatus,
  selectPeakSource,
} = require('../processMemory')

const STATUS = 'Name:\tnode\nVmHWM:\t    4096 kB\nVmRSS:\t    2048 kB\nThreads:\t4\n'

describe('the kernel high water', () => {
  afterEach(() => jest.restoreAllMocks())

  test('reads the high water and the current size in bytes from the process status', () => {
    jest.spyOn(fs, 'readFileSync').mockReturnValue(STATUS)

    expect(readProcStatus()).toEqual({ vmhwm: 4194304, vmrss: 2097152 })
  })

  test.each([
    ['a missing high water', 'VmRSS:\t 2048 kB\n'],
    ['a missing current size', 'VmHWM:\t 4096 kB\n'],
    ['an unparseable field', 'VmHWM:\t lots kB\nVmRSS:\t 2048 kB\n'],
  ])('is unavailable with %s', (_name, text) => {
    jest.spyOn(fs, 'readFileSync').mockReturnValue(text)

    expect(readProcStatus()).toBeNull()
  })

  test('is unavailable where the status cannot be read', () => {
    jest.spyOn(fs, 'readFileSync').mockImplementation(() => {
      throw new Error('ENOENT')
    })

    expect(readProcStatus()).toBeNull()
  })

  test('selects the source only when it can be read, and hands out a copy', () => {
    const selected = selectPeakSource(() => ({ vmhwm: 1, vmrss: 1 }))

    expect(selected).toEqual({ name: 'proc-self-status-vmhwm', units: 'bytes' })
    expect(selected).not.toBe(PEAK_SOURCE)
    expect(selectPeakSource(() => null)).toBeNull()
  })
})

describe('ProcessMemory', () => {
  const instances = []
  /** A meter over scripted readings, so each fact is exact. */
  const meter = (overrides = {}) => {
    const readings = { rss: 100, heapUsed: 10, external: 7, arrayBuffers: 3 }
    const status = { vmhwm: 500, vmrss: 100 }
    let tick = 0
    const memory = new ProcessMemory({
      read: () => status,
      gc: () => {},
      usage: () => ({ ...readings }),
      maxRss: () => 9999,
      now: () => ++tick,
      sleep: async () => {},
      ...overrides,
    })
    instances.push(memory)
    return { memory, readings, status }
  }

  afterEach(() => instances.splice(0).forEach((memory) => memory.stop()))

  test('freezes application imports at the loaded boundary and preserves later module names', () => {
    const files = ['probe.js']
    const { memory } = meter({ modules: () => [...files] })
    expect(memory.mark('startup').imports).toBeNull()
    files.push('client.js')
    memory.mark('loaded')
    expect(memory.mark('control:start').imports).toEqual({ count: 2, late: [] })
    files.push('lazy-wire.js')
    expect(memory.mark('control:c0').imports).toEqual({ count: 3, late: ['lazy-wire.js'] })
    expect(memory.mark('final').imports).toEqual({ count: 3, late: ['lazy-wire.js'] })
    expect(memory.mark('loaded').imports.late).toEqual(['lazy-wire.js'])
  })

  test('reads heap, external and RSS, and never adds the array buffers to the external bytes', () => {
    const { memory } = meter()

    expect(memory.current()).toEqual({
      at: 1,
      rss: 100,
      heapUsed: 10,
      external: 7,
      arrayBuffers: 3,
      retained: 17,
    })
  })

  test('records at a mark the high water, the raw maximum and what its interval covered', () => {
    const { memory, readings, status } = meter()
    readings.rss = 300
    readings.external = 40
    memory.current()
    readings.rss = 120
    readings.external = 9
    status.vmhwm = 800

    expect(memory.mark('first')).toEqual({
      phase: 'first',
      at: 2,
      rss: 120,
      vmhwm: 800,
      maxRss: 9999,
      covered: 300,
      coveredExternal: 40,
      imports: null,
    })
    expect(memory.mark('second')).toMatchObject({
      phase: 'second',
      covered: 120,
      coveredExternal: 9,
    })
    expect(memory.marks.map((mark) => mark.phase)).toEqual(['first', 'second'])
  })

  test('takes the current reading before the kernel high water, so the high water covers it', () => {
    const order = []
    const { memory } = meter({
      usage: () => {
        order.push('current')
        return { rss: 1, heapUsed: 1, external: 1, arrayBuffers: 0 }
      },
      read: () => {
        order.push('kernel')
        return { vmhwm: 5, vmrss: 1 }
      },
    })
    order.length = 0

    memory.mark('x')

    expect(order).toEqual(['current', 'kernel'])
  })

  test('has no high water, and reads no kernel status, without a selected source', () => {
    const read = jest.fn(() => null)
    const { memory } = meter({ read })
    read.mockClear()

    expect(memory.source).toBeNull()
    expect(memory.mark('x').vmhwm).toBeNull()
    expect(read).not.toHaveBeenCalled()
  })

  test('tells the engine collection function from a replacement', () => {
    expect(meter({ gc: null }).memory.gcCapability()).toEqual({
      available: false,
      native: false,
    })
    expect(meter({ gc: () => {} }).memory.gcCapability()).toEqual({
      available: true,
      native: false,
    })
    expect(meter({ gc: Math.max }).memory.gcCapability()).toEqual({
      available: true,
      native: true,
    })
  })

  test('settles with two collections around a 100 ms pause and two turns before the reading', async () => {
    const events = []
    const { memory } = meter({
      gc: () => events.push('gc'),
      sleep: async (ms) => events.push(`sleep ${ms}`),
      settleTurn: async () => events.push('collection callbacks'),
      usage: () => {
        events.push('read')
        return { rss: 100, heapUsed: 10, external: 7, arrayBuffers: 3 }
      },
    })
    const settled = memory.settled()
    events.push('first await')
    const reading = await settled

    expect(events).toEqual([
      'gc',
      'sleep 100',
      'first await',
      'gc',
      'collection callbacks',
      'collection callbacks',
      'read',
    ])
    expect(reading).toMatchObject({ retained: 17, rss: 100 })
    expect(reading.gc.completedAt).toBe(reading.at)
    expect(reading.gc.admittedAt).toBeLessThan(reading.gc.completedAt)
  })

  test('counts the collections the engine reported while it settled', async () => {
    const owner = {}
    owner.memory = meter({ gc: () => (owner.memory.collections += 1) }).memory
    // Only the two scripted collections may be reported; a real engine collection is not ours.
    owner.memory.observer.disconnect()

    expect((await owner.memory.settled()).gc.observed).toBe(2)
  })

  test('refuses a settled reading without a collection function', async () => {
    const { memory } = meter({ gc: null })

    await expect(memory.settled()).rejects.toThrow('A settled reading needs node --expose-gc')
  })
})

describe('PendingSampler', () => {
  /** A sampler over a scripted clock and fake interval timers. */
  const sampler = () => {
    let tick = 0
    const intervals = new Map()
    let next = 0
    const memory = new ProcessMemory({
      read: () => null,
      gc: () => {},
      usage: () => ({ rss: ++tick * 10, heapUsed: 1, external: tick, arrayBuffers: 0 }),
      now: () => tick * 20,
    })
    const subject = new PendingSampler(memory, {
      set: (fn, ms) => {
        intervals.set(++next, { fn, ms })
        return next
      },
      clear: (handle) => intervals.delete(handle),
    })
    return { subject, intervals, memory }
  }

  test('starts one 20 ms interval for the first pending job and stops it with the last', () => {
    const { subject, intervals, memory } = sampler()

    subject.begin()
    subject.begin()
    expect([...intervals.values()].map((interval) => interval.ms)).toEqual([20])
    subject.end()
    expect(intervals.size).toBe(1)
    subject.end()
    expect(intervals.size).toBe(0)
    expect(subject.summary().windows).toBe(1)
    memory.stop()
  })

  test('reads at every submission, settlement and interval tick, and keeps a bounded summary', () => {
    const { subject, intervals, memory } = sampler()

    subject.begin()
    ;[...intervals.values()][0].fn()
    ;[...intervals.values()][0].fn()
    subject.end()

    expect(subject.summary()).toEqual({
      count: 4,
      windows: 1,
      pendingWindows: [{ begin: 20, end: 80, count: 4, rss: 40, external: 4 }],
      overflow: false,
      maxGapMs: 20,
      intervalMs: 20,
      peak: { rss: 40, heapUsed: 1, external: 4, arrayBuffers: 0 },
    })
    memory.stop()
  })

  test('describes each pending window from its submission to its settlement', () => {
    const { subject, intervals, memory } = sampler()

    const submitted = subject.begin()
    ;[...intervals.values()][0].fn()
    const settled = subject.end()
    subject.begin()
    subject.end()

    const [first, second] = subject.summary().pendingWindows
    expect(first).toEqual({ begin: submitted.at, end: settled.at, count: 3, rss: 30, external: 3 })
    expect(second).toEqual({ begin: 80, end: 100, count: 2, rss: 50, external: 5 })
    memory.stop()
  })

  test('reports more windows than it describes as an overflow instead of growing', () => {
    const { subject, memory } = sampler()

    for (let window = 0; window <= WINDOW_CAPACITY; window++) {
      subject.begin()
      subject.end()
    }

    const summary = subject.summary()
    expect(summary.windows).toBe(WINDOW_CAPACITY + 1)
    expect(summary.pendingWindows).toHaveLength(WINDOW_CAPACITY)
    expect(summary.overflow).toBe(true)
    memory.stop()
  })

  test('measures a gap only within one pending window', () => {
    const { subject, memory } = sampler()
    subject.begin()
    subject.end()
    subject.begin()
    subject.end()

    expect(subject.summary()).toMatchObject({ count: 4, windows: 2, maxGapMs: 20 })
    memory.stop()
  })

  test('releases its interval whatever is pending', () => {
    const { subject, intervals, memory } = sampler()
    subject.begin()

    subject.stop()

    expect(intervals.size).toBe(0)
    memory.stop()
  })

  test('takes a reading without a window when work settles and starts after a stop', () => {
    const { subject, intervals, memory } = sampler()
    subject.begin()
    subject.stop()

    subject.end()
    const reading = subject.begin()

    expect(reading).toMatchObject({ at: 60, rss: 30, external: 3 })
    expect(intervals.size).toBe(0)
    expect(subject.summary()).toMatchObject({ windows: 1, count: 3 })
    expect(subject.summary().pendingWindows).toEqual([
      { begin: 20, end: 20, count: 1, rss: 10, external: 1 },
    ])
    memory.stop()
  })
})

describe('BoundaryWitness', () => {
  /** @returns {{witness: BoundaryWitness, settle: () => void, closed: Promise<void>}} */
  const disconnect = () => {
    let tick = 0
    let settle
    const closed = new Promise((resolve) => (settle = resolve))
    return { witness: new BoundaryWitness(() => ++tick), settle, closed }
  }

  test('records an action taken in the resolution continuation before its first yield', async () => {
    const { witness, settle, closed } = disconnect()
    const done = closed.then(() => {
      witness.resolved()
      witness.call('connect')
    })
    settle()
    await done
    await Promise.resolve()

    expect(witness.summary()).toEqual([
      { action: 'disconnect', at: 1 },
      { action: 'connect', at: 2 },
      { action: 'yield', at: 3 },
    ])
  })

  test('records an action behind an await after the first yield', async () => {
    const { witness, settle, closed } = disconnect()
    const done = closed.then(async () => {
      witness.resolved()
      await Promise.resolve()
      witness.call('connect')
    })
    settle()
    await done

    expect(witness.summary().map((event) => event.action)).toEqual([
      'disconnect',
      'yield',
      'connect',
    ])
  })
})

describe('NativeJournal', () => {
  const stops = []
  /** A journal over one codec module whose `compress` is the given function. */
  const journalOf = (compress, options = {}) => {
    let tick = 0
    const memory = new ProcessMemory({
      read: () => null,
      gc: () => {},
      usage: () => ({ rss: 1, heapUsed: 1, external: 1, arrayBuffers: 1 }),
      now: () => ++tick,
    })
    stops.push(() => memory.stop())
    const sampler = new PendingSampler(memory, { set: () => 1, clear: () => {} })
    const module = { compress }
    const journal = new NativeJournal({ memory, sampler, ...options })
    journal.observe('gzip', module)
    return { journal, module, sampler }
  }

  afterEach(() => stops.splice(0).forEach((stop) => stop()))

  test('returns exactly what the codec returned and journals the job, input and result', async () => {
    const backing = new ArrayBuffer(4096)
    const input = Buffer.from(backing, 0, 2000)
    const output = Buffer.alloc(100)
    const job = { buffer: input }
    const { journal, module } = journalOf(async () => output)

    const result = module.compress(job)
    expect(journal.live.size).toBe(1)

    expect(await result).toBe(output)
    expect(journal.live.size).toBe(0)
    const [row] = journal.summary().rows
    expect(row).toMatchObject({
      id: 1,
      codec: 'gzip',
      inputBytes: 2000,
      inputBackingBytes: 4096,
      outcome: 'resolved',
      resultBytes: 100,
      resultBackingBytes: 100,
      distinctBackingBytes: 4196,
      retired: false,
    })
    expect(new Set([row.jobId, row.inputId, row.inputBackingId, row.resultId]).size).toBe(4)
    expect(row.submitted.at).toBeLessThan(row.settled.at)
  })

  test('counts a backing store shared by the input and the result once', async () => {
    const backing = new ArrayBuffer(4096)
    const { module, journal } = journalOf(async () => Buffer.from(backing, 2000, 100))

    await module.compress({ buffer: Buffer.from(backing, 0, 2000) })

    const [row] = journal.summary().rows
    expect(row.inputBackingId).toBe(row.resultBackingId)
    expect(row.distinctBackingBytes).toBe(4096)
  })

  test('issues the same identity to the same object and never keeps it alive', async () => {
    const { journal, module } = journalOf(async () => Buffer.alloc(1))
    const object = {}

    expect(journal.idOf(object)).toBe(journal.idOf(object))
    expect(journal.idOf({})).not.toBe(journal.idOf(object))
    expect(journal.idOf(null)).toBeNull()
    expect(journal.idOf(3)).toBeNull()

    const job = { buffer: Buffer.alloc(2000) }
    await module.compress(job)
    expect(journal.issued).toBeInstanceOf(WeakMap)
    const roots = []
    const visit = (value, depth) => {
      if (value === null || typeof value !== 'object' || value instanceof WeakMap || depth > 4)
        return
      roots.push(value)
      for (const child of value instanceof Set || value instanceof Map
        ? value
        : Object.values(value))
        visit(child, depth + 1)
    }
    visit({ rows: journal.rows, live: journal.live, summary: journal.summary() }, 0)
    for (const held of [object, job, job.buffer, job.buffer.buffer])
      expect(roots.includes(held)).toBe(false)
  })

  test('preserves a rejection and records the job as rejected', async () => {
    const failure = new Error('codec failed')
    const { module, journal } = journalOf(async () => {
      throw failure
    })

    await expect(module.compress({ buffer: Buffer.alloc(10) })).rejects.toBe(failure)
    await Promise.resolve()

    expect(journal.summary().rows[0]).toMatchObject({ outcome: 'rejected', resultBytes: 0 })
    expect(journal.live.size).toBe(0)
  })

  test('preserves a synchronous throw and settles the row', () => {
    const failure = new Error('sync')
    const { module, journal } = journalOf(() => {
      throw failure
    })

    expect(() => module.compress({ buffer: Buffer.alloc(10) })).toThrow(failure)

    expect(journal.summary().rows[0]).toMatchObject({ outcome: 'rejected' })
    expect(journal.live.size).toBe(0)
  })

  test('marks a job that settles in a later lifecycle as retired', async () => {
    let epoch = 0
    let release
    const { module, journal } = journalOf(() => new Promise((resolve) => (release = resolve)), {
      epoch: () => epoch,
    })
    const pending = module.compress({ buffer: Buffer.alloc(10) })
    epoch += 1
    release(Buffer.alloc(1))
    await pending

    expect(journal.summary().rows[0].retired).toBe(true)
  })

  test('tells the arm about each submission, after the job was started', async () => {
    const order = []
    const { module } = journalOf(
      async () => {
        order.push('compress')
        return Buffer.alloc(1)
      },
      { onSubmit: (row) => order.push(`submitted ${row.id}`) }
    )

    await module.compress({ buffer: Buffer.alloc(10) })

    expect(order).toEqual(['compress', 'submitted 1'])
  })

  test('keeps at most its capacity and reports the overflow, while the codec still runs', async () => {
    const { module, journal } = journalOf(async () => Buffer.alloc(1), { capacity: 1 })

    await module.compress({ buffer: Buffer.alloc(10) })
    const second = await module.compress({ buffer: Buffer.alloc(10) })

    expect(second).toEqual(Buffer.alloc(1))
    expect(journal.summary()).toMatchObject({ overflow: true, live: 0 })
    expect(journal.summary().rows).toHaveLength(1)
  })

  test('records the most jobs it saw pending at once', async () => {
    const releases = []
    const { module, journal } = journalOf(() => new Promise((resolve) => releases.push(resolve)))
    const first = module.compress({ buffer: Buffer.alloc(10) })
    const second = module.compress({ buffer: Buffer.alloc(10) })
    releases.forEach((release) => release(Buffer.alloc(1)))
    await Promise.all([first, second])

    expect(journal.summary().maxLive).toBe(2)
  })

  test('restores every wrapped module', () => {
    const original = async () => Buffer.alloc(1)
    const { module, journal } = journalOf(original)
    expect(module.compress).not.toBe(original)

    journal.restoreAll()

    expect(module.compress).toBe(original)
  })
})
