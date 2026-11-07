const fs = require('fs')
const {
  PEAK_SOURCE,
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
    })
    const settled = memory.settled()
    events.push('first await')
    const reading = await settled

    expect(events).toEqual(['gc', 'sleep 100', 'first await', 'gc'])
    expect(reading).toMatchObject({ retained: 17, rss: 100 })
    expect(reading.gc.completedAt).toBe(reading.at)
    expect(reading.gc.admittedAt).toBeLessThan(reading.gc.completedAt)
  })

  test('counts the collections the engine reported while it settled', async () => {
    const owner = {}
    owner.memory = meter({ gc: () => (owner.memory.collections += 1) }).memory

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
      maxGapMs: 20,
      intervalMs: 20,
      peak: { rss: 40, heapUsed: 1, external: 4, arrayBuffers: 0 },
    })
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

  test('issues the same identity to the same object and never keeps it alive', () => {
    const { journal } = journalOf(async () => Buffer.alloc(1))
    const object = {}

    expect(journal.idOf(object)).toBe(journal.idOf(object))
    expect(journal.idOf({})).not.toBe(journal.idOf(object))
    expect(journal.idOf(null)).toBeNull()
    expect(journal.idOf(3)).toBeNull()
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
