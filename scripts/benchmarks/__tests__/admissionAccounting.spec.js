const { spawnSync } = require('child_process')
const path = require('path')
const { SHAPES, WINDOW, measure } = require('../admissionAccounting')
const sendPathCounters = require('../sendPathCounters')

const SCRIPT = path.join(__dirname, '..', 'admissionAccounting.js')

const run = (...args) => spawnSync(process.execPath, args, { encoding: 'utf8', timeout: 120000 })

test('declares the eight shapes of the specification and the window [0.9, 3.0]', () => {
  expect(SHAPES.map(([name]) => name)).toEqual([
    'empty record',
    'small strings',
    'small Buffers',
    '1 KiB',
    '64 KiB',
    'many headers',
    'one record per partition',
    'Buffer views over their own slabs',
  ])
  expect(WINDOW).toEqual([0.9, 3.0])
  for (const [, records] of SHAPES) {
    expect(records(0)).toEqual(expect.any(Array))
    expect(records(1)).not.toBe(records(1))
  }
})

test('a shape whose sends are held is counted near what they retain, and its control retains next to nothing', () => {
  const done = run('--expose-gc', SCRIPT, '--shape', '64 KiB', '--target', '8388608')
  const lines = done.stdout
    .trim()
    .split('\n')
    .map((line) => JSON.parse(line))

  expect(lines).toHaveLength(2)
  const [shape, summary] = lines
  expect(shape).toMatchObject({ shape: '64 KiB', sends: expect.any(Number), within: true })
  expect(shape.counted).toBeGreaterThan(8388608 * 0.9)
  expect(shape.ratio).toBeGreaterThanOrEqual(0.9)
  expect(shape.ratio).toBeLessThanOrEqual(3.0)
  expect(shape.control).toBeLessThan(0.1)
  expect(summary).toEqual({ ok: true })
  expect(done.status).toBe(0)
})

test('refuses to run without --expose-gc, and refuses an unknown shape', () => {
  expect(run(SCRIPT).status).toBe(2)
  expect(run('--expose-gc', SCRIPT, '--shape', 'no such shape').status).toBe(2)
})

test('measure reports a shape outside the window as not within it', async () => {
  const gc = global.gc
  global.gc = () => {}
  try {
    const line = await measure('nothing held', () => [], 1)
    expect(line.within).toBe(false)
  } finally {
    global.gc = gc
  }
})

describe('the counts the send-path benchmarks print', () => {
  const missing = (path) =>
    Object.assign(new Error(`Cannot find module '${path}'`), { code: 'MODULE_NOT_FOUND' })

  test('counts every append, every admission, the deadline each append starts and each send the flush inventory tracks, from zero again after a reset', () => {
    jest.isolateModules(() => {
      const counters = require('../sendPathCounters')()
      const Isolated = require('../../../src/producer/state/accumulator')
      const send = (records) =>
        new Isolated({
          linger: 0,
          batchSize: 16384,
          partition: async () => [],
          sendPartitioned: async () => {},
        })
          .append({ acks: -1, topicMessages: [{ topic: 'topic-a', messages: records }] })
          .catch(() => {})

      send([{ value: 'v' }])
      send([{ value: 'w' }])
      expect(counters.read()).toEqual({
        appendCalls: 2,
        acquireCalls: 2,
        deadlines: 2,
        flushTracked: 2,
      })

      counters.reset()
      send([])
      expect(counters.read()).toEqual({
        appendCalls: 1,
        acquireCalls: 0,
        deadlines: 0,
        flushTracked: 0,
      })
    })
  })

  test('a module the tree does not have reads as null; any other failure to load it is thrown', () => {
    const { optional } = sendPathCounters
    const path = '../../src/producer/deadline'

    expect(
      optional(path, () => {
        throw missing(path)
      })
    ).toBeNull()
    expect(() =>
      optional(path, () => {
        throw missing('./something-else')
      })
    ).toThrow("Cannot find module './something-else'")
    expect(optional(path, () => ({ Deadline: 1 }))).toEqual({ Deadline: 1 })
  })

  test('the memory reading is null without --expose-gc, and the heap after a collection with it', () => {
    const { heapAfterGc } = sendPathCounters
    const gc = global.gc
    try {
      global.gc = undefined
      expect(heapAfterGc()).toBeNull()
      global.gc = jest.fn()
      expect(heapAfterGc()).toEqual(expect.any(Number))
      expect(global.gc).toHaveBeenCalledTimes(1)
    } finally {
      global.gc = gc
    }
  })
})
