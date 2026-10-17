const { spawnSync } = require('child_process')
const { Gate, Schedule, interleavings, mulberry32 } = require('../schedule')

/** Lets every promise continuation that is already queued run. */
const flush = () => new Promise(setImmediate)

/** Longest a child may take to sample orderings, which it does in well under a second. */
const CHILD_TIMEOUT_MS = 5000

/**
 * Runs `interleavings` in a child process and returns what it listed. The sampler is a synchronous
 * loop: one that cannot finish would hold this process past any test timeout, but in a child it
 * ends as a failed assertion.
 * @param {string} steps JavaScript for the `steps` argument, evaluated in the child
 * @param {string} [options] JavaScript for the options argument
 * @returns {number[][]} the orderings
 */
const interleavingsInChild = (steps, options = '{}') => {
  const { status, stdout, error } = spawnSync(
    process.execPath,
    [
      '-e',
      `const { interleavings } = require(${JSON.stringify(require.resolve('../schedule'))})
       process.stdout.write(JSON.stringify(interleavings(${steps}, ${options})))`,
    ],
    { encoding: 'utf8', timeout: CHILD_TIMEOUT_MS, killSignal: 'SIGKILL', maxBuffer: 1 << 24 }
  )
  expect({ status, error: error?.code }).toEqual({ status: 0, error: undefined })

  return JSON.parse(stdout)
}

/**
 * Asserts that `orders` holds `count` different orderings, each using every index below `size`
 * once.
 * @param {number[][]} orders
 * @param {number} count
 * @param {number} size
 */
const expectDistinctOrderings = (orders, count, size) => {
  const indices = Array.from({ length: size }, (_, index) => index).join()

  expect(orders).toHaveLength(count)
  expect(new Set(orders.map((order) => order.join())).size).toBe(count)
  expect(orders.every((order) => [...order].sort((a, b) => a - b).join() === indices)).toBe(true)
}

describe('Gate', () => {
  test('resolves arrived() with the held call once it arrives', async () => {
    const schedule = new Schedule()
    const gate = schedule.hold({ api: 'Fetch' })
    const held = schedule.run({ api: 'Fetch', node: 1 }, () => 'default')

    expect(gate).toBeInstanceOf(Gate)
    await expect(gate.arrived()).resolves.toEqual({ api: 'Fetch', node: 1 })

    gate.release('late')
    await expect(held).resolves.toBe('late')
  })

  test('arrived() settles at once when the call came before anyone asked', async () => {
    const schedule = new Schedule()
    const gate = schedule.hold({ api: 'Fetch' })
    schedule.run({ api: 'Fetch' }, () => 'default')
    await flush()

    await expect(gate.arrived()).resolves.toEqual({ api: 'Fetch' })
  })

  test('arrived() stays pending until a call arrives', async () => {
    const schedule = new Schedule()
    const gate = schedule.hold({ api: 'Fetch' })
    const arrived = jest.fn()
    gate.arrived().then(arrived)

    schedule.run({ api: 'Metadata' }, () => 'default')
    await flush()

    expect(arrived).not.toHaveBeenCalled()
  })

  test('release() lets the held call return the value, and the call does not return before', async () => {
    const schedule = new Schedule()
    const gate = schedule.hold({ api: 'Fetch' })
    const returned = jest.fn()
    schedule.run({ api: 'Fetch' }, () => 'default').then(returned)
    await gate.arrived()
    await flush()

    expect(returned).not.toHaveBeenCalled()
    gate.release('answer')
    await flush()

    expect(returned).toHaveBeenCalledWith('answer')
  })

  test('fail() rejects the held call with the error', async () => {
    const schedule = new Schedule()
    const gate = schedule.hold({ api: 'Fetch' })
    const held = schedule.run({ api: 'Fetch' }, () => 'default')
    await gate.arrived()
    const error = new Error('connection reset')

    gate.fail(error)

    await expect(held).rejects.toBe(error)
  })

  test('release() before any call arrived throws and holds nothing back', async () => {
    const gate = new Schedule().hold({ api: 'Fetch' })

    expect(() => gate.release('too early')).toThrow('Gate released before a call arrived')
    expect(() => gate.fail(new Error('too early'))).toThrow('Gate released before a call arrived')
  })

  test('a gate can be released once', async () => {
    const schedule = new Schedule()
    const gate = schedule.hold({ api: 'Fetch' })
    schedule.run({ api: 'Fetch' }, () => 'default')
    await gate.arrived()
    gate.release('first')

    expect(() => gate.release('second')).toThrow('Gate already released')
    expect(() => gate.fail(new Error('second'))).toThrow('Gate already released')
  })

  test('holds one call in its lifetime: a second hold is refused, before and after the release', async () => {
    const gate = new Gate()
    const held = gate.hold({ api: 'A' })

    expect(() => gate.hold({ api: 'B' })).toThrow('Gate already holds a call')
    await expect(gate.arrived()).resolves.toEqual({ api: 'A' })
    gate.release('answer')
    await expect(held).resolves.toBe('answer')

    expect(() => gate.hold({ api: 'C' })).toThrow('Gate already holds a call')
  })

  test('holds one call only: the next matching call is not held', async () => {
    const schedule = new Schedule()
    const gate = schedule.hold({ api: 'Fetch' })
    schedule.run({ api: 'Fetch', n: 1 }, () => 'default')
    await gate.arrived()

    await expect(schedule.run({ api: 'Fetch', n: 2 }, () => 'default')).resolves.toBe('default')
  })
})

describe('Schedule', () => {
  test('logs every call in arrival order, held, answered and unmatched alike', async () => {
    const schedule = new Schedule()
    schedule.hold({ api: 'B' })
    schedule.once({ api: 'C' }, 'c')

    schedule.run({ api: 'A' }, () => 'a')
    schedule.run({ api: 'B' }, () => 'b')
    schedule.run({ api: 'C' }, () => 'c default')
    schedule.run({ api: 'A', again: true }, () => 'a')

    expect(schedule.calls.map(({ api }) => api)).toEqual(['A', 'B', 'C', 'A'])
    expect(schedule.calls[3]).toEqual({ api: 'A', again: true })
  })

  test('answers with the fallback, called with the call, when no rule picks it', async () => {
    const schedule = new Schedule()
    const fallback = jest.fn(({ topic }) => `answer for ${topic}`)

    await expect(schedule.run({ api: 'Fetch', topic: 't' }, fallback)).resolves.toBe('answer for t')

    expect(fallback).toHaveBeenCalledWith({ api: 'Fetch', topic: 't' })
  })

  test('a rejecting fallback rejects the call', async () => {
    const schedule = new Schedule()

    await expect(
      schedule.run({ api: 'Fetch' }, () => {
        throw new Error('no answer')
      })
    ).rejects.toThrow('no answer')
  })

  test.each([
    ['api', { api: 'Fetch' }, { api: 'Fetch', version: 1 }, { api: 'Produce', version: 1 }],
    ['version', { version: 18 }, { api: 'Fetch', version: 18 }, { api: 'Fetch', version: 17 }],
    ['node', { node: 2 }, { api: 'Fetch', node: 2 }, { api: 'Fetch', node: 3 }],
    ['topic', { topic: 't' }, { api: 'Fetch', topic: 't' }, { api: 'Fetch', topic: 'u' }],
    ['partition', { partition: 0 }, { api: 'Fetch', partition: 0 }, { api: 'Fetch', partition: 1 }],
  ])(
    'a selector on %s picks only the calls that carry that value',
    async (_name, selector, hit, miss) => {
      const schedule = new Schedule()
      schedule.once(selector, 'rule')

      await expect(schedule.run(miss, () => 'default')).resolves.toBe('default')
      await expect(schedule.run(hit, () => 'default')).resolves.toBe('rule')
    }
  )

  test('a selector with several fields needs every one of them', async () => {
    const schedule = new Schedule()
    schedule.once({ api: 'Fetch', node: 2, topic: 't' }, 'rule')

    await expect(
      schedule.run({ api: 'Fetch', node: 2, topic: 'u' }, () => 'default')
    ).resolves.toBe('default')
    await expect(
      schedule.run({ api: 'Fetch', node: 2, topic: 't' }, () => 'default')
    ).resolves.toBe('rule')
  })

  test('match() decides when the fields cannot', async () => {
    const schedule = new Schedule()
    schedule.once({ api: 'Fetch', match: ({ offsets }) => offsets.includes(50) }, 'rule')

    await expect(schedule.run({ api: 'Fetch', offsets: [1] }, () => 'default')).resolves.toBe(
      'default'
    )
    await expect(schedule.run({ api: 'Fetch', offsets: [1, 50] }, () => 'default')).resolves.toBe(
      'rule'
    )
  })

  test.each([
    ['a value', 'answer', 'answer'],
    ['a function of the call', (call) => `${call.api} answered`, 'Fetch answered'],
  ])('once() answers the next call with %s', async (_name, answer, expected) => {
    const schedule = new Schedule()
    schedule.once({ api: 'Fetch' }, answer)

    await expect(schedule.run({ api: 'Fetch' }, () => 'default')).resolves.toBe(expected)
  })

  test('once() rejects the call with an Error, given or returned', async () => {
    const schedule = new Schedule()
    const given = new Error('given')
    const returned = new Error('returned')
    schedule.once({ api: 'A' }, given)
    schedule.once({ api: 'B' }, () => returned)

    await expect(schedule.run({ api: 'A' }, () => 'default')).rejects.toBe(given)
    await expect(schedule.run({ api: 'B' }, () => 'default')).rejects.toBe(returned)
  })

  test('once() answers one call, then the fallback answers again', async () => {
    const schedule = new Schedule()
    schedule.once({ api: 'Fetch' }, 'rule')

    await expect(schedule.run({ api: 'Fetch' }, () => 'default')).resolves.toBe('rule')
    await expect(schedule.run({ api: 'Fetch' }, () => 'default')).resolves.toBe('default')
  })

  test('picks the oldest rule that matches, whether it holds or answers', async () => {
    const schedule = new Schedule()
    schedule.once({ api: 'Fetch' }, 'first')
    schedule.once({ api: 'Fetch', node: 1 }, 'second')

    await expect(schedule.run({ api: 'Fetch', node: 1 }, () => 'default')).resolves.toBe('first')
    await expect(schedule.run({ api: 'Fetch', node: 1 }, () => 'default')).resolves.toBe('second')
  })

  test('holds the matching calls in the order the holds were made', async () => {
    const schedule = new Schedule()
    const first = schedule.hold({ api: 'Fetch' })
    const second = schedule.hold({ api: 'Fetch' })

    schedule.run({ api: 'Fetch', n: 1 }, () => 'default')
    schedule.run({ api: 'Fetch', n: 2 }, () => 'default')

    await expect(first.arrived()).resolves.toMatchObject({ n: 1 })
    await expect(second.arrived()).resolves.toMatchObject({ n: 2 })
  })

  test('a held call does not stop other calls from being answered', async () => {
    const schedule = new Schedule()
    schedule.hold({ api: 'Fetch' })
    schedule.run({ api: 'Fetch' }, () => 'default')

    await expect(schedule.run({ api: 'Metadata' }, () => 'metadata')).resolves.toBe('metadata')
  })

  test('calls are released in the order the test chooses, whatever the order they arrived in', async () => {
    const schedule = new Schedule()
    const gates = [0, 1, 2].map(() => schedule.hold({ api: 'Fetch' }))
    const finished = []
    ;[0, 1, 2].forEach((n) =>
      schedule.run({ api: 'Fetch', n }, () => 'default').then(() => finished.push(n))
    )
    await Promise.all(gates.map((gate) => gate.arrived()))

    for (const n of [2, 0, 1]) {
      gates[n].release()
      await flush()
    }

    expect(schedule.calls.map(({ n }) => n)).toEqual([0, 1, 2])
    expect(finished).toEqual([2, 0, 1])
  })
})

describe('interleavings', () => {
  test('lists every ordering of three steps, lexicographic, each once', () => {
    expect(interleavings(['a', 'b', 'c'])).toEqual([
      [0, 1, 2],
      [0, 2, 1],
      [1, 0, 2],
      [1, 2, 0],
      [2, 0, 1],
      [2, 1, 0],
    ])
  })

  test('lists exactly 720 distinct orderings of six steps', () => {
    const orders = interleavings([1, 2, 3, 4, 5, 6])

    expect(orders).toHaveLength(720)
    expect(new Set(orders.map((order) => order.join(','))).size).toBe(720)
    expect(orders.every((order) => [...order].sort().join('') === '012345')).toBe(true)
    expect(orders[0]).toEqual([0, 1, 2, 3, 4, 5])
    expect(orders[719]).toEqual([5, 4, 3, 2, 1, 0])
  })

  test('has one ordering of no step and of one step', () => {
    expect(interleavings([])).toEqual([[]])
    expect(interleavings(['only'])).toEqual([[0]])
  })

  test('limit caps the list', () => {
    expect(interleavings([1, 2, 3, 4], { limit: 5 })).toHaveLength(5)
  })

  test('above six steps returns a sample of `limit` distinct orderings', () => {
    expectDistinctOrderings(interleavingsInChild('new Array(8).fill(0)', '{ limit: 100 }'), 100, 8)
  })

  test('above six steps a limit of one returns one ordering, without a search for more', () => {
    expectDistinctOrderings(interleavings(new Array(7).fill(0), { limit: 1 }), 1, 7)
  })

  test('the same seed gives the same sample and another seed another', () => {
    const sample = (seed) =>
      interleavingsInChild('new Array(7).fill(0)', `{ seed: ${seed}, limit: 50 }`)

    expect(sample(7)).toEqual(sample(7))
    expect(sample(7)).not.toEqual(sample(8))
  })

  test('a limit above the number of orderings returns all of them and stops', () => {
    expectDistinctOrderings(
      interleavingsInChild('new Array(7).fill(0)', '{ limit: 6000 }'),
      5040,
      7
    )
  })

  describe('with steps that have holes, which count by their length', () => {
    test.each([
      ['fully sparse', new Array(3), 3, 6],
      ['partially sparse', Object.assign(new Array(5), { 1: 'b', 3: 'd' }), 5, 120],
      ['fully sparse, at six steps', new Array(6), 6, 720],
    ])('lists every ordering of a %s array', (_name, steps, size, count) => {
      expectDistinctOrderings(interleavings(steps), count, size)
    })

    test.each([
      ['fully sparse', 'new Array(7)', 7],
      ['partially sparse', "Object.assign(new Array(8), { 1: 'b', 6: 'g' })", 8],
    ])('samples `limit` distinct orderings of a %s array', (_name, steps, size) => {
      expectDistinctOrderings(interleavingsInChild(steps), 720, size)
      expectDistinctOrderings(interleavingsInChild(steps, '{ limit: 5 }'), 5, size)
    })
  })
})

describe('mulberry32', () => {
  test('repeats a sequence from its seed', () => {
    const [a, b] = [mulberry32(42), mulberry32(42)]

    expect([a(), a(), a()]).toEqual([b(), b(), b()])
  })

  test('differs between seeds and stays in [0, 1)', () => {
    const [a, b] = [mulberry32(1), mulberry32(2)]
    const values = Array.from({ length: 1000 }, () => a())

    expect(values.every((value) => value >= 0 && value < 1)).toBe(true)
    expect(values[0]).not.toBe(b())
  })
})
