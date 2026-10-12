const v8 = require('v8')
const vm = require('vm')
const Fifo = require('./fifo')

const fromArray = (items) => {
  const fifo = new Fifo()
  items.forEach((item) => fifo.push(item))
  return fifo
}

describe('Fifo', () => {
  it('starts empty', () => {
    const fifo = new Fifo()

    expect(fifo.size).toBe(0)
    expect(fifo.peek()).toBeUndefined()
    expect(fifo.shift()).toBeUndefined()
    expect([...fifo]).toStrictEqual([])
  })

  it('returns items in the order they were pushed', () => {
    const fifo = fromArray(['a', 'b', 'c'])

    expect(fifo.size).toBe(3)
    expect(fifo.peek()).toBe('a')
    expect(fifo.shift()).toBe('a')
    expect(fifo.shift()).toBe('b')
    expect(fifo.size).toBe(1)
    fifo.push('d')
    expect(fifo.shift()).toBe('c')
    expect(fifo.shift()).toBe('d')
    expect(fifo.shift()).toBeUndefined()
    expect(fifo.size).toBe(0)
  })

  it('keeps the order across compactions while it is pushed to and shifted from', () => {
    const fifo = new Fifo()
    const expected = []
    const taken = []
    let next = 0
    for (let round = 0; round < 200; round += 1) {
      for (let i = 0; i < 7; i += 1) {
        fifo.push(next)
        expected.push(next)
        next += 1
      }
      for (let i = 0; i < 5; i += 1) {
        taken.push(fifo.shift())
      }
    }
    while (fifo.size > 0) {
      taken.push(fifo.shift())
    }

    expect(taken).toEqual(expected)
  })

  it('reads and iterates only the live items after the head has moved', () => {
    const fifo = fromArray([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
    fifo.shift()
    fifo.shift()
    fifo.shift()

    expect([...fifo]).toStrictEqual([3, 4, 5, 6, 7, 8, 9])
    expect(fifo.at(0)).toBe(3)
    expect(fifo.at(6)).toBe(9)
    expect(fifo.at(7)).toBeUndefined()
    expect(fifo.at(-1)).toBeUndefined()
  })

  describe('#insertAt', () => {
    it('inserts at the front, in the middle and at the back, past a moved head', () => {
      const fifo = fromArray(['a', 'b', 'c', 'd'])
      fifo.shift()

      fifo.insertAt(0, 'front')
      fifo.insertAt(2, 'middle')
      fifo.insertAt(fifo.size, 'back')

      expect([...fifo]).toStrictEqual(['front', 'b', 'middle', 'c', 'd', 'back'])
    })

    it('clamps an index outside the queue, past a moved head as well', () => {
      const fifo = fromArray(['a', 'b', 'c', 'd'])
      fifo.shift()

      fifo.insertAt(-5, 'first')
      fifo.insertAt(99, 'last')

      expect([...fifo]).toStrictEqual(['first', 'b', 'c', 'd', 'last'])
    })

    it('inserts into an empty queue', () => {
      const fifo = new Fifo()

      fifo.insertAt(0, 'only')

      expect([...fifo]).toStrictEqual(['only'])
    })
  })

  describe('#remove', () => {
    it('removes the first occurrence by identity and reports it', () => {
      const a = {}
      const b = {}
      const fifo = fromArray([a, b, a])

      expect(fifo.remove({})).toBe(false)
      expect(fifo.remove(a)).toBe(true)
      expect([...fifo]).toStrictEqual([b, a])
      expect(fifo.size).toBe(2)
    })

    it('does not see items that were shifted out', () => {
      const fifo = fromArray(['a', 'b', 'c'])
      fifo.shift()

      expect(fifo.remove('a')).toBe(false)
      expect(fifo.remove('c')).toBe(true)
      expect([...fifo]).toStrictEqual(['b'])
    })

    it('leaves a usable empty queue after removing the last item', () => {
      const fifo = fromArray(['a', 'b'])
      fifo.shift()

      expect(fifo.remove('b')).toBe(true)
      expect(fifo.size).toBe(0)
      fifo.push('c')
      expect(fifo.shift()).toBe('c')
    })
  })

  it('is empty and reusable after clear()', () => {
    const fifo = fromArray([1, 2, 3])
    fifo.shift()

    fifo.clear()

    expect(fifo.size).toBe(0)
    expect([...fifo]).toStrictEqual([])
    fifo.push(4)
    expect([...fifo]).toStrictEqual([4])
  })

  describe('compaction', () => {
    let slice

    afterEach(() => {
      slice.mockRestore()
    })

    it('rebuilds the backing array about once per queue length at a steady size', () => {
      const depth = 100
      const pairs = 200000
      const fifo = fromArray(Array.from({ length: depth }, (_, i) => i))
      slice = jest.spyOn(Array.prototype, 'slice')

      for (let i = 0; i < pairs; i += 1) {
        fifo.push(i)
        fifo.shift()
      }
      const calls = slice.mock.calls.length

      // a queue that never compacts makes no call; one that compacts on every shift makes `pairs`
      expect(calls).toBeGreaterThanOrEqual(pairs / (4 * depth))
      expect(calls).toBeLessThanOrEqual(pairs / depth)
      expect(fifo.size).toBe(depth)
    })

    it('does not rebuild a queue that drains completely', () => {
      const fifo = new Fifo()
      slice = jest.spyOn(Array.prototype, 'slice')

      for (let i = 0; i < 1000; i += 1) {
        fifo.push(i)
        fifo.shift()
      }

      expect(slice).not.toHaveBeenCalled()
    })
  })

  it('does not keep a shifted item alive', async () => {
    v8.setFlagsFromString('--expose-gc')
    const gc = vm.runInNewContext('gc')
    const fifo = new Fifo()
    const refs = []
    for (let i = 0; i < 20; i += 1) {
      const item = { payload: new Array(1000).fill(i) }
      refs.push(new WeakRef(item))
      fifo.push(item)
    }
    // 20 items, 8 shifted: below the compaction point, so the backing array is not rebuilt
    for (let i = 0; i < 8; i += 1) {
      fifo.shift()
    }

    await new Promise((resolve) => setImmediate(resolve))
    gc()

    expect(refs.slice(0, 8).map((ref) => ref.deref())).toEqual(new Array(8).fill(undefined))
    expect(refs.slice(8).every((ref) => ref.deref() !== undefined)).toBe(true)
  })
})
