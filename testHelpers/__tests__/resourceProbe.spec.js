const { EventEmitter } = require('events')
const v8 = require('v8')
const vm = require('vm')
const { createResourceProbe } = require('../resourceProbe')
const { createFakeSocketFactory, socketCounts } = require('../fakeSocket')

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
