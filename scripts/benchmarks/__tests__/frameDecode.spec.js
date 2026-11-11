const { SHAPES, parseArgs, chunksOf, timed, control } = require('../frameDecode')
const RequestQueue = require('../../../src/network/requestQueue')

const FRAME_HEADER = 8

describe('Benchmarks > frameDecode', () => {
  describe('parseArgs', () => {
    test('defaults to the 64-byte shape, timed', () => {
      expect(parseArgs([])).toEqual({ shape: '64b', control: false })
    })

    test.each(Object.keys(SHAPES))('selects the shape %s and the control', (shape) => {
      expect(parseArgs(['--shape', shape, '--control', '1'])).toEqual({ shape, control: true })
      expect(parseArgs(['--control', '0', '--shape', shape])).toEqual({ shape, control: false })
    })

    test.each([
      [['--shape', 'huge']],
      [['--shape']],
      [['--control', '2']],
      [['--frames', '3']],
      [['64b']],
    ])('%j is refused with the usage line', (argv) => {
      expect(() => parseArgs(argv)).toThrow(/^usage: node scripts\/benchmarks\/frameDecode\.js /)
    })
  })

  describe('the shapes', () => {
    const ids = [7, 8, 9]
    const frameBytes = (shape) => FRAME_HEADER + SHAPES[shape].body

    test('64b delivers one frame to each data event, size and correlation id first', () => {
      const chunks = chunksOf(SHAPES['64b'], ids)

      expect(chunks).toHaveLength(3)
      chunks.forEach((chunk, index) => {
        expect(chunk.length).toBe(frameBytes('64b'))
        expect(chunk.readInt32BE(0)).toBe(4 + 64)
        expect(chunk.readInt32BE(4)).toBe(ids[index])
      })
    })

    test('64k and 1m deliver frames in chunks of 16 KiB and a last short one', () => {
      for (const shape of ['64k', '1m']) {
        const chunks = chunksOf(SHAPES[shape], ids)
        const perFrame = Math.ceil(frameBytes(shape) / 16384)

        expect(chunks).toHaveLength(3 * perFrame)
        expect(chunks.slice(0, perFrame - 1).every(({ length }) => length === 16384)).toBe(true)
        expect(chunks.reduce((total, { length }) => total + length, 0)).toBe(3 * frameBytes(shape))
      }
    })

    test('coalesced puts 64 frames in a data event', () => {
      const frames = Array.from({ length: 128 }, (_, index) => index)
      const chunks = chunksOf(SHAPES.coalesced, frames)

      expect(chunks).toHaveLength(2)
      expect(chunks.every(({ length }) => length === 64 * frameBytes('coalesced'))).toBe(true)
    })

    test('byte-chunks delivers a frame a byte at a time', () => {
      const chunks = chunksOf(SHAPES['byte-chunks'], [1])

      expect(chunks).toHaveLength(frameBytes('byte-chunks'))
      expect(chunks.every(({ length }) => length === 1)).toBe(true)
      expect(Buffer.concat(chunks).readInt32BE(4)).toBe(1)
    })
  })

  describe('a run', () => {
    const originalGc = globalThis.gc
    beforeEach(() => {
      globalThis.gc = jest.fn()
    })
    afterEach(() => {
      globalThis.gc = originalGc
      jest.restoreAllMocks()
    })

    test('timed mode requires gc while the functional control does not', async () => {
      delete globalThis.gc
      await expect(timed('64b', 2)).rejects.toThrow(/gc/)
      await expect(control('64b', 2)).resolves.toMatchObject({ pass: true })
    })

    test('discards three warmups and collects before each of seven measured intervals', async () => {
      const events = []
      let intervalCalls = 2
      globalThis.gc = jest.fn(() => {
        events.push('gc')
        intervalCalls = 0
      })
      const clock = process.hrtime.bigint
      jest.spyOn(process.hrtime, 'bigint').mockImplementation(() => {
        if (intervalCalls++ < 2) events.push('time')
        return clock()
      })
      const result = await timed('64b', 2)
      expect(result.warmupRoundNsPerOp).toHaveLength(3)
      expect(result.roundNsPerOp).toHaveLength(7)
      expect(events).toEqual([
        ...Array.from({ length: 10 }, () => ['gc', 'time', 'time']).flat(),
        'gc',
        'gc',
      ])
      expect(result.nsPerFrame).toBe([...result.roundNsPerOp].sort((a, b) => a - b)[3])
    })
    test.each(Object.keys(SHAPES))(
      'times %s, and the control counts as many calls as frames fed',
      async (shape) => {
        const timedRun = await timed(shape, 2)
        const controlRun = await control(shape, 2)

        expect(timedRun).toMatchObject({ frames: 2, nsPerFrame: expect.any(Number) })
        expect(timedRun.nsPerFrame).toBeGreaterThan(0)
        expect(timedRun.roundNsPerOp).toHaveLength(7)
        expect(timedRun.roundNsPerOp.every((value) => value > 0)).toBe(true)
        expect(timedRun.nsPerFrame).toBe([...timedRun.roundNsPerOp].sort((a, b) => a - b)[3])
        expect(controlRun).toEqual({ control: true, shape, framesFed: 2, fulfilled: 2, pass: true })
      },
      60000
    )

    test('the control leaves the request queue as it found it', async () => {
      const original = RequestQueue.prototype.fulfillRequest

      await control('64b', 2)

      expect(RequestQueue.prototype.fulfillRequest).toBe(original)
    })

    test('a timed run carries no counter: it never wraps the request queue', async () => {
      const original = RequestQueue.prototype.fulfillRequest

      await timed('64b', 2)

      expect(RequestQueue.prototype.fulfillRequest).toBe(original)
    })
  })
})
