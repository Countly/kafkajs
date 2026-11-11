jest.mock('../../../src/protocol/requests/fetch/v11/response', () => ({ decode: jest.fn() }))
const { decode } = require('../../../src/protocol/requests/fetch/v11/response')
const { run: runBatch } = require('../recordBatchDecode')
const { run: runArray } = require('../arrayDecode')

describe('Decoder round work', () => {
  it('repeats batch and array operations and divides elapsed nanoseconds by the multiplied operation count', async () => {
    const clock = jest.spyOn(process.hrtime, 'bigint')
    try {
      for (const multiplier of [1, 2, 16]) {
        let tick = 0n
        clock.mockImplementation(() => {
          tick += 1600n
          return tick
        })
        decode.mockClear()
        const batch = await runBatch(Buffer.alloc(0), {}, 5, multiplier)
        expect(decode).toHaveBeenCalledTimes(2000 * multiplier + 5 * multiplier)
        expect(batch.nsPerOp).toBe(1600 / multiplier)
        expect(batch.roundNsPerOp).toEqual(Array(5).fill(1600 / multiplier))
        expect(batch.opsPerSecond).toBe(1e9 / batch.nsPerOp)
        const decodeArray = jest.fn()
        const array = await runArray(Buffer.alloc(0), { decode: decodeArray }, 5, multiplier)
        expect(decodeArray).toHaveBeenCalledTimes(50 * multiplier + 5 * multiplier)
        expect(array.nsPerOp).toBe(1600 / multiplier)
        expect(array.roundNsPerOp).toEqual(Array(5).fill(1600 / multiplier))
      }
    } finally {
      clock.mockRestore()
    }
  })
})
