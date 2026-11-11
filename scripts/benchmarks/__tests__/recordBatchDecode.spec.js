const path = require('path')
const { spawnSync } = require('child_process')

const SCRIPT = path.join(__dirname, '..', 'recordBatchDecode.js')

/** Runs the benchmark for one arm and returns its one JSON line. */
const measure = (arm) => {
  const run = spawnSync(process.execPath, [SCRIPT, '--iterations', '100', '--arm', arm], {
    encoding: 'utf8',
  })

  expect(run.status).toBe(0)
  const result = JSON.parse(run.stdout)
  if (result.available) {
    expect(result.roundNsPerOp).toHaveLength(5)
    expect(result.roundNsPerOp.every((value) => value > 0)).toBe(true)
    expect(result.nsPerOp).toBe([...result.roundNsPerOp].sort((a, b) => a - b)[2])
  }
  return result
}

describe('Benchmarks > recordBatchDecode', () => {
  test('the uncompressed arm decodes the committed fixture and calls no codec', () => {
    expect(measure('uncompressed')).toMatchObject({
      available: 1,
      records: 3,
      decompressCalls: 0,
      iterations: 100,
    })
  })

  test.each(['gzip', 'snappy'])(
    'the %s arm decodes 20 records and decompresses once per decode',
    (arm) => {
      const result = measure(arm)

      expect(result).toMatchObject({ available: 1, records: 20, decompressCalls: 10 })
      expect(result.nsPerOp).toBeGreaterThan(0)
    }
  )

  test('an arm the tree has no codec for says so instead of failing', () => {
    expect(measure('no-such-codec')).toEqual({ available: 0 })
  })
})
