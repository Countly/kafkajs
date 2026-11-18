const path = require('path')
const { spawnSync } = require('child_process')

const SCRIPT = path.join(__dirname, '..', 'recordBatchDecode.js')
const { checksumOf } = require('../recordBatchDecode')

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

  test('the checksum mode prints a decoded-content checksum per arm that changes with any decoded byte', () => {
    const run = spawnSync(
      process.execPath,
      [SCRIPT, '--checksum', '--arms', 'uncompressed,gzip,snappy'],
      { encoding: 'utf8' }
    )
    expect(run.status).toBe(0)
    const { checksums } = JSON.parse(run.stdout)
    expect(Object.keys(checksums)).toEqual(['uncompressed', 'gzip', 'snappy'])
    expect(checksums.uncompressed).toMatchObject({ available: 1, records: 3 })
    expect(checksums.gzip).toMatchObject({ available: 1, records: 20 })
    expect(checksums.gzip.sha256).toBe(checksums.snappy.sha256)
    expect(checksums.uncompressed.sha256).toMatch(/^[0-9a-f]{64}$/)

    const decoded = (value, key = Buffer.from('k'), offset = '7') => ({
      responses: [{ partitions: [{ partition: 0, messages: [{ offset, key, value }] }] }],
    })
    const base = checksumOf(decoded(Buffer.from('value')))
    expect(base.records).toBe(1)
    for (const changed of [
      decoded(Buffer.from('valuf')),
      decoded(Buffer.from('value'), Buffer.from('j')),
      decoded(Buffer.from('value'), Buffer.from('k'), '8'),
      decoded(Buffer.from('value'), null),
    ]) {
      expect(checksumOf(changed)).toEqual({
        records: 1,
        sha256: expect.not.stringMatching(base.sha256),
      })
    }
  })
})
