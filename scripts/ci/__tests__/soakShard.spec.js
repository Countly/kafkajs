const fs = require('fs')
const os = require('os')
const path = require('path')
const { spawnSync } = require('child_process')

describe('single-spec case sharding', () => {
  test('three real Jest runs execute every case once, with hooks and explicit skips preserved', () => {
    const rootDir = path.resolve(__dirname, '../../..')
    const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'soak-shards-'))
    try {
      const executions = []
      for (const shard of [1, 2, 3]) {
        const log = path.join(temp, `${shard}.log`)
        const result = spawnSync(
          process.execPath,
          [
            require.resolve('jest/bin/jest'),
            '--ci',
            '--runInBand',
            `--shard=${shard}/3`,
            '--config',
            JSON.stringify({
              rootDir,
              testMatch: ['**/scripts/ci/__tests__/fixtures/soakShard.js'],
              testEnvironment: '<rootDir>/scripts/ci/soakEnvironment.js',
              testSequencer: '<rootDir>/scripts/ci/soakSequencer.js',
            }),
          ],
          {
            encoding: 'utf8',
            timeout: 30000,
            env: { ...process.env, SOAK_SHARD_LOG: log },
          }
        )
        expect({ status: result.status, stderr: result.stderr }).toEqual({
          status: 0,
          stderr: expect.any(String),
        })
        const lines = fs.readFileSync(log, 'utf8').trim().split('\n')
        const expected = [
          [0, 3, 6],
          [1, 4],
          [2, 5],
        ][shard - 1]
        expect(lines.filter((line) => line !== 'afterEach').map(Number)).toEqual(expected)
        expect(lines.filter((line) => line === 'afterEach')).toHaveLength(expected.length)
        executions.push(...lines.filter((line) => line !== 'afterEach').map(Number))
      }
      expect(executions.sort((a, b) => a - b)).toEqual([0, 1, 2, 3, 4, 5, 6])
    } finally {
      fs.rmSync(temp, { recursive: true, force: true })
    }
  }, 120000)
})
