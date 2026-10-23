const path = require('path')
const { spawnSync } = require('child_process')
const { parseArgs, run, planOf } = require('../produceFrameSize')

const ROOT = path.join(__dirname, '../../..')
const CAP = 1048576

describe('Benchmarks > produceFrameSize', () => {
  test('flags default to the 5 MiB corpus under the default maxRequestSize, on this tree', () => {
    expect(parseArgs([])).toEqual({ arm: 'corpus', maxRequestSize: CAP, undercount: 0, tree: ROOT })
    expect(parseArgs(['--arm', 'fanout', '--undercount', '0', '--tree', '.'])).toEqual({
      arm: 'fanout',
      maxRequestSize: CAP,
      undercount: 0,
      tree: path.resolve('.'),
    })
  })

  test.each([
    [['--arm', 'huge'], '--arm is one of corpus, fanout, gzip, incompressible'],
    [['--max-request-size', '0'], '--max-request-size needs an integer from 1 to 2147483647'],
    [['--undercount', '-1'], '--undercount needs an integer from 0 to 2147483647'],
    [
      ['--max-request-size', '2147483648'],
      '--max-request-size needs an integer from 1 to 2147483647',
    ],
    [['--pace', '1'], 'unknown or incomplete flag --pace'],
  ])('%j is refused', (argv, message) => {
    expect(() => parseArgs(argv)).toThrow(message)
  })

  test('the plan of a request counts its framing, each topic, and each batch with its codec’s bound', () => {
    const request = {
      compression: 0,
      topicData: [{ topic: 'frame-size', partitions: [{ messages: [{ value: 'abc' }] }] }],
    }

    expect(planOf(request, 0)).toBe(32 + 16 + (32 + 10) + 16 + 61 + 27)
    expect(planOf(request, 20)).toBe(planOf(request, 0) - 20)
  })

  test.each(['corpus', 'fanout', 'incompressible'])(
    'the %s arm writes no frame above maxRequestSize, and every frame within the plan',
    async (arm) => {
      const result = await run(parseArgs(['--arm', arm]))

      expect(result.frames).toBeGreaterThan(result.sends)
      expect(result.largestFrameBytes).toBeLessThanOrEqual(CAP)
      expect(result.largestFrameBytes).toBeGreaterThan(0.9 * CAP)
      expect(result.attempts).toBe(result.frames)
      expect(result.modelOver).toBe(0)
    }
  )

  test('the gzip arm is planned on the raw records, so a compressible send is still cut', async () => {
    const result = await run(parseArgs(['--arm', 'gzip']))

    expect(result.framesPerSend).toBe(6)
    expect(result.largestFrameBytes).toBeLessThan(0.01 * CAP)
  })

  test('an undercounting plan is caught by the real frames', async () => {
    const result = await run(parseArgs(['--undercount', '200']))

    expect(result.modelOver).toBe(result.frames)
    expect(result.largestFrameBytes).toBeLessThanOrEqual(CAP)
  })

  test('without the cap the corpus goes out in frames far above the default maxRequestSize', async () => {
    const result = await run(parseArgs(['--max-request-size', '2147483647']))

    expect(result.framesPerSend).toBe(1)
    expect(result.largestFrameBytes).toBeGreaterThan(5 * CAP)
  })

  test('the command line prints one JSON line, and exits 2 on a bad argument', () => {
    const script = path.join(__dirname, '../produceFrameSize.js')
    const good = spawnSync(process.execPath, [script, '--arm', 'fanout'], { encoding: 'utf8' })
    const bad = spawnSync(process.execPath, [script, '--arm', 'huge'], { encoding: 'utf8' })

    expect(good.status).toBe(0)
    expect(JSON.parse(good.stdout)).toMatchObject({ arm: 'fanout', modelOver: 0 })
    expect(bad.status).toBe(2)
    expect(bad.stderr).toContain('--arm is one of')
  })
})
