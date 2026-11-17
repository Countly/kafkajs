const codec = require('../../../src/protocol/message/compression/snappy')
const { expansion, failuresOf } = require('../snappyCodec')

test('maximal-expansion stream has an independently known output', async () => {
  const decoded = await codec.decompress(expansion(1048576))
  expect(decoded).toEqual(Buffer.alloc(1048576, 97))
})

test('the benchmark refuses wrong byte counts and each declared gate breach', () => {
  const base = {
    checkedBytes: 1,
    outputBytes: 1,
    delayMs: 19.9,
    rssGrowth: 10,
    memoryBound: 10,
    decodeMiBs: 80,
    encodeMiBs: 30,
  }
  expect(failuresOf({ mixed: base })).toEqual([])
  for (const change of [
    { checkedBytes: 0 },
    { delayMs: 20 },
    { rssGrowth: 11 },
    { decodeMiBs: 79.9 },
    { encodeMiBs: 29.9 },
  ]) {
    expect(failuresOf({ mixed: { ...base, ...change } })).toHaveLength(1)
  }
})

describe('Snappy report session', () => {
  const fs = require('node:fs')
  const os = require('node:os')
  const path = require('node:path')
  const { ARMS, SESSION, snappySession } = require('../snappyCodec')

  const MiB = 1048576
  const SIZES = {
    mixed: 3 * MiB,
    json: 232000,
    random: MiB,
    expansion: 64 * MiB,
    encode64: 64 * MiB,
    mixedLarge: 32 * MiB,
    tinyBlocks: 1000000,
    tinyTokens: MiB,
  }
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'snappy-session-'))
  afterAll(() => fs.rmSync(dir, { recursive: true, force: true }))
  let files = 0
  const rawFile = () => path.join(dir, `fork-run-${++files}.jsonl`)
  const lines = (file) =>
    fs
      .readFileSync(file, 'utf8')
      .trim()
      .split('\n')
      .map((line) => JSON.parse(line))

  /** One `--arm` child's line, as `runArm` prints it. */
  const armLine = (name, decodeMiBs = 400) => ({
    decodeMiBs: name === 'mixed' ? decodeMiBs : 500,
    encodeMiBs: ['mixed', 'json', 'random', 'encode64'].includes(name) ? 190 : 0,
    delayMs: 3,
    rssGrowth: SIZES[name],
    memoryBound: 4 * SIZES[name],
    inputBytes: 1000,
    outputBytes: SIZES[name],
    checkedBytes: SIZES[name],
  })
  /** Scripted `--arm` children; `result(name, call)` may replace a child's result. */
  const children = (result = () => undefined, tick = () => {}) => {
    const calls = []
    const spawn = (command, args, options) => {
      calls.push({ args, timeout: options.timeout })
      tick()
      const name = args[args.indexOf('--arm') + 1]
      return (
        result(name, calls.length) ?? {
          status: 0,
          stdout: `${JSON.stringify(armLine(name))}\n`,
          stderr: '',
        }
      )
    }
    return { spawn, calls }
  }
  const owned = { BENCH_SNAPPY_ROWS: '1' }

  test('a Snappy session runs a warm-up and eight measured observations of the codec and of its A/A copy, keeping every child attempt', () => {
    const raw = rawFile()
    const { spawn, calls } = children()
    const record = snappySession({ raw, spawn, env: owned })

    expect(record).toMatchObject({
      schema: 1,
      session: 'snappy',
      status: 'accepted',
      runs: 8,
      observations: 18,
      missed: 0,
      void: false,
    })
    expect(SESSION.runs).toBe(8)
    expect(Object.keys(record.arms).sort()).toEqual(['snappy', 'snappy~AA'])
    for (const label of ['snappy', 'snappy~AA']) {
      expect(record.arms[label].decodeMiBs).toEqual({ median: 400, min: 400, max: 400, n: 8 })
      expect(record.arms[label].checkedBytes.median).toBe(174247040)
      expect(record.arms[label].readbacks).toEqual({ median: 7, min: 7, max: 7, n: 8 })
    }
    expect(record.control.percent).toEqual({ decodeMiBs: 0, encodeMiBs: 0 })
    expect(calls).toHaveLength(18 * ARMS.length)
    expect(calls.slice(0, 8).map(({ args }) => args.slice(2))).toEqual(
      ARMS.map((name) => ['--arm', name])
    )

    const attempts = lines(raw)
    expect(attempts).toHaveLength(144)
    expect(attempts[13]).toMatchObject({
      attempt: 14,
      label: 'snappy~AA',
      arm: ARMS[5],
      status: 0,
      stdout: `${JSON.stringify(armLine(ARMS[5]))}\n`,
    })
    const observations = lines(`${raw}.observations.jsonl`)
    expect(observations.map(({ label, warmup }) => [label, warmup]).slice(0, 4)).toEqual([
      ['snappy', true],
      ['snappy~AA', true],
      ['snappy~AA', false],
      ['snappy', false],
    ])
    expect(observations[2].result.arms.expansion).toEqual(armLine('expansion'))
    expect(observations[2].result).toMatchObject({ decodeMiBs: 400, readbacks: 7, failures: 0 })
  })

  test('a Snappy session whose A/A copy differs by more than three percent is void', () => {
    const fast = (name, call) => {
      const observation = Math.floor((call - 1) / ARMS.length)
      const label = (observation % 2) + Math.floor(observation / 2)
      if (name !== 'mixed' || label % 2 === 0) return undefined
      return { status: 0, stdout: `${JSON.stringify(armLine(name, 440))}\n`, stderr: '' }
    }
    const record = snappySession({ raw: rawFile(), spawn: children(fast).spawn, env: owned })

    expect(record.status).toBe('void')
    expect(record.arms['snappy~AA'].decodeMiBs.median).toBe(440)
    expect(record.control.percent.decodeMiBs).toBeCloseTo(10, 10)
  })

  test('a failed or unreadable Snappy child ends the session in an error and keeps its attempt', () => {
    for (const [result, reason] of [
      [{ status: 1, stdout: '', stderr: 'RangeError: boom\n' }, 'exit 1: RangeError: boom'],
      [{ status: 0, stdout: 'not json\n', stderr: '' }, 'exit 0: no result line'],
      [{ status: null, signal: 'SIGTERM', error: new Error('spawnSync ETIMEDOUT') }, 'ETIMEDOUT'],
    ]) {
      const raw = rawFile()
      const { spawn, calls } = children((name, call) => (call === 5 ? result : undefined))
      const record = snappySession({ raw, spawn, env: owned })

      expect(record.status).toBe('error')
      expect(record.reason).toContain(`snappy ${ARMS[4]}`)
      expect(record.reason).toContain(reason)
      expect(calls).toHaveLength(5)
      expect(lines(raw).at(-1)).toMatchObject({ attempt: 5, arm: ARMS[4] })
    }
  })

  test('a Snappy session gives each child only the time left in its budget and stops when none is left', () => {
    let now = 0
    const { spawn, calls } = children(undefined, () => {
      now += 7 * 60000
    })
    const record = snappySession({ raw: rawFile(), spawn, env: owned, now: () => now })

    expect(record.status).toBe('error')
    expect(record.reason).toBe(
      `the session budget of ${SESSION.budgetMs} ms leaves no time for snappy~AA ${ARMS[7]}`
    )
    expect(SESSION.budgetMs).toBe(100 * 60000)
    expect(calls.map(({ timeout }) => timeout)).toEqual([
      ...Array(13).fill(600000),
      SESSION.budgetMs - 13 * 7 * 60000,
      SESSION.budgetMs - 14 * 7 * 60000,
    ])
  })

  test('a Snappy session runs only in the workflow job that owns it', () => {
    const { spawn, calls } = children()
    expect(snappySession({ raw: rawFile(), spawn, env: {} })).toEqual({
      schema: 1,
      session: 'snappy',
      status: 'omitted',
      reason: 'runs in its own job of the benchmark workflow (BENCH_SNAPPY_ROWS=1)',
    })
    expect(calls).toEqual([])
  })
})
