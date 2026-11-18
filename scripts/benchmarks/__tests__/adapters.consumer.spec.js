const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const crypto = require('node:crypto')
const session = require('../adapters/consumerSession')
const fetchSessions = require('../adapters/fetchSessions')
const { runArm, SessionMeasurement } = require('../fetchSessions')
const { PartitionCoverage, verifyCoverage } = require('../consumeCoverage')
const {
  ROOT,
  FULL_WINDOW,
  flag,
  output,
  fakeRun,
  fakeChild,
  consumerFixture,
} = require('./fixtures/consumerRows')

const { ROWS, HEAD, manifestEntries } = session
const NODES = [
  { nodeId: 1, host: 'localhost', port: 9092 },
  { nodeId: 2, host: 'localhost', port: 9095 },
  { nodeId: 3, host: 'localhost', port: 9098 },
]

let tmp
let fixture
beforeEach(() => {
  tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'consumer-rows-'))
  fixture = consumerFixture(tmp)
})
afterEach(() => {
  jest.useRealTimers()
  fs.rmSync(tmp, { recursive: true, force: true })
})

/**
 * @param {string[]} args a run's node arguments
 * @returns {boolean} whether the run is this checkout's own driver copy
 */
const isHead = (args) => args.find((a) => a.endsWith('.js')).startsWith(path.join(ROOT, 'scripts'))

/**
 * @param {string} raw a session's observations file
 * @returns {object[]} its JSONL entries
 */
const entriesOf = (raw) =>
  fs
    .readFileSync(raw, 'utf8')
    .trim()
    .split('\n')
    .map((l) => JSON.parse(l))

/**
 * A client class whose admin and producer record every call and fail where asked.
 * @param {{ calls?: string[], fail?: string[], send?: Function }} [options]
 * @returns {Function} a `Kafka` stand-in for `fetchSessions.prepare`
 */
const fakeKafka = ({ calls = [], fail = [], send } = {}) => {
  /**
   * @param {string} name the recorded call
   * @returns {(arg: any) => Promise<any>} a step that records `name`, then fails if asked or
   *   returns its argument
   */
  const step = (name) => async (arg) => {
    calls.push(name)
    if (fail.includes(name)) throw new Error(`${name} failed`)
    return arg
  }
  /** The stand-in client class. */
  return class Kafka {
    /** @returns {object} an admin whose connection and topic calls are recorded steps */
    admin() {
      return {
        connect: step('admin.connect'),
        disconnect: step('admin.disconnect'),
        createTopics: step('createTopics'),
        deleteTopics: step('deleteTopics'),
      }
    }

    /** @returns {object} a producer whose calls are recorded steps, `send` replaceable */
    producer() {
      return {
        connect: step('producer.connect'),
        disconnect: step('producer.disconnect'),
        send: send ?? step('send'),
      }
    }
  }
}

describe('consumer report rows', () => {
  test('every consumer row reports its controls and corpus identity', async () => {
    for (const name of Object.keys(ROWS)) {
      const { result } = await fixture.runSession(name)
      const row = ROWS[name]
      const { detail } = result
      const expected = row.owed?.length ? 'inconclusive' : 'valid'
      expect([name, detail.status]).toEqual([name, expected])
      const names = new Set(detail.controls.map((control) => control.name))
      for (const control of row.controls) expect([name, names.has(control)]).toEqual([name, true])
      expect(detail.controls.filter(({ passed }) => !passed)).toEqual([])
      for (const config of row.configs) {
        const workload = [...row.node, row.driver, ...row.args, ...config.args].join(' ')
        expect(detail.notes).toContain(`workload ${config.name}: ${workload}`)
      }
      if (!['consumer-stickyAssign-g6'].includes(name)) {
        const corpus = detail.controls.filter((c) => c.name === 'identical corpus on every tree')
        expect([name, corpus.length]).toEqual([name, row.configs.length])
      }
      expect(detail.notes).toContain(
        `contracts: ${row.contracts.join(', ') || 'none (additional row)'}`
      )
      const numbered = result.rows.filter((r) => r.status === expected)
      const arms = session.armsOf(row).length
      expect(numbered).toHaveLength(arms * row.metrics.length)
      expect(
        numbered.every((r) => Number.isFinite(r.median) && r.runs === (row.measured ?? 7))
      ).toBe(true)
    }
  })

  test('the sessions row reads BENCH_NODES from the started containers', async () => {
    const created = []
    const sent = []
    const Kafka = fakeKafka({
      send: async ({ messages }) => sent.push(messages.length),
    })
    /** A client whose admin records the topics it creates. */
    const recording = class extends Kafka {
      /** @returns {object} the admin, `createTopics` recording its topics */
      admin() {
        const admin = super.admin()
        return { ...admin, createTopics: async ({ topics }) => created.push(...topics) }
      }
    }
    const hooks = { prepare: (context) => fetchSessions.prepare(context, { Kafka: recording }) }
    const env = fixture.frozenEnv({ BENCH_NODES: JSON.stringify(NODES) })
    const { line, result, run } = await fixture.runSession('consumer-sessions-L1', { env, hooks })

    expect(line.nodes).toEqual(NODES)
    const timed = run.calls.filter(({ args }) => args.includes('--arm'))
    expect(timed.length).toBeGreaterThan(0)
    for (const call of timed) {
      expect(JSON.parse(call.env.BENCH_NODES)).toEqual(NODES)
      expect(path.isAbsolute(call.env.BENCH_EVIDENCE_DIR)).toBe(true)
      expect(call.args.slice(-6)).toEqual([
        '--arm',
        'L1',
        '--sessions',
        expect.stringMatching(/^(true|false)$/),
        '--topic',
        created[0].topic,
      ])
    }
    const leaders = [1, 2, 3].map(
      (id) => created[0].replicaAssignment.filter((p) => p.replicas[0] === id).length
    )
    expect(leaders).toEqual([334, 333, 333])
    expect(sent.reduce((a, b) => a + b, 0)).toBe(200000)
    expect(result.detail.controls).toContainEqual(
      expect.objectContaining({ name: 'BENCH_NODES from the started containers', passed: true })
    )

    const missing = await fixture.runSession('consumer-sessions-L1', {
      env: fixture.frozenEnv(),
      hooks,
    })
    expect(missing.line.error).toBe(
      'BENCH_NODES is not set: the workflow reads it from the started containers'
    )
    expect(missing.result.rows).toEqual([
      expect.objectContaining({ status: 'error', reason: missing.line.error }),
    ])
  })

  test('the frozen alpha.9c and alpha.9b arms print their tree SHA and driver digest', async () => {
    const driver = path.join(ROOT, 'scripts', 'benchmarks', 'consumeThroughput.js')
    const digest = crypto.createHash('sha256').update(fs.readFileSync(driver)).digest('hex')
    const { result } = await fixture.runSession('consumer-eachBatch')
    for (const label of ['alpha.9c', 'alpha.9b']) {
      const sha = crypto.createHash('sha1').update(label).digest('hex')
      expect(result.detail.notes).toContain(`tree ${label}: ${sha}, driver sha256 ${digest}`)
      expect(result.detail.controls).toContainEqual({
        arm: label,
        name: 'identical comparison driver',
        passed: true,
        detail: `${digest} vs ${digest}`,
      })
      for (const arm of [label, `${label}~AA`]) {
        expect(result.rows.filter((r) => r.arm === arm && r.status === 'valid')).toHaveLength(3)
      }
    }

    const changed = fixture.frozenEnv()
    const trees = JSON.parse(changed.BENCH_TREES)
    trees['alpha.9b'] = fixture.tree('alpha.9b-changed', { driverBytes: '// another driver\n' })
    const mismatch = await fixture.runSession('consumer-eachBatch', {
      env: { BENCH_TREES: JSON.stringify(trees) },
    })
    expect(mismatch.result.detail.status).toBe('invalid')
    expect(mismatch.result.detail.reason).toContain('alpha.9b identical comparison driver')

    const absent = await fixture.runSession('consumer-eachBatch', { env: {} })
    expect(absent.result.detail.status).toBe('valid')
    expect(absent.result.rows.find((r) => r.arm === 'alpha.9c')).toMatchObject({
      status: 'error',
      reason: 'round 1: BENCH_TREES names no alpha.9c tree',
    })
  })

  test('a sizing run with a partition that received no data is error with its reason', async () => {
    const stderr = 'Error: Data reached 999 of 1000 partitions\n    at SizingObservation.finish'
    const run = fakeRun((args) => (args.includes('--rss') ? { status: 1, stderr } : undefined))
    const { result, line } = await fixture.runSession('consumer-sizing-25mib', { run })
    expect(line.arms[HEAD].errors[0]).toBe(
      'round 1: exit 1: Error: Data reached 999 of 1000 partitions at SizingObservation.finish'
    )
    expect(result.detail.status).toBe('error')
    expect(result.detail.reason).toContain('Data reached 999 of 1000 partitions')
    for (const row of result.rows) {
      expect(row.status).toBe('error')
      expect(row.reason).toContain('Data reached 999 of 1000 partitions')
      expect(row).not.toHaveProperty('median')
    }
  })

  test('the sizing rows come from consumeThroughput --rss, never from fetchRound', async () => {
    const sizes = { 10: 10485760, 25: 26214400, 50: 52428800 }
    for (const [mib, bytes] of Object.entries(sizes)) {
      const name = `consumer-sizing-${mib}mib`
      const row = ROWS[name]
      expect(row.contracts).toEqual([`sizing/max-bytes-${mib}mib`])
      expect(row.driver).toBe('consumeThroughput.js')
      expect(row.node).toContain('--expose-gc')
      expect(row.args).toEqual(['--partitions', '1000', '--max-bytes', String(bytes), '--rss'])
      const { run, result } = await fixture.runSession(name)
      expect(result.detail.status).toBe('valid')
      for (const { args } of run.calls) {
        expect(path.basename(args.find((arg) => arg.endsWith('.js')))).toBe('consumeThroughput.js')
        expect(args).toEqual(expect.arrayContaining(['--rss', '--max-bytes', String(bytes)]))
      }
    }
    const fetchRoundContracts = Object.values(ROWS)
      .filter((row) => row.driver === 'fetchRound.js')
      .flatMap((row) => row.contracts)
    expect(fetchRoundContracts).toEqual(['performance/fetch-round'])
  })

  test('the G6 row runs stag-500+500x30k with one discarded warm-up and seven measured runs and archives the CPU profiles', async () => {
    const run = fakeRun((args, n) => {
      const line = output(args, n)
      return flag(args, '--worker') === 'time' && n <= 9 ? { ms: 999999 } : line
    })
    const { line, result, raw } = await fixture.runSession('consumer-stickyAssign-g6', { run })
    const timed = run.calls.filter(({ args }) => flag(args, '--worker') === 'time')
    expect(timed).toHaveLength(6 * 8)
    for (const { args, cwd } of timed) {
      expect(cwd).toBe(ROOT)
      expect(flag(args, '--shape')).toBe('stag-500+500x30k')
      expect(args).toContain('--cpu-prof')
      expect(args.find((a) => a.startsWith('--cpu-prof-dir='))).toContain(path.dirname(raw))
      expect(path.isAbsolute(flag(args, '--tree'))).toBe(true)
    }
    const observations = entriesOf(raw)
    expect(observations.filter((o) => o.warmup)).toHaveLength(6)
    expect(observations.filter((o) => !o.warmup)).toHaveLength(42)
    expect(line).toMatchObject({ warmup: 1, measured: 7 })
    for (const arm of Object.values(line.arms)) {
      expect(arm.stats.ms.n).toBe(7)
      expect(arm.stats.ms.max).toBeLessThan(999999)
    }
    expect(Object.values(line.profiles)).toEqual(Array(6).fill(8))
    expect(line.selfTime[HEAD][0]).toEqual({ fn: 'balance stickyCore.js:42', ms: 32 })
    expect(result.detail.status).toBe('inconclusive')
    expect(result.detail.reason).toBe(`owed: ${ROWS['consumer-stickyAssign-g6'].owed[0]}`)
    expect(
      result.detail.notes.some((n) => n.startsWith('head self time: balance stickyCore.js:42'))
    ).toBe(true)
    expect(result.detail.controls.filter((c) => c.name === 'CPU profile per run')).toHaveLength(6)

    const unprofiled = fakeRun((args) =>
      flag(args, '--worker') === 'time' ? { ms: 4100 } : undefined
    )
    const without = await fixture.runSession('consumer-stickyAssign-g6', {
      run: async (args, options) =>
        unprofiled(
          args.filter((a) => !a.startsWith('--cpu-prof')),
          options
        ),
    })
    expect(without.result.detail.status).toBe('invalid')
    expect(without.result.detail.reason).toContain('head CPU profile per run')
  })

  test('a consumer row outside its own workflow job is omitted with its reason', async () => {
    const entry = manifestEntries().find((e) => e.name === 'consumer-eachBatch')
    const lines = []
    const raw = path.join(tmp, 'bench-raw', entry.name, 'fork-run-1.jsonl')
    const code = await session.main(['--row', entry.name, '--raw', raw], {
      env: {},
      log: (line) => lines.push(line),
    })
    expect(code).toBe(0)
    expect(fs.existsSync(raw)).toBe(false)
    const adapter = require('../adapters/consumeThroughput')
    const result = adapter.read({ status: 0, stdout: `${lines[0]}\n` }, entry)
    expect(adapter.rows(entry, 'fork', { outputs: [result] }, 1).rows).toEqual([
      expect.objectContaining({ status: 'omitted', reason: session.OUTSIDE }),
    ])
  })

  test('an A/A spread beyond 3 % makes that row inconclusive, and exactly 3 % stays valid', async () => {
    const { line } = await fixture.runSession('consumer-subscriptionResolve')
    const adapter = require('../adapters/subscriptionResolve')
    const entry = manifestEntries().find((e) => e.name === 'consumer-subscriptionResolve')
    /**
     * @param {number} factor
     * @returns {object} the report of the session whose alpha.9c copy measured `factor` times
     *   every alpha.9c run, its runs and summaries agreeing
     */
    const withCopy = (factor) => {
      const copy = JSON.parse(JSON.stringify(line))
      const { results, stats } = copy.arms['alpha.9c']
      copy.arms['alpha.9c~AA'].results = results.map((r) => ({
        ...r,
        evaluationMs: r.evaluationMs * factor,
      }))
      const { median, min, max, n } = stats.evaluationMs
      copy.arms['alpha.9c~AA'].stats.evaluationMs = {
        median: median * factor,
        min: min * factor,
        max: max * factor,
        n,
      }
      return adapter.rows(entry, 'fork', { outputs: [copy] }, 1)
    }
    const noisy = withCopy(1.032)
    expect(noisy.detail.status).toBe('inconclusive')
    expect(noisy.detail.reason).toBe('A/A beyond 3 %: alpha.9c~AA evaluationMs +3.20 %')
    expect(noisy.rows.every((r) => r.status === 'inconclusive' && Number.isFinite(r.median))).toBe(
      true
    )
    expect(withCopy(0.968).detail.status).toBe('inconclusive')
    expect(withCopy(1.03).detail.status).toBe('valid')
    expect(withCopy(0.97).detail.status).toBe('valid')
    expect(withCopy(1.0300001).detail.status).toBe('inconclusive')
    expect(withCopy(1.0299999).detail.status).toBe('valid')
  })

  test('raw receipts keep every driver run with its arguments, exit and exact stdout and stderr', async () => {
    const special = {
      1: {
        status: 0,
        stdout: 'warn: first\n{"evaluationMs":2.5,"matches":9000,"evaluations":100}\n',
        stderr: 'stderr of run 1',
      },
      3: { status: 0, stdout: 'not a JSON line', stderr: 'parse me' },
      4: { status: 3, stdout: 'partial output', stderr: 'Error: boom\n    at driver' },
      5: { status: null, signal: 'SIGKILL', stdout: 'before the kill', stderr: '' },
    }
    const run = fakeRun((args, n) => special[n])
    const { raw, result } = await fixture.runSession('consumer-subscriptionResolve', {
      run,
      env: {},
    })
    const ran = entriesOf(raw).filter((e) => e.invocation)
    expect(ran).toHaveLength(run.calls.length)
    const dir = path.dirname(raw)
    for (const [n, want] of Object.entries(special)) {
      const { invocation } = ran[n - 1]
      expect(invocation).toMatchObject({
        args: run.calls[n - 1].args,
        cwd: run.calls[n - 1].cwd,
        status: want.status,
        signal: want.signal ?? null,
      })
      expect(fs.readFileSync(path.join(dir, invocation.stdout), 'utf8')).toBe(want.stdout)
      expect(fs.readFileSync(path.join(dir, invocation.stderr), 'utf8')).toBe(want.stderr)
    }
    expect(ran[2].error).toBe('exit 0: no JSON line: parse me')
    expect(ran[3].error).toBe('exit 3: Error: boom at driver')
    expect(ran[4].error).toBe('killed by SIGKILL')
    expect(result.detail.status).toBe('error')
    expect(result.detail.reason).toMatch(/^head(~AA)?: round 1: /)

    const big = await session.runProcess(
      ['-e', 'process.stdout.write("x".repeat(100)); process.stderr.write("e")'],
      { cwd: tmp, env: process.env, timeoutMs: 30000, maxOutput: 10 }
    )
    expect(big).toMatchObject({ status: 0, stdout: 'x'.repeat(10), stderr: 'e' })
    expect(big.truncated).toEqual({ stdout: true })
  })

  test('an active fetch-session run with the whole corpus but too few batch gaps is inconclusive and keeps its numbers', async () => {
    let clock = 0
    const measurement = new SessionMeasurement({
      now: () => clock,
      cpu: () => ({ user: clock, system: 0 }),
    })
    const consumer = {
      connect: async () => {},
      subscribe: async () => {},
      run: async ({ eachBatch }) => {
        for (let i = 0; i < 100; i++) {
          clock += 1
          await eachBatch({ batch: { messages: Array(i < 99 ? 2000 : 2001).fill(null) } })
        }
      },
      stop: async () => {},
      disconnect: async () => {},
    }
    const sample = await runArm(
      { consumer: () => consumer },
      { arm: 'S1', topic: 't', sessions: false, measurement, readRetained: async () => 2.2e7 }
    )
    expect(sample).toMatchObject({ deliveredMessages: 200001, handlerEntries: 100, gapSamples: 99 })

    const run = fakeRun((args) =>
      args.includes('--arm')
        ? {
            arm: 'S1',
            sessions: flag(args, '--sessions') === 'true',
            topic: 't',
            sample,
            window: {},
          }
        : undefined
    )
    const { result } = await fixture.runSession('consumer-sessions-S1', { run, env: {} })
    expect(result.detail.status).toBe('inconclusive')
    expect(result.detail.reason).toBe(
      'fewer than 1000 batch gaps on head/sessions-false, head/sessions-false~AA, head/sessions-true, head/sessions-true~AA'
    )
    const head = result.rows.filter((r) => r.arm.startsWith('head'))
    expect(head.every((r) => r.status === 'inconclusive' && Number.isFinite(r.median))).toBe(true)

    for (const gaps of [undefined, NaN]) {
      const counted = fakeRun((args, n) => ({ ...output(args, n), gapSamples: gaps }))
      const throughput = await fixture.runSession('consumer-eachMessage', { run: counted, env: {} })
      expect(throughput.result.detail.status).toBe('inconclusive')
      expect(throughput.result.detail.reason).toBe(
        'fewer than 200000 counted delivery-gap samples on head, head~AA'
      )
    }
  })

  test('the L2 producer window counts offered, acknowledged, failed and unsettled sends and settles before the next run', async () => {
    jest.useFakeTimers()
    const window = (send) => new fetchSessions.WindowProducer({ producer: { send }, topic: 't' })

    const full = window(async () => {})
    full.start()
    await jest.advanceTimersByTimeAsync(60000)
    expect(await full.settle()).toMatchObject({
      offered: 12000,
      acknowledged: 12000,
      failed: 0,
      unsettled: 0,
      partitions: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
      ticks: 600,
      endedBy: 'window',
    })
    expect(jest.getTimerCount()).toBe(0)

    let n = 0
    const failing = window(async () => {
      if (++n === 5) throw new Error('NOT_LEADER_OR_FOLLOWER')
    })
    failing.start()
    await jest.advanceTimersByTimeAsync(60000)
    expect(await failing.settle()).toMatchObject({
      offered: 12000,
      acknowledged: 11980,
      failed: 20,
      errors: ['NOT_LEADER_OR_FOLLOWER'],
    })

    let m = 0
    const hanging = window(() => (++m === 600 ? new Promise(() => {}) : Promise.resolve()))
    hanging.start()
    await jest.advanceTimersByTimeAsync(60000)
    const settling = hanging.settle()
    await jest.advanceTimersByTimeAsync(30000)
    expect(await settling).toMatchObject({ offered: 12000, acknowledged: 11980, unsettled: 20 })
    expect(jest.getTimerCount()).toBe(0)

    const short = window(async () => {})
    short.start()
    await jest.advanceTimersByTimeAsync(30000)
    short.stop('child-close')
    expect(await short.settle()).toMatchObject({
      offered: 6000,
      ticks: 300,
      endedBy: 'child-close',
    })
    expect(jest.getTimerCount()).toBe(0)
  })

  test('an L2 run whose producer window fell short or failed is never a valid row', async () => {
    jest.useFakeTimers()
    /**
     * Runs the L2 row on this tree with the real preparation, the window lasting `windowMs`.
     * @param {{ windowMs: number, send: Function }} options
     * @returns {Promise<object>} what `runSession` returns
     */
    const l2Session = async ({ windowMs, send }) => {
      const Kafka = fakeKafka({ send })
      const hooks = { prepare: (context) => fetchSessions.prepare(context, { Kafka }) }
      const run = fakeRun(async (args, n, options) => {
        if (!args.includes('--arm')) return undefined
        const child = fakeChild()
        options.onMessage({ type: 'measurement-ready' }, child)
        await jest.advanceTimersByTimeAsync(windowMs)
        child.emit('close')
        return undefined
      })
      const env = { BENCH_NODES: JSON.stringify(NODES) }
      return fixture.runSession('consumer-sessions-L2', { run, env, hooks })
    }

    const complete = await l2Session({ windowMs: 60100, send: async () => {} })
    expect(complete.result.detail.status).toBe('valid')
    for (const label of ['head/sessions-false', 'head/sessions-true~AA']) {
      for (const result of complete.line.arms[label].results) {
        expect(result.producer).toMatchObject({
          ...FULL_WINDOW,
          partitions: FULL_WINDOW.partitions,
        })
      }
    }

    let sends = 0
    const failed = await l2Session({
      windowMs: 60100,
      send: async () => {
        // Sends 1 to 2400 are the four discarded warm-up runs.
        if (++sends === 3007) throw new Error('REQUEST_TIMED_OUT')
      },
    })
    expect(failed.result.detail.status).toBe('invalid')
    expect(failed.result.detail.reason).toContain('L2 producer window complete')
    const detail = failed.result.detail.controls.find(
      (c) => c.name === 'L2 producer window complete'
    )
    expect(detail.detail).toContain('REQUEST_TIMED_OUT')

    const short = await l2Session({ windowMs: 30000, send: async () => {} })
    expect(short.result.detail.status).toBe('invalid')
    expect(short.result.detail.reason).toContain('L2 producer window complete')
    expect(jest.getTimerCount()).toBe(0)
  })

  test('an L2 window still sending after its settlement limit ends the session before another run shares its producer and topic', async () => {
    jest.useFakeTimers()
    const events = []
    const calls = []
    let sends = 0
    let release
    let pumping
    const ended = { done: false }
    /**
     * Advances the clock until the session ends, as the wall clock would during settlement.
     * @returns {Promise<void>}
     */
    const pump = async () => {
      while (!ended.done) await jest.advanceTimersByTimeAsync(1000)
    }
    /**
     * Acknowledges every send at once except send 3007, which is held until `release`.
     * @returns {Promise<void>}
     */
    const send = () => {
      // Sends 1 to 2400 are the four discarded warm-up runs; 3007 falls in a measured run.
      if (++sends !== 3007) return Promise.resolve()
      events.push('send held')
      return new Promise((resolve) => {
        release = () => {
          events.push('held send acknowledged')
          resolve()
        }
      })
    }
    const Kafka = fakeKafka({ calls, send })
    const hooks = { prepare: (context) => fetchSessions.prepare(context, { Kafka }) }
    const run = fakeRun(async (args, n, options) => {
      if (!args.includes('--arm')) return undefined
      events.push(`run ${n}`)
      const child = fakeChild()
      options.onMessage({ type: 'measurement-ready' }, child)
      await jest.advanceTimersByTimeAsync(60100)
      child.emit('close')
      if (release) pumping ??= pump()
      return undefined
    })
    const env = { BENCH_NODES: JSON.stringify(NODES) }
    const { line, result, raw } = await fixture.runSession('consumer-sessions-L2', {
      run,
      env,
      hooks,
    })
    ended.done = true
    await pumping
    release()
    await Promise.resolve()

    const held = events.indexOf('send held')
    expect(held).toBeGreaterThan(0)
    expect(events.slice(held + 1)).toEqual(['held send acknowledged'])
    expect(calls.slice(-3)).toEqual(['deleteTopics', 'producer.disconnect', 'admin.disconnect'])
    const timed = entriesOf(raw).filter((e) => !e.error)
    expect(timed.at(-1).result.producer).toMatchObject({ offered: 12000, unsettled: 20 })
    expect(result.detail.status).toBe('error')
    expect(result.detail.reason).toContain('20 messages unsettled after 30000 ms')
    expect(line.arms['head/sessions-true~AA'].errors.at(-1)).toContain('20 messages unsettled')
    expect(jest.getTimerCount()).toBe(0)
  })

  test('preparing a fetch-session row disconnects every client and keeps the first error whatever step fails', async () => {
    const context = {
      row: ROWS['consumer-sessions-L1'],
      name: 'consumer-sessions-L1',
      dir: tmp,
      env: { BENCH_NODES: JSON.stringify(NODES) },
    }
    for (const failing of ['admin.connect', 'producer.connect', 'createTopics', 'send']) {
      const calls = []
      const Kafka = fakeKafka({ calls, fail: [failing] })
      await expect(fetchSessions.prepare(context, { Kafka })).rejects.toThrow(`${failing} failed`)
      expect([failing, calls.slice(-2)]).toEqual([
        failing,
        ['producer.disconnect', 'admin.disconnect'],
      ])
    }

    const calls = []
    const Kafka = fakeKafka({ calls, fail: ['deleteTopics', 'producer.disconnect'] })
    const prepared = await fetchSessions.prepare(context, { Kafka })
    await expect(prepared.cleanup()).rejects.toThrow('deleteTopics failed')
    expect(calls.slice(-3)).toEqual(['deleteTopics', 'producer.disconnect', 'admin.disconnect'])
  })

  test('an arm its tree cannot run is labelled unsupported and never measured', async () => {
    const old = fixture.tree('alpha.9b-old', { lacks: ['fetchSessions option'] })
    const env = {
      BENCH_TREES: JSON.stringify({ 'alpha.9c': fixture.tree('alpha.9c'), 'alpha.9b': old }),
    }
    const { line, result, run } = await fixture.runSession('consumer-sessions-S1', { env })
    const ranOnOld = run.calls.filter(({ args }) => args.some((a) => a.startsWith(old)))
    expect(ranOnOld.some(({ args }) => flag(args, '--sessions') === 'true')).toBe(false)
    expect(ranOnOld.filter(({ args }) => flag(args, '--sessions') === 'false')).toHaveLength(16)
    const reason = 'alpha.9b has no fetchSessions option'
    for (const label of ['alpha.9b/sessions-true', 'alpha.9b/sessions-true~AA']) {
      expect(line.arms[label]).toMatchObject({ unsupported: reason, measured: 0 })
      expect(
        result.rows
          .filter((r) => r.arm === label)
          .every((r) => r.status === 'omitted' && r.reason === reason)
      ).toBe(true)
    }
    expect(line.arms['alpha.9b/sessions-false'].measured).toBe(7)
    expect(result.detail.status).toBe('valid')
    expect(result.detail.notes).toContain(`unsupported alpha.9b/sessions-true: ${reason}`)

    const noModule = fixture.tree('alpha.9b-nosub', { lacks: ['subscription module'] })
    const subscription = await fixture.runSession('consumer-subscriptionResolve', {
      env: {
        BENCH_TREES: JSON.stringify({ 'alpha.9c': fixture.tree('alpha.9c'), 'alpha.9b': noModule }),
      },
    })
    expect(subscription.run.calls.some(({ args }) => args.includes(noModule))).toBe(false)
    expect(subscription.line.arms['alpha.9b'].unsupported).toBe(
      'alpha.9b has no subscription module'
    )
  })

  test('a proof the row cannot give is printed as owed and keeps the row inconclusive', async () => {
    for (const name of ['consumer-stickyAssign-g6', 'consumer-offsetCommit-retirement']) {
      const { owed } = ROWS[name]
      expect(owed.length).toBeGreaterThan(0)
      const { result } = await fixture.runSession(name)
      expect(result.detail.status).toBe('inconclusive')
      expect(result.detail.reason).toBe(owed.map((o) => `owed: ${o}`).join('; '))
      for (const o of owed) expect(result.detail.notes).toContain(`owed: ${o}`)
    }
    const mismatched = fakeRun((args) =>
      flag(args, '--worker') === 'bytes' && flag(args, '--tree').endsWith(`${path.sep}alpha.9b`)
        ? { sticky: ['m-1:0000'] }
        : undefined
    )
    const g6 = await fixture.runSession('consumer-stickyAssign-g6', { run: mismatched })
    expect(g6.result.detail.status).toBe('invalid')
    expect(g6.result.detail.reason).toContain('alpha.9b assignment identical across trees')
  })

  test('the decode row needs equal decoded-content checksums on every tree, not only equal counts', async () => {
    const valid = await fixture.runSession('consumer-recordBatchDecode')
    expect(valid.result.detail.status).toBe('valid')
    const controls = valid.run.calls.filter(({ args }) => args.includes('--checksum'))
    expect(controls).toHaveLength(3)
    expect(controls.map(({ args }) => flag(args, '--arms'))).toEqual(
      Array(3).fill('uncompressed,gzip,snappy')
    )

    const changed = fakeRun((args, n) => {
      const line = output(args, n)
      if (
        args.includes('--checksum') &&
        args.some((a) => a.includes(`${path.sep}alpha.9b${path.sep}`))
      ) {
        line.checksums.gzip.sha256 = 'f'.repeat(64)
      }
      return line
    })
    const differ = await fixture.runSession('consumer-recordBatchDecode', { run: changed })
    expect(differ.result.detail.status).toBe('invalid')
    expect(differ.result.detail.reason).toContain('gzip decoded checksum equal across trees')

    const empty = fakeRun((args, n) => {
      const line = output(args, n)
      if (args.includes('--checksum')) {
        for (const c of Object.values(line.checksums)) c.records = 0
      } else line.records = 0
      return line
    })
    const none = await fixture.runSession('consumer-recordBatchDecode', { run: empty })
    expect(none.result.detail.status).toBe('invalid')
    expect(none.result.detail.reason).toContain('decoded records equal across trees')

    for (const sha256 of [undefined, null, '', 'gzip-content']) {
      for (const where of ['alpha.9b', 'every tree']) {
        const absent = fakeRun((args, n) => {
          const line = output(args, n)
          const onTree = args.some((a) => a.includes(`${path.sep}alpha.9b${path.sep}`))
          if (args.includes('--checksum') && (where === 'every tree' || onTree)) {
            if (sha256 === undefined) delete line.checksums.gzip.sha256
            else line.checksums.gzip.sha256 = sha256
          }
          return line
        })
        const { result } = await fixture.runSession('consumer-recordBatchDecode', { run: absent })
        const gzip = result.detail.controls.find(
          (c) => c.arm === 'gzip' && c.name === 'decoded checksum equal across trees'
        )
        expect([sha256, where, result.detail.status, gzip.passed]).toEqual([
          sha256,
          where,
          'invalid',
          false,
        ])
      }
    }
  })

  test('the 1000-partition row needs every partition populated and consumed on every tree, else it is error', async () => {
    const valid = await fixture.runSession('consumer-partitions-1000')
    expect(valid.result.detail.status).toBe('valid')
    const controls = valid.run.calls.filter(({ args }) =>
      args.some((a) => a.endsWith('consumeCoverage.js'))
    )
    expect(controls).toHaveLength(3)
    for (const { args, cwd } of controls) {
      expect(args).toEqual(expect.arrayContaining(['--expose-gc', '--partitions', '1000']))
      expect(args.find((a) => a.endsWith('.js')).startsWith(cwd)).toBe(true)
    }

    const shortOn9c = fakeRun((args, n) =>
      args.some((a) => a.endsWith('consumeCoverage.js')) && args.some((a) => a.includes('alpha.9c'))
        ? { ...output(args, n), partitionsWithData: 999 }
        : undefined
    )
    const short = await fixture.runSession('consumer-partitions-1000', { run: shortOn9c })
    expect(short.result.detail.status).toBe('error')
    expect(short.result.detail.reason).toContain('alpha.9c data on every partition')

    const headFails = fakeRun((args) =>
      args.some((a) => a.endsWith('consumeCoverage.js')) && isHead(args)
        ? { status: 1, stderr: 'Error: Data reached 999 of 1000 partitions' }
        : undefined
    )
    const failed = await fixture.runSession('consumer-partitions-1000', { run: headFails })
    expect(failed.result.detail.status).toBe('error')
    expect(failed.result.detail.reason).toContain('head data on every partition')
    expect(
      failed.result.detail.controls.find(
        (c) => c.arm === HEAD && c.name === 'data on every partition'
      ).detail
    ).toContain('Data reached 999 of 1000 partitions')

    const coverage = new PartitionCoverage(3)
    await coverage.collect()
    coverage.start()
    coverage.observe(0)
    coverage.observe(2)
    expect(await coverage.finish()).toEqual({ partitionsWithData: 2 })
    expect(() => coverage.observe(3)).toThrow('Unexpected partition 3')
    expect(() =>
      verifyCoverage({ partitions: 1000, populatedPartitions: 1000, partitionsWithData: 999 })
    ).toThrow('Data reached 999 of 1000 partitions')
    expect(() =>
      verifyCoverage({ partitions: 1000, populatedPartitions: 998, partitionsWithData: 1000 })
    ).toThrow('The corpus populated 998 of 1000 partitions')
    expect(() =>
      verifyCoverage({ partitions: 1000, populatedPartitions: 1000, partitionsWithData: 1000 })
    ).not.toThrow()
  })

  test('the fetch-session rows need the remove-byte-counter mutant rejected on every tree', async () => {
    const valid = await fixture.runSession('consumer-sessions-S2')
    expect(valid.result.detail.status).toBe('valid')
    for (const mutant of [{ rejected: false }, undefined]) {
      const run = fakeRun((args, n) => {
        const line = output(args, n)
        if (args.includes('--control') && args.some((a) => a.includes('alpha.9c')))
          line.mutant = mutant
        return line
      })
      const { result } = await fixture.runSession('consumer-sessions-S2', { run })
      expect(result.detail.status).toBe('invalid')
      expect(result.detail.reason).toContain('alpha.9c remove-byte-counter mutant rejected')
    }
  })

  test('each row prints its original bounds against alpha.9b and against its same-tree arm as information only', async () => {
    const slowHead = fakeRun((args, n) => {
      const line = output(args, n)
      if (isHead(args) && flag(args, '--listener') === '1') line.messagesPerSecond *= 0.5
      else if (isHead(args)) line.messagesPerSecond *= 0.8
      return line
    })
    const { result } = await fixture.runSession('consumer-listener', { run: slowHead })
    expect(result.detail.status).toBe('valid')
    const notes = result.detail.notes.filter((n) => n.startsWith('bound '))
    const note = (prefix) => notes.find((n) => n.startsWith(prefix))
    expect(
      note('bound messagesPerSecond head/listener-0 vs alpha.9b/listener-0 (information): ')
    ).toMatch(/ -(19|20)\.\d\d % \(original bound -3 %\)$/)
    expect(
      note('bound messagesPerSecond alpha.9c/listener-0 vs alpha.9b/listener-0 (information): ')
    ).toMatch(/ [+-]0\.\d\d % \(original bound -3 %\)$/)
    expect(
      note('bound messagesPerSecond head/listener-1 vs head/listener-0 (information): ')
    ).toMatch(/ -(37|38)\.\d\d % \(original bound within \+3 % of the no-listener arm\)$/)
    expect(
      note('bound heapRetainedBytes head/listener-0 vs alpha.9b/listener-0 (information): ')
    ).toMatch(/ [+-]0\.\d\d MiB \(original bound \+2 MiB\)$/)
    expect(result.detail.controls.filter((c) => c.name.startsWith('A/A'))).toHaveLength(6)

    const l2 = await fixture.runSession('consumer-sessions-L2')
    expect(l2.result.detail.notes).toContain(
      'bound sample.bytesPerMessage head/sessions-true vs head/sessions-false (information): 100.00 % of the reference (original bound at most 10 % of sessionless)'
    )
    expect(l2.result.detail.notes).toContain(
      'rule (information): fetchSessions stays false unless the correctness matrix passed and every fixed row of the default table passes with valid controls; alpha.9b is a separate sessionless comparator'
    )

    const absent = await fixture.runSession('consumer-eachBatch', { env: {} })
    expect(absent.result.detail.notes).toContain(
      'bound messagesPerSecond head vs alpha.9b (information): not measured (original bound -3 %)'
    )
  })
})
