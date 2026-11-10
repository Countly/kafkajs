const fs = require('fs')
const path = require('path')
const { BrokerBouncer } = require('../leaderElection')

const evidenceFile = path.resolve(
  __dirname,
  '../../.tmp/e2e',
  `broker-bouncer-${process.pid}.jsonl`
)
const oldStart = '2026-11-09T12:00:00.000000001Z'
const newStart = '2026-11-09T12:00:00.000000002Z'
const state = (overrides = {}) => ({
  Id: 'container-kafka2',
  Status: 'running',
  Running: true,
  Paused: false,
  Restarting: false,
  StartedAt: oldStart,
  FinishedAt: '0001-01-01T00:00:00Z',
  ExitCode: 0,
  OOMKilled: false,
  Error: '',
  ...overrides,
})
const exited = state({ Status: 'exited', Running: false, ExitCode: 137 })
const restarted = state({ StartedAt: newStart })
const cluster = { admin: {}, topics: ['workload'] }

/** A compose double with explicit state transitions and a clock advanced only by polling. */
const harness = ({ afterDown = [exited], afterStart = [restarted], onRun = () => {} } = {}) => {
  const commands = []
  const waits = []
  let phase = 'beforeDown'
  let clock = 0
  const queues = { beforeDown: [state()], afterDown: [...afterDown], afterStart: [...afterStart] }
  const bouncer = new BrokerBouncer({
    run: async (...args) => {
      commands.push(args)
      if (['stop', 'kill', 'pause'].includes(args[0])) phase = 'afterDown'
      if (['start', 'unpause'].includes(args[0])) phase = 'afterStart'
      await onRun(...args)
      if (args[0] === 'inspect') {
        const queue = queues[phase]
        return { stdout: JSON.stringify(queue.length > 1 ? queue.shift() : queue[0]), stderr: '' }
      }
      if (args[0] === 'logs') return { stdout: 'broker log\n', stderr: 'log stderr\n' }
      return { stdout: '', stderr: 'compose progress' }
    },
    answers: async () => true,
    waitForFullIsr: async () => {},
    now: () => clock,
    sleep: async (ms) => {
      waits.push(ms)
      clock += ms
    },
    nodes: { kafka2: {} },
  })
  bouncer.read = bouncer.run
  const records = () =>
    fs.existsSync(evidenceFile)
      ? fs.readFileSync(evidenceFile, 'utf8').trim().split('\n').filter(Boolean).map(JSON.parse)
      : []
  const controls = () =>
    commands.filter(([verb]) => ['stop', 'kill', 'start', 'pause', 'unpause'].includes(verb))
  return { bouncer, commands, waits, controls, records, elapsed: () => clock }
}

describe('BrokerBouncer lifecycle', () => {
  beforeEach(() => fs.rmSync(evidenceFile, { force: true }))
  afterEach(() => fs.rmSync(evidenceFile, { force: true }))

  test.each(['stop', 'kill'])(
    'waits for a stale running container to exit after %s before starting it',
    async (kind) => {
      const h = harness({ afterDown: [state(), state(), exited] })
      await h.bouncer.step({ service: 'kafka2', kind }, cluster)
      expect(h.commands.map(([verb]) => verb)).toEqual([
        'inspect',
        kind,
        'inspect',
        'inspect',
        'inspect',
        'start',
        'inspect',
      ])
      expect(h.waits.filter((ms) => ms > 0)).toEqual([200, 200])
      expect(h.bouncer.down.size).toBe(0)
      expect(h.records()).toEqual([])
    }
  )

  test('detects a start no-op with an unchanged StartedAt and records diagnostics without retrying', async () => {
    const h = harness({ afterStart: [state()] })
    await expect(h.bouncer.step({ service: 'kafka2', kind: 'kill' }, cluster)).rejects.toThrow(
      /restart.*30000 ms/i
    )
    expect(h.elapsed()).toBe(30000)
    expect(h.controls()).toEqual([
      ['kill', 'kafka2'],
      ['start', 'kafka2'],
    ])
    const [record] = h.records()
    expect(record.service).toBe('kafka2')
    expect(record.states.beforeDown.StartedAt).toBe(oldStart)
    expect(record.states.afterDown.Running).toBe(false)
    expect(record.states.afterStart.StartedAt).toBe(oldStart)
    expect(record.commands).toEqual([
      expect.objectContaining({
        args: ['kill', 'kafka2'],
        startedAt: expect.any(Number),
        finishedAt: expect.any(Number),
        status: 0,
        stderr: 'compose progress',
      }),
      expect.objectContaining({
        args: ['start', 'kafka2'],
        startedAt: expect.any(Number),
        finishedAt: expect.any(Number),
        status: 0,
        stderr: 'compose progress',
      }),
    ])
    expect(record.logs).toEqual({ stdout: 'broker log\n', stderr: 'log stderr\n' })
    expect(h.commands).toContainEqual(['logs', '--timestamps', '--tail', '200', 'kafka2'])
    await expect(h.bouncer.step({ service: 'kafka2', kind: 'stop' }, cluster)).rejects.toThrow(
      'while kafka2 is down'
    )
    expect(h.controls()).toHaveLength(2)
  })

  test('reports a new instance that exits with OOM state immediately', async () => {
    const crashed = state({
      StartedAt: newStart,
      Status: 'exited',
      Running: false,
      ExitCode: 137,
      OOMKilled: true,
      Error: 'out of memory',
    })
    const h = harness({ afterStart: [crashed] })
    await expect(h.bouncer.step({ service: 'kafka2', kind: 'stop' }, cluster)).rejects.toThrow(
      /exited/
    )
    expect(h.elapsed()).toBe(0)
    expect(h.records()[0].states.afterStart).toMatchObject(crashed)
    expect(h.controls()).toHaveLength(2)
  })

  test('accepts a newer StartedAt within the same millisecond after delayed start observation', async () => {
    const h = harness({ afterStart: [exited, restarted] })
    await expect(
      h.bouncer.step({ service: 'kafka2', kind: 'kill' }, cluster)
    ).resolves.toMatchObject({ service: 'kafka2' })
    expect(h.waits).toContain(200)
    expect(h.bouncer.down.size).toBe(0)
  })

  test('bounds the exit barrier and never starts a container still running at the deadline', async () => {
    const h = harness({ afterDown: [state()] })
    await expect(h.bouncer.step({ service: 'kafka2', kind: 'kill' }, cluster)).rejects.toThrow(
      /exit.*30000 ms/i
    )
    expect(h.elapsed()).toBe(30000)
    expect(h.controls()).toEqual([['kill', 'kafka2']])
    expect(h.records()[0].states.afterDown.Running).toBe(true)
    expect(h.bouncer.down.has('kafka2')).toBe(true)
  })

  test('does not start when the exit inspection completes after the barrier deadline', async () => {
    let inspections = 0
    const h = harness({
      onRun: async (verb) => {
        if (verb === 'inspect' && ++inspections === 2) await h.bouncer.wait(31000)
      },
    })
    await expect(h.bouncer.step({ service: 'kafka2', kind: 'kill' }, cluster)).rejects.toThrow(
      /exit.*30000 ms/i
    )
    expect(h.controls()).toEqual([['kill', 'kafka2']])
    expect(h.bouncer.down.has('kafka2')).toBe(true)
  })

  test('captures a later crash when readiness fails after restart proof succeeded', async () => {
    const crashed = state({
      ...restarted,
      Running: false,
      Status: 'exited',
      ExitCode: 137,
      OOMKilled: true,
    })
    const first = new Error('readiness failed')
    const h = harness({ afterStart: [restarted, crashed] })
    h.bouncer.waitForReachable = async () => {
      throw first
    }
    await expect(h.bouncer.step({ service: 'kafka2', kind: 'kill' }, cluster)).rejects.toBe(first)
    expect(h.records()[0].states.failure).toMatchObject(crashed)
    expect(h.bouncer.down.has('kafka2')).toBe(true)
  })

  test('preserves a lifecycle inspection failure without issuing start', async () => {
    const first = new Error('inspect timed out')
    let down = false
    const h = harness({
      onRun: (verb) => {
        if (verb === 'kill') down = true
        if (verb === 'inspect' && down) throw first
      },
    })
    await expect(h.bouncer.step({ service: 'kafka2', kind: 'kill' }, cluster)).rejects.toBe(first)
    expect(h.controls()).toEqual([['kill', 'kafka2']])
    expect(h.records()[0].diagnosticErrors).toContain('inspect timed out')
  })

  test('records state and logs on a reachability timeout before rethrowing it', async () => {
    const h = harness()
    let probes = 0
    h.bouncer.probe = async () => ++probes === 1
    await expect(h.bouncer.step({ service: 'kafka2', kind: 'stop' }, cluster)).rejects.toThrow(
      'kafka2 did not answer within 120000 ms'
    )
    expect(h.records()[0]).toMatchObject({
      states: { afterStart: restarted },
      logs: { stdout: 'broker log\n' },
    })
    expect(h.bouncer.down.has('kafka2')).toBe(true)
  })

  test('keeps the first error when restore and diagnostics both fail', async () => {
    const first = Object.assign(new Error('kill failed'), { code: 17, stderr: 'kill stderr' })
    let restoring = false
    const h = harness({
      onRun: (verb) => {
        if (verb === 'kill') throw first
        if (verb === 'start') {
          restoring = true
          throw new Error('start failed')
        }
        if (restoring) throw new Error('diagnostics failed')
      },
    })
    await expect(h.bouncer.step({ service: 'kafka2', kind: 'kill' }, cluster)).rejects.toBe(first)
    expect(h.records()[0].commands[0]).toMatchObject({ status: 17, stderr: 'kill stderr' })
    expect(h.records()[0].failures).toEqual(['kill failed', 'start failed'])
    expect(h.records()[0].diagnosticErrors).toHaveLength(2)
    expect(h.bouncer.down.has('kafka2')).toBe(true)
  })

  test('keeps the restore error when writing diagnostics fails', async () => {
    const first = new Error('start failed')
    const h = harness({
      onRun: (verb) => {
        if (verb === 'start') throw first
      },
    })
    const write = jest.spyOn(fs, 'appendFileSync').mockImplementation(() => {
      throw new Error('disk full')
    })
    try {
      await expect(h.bouncer.step({ service: 'kafka2', kind: 'stop' }, cluster)).rejects.toBe(first)
    } finally {
      write.mockRestore()
    }
    expect(h.bouncer.down.has('kafka2')).toBe(true)
  })

  test('bounds captured logs and excludes configuration from inspect evidence', async () => {
    const h = harness({
      afterStart: [
        state({
          ...restarted,
          Running: false,
          Status: 'exited',
          Config: { Env: ['SECRET=hidden'] },
        }),
      ],
    })
    const run = h.bouncer.read
    h.bouncer.read = (...args) =>
      args[0] === 'logs' ? { stdout: 'x'.repeat(300000), stderr: '' } : run(...args)
    await expect(h.bouncer.step({ service: 'kafka2', kind: 'kill' }, cluster)).rejects.toThrow()
    const output = fs.readFileSync(evidenceFile, 'utf8')
    expect(output.length).toBeLessThan(100000)
    expect(output).not.toContain('SECRET')
    expect(h.records()[0].logs.stdout.length).toBeGreaterThan(0)
  })

  test('keeps lifecycle reads out of existing control wrappers', async () => {
    const h = harness()
    const run = h.bouncer.run
    h.bouncer.run = (...args) => {
      if (!['kill', 'start'].includes(args[0])) throw new Error('unexpected control')
      return run(...args)
    }
    await expect(
      h.bouncer.step({ service: 'kafka2', kind: 'kill' }, cluster)
    ).resolves.toMatchObject({ service: 'kafka2' })
    expect(h.controls()).toEqual([
      ['kill', 'kafka2'],
      ['start', 'kafka2'],
    ])
  })

  test('leaves pause and unpause free of restart checks', async () => {
    const h = harness()
    await h.bouncer.step({ service: 'kafka2', kind: 'pause' }, cluster)
    expect(h.commands).toEqual([
      ['pause', 'kafka2'],
      ['unpause', 'kafka2'],
    ])
  })
})

describe('BrokerBouncer compose runner', () => {
  const originalEnv = { ...process.env }
  let execFile
  const load = () => {
    let helpers
    jest.isolateModules(() => {
      jest.doMock('child_process', () => ({ execFile }))
      helpers = require('../leaderElection')
    })
    return new helpers.BrokerBouncer()
  }

  beforeEach(() => {
    delete process.env.KAFKAJS_CLUSTER_GUARD
    delete process.env.KAFKAJS_CLUSTER_MANIFEST
    execFile = jest.fn((file, args, options, callback) => {
      const stdout = args.includes('ps') ? 'a'.repeat(64) + '\n' : JSON.stringify(state())
      callback(null, { stdout, stderr: '' })
    })
  })

  afterEach(() => {
    jest.dontMock('child_process')
    process.env = { ...originalEnv }
  })

  test('bounds state and log commands and inspects only the selected Compose container', async () => {
    const bouncer = load()
    await expect(bouncer.inspect('kafka2')).resolves.toMatchObject(state())
    await bouncer.run('logs', '--timestamps', '--tail', '200', 'kafka2')
    const calls = execFile.mock.calls
    expect(calls).toHaveLength(3)
    for (const [file, args, options] of calls) {
      expect(file).toBe('timeout')
      expect(args.slice(0, 3)).toEqual(['--kill-after=1s', '5s', 'docker'])
      expect(options.maxBuffer).toBe(256 * 1024)
    }
    expect(calls[0][1].slice(-4)).toEqual(['ps', '--all', '--quiet', 'kafka2'])
    expect(calls[1][1].slice(3, 5)).toEqual(['inspect', '--format'])
    expect(calls[1][1].at(-1)).toBe('a'.repeat(64))
    expect(calls[1][1][5]).toContain('.State.OOMKilled')
    expect(calls[1][1][5]).not.toMatch(/Config|Env/)
    expect(calls[2][1].slice(-5)).toEqual(['logs', '--timestamps', '--tail', '200', 'kafka2'])
  })

  test('refuses ambiguous Compose container ids without inspecting any container', async () => {
    execFile.mockImplementation((file, args, options, callback) =>
      callback(null, { stdout: 'a'.repeat(64) + '\n' + 'b'.repeat(64), stderr: '' })
    )
    await expect(load().inspect('kafka2')).rejects.toThrow('Expected one container for kafka2')
    expect(execFile).toHaveBeenCalledTimes(1)
  })

  test('routes reads through the ownership guard and never falls back after a refusal', async () => {
    process.env.KAFKAJS_CLUSTER_GUARD = '/guard.js'
    process.env.KAFKAJS_CLUSTER_MANIFEST = '/manifest.json'
    const first = Object.assign(new Error('guard refused inspection'), { code: 1 })
    execFile.mockImplementation((file, args, options, callback) => callback(first))
    await expect(load().inspect('kafka2')).rejects.toBe(first)
    expect(execFile).toHaveBeenCalledTimes(1)
    expect(execFile.mock.calls[0][1]).toEqual([
      '--kill-after=10s',
      '5s',
      'node',
      '/guard.js',
      'inspect',
      '/manifest.json',
      'kafka2',
    ])
  })
})
