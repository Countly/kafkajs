const fs = require('node:fs')
const path = require('node:path')
const vm = require('node:vm')
const v8 = require('node:v8')
const yaml = require('js-yaml')
const { EventEmitter } = require('node:events')
const { PassThrough } = require('node:stream')
const { MEMORY_FLAGS } = require('../../../testHelpers/processMemory')
const { supervise } = require('../../../scripts/e2e/telemetry/warmupWitness')

const source = (file) => fs.readFileSync(path.join(__dirname, 'fixtures', file), 'utf8')
const load = () => require('./fixtures/controlDiagnostics')

afterEach(() => jest.restoreAllMocks())

describe('resource control attribution diagnostics', () => {
  test('records heap spaces, heap capacity, code, memory and raw maxRSS without collecting', () => {
    const spaces = [
      {
        space_name: 'old_space',
        space_size: 11,
        space_used_size: 7,
        space_available_size: 3,
        physical_space_size: 10,
      },
    ]
    const heap = {
      total_heap_size: 11,
      total_physical_size: 10,
      malloced_memory: 2,
      peak_malloced_memory: 4,
      external_memory: 5,
      ignored: 99,
    }
    const code = {
      code_and_metadata_size: 6,
      bytecode_and_metadata_size: 8,
      external_script_source_size: 9,
    }
    const memory = { rss: 20, heapTotal: 11, heapUsed: 7, external: 5, arrayBuffers: 1 }
    jest.spyOn(v8, 'getHeapSpaceStatistics').mockReturnValue(spaces)
    jest.spyOn(v8, 'getHeapStatistics').mockReturnValue(heap)
    jest.spyOn(v8, 'getHeapCodeStatistics').mockReturnValue(code)
    jest.spyOn(process, 'memoryUsage').mockReturnValue(memory)
    jest.spyOn(process, 'resourceUsage').mockReturnValue({ maxRSS: 123 })
    expect(load()()).toEqual({
      heapSpaces: spaces,
      heap: {
        total_heap_size: 11,
        total_physical_size: 10,
        malloced_memory: 2,
        peak_malloced_memory: 4,
        external_memory: 5,
      },
      code,
      memory,
      maxRSS: 123,
    })
  })

  test.each(['producer', 'consumer', 'admin', 'shareConsumer'])(
    'records C0 and C1 after the original readings without changing control order for %s',
    async (kind) => {
      const text = source('memoryForeground.js')
      const start = text.indexOf('const runControl = async')
      const end = text.indexOf('/**', start)
      const events = []
      const c0 = { at: 1, rss: 100, heapUsed: 20 }
      const c1 = { at: 2, rss: 101, heapUsed: 21 }
      const released = { at: 3, rss: 99, heapUsed: 19 }
      const readings = [c0, c1, released]
      const original = JSON.stringify(readings)
      const diagnostics = []
      const controlDiagnostics = () => {
        events.push('diagnostics')
        const value = { memory: { rss: 999 }, maxRSS: 123 }
        diagnostics.push(value)
        return value
      }
      const foreground = {
        build: () => foreground,
        open: async () => events.push('open'),
        close: async () => events.push('close'),
        drain: async () => events.push('drain'),
        release: () => events.push('release'),
        facts: () => ({ enabled: false }),
        client: {
          clientInstanceId: async () => {
            throw Object.assign(new Error('disabled'), { reason: 'disabled' })
          },
        },
      }
      const result = await vm.runInNewContext(
        `(async () => { ${text.slice(start, end)}
        return runControl(input) })()`,
        {
          controlDiagnostics,
          MemoryForeground: function (name, options) {
            expect(name).toBe(kind)
            expect(options).toEqual({ enabled: false })
            return foreground
          },
          setImmediate: (fn) => {
            events.push('turn')
            fn()
          },
          input: {
            kind,
            lifecycles: 3,
            live: [],
            journal: { rows: [] },
            roles: { run: (_role, fn) => fn(), read: () => ({}) },
            memory: {
              mark: (name) => events.push(name),
              settled: async () => {
                events.push('settled')
                return readings.shift()
              },
            },
          },
        }
      )
      expect(result.c0).toBe(c0)
      expect(result.c1).toBe(c1)
      expect(result.released).toBe(released)
      expect(JSON.stringify([c0, c1, released])).toBe(original)
      expect(result.diagnostics).toEqual({ c0: diagnostics[0], c1: diagnostics[1] })
      expect(diagnostics).toHaveLength(2)
      expect(events).toEqual([
        'control:start',
        'open',
        'settled',
        'diagnostics',
        'control:c0',
        'close',
        'drain',
        'open',
        'close',
        'drain',
        'open',
        'settled',
        'diagnostics',
        'control:c1',
        'close',
        'drain',
        'turn',
        'turn',
        'release',
        'settled',
        'control:released',
      ])
    }
  )

  test.each(['publicMemoryChild.js', 'resourceChild.js'])(
    'publishes separate control diagnostics in %s and marks flag runs ineligible',
    (file) => {
      const text = source(file)
      expect(text).toContain('controlDiagnostics: control.diagnostics')
      expect(text).toContain("process.execArgv.includes('--no-flush-bytecode')")
      const start = text.indexOf('const readings = {')
      const end = text.indexOf('startup,', start)
      const prefix = text.slice(start, end)
      expect(prefix).toContain('acceptanceEligible: false')
      expect(prefix).toContain('diagnostic: true')
    }
  )

  test('launches the no-flush comparison on the original share children with bounded supervision', async () => {
    const calls = []
    const launch = (file, args, options) => {
      calls.push({ file, args, options })
      const child = new EventEmitter()
      child.pid = 123
      child.stdout = new PassThrough()
      child.stderr = new PassThrough()
      child.kill = jest.fn()
      setImmediate(() => {
        if (args.at(-2).endsWith('publicMemoryChild.js'))
          child.stdout.write(JSON.stringify({ receipt: 'disconnect', childId: 'child' }) + '\n')
        child.stdout.end(
          JSON.stringify({ readings: { manifest: { execArgv: args.slice(0, -2) } } }) + '\n'
        )
        child.emit('close', 0, null)
      })
      return child
    }
    for (const arm of ['public', 'collector']) {
      const run = await supervise(
        {
          arm,
          kind: 'shareConsumer',
          mode: 'unseeded',
          variant: 'no-flush-bytecode',
          childId: 'child',
        },
        launch
      )
      expect(run).toMatchObject({ diagnostic: true, acceptanceEligible: false, failure: null })
      expect(run.readings.manifest.execArgv).toEqual([...MEMORY_FLAGS, '--no-flush-bytecode'])
    }
    expect(calls.map((call) => path.basename(call.args.at(-2)))).toEqual([
      'publicMemoryChild.js',
      'resourceChild.js',
    ])
    expect(calls.every((call) => call.args.at(-1) === 'shareConsumer')).toBe(true)
  })

  test('archives four serial original controls and their exact C1 minus C0 RSS comparison', async () => {
    const parent = path.join(process.cwd(), '.tmp')
    fs.mkdirSync(parent, { recursive: true })
    const directory = fs.mkdtempSync(path.join(parent, 'control-diagnostic-'))
    fs.rmdirSync(directory)
    const calls = []
    const run = async (identity) => {
      calls.push(identity)
      return {
        diagnostic: true,
        acceptanceEligible: false,
        identity,
        code: 0,
        signal: null,
        timedOut: false,
        failure: null,
        stderrBytes: 0,
        readings: {
          c0: { rss: 100 },
          c1: { rss: identity.variant === 'scalar' ? 130 : 110 },
          controlDiagnostics: { c0: { maxRSS: 1 }, c1: { maxRSS: 2 } },
        },
      }
    }
    try {
      const { collect } = require('../../../scripts/e2e/telemetry/controlDiagnostic')
      await collect(directory, { tree: 'tree', runId: 'run' }, run)
      expect(calls.map(({ arm, variant }) => [arm, variant])).toEqual([
        ['public', 'scalar'],
        ['public', 'no-flush-bytecode'],
        ['collector', 'scalar'],
        ['collector', 'no-flush-bytecode'],
      ])
      expect(new Set(calls.map((row) => row.childId)).size).toBe(4)
      expect(calls.every((row) => row.kind === 'shareConsumer' && row.mode === 'unseeded')).toBe(
        true
      )
      const summary = JSON.parse(fs.readFileSync(path.join(directory, 'summary.json')))
      expect(summary).toMatchObject({ diagnostic: true, acceptanceEligible: false })
      for (const arm of ['public', 'collector']) {
        expect(summary.comparisons[arm]).toEqual({
          scalarRssDelta: 30,
          noFlushRssDelta: 10,
          difference: -20,
        })
        for (const variant of ['scalar', 'no-flush-bytecode']) {
          const record = JSON.parse(fs.readFileSync(path.join(directory, `${arm}-${variant}.json`)))
          expect(record.acceptanceEligible).toBe(false)
          expect(record.readings.controlDiagnostics.c1.maxRSS).toBe(2)
        }
      }
    } finally {
      fs.rmSync(directory, { recursive: true, force: true })
    }
  })

  test('bounds the dispatch-only control job, selects it with only and excludes it from verdict needs', () => {
    const { jobs } = yaml.load(fs.readFileSync('.github/workflows/memory-gates.yml', 'utf8'))
    const id = 'resource-control-diagnostic'
    const job = jobs[id]
    expect(job).toBeDefined()
    expect(job.if).toContain("github.event_name == 'workflow_dispatch'")
    expect(job.if).toContain(`'${id}'`)
    expect(job['timeout-minutes']).toBeLessThanOrEqual(15)
    expect(job.strategy.matrix.node).toEqual([22, 24, 26])
    expect(jobs.select.steps[0].run).toContain(`'${id}'`)
    expect(jobs.verdict.needs).not.toContain(id)
    const run = job.steps.find((step) => step.run?.includes('controlDiagnostic.js'))
    expect(run.run).toContain('timeout -k 10s 420s')
    expect(run.run).toContain('--diagnostic-only')
    for (const step of job.steps) expect(step['timeout-minutes']).toBeGreaterThan(0)
    const upload = job.steps.find((step) => step.uses?.startsWith('actions/upload-artifact'))
    expect(upload.if).toBe('always()')
    expect(upload.with.name).toContain('resource-control-diagnostic')
  })
})
