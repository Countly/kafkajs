const fs = require('node:fs')
const path = require('node:path')
// eslint-disable-next-line n/no-unsupported-features/node-builtins -- Native resource creation is the contract under test.
const { createHook } = require('node:async_hooks')
// eslint-disable-next-line n/no-extraneous-require -- YAML parser is pinned by the ESLint/Jest toolchain
const { load } = require('js-yaml')
const { childArgs } = require('../shareSoakContract')
const { runNaturalExit } = require('../naturalExit')

const root = path.resolve(__dirname, '../../..')
const reporter = 'testHelpers/quietReporter.js'
const flag = `--reporters=./${reporter}`
const scripts = JSON.parse(fs.readFileSync(path.join(root, 'package.json'), 'utf8')).scripts

describe('resource measurement reporter', () => {
  test('uses only the timer-free reporter in measured package commands', () => {
    for (const name of [
      'test:unit:soak',
      'test:group:share-shutdown',
      'test:group:share-soak',
      'test:group:telemetry-shutdown',
      'test:group:telemetry-soak',
      'test:group:telemetry-seeded-control',
    ]) {
      expect({ name, reporters: scripts[name].match(/--reporters[= ][^ ]+/g) }).toEqual({
        name,
        reporters: [flag],
      })
    }
  })

  test('propagates the timer-free reporter through guarded telemetry and share commands', () => {
    const guarded = Object.entries(scripts).filter(
      ([name]) =>
        name.startsWith('test:e2e:telemetry-') ||
        ['test:e2e:share-soak', 'test:e2e:share-shutdown'].includes(name)
    )
    expect(guarded).toHaveLength(11)
    for (const [name, command] of guarded) {
      expect({ name, configured: command.startsWith('KAFKAJS_RESOURCE_REPORTER=1 ') }).toEqual({
        name,
        configured: true,
      })
    }
  })

  test('selects only the timer-free reporter when the guard environment requests it', () => {
    const previous = process.env.KAFKAJS_RESOURCE_REPORTER
    try {
      process.env.KAFKAJS_RESOURCE_REPORTER = '1'
      jest.isolateModules(() => {
        expect(require(path.join(root, 'jest.config.js')).reporters).toEqual([
          `<rootDir>/${reporter}`,
        ])
      })
    } finally {
      if (previous === undefined) delete process.env.KAFKAJS_RESOURCE_REPORTER
      else process.env.KAFKAJS_RESOURCE_REPORTER = previous
    }
  })

  test('uses only the timer-free reporter in every in-band telemetry workflow command', () => {
    const commands = []
    for (const file of ['telemetry.yml', 'memory-gates.yml']) {
      const workflow = load(fs.readFileSync(path.join(root, '.github/workflows', file), 'utf8'))
      for (const job of Object.values(workflow.jobs)) {
        for (const step of job.steps || []) {
          for (const command of (step.run || '').split('\n')) {
            if (/\bjest\b/.test(command) && command.includes('--runInBand'))
              commands.push({ file, command })
          }
        }
      }
    }
    expect(commands).toHaveLength(4)
    for (const { file, command } of commands) {
      expect({ file, reporters: command.match(/--reporters[= ][^ ]+/g) }).toEqual({
        file,
        reporters: [flag],
      })
    }
    const controls = commands.find(({ command }) => command.includes('memoryVerdicts.spec.js'))
    expect(controls.command).toContain('--json --outputFile=')
  })

  test('uses only the timer-free reporter in the fixed share children and keeps their JSON report', () => {
    const short = childArgs('short')
    expect(short.filter((arg) => arg.startsWith('--reporters'))).toEqual([flag])
    expect(short).toContain('--json')
    expect(short).toContain('--outputFile=.tmp/e2e/share-soak-jest.json')
    let shutdown
    expect(() =>
      runNaturalExit('shutdown', {
        spawn: (executable, args) => {
          shutdown = args
          return { status: 1, signal: null }
        },
        now: () => 1000,
        writeFile: () => {},
      })
    ).toThrow('unsuccessful child exit')
    expect(shutdown.filter((arg) => arg.startsWith('--reporters'))).toEqual([flag])
  })

  test('prints completed tests and failures without creating Timeout or Immediate resources', () => {
    const created = []
    const hook = createHook({
      init: (id, type) => {
        if (type === 'Timeout' || type === 'Immediate') created.push(type)
      },
    })
    const output = []
    const write = jest.spyOn(process.stderr, 'write').mockImplementation((text) => {
      output.push(text)
      return true
    })
    const failure = 'suite failing case\nError: expected clean shutdown'
    const runtimeFailure = 'Error: fixture startup failed'
    try {
      hook.enable()
      const Reporter = require(path.join(root, reporter))
      const instance = new Reporter()
      const test = { path: 'measured.spec.js' }
      const results = {}
      instance.onRunStart?.(results, { estimatedTime: 0 })
      instance.onTestStart?.(test)
      instance.onTestCaseResult?.(test, { fullName: 'suite passing case', status: 'passed' })
      for (const [status, name, failureMessage, testExecError] of [
        ['passed', 'passing', undefined, undefined],
        ['failed', 'failing', failure, undefined],
        ['pending', 'skipped', undefined, undefined],
        ['failed', 'startup', undefined, { message: runtimeFailure }],
      ]) {
        instance.onTestResult(
          test,
          {
            testFilePath: `${name}.spec.js`,
            numFailingTests: status === 'failed' ? 1 : 0,
            testExecError,
            testResults: testExecError ? [] : [{ status, fullName: `suite ${name} case` }],
            failureMessage,
          },
          results
        )
      }
      instance.onRunComplete?.(new Set(), results)
    } finally {
      hook.disable()
      write.mockRestore()
    }
    expect(created).toEqual([])
    const log = output.join('')
    for (const name of ['passing', 'failing', 'skipped', 'startup'])
      expect(log).toContain(`${name}.spec.js`)
    for (const name of ['passing', 'failing', 'skipped'])
      expect(log).toContain(`suite ${name} case`)
    expect(log).toContain(failure)
    expect(log).toContain(runtimeFailure)
    expect(log).toContain('passed')
    expect(log).toContain('failed')
    expect(log).toContain('pending')
  })
})
