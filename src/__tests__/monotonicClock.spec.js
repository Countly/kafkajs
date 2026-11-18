const path = require('node:path')
const { execFileSync } = require('node:child_process')
const { performance: importedClock } = require('node:perf_hooks')
const RebalanceHooks = require('../consumer/rebalanceHooks')
const ClientTelemetry = require('../telemetry/client')
const TelemetryMetrics = require('../telemetry/metrics')
const { TelemetryFactory } = require('../telemetry/factory')

const ROOT = path.resolve(__dirname, '../..')
const ESLINT = path.join(path.dirname(require.resolve('eslint/package.json')), 'bin/eslint.js')
const WORKER_AGE_MS = 600001

/**
 * Puts the imported `node:perf_hooks` clock ten minutes ahead, as in a Jest worker that has run
 * many suites. Jest's fake timers replace only the global `performance`, so a module still reading
 * the imported one sees this age instead of fake time. Callers restore with `jest.restoreAllMocks()`.
 * @returns {void}
 */
const ageWorker = () => {
  const real = importedClock.now.bind(importedClock)
  jest.spyOn(importedClock, 'now').mockImplementation(() => real() + WORKER_AGE_MS)
}

/**
 * Lints one source text with the repository's ESLint configuration.
 * @param {string} filePath repository-relative path the text is linted as
 * @param {string} source the text
 * @returns {string[]} the rule ids reported
 */
const lintRules = (filePath, source) => {
  let output
  try {
    output = execFileSync(
      process.execPath,
      [ESLINT, '--format', 'json', '--stdin', '--stdin-filename', filePath],
      { cwd: ROOT, input: source, encoding: 'utf8' }
    )
  } catch (error) {
    if (error.status !== 1) throw error
    output = error.stdout
  }
  return JSON.parse(output)[0].messages.map(({ ruleId }) => ruleId)
}

describe('One monotonic clock under fake timers on a long-lived worker', () => {
  beforeEach(ageWorker)
  afterEach(() => {
    jest.useRealTimers()
    jest.restoreAllMocks()
  })

  test('a rebalance hook deadline is measured on the faked global clock', async () => {
    jest.useFakeTimers({ doNotFake: ['setImmediate'] })
    expect(performance.now()).toBeLessThan(WORKER_AGE_MS)
    const hook = jest.fn(async () => {})
    const hooks = new RebalanceHooks(
      { onPartitionsAssigned: hook },
      { isCurrent: () => true, onExpire: jest.fn() }
    )
    await hooks.invoke(
      'onPartitionsAssigned',
      {},
      { deadlineAt: performance.now() + 60000, identity: { acquisitions: [] } }
    )
    expect(hook).toHaveBeenCalledTimes(1)
    expect(jest.getTimerCount()).toBe(0)
  })

  test('the default telemetry clocks read the faked global clock and arm faked timers', () => {
    jest.useFakeTimers()
    const factory = new TelemetryFactory({
      kind: 'producer',
      config: { enableMetricsPush: false, telemetryMaxBytes: 1048576 },
    })
    const client = new ClientTelemetry({
      kind: 'producer',
      metrics: new TelemetryMetrics({ kind: 'producer' }),
      transport: { candidates: () => [] },
    })
    for (const { clock } of [factory, client]) {
      expect(clock.now()).toBe(performance.now())
      const fired = jest.fn()
      clock.setTimeout(fired, 50)
      const cleared = clock.setTimeout(fired, 50)
      expect(jest.getTimerCount()).toBe(2)
      clock.clearTimeout(cleared)
      jest.advanceTimersByTime(50)
      expect(fired).toHaveBeenCalledTimes(1)
      expect(clock.now()).toBe(performance.now())
      expect(jest.getTimerCount()).toBe(0)
    }
  })
})

describe('Lint forbids the imported clock in client code', () => {
  test.each(["require('node:perf_hooks')", "require('perf_hooks')"])(
    'rejects %s under src/',
    (call) => {
      const source = `const { performance } = ${call}\n\nmodule.exports = () => performance.now()\n`
      expect(lintRules('src/consumer/clockProbe.js', source)).toEqual(['n/no-restricted-require'])
      expect(lintRules('src/consumer/__tests__/clockProbe.spec.js', source)).toEqual([])
    },
    30000
  )
})
