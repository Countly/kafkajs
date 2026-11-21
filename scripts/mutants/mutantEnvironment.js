/**
 * The mutant runner's Jest test environment. It extends the project's own environment
 * (`MUTANT_BASE_ENVIRONMENT`, resolved by the runner from the project's Jest configuration) and
 * records, for every test Jest registers, its full name, the file, line and column of the call that
 * registered it (from the stack Jest captured at that call), whether it is a `.failing` test, and the
 * lifecycle events Jest then dispatched for that very test object: `test_start`, `test_fn_start`,
 * `test_fn_failure` (`:timeout` for Jest's own timeout, `:load` when Jest could not load a module the
 * body required: a parse, transform or resolution failure), `test_fn_success`, `test_retry`, `test_done`
 * and `hook_failure:<type>` (`:timeout` for Jest's own hook timeout). Each test carries
 * `hookTimeoutsRecorded: true`. Events that belong to no test (`error`, `describe_retry`, `*All` hook
 * failures) go to `run`. The record lives in memory and is written once, at teardown, to
 * `MUTANT_EVIDENCE_FILE` with the per-run `MUTANT_NONCE` the runner chose.
 */
const fs = require('fs')

const base = require(process.env.MUTANT_BASE_ENVIRONMENT)
const Base = base.TestEnvironment || base.default || base

const FRAME = /^\s*at (?:.*? \()?(.+?):(\d+):(\d+)\)?$/
const INTERNAL = /\/jest-(?:each|circus)\/build\//
const LOADER = /\/jest-runtime\/build\//
const TEST_EVENTS = new Set([
  'test_start',
  'test_fn_start',
  'test_fn_success',
  'test_retry',
  'test_done',
])

/**
 * @param {string} stack an error stack captured by Jest at a registration call
 * @returns {{ file: string, line: number, column: number }|null} the first frame outside Jest's own code
 */
const registrationFrame = (stack) => {
  for (const text of String(stack).split('\n').slice(1)) {
    const m = FRAME.exec(text)
    if (m && !INTERNAL.test(m[1])) return { file: m[1], line: Number(m[2]), column: Number(m[3]) }
  }
  return null
}

/**
 * Tells a failure of Jest's module loading from an error the code under test threw: a transform
 * (Babel) parse error, a module Jest's resolver cannot find, or a SyntaxError whose first frame
 * outside Node is Jest's loader (a JSON module or an untransformed script). A SyntaxError the code
 * itself raises, such as `JSON.parse` on bad input, stays an ordinary failure.
 * @param {unknown} error the body's error
 * @returns {boolean} whether the body failed loading a module
 */
const loadFailure = (error) => {
  if (!error || typeof error !== 'object') return false
  if (error.code === 'BABEL_PARSE_ERROR' || error.code === 'MODULE_NOT_FOUND') return true
  if (error.name !== 'SyntaxError') return false
  const frames = String(error.stack)
    .split('\n')
    .filter((l) => FRAME.test(l) && !/\((?:node:|<anonymous>|native)/.test(l))
  return LOADER.test(frames[0] || '')
}

/** The project's environment, plus a private record of each test's registration and lifecycle events. */
class EventEnvironment extends Base {
  /**
   * @param {object} config Jest's project and global configuration
   * @param {object} context the test file's context
   */
  constructor(config, context) {
    super(config, context)
    this.tests = new Map()
    this.run = []
  }

  /**
   * @param {{ name: string, test?: object, hook?: { type: string }, error?: unknown, testName?: string, asyncError?: Error }} event a Jest circus event
   * @param {{ currentDescribeBlock: { name: string, parent?: object, children: object[] } }} state the circus state
   * @returns {unknown} whatever the project's own handler returns
   */
  handleTestEvent(event, state) {
    const result = super.handleTestEvent?.(event, state)
    const { name, test } = event
    if (name === 'add_test') {
      const added = state.currentDescribeBlock.children.at(-1)
      const names = [event.testName]
      for (let block = state.currentDescribeBlock; block?.parent; block = block.parent)
        names.unshift(block.name)
      if (added?.asyncError === event.asyncError)
        this.tests.set(added, {
          fullName: names.join(' '),
          ...registrationFrame(event.asyncError?.stack),
          failing: Boolean(added.failing),
          hookTimeoutsRecorded: true,
          events: [],
        })
    } else if (name === 'test_fn_failure') {
      const timeout = typeof event.error === 'string' && event.error.startsWith('Exceeded timeout')
      const label = timeout ? ':timeout' : loadFailure(event.error) ? ':load' : ''
      this.tests.get(test)?.events.push(`test_fn_failure${label}`)
    } else if (name === 'hook_failure') {
      const timeout = typeof event.error === 'string' && event.error.startsWith('Exceeded timeout')
      const label = `hook_failure:${event.hook?.type}${timeout ? ':timeout' : ''}`
      const record = this.tests.get(test)
      if (record) record.events.push(label)
      else this.run.push(label)
    } else if (TEST_EVENTS.has(name)) {
      this.tests.get(test)?.events.push(name)
    } else if (name === 'error' || name === 'describe_retry') {
      this.run.push(name)
    }
    return result
  }

  /**
   * Writes the evidence from private state, then lets the project's environment tear down.
   * @returns {Promise<void>}
   */
  async teardown() {
    if (process.env.MUTANT_EVIDENCE_FILE) {
      fs.writeFileSync(
        process.env.MUTANT_EVIDENCE_FILE,
        JSON.stringify({
          nonce: process.env.MUTANT_NONCE,
          tests: [...this.tests.values()],
          run: this.run,
        })
      )
    }
    await super.teardown()
  }
}

module.exports = EventEnvironment
