const fs = require('node:fs')
const path = require('node:path')
const yaml = require('js-yaml')
const { integrationPlan } = require('../integrationPlan')

const WORKFLOWS_DIR = path.join(__dirname, '..', '..', '..', '.github', 'workflows')
const LOCAL_PREFIX = './.github/workflows/'
const MAX_TIMEOUT_MINUTES = 360

/**
 * Parses every workflow file in the repository.
 * @returns {Object<string, any>} documents keyed by file name
 */
const loadWorkflows = () =>
  Object.fromEntries(
    fs
      .readdirSync(WORKFLOWS_DIR)
      .filter((file) => /\.ya?ml$/.test(file))
      .map((file) => [file, yaml.load(fs.readFileSync(path.join(WORKFLOWS_DIR, file), 'utf8'))])
  )

/**
 * @param {Object<string, string>} sources workflow text keyed by file name
 * @returns {Object<string, any>} parsed documents keyed by file name
 */
const parseAll = (sources) =>
  Object.fromEntries(Object.entries(sources).map(([file, text]) => [file, yaml.load(text)]))

/**
 * @param {unknown} value a job's `timeout-minutes`
 * @returns {boolean} true for an integer from 1 to 360, or an expression
 */
const isValidTimeout = (value) =>
  (Number.isInteger(value) && value >= 1 && value <= MAX_TIMEOUT_MINUTES) ||
  (typeof value === 'string' && value.includes('${{'))

/**
 * Finds runner-executed jobs without a valid timeout. A `uses:` caller job is exempt itself:
 * it is reported when its callee cannot be resolved or the callee has a failing job.
 * @param {string} file workflow to check
 * @param {Object<string, any>} workflowsByName parsed workflows keyed by file name
 * @returns {string[]} `<file>#<job>` entries
 */
const jobsMissingTimeout = (file, workflowsByName) =>
  Object.entries(workflowsByName[file].jobs ?? {}).flatMap(([name, job]) => {
    const id = `${file}#${name}`
    if (job.uses === undefined) return isValidTimeout(job['timeout-minutes']) ? [] : [id]
    const callee = String(job.uses).startsWith(LOCAL_PREFIX)
      ? job.uses.slice(LOCAL_PREFIX.length)
      : ''
    if (!Object.hasOwn(workflowsByName, callee)) return [id]
    const inner = jobsMissingTimeout(callee, workflowsByName)
    return inner.length > 0 ? [id, ...inner] : []
  })

describe('workflow job timeouts', () => {
  const workflows = loadWorkflows()

  test('every runner-executed job sets timeout-minutes', () => {
    const files = Object.keys(workflows)
    const missing = files.flatMap((file) => jobsMissingTimeout(file, workflows))

    expect(files.length).toBeGreaterThan(0)
    expect(missing).toEqual([])
  })

  test('weekly integration uses the CI timeout expression and shared-group overrides', () => {
    const ci = workflows['ci.yml'].jobs.integration
    const weekly = workflows['weekly.yml'].jobs.integration
    const matrix = weekly.strategy.matrix
    const overrides = integrationPlan('push').include.filter(
      (row) => matrix.group.includes(row.group) && row.timeout !== undefined
    )

    expect(ci['timeout-minutes']).toBe(`\${{ matrix.timeout || 20 }}`)
    expect(weekly['timeout-minutes']).toBe(ci['timeout-minutes'])
    expect(matrix.include ?? []).toEqual(overrides)
  })

  test('a reusable-workflow caller job is exempt and its callee is checked', () => {
    for (const workflow of Object.values(workflows)) {
      for (const job of Object.values(workflow.jobs ?? {})) {
        if (job.uses === undefined) continue
        expect(job.uses).toMatch(/^\.\/\.github\/workflows\/[^/]+$/)
        const callee = job.uses.slice(LOCAL_PREFIX.length)
        expect(Object.keys(workflows)).toContain(callee)
        expect(jobsMissingTimeout(callee, workflows)).toEqual([])
      }
    }

    const calleeJobs = (timeout) =>
      `on: workflow_call\njobs:\n  work:\n    runs-on: ubuntu-latest\n${timeout}`
    const caller = 'on: push\njobs:\n  call:\n    uses: ./.github/workflows/callee.yml\n'
    const good = parseAll({
      'caller.yml': caller,
      'callee.yml': calleeJobs('    timeout-minutes: 5\n'),
    })
    const bad = parseAll({ 'caller.yml': caller, 'callee.yml': calleeJobs('') })

    expect(jobsMissingTimeout('caller.yml', good)).toEqual([])
    expect(jobsMissingTimeout('caller.yml', bad)).toEqual(['caller.yml#call', 'callee.yml#work'])
  })

  test('fails on a reusable-workflow call whose callee cannot be resolved', () => {
    const byName = parseAll({
      'inline.yml': [
        'on: push',
        'jobs:',
        '  absent:',
        '    uses: ./.github/workflows/missing.yml',
        '  remote:',
        '    uses: owner/repo/.github/workflows/x.yml@0123456789abcdef',
        '',
      ].join('\n'),
    })

    expect(jobsMissingTimeout('inline.yml', byName)).toEqual([
      'inline.yml#absent',
      'inline.yml#remote',
    ])
  })

  test('rejects a job with no timeout (negative control)', () => {
    const byName = parseAll({
      'inline.yml': [
        'on: push',
        'jobs:',
        '  timed:',
        '    runs-on: ubuntu-latest',
        '    timeout-minutes: 10',
        '  untimed:',
        '    runs-on: ubuntu-latest',
        '',
      ].join('\n'),
    })

    expect(jobsMissingTimeout('inline.yml', byName)).toEqual(['inline.yml#untimed'])
  })

  test('rejects a timeout of 0 or above 360', () => {
    const jobs = { zero: 0, over: 361, one: 1, cap: 360, expression: `'\${{ matrix.cap }}'` }
    const body = Object.entries(jobs)
      .map(
        ([name, value]) => `  ${name}:\n    runs-on: ubuntu-latest\n    timeout-minutes: ${value}\n`
      )
      .join('')
    const byName = parseAll({ 'inline.yml': `on: push\njobs:\n${body}` })

    expect(jobsMissingTimeout('inline.yml', byName)).toEqual(['inline.yml#zero', 'inline.yml#over'])
  })
})

const root = path.resolve(__dirname, '../../..')
const workflows = path.join(root, '.github/workflows')

describe('Workflow timeouts', () => {
  test('every runner job has a timeout and reusable jobs call bounded local workflows', () => {
    for (const file of fs.readdirSync(workflows).filter((file) => /\.ya?ml$/.test(file))) {
      const { jobs } = yaml.load(fs.readFileSync(path.join(workflows, file), 'utf8'))
      for (const job of Object.values(jobs)) {
        if (job.uses) {
          expect(job.uses).toMatch(/^\.\/\.github\/workflows\//)
          expect(fs.existsSync(path.join(root, job.uses))).toBe(true)
        } else {
          const cap = job['timeout-minutes']
          expect(cap).toBeDefined()
          if (typeof cap === 'number')
            expect(Number.isInteger(cap) && cap > 0 && cap <= 360).toBe(true)
          else expect(cap).toMatch(/^\$\{\{.+\}\}$/)
        }
      }
    }
  })
})
