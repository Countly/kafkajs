const fs = require('node:fs')
const path = require('node:path')
const yaml = require('js-yaml')
const vm = require('node:vm')

const WORKFLOWS_DIR = path.join(__dirname, '..', '..', '..', '.github', 'workflows')
const PENDING_UNTIL_PR_15_MERGES = ['memory-gates.yml']

describe('workflow run expressions', () => {
  test('no run script interpolates a workflow expression outside pending PR merges', () => {
    const files = fs.readdirSync(WORKFLOWS_DIR).filter((file) => /\.ya?ml$/.test(file))
    const violations = []

    for (const file of files) {
      const workflow = yaml.load(fs.readFileSync(path.join(WORKFLOWS_DIR, file), 'utf8'))
      if (PENDING_UNTIL_PR_15_MERGES.includes(file)) continue
      for (const [job, config] of Object.entries(workflow.jobs ?? {})) {
        for (const [index, step] of (config.steps ?? []).entries()) {
          if (typeof step.run === 'string' && step.run.includes('${{'))
            violations.push(`${file}#${job}:${step.name ?? step.id ?? index}`)
        }
      }
    }

    expect(files.length).toBeGreaterThan(0)
    expect(violations).toEqual([])
  })
})

describe('long-window acceptance workflow', () => {
  const workflow = () =>
    yaml.load(fs.readFileSync(path.join(WORKFLOWS_DIR, 'memory-gates.yml'), 'utf8'))

  test('selects three replicates on pull requests and by default on dispatch', () => {
    const config = workflow()
    const job = config.jobs['telemetry-long-window']
    expect(job.if).not.toMatch(/workflow_dispatch/)
    expect(config.on.workflow_dispatch.inputs.replicates.default).toBe('3')
    const expression = job.strategy.matrix.replicate.slice(3, -3)
    const evaluate = (replicates) => {
      const github = { event: { inputs: { replicates } } }
      return vm.runInNewContext(expression, { github, fromJSON: JSON.parse })
    }
    expect(evaluate(undefined)).toEqual([1, 2, 3])
    expect(evaluate('3')).toEqual([1, 2, 3])
    expect(evaluate('2')).toEqual([1, 2])
    expect(evaluate('1')).toEqual([1])
    const driver = job.steps.find((step) => step.run?.includes('longWindowDiagnostic.js'))
    expect(driver.run).toContain('--acceptance')
    expect(driver.run).toContain('--diagnostic-only')
  })

  test('requires the long-window artifact verdict and retires the five-minute seeded requirement', () => {
    const config = workflow()
    const job = config.jobs['telemetry-long-window-verdict']
    expect(job).toBeDefined()
    expect(job.needs).toEqual(['select', 'telemetry-long-window'])
    expect(job.if).toContain('always()')
    expect(job.if).toContain('telemetry-long-window-verdict')
    const download = job.steps.find((step) => step.uses?.startsWith('actions/download-artifact@'))
    expect(download.with.pattern).toBe('telemetry-long-window-node*')
    expect(download.with['merge-multiple']).not.toBe(true)
    const run = job.steps.find((step) => step.run?.includes('longWindowReport.js')).run
    for (const flag of ['--verdict', '--tree', '--run', '--attempt']) expect(run).toContain(flag)
    expect(run).not.toContain('--replay-diagnostic')
    expect(config.jobs.verdict.needs).toContain('telemetry-long-window-verdict')
    expect(config.jobs.verdict.needs).not.toContain('telemetry-seeded-control')
    const select = config.jobs.select.steps[0].run
    expect(select).toContain("['telemetry-long-window', 'telemetry-long-window-verdict']")
  })
})
