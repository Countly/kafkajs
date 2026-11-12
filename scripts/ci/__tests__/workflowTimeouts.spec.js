const fs = require('fs')
const path = require('path')
// eslint-disable-next-line n/no-extraneous-require -- provided by the toolchain
const yaml = require('js-yaml')

const root = path.resolve(__dirname, '../../..')
const workflows = path.join(root, '.github/workflows')

describe('Workflow timeouts', () => {
  test('warm-up witness jobs bound serial original-child pairs on Node 24 and 26', () => {
    const { jobs } = yaml.load(fs.readFileSync(path.join(workflows, 'memory-gates.yml'), 'utf8'))
    const job = jobs['warmup-witnesses']
    expect(job.strategy.matrix.node).toEqual([24, 26])
    expect(job.strategy['fail-fast']).toBe(false)
    expect(job['timeout-minutes']).toBe(110)
    const run = job.steps.find((step) => step.name === 'Record paired warm-up witnesses')
    expect(run['timeout-minutes']).toBe(100)
    expect(run.run).toContain('timeout -k 10s 5900s node scripts/e2e/telemetry/warmupWitness.js')
    expect(job.steps.some((step) => /docker|compose|java|jest/.test(step.run || ''))).toBe(false)
    for (const step of job.steps) if (step.run) expect(step.run).not.toContain('$' + '{{')
    expect(job.steps.find((step) => step.uses === 'actions/upload-artifact@v4').if).toBe('always()')
    expect(jobs.verdict.needs).toContain('warmup-witnesses')
  })

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
