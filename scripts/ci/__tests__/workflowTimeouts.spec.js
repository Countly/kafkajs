const fs = require('fs')
const path = require('path')
// eslint-disable-next-line n/no-extraneous-require -- provided by the toolchain
const yaml = require('js-yaml')

const root = path.resolve(__dirname, '../../..')
const workflows = path.join(root, '.github/workflows')

describe('Workflow timeouts', () => {
  test('bounds the isolated share peak arms and keeps expressions outside shell commands', () => {
    const workflow = yaml.load(fs.readFileSync(path.join(workflows, 'memory-gates.yml'), 'utf8'))
    const job = workflow.jobs['share-peak']
    expect(job['timeout-minutes']).toBe(30)
    expect(job.strategy.matrix).toEqual({
      node: [22],
      kafka: ['4.3.1', '4.2.2'],
      arm: ['enabled', 'disabled'],
    })
    // eslint-disable-next-line no-template-curly-in-string -- GitHub expression is literal YAML data
    expect(job.env.SHARE_PEAK_ARM).toBe('${{ matrix.arm }}')
    for (const step of job.steps) {
      if (step.run) {
        expect(step.run).not.toContain('${{')
        expect(step['timeout-minutes']).toBeGreaterThan(0)
      }
    }
    expect(job.steps.some((step) => step.run?.includes('sharePeakDiagnostic.js'))).toBe(true)
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
