const fs = require('node:fs')
const path = require('node:path')
const yaml = require('js-yaml')

const WORKFLOWS_DIR = path.join(__dirname, '..', '..', '..', '.github', 'workflows')
const PENDING_UNTIL_PRS_15_AND_16_MERGE = ['gate.yml', 'memory-gates.yml']

describe('workflow run expressions', () => {
  test('no run script interpolates a workflow expression outside pending PR merges', () => {
    const files = fs.readdirSync(WORKFLOWS_DIR).filter((file) => /\.ya?ml$/.test(file))
    const violations = []

    for (const file of files) {
      const workflow = yaml.load(fs.readFileSync(path.join(WORKFLOWS_DIR, file), 'utf8'))
      if (PENDING_UNTIL_PRS_15_AND_16_MERGE.includes(file)) continue
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
