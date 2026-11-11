const fs = require('node:fs')
const path = require('node:path')
const yaml = require('js-yaml')

const WORKFLOW_FILE = path.join(__dirname, '..', '..', '..', '.github', 'workflows', 'bench.yml')
const raw = fs.readFileSync(WORKFLOW_FILE, 'utf8')
const workflow = yaml.load(raw)
const jobs = Object.values(workflow.jobs)
const steps = jobs.flatMap((job) => job.steps)
const runs = (step) => step.run ?? ''
const stepWith = (text) => steps.find((step) => runs(step).includes(text))
const uploadStep = () => steps.find((step) => step.uses?.startsWith('actions/upload-artifact@'))
/** A GitHub Actions expression, as it is written in the workflow file. */
const expr = (inner) => `\${{ ${inner} }}`

describe('bench.yml', () => {
  test('starts the cluster with NO_LOGS so the startup script returns', () => {
    const starts = steps.filter((step) => runs(step).includes('dockerComposeUp.sh'))
    const startupScript = fs.readFileSync(
      path.join(__dirname, '..', '..', 'dockerComposeUp.sh'),
      'utf8'
    )

    expect(starts).toHaveLength(1)
    expect(startupScript).toMatch(/if \[ -z "\$\{NO_LOGS\}" \]/)
    for (const step of starts) {
      const noLogs = step.env?.NO_LOGS ?? runs(step).match(/\bNO_LOGS=(\S+)/)?.[1]
      expect(String(noLogs ?? '')).not.toBe('')
    }
  })

  test('is dispatch-only with read-only contents permission and a timeout', () => {
    expect(workflow.on).toEqual({ workflow_dispatch: null })
    expect(workflow.permissions).toEqual({ contents: 'read' })
    expect(jobs).toHaveLength(1)
    for (const job of jobs) {
      expect(job.permissions).toBeUndefined()
      expect(job['timeout-minutes']).toEqual(expect.any(Number))
      expect(job['timeout-minutes']).toBeGreaterThan(0)
      expect(job['runs-on']).toBe('ubuntu-latest')
    }
  })

  test('uploads the report and tears the cluster down even when the run fails', () => {
    const upload = uploadStep()
    const teardown = stepWith('docker compose')

    expect(upload.if).toBe('always()')
    expect(upload.with.path).toEqual(expect.stringContaining('bench-report.md'))
    expect(upload.with.path).toEqual(expect.stringContaining('bench-report.json'))
    expect(runs(teardown)).toMatch(/docker compose .*down --volumes --remove-orphans/)
    expect(teardown.if).toBe('always()')
    expect(steps.indexOf(teardown)).toBeGreaterThan(steps.indexOf(upload))
  })

  test('runs on Kafka 4.4.0 and waits for the cluster before the report', () => {
    const start = stepWith('dockerComposeUp.sh')
    const wait = stepWith('waitForKafka.js')
    const report = stepWith('report.js')

    expect(jobs[0].env.KAFKA_VERSION).toBe('4.4.0')
    expect(steps.indexOf(wait)).toBeGreaterThan(steps.indexOf(start))
    expect(steps.indexOf(report)).toBeGreaterThan(steps.indexOf(wait))
  })

  test('installs the fork tarball and kafkajs 2.2.4 into separate scratch prefixes', () => {
    const script = steps.map(runs).join('\n')

    expect(script).toMatch(/npm pack\b/)
    expect(script).toMatch(/npm install --prefix "\$RUNNER_TEMP\/fork" /)
    expect(script).toMatch(/npm install --prefix "\$RUNNER_TEMP\/base" kafkajs@2\.2\.4\b/)
  })

  test('runs report.js for 3 runs against the two prefixes and writes into one directory', () => {
    const report = runs(stepWith('report.js'))
    const out = report.match(/--out (\S+)/)?.[1]

    expect(report).toMatch(/node scripts\/benchmarks\/report\.js\b/)
    expect(report).toMatch(/--runs 3\b/)
    expect(report).toContain('--fork-dir "$RUNNER_TEMP/fork"')
    expect(report).toContain('--baseline-dir "$RUNNER_TEMP/base"')
    expect(out).toBeDefined()
    expect(uploadStep().with.path).toContain(
      out.replace(/"/g, '').replace('$RUNNER_TEMP', expr('runner.temp'))
    )
  })

  test('shows the Markdown report in the job summary', () => {
    const summary = stepWith('GITHUB_STEP_SUMMARY')

    expect(runs(summary)).toMatch(/bench-report\.md/)
    expect(summary.if).toBe('always()')
  })

  test('passes no expression into a run script', () => {
    for (const step of steps) expect(runs(step)).not.toContain('${{')
  })
})
