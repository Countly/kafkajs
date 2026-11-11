const fs = require('fs')
const path = require('path')

const root = path.join(__dirname, '..', '..', '..')
const gate = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'gate.json'), 'utf8'))
const workflow = fs.readFileSync(path.join(root, '.github', 'workflows', 'gate.yml'), 'utf8')

/** The live arms the exit gate must run, by the environment flag that enables each spec. */
const LIVE_ARMS = {
  GSSAPI_E2E: 'docker-compose.kerberos.yml',
  TLS_NEGATIVE_E2E: 'docker-compose.tls-required.yml',
  OAUTH_HELPERS_E2E: 'docker-compose.oauthbearer.yml',
  IDLE_REAP_E2E: 'docker-compose.yml',
  ADMIN_LEDGER_E2E: 'docker-compose.yml',
  ADMIN_LIFECYCLE_E2E: 'docker-compose.yml',
  ADMIN_QUOTA_E2E: 'docker-compose.yml',
  ADMIN_TOKENS_E2E: 'docker-compose.tokens.yml',
  ADMIN_LOGDIRS_E2E: 'docker-compose.logdirs.yml',
  ADMIN_TIERED_E2E: 'docker-compose.tiered.yml',
}

/** @returns {string[]} the `needs` flags the workflow's "Read check" step accepts */
const supportedNeeds = () =>
  JSON.parse(/const supported = (\[[^\]]*\])/.exec(workflow)[1].replace(/'/g, '"'))

/**
 * @param {object} check a gate check
 * @returns {RegExp[]} the Jest path patterns its `jest` arguments select
 */
const patternsOf = (check) => {
  const from = check.jest.indexOf('--testPathPatterns')
  if (from < 0) return []
  const patterns = []
  for (let at = from + 1; at < check.jest.length && !check.jest[at].startsWith('--'); at++)
    patterns.push(new RegExp(check.jest[at]))
  return patterns
}

describe('exit gate manifest', () => {
  test('decoder performance uses three parallel capped partitions and a combined check with the original id', () => {
    const yaml = require('js-yaml')
    const jobs = yaml.load(workflow).jobs
    const check = gate.checks.find(({ id }) => id === 'decoder-count-performance')
    expect(check.partitions).toEqual(['frame-1m', 'frame-other', 'batch-array'])
    expect(check.capMinutes).toBe(30)
    expect(check.command).toContain('--partition "$PARTITION"')
    expect(check.command).toMatch(/\|\| exit 1$/)
    const planStep = jobs.plan.steps.find(({ id }) => id === 'plan')
    expect(planStep.env.ONLY).toBe('$' + '{{ inputs.only }}')
    const plan = planStep.run
    expect(plan).toContain('process.env.ONLY')
    expect(jobs.check.env).toMatchObject({
      CHECK_ID: '$' + '{{ matrix.checkId || matrix.id }}',
      MATRIX_ID: '$' + '{{ matrix.id }}',
      PARTITION: '$' + '{{ matrix.partition }}',
    })
    const record = jobs.check.steps.find(({ name }) => name === 'Record result')
    expect(record.env.RESULT).toBe('$' + '{{ job.status }}')
    expect(record.run).toContain('process.env.MATRIX_ID')
    expect(record.run).toContain('process.env.RESULT')
    const summary = jobs.verdict.steps.find(({ name }) => name === 'Summarize results')
    expect(summary.env).toEqual({
      MATRIX: '$' + '{{ needs.plan.outputs.matrix }}',
      DECODER: '$' + '{{ needs.plan.outputs.decoder }}',
      PLAN_RESULT: '$' + '{{ needs.plan.result }}',
      CHECK_RESULT: '$' + '{{ needs.check.result }}',
      TELEMETRY_RESULT: '$' + '{{ needs.telemetry.result }}',
    })
    const vm = require('vm')
    let output = ''
    vm.runInNewContext(plan.match(/node <<'NODE'\n([\s\S]*?)\nNODE/)[1], {
      process: { env: { ONLY: check.id, GITHUB_OUTPUT: 'output' } },
      require: () => ({
        readFileSync: () => JSON.stringify(gate),
        appendFileSync: (_file, text) => {
          output += text
        },
      }),
    })
    const matrix = JSON.parse(
      output
        .split('\n')
        .find((line) => line.startsWith('matrix='))
        .slice('matrix='.length)
    ).include
    expect(matrix.map(({ partition }) => partition)).toEqual(check.partitions)
    expect(new Set(matrix.map(({ id }) => id)).size).toBe(3)
    expect(matrix.every(({ capMinutes }) => capMinutes === 30)).toBe(true)
    expect(jobs.check.strategy['fail-fast']).toBe(false)
    expect(jobs['decoder-combine'].needs).toContain('check')
    expect(jobs['decoder-combine']['timeout-minutes']).toBeGreaterThan(0)
    expect(jobs.verdict.needs).toContain('decoder-combine')
    expect(JSON.stringify(jobs['decoder-combine'])).toContain(
      'gate-result-decoder-count-performance'
    )
    for (const job of Object.values(jobs)) {
      if (!job.uses) expect(job['timeout-minutes']).toBeDefined()
      for (const step of job.steps || []) if (step.run) expect(step.run).not.toContain('$' + '{{')
    }
  })

  test('every check has a unique kebab-case id and a whole number of minutes', () => {
    const ids = gate.checks.map(({ id }) => id)
    expect(new Set(ids).size).toBe(ids.length)
    for (const { id, capMinutes } of gate.checks) {
      expect(id).toMatch(/^[a-z0-9][a-z0-9-]*$/)
      expect(Number.isInteger(capMinutes) && capMinutes >= 1).toBe(true)
    }
  })

  test('a check asks only for fixtures the workflow can prepare', () => {
    const supported = supportedNeeds()
    for (const check of gate.checks)
      for (const flag of Object.keys(check.needs || {})) expect(supported).toContain(flag)
  })

  test('a check names a broker release as major.minor.patch or not at all', () => {
    for (const { kafkaVersion } of gate.checks)
      if (kafkaVersion !== undefined) expect(kafkaVersion).toMatch(/^\d+\.\d+\.\d+$/)
  })

  test('the workflow rejects a malformed broker release in both of its steps that read checks', () => {
    expect(workflow.match(/Invalid kafkaVersion/g)).toHaveLength(2)
    expect(workflow).toContain('`KAFKA_VERSION=$' + '{check.kafkaVersion ||')
  })

  test.each(Object.entries(LIVE_ARMS))('%s runs in exactly one check, on %s', (flag, compose) => {
    const checks = gate.checks.filter((check) => check.env?.[flag] === '1')
    expect(checks).toHaveLength(1)
    expect(checks[0].kind).toBe('cluster')
    expect(checks[0].compose).toBe(compose)
  })

  test('a live arm lists the tests it must prove, so a self-skipping spec cannot pass', () => {
    for (const check of gate.checks) {
      const live = Object.keys(check.env || {}).some((name) => /_E2E$/.test(name))
      if (live && check.jest) expect(check.titles.length).toBeGreaterThan(0)
    }
  })

  test('the frozen coverage run tolerates every skip a group check on the same compose file tolerates', () => {
    const frozen = gate.checks.find(({ id }) => id === 'coverage-frozen')
    const tolerated = new Set(frozen.allowedSkips)
    const missing = gate.checks
      .filter(({ id, compose }) => id.startsWith('group-') && compose === frozen.compose)
      .flatMap(({ allowedSkips = [] }) => allowedSkips)
      .filter((skip) => !tolerated.has(skip))
    expect(missing).toEqual([])
  })

  test('the KDC image builds on the host network so its package fetch uses the runner DNS', () => {
    const build = workflow.split('\n').find((line) => /docker build .*gate-kdc:local/.test(line))
    expect(build).toMatch(/docker build --network=host /)
  })

  test('every expected title names an existing spec that its check selects', () => {
    for (const check of gate.checks) {
      if (!check.jest || !check.titles) continue
      const patterns = patternsOf(check)
      for (const title of check.titles) {
        const file = title.split(' › ')[0]
        expect(fs.existsSync(path.join(root, file))).toBe(true)
        if (patterns.length) expect(patterns.some((pattern) => pattern.test(file))).toBe(true)
      }
    }
  })
})
