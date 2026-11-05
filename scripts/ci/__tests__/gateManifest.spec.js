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
