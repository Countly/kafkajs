const { AssertionError } = require('assert')
const crypto = require('crypto')
const fs = require('fs')
const path = require('path')
const { verifyEvidence, verifyScenario } = require('../../scripts/e2e/telemetry/evidence')
const { SEGMENT_FORMAT } = require('../telemetrySegments')

const FIXTURE = path.resolve(__dirname, '../../scripts/e2e/telemetry')
const pinnedBytes = fs.readFileSync(path.join(FIXTURE, 'fixtures/manifest.json'))
const pinned = JSON.parse(pinnedBytes)
const source = fs.readFileSync(path.join(FIXTURE, 'CountingTelemetryReporter.java'), 'utf8')
const sha = (bytes) => crypto.createHash('sha256').update(bytes).digest('hex')

/**
 * Creates a receipt-bound identity archive without Java or brokers. Churn rows are deliberately
 * absent: reaching their validation proves that the real scenario verifier admitted the jar.
 * @param {string} directory isolated archive directory
 * @param {string} line pinned broker line
 * @returns {object} stamped scenario, mutable identity and sidecar resealing function
 */
const archive = (directory, line = '4.3.1') => {
  const receipt = {
    diagnostic: false,
    acceptanceEligible: true,
    line,
    sinkBytes: 5242880,
    sinkBudget: {
      observedBytes: 2096328,
      droppedRows: 379,
      tailRows: 48,
      requiredBytes: 3845747,
      margin: 1.25,
    },
    sourceSha256: pinned.scripts['CountingTelemetryReporter.java'],
    preparedSourceSha256: sha(
      source.replace('MAX_FILE_BYTES = 2L * 1024 * 1024;', 'MAX_FILE_BYTES = 5242880L;')
    ),
    retainedSha256: pinned.lines[line].reporterJar.sha256,
    dependencies: [`kafka-clients-${line}`, 'protobuf-java'].map((key) => ({
      file: `/cache/${pinned.inputs[key].cachePath}`,
      sha256: pinned.inputs[key].sha256,
    })),
    sha256: sha(`compiled-reporter-${line}`),
  }
  const identity = {
    project: 'telemetry-receipt-test',
    line,
    compose: 'docker-compose.telemetry.yml',
    preflightPassed: true,
    preflight: { containers: [], networks: [], volumes: [] },
    readyAt: 'ready',
    startedAt: 'started',
    configSha256: 'config',
    images: [1, 2, 3].map((n) => ({
      container: String(n),
      repoDigests: pinned.lines[line].repoDigests,
    })),
    brokers: [1, 2, 3].map(() => ({ reporterSha256: receipt.sha256, configSha256: 'config' })),
    churnReporter: receipt,
  }
  const record = {
    status: 'passed',
    run: {
      project: identity.project,
      line,
      composeSha256: pinned.composeSha256['docker-compose.telemetry.yml'],
    },
    fixture: { manifestSha256: sha(pinnedBytes) },
    sidecars: {
      root: 'case-receipt',
      scenario: 'telemetry-churn',
      format: SEGMENT_FORMAT,
      files: [],
      owners: {},
      cleanupReceipt: { path: 'ownership-receipt.json', project: identity.project },
    },
  }
  fs.mkdirSync(path.join(directory, record.sidecars.root))
  const seal = () => {
    const bytes = Buffer.from(JSON.stringify(identity))
    const relative = `${record.sidecars.root}/identity.json`
    fs.writeFileSync(path.join(directory, relative), bytes)
    record.sidecars.files = [{ path: relative, bytes: bytes.length, sha256: sha(bytes) }]
  }
  seal()
  return { record, identity, seal }
}

describe('bounded reporter evidence identity', () => {
  let directory
  let project
  beforeEach(() => {
    fs.mkdirSync(path.resolve('.tmp'), { recursive: true })
    directory = fs.mkdtempSync(path.resolve('.tmp/evidjar-test-'))
    project = process.env.COMPOSE_PROJECT_NAME
  })
  afterEach(() => {
    fs.rmSync(directory, { recursive: true, force: true })
    if (project === undefined) delete process.env.COMPOSE_PROJECT_NAME
    else process.env.COMPOSE_PROJECT_NAME = project
  })

  test.each(['4.2.2', '4.3.1'])(
    'authenticates the derived %s jar before checking churn rows through both entry points',
    (line) => {
      const { record } = archive(directory, line)
      expect(() => verifyScenario(record, directory)).toThrow('Missing churn rows')
      process.env.COMPOSE_PROJECT_NAME = record.run.project
      const file = path.join(directory, 'scenario.jsonl')
      fs.writeFileSync(file, JSON.stringify(record) + '\n')
      expect(() => verifyEvidence('telemetry-soak', directory, [file])).toThrow(
        'Missing churn rows'
      )
    }
  )

  test.each([
    ['missing receipt', (i) => delete i.churnReporter, AssertionError],
    ['source identity', (i) => (i.churnReporter.sourceSha256 = '0'.repeat(64)), 'source identity'],
    ['file cap', (i) => (i.churnReporter.sinkBytes = 2097152), 'sink cap'],
    [
      'non-cap source change',
      (i) => {
        i.churnReporter.preparedSourceSha256 = sha(
          source
            .replace('MAX_FILE_BYTES = 2L * 1024 * 1024;', 'MAX_FILE_BYTES = 5242880L;')
            .replace('MAX_RECORD_BYTES = 4096;', 'MAX_RECORD_BYTES = 8192;')
        )
      },
      'prepared source',
    ],
    ['sink budget', (i) => (i.churnReporter.sinkBudget.margin = 1), 'sink budget'],
    ['retained jar', (i) => (i.churnReporter.retainedSha256 = '0'.repeat(64)), 'retained jar'],
    [
      'compiler input',
      (i) => (i.churnReporter.dependencies[0].sha256 = '0'.repeat(64)),
      'dependencies',
    ],
    ['missing input', (i) => i.churnReporter.dependencies.pop(), 'dependencies'],
    ['broker line', (i) => (i.churnReporter.line = '4.2.2'), 'broker line'],
    ['diagnostic receipt', (i) => (i.churnReporter.diagnostic = true), 'diagnostic'],
    ['ineligible receipt', (i) => (i.churnReporter.acceptanceEligible = false), 'acceptance'],
    ['mounted jar', (i) => (i.brokers[1].reporterSha256 = '0'.repeat(64)), AssertionError],
    [
      'malformed built hash',
      (i) => {
        i.churnReporter.sha256 = 'invalid'
        for (const broker of i.brokers) broker.reporterSha256 = 'invalid'
      },
      'built jar hash',
    ],
  ])('rejects %s after resealing the identity sidecar', (_name, mutate, message) => {
    const { record, identity, seal } = archive(directory)
    mutate(identity)
    seal()
    expect(() => verifyScenario(record, directory)).toThrow(message)
  })

  test.each([
    'telemetry-reporter',
    'telemetry-prefix',
    'telemetry-custom',
    'telemetry-payload',
    'telemetry-java',
    'telemetry-outcomes',
    'telemetry-shutdown',
  ])('keeps the retained jar pin for %s even with a valid churn receipt', (scenario) => {
    const { record } = archive(directory)
    record.sidecars.scenario = scenario
    expect(() => verifyScenario(record, directory)).toThrow(AssertionError)
  })

  test.each(['telemetry-reporter', 'telemetry-shutdown'])(
    'preserves retained jar evidence for %s',
    (scenario) => {
      const { record, identity, seal } = archive(directory)
      record.sidecars.scenario = scenario
      delete identity.churnReporter
      for (const broker of identity.brokers)
        broker.reporterSha256 = pinned.lines[record.run.line].reporterJar.sha256
      seal()
      delete record.sidecars.format
      expect(() => verifyScenario(record, directory)).toThrow(
        scenario === 'telemetry-shutdown'
          ? 'Missing segmented evidence format'
          : 'Required owner catalogue differs'
      )
    }
  )
})
