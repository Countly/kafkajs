const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const crypto = require('node:crypto')
const { spawnSync } = require('node:child_process')
const yaml = require('js-yaml')
const { prepareDecoder } = require('../../e2e/telemetry/prepareDecoder')

const ROOT = path.join(__dirname, '..', '..', '..')
const FIXTURES = 'scripts/e2e/telemetry/fixtures/'
const CONFIGURED = 'docker-compose.telemetry.yml'
const BASE = 'docker-compose.yml'
const CONFIGURED_GROUPS = [
  'telemetry-reporter',
  'telemetry-prefix',
  'telemetry-custom',
  'telemetry-payload',
  'telemetry-java',
  'telemetry-outcomes',
  'telemetry-shutdown',
]

/**
 * @param {string} file repository-relative path
 * @returns {string} file text
 */
const read = (file) => fs.readFileSync(path.join(ROOT, file), 'utf8')

/**
 * @param {string} file repository-relative path
 * @returns {string} SHA-256 of the file bytes
 */
const sha256 = (file) =>
  crypto
    .createHash('sha256')
    .update(fs.readFileSync(path.join(ROOT, file)))
    .digest('hex')

/**
 * Runs the `plan` job's inline script exactly as the workflow does.
 * @param {{full: boolean, memory?: boolean}} options plan inputs
 * @returns {{node: number, kafka: string, group: string, compose: string}[]} matrix rows
 */
const planRows = ({ full, memory = false }) => {
  const workflow = yaml.load(read('.github/workflows/telemetry.yml'))
  const step = workflow.jobs.plan.steps.find((s) => s.id === 'plan')
  const script = step.run.match(/node <<'NODE'\n([\s\S]*?)\nNODE/)[1]
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'telemetry-plan-'))
  const output = path.join(directory, 'output')
  try {
    const result = spawnSync(process.execPath, ['-e', script], {
      encoding: 'utf8',
      env: {
        ...process.env,
        FULL: String(full),
        MEMORY_VERDICTS: String(memory),
        GITHUB_OUTPUT: output,
      },
    })
    if (result.status !== 0) throw new Error(`plan script failed: ${result.stderr}`)
    return JSON.parse(fs.readFileSync(output, 'utf8').trim().slice('matrix='.length)).include
  } finally {
    fs.rmSync(directory, { recursive: true, force: true })
  }
}

/**
 * @param {{kafka: string, group: string, compose: string}[]} rows matrix rows
 * @param {string} compose compose file to keep
 * @returns {Object<string, string[]>} sorted Kafka lines per group
 */
const linesByGroup = (rows, compose) => {
  const groups = {}
  for (const row of rows) {
    if (row.compose === compose) (groups[row.group] ??= new Set()).add(row.kafka)
  }
  return Object.fromEntries(Object.entries(groups).map(([group, set]) => [group, [...set].sort()]))
}

describe('telemetry lane configuration', () => {
  test('the full telemetry matrix lists the exact Kafka lines per group', () => {
    const rows = planRows({ full: true })

    expect([...new Set(rows.map((row) => row.node))]).toEqual([22, 24, 26])
    expect(linesByGroup(rows, CONFIGURED)).toEqual(
      Object.fromEntries(CONFIGURED_GROUPS.map((group) => [group, ['4.2.2', '4.4.0']]))
    )
    expect(linesByGroup(rows, BASE)).toEqual({
      'telemetry-base': ['4.0.2', '4.1.2', '4.2.2', '4.4.0'],
      'telemetry-unsupported': ['4.0.2', '4.1.2', '4.2.2', '4.4.0'],
      'telemetry-outcomes': ['4.1.2', '4.4.0'],
    })
    expect(rows).toHaveLength(72)
    expect(new Set(rows.map((row) => JSON.stringify(row))).size).toBe(72)
  })

  test('the development telemetry matrix runs Kafka 4.4.0 only', () => {
    const rows = planRows({ full: false })

    expect(rows.every((row) => row.node === 24 && row.kafka === '4.4.0')).toBe(true)
    expect(rows).toHaveLength(9)
    expect(rows.filter((row) => row.group === 'telemetry-outcomes').map((r) => r.compose)).toEqual([
      CONFIGURED,
    ])
  })

  test('no telemetry lane runs Kafka 4.3.1', () => {
    for (const options of [{ full: true }, { full: false }, { full: true, memory: true }])
      expect(planRows(options).filter((row) => row.kafka === '4.3.1')).toEqual([])
  })

  test('docker-compose.telemetry.yml defaults to Kafka 4.4.0 and its reporter jar name follows KAFKA_VERSION', () => {
    const common = yaml.load(read(CONFIGURED))['x-kafka-common']
    const version = '$' + '{KAFKA_VERSION:-4.4.0}'

    expect(common.image).toBe(`apache/kafka:${version}`)
    expect(common.volumes).toContain(
      `./scripts/e2e/telemetry/fixtures/counting-telemetry-reporter-${version}.jar:/opt/kafka/libs/counting-telemetry-reporter.jar:ro,z`
    )
  })

  test('the telemetry manifest has a 4.4.0 line whose jars exist and match their SHA-256', () => {
    const line = JSON.parse(read(FIXTURES + 'manifest.json')).lines['4.4.0']

    expect(line).toBeDefined()
    expect(line.reporterJar.path).toBe(FIXTURES + 'counting-telemetry-reporter-4.4.0.jar')
    expect(line.javaClientJar.path).toBe(FIXTURES + 'java-telemetry-client-4.4.0.jar')
    for (const jar of [line.reporterJar, line.javaClientJar])
      expect(sha256(jar.path)).toBe(jar.sha256)
    expect(line.repoDigests.some((d) => /^apache\/kafka@sha256:[0-9a-f]{64}$/.test(d))).toBe(true)
  })

  test('the fixture runners accept Kafka 4.4.0', () => {
    const runner = read('scripts/e2e/telemetry/run.sh')

    for (const mode of ['verify', 'decode', 'prepare', 'client', 'client-check'])
      expect(runner).toContain(`${mode}:4.4.0`)
    expect(() => prepareDecoder({ line: '4.4.0', archive: 'relative' })).toThrow(
      'Expected an absolute decoder archive path'
    )
    expect(read('scripts/e2e/telemetry/evidence.js')).toContain("'4.4.0'")
  })

  test('the reporter ordering step runs on a Kafka 4.4.0 row and covers every retained reporter jar', () => {
    const steps = yaml.load(read('.github/workflows/telemetry.yml')).jobs.telemetry.steps
    const ordering = steps.find((step) => step.run?.includes('test-reporter-order.sh'))

    expect(ordering.if).toBe("matrix.group == 'telemetry-reporter' && matrix.kafka == '4.4.0'")
    expect(read('scripts/e2e/telemetry/test-reporter-order.sh')).toContain(
      'for V in 4.2.2 4.3.1 4.4.0'
    )
  })

  test.each([
    ['4.4.0', '8ae1b15a3f459992abc6f07e3f1ae4ed05b6c2371f4cb9fcaa451cac550959b8'],
    ['4.3.1', 'f624d0d3b774048ce50c21d418fe61429384a37e35fbc11d61170e2ea8e6cb9c'],
    ['4.2.2', 'f624d0d3b774048ce50c21d418fe61429384a37e35fbc11d61170e2ea8e6cb9c'],
  ])(
    'prepares the compatible reporter source for Kafka %s without changing retained bytes',
    (line, expected) => {
      const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'telemetry-reporter-source-'))
      const source = 'scripts/e2e/telemetry/CountingTelemetryReporter.java'
      const client = 'scripts/e2e/telemetry/JavaTelemetryClient.java'
      const prepare = 'scripts/e2e/telemetry/prepare-reporter-source.sh'
      try {
        const output = path.join(directory, 'source with spaces')
        const result = spawnSync('bash', [path.join(ROOT, prepare), line, output], {
          encoding: 'utf8',
        })

        expect(result.status).toBe(0)
        expect(result.stderr).toBe('')
        const file = path.join(output, 'CountingTelemetryReporter.java')
        expect(result.stdout).toBe(file + '\n')
        const prepared = fs.readFileSync(file)
        expect(crypto.createHash('sha256').update(prepared).digest('hex')).toBe(expected)
        const amendment = JSON.parse(read(FIXTURES + 'manifest.json')).amendments.find(
          (entry) => entry.id === 'app-info-parser-4.4.0'
        )
        expect(amendment.preparedReporterSha256[line]).toBe(expected)
        expect(sha256(source)).toBe(
          'f624d0d3b774048ce50c21d418fe61429384a37e35fbc11d61170e2ea8e6cb9c'
        )
        expect(sha256(client)).toBe(
          '8c9254fd4e929ce95dd6b8d8cfbca8513f8ce8fe37d19d596421e4fa3e7736c3'
        )
        for (const script of ['build.sh', 'test-reporter-order.sh']) {
          const text = read('scripts/e2e/telemetry/' + script)
          expect(text).toContain(
            'REPORTER_SOURCE=$(bash "$HERE/prepare-reporter-source.sh" "$V" "$WORK/source")'
          )
          expect(text).toMatch(/javac [^\n]+ "\$REPORTER_SOURCE"/)
          expect(text).not.toMatch(/javac [^\n]+ "\$HERE\/CountingTelemetryReporter\.java"/)
        }
      } finally {
        fs.rmSync(directory, { recursive: true, force: true })
      }
    }
  )

  test('rejects unsupported reporter source versions before writing output', () => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'telemetry-reporter-version-'))
    try {
      const output = path.join(directory, 'source')
      const result = spawnSync(
        'bash',
        [path.join(ROOT, 'scripts/e2e/telemetry/prepare-reporter-source.sh'), '4.1.2', output],
        {
          encoding: 'utf8',
        }
      )

      expect(result.status).toBe(2)
      expect(result.stdout).toBe('')
      expect(fs.existsSync(output)).toBe(false)
    } finally {
      fs.rmSync(directory, { recursive: true, force: true })
    }
  })

  test('preserves the original Java 17 client for older jars and builds the current client for Java 21', () => {
    const source = 'scripts/e2e/telemetry/legacy/JavaTelemetryClient.java'
    expect(sha256(source)).toBe('27e3fb34c247f514a0e07a07e85448e7c1bd6bc27eaa3fe301af04721f01eab3')
    const manifest = JSON.parse(read(FIXTURES + 'manifest.json'))
    expect(sha256(source)).toBe(manifest.scripts['legacy/JavaTelemetryClient.java'])
    expect(sha256('scripts/e2e/telemetry/JavaTelemetryClient.java')).toBe(
      manifest.scripts['JavaTelemetryClient.java']
    )
    const amendment = manifest.amendments.find((entry) => entry.id === 'app-info-parser-4.4.0')
    expect(amendment.javaClientBuilds).toEqual({
      '4.4.0': {
        source: 'JavaTelemetryClient.java',
        sha256: '8c9254fd4e929ce95dd6b8d8cfbca8513f8ce8fe37d19d596421e4fa3e7736c3',
        release: 21,
      },
      '4.3.1': {
        source: 'legacy/JavaTelemetryClient.java',
        sha256: '27e3fb34c247f514a0e07a07e85448e7c1bd6bc27eaa3fe301af04721f01eab3',
        release: 17,
      },
      '4.2.2': {
        source: 'legacy/JavaTelemetryClient.java',
        sha256: '27e3fb34c247f514a0e07a07e85448e7c1bd6bc27eaa3fe301af04721f01eab3',
        release: 17,
      },
    })
    const build = read('scripts/e2e/telemetry/build.sh')
    expect(build).toContain(
      'CLIENT_SOURCE=$(bash "$HERE/prepare-client-source.sh" "$V" "$WORK/source")'
    )
    expect(build).toContain(`CLIENT_RELEASE=17
CLIENT_CP=$KC:$SLF
if [ "$V" = 4.4.0 ]; then
  CLIENT_RELEASE=21
  CLIENT_CP=$CLIENT_CP:$PB:$OUT/otlp-v1.9.0-bindings-unrelocated.jar
fi`)
    expect(build).toMatch(
      /javac --release "\$CLIENT_RELEASE" [^\n]+ -cp "\$CLIENT_CP" [^\n]+ "\$CLIENT_SOURCE"/
    )
  })

  test.each([
    ['4.4.0', 'f84cbb66d46b3f20257daaa23f14783b636931fb5e228abcc1735241463cf6b4'],
    ['4.3.1', '27e3fb34c247f514a0e07a07e85448e7c1bd6bc27eaa3fe301af04721f01eab3'],
    ['4.2.2', '27e3fb34c247f514a0e07a07e85448e7c1bd6bc27eaa3fe301af04721f01eab3'],
  ])(
    'prepares an independent OTLP client for Kafka %s while preserving historical sources',
    (line, expected) => {
      const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'telemetry-client-source-'))
      const prepare = 'scripts/e2e/telemetry/prepare-client-source.sh'
      try {
        const output = path.join(directory, 'source with spaces')
        const result = spawnSync('bash', [path.join(ROOT, prepare), line, output], {
          encoding: 'utf8',
        })
        expect(result.status).toBe(0)
        expect(result.stderr).toBe('')
        const file = path.join(output, 'JavaTelemetryClient.java')
        expect(result.stdout).toBe(file + '\n')
        const prepared = fs.readFileSync(file)
        expect(crypto.createHash('sha256').update(prepared).digest('hex')).toBe(expected)
        const manifest = JSON.parse(read(FIXTURES + 'manifest.json'))
        const amendment = manifest.amendments.find(
          (entry) => entry.id === 'independent-client-otlp-4.4.0'
        )
        expect(amendment.preparedClientSha256[line]).toBe(expected)
        expect(manifest.scripts['prepare-client-source.sh']).toBe(sha256(prepare))
        if (line === '4.4.0') {
          const source = prepared.toString()
          expect(source).toContain('import io.opentelemetry.proto.metrics.v1.MetricsData;')
          expect(source).toContain('MetricsData.parseFrom(push.metricsData(4096))')
          expect(source).not.toMatch(/ClientTelemetryUtils|org\.apache\.kafka\.shaded/)
          expect(source).toContain('if (count != (corrected ? 4 : 3))')
        } else
          expect(prepared).toEqual(
            fs.readFileSync(
              path.join(ROOT, 'scripts/e2e/telemetry/legacy/JavaTelemetryClient.java')
            )
          )
        expect(sha256('scripts/e2e/telemetry/JavaTelemetryClient.java')).toBe(
          '8c9254fd4e929ce95dd6b8d8cfbca8513f8ce8fe37d19d596421e4fa3e7736c3'
        )
        expect(sha256('scripts/e2e/telemetry/CountingTelemetryReporter.java')).toBe(
          'f624d0d3b774048ce50c21d418fe61429384a37e35fbc11d61170e2ea8e6cb9c'
        )
      } finally {
        fs.rmSync(directory, { recursive: true, force: true })
      }
    }
  )

  test('rejects unsupported client source versions before writing output', () => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'telemetry-client-version-'))
    try {
      const output = path.join(directory, 'source')
      const result = spawnSync(
        'bash',
        [path.join(ROOT, 'scripts/e2e/telemetry/prepare-client-source.sh'), '4.1.2', output],
        {
          encoding: 'utf8',
        }
      )
      expect(result.status).toBe(2)
      expect(result.stdout).toBe('')
      expect(fs.existsSync(output)).toBe(false)
    } finally {
      fs.rmSync(directory, { recursive: true, force: true })
    }
  })

  test('packages independent client bindings and runs the real Kafka 4.4.0 controls in the provenance build', () => {
    const build = read('scripts/e2e/telemetry/build.sh')
    expect(build).toContain(
      'if [ "$V" = 4.4.0 ]; then\n  cp -r "$WORK/classes-orig/." "$WORK/classes-client/"\nfi'
    )
    expect(build).toContain(
      'java -cp "$OUT/java-telemetry-client-$V.jar:$KC:$PB:$SLF" org.countly.kafkajs.telemetry.JavaTelemetryClient --controls'
    )
    expect(build.indexOf('cp -r "$WORK/classes-orig/."')).toBeLessThan(
      build.indexOf('jar --create --date=$DATE --file "$OUT/java-telemetry-client-$V.jar"')
    )
    const runner = read('scripts/e2e/telemetry/run.sh')
    expect(runner).toContain("'prepare-client-source.sh'")
  })

  test('the temporary telemetry fixtures build workflow sets its cache on the runner and supports manual dispatch', () => {
    const workflow = yaml.load(read('.github/workflows/telemetry-fixtures-build.yml'))
    const job = workflow.jobs.build
    const cache = job.steps.findIndex((step) => step.name === 'Set telemetry fixture cache path')
    const fetch = job.steps.findIndex(
      (step) => step.name === 'Fetch and verify the pinned build inputs'
    )

    expect(workflow.on).toHaveProperty('workflow_dispatch')
    expect(workflow.on.pull_request.paths).toContain(
      '.github/workflows/telemetry-fixtures-build.yml'
    )
    expect(job['timeout-minutes']).toBe(20)
    expect(job.env).toEqual({ KAFKA_IMAGE: 'apache/kafka:4.4.0' })
    expect(cache).toBeGreaterThanOrEqual(0)
    expect(fetch).toBeGreaterThan(cache)
    expect(job.steps[cache].run).toBe('echo "TF1_CACHE=$RUNNER_TEMP/tf1-cache" >> "$GITHUB_ENV"')
    expect(job.steps.at(-1).with.path).toBe('$' + '{{ runner.temp }}/collected/')
  })
})
