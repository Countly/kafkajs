const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { spawnSync } = require('node:child_process')
const yaml = require('js-yaml')
const manifest = require('../../benchmarks/manifest.json')
const benchJobs = require('../benchJobs')
const report = require('../../benchmarks/report')
const { setupFailure } = require('../../benchmarks/session')
const { ROWS: CONSUMER_ROWS } = require('../../benchmarks/adapters/consumerSession')

const WORKFLOW_FILE = path.join(__dirname, '..', '..', '..', '.github', 'workflows', 'bench.yml')
const raw = fs.readFileSync(WORKFLOW_FILE, 'utf8')
const workflow = yaml.load(raw)
const jobs = Object.values(workflow.jobs)
const steps = jobs.flatMap((job) => job.steps)
const runs = (step) => step.run ?? ''
const stepWith = (text) => steps.find((step) => runs(step).includes(text))
const stepIn = (job, text) => job.steps.find((step) => runs(step).includes(text))
const at = (job, text) => job.steps.findIndex((step) => runs(step).includes(text))
const uploads = (job) =>
  job.steps.filter((step) => step.uses?.startsWith('actions/upload-artifact@'))
/** A GitHub Actions expression, as it is written in the workflow file. */
const expr = (inner) => `\${{ ${inner} }}`
/** The jobs that run report rows, and the jobs among them that start a cluster. */
const ROW_JOBS = [
  'bench',
  'share-rows',
  'snappy-row',
  'consumer-rows',
  'consumer-audit',
  'telemetry-rows',
]
const CLUSTER_JOBS = ['bench', 'share-rows', 'consumer-rows', 'telemetry-rows']
const names = (entries) => entries.map(({ name }) => name)
const temporary = []
const tmp = () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'bench-workflow-'))
  temporary.push(dir)
  return dir
}

afterAll(() => {
  for (const dir of temporary) fs.rmSync(dir, { recursive: true, force: true })
})

describe('bench.yml', () => {
  test('starts the cluster with NO_LOGS so the startup script returns', () => {
    const starts = steps.filter((step) => runs(step).includes('dockerComposeUp.sh'))
    const startupScript = fs.readFileSync(
      path.join(__dirname, '..', '..', 'dockerComposeUp.sh'),
      'utf8'
    )

    expect(starts).toHaveLength(CLUSTER_JOBS.length)
    expect(startupScript).toMatch(/if \[ -z "\$\{NO_LOGS\}" \]/)
    for (const step of starts) {
      const noLogs = step.env?.NO_LOGS ?? runs(step).match(/\bNO_LOGS=(\S+)/)?.[1]
      expect(String(noLogs ?? '')).not.toBe('')
    }
  })

  test('is dispatch-only with read-only contents permission and a timeout', () => {
    expect(Object.keys(workflow.on)).toEqual(['workflow_dispatch'])
    expect(workflow.permissions).toEqual({ contents: 'read' })
    expect(Object.keys(workflow.jobs)).toEqual(['plan', ...ROW_JOBS, 'report'])
    for (const job of jobs) {
      expect(job.permissions).toBeUndefined()
      expect(job['timeout-minutes']).toEqual(expect.any(Number))
      expect(job['timeout-minutes']).toBeGreaterThan(0)
      expect(job['timeout-minutes']).toBeLessThanOrEqual(360)
      expect(job['runs-on']).toBe('ubuntu-latest')
    }
  })

  test('uploads the report and tears the cluster down even when the run fails', () => {
    for (const name of CLUSTER_JOBS) {
      const job = workflow.jobs[name]
      const teardown = job.steps.at(-1)

      expect(runs(teardown)).toMatch(/docker compose .*down --volumes --remove-orphans/)
      expect(teardown.if).toBe('always()')
      for (const upload of uploads(job)) {
        expect(upload.if).toBe('always()')
        expect(job.steps.indexOf(teardown)).toBeGreaterThan(job.steps.indexOf(upload))
      }
    }
    for (const name of ROW_JOBS) {
      const [summary] = uploads(workflow.jobs[name])
      expect(summary.with.name).toMatch(/^report-/)
      expect(summary.with.path).toContain(`${expr('runner.temp')}/bench/bench-report.md`)
      expect(summary.with.path).toContain(`${expr('runner.temp')}/bench/bench-report.json`)
    }
  })

  test('uploads the raw evidence of every row even when the run fails', () => {
    for (const name of ROW_JOBS) {
      const job = workflow.jobs[name]
      const out = runs(stepIn(job, 'report.js')).match(/--out (\S+)/)?.[1]
      const evidence = uploads(job)[1]

      expect(out).toBe('"$RUNNER_TEMP/bench"')
      expect(evidence.if).toBe('always()')
      expect(evidence.with.name).toMatch(/^bench-/)
      expect(evidence.with.path).toContain(`${expr('runner.temp')}/bench/`)
    }
  })

  test('roots every raw evidence artifact at the report directory, so the reported bench-raw locator resolves in it', () => {
    for (const name of ROW_JOBS)
      expect(uploads(workflow.jobs[name])[1].with.path).toBe(`${expr('runner.temp')}/bench/`)
    const readback = stepIn(workflow.jobs['snappy-row'], '--read-back')
    expect(runs(readback)).toContain('mkdir -p .tmp/bench "$RUNNER_TEMP/bench"')
    expect(runs(readback)).toContain(
      `| tee ${manifest.entries.find((e) => e.name === 'snappyReadback').readback} "$RUNNER_TEMP/bench/snappy-java-readback.log"`
    )
  })

  test('runs on Kafka 4.4.0 and waits for the cluster before the report', () => {
    for (const name of CLUSTER_JOBS) {
      const job = workflow.jobs[name]

      expect(job.env.KAFKA_VERSION).toBe('4.4.0')
      expect(at(job, 'waitForKafka.js')).toBeGreaterThan(at(job, 'dockerComposeUp.sh'))
      expect(at(job, 'report.js')).toBeGreaterThan(at(job, 'waitForKafka.js'))
    }
  })

  test('every job starts at most one cluster and waits for it before its report', () => {
    for (const job of jobs) {
      const starts = job.steps.filter((step) => runs(step).includes('dockerComposeUp.sh'))
      expect(starts.length).toBeLessThanOrEqual(1)
      if (!starts.length) continue
      expect(runs(starts[0]).match(/dockerComposeUp\.sh/g)).toHaveLength(1)
      expect(at(job, 'waitForKafka.js')).toBeGreaterThan(at(job, 'dockerComposeUp.sh'))
      expect(at(job, 'report.js')).toBeGreaterThan(at(job, 'waitForKafka.js'))
    }
  })

  test('installs the fork tarball and kafkajs 2.2.4 into separate scratch prefixes', () => {
    const script = workflow.jobs.bench.steps.map(runs).join('\n')

    expect(script).toMatch(/npm pack\b/)
    expect(script).toMatch(/npm install --prefix "\$RUNNER_TEMP\/fork" /)
    expect(script).toMatch(/npm install --prefix "\$RUNNER_TEMP\/base" kafkajs@2\.2\.4\b/)
  })

  test('runs report.js for 3 runs against the two prefixes and writes into one directory', () => {
    const job = workflow.jobs.bench
    const command = runs(stepIn(job, 'report.js'))
    const out = command.match(/--out (\S+)/)?.[1]

    expect(command).toMatch(/node scripts\/benchmarks\/report\.js\b/)
    expect(command).toMatch(/--runs 3\b/)
    expect(command).toContain('--fork-dir "$RUNNER_TEMP/fork"')
    expect(command).toContain('--baseline-dir "$RUNNER_TEMP/base"')
    expect(uploads(job)[1].with.path).toContain(
      out.replace(/"/g, '').replace('$RUNNER_TEMP', expr('runner.temp'))
    )
  })

  test('runs only the rows named by the optional rows input, every row not marked namedOnly when it is empty', () => {
    const plan = workflow.jobs.plan.steps.at(-1)
    const bench = stepIn(workflow.jobs.bench, 'report.js')

    expect(workflow.on.workflow_dispatch.inputs).toEqual({
      rows: {
        description: expect.any(String),
        required: false,
        default: '',
        type: 'string',
      },
    })
    expect(plan.env).toEqual({ ROWS: expr('inputs.rows') })
    expect(runs(plan)).toBe('node scripts/ci/benchJobs.js plan >> "$GITHUB_OUTPUT"')
    expect(Object.keys(workflow.jobs.plan.outputs)).toEqual([
      'ordinary',
      'share',
      'snappy',
      'consumer',
      'audit',
      'telemetry',
    ])
    expect(bench.env.ROWS).toBe(expr('needs.plan.outputs.ordinary'))
    expect(runs(bench)).toMatch(/ --only "\$ROWS"(\s|$)/)
    expect(benchJobs.plan('').selected.sort()).toEqual(
      names(manifest.entries.filter((entry) => !entry.namedOnly)).sort()
    )
    expect(benchJobs.plan('murmur2').selected).toEqual(['murmur2'])
  })

  test('a namedOnly row runs only when the dispatch names it, and an empty selection reports why it was left out', () => {
    const namedOnly = names(manifest.entries.filter((entry) => entry.namedOnly))
    const reports = tmp()

    expect(namedOnly.length).toBeGreaterThan(0)
    expect(benchJobs.plan('').ordinary).not.toEqual(expect.arrayContaining([namedOnly[0]]))
    expect(benchJobs.plan(namedOnly.join(',')).ordinary).toEqual(namedOnly)
    expect(benchJobs.plan(`murmur2,${namedOnly[1]}`).ordinary).toEqual([namedOnly[1], 'murmur2'])
    const left = benchJobs
      .collect(reports, { input: '' })
      .rows.filter(({ benchmark }) => namedOnly.includes(benchmark))
    expect(left.map(({ status, reason }) => [status, reason])).toEqual(
      namedOnly.map(() => ['not selected', 'runs only when named in the rows input'])
    )
    const other = benchJobs
      .collect(reports, { input: 'murmur2' })
      .rows.find(({ benchmark }) => benchmark === namedOnly[0])
    expect(other.reason).toBe('left out by the rows input')
    expect(benchJobs.collect(reports, { input: '' }).meta.selection).toBe(
      'every row not marked namedOnly'
    )
    expect(benchJobs.collect(reports, { input: ' murmur2 ' }).meta.selection).toBe('murmur2')
  })

  test('plans every selected row into exactly one owning job, and fails on an unknown row name', () => {
    const owners = Object.fromEntries(
      manifest.entries
        .filter((entry) => !entry.namedOnly)
        .map((entry) => [entry.name, benchJobs.ownerOf(entry)])
    )
    const all = benchJobs.plan('')
    const once = all.units.filter(({ job }) => job !== 'share-rows').flatMap(({ rows }) => rows)

    expect(new Set(once).size).toBe(once.length)
    for (const unit of all.units) for (const row of unit.rows) expect(owners[row]).toBe(unit.job)
    expect(Object.keys(owners).filter((row) => !all.selected.includes(row))).toEqual([])
    expect(owners).toMatchObject({
      'stickyBalance-healthy': 'bench',
      'shareRecordWalk-1m': 'bench',
      shareThroughput: 'share-rows',
      snappyReadback: 'snappy-row',
      'consumer-eachBatch': 'consumer-rows',
      'consumer-session-audit': 'consumer-audit',
      'telemetrySession-delivery-normal-eachBatch': 'telemetry-rows',
    })
    expect(all.share.map(({ key }) => key)).toEqual([
      'shareThroughput-node24',
      'shareThroughput-eachMessage-node22',
      'shareThroughput-eachMessage-node24',
      'shareThroughput-eachMessage-node26',
    ])
    expect(all.consumer).toHaveLength(Object.keys(CONSUMER_ROWS).length)
    expect(all.telemetry).toHaveLength(30)
    expect(() => benchJobs.plan('murmur2,no-such-row')).toThrow(
      'rows names no manifest row no-such-row'
    )
    const errors = []
    const code = benchJobs.main(['plan'], {
      env: { ROWS: 'no-such-row' },
      log: () => {
        throw new Error('printed outputs for an unknown row')
      },
      error: (line) => errors.push(line),
    })
    expect([code, errors]).toEqual([2, ['rows names no manifest row no-such-row']])
  })

  test('a consumer-only or telemetry-only selection plans no other job', () => {
    const lines = (input) => {
      const out = []
      expect(benchJobs.main(['plan'], { env: { ROWS: input }, log: (l) => out.push(l) })).toBe(0)
      return Object.fromEntries(out[0].split('\n').map((line) => line.split(/=(.*)/s).slice(0, 2)))
    }

    expect(lines(' consumer-eachBatch ')).toEqual({
      ordinary: '',
      share: '[]',
      snappy: '[]',
      consumer: JSON.stringify([
        {
          key: 'consumer-eachBatch',
          row: 'consumer-eachBatch',
          compose: 'docker-compose.yml',
          trees: 'alpha.9c,alpha.9b',
        },
      ]),
      audit: '[]',
      telemetry: '[]',
    })
    const delivery = 'telemetrySession-delivery-control-eachBatch'
    expect(lines(delivery)).toMatchObject({
      ordinary: '',
      share: '[]',
      snappy: '[]',
      consumer: '[]',
      audit: '[]',
      telemetry: JSON.stringify([{ key: delivery, row: delivery, delivery: true }]),
    })
    expect(workflow.jobs.bench.if).toBe("needs.plan.outputs.ordinary != ''")
    for (const [name, output] of [
      ['share-rows', 'share'],
      ['snappy-row', 'snappy'],
      ['consumer-rows', 'consumer'],
      ['telemetry-rows', 'telemetry'],
    ]) {
      const job = workflow.jobs[name]
      expect(job.needs).toBe('plan')
      expect(job.if).toBe(`needs.plan.outputs.${output} != '[]'`)
      expect(job.strategy.matrix).toEqual({
        include: expr(`fromJSON(needs.plan.outputs.${output})`),
      })
    }
  })

  test('selecting the consumer audit alone also selects every consumer row it audits', () => {
    const inherited = Object.keys(CONSUMER_ROWS).filter(
      (name) => CONSUMER_ROWS[name].contracts.length
    )
    const alone = benchJobs.plan('consumer-session-audit')
    const partial = benchJobs.plan(
      'consumer-session-audit,consumer-eachMessage,consumer-rebalanceStorm-200'
    )
    const job = workflow.jobs['consumer-audit']

    expect(alone.consumer.map(({ row }) => row)).toEqual(inherited)
    expect(alone.audit).toEqual([
      { key: 'consumer-session-audit', row: 'consumer-session-audit', scope: inherited.join(',') },
    ])
    expect(partial.consumer.map(({ row }) => row)).toEqual([
      'consumer-eachMessage',
      'consumer-rebalanceStorm-200',
    ])
    expect(partial.audit[0].scope).toBe('consumer-eachMessage')
    expect(benchJobs.plan('consumer-eachMessage').audit).toEqual([])
    expect(job.needs).toEqual(['plan', 'consumer-rows'])
    expect(job.if).toBe(
      expr("always() && needs.plan.result == 'success' && needs.plan.outputs.audit != '[]'")
    )
    expect(job.env).toEqual({
      BENCH_CONSUMER_ROWS: '1',
      BENCH_ROW: expr('matrix.row'),
      BENCH_AUDIT_ROWS: expr('matrix.scope'),
    })
  })

  test('runs each share row alone on its own share-enabled cluster, inside the hosted job limit', () => {
    const job = workflow.jobs['share-rows']
    const order = (text) => at(job, text)
    const run = stepIn(job, 'report.js')
    const { timeoutMs } = require('../../benchmarks/adapters/shareThroughput')

    expect(job.strategy).toEqual({
      'fail-fast': false,
      matrix: { include: expr('fromJSON(needs.plan.outputs.share)') },
    })
    expect(job.env).toEqual({
      KAFKA_VERSION: '4.4.0',
      COMPOSE_FILE: 'docker-compose.share.yml',
      BENCH_SHARE_ROWS: '1',
      BENCH_ROW: expr('matrix.row'),
    })
    expect(job.steps[1].with['node-version']).toBe(expr('matrix.node'))
    expect(order('dockerComposeUp.sh')).toBeLessThan(order('waitForKafka.js'))
    expect(order('waitForKafka.js')).toBeLessThan(order('shareKafka44Setup.sh'))
    expect(order('shareKafka44Setup.sh')).toBeLessThan(order('cluster.json'))
    expect(order('cluster.json')).toBeLessThan(order('report.js'))
    expect(run.env.KAFKAJS_CLUSTER_MANIFEST).toBe(`${expr('runner.temp')}/bench/cluster.json`)
    expect(runs(run)).toMatch(/ --only "\$BENCH_ROW"$/m)
    expect(job['timeout-minutes'] * 60000).toBeGreaterThan(timeoutMs + 20 * 60000)
    expect(uploads(job).map((upload) => upload.with.name)).toEqual([
      `report-${expr('matrix.key')}`,
      `bench-${expr('matrix.key')}`,
    ])
    expect(runs(job.steps.at(-1))).toBe(
      'docker compose -f docker-compose.share.yml down --volumes --remove-orphans'
    )
  })

  test('runs the Snappy row with the Java read-back in a job without a cluster', () => {
    const job = workflow.jobs['snappy-row']
    const java = job.steps.find((step) => step.uses?.startsWith('actions/setup-java@'))
    const readback = stepIn(job, '--read-back')
    const row = manifest.entries.find((entry) => entry.name === 'snappyReadback')
    const { timeoutMs } = require('../../benchmarks/adapters/snappyReadback')

    expect(java.uses).toBe('actions/setup-java@cf277c60eb25467037889841efdb72551f06f6c3')
    expect(java.with).toEqual({ distribution: 'temurin', 'java-version': '21' })
    expect(job.env).toEqual({ BENCH_SNAPPY_ROWS: '1', BENCH_ROW: expr('matrix.row') })
    expect(benchJobs.plan('snappyReadback').snappy).toEqual([
      { key: 'snappyReadback', row: 'snappyReadback' },
    ])
    expect(readback['continue-on-error']).toBe(true)
    expect(runs(readback)).toContain(`| tee ${row.readback}`)
    expect(runs(stepIn(job, 'report.js'))).toMatch(/ --only "\$BENCH_ROW"$/m)
    expect(job.steps.some((step) => /docker|shareKafka44Setup/.test(runs(step)))).toBe(false)
    expect(job['timeout-minutes'] * 60000).toBeGreaterThan(timeoutMs + 20 * 60000)
  })

  test('keeps the ordinary job on the plain cluster with no share, Snappy, consumer or telemetry setup', () => {
    const job = workflow.jobs.bench
    const script = job.steps.map(runs).join('\n')
    const ordinary = benchJobs.plan('').ordinary

    expect(job.env).toEqual({ KAFKA_VERSION: '4.4.0' })
    expect(script).not.toMatch(/shareKafka44Setup|BENCH_|git worktree|prepareDecoder/)
    expect(job.steps.some((step) => step.uses?.startsWith('actions/setup-java@'))).toBe(false)
    expect(job.steps.some((step) => step.env?.COMPOSE_FILE)).toBe(false)
    for (const row of ordinary)
      expect(benchJobs.ownerOf(manifest.entries.find((e) => e.name === row))).toBe('bench')
    expect(ordinary).toEqual(expect.arrayContaining(['shareRecordWalk-100k', 'shareRecordWalk-1m']))
  })

  test('a selected job whose setup fails still runs its row with every setup outcome', () => {
    for (const name of ROW_JOBS) {
      const job = workflow.jobs[name]
      const index = at(job, 'report.js')
      const run = job.steps[index]
      const prerequisites = job.steps
        .slice(0, index)
        .filter((step) => step.if !== 'failure()' && step['continue-on-error'] !== true)
        .filter((step) => step.run || step.id)
      const ids = prerequisites.map((step) => step.id)

      expect(ids.every(Boolean)).toBe(true)
      expect(run.if).toBe(expr('!cancelled()'))
      expect(run.env.BENCH_SETUP).toBe(
        ids.map((id) => `${id}=${expr(`steps.${id}.outcome`)}`).join(' ')
      )
      for (const failing of ids) {
        const outcomes = run.env.BENCH_SETUP.replace(
          expr(`steps.${failing}.outcome`),
          'failure'
        ).replace(/\$\{\{ steps\.[\w-]+\.outcome \}\}/g, 'success')
        expect(setupFailure(outcomes)).toBe(`workflow setup did not succeed: ${failing}=failure`)
      }
      for (const upload of uploads(job)) {
        expect(job.steps.indexOf(upload)).toBeGreaterThan(index)
        expect(upload.if).toBe('always()')
      }
      if (CLUSTER_JOBS.includes(name)) {
        const diagnostics = job.steps.find((step) => step.if === 'failure()')
        expect(job.steps.indexOf(diagnostics)).toBeLessThan(index)
        expect(runs(diagnostics)).toContain('"$RUNNER_TEMP/bench/setup-diagnostics.log"')
        expect(uploads(job)[1].with.path).toContain(`${expr('runner.temp')}/bench/`)
      }

      const output = { 'consumer-rows': 'consumer', 'consumer-audit': 'audit' }[name]
      const row =
        name === 'bench' ? 'murmur2' : benchJobs.plan('')[output ?? name.split('-')[0]][0].row
      const out = tmp()
      const failed = run.env.BENCH_SETUP.replace(
        /\$\{\{ steps\.[\w-]+\.outcome \}\}/,
        'failure'
      ).replace(/\$\{\{ steps\.[\w-]+\.outcome \}\}/g, 'skipped')
      const code = report.main(
        ['--runs', '1', '--fork-dir', out, '--baseline-dir', out, '--out', out, '--only', row],
        {
          env: { BENCH_SETUP: failed },
          log: () => {},
          spawn: () => {
            throw new Error(`${name} ran its row after a failed setup`)
          },
        }
      )
      const written = JSON.parse(fs.readFileSync(path.join(out, 'bench-report.json'), 'utf8'))
      expect(code).toBe(0)
      expect(written.rows.filter((r) => r.benchmark === row)).toEqual([
        expect.objectContaining({ status: 'error', reason: setupFailure(failed) }),
      ])
      expect(
        JSON.parse(fs.readFileSync(path.join(out, 'bench-raw', row, 'setup.json'), 'utf8'))
      ).toEqual({ setup: failed, reason: setupFailure(failed) })
    }
  })

  test('runs every consumer row in its own job on its cluster with the comparison trees and node inventory', () => {
    const job = workflow.jobs['consumer-rows']
    const planned = Object.fromEntries(benchJobs.plan('').consumer.map((unit) => [unit.row, unit]))

    expect(job.strategy).toEqual({
      'fail-fast': false,
      'max-parallel': 6,
      matrix: { include: expr('fromJSON(needs.plan.outputs.consumer)') },
    })
    expect(job.env).toEqual({
      KAFKA_VERSION: '4.4.0',
      BENCH_CONSUMER_ROWS: '1',
      BENCH_ROW: expr('matrix.row'),
      BENCH_COMPOSE: expr('matrix.compose'),
      BENCH_TREE_LABELS: expr('matrix.trees'),
    })
    expect(job.steps[0].with['fetch-depth']).toBe(0)
    expect(job['timeout-minutes'] * 60000).toBeGreaterThan(330 * 60000)
    for (const [name, row] of Object.entries(CONSUMER_ROWS)) {
      const expected = !row.broker
        ? ''
        : row.adapter === 'fetchSessions'
          ? 'docker-compose.racks.yml'
          : 'docker-compose.yml'
      expect(planned[name].compose).toBe(expected)
      expect(planned[name].trees.split(',').filter(Boolean)).toEqual(
        row.trees.filter((tree) => tree !== 'head')
      )
    }
    expect(planned['consumer-offsetCommit-retirement'].trees).toBe('before-retirement')
    expect(planned['consumer-rebalanceStorm-200'].trees).toBe('')
    const trees = runs(stepIn(job, 'git worktree add'))
    expect(trees).toContain('ref="refs/tags/3.0.0-$label"')
    expect(trees).toContain('cp scripts/benchmarks/*.js "$trees/$label/scripts/benchmarks/"')
    const nodes = runs(stepIn(job, 'BENCH_NODES='))
    expect(nodes).toContain('if [ "$BENCH_COMPOSE" != docker-compose.racks.yml ]; then exit 0; fi')
    expect(nodes).toContain("docker inspect --format '{{json .Config.Env}}'")
    expect(uploads(job)[0].with.path).toContain(
      `${expr('runner.temp')}/bench/bench-raw/*/fork.json`
    )
  })

  test('a history without the committed-offset retirement leaves out only the before-retirement tree', () => {
    const script = runs(stepIn(workflow.jobs['consumer-rows'], 'git worktree add'))
    const repo = tmp()
    const runner = tmp()
    const env = fs.openSync(path.join(runner, 'env'), 'w')
    fs.closeSync(env)
    const git = (...args) => {
      const done = spawnSync('git', args, { cwd: repo, encoding: 'utf8' })
      expect(done.status).toBe(0)
    }
    git('init', '-q')
    fs.writeFileSync(path.join(repo, 'README'), 'no retirement here\n')
    git('add', 'README')
    git(
      ...['-c', 'user.name=bench', '-c', 'user.email=bench@example.invalid'],
      ...['-c', 'commit.gpgsign=false', 'commit', '-qm', 'one']
    )
    const result = spawnSync('bash', ['-e', '-c', script], {
      cwd: repo,
      encoding: 'utf8',
      env: {
        PATH: process.env.PATH,
        RUNNER_TEMP: runner,
        GITHUB_ENV: path.join(runner, 'env'),
        BENCH_TREE_LABELS: 'before-retirement',
      },
    })

    expect(result.status).toBe(0)
    expect(result.stdout).toContain('the before-retirement tree is left out')
    expect(fs.readFileSync(path.join(runner, 'env'), 'utf8')).toBe('BENCH_TREES={}\n')
  })

  test('the consumer audit joins the session receipts of the selected rows even when a row failed', () => {
    const job = workflow.jobs['consumer-audit']
    const download = job.steps.find((step) => step.uses?.startsWith('actions/download-artifact@'))
    const join = runs(stepIn(job, 'bench-raw'))

    expect(download.with).toEqual({
      pattern: 'report-consumer-*',
      path: `${expr('runner.temp')}/rows`,
    })
    expect(join).toContain('cp -R "$raw"/. "$RUNNER_TEMP/bench/bench-raw/"')
    expect(at(job, 'cp -R')).toBeLessThan(at(job, 'report.js'))
    expect(runs(stepIn(job, 'report.js'))).toMatch(/ --only "\$BENCH_ROW"$/m)
    expect(benchJobs.plan('consumer-session-audit').audit[0].row).toBe('consumer-session-audit')
  })

  test('runs each selected telemetry row in its own job on the telemetry cluster', () => {
    const job = workflow.jobs['telemetry-rows']
    const own = (text) => stepIn(job, text)

    expect(job.needs).toBe('plan')
    expect(job.strategy).toEqual({
      'fail-fast': false,
      'max-parallel': 6,
      matrix: { include: expr('fromJSON(needs.plan.outputs.telemetry)') },
    })
    expect(job.env).toMatchObject({
      BENCH_TELEMETRY_ROW: expr('matrix.row'),
      TELEMETRY_DELIVERY: expr('matrix.delivery'),
      COMPOSE_FILE: 'docker-compose.telemetry.yml',
      COMPOSE_PROJECT_NAME: 'bench-telemetry',
    })
    expect(job['runs-on']).toBe('ubuntu-latest')
    expect(job['timeout-minutes']).toBe(360)
    const deadline = runs(job.steps[0]).match(
      /TELEMETRY_JOB_DEADLINE=\$\(\( \$\(date \+%s\) \+ (\d+) \* 60 \)\)/
    )
    expect(Number(deadline[1])).toBe(355)
    const baseline = job.steps.find((step) => step.with?.ref === '3.0.0-alpha.9e')
    expect(job.env.TELEMETRY_PREVIOUS_ROOT).toBe(
      `${expr('github.workspace')}/${baseline.with.path}`
    )
    for (const text of ['prepareDecoder.js', 'dockerComposeUp.sh', 'waitForKafka.js']) {
      expect(own(text).if).toBeUndefined()
      expect(runs(own(text))).toMatch(
        /^if \[ "\$TELEMETRY_DELIVERY" != true \]; then .*exit 0; fi$/m
      )
    }
    expect(runs(job.steps.at(-1))).toBe(
      'if [ "$TELEMETRY_DELIVERY" = true ]; then docker compose down --volumes --remove-orphans; fi\n'
    )
    expect(runs(own('report.js'))).toContain('--only "$BENCH_TELEMETRY_ROW"')
    expect(benchJobs.plan('').telemetry.filter(({ delivery }) => delivery)).toHaveLength(4)
  })

  test('joins the jobs reports into one report by row, with an error row for a missing or doubly run row', () => {
    const reports = tmp()
    const write = (key, rows, details = []) => {
      fs.mkdirSync(path.join(reports, `report-${key}`))
      fs.writeFileSync(
        path.join(reports, `report-${key}`, 'bench-report.json'),
        JSON.stringify({ meta: { key }, rows, details })
      )
    }
    const valid = (benchmark) => ({
      benchmark,
      arm: 'fork',
      metric: 'm',
      unit: 'ns',
      runs: 1,
      status: 'valid',
      median: 1,
      min: 1,
      max: 1,
    })
    const each = 'shareThroughput-eachMessage'
    write('ordinary', [valid('murmur2'), valid('consumer-eachBatch')])
    const detail = { status: 'valid', controls: [], notes: ['n'], raw: `bench-raw/${each}/` }
    write(`${each}-node22`, [valid(each)], [{ benchmark: each, ...detail }])
    write(`${each}-node24`, [valid(each)])
    write('consumer-eachBatch', [
      { ...valid('consumer-eachBatch'), status: 'not selected' },
      { ...valid('murmur2'), status: 'not selected' },
    ])
    const input = `murmur2,${each},consumer-eachBatch`
    const joined = benchJobs.collect(reports, {
      input,
      needs: { 'share-rows': { result: 'failure' }, 'consumer-rows': { result: 'success' } },
      download: 'success',
    })
    const of = (row) => joined.rows.filter((r) => r.benchmark === row)

    expect(of('murmur2')).toEqual([{ ...valid('murmur2'), source: 'ordinary' }])
    expect(of(each).map(({ arm, status, source }) => [arm, status, source])).toEqual([
      ['fork (node 22)', 'valid', `${each}-node22`],
      ['fork (node 24)', 'valid', `${each}-node24`],
      ['-', 'error', `${each}-node26`],
    ])
    expect(of(each)[2].reason).toBe('no report from its share-rows job (job failure)')
    expect(
      of('consumer-eachBatch').map(({ status, reason, source }) => [status, reason, source])
    ).toEqual([
      ['error', 'also run by the bench job (ordinary), which does not own it', 'ordinary'],
      ['error', 'the report of its consumer-rows job has no row for it', 'consumer-eachBatch'],
    ])
    expect(joined.details).toEqual([
      {
        ...detail,
        benchmark: `${each} (node 22)`,
        raw: `artifact bench-${each}-node22, bench-raw/${each}/`,
        source: `${each}-node22`,
      },
    ])
    const unselected = joined.rows.filter((r) => r.status === 'not selected')
    expect(unselected).toHaveLength(manifest.entries.length - 3)
    expect(joined.meta).toEqual({ selection: input, jobs: '4 of 5 reported' })

    const out = tmp()
    const code = benchJobs.main(['collect', reports, out], {
      env: { ROWS: input, NEEDS: '{}', DOWNLOAD: 'failure', GITHUB_SHA: 'abc' },
      error: (line) => {
        throw new Error(line)
      },
    })
    expect(code).toBe(0)
    const written = JSON.parse(fs.readFileSync(path.join(out, 'bench-report.json'), 'utf8'))
    expect(written.rows.find((r) => r.source === `${each}-node26`).reason).toBe(
      'no report from its share-rows job (job unknown, report download failure)'
    )
    const markdown = fs.readFileSync(path.join(out, 'bench-report.md'), 'utf8')
    expect(markdown).toContain('- commit: abc')
    expect(markdown).toContain(`### ${each} (node 22): valid`)
  })

  test('joins the lone report that the download extracts into the reports directory itself when one artifact matched', () => {
    const reports = tmp()
    const input = 'compare-produce-latency,compare-consume-throughput,compare-consume-throughput-1k'
    const row = (benchmark, arm, status, extra = {}) => ({
      benchmark,
      arm,
      metric: 'messagesPerSecond',
      unit: 'msg/s',
      runs: 5,
      status,
      median: 1,
      min: 1,
      max: 1,
      ...extra,
    })
    const rows = [
      row('compare-produce-latency', 'fork', 'valid', { ratio: 0.1 }),
      row('compare-produce-latency', 'kafkajs@2.2.4', 'valid'),
      row('compare-consume-throughput', 'fork', 'inconclusive', { reason: 'measured window' }),
      row('compare-consume-throughput-1k', 'kafkajs@2.2.4', 'error', { runs: 0, reason: 'x' }),
      { benchmark: 'murmur2', arm: '-', metric: '-', unit: '-', runs: 0, status: 'not selected' },
    ]
    const detail = { benchmark: 'compare-produce-latency', status: 'valid', raw: 'bench-raw/r/' }
    const report = { meta: { only: input.split(',') }, rows, details: [detail] }
    fs.writeFileSync(path.join(reports, 'bench-report.json'), JSON.stringify(report))
    fs.writeFileSync(path.join(reports, 'bench-report.md'), '# Benchmark report\n')

    const joined = benchJobs.collect(reports, {
      input,
      needs: { bench: { result: 'success' } },
      download: 'success',
    })

    expect(joined.meta.jobs).toBe('1 of 1 reported')
    expect(joined.rows.filter((r) => r.source === 'ordinary')).toEqual(
      rows.slice(0, 4).map((r) => ({ ...r, source: 'ordinary' }))
    )
    expect(joined.details).toEqual([
      { ...detail, raw: 'artifact bench-ordinary, bench-raw/r/', source: 'ordinary' },
    ])
  })

  test('the report job joins every job even after a failed one and uploads the joined report', () => {
    const job = workflow.jobs.report
    const join = stepIn(job, 'benchJobs.js collect')
    const download = job.steps.find((step) => step.uses?.startsWith('actions/download-artifact@'))

    expect(job.needs).toEqual(['plan', ...ROW_JOBS])
    expect(job.if).toBe(expr("always() && needs.plan.result == 'success'"))
    expect(download['continue-on-error']).toBe(true)
    expect(download.with.pattern).toBe('report-*')
    expect(join.env).toEqual({
      ROWS: expr('inputs.rows'),
      NEEDS: expr('toJSON(needs)'),
      DOWNLOAD: expr('steps.download.outcome'),
    })
    expect(uploads(job)).toEqual([
      expect.objectContaining({
        if: 'always()',
        with: expect.objectContaining({ name: 'bench-report' }),
      }),
    ])
  })

  test('runs 24 healthy adaptive comparison rounds and reports buffer refusals on a slow broker', () => {
    const row = (name) => manifest.entries.find((entry) => entry.name === name)
    const { command } = row('stickyBalance-healthy')

    expect(command[command.indexOf('--runs') + 1]).toBe('24')
    expect(row('stickyBalance-slow-broker').metrics).toContainEqual({
      key: 'refused',
      unit: 'records',
    })
  })

  test('shows the Markdown report in the job summary', () => {
    for (const name of [...ROW_JOBS, 'report']) {
      const summary = stepIn(workflow.jobs[name], 'GITHUB_STEP_SUMMARY')

      expect(runs(summary)).toMatch(/bench-report\.md/)
      expect(summary.if).toBe('always()')
    }
  })

  test('pins every action to a full commit SHA', () => {
    for (const step of steps.filter((step) => step.uses))
      expect(step.uses).toMatch(/^[\w.-]+\/[\w.-]+@[0-9a-f]{40}$/)
    expect(raw).toContain('actions/setup-java@cf277c60eb25467037889841efdb72551f06f6c3 # v4.9.1')
  })

  test('passes no expression into a run script', () => {
    for (const step of steps) expect(runs(step)).not.toContain('${{')
    expect(stepWith('dockerComposeUp.sh')).toBeDefined()
  })
})
