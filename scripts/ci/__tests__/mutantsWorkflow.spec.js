const fs = require('node:fs')
const path = require('node:path')
const yaml = require('js-yaml')

const { MutationReport } = require('../../mutants/report')
const { MutantSelection } = require('../../mutants/select')

const FILE = path.join(__dirname, '..', '..', '..', '.github', 'workflows', 'mutants.yml')

/** @returns {object} the parsed mutants workflow */
const workflow = () => yaml.load(fs.readFileSync(FILE, 'utf8'))

/**
 * @param {object} job a workflow job
 * @param {string} text a fragment of the step's run script
 * @returns {number} the index of the first step whose script contains it, or -1
 */
const stepIndex = (job, text) => job.steps.findIndex((step) => step.run?.includes(text))

describe('mutants workflow', () => {
  test('mutants runs only on workflow_dispatch', () => {
    const config = workflow()
    expect(Object.keys(config.on)).toEqual(['workflow_dispatch'])
    const { inputs } = config.on.workflow_dispatch
    expect(inputs.shards.default).toBe('16')
    expect(inputs['time-cap-minutes'].default).toBe('40')
    expect(inputs.areas.default).toBe('all')
    expect(inputs['per-area'].default).toBe('40')
    expect(Object.keys(inputs).sort()).toEqual(
      ['areas', 'per-area', 'ref', 'seed', 'shards', 'time-cap-minutes'].sort()
    )
  })

  test('every shard has a timeout of its cap plus the longest record and cleanup', () => {
    const { jobs } = workflow()
    const plan = jobs.prepare.steps[stepIndex(jobs.prepare, 'select.js')]
    expect(plan.env).toMatchObject({
      SHARDS: `\${{ inputs.shards }}`,
      CAP: `\${{ inputs.time-cap-minutes }}`,
    })
    expect(plan.run).toContain('--shards "$SHARDS" --time-cap "$CAP"')
    expect(plan.run).toContain('--github-output "$GITHUB_OUTPUT"')
    expect(plan.run).toContain('--historical scripts/mutants/inventory.json')
    expect(jobs.prepare.outputs['shard-timeout']).toBe(`\${{ steps.select.outputs.shard-timeout }}`)

    const shard = jobs.shard
    expect(shard['timeout-minutes']).toBe(`\${{ fromJSON(needs.prepare.outputs.shard-timeout) }}`)
    expect(shard.strategy).toEqual({
      'fail-fast': false,
      matrix: { shard: `\${{ fromJSON(needs.prepare.outputs.shards) }}` },
    })
    const run = shard.steps[stepIndex(shard, 'runner.js')]
    expect(run.env).toMatchObject({ CAP: `\${{ needs.prepare.outputs.cap }}` })
    expect(run.run).toContain('--time-cap "$CAP"')
    expect(run.run).toContain('--shard "$SHARD/$SHARDS"')
    expect(MutantSelection.plan({ shards: '16', capMinutes: '40' }).shardTimeout).toBe(40 + 36)
  })

  test('every shard checks out the selected tree on its own runner', () => {
    const { jobs } = workflow()
    for (const name of ['shard', 'live', 'report']) {
      const checkout = jobs[name].steps.find((s) => s.uses?.startsWith('actions/checkout@'))
      expect(checkout.with).toMatchObject({
        ref: `\${{ needs.prepare.outputs.sha }}`,
        'persist-credentials': false,
      })
    }
    const install = jobs.shard.steps[stepIndex(jobs.shard, 'npm ci')]
    expect(install.run).toBe('npm ci --ignore-scripts')
    expect(jobs.shard.services).toBeUndefined()
    expect(JSON.stringify(jobs.shard)).not.toMatch(/docker|compose|testWithKafka/)
  })

  test('the report job runs always and uploads every shard file', () => {
    const { jobs } = workflow()
    const report = jobs.report
    expect(report.needs).toEqual(['prepare', 'shard', 'live'])
    expect(report.if).toBe("always() && needs.prepare.result == 'success'")
    const download = report.steps.find((s) => s.uses?.startsWith('actions/download-artifact@'))
    expect(download.with).toEqual({
      pattern: 'mutants-*',
      'merge-multiple': true,
      path: 'artifacts',
    })
    const write = report.steps[stepIndex(report, 'report.js')]
    expect(write.run).toContain('artifacts/mutants-*.jsonl')
    expect(write.run).toContain('--evidence artifacts/live-guard.json')
    expect(write.run).toContain('--selection artifacts/selection.json')
    const summary = report.steps[stepIndex(report, 'GITHUB_STEP_SUMMARY')]
    expect(summary.run).toContain('report/mutation-report.md')
    const upload = report.steps.find((s) => s.uses?.startsWith('actions/upload-artifact@'))
    expect(upload.if).toBe('always()')
    expect(upload.with.path.split('\n').filter(Boolean)).toEqual([
      'report/mutation-report.md',
      'report/mutation-report.json',
      'artifacts/selection.json',
      'artifacts/live-guard.json',
      'artifacts/mutants-*.jsonl',
    ])
    const shardUpload = jobs.shard.steps.find((s) => s.uses?.startsWith('actions/upload-artifact@'))
    expect(shardUpload).toMatchObject({
      if: 'always()',
      with: {
        name: `mutants-shard-\${{ matrix.shard }}`,
        path: `mutants-\${{ matrix.shard }}.jsonl`,
      },
    })
  })

  test('the workflow grants only contents: read', () => {
    const config = workflow()
    expect(config.permissions).toEqual({ contents: 'read' })
    for (const job of Object.values(config.jobs)) expect(job.permissions).toBeUndefined()
  })

  test('every action is pinned by a full commit SHA', () => {
    const uses = Object.values(workflow().jobs).flatMap((job) =>
      job.steps.filter((s) => s.uses).map((s) => s.uses)
    )
    expect(uses.length).toBeGreaterThan(0)
    for (const ref of uses) expect(ref).toMatch(/^actions\/[a-z-]+@[0-9a-f]{40}$/)
  })

  test('the live table is separate and counts a live kill only with valid guard evidence', () => {
    const sha = 'a'.repeat(40)
    const unitRecord = {
      id: 'u',
      area: 'offsets',
      commandClass: 'unit',
      disposition: 'run',
      target: 'src/a.js',
      line: 1,
      operator: 'constant',
      original: '1',
      mutated: '2',
      spec: 'src/a.spec.js',
    }
    const liveRecord = {
      ...unitRecord,
      id: 'D/D12',
      area: 'D',
      commandClass: 'live',
      disposition: 'live',
      operator: 'patch',
    }
    const guard = {
      at: '2026-11-17T10:30:00.000Z',
      lane: 'docker',
      node: 'v26.0.0',
      brokerVersion: '4.4.0',
      wireVersions: { Produce: [3, 12] },
      config: {
        compose: 'docker-compose.yml',
        env: { NODE_ENV: 'production', BROKER_BOUNCE_E2E: '1' },
      },
      composeImages: { kafka1: 'apache/kafka:4.4.0@sha256:1' },
      runId: '7',
      sha,
    }
    const build = (evidence) =>
      new MutationReport({
        selection: {
          sha,
          runId: '7',
          shards: 1,
          startedAt: '2026-11-17T10:00:00.000Z',
          records: [unitRecord, liveRecord],
        },
        ledgers: [
          { file: 'mutants-1.jsonl', rows: [{ id: 'u', verdict: 'killed', reason: '' }] },
          { file: 'mutants-live.jsonl', rows: [{ id: 'D/D12', verdict: 'killed', reason: '' }] },
        ],
        evidence,
        runId: '7',
        now: () => Date.parse('2026-11-17T11:00:00.000Z'),
      }).build()
    const valid = build(guard)
    expect(valid.unit.areas.map((a) => a.area)).toEqual(['offsets'])
    expect(valid.live.areas).toEqual([
      expect.objectContaining({ area: 'D', killed: 1, invalid: 0 }),
    ])
    expect(valid.live.evidence).toBe('valid')
    const md = MutationReport.markdown(valid)
    expect(md.indexOf('## Live mutants')).toBeGreaterThan(md.indexOf('## Unit mutants by area'))
    expect(md.split('## Live mutants')[0]).not.toContain('| D |')
    expect(md.split('## Live mutants')[1]).toContain(
      '| D | P2 | 1 | 1 | 0 | 0 | 0 | 0 | 0 | 100.0% |'
    )

    const timeless = build({ ...guard, at: '' })
    expect(timeless.live.areas[0]).toMatchObject({ killed: 0, invalid: 1 })
    expect(timeless.live.evidence).toBe('guard evidence lacks at')
    expect(MutationReport.markdown(timeless)).toContain('OPEN: D/D12')
  })

  test('the live job starts its own Compose cluster with the broker-bounce environment', () => {
    const { jobs } = workflow()
    const live = jobs.live
    expect(live.needs).toBe('prepare')
    expect(live.env).toEqual({ KAFKA_VERSION: '4.4.0', COMPOSE_FILE: 'docker-compose.yml' })
    const install = live.steps[stepIndex(live, 'npm ci')]
    expect(install.env).toBeUndefined()
    const start = live.steps[stepIndex(live, 'testWithKafka.sh')]
    expect(start.env).toEqual({ NODE_ENV: 'production', BROKER_BOUNCE_E2E: '1', DO_NOT_STOP: '1' })
    expect(start.run).toBe(
      './scripts/testWithKafka.sh "node scripts/mutants/liveEvidence.js --out live-guard.json"'
    )
    const stop = live.steps[stepIndex(live, 'docker compose')]
    expect(stop).toMatchObject({ if: 'always()' })
    expect(stop.run).toContain('down --volumes --remove-orphans')
  })

  test('the live job writes guard evidence before running the live class with a 75-minute timeout', () => {
    const { jobs } = workflow()
    const live = jobs.live
    expect(live['timeout-minutes']).toBe(2 * 30 + 15)
    const evidence = stepIndex(live, 'liveEvidence.js')
    const run = stepIndex(live, 'runner.js')
    expect(evidence).toBeGreaterThan(-1)
    expect(run).toBeGreaterThan(evidence)
    expect(live.steps[run].env).toEqual({ NODE_ENV: 'production', BROKER_BOUNCE_E2E: '1' })
    expect(live.steps[run].run).toBe(
      'node scripts/mutants/runner.js selection/live.json --live-evidence live-guard.json --out mutants-live.jsonl'
    )
    const upload = live.steps.find((s) => s.uses?.startsWith('actions/upload-artifact@'))
    expect(upload).toMatchObject({
      if: 'always()',
      with: { name: 'mutants-live', path: 'live-guard.json\nmutants-live.jsonl\n' },
    })
  })
})
