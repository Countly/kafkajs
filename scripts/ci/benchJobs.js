/**
 * The benchmark workflow's row plan and report collector (`.github/workflows/bench.yml`).
 *
 * A dispatch names manifest rows (empty: every row). `plan` gives each selected row exactly one
 * owning job, from the script its manifest command runs: the consumer controller (one row per job,
 * and the audit after them), the share throughput session (one job per row and Node version), the
 * Snappy session, the telemetry adapter (one row per job), or the ordinary report job for every
 * other row. Every job writes its own report; `collect` joins them into one report by manifest
 * row, so a selected row that no job reported, or that a job which does not own it also ran, is an
 * `error` row with that reason.
 *
 * Usage: node scripts/ci/benchJobs.js plan                       (env ROWS; prints job outputs)
 *        node scripts/ci/benchJobs.js collect <reports> <out>    (env ROWS, NEEDS, DOWNLOAD)
 * `<reports>` holds one `report-<unit>/bench-report.json` per job that reported (or, when only one
 * job reported, that job's `bench-report.json` itself); `NEEDS` is the workflow's `needs` context
 * as JSON. Exit codes: 0 done, 2 a bad argument or an unknown row name.
 */
const fs = require('node:fs')
const path = require('node:path')
const defaultManifest = require('../benchmarks/manifest.json')
const { renderMarkdown } = require('../benchmarks/report')
const { ROWS: CONSUMER_ROWS, HEAD } = require('../benchmarks/adapters/consumerSession')

/** The job that runs a manifest command's script; every other script runs in `bench`. */
const OWNERS = {
  'consumerSession.js': 'consumer-rows',
  'shareThroughput.js': 'share-rows',
  'snappyCodec.js': 'snappy-row',
  'telemetrySession.js': 'telemetry-rows',
}

/** The Node versions each share throughput row is measured on. */
const SHARE_NODES = { shareThroughput: [24], 'shareThroughput-eachMessage': [22, 24, 26] }

/**
 * @typedef {{ name: string, command: string[], broker: boolean, adapter?: string,
 *   contracts?: string[], namedOnly?: boolean }} Entry a manifest entry; a `namedOnly` entry runs
 *   only when the dispatch names it, so an empty selection leaves it out
 * @typedef {{ key: string, job: string, rows: string[], label?: string }} Unit one job's report:
 *   `key` names its artifacts (`report-<key>`, `bench-<key>`), `label` tells apart units of one row
 */

/**
 * @param {Entry} entry
 * @returns {string} the workflow job that owns the entry
 */
const ownerOf = (entry) => {
  const owner = OWNERS[path.basename(entry.command.find((arg) => arg.endsWith('.js')))]
  if (owner === 'consumer-rows' && entry.command.includes('--audit')) return 'consumer-audit'
  return owner ?? 'bench'
}

/**
 * @param {string} name a consumer row
 * @returns {{ key: string, row: string, compose: string, trees: string }} its matrix entry:
 *   `compose` is the cluster it needs (empty for a broker-free row), `trees` the archived trees
 */
const consumerUnit = (name) => {
  const row = CONSUMER_ROWS[name]
  const compose = !row.broker
    ? ''
    : row.adapter === 'fetchSessions'
      ? 'docker-compose.racks.yml'
      : 'docker-compose.yml'
  return { key: name, row: name, compose, trees: row.trees.filter((t) => t !== HEAD).join(',') }
}

/**
 * Plans a dispatch: which rows run, in which job.
 * Selecting the consumer audit without any consumer row it audits selects all of those rows, so
 * the audit never reads evidence that no job produced.
 * @param {string} input comma-separated manifest row names; empty selects every row that is not
 *   `namedOnly`
 * @param {{ entries: Entry[] }} [manifest]
 * @returns {{ selected: string[], ordinary: string[], share: object[], snappy: object[],
 *   consumer: object[], audit: object[], telemetry: object[], units: Unit[] }}
 * @throws {Error} for a name that is no manifest row
 */
const plan = (input, manifest = defaultManifest) => {
  const wanted = String(input ?? '')
    .split(',')
    .map((name) => name.trim())
    .filter(Boolean)
  const unknown = wanted.find((name) => !manifest.entries.some((entry) => entry.name === name))
  if (unknown !== undefined) throw new Error(`rows names no manifest row ${unknown}`)
  const by = (job) => manifest.entries.filter((entry) => ownerOf(entry) === job)
  const picked = (job) =>
    by(job)
      .filter((entry) => (wanted.length ? wanted.includes(entry.name) : !entry.namedOnly))
      .map(({ name }) => name)

  const audit = picked('consumer-audit')
  const audited = (names) => names.filter((name) => CONSUMER_ROWS[name].contracts.length)
  let consumer = picked('consumer-rows')
  if (audit.length && !audited(consumer).length)
    consumer = by('consumer-rows')
      .map(({ name }) => name)
      .filter((name) => consumer.includes(name) || CONSUMER_ROWS[name].contracts.length)

  const ordinary = picked('bench')
  const share = picked('share-rows').flatMap((row) =>
    SHARE_NODES[row].map((node) => ({ key: `${row}-node${node}`, row, node }))
  )
  const snappy = picked('snappy-row').map((row) => ({ key: row, row }))
  const telemetry = picked('telemetry-rows').map((row) => ({
    key: row,
    row,
    delivery: manifest.entries.find((entry) => entry.name === row).broker,
  }))
  const auditUnits = audit.map((row) => ({ key: row, row, scope: audited(consumer).join(',') }))
  const one = (job) => (unit) => ({ key: unit.key, job, rows: [unit.row] })
  const units = [
    ...(ordinary.length ? [{ key: 'ordinary', job: 'bench', rows: ordinary }] : []),
    ...share.map((unit) => ({ ...one('share-rows')(unit), label: `node ${unit.node}` })),
    ...snappy.map(one('snappy-row')),
    ...consumer.map((row) => one('consumer-rows')({ key: row, row })),
    ...auditUnits.map(one('consumer-audit')),
    ...telemetry.map(one('telemetry-rows')),
  ]
  return {
    selected: [...new Set(units.flatMap(({ rows }) => rows))],
    ordinary,
    share,
    snappy,
    consumer: consumer.map(consumerUnit),
    audit: auditUnits,
    telemetry,
    units,
  }
}

/**
 * @param {ReturnType<typeof plan>} planned
 * @returns {string} the plan as `GITHUB_OUTPUT` lines: `ordinary` a comma-separated list, every
 *   other job a JSON matrix list (`[]` when it runs nothing)
 */
const outputs = (planned) =>
  [
    `ordinary=${planned.ordinary.join(',')}`,
    ...['share', 'snappy', 'consumer', 'audit', 'telemetry'].map(
      (job) => `${job}=${JSON.stringify(planned[job])}`
    ),
  ].join('\n') + '\n'

/**
 * @param {string} file
 * @returns {object|undefined} the parsed report, undefined when it is absent or unreadable
 */
const readReport = (file) => {
  try {
    return JSON.parse(fs.readFileSync(file, 'utf8'))
  } catch {
    return undefined
  }
}

/**
 * Reads each unit's report from the downloaded artifacts. The download action puts each artifact
 * in a `report-<unit>` directory, except when exactly one artifact matched: then it extracts that
 * one into `reports` itself. Such a lone report belongs to the one unit whose rows hold every row
 * it ran; when no unit or more than one does, no unit gets it.
 * @param {string} reports the download directory
 * @param {Unit[]} units
 * @returns {Map<string, object>} the parsed reports by unit key
 */
const readReports = (reports, units) => {
  const found = new Map()
  for (const unit of units) {
    const report = readReport(path.join(reports, `report-${unit.key}`, 'bench-report.json'))
    if (report) found.set(unit.key, report)
  }
  const lone = readReport(path.join(reports, 'bench-report.json'))
  if (!Array.isArray(lone?.rows)) return found
  const ran = lone.rows.filter((r) => r.status !== 'not selected').map((r) => r.benchmark)
  const owners = units.filter((unit) => ran.every((row) => unit.rows.includes(row)))
  if (owners.length === 1 && !found.has(owners[0].key)) found.set(owners[0].key, lone)
  return found
}

/**
 * Joins the jobs' reports into one, by manifest row. Each unit's selected rows come from its own
 * report; a missing report, or one without the row, makes the row `error` with the job's result.
 * A row a report ran although its unit does not own it is an `error` row of that unit. Rows the
 * dispatch left out are `not selected`.
 * @param {string} reports the directory of downloaded `report-<unit>` artifacts (`readReports`)
 * @param {{ input: string, needs?: Record<string, { result?: string }>, download?: string,
 *   manifest?: { entries: Entry[] }, meta?: object }} options
 * @returns {{ meta: object, units: object[], rows: object[], details: object[] }}
 */
const collect = (
  reports,
  { input, needs = {}, download, manifest = defaultManifest, meta = {} }
) => {
  const planned = plan(input, manifest)
  const found = readReports(reports, planned.units)
  const rows = []
  const details = []
  const units = planned.units.map((unit) => {
    const report = found.get(unit.key)
    const result = needs[unit.job]?.result ?? 'unknown'
    const named = (row) => (unit.label ? `${row} (${unit.label})` : row)
    const error = (benchmark, reason) => ({
      benchmark,
      arm: '-',
      metric: '-',
      unit: '-',
      runs: 0,
      status: 'error',
      reason,
      source: unit.key,
    })
    if (!Array.isArray(report?.rows)) {
      const why = `no report from its ${unit.job} job (job ${result}${download && download !== 'success' ? `, report download ${download}` : ''})`
      rows.push(...unit.rows.map((row) => error(row, why)))
      return { key: unit.key, job: unit.job, result, reported: false }
    }
    for (const row of unit.rows) {
      const own = report.rows.filter((r) => r.benchmark === row && r.status !== 'not selected')
      if (!own.length) rows.push(error(row, `the report of its ${unit.job} job has no row for it`))
      for (const r of own)
        rows.push({
          ...r,
          ...(unit.label && { arm: `${r.arm} (${unit.label})` }),
          source: unit.key,
        })
    }
    const strays = new Set(
      report.rows
        .filter((r) => !unit.rows.includes(r.benchmark) && r.status !== 'not selected')
        .map((r) => r.benchmark)
    )
    for (const row of strays)
      rows.push(error(row, `also run by the ${unit.job} job (${unit.key}), which does not own it`))
    for (const detail of report.details ?? [])
      if (unit.rows.includes(detail.benchmark))
        details.push({
          ...detail,
          benchmark: named(detail.benchmark),
          raw: `artifact bench-${unit.key}, ${detail.raw}`,
          source: unit.key,
        })
    return { key: unit.key, job: unit.job, result, reported: true, meta: report.meta }
  })
  for (const { name, namedOnly } of manifest.entries)
    if (!planned.selected.includes(name))
      rows.push({
        benchmark: name,
        arm: '-',
        metric: '-',
        unit: '-',
        runs: 0,
        status: 'not selected',
        reason:
          namedOnly && !input?.trim()
            ? 'runs only when named in the rows input'
            : 'left out by the rows input',
      })
  const joined = {
    ...meta,
    selection: input?.trim() ? input.trim() : 'every row',
    jobs: `${units.filter((u) => u.reported).length} of ${units.length} reported`,
  }
  return { meta: joined, units, rows, details }
}

/**
 * @param {string[]} argv `plan`, or `collect <reports> <out>`
 * @param {{ env?: object, log?: Function, error?: Function }} [io]
 * @returns {number} 0 when done, 2 for a bad argument or an unknown row name
 */
const main = (argv, { env = process.env, log = console.log, error = console.error } = {}) => {
  try {
    if (argv[0] === 'plan' && argv.length === 1) {
      log(outputs(plan(env.ROWS)).trimEnd())
      return 0
    }
    if (argv[0] === 'collect' && argv.length === 3) {
      const [, reports, out] = argv
      const report = collect(reports, {
        input: env.ROWS,
        needs: env.NEEDS ? JSON.parse(env.NEEDS) : {},
        download: env.DOWNLOAD,
        meta: {
          ...(env.GITHUB_SHA && { commit: env.GITHUB_SHA }),
          ...(env.GITHUB_RUN_ID && { run: env.GITHUB_RUN_ID }),
          date: new Date().toISOString(),
        },
      })
      fs.mkdirSync(out, { recursive: true })
      fs.writeFileSync(path.join(out, 'bench-report.json'), `${JSON.stringify(report, null, 2)}\n`)
      fs.writeFileSync(
        path.join(out, 'bench-report.md'),
        renderMarkdown(report.rows, report.meta, report.details)
      )
      return 0
    }
  } catch (failure) {
    error(failure.message)
    return 2
  }
  error('usage: benchJobs.js plan | benchJobs.js collect <reports> <out>')
  return 2
}

module.exports = { OWNERS, SHARE_NODES, ownerOf, plan, outputs, collect, main }

if (require.main === module) {
  process.exitCode = main(process.argv.slice(2))
}
