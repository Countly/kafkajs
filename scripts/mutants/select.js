/**
 * Chooses the records one mutation run executes and archives that choice with its provenance.
 *
 * Every historical unit record runs (stale ones too: the runner reports them `stale`). Generated records
 * are sampled per area: `perArea` of them, spread evenly over the area's (target file, operator) groups,
 * in an order fixed by `seed`, so a small group gives its unused slots to the others. Live records go to
 * their own inventory for the guarded live job. The historical inventory's `excluded` entries stay as
 * `excluded: <reason>` whatever the area filter. Every other record stays in the selection, labelled `not-run: sampling` or
 * `not-run: area`, so the report never shrinks its denominator.
 *
 * Writes `<out-dir>/selection.json` (provenance and every record with its disposition),
 * `<out-dir>/unit.json` and `<out-dir>/live.json` (runner inventories, `{ records }`).
 *
 * Usage: node scripts/mutants/select.js --historical <inventory.json> --generated <inventory.json>
 *   --sha <tree> --run-id <id> --seed <seed> [--per-area <n>|all] [--areas all|<a,b>] --shards <n>
 *   --time-cap <minutes> --out-dir <dir> [--github-output <file>]
 */
const crypto = require('crypto')
const fs = require('fs')
const path = require('path')

/** The longest one record can take after the time cap stopped new ones: 2 × (showConfig 120 s + 30 s) + 2 × (witness 600 s + 30 s). */
const RECORD_HEADROOM_MINUTES = 26
/** Checkout, install, copying the tree and upload. */
const CLEANUP_MINUTES = 10

/** Selects and archives the records of one mutation run. */
class MutantSelection {
  /**
   * @param {string} seed
   * @param {string} key
   * @returns {string} a sort key fixed by the seed
   */
  static rank(seed, key) {
    return crypto.createHash('sha256').update(`${seed}\u0000${key}`).digest('hex')
  }

  /**
   * Samples one area's generated records: round-robin over its (target, operator) groups, both the
   * groups and their members in seeded order.
   * @param {object[]} records the area's generated records
   * @param {number} count how many to keep
   * @param {string} seed
   * @returns {Set<string>} the kept ids
   */
  static sample(records, count, seed) {
    const groups = new Map()
    for (const r of records) {
      const key = `${r.target}\u0000${r.operator}`
      if (!groups.has(key)) groups.set(key, [])
      groups.get(key).push(r)
    }
    const queues = [...groups.entries()]
      .sort(([a], [b]) =>
        MutantSelection.rank(seed, a).localeCompare(MutantSelection.rank(seed, b))
      )
      .map(([, members]) =>
        members.sort((a, b) =>
          MutantSelection.rank(seed, a.id).localeCompare(MutantSelection.rank(seed, b.id))
        )
      )
    const kept = new Set()
    for (let round = 0; kept.size < count && queues.some((q) => q.length > round); round++)
      for (const q of queues) if (q[round] && kept.size < count) kept.add(q[round].id)
    return kept
  }

  /**
   * @param {object} options
   * @param {object[]} options.historical the refreshed historical records
   * @param {{ id: string, target: string, reason: string }[]} [options.excluded] the historical
   *   inventory's exclusions; the area is the id's prefix
   * @param {object[]} options.generated the generator's records
   * @param {number|'all'} options.perArea generated records kept per area
   * @param {string} options.seed
   * @param {string} options.areas `all`, or comma-separated area names
   * @param {string} options.sha the tree under test
   * @param {string} options.runId the workflow run
   * @param {number} options.shards the number of unit shards
   * @param {() => number} [options.now] the clock
   * @returns {{ selection: object, unit: { records: object[] }, live: { records: object[] } }}
   */
  static select({
    historical,
    excluded = [],
    generated,
    perArea,
    seed,
    areas,
    sha,
    runId,
    shards,
    now = Date.now,
  }) {
    const wanted = areas === 'all' ? null : new Set(areas.split(',').map((a) => a.trim()))
    const inArea = (r) => !wanted || wanted.has(r.area)
    const kept = new Set()
    const byArea = new Map()
    for (const r of generated.filter(inArea)) {
      if (!byArea.has(r.area)) byArea.set(r.area, [])
      byArea.get(r.area).push(r)
    }
    for (const records of byArea.values()) {
      const ids =
        perArea === 'all'
          ? records.map((r) => r.id)
          : MutantSelection.sample(records, perArea, seed)
      for (const id of ids) kept.add(id)
    }
    const unit = []
    const live = []
    const records = []
    const sources = [
      ...historical.map((r) => ['historical', r]),
      ...excluded.map((e) => [
        'historical',
        { ...e, area: e.id.split('/')[0], commandClass: 'unit', excluded: e.reason },
      ]),
      ...generated.map((r) => ['generated', r]),
    ]
    for (const [source, r] of sources) {
      let disposition
      if (r.excluded) disposition = `excluded: ${r.excluded}`
      else if (!inArea(r)) disposition = 'not-run: area'
      else if (r.commandClass === 'live') disposition = 'live'
      else if (source === 'generated' && !kept.has(r.id)) disposition = 'not-run: sampling'
      else disposition = 'run'
      if (disposition === 'live') live.push(r)
      if (disposition === 'run') unit.push(r)
      records.push({
        id: r.id,
        area: r.area,
        source,
        commandClass: r.commandClass,
        disposition,
        target: r.target,
        line: r.line ?? null,
        operator: r.operator ?? null,
        original: r.original ?? null,
        mutated: r.mutated ?? null,
        spec: r.spec ?? null,
        title: r.title ?? null,
      })
    }
    const selection = {
      sha,
      runId,
      seed,
      perArea,
      areas,
      shards,
      startedAt: new Date(now()).toISOString(),
      records,
    }
    return { selection, unit: { records: unit }, live: { records: live } }
  }

  /**
   * The shard matrix and each shard's job timeout. The time cap only stops new records from starting,
   * so the timeout adds the longest record and the job's cleanup.
   * The largest cap, 300 minutes, keeps the timeout within GitHub's 360-minute job limit.
   * @param {{ shards: string|number, capMinutes: string|number }} input the dispatch inputs
   * @returns {{ shards: number[], capMinutes: number, shardTimeout: number }}
   * @throws {Error} when shards is not 1–32 or the cap is not 1–300 minutes
   */
  static plan({ shards, capMinutes }) {
    const n = Number(shards)
    const cap = Number(capMinutes)
    if (!Number.isInteger(n) || n < 1 || n > 32)
      throw new Error(`shards must be 1–32, got ${shards}`)
    if (!Number.isInteger(cap) || cap < 1 || cap > 300)
      throw new Error(`the time cap must be 1–300 minutes, got ${capMinutes}`)
    return {
      shards: Array.from({ length: n }, (_, i) => i + 1),
      capMinutes: cap,
      shardTimeout: cap + RECORD_HEADROOM_MINUTES + CLEANUP_MINUTES,
    }
  }

  /**
   * The CLI.
   * @param {string[]} args the command-line arguments
   * @throws {Error} on a missing or malformed argument
   */
  static main(args) {
    const option = (name, fallback) => {
      const at = args.indexOf(name)
      if (at !== -1 && args[at + 1] !== undefined) return args[at + 1]
      if (fallback !== undefined) return fallback
      throw new Error(`${name} is required`)
    }
    const read = (file) => JSON.parse(fs.readFileSync(file, 'utf8'))
    const historical = read(option('--historical'))
    const plan = MutantSelection.plan({
      shards: option('--shards'),
      capMinutes: option('--time-cap'),
    })
    const perAreaText = option('--per-area', '40')
    const perArea = perAreaText === 'all' ? 'all' : Number(perAreaText)
    if (perArea !== 'all' && !(Number.isInteger(perArea) && perArea > 0))
      throw new Error(`--per-area needs a positive integer or all, got ${perAreaText}`)
    const { selection, unit, live } = MutantSelection.select({
      historical: historical.records,
      excluded: historical.excluded || [],
      generated: read(option('--generated')).records,
      perArea,
      seed: option('--seed'),
      areas: option('--areas', 'all'),
      sha: option('--sha'),
      runId: option('--run-id'),
      shards: plan.shards.length,
    })
    if (!/^[0-9a-f]{40}$/.test(selection.sha))
      throw new Error(`--sha needs a commit SHA, got ${selection.sha}`)
    const dir = option('--out-dir')
    fs.mkdirSync(dir, { recursive: true })
    fs.writeFileSync(path.join(dir, 'selection.json'), JSON.stringify(selection, null, 1))
    fs.writeFileSync(path.join(dir, 'unit.json'), JSON.stringify(unit))
    fs.writeFileSync(path.join(dir, 'live.json'), JSON.stringify(live))
    const output = option('--github-output', '')
    if (output)
      fs.appendFileSync(
        output,
        [
          `sha=${selection.sha}`,
          `shards=${JSON.stringify(plan.shards)}`,
          `cap=${plan.capMinutes}`,
          `shard-timeout=${plan.shardTimeout}`,
          '',
        ].join('\n')
      )
    console.log(
      `selected ${unit.records.length} unit and ${live.records.length} live of ${selection.records.length} records`
    )
  }
}

module.exports = { MutantSelection }

if (require.main === module) {
  try {
    MutantSelection.main(process.argv.slice(2))
  } catch (error) {
    console.error(`select: ${error.message}`)
    process.exitCode = 1
  }
}
