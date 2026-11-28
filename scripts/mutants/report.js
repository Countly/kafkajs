/**
 * Turns one mutation run's selection and ledgers into a report a non-expert can read:
 * `mutation-report.md` (for the job summary) and `mutation-report.json`.
 *
 * Every selected record gets exactly one verdict. A record with no ledger row (its shard was cancelled,
 * cut off or failed) is `not-run`, never killed; so are records left out by sampling or the area filter.
 * An inventory exclusion is `excluded`, with its reason.
 * A live verdict counts only when the live job's guard evidence validates here, independently of the
 * runner: every field present, the selected tree's SHA, and a time inside this run.
 * The score is killed / (killed + survived); stale, invalid, excluded and not-run records are listed,
 * never scored.
 * Survivors are ranked high risk first (a removed check or boundary in a P1 area, or any live survivor),
 * then by area severity and operator class.
 * The report gates nothing: it always exits 0 once written.
 *
 * Usage: node scripts/mutants/report.js --selection <selection.json> [--evidence <live-guard.json>]
 *   [--run-id <id>] --out-dir <dir> <ledger.jsonl>...
 */
const fs = require('fs')
const path = require('path')
const { LiveEvidence } = require('./liveEvidence')

const VERDICTS = ['killed', 'survived', 'stale', 'invalid', 'not-run']
const NOT_RUN = 'not-run: '
const EXCLUDED = 'excluded: '
const NO_ROW = 'no ledger row: its shard was cancelled, cut off or failed'
/** Generated areas by risk; any other area (the historical consumer sets) is P2. */
const SEVERITY = {
  transactions: 'P1',
  offsets: 'P1',
  'group-timing': 'P1',
  share: 'P1',
  protocol: 'P1',
  'producer-send': 'P1',
  network: 'P1',
  'fetch-loop': 'P1',
  compression: 'P1',
  'aged-metadata': 'P2',
  cluster: 'P2',
  admin: 'P2',
}
/**
 * Operator classes, riskiest first; a hand-written historical patch has no operator class. A live record
 * is classed `live` whatever its operator.
 */
const OPERATOR_CLASS = {
  'removed-call': 'removed check',
  'negated-condition': 'removed check',
  'early-return': 'removed check',
  boundary: 'boundary',
  constant: 'constant',
}
const CLASS_ORDER = ['live', 'removed check', 'boundary', 'constant', 'hand-written']
/** Survivors listed in the markdown; the JSON lists all. */
const MARKDOWN_SURVIVORS = 500

/** One run's report. */
class MutationReport {
  /**
   * @param {object} input
   * @param {object} input.selection `select.js`'s selection.json
   * @param {{ file: string, rows: object[] }[]} input.ledgers the runner's ledgers, unit and live
   * @param {object|null} [input.evidence] the live job's guard evidence
   * @param {string} [input.runId] this workflow run; the selection must belong to it
   * @param {() => number} [input.now] the clock
   */
  constructor({ selection, ledgers, evidence = null, runId, now = Date.now }) {
    this.selection = selection
    this.ledgers = ledgers
    this.evidence = evidence
    this.runId = runId
    this.now = now
  }

  /**
   * @param {string} area
   * @returns {'P1'|'P2'|'P3'} the area's severity
   */
  static severity(area) {
    return SEVERITY[area] || 'P2'
  }

  /**
   * @param {string|null} operator
   * @returns {string} the operator's class
   */
  static operatorClass(operator) {
    return OPERATOR_CLASS[operator] || 'hand-written'
  }

  /**
   * @param {string|null} original
   * @param {string|null} mutated
   * @returns {string} the sentence naming the assertion the selected witnesses do not make
   */
  static missingAssertion(original, mutated) {
    const was = String(original ?? '').trim()
    const now = String(mutated ?? '').trim()
    return now === ''
      ? `No selected witness produced a valid kill when \`${was}\` is removed.`
      : `No selected witness produced a valid kill when \`${was}\` becomes \`${now}\`.`
  }

  /**
   * Builds the report.
   * @returns {object} the JSON report
   * @throws {Error} when the selection belongs to another run
   */
  build() {
    const { selection } = this
    if (this.runId !== undefined && String(selection.runId) !== String(this.runId))
      throw new Error(`the selection belongs to run ${selection.runId}, not ${this.runId}`)
    const rows = new Map()
    const selected = new Set(selection.records.map((r) => r.id))
    let foreign = 0
    for (const { rows: ledger } of this.ledgers)
      for (const row of ledger) {
        if (!selected.has(row.id)) foreign++
        else rows.set(row.id, [...(rows.get(row.id) || []), row])
      }
    const refusal = LiveEvidence.refusal(this.evidence, {
      sha: selection.sha,
      since: Date.parse(selection.startedAt),
      until: this.now(),
    })
    const records = selection.records.map((r) => this.judge(r, rows.get(r.id) || [], refusal))
    const section = (list) => {
      const areas = MutationReport.tally(list)
      return { areas, total: MutationReport.row('total', '', list) }
    }
    const unit = records.filter((r) => r.commandClass !== 'live')
    const live = records.filter((r) => r.commandClass === 'live')
    const survivors = records
      .filter((r) => r.verdict === 'survived')
      .map((r) => ({
        id: r.id,
        area: r.area,
        severity: MutationReport.severity(r.area),
        operatorClass:
          r.commandClass === 'live' ? 'live' : MutationReport.operatorClass(r.operator),
        file: r.target,
        line: r.line,
        original: r.original,
        mutated: r.mutated,
        witness: r.witness,
        missingAssertion: MutationReport.missingAssertion(r.original, r.mutated),
      }))
      .map((s) => ({
        ...s,
        highRisk:
          s.operatorClass === 'live' ||
          (s.severity === 'P1' && ['removed check', 'boundary'].includes(s.operatorClass)),
      }))
      .sort(
        (a, b) =>
          b.highRisk - a.highRisk ||
          a.severity.localeCompare(b.severity) ||
          CLASS_ORDER.indexOf(a.operatorClass) - CLASS_ORDER.indexOf(b.operatorClass) ||
          a.area.localeCompare(b.area) ||
          a.file.localeCompare(b.file) ||
          (a.line ?? 0) - (b.line ?? 0) ||
          a.id.localeCompare(b.id)
      )
    return {
      provenance: {
        sha: selection.sha,
        runId: selection.runId,
        seed: selection.seed,
        perArea: selection.perArea,
        areas: selection.areas,
        startedAt: selection.startedAt,
        ledgers: {
          found: this.ledgers.filter((l) => /mutants-\d+\.jsonl$/.test(l.file)).length,
          expected: selection.shards,
        },
        foreignRows: foreign,
      },
      unit: section(unit),
      live: {
        ...section(live),
        evidence: refusal || 'valid',
        records: live.map((r) => ({ id: r.id, verdict: r.verdict, reason: r.reason })),
        open: live.filter((r) => r.verdict !== 'killed').map((r) => r.id),
      },
      survivors,
      records,
    }
  }

  /**
   * One record's verdict from its ledger rows.
   * @param {object} record the selection record
   * @param {object[]} rows its ledger rows
   * @param {string|null} refusal why the live guard evidence fails, or null
   * @returns {object} the record with `verdict`, `reason`, `line` and `witness`
   */
  judge(record, rows, refusal) {
    const witness = record.title ? `${record.spec} › ${record.title}` : record.spec
    const out = (verdict, reason, row = {}) => ({
      ...record,
      line: row.line ?? record.line,
      witness: row.witness || witness,
      verdict,
      reason,
    })
    if (record.disposition.startsWith(EXCLUDED))
      return out('excluded', record.disposition.slice(EXCLUDED.length))
    if (record.disposition.startsWith(NOT_RUN))
      return out('not-run', record.disposition.slice(NOT_RUN.length))
    if (rows.length === 0) return out('not-run', NO_ROW)
    if (rows.length > 1) return out('invalid', `${rows.length} ledger rows`)
    const [row] = rows
    if (!VERDICTS.includes(row.verdict))
      return out('invalid', `unknown verdict ${row.verdict}`, row)
    if (record.commandClass === 'live' && ['killed', 'survived'].includes(row.verdict) && refusal)
      return out('invalid', refusal, row)
    return out(row.verdict, row.reason || '', row)
  }

  /**
   * @param {object[]} records judged records
   * @returns {object[]} one row per area, by area name
   */
  static tally(records) {
    const areas = [...new Set(records.map((r) => r.area))].sort()
    return areas.map((area) =>
      MutationReport.row(
        area,
        MutationReport.severity(area),
        records.filter((r) => r.area === area)
      )
    )
  }

  /**
   * @param {string} area
   * @param {string} severity
   * @param {object[]} records the area's judged records
   * @returns {object} the counts and the score (null when nothing was killed or survived)
   */
  static row(area, severity, records) {
    const count = (v) => records.filter((r) => r.verdict === v).length
    const killed = count('killed')
    const survived = count('survived')
    return {
      area,
      severity,
      mutants: records.length,
      killed,
      survived,
      stale: count('stale'),
      invalid: count('invalid'),
      excluded: count('excluded'),
      notRun: count('not-run'),
      score: killed + survived === 0 ? null : killed / (killed + survived),
    }
  }

  /**
   * @param {string} text
   * @returns {string} inline code that survives backticks inside the text
   */
  static code(text) {
    const longest = Math.max(0, ...(String(text).match(/`+/g) || []).map((m) => m.length))
    const fence = '`'.repeat(longest + 1)
    return longest ? `${fence} ${text} ${fence}` : `${fence}${text}${fence}`
  }

  /**
   * @param {{ areas: object[], total: object }} section
   * @returns {string[]} a markdown table of the section
   */
  static table({ areas, total }) {
    const line = (r, name) =>
      `| ${name} | ${r.severity} | ${r.mutants} | ${r.killed} | ${r.survived} | ${r.stale} | ${r.invalid} | ${r.excluded} | ${r.notRun} | ${
        r.score === null ? '—' : `${(r.score * 100).toFixed(1)}%`
      } |`
    return [
      '| Area | Severity | Mutants | Killed | Survived | Stale | Invalid | Excluded | Not run | Score |',
      '|---|---|---|---|---|---|---|---|---|---|',
      ...areas.map((r) => line(r, r.area)),
      line(total, '**total**'),
    ]
  }

  /**
   * @param {object[]} records judged records
   * @param {string} verdict
   * @returns {string[]} markdown lines counting the records of that verdict by reason, or `- none`
   */
  static reasons(records, verdict) {
    const counts = new Map()
    for (const r of records.filter((x) => x.verdict === verdict))
      counts.set(r.reason || '(none)', (counts.get(r.reason || '(none)') || 0) + 1)
    const lines = [...counts.entries()]
      .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
      .map(([reason, n]) => `- ${n} × ${reason}`)
    return lines.length > 0 ? lines : ['- none']
  }

  /**
   * Renders the JSON report as markdown.
   * @param {object} report from {@link MutationReport#build}
   * @returns {string} the markdown
   */
  static markdown(report) {
    const { provenance: p, unit, live, survivors, records } = report
    const code = MutationReport.code
    const shown = survivors.slice(0, MARKDOWN_SURVIVORS)
    const lines = [
      '# Mutation report',
      '',
      '- A mutation run plants one small deliberate bug (a mutant) in the code at a time and runs only its selected witness specs, sometimes one named test in a spec, not the whole suite.',
      '- A killed mutant means a witness noticed the bug and failed through its own test body; a survivor means no witness produced a valid kill and at least one still passed (another may have been invalid): its missing assertion is a candidate for review, since another spec may already guard it.',
      '- Score = killed / (killed + survived); stale, invalid, excluded and not-run mutants are listed but never counted as killed.',
      '',
      `Tree ${code(p.sha)} · run ${p.runId} · seed ${p.seed} · ${p.perArea} generated per area · areas: ${p.areas} · unit ledgers found: ${p.ledgers.found} of ${p.ledgers.expected}${
        p.foreignRows ? ` · ${p.foreignRows} ledger rows for unselected records ignored` : ''
      }${p.unreadableLines ? ` · ${p.unreadableLines} unreadable ledger lines skipped` : ''}`,
      '',
      '## Unit mutants by area',
      '',
      ...MutationReport.table(unit),
      '',
      '## Live mutants (own Compose cluster, guard evidence required)',
      '',
      ...MutationReport.table(live),
      '',
      `Guard evidence: ${live.evidence}.`,
      ...live.records.map((r) => `- ${r.id}: ${r.verdict}${r.reason ? ` (${r.reason})` : ''}`),
      live.open.length > 0
        ? `- OPEN: ${live.open.join(', ')} (a live row closes only on a killed verdict with valid guard evidence)`
        : '- Every live row is killed with valid guard evidence.',
      '',
      `## Survivors, highest risk first (${survivors.length})`,
      '',
      ...shown.flatMap((s, i) => [
        `${i + 1}. **${s.severity} · ${s.operatorClass}${s.highRisk ? ' · high risk' : ''}** ${code(`${s.file}:${s.line}`)} (${s.area}, ${s.id})`,
        `   - original: ${code(s.original ?? '')}`,
        `   - mutated: ${s.mutated ? code(s.mutated) : '(line removed)'}`,
        `   - witness: ${code(s.witness)}`,
        `   - candidate missing assertion: ${s.missingAssertion}`,
      ]),
      ...(survivors.length > shown.length
        ? [`- ${survivors.length - shown.length} more in mutation-report.json`]
        : []),
      '',
      '## What was not run',
      '',
      `Unit ledgers found: ${p.ledgers.found} of ${p.ledgers.expected} shards.`,
      'Not run, by reason:',
      ...MutationReport.reasons(records, 'not-run'),
      'Excluded from the inventory, by reason:',
      ...MutationReport.reasons(records, 'excluded'),
      '',
      '## Invalid, by reason',
      '',
      ...MutationReport.reasons(records, 'invalid'),
      '',
    ]
    return lines.join('\n')
  }

  /**
   * The CLI: reads the selection, the evidence and the ledgers, and writes both reports.
   * @param {string[]} args the command-line arguments
   * @throws {Error} without `--selection` or `--out-dir`, or on an unreadable selection or ledger file;
   *   an unreadable ledger line (a shard cut off mid-write) is skipped and counted
   */
  static main(args) {
    const option = (name) => {
      const at = args.indexOf(name)
      return at === -1 ? undefined : args.splice(at, 2)[1]
    }
    const selectionFile = option('--selection')
    const evidenceFile = option('--evidence')
    const runId = option('--run-id')
    const dir = option('--out-dir')
    if (!selectionFile || !dir)
      throw new Error('usage: report.js --selection <file> --out-dir <dir> <ledger.jsonl>...')
    let unreadable = 0
    const parse = (line) => {
      try {
        return [JSON.parse(line)]
      } catch {
        unreadable++
        return []
      }
    }
    const ledgers = args.map((file) => ({
      file,
      rows: fs.readFileSync(file, 'utf8').split('\n').filter(Boolean).flatMap(parse),
    }))
    const report = new MutationReport({
      selection: JSON.parse(fs.readFileSync(selectionFile, 'utf8')),
      ledgers,
      evidence: evidenceFile ? LiveEvidence.read(evidenceFile) : null,
      runId,
    }).build()
    report.provenance.unreadableLines = unreadable
    fs.mkdirSync(dir, { recursive: true })
    fs.writeFileSync(path.join(dir, 'mutation-report.json'), JSON.stringify(report, null, 1))
    fs.writeFileSync(path.join(dir, 'mutation-report.md'), MutationReport.markdown(report))
  }
}

module.exports = { MutationReport }

if (require.main === module) {
  try {
    MutationReport.main(process.argv.slice(2))
  } catch (error) {
    console.error(`report: ${error.message}`)
    process.exitCode = 1
  }
}
