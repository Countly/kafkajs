/**
 * Report adapter of the `compare-*` rows: the fork and kafkajs 2.2.4 through `clientCompare.js`,
 * five runs per arm, interleaved (fork, 2.2.4, fork, ...) so drift on the runner reaches both arms
 * alike. Each run excludes its own warm-up. An arm with a run whose measured window is shorter
 * than 30 seconds is `inconclusive`: a shorter window does not mean anything in either direction.
 * Beside the rows, each arm shows the median, minimum, maximum and spread of every metric, and,
 * from its first run, the consumer settings in force, the LZ4 codec and the Fetch version, so a
 * consume ratio can be read from the report alone.
 */
const { readRun, rowsOf } = require('../report')

const RUNS = 5
const MIN_WINDOW_MS = 30000

/**
 * Seconds one run spends outside its measured window: process start, topic creation and deletion,
 * connecting, and for a consume run joining the group.
 */
const RUN_OVERHEAD_S = 15
/** Produce warm-up seconds before the measured window (`clientCompare.js` `WARMUP_MS`). */
const WARMUP_S = 5
/**
 * Planning rates for a consume run, in bytes per second, below what the recorded comparison saw on
 * a hosted runner (about 26 MB/s produced and 45 MB/s consumed at 200 bytes a record).
 */
const CORPUS_BYTES_PER_S = 25e6
const CONSUME_BYTES_PER_S = 35e6

/**
 * @param {string[]} command
 * @param {string} flag
 * @returns {string|undefined} the value after `flag`
 */
const valueOf = (command, flag) => {
  const at = command.indexOf(flag)
  return at === -1 ? undefined : command[at + 1]
}

/**
 * @param {{ median: number, min: number, max: number }} row
 * @returns {number|undefined} `(max - min) / median` in percent; undefined for a median of 0
 */
const spreadOf = ({ median, min, max }) => (median ? ((max - min) / median) * 100 : undefined)

/**
 * The wall-clock seconds a compare row is planned to take with both arms: a produce row by its
 * `--duration-ms`, a consume row by its corpus (`--messages` times `--size`) at the planning rates.
 * @param {{ command: string[], arms: string[] }} entry
 * @param {number} [runs]
 * @returns {number}
 */
const plannedSeconds = ({ command, arms }, runs = RUNS) => {
  const duration = Number(valueOf(command, '--duration-ms') ?? 0) / 1000
  const bytes =
    Number(valueOf(command, '--messages') ?? 0) * Number(valueOf(command, '--size') ?? 200)
  const perRun =
    valueOf(command, '--workload') === 'consume-throughput'
      ? RUN_OVERHEAD_S + bytes / CORPUS_BYTES_PER_S + bytes / CONSUME_BYTES_PER_S
      : RUN_OVERHEAD_S + WARMUP_S + duration
  return perRun * runs * arms.length
}

/**
 * @param {object} output one run's JSON line
 * @returns {string[]} what the arm ran with: consumer settings, LZ4 codec and Fetch version
 */
const notesOf = (output) => {
  const pairs = (settings) =>
    Object.entries(settings)
      .map(([key, value]) => `${key}=${value}`)
      .join(', ')
  const notes = []
  if (output.settings?.same) {
    notes.push(
      `consumer settings in force (the same explicit settings on both arms): ${pairs(output.settings.same)}`
    )
  } else if (output.settings) {
    const defaults = output.settings.defaults
    notes.push(
      `consumer settings in force (this package's defaults): ${defaults ? pairs(defaults) : 'unknown package'}`
    )
  }
  if (output.codec) {
    const how =
      output.codec.codec === 'built-in'
        ? "the package's built-in codec"
        : 'a frame codec this benchmark registers through CompressionCodecs (2.2.4 has no built-in LZ4)'
    notes.push(`lz4: ${output.codec.package}@${output.codec.version}, ${how}`)
  }
  if (output.fetchVersion !== undefined) notes.push(`Fetch version: ${output.fetchVersion}`)
  return notes
}

/**
 * @param {import('../report').ManifestEntry} entry
 * @param {string} arm
 * @param {import('../report').Outcome} outcome
 * @param {number} runs
 * @returns {{ rows: import('../report').Row[], detail: import('../report').Detail }} the plain
 *   rows, `inconclusive` when a run's window is under 30 s, and the arm's spread table and notes
 */
const rows = (entry, arm, outcome, runs) => {
  const plain = rowsOf(entry, arm, outcome, runs)
  const short = outcome.outputs
    .map((output, i) => [i + 1, output.elapsedMs])
    .filter(([, ms]) => !(ms >= MIN_WINDOW_MS))
  const window = {
    arm,
    name: 'measured window',
    passed: !outcome.error && short.length === 0,
    detail: short.length
      ? `under ${MIN_WINDOW_MS} ms in run ${short.map(([n, ms]) => `${n} (${ms} ms)`).join(', ')}`
      : `every run measured at least ${MIN_WINDOW_MS} ms after its warm-up`,
  }
  const reason = short.length ? `measured window ${window.detail}` : undefined
  const result = plain.map((row) =>
    row.status === 'valid' && reason ? { ...row, status: 'inconclusive', reason } : row
  )
  const numbered = result.filter((row) => row.median !== undefined)
  const status = outcome.error ? 'error' : reason ? 'inconclusive' : 'valid'
  const fmt = (n) => (n === undefined ? '-' : String(Number(n.toPrecision(4))))
  const detail = {
    status,
    ...(outcome.error && { reason: outcome.error }),
    controls: [window],
    notes: [
      `${arm}: ${outcome.outputs.length} of ${runs} runs, interleaved with the other arm; each run's warm-up is not measured`,
      ...(outcome.outputs.length ? notesOf(outcome.outputs[0]) : []),
    ],
    ...(numbered.length && {
      table: {
        caption: `${arm}: median, minimum, maximum and spread ((max - min) / median) of each metric`,
        header: ['metric', 'median', 'min', 'max', 'spread %', 'unit'],
        rows: numbered.map((row) => [
          row.metric,
          fmt(row.median),
          fmt(row.min),
          fmt(row.max),
          fmt(spreadOf(row)),
          row.unit,
        ]),
      },
    }),
  }
  return { rows: result, detail }
}

module.exports = {
  runs: RUNS,
  interleave: true,
  read: readRun,
  rows,
  spreadOf,
  plannedSeconds,
  MIN_WINDOW_MS,
}
