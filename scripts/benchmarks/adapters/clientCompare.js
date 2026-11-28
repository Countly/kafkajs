/**
 * Report adapter of the `compare-*` rows: the fork and kafkajs 2.2.4 through `clientCompare.js`,
 * five runs per arm, interleaved (fork, 2.2.4, fork, ...) so drift on the runner reaches both arms
 * alike. Each run excludes its own warm-up. An arm with a run whose measured window is shorter
 * than 30 seconds is `inconclusive`: a shorter window does not mean anything in either direction.
 * Beside the rows, each arm shows the median, minimum, maximum and spread of every metric, and,
 * from its first run, the producer or consumer settings in force, the LZ4 or gzip codec, the
 * workload shape, the Fetch version, the consume passes and the consume corpus layout, so a ratio
 * can be read from the report alone. A row with `profile` names each arm's CPU profile, taken in
 * one extra run outside the medians. A row with `diagnostic` reports its `diagnostic.metrics` from
 * one extra run per arm outside the medians, with a window of at least 30 seconds like the
 * others, so what they cost to measure never reaches the rate and CPU of the runs above. A metric
 * in `OPTIONAL_METRICS` is left out of an arm's rows, with a note, only when
 * every run of the arm says its Node cannot measure it; a run that can but did not report it makes
 * the metric, and the arm, an error.
 */
const { readRun, rowsOf } = require('../report')

const RUNS = 5
const MIN_WINDOW_MS = 30000
/**
 * Metrics a run reports only where its Node can measure them: the run's flag that says whether it
 * can, and why the metric is missing when it cannot.
 */
const OPTIONAL_METRICS = {
  mainThreadCpuMicrosPerMessage: {
    supported: 'threadCpuUsageSupported',
    reason: 'the Node running this arm has no process.threadCpuUsage',
  },
}

/**
 * Seconds one run spends outside its warm-up and measured window: process start, topic creation
 * and deletion, connecting, and for a consume run joining the group. Benchmark run 45023473404
 * (hosted runner, Kafka 4.4.0) took 35.6 s per 30-second produce run, and about 2 s besides the
 * corpus and its consumption per consume run.
 */
const RUN_OVERHEAD_S = 2
/** Produce warm-up seconds before the measured window (`clientCompare.js` `WARMUP_MS`). */
const WARMUP_S = 5
/**
 * Microseconds per record and per byte to produce a consume corpus, and to consume it at the slower
 * arm's rate, fitted to run 45023473404: corpora of 200-byte records were produced at about
 * 205,000 records/s and 1 KiB ones at 91,000; the fork consumed about 385,000 and 215,000 records/s.
 */
const CORPUS_US = { record: 3.4, byte: 0.0074 }
const CONSUME_US = { record: 2.1, byte: 0.0025 }

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
 * `--duration-ms`, a consume row by producing its corpus once and reading it `--passes` times at
 * the planning costs. A row with `profile` or `diagnostic` has one more run per arm for each.
 * @param {{ command: string[], arms: string[], profile?: boolean, diagnostic?: object }} entry
 * @param {number} [runs]
 * @returns {number}
 */
const plannedSeconds = ({ command, arms, profile, diagnostic }, runs = RUNS) => {
  const duration = Number(valueOf(command, '--duration-ms') ?? 0) / 1000
  const records = Number(valueOf(command, '--messages') ?? 0)
  const size = Number(valueOf(command, '--size') ?? 200)
  const passes = Number(valueOf(command, '--passes') ?? 1)
  const seconds = ({ record, byte }, n) => (n * (record + size * byte)) / 1e6
  const perRun =
    valueOf(command, '--workload') === 'consume-throughput'
      ? RUN_OVERHEAD_S + seconds(CORPUS_US, records) + seconds(CONSUME_US, records * passes)
      : RUN_OVERHEAD_S + WARMUP_S + duration
  return perRun * (runs + (profile ? 1 : 0) + (diagnostic ? 1 : 0)) * arms.length
}

/**
 * @param {object} output one run's JSON line
 * @param {string} workload the row's `--workload`
 * @returns {string[]} what the arm ran with: producer or consumer settings, LZ4 or gzip codec, the
 *   workload's shape (records per send, unawaited sends, keys, headers, partitioner, value type),
 *   Fetch version, passes and corpus layout
 */
const notesOf = (output, workload) => {
  const pairs = (settings) =>
    Object.entries(settings)
      .map(([key, value]) => `${key}=${JSON.stringify(value)}`)
      .join(', ')
  const notes = []
  const client = workload === 'consume-throughput' ? 'consumer' : 'producer'
  if (output.settings?.same) {
    notes.push(
      `${client} settings in force (the same explicit settings on both arms): ${pairs(output.settings.same)}`
    )
  } else if (output.settings) {
    const defaults = output.settings.defaults
    const custom = output.shape?.partitioner === 'custom'
    notes.push(
      `${client} settings in force (this package's defaults${custom ? ', except the partitioner, which is the custom one' : ''}): ${defaults ? pairs(defaults) : 'unknown package'}`
    )
  }
  if (output.codec) {
    const how =
      output.codec.codec === 'built-in'
        ? "the package's built-in codec"
        : 'a frame codec this benchmark registers through CompressionCodecs (2.2.4 has no built-in LZ4)'
    notes.push(
      `${output.codec.name ?? 'lz4'}: ${output.codec.package}@${output.codec.version}, ${how}`
    )
  }
  if (output.shape) {
    notes.push(`workload shape (the same on both arms): ${pairs(output.shape)}`)
  }
  if (output.fetchVersion !== undefined) notes.push(`Fetch version: ${output.fetchVersion}`)
  if (output.passes > 1) {
    notes.push(
      `the corpus is read ${output.passes} times: each partition is sought back to its first offset after its last record`
    )
  }
  if (output.corpus) {
    notes.push(
      `corpus layout (the same on both arms, each written by the arm's own client): ${pairs(output.corpus)}`,
      "each partition's record count was checked against the layout before measuring; the order of the batches inside a partition was not"
    )
  }
  return notes
}

/**
 * @param {import('../report').ManifestEntry} entry
 * @param {string} arm
 * @param {import('../report').Outcome} outcome
 * @returns {import('../report').Row[]} the rows of `entry.diagnostic.metrics` from the arm's one
 *   diagnostic run (`runs` 1, median, minimum and maximum the one value): `error` rows with the
 *   reason when that run or the arm failed, `inconclusive` when its window is under 30 s; none
 *   for an entry without `diagnostic`
 */
const diagnosticRows = (entry, arm, { error, diagnostic }) => {
  if (!entry.diagnostic) return []
  const output = diagnostic?.output
  const failure = output ? undefined : (diagnostic?.error ?? `no diagnostic run: ${error}`)
  const result = rowsOf(
    { ...entry, metrics: entry.diagnostic.metrics },
    arm,
    { outputs: output ? [output] : [], error: failure },
    1
  )
  if (!output || output.elapsedMs >= MIN_WINDOW_MS) return result
  const reason = `diagnostic window ${output.elapsedMs} ms is under ${MIN_WINDOW_MS} ms`
  return result.map((row) =>
    row.status === 'valid' ? { ...row, status: 'inconclusive', reason } : row
  )
}

/**
 * @param {import('../report').ManifestEntry} entry
 * @param {string} arm
 * @param {import('../report').Outcome} outcome
 * @param {number} runs
 * @returns {{ rows: import('../report').Row[], detail: import('../report').Detail }} the plain
 *   rows, `inconclusive` when a run's window is under 30 s, then the diagnostic rows, and the arm's
 *   spread table (of the plain rows) and notes; an optional metric that every run says it cannot
 *   measure has no row, and an error row, a diagnostic one included, makes the arm an error
 */
const rows = (entry, arm, outcome, runs) => {
  const absent = entry.metrics
    .map(({ key }) => key)
    .filter(
      (key) =>
        Object.hasOwn(OPTIONAL_METRICS, key) &&
        !outcome.error &&
        outcome.outputs.length > 0 &&
        outcome.outputs.every((output) => output[OPTIONAL_METRICS[key].supported] === false)
    )
  const metrics = entry.metrics.filter(({ key }) => !absent.includes(key))
  const plain = rowsOf({ ...entry, metrics }, arm, outcome, runs)
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
  const diagnostic = diagnosticRows(entry, arm, outcome)
  const failed =
    outcome.error ?? [...result, ...diagnostic].find((row) => row.status === 'error')?.reason
  const status = failed ? 'error' : reason ? 'inconclusive' : 'valid'
  const fmt = (n) => (n === undefined ? '-' : String(Number(n.toPrecision(4))))
  const detail = {
    status,
    ...(failed && { reason: failed }),
    controls: [window],
    notes: [
      `${arm}: ${outcome.outputs.length} of ${runs} runs, interleaved with the other arm; each run's warm-up is not measured`,
      ...(outcome.outputs.length
        ? notesOf(outcome.outputs[0], valueOf(entry.command, '--workload'))
        : []),
      ...absent.map((key) => `${key}: not reported, ${OPTIONAL_METRICS[key].reason}`),
      ...(outcome.profile
        ? [`CPU profile (one extra run, not in the medians): ${outcome.profile}`]
        : []),
      ...(entry.diagnostic
        ? [
            `${entry.diagnostic.metrics.map(({ key }) => key).join(', ')}: from one extra run with ${entry.diagnostic.args.join(' ')}, not in the medians; the ${runs} runs above do not measure them`,
          ]
        : []),
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
  return { rows: [...result, ...diagnostic], detail }
}

module.exports = {
  runs: RUNS,
  interleave: true,
  read: readRun,
  rows,
  spreadOf,
  plannedSeconds,
  MIN_WINDOW_MS,
  OPTIONAL_METRICS,
}
