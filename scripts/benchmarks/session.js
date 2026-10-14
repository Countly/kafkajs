/**
 * Interleaved benchmark sessions. Every arm runs in every position of a round, a duplicate of the
 * reference arm (the A/A control) runs beside the others, and a session whose control differs from
 * its reference by more than `--void-over` percent is void. `--runs n` is `n` measured runs per
 * arm after one warm-up run that is discarded; every observation is kept, the warm-up first.
 *
 * Usage:
 *   node session.js run --command "<command>" --runs <n> --metrics <key,...> --raw <file>
 *     --reference <label> [--pair <label>:<against> [--pair-over <percent>]] ...
 *     [--control-on <key,...>] [--void-over 3] [--cpu] [--timeout <ms>]
 *     <label>=<directory>[ <extra arguments>] ...
 *   node session.js summarize <raw file> --metrics <key,...> --reference <label> [--pair ...]
 * Each arm runs `<command> <extra arguments>` in its directory and must print one JSON line of
 * numbers; arguments are split on spaces, with no quoting. `--cpu` adds `cpuMs`, the CPU time the
 * run's processes used; it needs bash and GNU coreutils' `timeout`, so Linux. `--runs` is a
 * multiple of the number of arms, the control included, so that every arm takes every position
 * equally often; `run` refuses any other number, and `summarize` refuses observations in which
 * the positions are unequal. A percent of a median of 0 is undefined, so `summarize` refuses a
 * reference arm whose median of a compared metric is 0.
 * A `--pair-over <percent>` gates the `--pair` before it: the session fails when the pair's median
 * is further from `<against>`'s than that percent of it, on the `--control-on` keys (every
 * `--metrics` key without them). `+<percent>` fails only a median above `<against>`'s, for
 * metrics where more is worse. The rule is the void rule's, on unrounded figures; the printed
 * percentages carry two decimals.
 * Exit codes: 0 a valid session within its limits, 1 a run failed or a limited pair is outside
 * its limit, 2 a bad argument, 3 the session is void (that wins over 1: a void session says
 * nothing about its pairs).
 */
const fs = require('fs')
const { spawnSync } = require('child_process')

const CONTROL_SUFFIX = '~AA'
const DEFAULT_VOID_OVER = 3
const DEFAULT_TIMEOUT = 900000
const KILL_AFTER_MS = 5000
const VOID_TOLERANCE = 1e-9

/** A command line the session cannot act on; the exit code is 2. */
class UsageError extends Error {}

/**
 * @typedef {{ label: string, run: (reportAs?: string) => Record<string, number> }} Arm `run`
 *   names `reportAs` in its error: the control shares its reference's `run`
 * @typedef {{ round: number, position: number, label: string, warmup: boolean,
 *   result: Record<string, number> }} Observation
 * @typedef {{ median: number, min: number, max: number, n: number }} Stat
 * @typedef {{ label: string, against: string, percent: Record<string, number>,
 *   over?: boolean|null }} Comparison `over` is set on a `--pair`: null without a limit
 * @typedef {{ arms: Record<string, Record<string, Stat>>, comparisons: Comparison[],
 *   control: Comparison|null, void: boolean, over: boolean }} Summary
 */

/**
 * @param {Arm[]} arms
 * @param {number} round
 * @returns {Arm[]} the arms in the order of that round: each round moves every arm one place
 */
const rotate = (arms, round) => arms.map((_, i) => arms[(i + round) % arms.length])

/**
 * @param {number[]} values
 * @returns {number} the middle value; the upper middle of an even count
 */
const median = (values) => [...values].sort((a, b) => a - b)[Math.floor(values.length / 2)]

/**
 * Decides on the absolute difference, not on a percent: `(103 / 100 - 1) * 100` is
 * 3.0000000000000027, and a session whose control sits exactly on the limit is not void. The
 * tolerance, `VOID_TOLERANCE` times the reference (1e-9, far below any measurement's noise), absorbs
 * the rounding of the limit itself.
 * @param {number} value the control's median
 * @param {number} reference the reference's median
 * @param {number} limit percent of `reference`
 * @returns {boolean} whether `value` is more than `limit` percent from `reference`
 */
const beyond = (value, reference, limit) =>
  Math.abs(value - reference) >
  (Math.abs(reference) * limit) / 100 + Math.abs(reference) * VOID_TOLERANCE

/**
 * @param {number} value a pair's median
 * @param {number} reference the median it is held against
 * @param {string} limit `<percent>` (either way) or `+<percent>` (only above `reference`)
 * @returns {boolean} whether `value` is outside `limit`, by the rule of `beyond`
 */
const outside = (value, reference, limit) =>
  beyond(value, reference, Number(limit)) && (!limit.startsWith('+') || value > reference)

/**
 * Runs a session: one warm-up round and `runs` measured rounds, each with the arms rotated by one
 * place, the control (a second copy of the reference arm) among them.
 * @param {object} options
 * @param {Arm[]} options.arms
 * @param {number} options.runs measured runs per arm
 * @param {string} [options.reference] label of the arm that gets an A/A control
 * @param {(observation: Observation) => void} [options.onObservation] called after every run
 * @returns {Observation[]} every observation in run order, the warm-up round first
 * @throws {Error} for an unknown reference
 * @throws {UsageError} before any run, unless `runs` is a multiple of the number of arms, the
 *   control included: each arm must take every position equally often
 */
const runSession = ({ arms, runs, reference, onObservation = () => {} }) => {
  const base = arms.find(({ label }) => label === reference)
  if (reference !== undefined && !base) {
    throw new Error(`no arm is named ${reference}`)
  }
  const all = base ? [...arms, { ...base, label: reference + CONTROL_SUFFIX }] : arms
  if (runs < all.length || runs % all.length !== 0) {
    throw new UsageError(
      `${runs} runs cannot put each of ${all.length} arms in every position equally often: ` +
        `give a multiple of ${all.length}`
    )
  }
  const observations = []
  for (let round = 0; round <= runs; round += 1) {
    rotate(all, round).forEach(({ label, run }, position) => {
      const observation = { round, position, label, warmup: round === 0, result: run(label) }
      observations.push(observation)
      onObservation(observation)
    })
  }
  return observations
}

/**
 * @param {Observation[]} measured the measured observations of a session
 * @param {string[]} labels every arm, the control included
 * @throws {Error} unless every arm took each of `labels.length` positions equally often
 */
const assertBalanced = (measured, labels) => {
  const positions = new Map(labels.map((label) => [label, new Map()]))
  for (const { label, position } of measured) {
    const counts = positions.get(label)
    counts.set(position, (counts.get(position) ?? 0) + 1)
  }
  for (const [label, counts] of positions) {
    if (counts.size !== labels.length || new Set(counts.values()).size !== 1) {
      throw new Error(
        `${label} did not take every position equally often: ` +
          `${JSON.stringify(Object.fromEntries(counts))}`
      )
    }
  }
}

/**
 * Reduces the measured runs of a session to a median, minimum and maximum per arm and metric, the
 * percent difference of each requested pair of arms, and the A/A verdict.
 * @param {Observation[]} observations
 * @param {object} options
 * @param {string[]} options.metrics the keys to report
 * @param {string} [options.reference] label of the arm whose control is `<label>~AA`
 * @param {Array<[string, string, string?]>} [options.pairs] `[label, against, limit]`: label's
 *   median against that arm's; a `limit` (`--pair-over`'s text) makes it a gate
 * @param {string[]} [options.controlOn] the keys the A/A rule and the pair limits judge; defaults
 *   to `metrics`
 * @param {number} [options.voidOver] percent of the reference's median; the session is void when
 *   the control's median is further than that from it (percentages stay unrounded in the report)
 * @returns {Summary}
 * @throws {Error} when a run lacks a metric, the arms have different numbers of measured runs or
 *   did not take every position equally often (a raw file is checked as a live session is), or the
 *   median of a reference arm is 0 for a metric that is compared as a percent
 */
const summarize = (
  observations,
  { metrics, reference, pairs = [], controlOn = metrics, voidOver = DEFAULT_VOID_OVER }
) => {
  const labels = [...new Set(observations.map(({ label }) => label))]
  const samples = Object.fromEntries(
    labels.map((label) => [label, Object.fromEntries(metrics.map((metric) => [metric, []]))])
  )
  for (const { label, result, warmup } of observations) {
    for (const metric of warmup ? [] : metrics) {
      if (!Number.isFinite(result[metric])) {
        throw new Error(`${label} reported no number for ${metric}`)
      }
      samples[label][metric].push(result[metric])
    }
  }
  if (new Set(labels.map((label) => samples[label][metrics[0]].length)).size !== 1) {
    throw new Error('the arms have different numbers of measured runs')
  }
  assertBalanced(
    observations.filter(({ warmup }) => !warmup),
    labels
  )
  const arms = Object.fromEntries(
    labels.map((label) => [
      label,
      Object.fromEntries(
        metrics.map((metric) => {
          const values = samples[label][metric]
          return [
            metric,
            {
              median: median(values),
              min: Math.min(...values),
              max: Math.max(...values),
              n: values.length,
            },
          ]
        })
      ),
    ])
  )
  const compare = (label, against, keys = metrics) => {
    if (!arms[label] || !arms[against]) {
      throw new Error(`cannot compare ${label} with ${against}: no such arm`)
    }
    const zero = keys.find((key) => arms[against][key].median === 0)
    if (zero !== undefined) {
      throw new Error(`${against} has a median of 0 for ${zero}: a percent of it is undefined`)
    }
    return {
      label,
      against,
      percent: Object.fromEntries(
        keys.map((key) => [key, (arms[label][key].median / arms[against][key].median - 1) * 100])
      ),
    }
  }
  const control =
    reference === undefined ? null : compare(reference + CONTROL_SUFFIX, reference, controlOn)
  const comparisons = pairs.map(([label, against, limit]) => ({
    ...compare(label, against),
    over:
      limit === undefined
        ? null
        : controlOn.some((key) =>
            outside(arms[label][key].median, arms[against][key].median, limit)
          ),
  }))
  return {
    arms,
    comparisons,
    control,
    void:
      control !== null &&
      Object.keys(control.percent).some((key) =>
        beyond(arms[control.label][key].median, arms[reference][key].median, voidOver)
      ),
    over: comparisons.some(({ over }) => over === true),
  }
}

/**
 * @param {Summary} summary
 * @returns {number} the exit code of a finished session: 3 when it is void, else 1 when a limited
 *   pair is outside its limit, else 0
 */
const exitCode = ({ void: isVoid, over }) => (isVoid ? 3 : over ? 1 : 0)

/**
 * @param {Summary} summary
 * @param {string[]} metrics
 * @param {number} [voidOver]
 * @returns {string[]} one line per arm, per comparison, and the verdict
 */
const format = (summary, metrics, voidOver = DEFAULT_VOID_OVER) => {
  const percent = (value) => `${value >= 0 ? '+' : ''}${value.toFixed(2)} %`
  const lines = Object.entries(summary.arms).map(([label, byMetric]) => {
    const cells = metrics.map((metric) => {
      const { median: m, min, max } = byMetric[metric]
      return `${metric} ${+m.toFixed(3)} [${+min.toFixed(3)}-${+max.toFixed(3)}]`
    })
    return `${label.padEnd(12)} n ${byMetric[metrics[0]].n}  ${cells.join('  ')}`
  })
  for (const { label, against, percent: byMetric } of [
    ...summary.comparisons,
    ...(summary.control ? [summary.control] : []),
  ]) {
    const cells = Object.entries(byMetric).map(([key, value]) => `${key} ${percent(value)}`)
    lines.push(`${label} vs ${against}: ${cells.join('  ')}`)
  }
  if (summary.control) {
    lines.push(
      summary.void
        ? `SESSION VOID: the A/A control differs from its reference by more than ${voidOver} %`
        : `A/A within ${voidOver} %`
    )
  }
  return lines
}

/**
 * Builds an arm that runs a command in a directory, once per call. A run that outlasts `timeout`
 * fails. With `cpu` the command runs under GNU coreutils' `timeout`, which stops its whole process
 * group (SIGTERM, then SIGKILL after 5 s), so no process of a timed-out run outlives it, and bash
 * forwards an INT, TERM or HUP it receives to `timeout`, which sits in a group of its own, so
 * interrupting the runner stops the command too. That needs bash and coreutils, so Linux.
 * `spawnSync`'s own limit is a backstop beyond it.
 * @param {object} options
 * @param {string} options.label
 * @param {string} options.cwd
 * @param {string[]} options.argv the executable and its arguments
 * @param {boolean} options.cpu add `cpuMs` from bash's `time`, which covers the run's processes
 * @param {number} options.timeout milliseconds
 * @returns {Arm}
 * @throws {Error} from `run()` when the command exits non-zero or times out, or prints no JSON
 *   line of numbers; the message names `run`'s argument, else `label`
 */
const commandArm = ({ label, cwd, argv, cpu, timeout }) => ({
  label,
  run: (reportAs = label) => {
    const [file, ...args] = cpu
      ? [
          'bash',
          '-c',
          'TIMEFORMAT="%U %S"; limit=$1; shift; time { timeout -k ' +
            `${KILL_AFTER_MS / 1000} "$limit" "$@" & ` +
            "trap 'kill $!' INT TERM HUP; wait $!; }",
          'bash',
          `${timeout / 1000}`,
          ...argv,
        ]
      : argv
    const child = spawnSync(file, args, {
      cwd,
      encoding: 'utf8',
      timeout: cpu ? timeout + 2 * KILL_AFTER_MS : timeout,
    })
    if (child.status !== 0) {
      throw new Error(`${reportAs} failed (${child.status ?? child.signal}): ${child.stderr}`)
    }
    const result = JSON.parse(child.stdout.trim().split('\n').pop())
    if (cpu) {
      const [user, system] = child.stderr.trim().split('\n').pop().split(' ').map(Number)
      result.cpuMs = (user + system) * 1000
    }
    return result
  },
})

/**
 * Reads the flags of one command line. `--pair` and arms may repeat; a `--pair-over` joins the
 * `--pair` before it.
 * @param {string[]} argv the arguments after the subcommand
 * @returns {{ flags: Record<string, string>, pairs: string[][], positional: string[], cpu: boolean }}
 * @throws {Error} for a flag without a value, or a `--pair-over` that has no `--pair` before it or
 *   no percent
 */
const parseFlags = (argv) => {
  const flags = {}
  const pairs = []
  const positional = []
  let cpu = false
  for (let i = 0; i < argv.length; i += 1) {
    if (argv[i] === '--cpu') {
      cpu = true
    } else if (argv[i].startsWith('--')) {
      if (argv[i + 1] === undefined) {
        throw new UsageError(`${argv[i]} needs a value`)
      }
      if (argv[i] === '--pair') {
        pairs.push(argv[i + 1].split(':'))
      } else if (argv[i] === '--pair-over') {
        if (pairs.length === 0 || !/^\+?\d+(\.\d+)?$/.test(argv[i + 1])) {
          throw new UsageError('--pair-over <percent> or +<percent> follows the --pair it limits')
        }
        pairs[pairs.length - 1][2] = argv[i + 1]
      } else {
        flags[argv[i].slice(2)] = argv[i + 1]
      }
      i += 1
    } else {
      positional.push(argv[i])
    }
  }
  return { flags, pairs, positional, cpu }
}

/**
 * The command line. Prints the summary after a valid or void session; a run that fails ends it
 * with no summary and the raw file as far as it got.
 * @param {string[]} argv `process.argv.slice(2)`
 * @param {{ log: (line: string) => void, error: (line: string) => void }} [io]
 * @returns {number} the exit code
 */
const main = (argv, { log, error } = console) => {
  const [subcommand, ...rest] = argv
  try {
    const { flags, pairs, positional, cpu } = parseFlags(rest)
    const metrics = (flags.metrics ?? '').split(',').filter(Boolean)
    const voidOver =
      flags['void-over'] === undefined ? DEFAULT_VOID_OVER : Number(flags['void-over'])
    if (
      !['run', 'summarize'].includes(subcommand) ||
      metrics.length === 0 ||
      !Number.isFinite(voidOver)
    ) {
      throw new UsageError('usage: session.js run|summarize --metrics <key,...> ...')
    }
    const options = {
      metrics,
      reference: flags.reference,
      pairs,
      voidOver,
      ...(flags['control-on'] ? { controlOn: flags['control-on'].split(',') } : {}),
    }
    let observations
    if (subcommand === 'summarize') {
      observations = fs
        .readFileSync(positional[0], 'utf8')
        .trim()
        .split('\n')
        .map((line) => JSON.parse(line))
    } else {
      const runs = Number(flags.runs)
      if (
        !Number.isInteger(runs) ||
        runs < 1 ||
        !flags.command ||
        !flags.raw ||
        positional.length === 0
      ) {
        throw new UsageError('run needs --runs, --command, --raw and at least one arm')
      }
      const arms = positional.map((arm) => {
        const [, label, spec] = /^([^=]+)=(.*)$/.exec(arm) ?? []
        if (!label) {
          throw new UsageError(`an arm is <label>=<directory>[ <arguments>], got ${arm}`)
        }
        const [cwd, ...extra] = spec.split(' ')
        return commandArm({
          label,
          cwd,
          argv: [...flags.command.split(' '), ...extra],
          cpu,
          timeout: Number(flags.timeout ?? DEFAULT_TIMEOUT),
        })
      })
      fs.writeFileSync(flags.raw, '')
      observations = runSession({
        arms,
        runs,
        reference: flags.reference,
        onObservation: (observation) =>
          fs.appendFileSync(flags.raw, `${JSON.stringify(observation)}\n`),
      })
    }
    const summary = summarize(observations, options)
    format(summary, metrics, voidOver).forEach((line) => log(line))
    return exitCode(summary)
  } catch (e) {
    error(e.message)
    return e instanceof UsageError ? 2 : 1
  }
}

module.exports = { rotate, median, runSession, summarize, format, exitCode, commandArm, main }

if (require.main === module) {
  process.exitCode = main(process.argv.slice(2))
}
