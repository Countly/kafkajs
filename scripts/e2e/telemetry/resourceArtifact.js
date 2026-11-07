'use strict'

/**
 * Judges and validates the machine-readable measurements the quiet resource job records, so the
 * numbers that decide a run live in one place and a recorded artifact can be re-checked after the
 * run. An artifact that is missing, malformed, from another tree or Node, taken on a noisy host or
 * above a limit is a failure or inconclusive, never a pass.
 *
 * Usage: node scripts/e2e/telemetry/resourceArtifact.js <directory> --tree <hash> --node <version>
 */
const fs = require('node:fs')
const path = require('node:path')
const { compareSnapshots } = require('../../../testHelpers/resourceProbe')

const KINDS = Object.freeze(['producer', 'consumer', 'admin', 'shareConsumer'])
const ARMS = Object.freeze(['public', 'collector'])

/** The acceptance values of one single-client memory arm, in bytes and milliseconds. */
const LIMITS = Object.freeze({
  rss: 67108864,
  retained: 8388608,
  closedTransient: 262144,
  closeAfterMarkMs: 10000,
  outputBytes: 65536,
  descriptors: 128,
  rawBytes: 1048576,
})

/** What a host may look like at the start of a run before its numbers stop meaning anything. */
const NOISE = Object.freeze({ loadPerCpu: 0.75, minFreeBytes: 268435456, memoryPressureAvg10: 5 })

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/
const READING_FIELDS = Object.freeze(['heapUsed', 'external', 'arrayBuffers', 'retained', 'rss'])

/**
 * @param {*} value
 * @returns {boolean} whether it is a finite non-negative integer
 */
const count = (value) => Number.isSafeInteger(value) && value >= 0

/**
 * @param {*} reading one settled reading
 * @param {string} name its name in a problem
 * @param {string[]} problems collects every shape or arithmetic problem
 * @returns {void}
 */
const checkReading = (reading, name, problems) => {
  const complete = READING_FIELDS.every((field) => count(reading?.[field]))
  if (!complete) {
    problems.push(`${name} reading is incomplete`)
    return
  }
  if (reading.retained !== reading.heapUsed + reading.external)
    problems.push(`${name} retained bytes are not heap plus external`)
  if (reading.arrayBuffers > reading.external)
    problems.push(`${name} array buffers exceed external bytes`)
}

/**
 * @param {object} context one host context reading
 * @returns {string[]} what makes the context unusable, empty when it is complete
 */
const contextProblems = (context) => {
  const complete =
    count(context?.cpus) &&
    context.cpus > 0 &&
    Array.isArray(context.load) &&
    context.load.length === 3 &&
    context.load.every(Number.isFinite) &&
    count(context.freeBytes) &&
    count(context.totalBytes)
  return complete ? [] : ['host context is incomplete']
}

/**
 * @param {object} context the host context before the run
 * @returns {string[]} the ways the host was too busy to trust the run
 */
const noiseOf = (context) => {
  const noisy = []
  if (context.load[0] / context.cpus > NOISE.loadPerCpu) noisy.push('host load was high')
  if (context.freeBytes < NOISE.minFreeBytes) noisy.push('free memory was low')
  if (context.psi && context.psi.memorySomeAvg10 > NOISE.memoryPressureAvg10)
    noisy.push('memory pressure was high')
  return noisy
}

/**
 * Judges one recorded memory arm.
 * @param {object} record `{ schema, arm, kind, exit, context, readings }` as the resource spec
 *   writes it
 * @returns {{ verdict: 'pass'|'fail'|'inconclusive', problems: string[] }} `fail` for any broken
 *   fact or gate, `inconclusive` when only the setup or the host stops the numbers deciding
 */
const judgeMemoryArm = (record) => {
  const failures = []
  const inconclusive = []
  const { exit, readings, context, arm } = record ?? {}
  if (record?.schema !== 1) failures.push('unknown schema')
  if (!ARMS.includes(arm)) failures.push('unknown arm')
  if (!KINDS.includes(record?.kind)) failures.push('unknown kind')

  if (exit?.timedOut !== false) failures.push('child exceeded its bound')
  if (exit?.code !== 0 || exit?.signal !== null)
    failures.push('child did not exit with code 0 and no signal')
  if (exit?.stderrBytes !== 0) failures.push('child wrote to stderr')
  if (!count(exit?.stdoutBytes) || exit.stdoutBytes > LIMITS.outputBytes)
    failures.push('child output exceeded its bound')
  if (exit?.receipts !== (arm === 'public' ? 1 : 0))
    failures.push('child sent the wrong number of disconnect receipts')
  if (!(exit?.closedAfterMarkMs <= LIMITS.closeAfterMarkMs))
    failures.push('child did not close within 10 s of its receipt')

  const problems = contextProblems(context?.before).concat(contextProblems(context?.after))
  failures.push(...problems)
  if (problems.length === 0) inconclusive.push(...noiseOf(context.before))

  if (!readings) {
    failures.push('readings are missing')
    return { verdict: 'fail', problems: failures }
  }
  for (const name of ['baseline', 'matched', 'held', 'closed'])
    checkReading(readings[name], name, failures)
  if (![readings.baselinePeakRss, readings.peakRss, readings.rssPeakSample].every(count))
    failures.push('peak RSS readings are incomplete')
  else {
    if (readings.peakRss < readings.baselinePeakRss)
      failures.push('lifetime peak is below baseline')
    if (readings.rssPeakSample > readings.peakRss)
      failures.push('sampled RSS exceeds lifetime peak')
    if (readings.baselinePeakRss > LIMITS.rss)
      inconclusive.push('setup exceeds the 64 MiB RSS limit')
    else if (readings.peakRss > LIMITS.rss) failures.push('peak RSS exceeds 64 MiB')
  }
  if (failures.length === 0) {
    if (readings.held.retained - readings.matched.retained > LIMITS.retained)
      failures.push('held retained bytes exceed the matched baseline by more than 8 MiB')
    if (readings.closed.retained - readings.matched.retained > LIMITS.closedTransient)
      failures.push('closed transient bytes exceed 256 KiB')
  }

  const codecs = ['gzip', ...(readings.zstdSupported ? ['zstd'] : [])]
  if (JSON.stringify(readings.builds?.map((build) => build.codec)) !== JSON.stringify(codecs))
    failures.push('native builds are not the expected codecs')
  for (const build of readings.builds ?? []) {
    const type = build.codec === 'zstd' ? 4 : 1
    if (build.compressionType !== type) failures.push(`${build.codec} build used another codec`)
    if (!(build.rawBytes > 1000 && build.rawBytes <= LIMITS.rawBytes))
      failures.push(`${build.codec} raw bytes are out of range`)
    if (!(build.transmittedBytes > 0 && build.transmittedBytes < build.rawBytes))
      failures.push(`${build.codec} payload was not compressed`)
    if (arm === 'collector' && (build.points !== 257 || build.retiredCompletion !== true))
      failures.push(`${build.codec} collector build is incomplete`)
  }
  if (!(readings.rssSamples >= 2 * codecs.length)) failures.push('too few current RSS samples')
  if (arm === 'public') {
    if (!UUID.test(readings.uuid) || readings.uuidHeld !== readings.uuid)
      failures.push('client instance id is missing or changed')
    if (readings.descriptors !== LIMITS.descriptors) failures.push('descriptor count is not 128')
    const {
      baselineResources: baseline,
      matchedResources: matched,
      closedResources: closed,
    } = readings
    if (!baseline || !matched || !closed) failures.push('resource roles are missing')
    else {
      failures.push(
        ...compareSnapshots(
          { resources: matched },
          { resources: closed },
          { resources: ['target', 'fixture'] }
        ).map((problem) => `resource roles: ${problem}`)
      )
      if (Object.values(closed.target ?? {}).some((ids) => ids.length))
        failures.push('target handles survive the public close')
    }
  }

  const verdict = failures.length ? 'fail' : inconclusive.length ? 'inconclusive' : 'pass'
  return { verdict, problems: [...failures, ...inconclusive] }
}

/**
 * Validates every arm of a run's artifact directory.
 * @param {string} directory holds `resources-<arm>-<kind>.json`, one file per arm and kind
 * @param {{ tree: string, node: string }} expected the tree and Node this run belongs to
 * @returns {{ ok: boolean, results: Array<{ arm: string, kind: string, verdict: string,
 *   problems: string[] }> }} one result per expected arm and kind; `ok` only when all pass
 */
const validateResourceArtifacts = (directory, { tree, node }) => {
  const results = []
  for (const arm of ARMS)
    for (const kind of KINDS) {
      const file = path.join(directory, `resources-${arm}-${kind}.json`)
      let verdict = { verdict: 'fail', problems: ['artifact is missing'] }
      if (fs.existsSync(file)) {
        try {
          const record = JSON.parse(fs.readFileSync(file, 'utf8'))
          const stamped = record.stamp?.tree === tree && record.stamp?.node === node
          verdict = judgeMemoryArm(record)
          if (!stamped || record.arm !== arm || record.kind !== kind)
            verdict = {
              verdict: 'fail',
              problems: [
                'artifact is not stamped for this tree, Node, arm and kind',
                ...verdict.problems,
              ],
            }
        } catch (error) {
          verdict = { verdict: 'fail', problems: [`artifact is not valid JSON: ${error.message}`] }
        }
      }
      results.push({ arm, kind, ...verdict })
    }
  return { ok: results.every((result) => result.verdict === 'pass'), results }
}

/**
 * @param {string[]} argv `<directory> --tree <hash> --node <version>`
 * @returns {number} the exit code: 0 only when every arm passed
 */
const main = (argv) => {
  const [directory, ...rest] = argv
  const option = (name) => rest[rest.indexOf(name) + 1]
  if (!directory || !option('--tree') || !option('--node')) {
    process.stderr.write('Usage: resourceArtifact.js <directory> --tree <hash> --node <version>\n')
    return 2
  }
  const summary = validateResourceArtifacts(directory, {
    tree: option('--tree'),
    node: option('--node'),
  })
  fs.writeFileSync(path.join(directory, 'summary.json'), `${JSON.stringify(summary, null, 2)}\n`)
  process.stdout.write(`${JSON.stringify(summary)}\n`)
  return summary.ok ? 0 : 1
}

module.exports = { ARMS, KINDS, LIMITS, NOISE, judgeMemoryArm, validateResourceArtifacts }

if (require.main === module) process.exitCode = main(process.argv.slice(2))
