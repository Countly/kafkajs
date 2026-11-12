#!/usr/bin/env node
'use strict'

/** Offline S1/S2/S3 report and acceptance verdict for the frozen long-window method. */
const fs = require('node:fs')
const path = require('node:path')
const { isDeepStrictEqual } = require('node:util')
const { LONG_WINDOW, identityOf } = require('./longWindow')

const KIB = 1024
const FILE = 'long-window.json'
const NODES = ['22', '24', '26']
const KAFKAS = ['4.2.2', '4.3.1']
const KINDS = ['producer', 'consumer', 'admin', 'shareConsumer']
const REPLICATES = [1, 2, 3]

/** @param {Array<string|number>} parts identity fields @returns {string} an unambiguous map key */
const keyOf = (parts) => JSON.stringify(parts)

/**
 * Indexes one frozen candidate without combining versions, commits, runtimes or duplicate downloads.
 * Unversioned records belong to candidate v1.
 * @param {object[]} records the long-window job records
 * @returns {Map<string, object>} workers by Node major, Kafka, arm, replicate and kind
 * @throws {Error} for duplicate identities, mixed candidates, commits or runtimes, or an unexpected tuple
 */
const observationsOf = (records) => {
  const observations = new Map()
  const identities = new Set()
  const runtimes = new Map()
  const versions = new Set()
  let commit = null
  for (const record of records) {
    const { node, kafka, arm, replicate, workers } = record
    versions.add(record.candidate?.version ?? 1)
    if (versions.size > 1) throw new Error('mixed candidate versions in long-window collection')
    const sha = record.sha ?? record.tree
    const major = /^v(\d+)\./.exec(node)?.[1]
    if (typeof sha !== 'string' || !sha.length) throw new Error('missing collection commit')
    commit ??= sha
    if (sha !== commit) throw new Error('mixed commits in long-window collection')
    if (runtimes.has(major) && runtimes.get(major) !== node)
      throw new Error('mixed Node runtimes for Node ' + major)
    runtimes.set(major, node)
    if (
      !NODES.includes(major) ||
      !KAFKAS.includes(kafka) ||
      !LONG_WINDOW.arms.includes(arm) ||
      !REPLICATES.includes(replicate) ||
      (arm === 'warmup-seed' && (kafka !== '4.3.1' || replicate !== 1)) ||
      !Array.isArray(workers)
    )
      throw new Error('unexpected long-window matrix tuple')
    for (const worker of workers) {
      if (!KINDS.includes(worker.kind) || worker.arm !== arm)
        throw new Error('unexpected long-window worker tuple')
      const identity = keyOf([sha, node, kafka, arm, replicate, worker.kind])
      if (identities.has(identity)) throw new Error('duplicate long-window identity ' + identity)
      identities.add(identity)
      observations.set(keyOf([major, kafka, arm, replicate, worker.kind]), worker)
    }
  }
  return observations
}

/**
 * @param {string} directory the downloaded artifacts root
 * @returns {object[]} every `long-window.json` beneath it, parsed
 * @throws {Error} for an unreadable directory or invalid JSON
 */
const readRecords = (directory) => {
  const records = []
  for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
    const file = path.join(directory, entry.name)
    if (entry.isDirectory()) records.push(...readRecords(file))
    else if (entry.name === FILE) {
      const record = JSON.parse(fs.readFileSync(file, 'utf8'))
      records.push(record)
    }
  }
  return records
}

/**
 * @param {number[][]} groups the values of each tuple, one per replicate
 * @returns {number|null} the pooled within-tuple standard deviation, the square root of the summed
 *   squared deviations over the summed degrees of freedom; null when no tuple has a second value
 */
const pooledSd = (groups) => {
  let squares = 0
  let degrees = 0
  for (const group of groups) {
    if (group.length < 2) continue
    const mean = group.reduce((sum, value) => sum + value, 0) / group.length
    squares += group.reduce((sum, value) => sum + (value - mean) ** 2, 0)
    degrees += group.length - 1
  }
  return degrees === 0 ? null : Math.sqrt(squares / degrees)
}

const mean = (values) => values.reduce((sum, value) => sum + value, 0) / values.length

/**
 * S3 of one Node from its seeded slopes and its unseeded slopes grouped by tuple (Kafka and kind).
 * @param {number[]} seeded the seeded data slopes, KiB/min
 * @param {number[][]} tuples the unseeded data slopes of each tuple, KiB/min
 * @param {boolean} complete whether every expected observation is complete
 * @returns {object} `status` `pass`, `fail` or `insufficient`, the two means, the pooled SD and the
 *   two bounds the thresholds are compared with
 */
const s3Of = (seeded, tuples, complete) => {
  const unseeded = tuples.flat()
  const sd = pooledSd(tuples)
  const seededMean = seeded.length ? mean(seeded) : null
  const unseededMean = unseeded.length ? mean(unseeded) : null
  if (!complete || sd === null || seededMean === null)
    return {
      status: 'insufficient',
      seededMean,
      unseededMean,
      sd,
      seededBound: null,
      unseededBound: null,
    }
  const { thresholds } = LONG_WINDOW
  const seededBound = seededMean - thresholds.sigmas * sd
  const unseededBound = unseededMean + thresholds.sigmas * sd
  const pass =
    seededBound >= thresholds.seededSlopeBytesPerMinute / KIB &&
    unseededBound <= thresholds.unseededSlopeBytesPerMinute / KIB
  return {
    status: pass ? 'pass' : 'fail',
    seededMean,
    unseededMean,
    sd,
    seededBound,
    unseededBound,
  }
}

/**
 * @param {object[]} records the long-window records of every job
 * @returns {{nodes: Object<string, object>}} the frozen three-replicate table for Node 22/24/26.
 *   Missing and incomplete observations stay in the denominators and make their lane insufficient;
 *   S3 requires the complete collection and uses only valid slopes for its descriptive statistics.
 * @throws {Error} for duplicate identities or records outside one frozen collection
 */
const summarise = (records) => {
  const observations = observationsOf(records)
  const nodes = {}
  for (const major of NODES) {
    const lane = () => ({ pass: 0, total: 0, missing: 0, incomplete: 0 })
    const node = {
      s1: lane(),
      s2: lane(),
      masked: 0,
      firstUse: { tripped: 0, total: 0, missing: 0, incomplete: 0 },
      missing: 0,
      incomplete: 0,
    }
    const seeded = []
    const tuples = []
    const observe = (kafka, arm, replicate, kind, target) => {
      target.total += 1
      const worker = observations.get(keyOf([major, kafka, arm, replicate, kind]))
      if (!worker) {
        node.missing += 1
        target.missing += 1
        return null
      }
      const verdict = arm === 'seeded' ? worker.wouldBe?.s1 : worker.wouldBe?.s2
      const valid =
        worker.complete === true &&
        Number.isFinite(worker.mid?.data?.slopeBytesPerMinute) &&
        (arm === 'warmup-seed'
          ? typeof worker.firstUse?.tripped === 'boolean'
          : typeof verdict?.pass === 'boolean')
      if (!valid) {
        node.incomplete += 1
        target.incomplete += 1
        return null
      }
      return worker
    }
    for (const kafka of KAFKAS)
      for (const kind of KINDS) {
        const unseeded = []
        for (const replicate of REPLICATES) {
          const positive = observe(kafka, 'seeded', replicate, kind, node.s1)
          if (positive) {
            if (
              positive.wouldBe.s1.pass &&
              positive.mid.data.slopeBytesPerMinute >=
                LONG_WINDOW.thresholds.seededSlopeBytesPerMinute
            )
              node.s1.pass += 1
            seeded.push(positive.mid.data.slopeBytesPerMinute / KIB)
          }
          const negative = observe(kafka, 'unseeded', replicate, kind, node.s2)
          if (negative) {
            const masked =
              negative.masked ||
              negative.mid.data.slopeBytesPerMinute <
                LONG_WINDOW.thresholds.maskingSlopeBytesPerMinute
            if (
              negative.wouldBe.s2.pass &&
              !masked &&
              negative.mid.data.slopeBytesPerMinute <=
                LONG_WINDOW.thresholds.unseededSlopeBytesPerMinute &&
              Number.isFinite(negative.mid.data.endpointBytes) &&
              negative.mid.data.endpointBytes <= LONG_WINDOW.thresholds.endpointBytes
            )
              node.s2.pass += 1
            if (masked) node.masked += 1
            unseeded.push(negative.mid.data.slopeBytesPerMinute / KIB)
          }
        }
        tuples.push(unseeded)
      }
    for (const kind of KINDS) {
      const worker = observe('4.3.1', 'warmup-seed', 1, kind, node.firstUse)
      if (worker?.firstUse.tripped) node.firstUse.tripped += 1
    }
    for (const target of [node.s1, node.s2, node.firstUse])
      target.status =
        target.missing || target.incomplete
          ? 'insufficient'
          : (target.pass ?? target.tripped) === target.total
            ? 'pass'
            : 'fail'
    if (node.masked && node.s2.status !== 'insufficient') node.s2.status = 'inconclusive'
    node.s3 = s3Of(seeded, tuples, node.missing === 0 && node.incomplete === 0)
    nodes[major] = node
  }
  return { nodes }
}

/**
 * @param {object} s3 the S3 summary of one Node @returns {string} its table cell
 */
const s3Text = (s3) => {
  if (s3.status === 'insufficient')
    return 'insufficient (needs the complete collection with three replicates per tuple)'
  const sigmas = LONG_WINDOW.thresholds.sigmas
  const sd = s3.sd.toFixed(1)
  return (
    `${s3.status} (seeded ${s3.seededMean.toFixed(1)} - ${sigmas}*${sd} = ${s3.seededBound.toFixed(1)} ` +
    `vs >= 96; unseeded ${s3.unseededMean.toFixed(1)} + ${sigmas}*${sd} = ${s3.unseededBound.toFixed(1)} vs <= 64)`
  )
}

/**
 * @param {{nodes: Object<string, object>}} summary from `summarise`
 * @param {boolean} [acceptance] whether to label an eligible acceptance collection
 * @returns {string} the printed table, one line per Node in numeric order
 */
const render = ({ nodes }, acceptance = false) => {
  const cell = (text, width) => String(text).padEnd(width)
  const lines = [
    `long-window ${acceptance ? 'acceptance' : 'diagnostic (never acceptance)'} table; mid-cycle data slopes, KiB/min`,
    [
      cell('node', 6),
      cell('S1 seeded>=96', 23),
      cell('S2 unseeded<=64,<=1MiB,unmasked', 40),
      cell('masked', 8),
      cell('first-use', 21),
      'S3',
    ].join(''),
  ]
  for (const major of Object.keys(nodes).sort((a, b) => Number(a) - Number(b))) {
    const node = nodes[major]
    lines.push(
      [
        cell(major, 6),
        cell(`${node.s1.pass}/${node.s1.total} ${node.s1.status}`, 23),
        cell(`${node.s2.pass}/${node.s2.total} ${node.s2.status}`, 40),
        cell(node.masked, 8),
        cell(`${node.firstUse.tripped}/${node.firstUse.total} ${node.firstUse.status}`, 21),
        s3Text(node.s3) +
          (node.missing ? ` [${node.missing} missing workers]` : '') +
          (node.incomplete ? ` [${node.incomplete} incomplete workers]` : ''),
      ].join('')
    )
  }
  return lines.join('\n')
}

/**
 * Requires the complete adopted collection and reuses the report's S1/S2/S3 and first-use judgement.
 * Historical diagnostic replay is explicit and does not make its records acceptance-eligible.
 * @param {object[]} records the 39 job records
 * @param {{tree:string, runId:string, attempt:string, replayDiagnostic?:boolean}} expected provenance
 * @returns {object} the passing summary
 * @throws {Error} for wrong provenance, ineligible or incomplete evidence, or any failing check
 */
const verifyAcceptance = (records, { tree, runId, attempt, replayDiagnostic = false }) => {
  if (![tree, runId, attempt].every((value) => typeof value === 'string' && value.length))
    throw new Error('Expected tree, run and attempt are required')
  if (records.length !== 39)
    throw new Error(`Expected 39 long-window artifacts, found ${records.length}`)
  for (const record of records) {
    if (
      record.tree !== tree ||
      record.sha !== tree ||
      record.runId !== runId ||
      record.attempt !== attempt
    )
      throw new Error('Foreign tree, run or attempt in long-window collection')
    if (
      !isDeepStrictEqual(
        record.candidate,
        identityOf({
          arm: record.arm,
          replicate: record.replicate,
          tree,
          env: {},
        }).candidate
      )
    )
      throw new Error('Expected adopted long-window v2 identity')
    const eligible = replayDiagnostic
      ? record.diagnostic === true && record.acceptanceEligible === false
      : record.diagnostic === false && record.acceptanceEligible === true
    if (!eligible) throw new Error('Ineligible long-window record')
    if (record.workers?.length !== KINDS.length)
      throw new Error('Expected four long-window workers')
    for (const worker of record.workers) {
      const workerEligible = replayDiagnostic
        ? worker.diagnostic === true && worker.acceptance === false
        : worker.diagnostic === false && worker.acceptance === true && worker.scope === 'acceptance'
      if (!workerEligible) throw new Error('Ineligible long-window worker report')
      if (
        !Number.isFinite(worker.firstUse?.dataBytes) ||
        worker.firstUse.tripped !== worker.firstUse.dataBytes > LONG_WINDOW.thresholds.firstUseBytes
      )
        throw new Error('Invalid long-window first-use measurement')
      if (record.arm === 'unseeded' && worker.firstUse.tripped)
        throw new Error('Unseeded long-window first-use exceeds 1 MiB')
      if (!LONG_WINDOW.v8Flags.every((flag) => worker.execArgv?.includes(flag)))
        throw new Error('Missing long-window worker flag --no-flush-bytecode')
    }
  }
  const summary = summarise(records)
  for (const [major, node] of Object.entries(summary.nodes))
    if (
      node.masked ||
      [node.s1, node.s2, node.s3, node.firstUse].some((check) => check.status !== 'pass')
    )
      throw new Error(`Node ${major}: S1, S2, S3 or first-use failed, inconclusive or incomplete`)
  return summary
}

/**
 * @param {string} directory the separate artifact directories downloaded from one run
 * @returns {object[]} one record per correctly named artifact
 * @throws {Error} for an extra, empty, duplicate or incorrectly named artifact
 */
const readArtifacts = (directory) => {
  const records = []
  for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
    if (!entry.isDirectory()) throw new Error('Unexpected long-window artifact entry')
    const found = readRecords(path.join(directory, entry.name))
    if (found.length !== 1) throw new Error('Expected one record per long-window artifact')
    const record = found[0]
    const major = /^v(\d+)\./.exec(record.node)?.[1]
    const name = `telemetry-long-window-node${major}-${record.kafka}-${record.arm}-r${record.replicate}`
    if (entry.name !== name) throw new Error('Unexpected long-window artifact name')
    records.push(record)
  }
  return records
}

/**
 * @param {string[]} argv directory, optionally `--verdict --tree SHA --run ID --attempt N` and
 *   `--replay-diagnostic` for historical diagnostics only
 * @param {function(string):void} [write] output sink
 * @returns {number} 0 for a report or passing verdict; 1 for a failed verdict
 * @throws {Error} for invalid arguments or unreadable evidence
 */
const main = (argv, write = console.log) => {
  const [directory, ...args] = argv
  const options = {}
  for (let i = 0; i < args.length; i++) {
    const flag = args[i]
    if (['--verdict', '--replay-diagnostic'].includes(flag)) options[flag] = true
    else if (['--tree', '--run', '--attempt'].includes(flag) && args[i + 1])
      options[flag] = args[++i]
    else
      throw new Error(
        'usage: longWindowReport.js <artifact-directory> [--verdict --tree SHA --run ID --attempt N [--replay-diagnostic]]'
      )
  }
  const verdict = options['--verdict'] === true
  if (
    !directory ||
    (args.length && !verdict) ||
    (verdict && !['--tree', '--run', '--attempt'].every((flag) => options[flag]))
  )
    throw new Error(
      'usage: longWindowReport.js <artifact-directory> [--verdict --tree SHA --run ID --attempt N [--replay-diagnostic]]'
    )
  if (!verdict) {
    const records = readRecords(directory)
    const eligible =
      records.length > 0 &&
      records.every((record) => record.diagnostic === false && record.acceptanceEligible === true)
    write(render(summarise(records), eligible))
    return 0
  }
  try {
    const records = readArtifacts(directory)
    write(render(summarise(records), !options['--replay-diagnostic']))
    verifyAcceptance(records, {
      tree: options['--tree'],
      runId: options['--run'],
      attempt: options['--attempt'],
      replayDiagnostic: options['--replay-diagnostic'] === true,
    })
    write(
      options['--replay-diagnostic']
        ? 'PASS: historical diagnostic replay only; records remain ineligible'
        : 'PASS: long-window acceptance'
    )
    return 0
  } catch (error) {
    write(`FAIL: ${error.message}`)
    return 1
  }
}

module.exports = { main, pooledSd, readRecords, render, summarise, verifyAcceptance }
if (require.main === module) {
  try {
    process.exitCode = main(process.argv.slice(2))
  } catch (error) {
    process.stderr.write(`${error.message}\n`)
    process.exitCode = 1
  }
}
