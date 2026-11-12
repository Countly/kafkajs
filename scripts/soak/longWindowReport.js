#!/usr/bin/env node
'use strict'

/**
 * Offline analysis of the long-window shadow lane. It reads the downloaded artifacts of the lane's
 * jobs and prints, per Node, the would-be S1 (seeded workers reach 96 KiB/min), S2 (unseeded workers
 * stay within 64 KiB/min and a 1 MiB endpoint, unmasked) and S3 (the seeded mean less three pooled
 * SDs reaches 96 and the unseeded mean plus three stays within 64) tables, the masked workers and the
 * warm-up seed's first-use trips. It judges nothing: every record it reads is diagnostic.
 */
const fs = require('node:fs')
const path = require('node:path')
const { LONG_WINDOW } = require('./longWindow')

const KIB = 1024
const FILE = 'long-window.json'
const NODES = ['22', '24', '26']
const KAFKAS = ['4.2.2', '4.3.1']
const KINDS = ['producer', 'consumer', 'admin', 'shareConsumer']
const REPLICATES = [1, 2, 3]

/** @param {Array<string|number>} parts identity fields @returns {string} an unambiguous map key */
const keyOf = (parts) => JSON.stringify(parts)

/**
 * Indexes one frozen collection without combining commits, runtimes or duplicate downloads.
 * @param {object[]} records the diagnostic job records
 * @returns {Map<string, object>} workers by Node major, Kafka, arm, replicate and kind
 * @throws {Error} for duplicate identities, mixed commits/runtimes or an unexpected matrix tuple
 */
const observationsOf = (records) => {
  const observations = new Map()
  const identities = new Set()
  const runtimes = new Map()
  let commit = null
  for (const record of records) {
    const { node, kafka, arm, replicate, workers } = record
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
 * @throws {Error} for an unreadable directory or a record that is not diagnostic and ineligible
 */
const readRecords = (directory) => {
  const records = []
  for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
    const file = path.join(directory, entry.name)
    if (entry.isDirectory()) records.push(...readRecords(file))
    else if (entry.name === FILE) {
      const record = JSON.parse(fs.readFileSync(file, 'utf8'))
      if (record.diagnostic !== true || record.acceptanceEligible !== false)
        throw new Error(`${file} is not a diagnostic long-window record`)
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
            if (positive.wouldBe.s1.pass) node.s1.pass += 1
            seeded.push(positive.mid.data.slopeBytesPerMinute / KIB)
          }
          const negative = observe(kafka, 'unseeded', replicate, kind, node.s2)
          if (negative) {
            if (negative.wouldBe.s2.pass) node.s2.pass += 1
            if (negative.masked) node.masked += 1
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
 * @returns {string} the printed table, one line per Node in numeric order
 */
const render = ({ nodes }) => {
  const cell = (text, width) => String(text).padEnd(width)
  const lines = [
    'long-window shadow table (diagnostic, never acceptance); mid-cycle data slopes, KiB/min',
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
 * @param {string[]} argv `[<artifact-directory>]`
 * @param {function(string):void} [write] where the table goes, `console.log` by default
 * @returns {number} the exit code, 0: the report is a table, never a verdict
 * @throws {Error} the usage line for another argument list
 */
const main = (argv, write = console.log) => {
  if (argv.length !== 1)
    throw new Error('usage: node scripts/soak/longWindowReport.js <artifact-directory>')
  write(render(summarise(readRecords(argv[0]))))
  return 0
}

module.exports = { main, pooledSd, readRecords, render, summarise }
if (require.main === module) {
  try {
    process.exitCode = main(process.argv.slice(2))
  } catch (error) {
    process.stderr.write(`${error.message}\n`)
    process.exitCode = 1
  }
}
