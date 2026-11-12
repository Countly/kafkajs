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
 * @returns {object} `status` `pass`, `fail` or `insufficient`, the two means, the pooled SD and the
 *   two bounds the thresholds are compared with
 */
const s3Of = (seeded, tuples) => {
  const unseeded = tuples.flat()
  const sd = pooledSd(tuples)
  const seededMean = seeded.length ? mean(seeded) : null
  const unseededMean = unseeded.length ? mean(unseeded) : null
  if (sd === null || seededMean === null)
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
 * @returns {{nodes: Object<string, object>}} per Node major version: `s1` and `s2` as `{pass, total}`,
 *   the masked unseeded workers, the first-use trips of the warm-up seed, the incomplete workers and
 *   `s3`; incomplete workers are counted and never enter a table
 */
const summarise = (records) => {
  const nodes = {}
  const seeded = {}
  const tuples = {}
  for (const record of records) {
    const major = /^v(\d+)\./.exec(record.node)?.[1] ?? String(record.node)
    nodes[major] ??= {
      s1: { pass: 0, total: 0 },
      s2: { pass: 0, total: 0 },
      masked: 0,
      firstUse: { tripped: 0, total: 0 },
      incomplete: 0,
    }
    seeded[major] ??= []
    tuples[major] ??= new Map()
    const node = nodes[major]
    for (const worker of record.workers) {
      if (!worker.complete) {
        node.incomplete += 1
        continue
      }
      const slope = worker.mid.data.slopeBytesPerMinute / KIB
      if (record.arm === 'seeded') {
        node.s1.total += 1
        if (worker.wouldBe?.s1?.pass) node.s1.pass += 1
        seeded[major].push(slope)
      } else if (record.arm === 'unseeded') {
        node.s2.total += 1
        if (worker.wouldBe?.s2?.pass) node.s2.pass += 1
        if (worker.masked) node.masked += 1
        const key = `${record.kafka}|${worker.kind}`
        tuples[major].set(key, [...(tuples[major].get(key) ?? []), slope])
      } else if (record.arm === 'warmup-seed') {
        node.firstUse.total += 1
        if (worker.firstUse?.tripped) node.firstUse.tripped += 1
      }
    }
  }
  for (const major of Object.keys(nodes))
    nodes[major].s3 = s3Of(seeded[major], [...tuples[major].values()])
  return { nodes }
}

/**
 * @param {object} s3 the S3 summary of one Node @returns {string} its table cell
 */
const s3Text = (s3) => {
  if (s3.status === 'insufficient')
    return 'n/a (needs seeded and unseeded workers and two replicates per tuple)'
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
      cell('S1 seeded>=96', 15),
      cell('S2 unseeded<=64,<=1MiB,unmasked', 33),
      cell('masked', 8),
      cell('first-use', 11),
      'S3',
    ].join(''),
  ]
  for (const major of Object.keys(nodes).sort((a, b) => Number(a) - Number(b))) {
    const node = nodes[major]
    lines.push(
      [
        cell(major, 6),
        cell(`${node.s1.pass}/${node.s1.total}`, 15),
        cell(`${node.s2.pass}/${node.s2.total}`, 33),
        cell(node.masked, 8),
        cell(`${node.firstUse.tripped}/${node.firstUse.total}`, 11),
        s3Text(node.s3) + (node.incomplete ? ` [${node.incomplete} incomplete workers]` : ''),
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
