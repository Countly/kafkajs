#!/usr/bin/env node
'use strict'

const fs = require('node:fs')
const path = require('node:path')
const crypto = require('node:crypto')
const { execFileSync } = require('node:child_process')
const { supervise } = require('./warmupWitness')

/**
 * Archives serial original shareConsumer children, one per arm and VARIANTS entry (default flags
 * and pinned young-generation sizes). No acceptance verdict or corrected RSS is computed. Every
 * record is ineligible, including the default-flag companion; original readings remain verbatim.
 * @param {string} directory fresh output directory; pre-existing output is refused
 * @param {{tree:string, runId:string}} stamp parent-issued run identity
 * @param {Function} [run] bounded original-child supervisor, replaceable for broker-free tests
 * @returns {Promise<void>} writes all child records and each variant's exact C1 minus C0 RSS
 * @throws {Error} after archiving all children when any run or endpoint is incomplete
 */
/** Diagnostic variants; supervise maps each non-scalar one to its V8 flags. */
const VARIANTS = ['scalar', 'semi-space-1', 'semi-space-16']

const collect = async (directory, stamp, run = supervise) => {
  fs.mkdirSync(directory)
  const write = (file, value) =>
    fs.writeFileSync(path.join(directory, file), JSON.stringify(value) + '\n')
  const summary = {
    diagnostic: true,
    acceptanceEligible: false,
    ...stamp,
    comparisons: {},
    failures: [],
  }
  for (const arm of ['public', 'collector']) {
    const deltas = {}
    for (const variant of VARIANTS) {
      const identity = {
        ...stamp,
        arm,
        kind: 'shareConsumer',
        mode: 'unseeded',
        variant,
        childId: crypto.randomBytes(16).toString('hex'),
      }
      const record = { ...(await run(identity)), diagnostic: true, acceptanceEligible: false }
      write(`${arm}-${variant}.json`, record)
      const { readings } = record
      const complete =
        record.code === 0 &&
        record.signal === null &&
        record.timedOut === false &&
        record.failure === null &&
        record.stderrBytes === 0 &&
        readings?.controlDiagnostics?.c0 &&
        readings?.controlDiagnostics?.c1 &&
        [readings?.c0?.rss, readings?.c1?.rss].every((n) => Number.isFinite(n) && n >= 0)
      if (!complete) summary.failures.push(`${arm}-${variant}: incomplete child evidence`)
      else deltas[variant] = readings.c1.rss - readings.c0.rss
    }
    summary.comparisons[arm] = deltas
    write('summary.json', summary)
  }
  if (summary.failures.length) throw new Error(summary.failures.join('; '))
}

/**
 * Requires explicit diagnostic intent and runs a bounded serial comparison, without brokers.
 * @param {string[]} args diagnostic-only switch and fresh output directory
 * @returns {Promise<void>} archives evidence; never dispatches acceptance verification
 * @throws {Error} on invalid arguments, incomplete child evidence or existing output
 */
const main = async (args) => {
  if (args.length !== 2 || args[0] !== '--diagnostic-only')
    throw new Error('usage: controlDiagnostic.js --diagnostic-only <new directory>')
  const tree =
    process.env.KAFKAJS_TREE ||
    execFileSync('git', ['rev-parse', 'HEAD'], {
      encoding: 'utf8',
      cwd: path.resolve(__dirname, '../../..'),
    }).trim()
  await collect(path.resolve(args[1]), {
    tree,
    runId: process.env.GITHUB_RUN_ID || 'local-control-diagnostic',
  })
}

if (require.main === module)
  main(process.argv.slice(2)).catch((error) => {
    process.stderr.write(`${error.stack}\n`)
    process.exitCode = 1
  })

module.exports = { VARIANTS, collect, main }
