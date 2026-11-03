#!/usr/bin/env node
'use strict'

/**
 * CI oracle for the exit gate. Live specs skip themselves when their env flag is missing, so a
 * green Jest exit proves nothing; this check reads the Jest JSON report and fails a gate job
 * when it ran nothing, failed, skipped an unlisted test, or lacks an expected passing test.
 *
 * Usage: node scripts/ci/checkIdentities.js <jest-json-report> <check-id> [gate.json]
 */

const fs = require('fs')
const path = require('path')

const ROOT = path.resolve(__dirname, '..', '..')
const DEFAULT_GATE = path.join(__dirname, 'gate.json')
const SEPARATOR = ' › '

/**
 * Makes a report's test file path repo-relative with forward slashes.
 * A path outside the root is returned as given, so it can never match a gate title.
 * @param {string} file absolute or repo-relative path
 * @param {string} [root] repo root
 * @returns {string}
 */
const normalizePath = (file, root = ROOT) => {
  const relative = path.isAbsolute(file) ? path.relative(root, file) : path.normalize(file)
  return relative.startsWith('..') ? file : relative.split(path.sep).join('/')
}

/**
 * Builds the identity a gate title is written as: `<repo-relative path> › <full title>`.
 * @param {string} file test file path
 * @param {string} fullName Jest `fullName` (ancestor titles and title joined by spaces)
 * @param {string} [root] repo root
 * @returns {string}
 */
const identityOf = (file, fullName, root = ROOT) =>
  `${normalizePath(file, root)}${SEPARATOR}${fullName}`

/**
 * Groups every reported test by identity, collecting each run's status.
 * A suite that failed to run has no tests, so it is returned apart.
 * @param {object} report Jest `--json` report
 * @param {string} root repo root
 * @returns {{statuses: Map<string, string[]>, brokenSuites: string[]}}
 */
const collect = (report, root) => {
  const statuses = new Map()
  const brokenSuites = []
  for (const suite of report.testResults || []) {
    const results = suite.assertionResults || []
    if (suite.status === 'failed' && results.length === 0)
      brokenSuites.push(normalizePath(suite.name, root))
    for (const test of results) {
      const id = identityOf(suite.name, test.fullName, root)
      statuses.set(id, [...(statuses.get(id) || []), test.status])
    }
  }
  return { statuses, brokenSuites }
}

/**
 * Judges a Jest report against a gate check.
 * Fails on: no tests, a suite that did not run, a failed test, a skipped/pending/todo test that
 * is not in `allowedSkips`, and an expected title that is missing or did not pass.
 * @param {object} report Jest `--json` report
 * @param {string[]} expectedTitles `<path> › <title>` entries that must all have passed
 * @param {{allowedSkips?: string[], root?: string}} [options] `allowedSkips` are tolerated skips
 * @returns {{ok: boolean, problems: string[], passed: number, total: number}}
 */
const check = (report, expectedTitles, { allowedSkips = [], root = ROOT } = {}) => {
  const { statuses, brokenSuites } = collect(report, root)
  const expected = new Set(expectedTitles)
  const tolerated = new Set(allowedSkips)
  const problems = []
  let total = 0
  let passed = 0

  if (statuses.size === 0) problems.push('the report has no tests')
  for (const suite of brokenSuites) problems.push(`suite did not run: ${suite}`)

  for (const title of expectedTitles) {
    const seen = statuses.get(title)
    if (!seen) problems.push(`expected test is missing: ${title}`)
    else if (seen.some((status) => status !== 'passed'))
      problems.push(`expected test did not pass (${seen.join(', ')}): ${title}`)
  }

  for (const [id, seen] of statuses) {
    total += seen.length
    passed += seen.filter((status) => status === 'passed').length
    if (expected.has(id)) continue
    if (seen.includes('failed')) problems.push(`test failed: ${id}`)
    else if (!tolerated.has(id) && seen.some((status) => status !== 'passed'))
      problems.push(`test was not run (${seen.join(', ')}): ${id}`)
  }

  if (total > 0 && passed === 0 && problems.length === 0)
    problems.push('the report has no passing tests')
  return { ok: problems.length === 0, problems, passed, total }
}

/**
 * Reads and parses a JSON file.
 * @param {string} file path
 * @param {string} what label for the error message
 * @returns {object}
 * @throws {Error} when the file is unreadable or not JSON
 */
const readJson = (file, what) => {
  try {
    return JSON.parse(fs.readFileSync(file, 'utf8'))
  } catch (error) {
    throw new Error(`cannot read ${what} ${file}: ${error.message}`)
  }
}

/**
 * Runs the CLI.
 * @param {string[]} argv `[report, checkId, gatePath?]`
 * @param {{out?: (line: string) => void, err?: (line: string) => void}} [io] line writers
 * @returns {number} process exit code: 0 pass, 1 anything else
 */
const run = (argv, { out = console.log, err = console.error } = {}) => {
  const [reportPath, checkId, gatePath = DEFAULT_GATE] = argv
  if (!reportPath || !checkId) {
    err('usage: checkIdentities.js <jest-json-report> <check-id> [gate.json]')
    return 1
  }
  let gateCheck
  let result
  try {
    const gate = readJson(gatePath, 'gate file')
    gateCheck = (gate.checks || []).find((entry) => entry.id === checkId)
    if (!gateCheck) {
      err(`${checkId}: unknown check id in ${gatePath}`)
      return 1
    }
    result = check(readJson(reportPath, 'report'), gateCheck.titles || [], {
      allowedSkips: gateCheck.allowedSkips,
    })
  } catch (error) {
    err(error.message)
    return 1
  }
  if (!result.ok) {
    for (const problem of result.problems) err(`${checkId}: ${problem}`)
    return 1
  }
  out(
    `${checkId}: ok, ${result.passed}/${result.total} tests passed, ${(gateCheck.titles || []).length} expected titles found`
  )
  return 0
}

module.exports = { check, normalizePath, identityOf, run }

if (require.main === module) process.exitCode = run(process.argv.slice(2))
