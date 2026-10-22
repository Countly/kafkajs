const fs = require('fs')

/**
 * Reads a Mocha JSON report as unique full-title and final-status pairs.
 * @param {object} report a JSON reporter result
 * @returns {Map<string, string>} test identity to passed, failed or pending
 * @throws {Error} for an incomplete, duplicate or inconsistent report
 */
const identities = (report) => {
  if (
    !report ||
    !['tests', 'passes', 'failures', 'pending'].every((key) => Array.isArray(report[key]))
  ) {
    throw new Error('expected tests, passes, failures and pending arrays')
  }
  const names = (rows) =>
    rows.map((row) => {
      if (!row || typeof row.fullTitle !== 'string' || row.fullTitle.trim() === '') {
        throw new Error('every test needs a nonempty fullTitle')
      }
      return row.fullTitle
    })
  const tests = names(report.tests)
  if (tests.length === 0 || new Set(tests).size !== tests.length)
    throw new Error('tests must be nonempty and unique')
  const result = new Map()
  let hooks = 0
  for (const [key, status] of [
    ['passes', 'passed'],
    ['failures', 'failed'],
    ['pending', 'pending'],
  ]) {
    for (const name of names(report[key])) {
      // Mocha reports a failed hook as a failure that is not one of `tests`.
      const hook = !tests.includes(name)
      if ((hook && key !== 'failures') || result.has(name))
        throw new Error(`inconsistent status for ${name}`)
      hooks += hook ? 1 : 0
      result.set(name, status)
    }
  }
  if (result.size !== tests.length + hooks)
    throw new Error('every test needs exactly one final status')
  return result
}

/**
 * Compares two reports, ignoring timings and ordering, with deterministic identity diagnostics.
 * @param {object} previous baseline JSON reporter output
 * @param {object} current candidate JSON reporter output
 * @returns {string[]} removed and added identity/status pairs, sorted by full title
 * @throws {Error} for an invalid report
 */
const compare = (previous, current) => {
  const left = identities(previous)
  const right = identities(current)
  const changes = []
  for (const title of [...new Set([...left.keys(), ...right.keys()])].sort()) {
    if (left.get(title) === right.get(title)) continue
    if (left.has(title)) changes.push(`- ${JSON.stringify([title, left.get(title)])}`)
    if (right.has(title)) changes.push(`+ ${JSON.stringify([title, right.get(title)])}`)
  }
  return changes
}

/**
 * Compares two report paths. Diagnostics go to stderr for invalid input and stdout for a diff.
 * @param {string[]} args exactly two paths
 * @returns {number} 0 for identical sets, 1 for a difference, 2 for invalid input
 */
const main = (args) => {
  try {
    if (args.length !== 2)
      throw new Error('usage: node scripts/canary/compare.js <previous.json> <current.json>')
    const changes = compare(...args.map((file) => JSON.parse(fs.readFileSync(file, 'utf8'))))
    process.stdout.write(
      changes.length ? `${changes.join('\n')}\n` : 'identical test identities and statuses\n'
    )
    return changes.length ? 1 : 0
  } catch (error) {
    process.stderr.write(`${error.message}\n`)
    return 2
  }
}
if (require.main === module) process.exitCode = main(process.argv.slice(2))
module.exports = { compare }
