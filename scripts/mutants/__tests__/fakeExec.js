const fs = require('fs')
const path = require('path')

/**
 * A stand-in for the runner's Jest execution: the witness passes while the target still exports 1.
 * It writes the Jest report and the event record exactly as the real environment would.
 * @param {{ target: string, spec: string, title: string }} rec the record
 * @param {string} root the tree
 * @param {string} outFile the Jest JSON file to write
 * @param {{ evidence: string, nonce: string }} run the event file and the run's nonce
 * @returns {{ status: number, signal: null }} Jest's exit
 */
const fakeExec = (rec, root, outFile, run) => {
  const passed = /= 1\b/.test(fs.readFileSync(path.join(root, rec.target), 'utf8'))
  const file = path.join(root, rec.spec)
  const status = passed ? 'passed' : 'failed'
  const assertion = { fullName: rec.title, status, location: { line: 1, column: 1 } }
  const report = { numRuntimeErrorTestSuites: 0, wasInterrupted: false, testResults: [] }
  report.testResults.push({ name: file, assertionResults: [assertion] })
  fs.writeFileSync(outFile, JSON.stringify(report))
  const events = passed
    ? ['test_start', 'test_fn_start', 'test_fn_success', 'test_done']
    : ['test_start', 'test_fn_start', 'test_fn_failure', 'test_done']
  const test = { fullName: rec.title, file, line: 1, column: 1, failing: false, events }
  fs.writeFileSync(run.evidence, JSON.stringify({ nonce: run.nonce, tests: [test], run: [] }))
  return { status: passed ? 0 : 1, signal: null }
}

module.exports = fakeExec
