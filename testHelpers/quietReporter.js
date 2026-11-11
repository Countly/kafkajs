/** Reports completed suites without status ticks, output buffering or reporter-owned timers. */
class QuietReporter {
  /**
   * Writes test names, outcomes and failure details directly when a suite completes.
   * @param {object} test Jest test descriptor.
   * @param {object} result Completed suite result, including runtime failures.
   * @returns {void}
   */
  onTestResult(test, result) {
    const lines = [
      result.testFilePath,
      ...result.testResults.map(({ status, fullName }) => `  ${status} ${fullName}`),
    ]
    const failure =
      result.failureMessage || result.testExecError?.stack || result.testExecError?.message
    if (failure) lines.push(failure)
    process.stderr.write(lines.join('\n') + '\n')
  }
}

module.exports = QuietReporter
