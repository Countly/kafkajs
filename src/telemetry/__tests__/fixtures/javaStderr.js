const EXPECTED_STDERR =
  /^(SLF4J: .*|WARNING: .*(deprecat|will be removed).*|Note: .*(unchecked|unsafe|Xlint).*)$/
const SOURCE_DEPRECATION =
  /^Note: (?:.*[/\\])?JavaTelemetryClient\.java uses or overrides a deprecated API\.$/

/**
 * Classifies the bounded Java source launcher's stderr without discarding unexpected diagnostics.
 * @param {string} stderr the child's complete, bounded stderr
 * @returns {string[]} every line that is not a known compiler or no-binding notice
 */
const unexpectedJavaStderr = (stderr) =>
  stderr
    .split('\n')
    .filter((line) => line && !EXPECTED_STDERR.test(line) && !SOURCE_DEPRECATION.test(line))

module.exports = { unexpectedJavaStderr }
