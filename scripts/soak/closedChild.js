'use strict'

const { LongWindowDiagnostic } = require('./longWindowDiagnostic')
LongWindowDiagnostic.aliasTestHelpers()
const { installClosedDiagnostic } = require('./closedDiagnostic')
const { TelemetryWorkerProcess } = require('./telemetryChurn')
if (process.env.CI !== 'true' || process.env.TELEMETRY_CLOSED_DIAGNOSTIC !== '1')
  throw new Error('closed child requires explicit diagnostic CI')
const run = TelemetryWorkerProcess.prototype.run
TelemetryWorkerProcess.prototype.run = function () {
  installClosedDiagnostic(this)
  return run.call(this)
}
require('./soak')
  .main('telemetry')
  .catch((error) => {
    process.stderr.write(`${error.stack}\n`)
    process.exitCode = 1
  })
