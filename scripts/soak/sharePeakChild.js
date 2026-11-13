'use strict'

const path = require('node:path')
const Module = require('node:module')
const root = path.resolve(__dirname, '../..')
const resolve = Module._resolveFilename
Module._resolveFilename = function (request, ...rest) {
  return resolve.call(
    this,
    request === 'testHelpers' ? path.join(root, 'testHelpers/index.js') : request,
    ...rest
  )
}
const soak = require('./soak')
const { TelemetryWorkerProcess } = require('./telemetryChurn')
const { installSharePeak } = require('./sharePeak')
if (process.env.CI !== 'true' || process.env.SHARE_PEAK_DIAGNOSTIC !== '1')
  throw new Error('share peak requires explicit diagnostic CI')
const run = TelemetryWorkerProcess.prototype.run
TelemetryWorkerProcess.prototype.run = function () {
  installSharePeak(this, process.env.SHARE_PEAK_ARM)
  return run.call(this)
}
soak.main('telemetry').catch((error) => {
  process.stderr.write(`${error.stack}\n`)
  process.exitCode = 1
})
