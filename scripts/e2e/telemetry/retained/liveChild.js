'use strict'

const fs = require('node:fs')
const path = require('node:path')
const Module = require('node:module')
const { LiveCensus, captureSink } = require('./liveCensus')
const root = path.resolve(__dirname, '../../../..')
const resolve = Module._resolveFilename
Module._resolveFilename = function (request, ...rest) {
  return resolve.call(
    this,
    request === 'testHelpers' ? path.join(root, 'testHelpers/index.js') : request,
    ...rest
  )
}
const soak = require('../../../soak/soak')
const { WORKER_ENV } = require('../../../soak/telemetryChurn')
const identity = JSON.parse(fs.readFileSync(process.env.LIVE_CENSUS_IDENTITY, 'utf8'))
if (
  process.env.CI !== 'true' ||
  identity.diagnostic !== true ||
  identity.acceptanceEligible !== false ||
  !['capture', 'scalar'].includes(identity.variant)
)
  throw new Error('live census requires diagnostic CI identity')
const kind = process.env[WORKER_ENV]
const directory = path.join(process.env.LIVE_CENSUS_DIRECTORY, kind)
const census = new LiveCensus({
  sampleAt: soak.TELEMETRY.sampleAt,
  capture: captureSink(directory, { ...identity, kind }),
})
const run = soak.Soak.prototype.run
soak.Soak.prototype.run = async function () {
  const record = this.record
  this.record = (row) => {
    census.observe(row)
    record(row)
  }
  return run.call(this)
}
soak
  .main('telemetry')
  .then(() => {
    if (!census.complete()) throw new Error('incomplete live census checkpoint grid')
  })
  .catch((error) => {
    process.stderr.write(`${error.stack}\n`)
    process.exitCode = 1
  })
