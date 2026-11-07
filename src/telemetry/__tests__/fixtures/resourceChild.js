'use strict'

/**
 * One single-client memory arm, run in its own `node --expose-gc` process by `resources.spec.js`.
 * It builds the maximum legal collector, one persistent payload builder and one sample, compresses
 * with the real built-in codecs, and prints one JSON line of settled readings. It never calls
 * `process.exit`: a handle it leaves behind keeps the process alive, which the parent fails.
 */
const v8 = require('node:v8')
const TelemetryPayload = require('../../payload')
const zstd = require('../../../protocol/message/compression/zstd')
const { maximum } = require('./maximum')

const MAX_RSS_SAMPLES = 4096

/**
 * Collects twice, a pause apart, and reads heap plus external. `arrayBuffers` is part of
 * `external`, so it is not added again.
 * @returns {Promise<{heapUsed:number, external:number, retained:number, rss:number}>}
 */
const settled = async () => {
  globalThis.gc()
  await new Promise((resolve) => setTimeout(resolve, 100))
  globalThis.gc()
  const usage = process.memoryUsage()
  return {
    heapUsed: usage.heapUsed,
    external: usage.external,
    retained: usage.heapUsed + usage.external,
    rss: usage.rss,
  }
}

/**
 * Runs one build while sampling the current RSS at least every 20 ms.
 * @param {Function} build starts the real build @returns {Promise<{result:*, samples:number[]}>}
 */
const sampled = async (build) => {
  const samples = [process.memoryUsage.rss()]
  const timer = setInterval(() => {
    if (samples.length < MAX_RSS_SAMPLES) samples.push(process.memoryUsage.rss())
  }, 20)
  try {
    const result = await build()
    samples.push(process.memoryUsage.rss())
    return { result, samples }
  } finally {
    clearInterval(timer)
  }
}

/**
 * Builds the readings of one kind.
 * @param {string} kind public client kind @returns {Promise<object>} serializable readings
 */
const measure = async (kind) => {
  const baseline = await settled()
  const { metrics, subscription } = maximum(kind)
  const matched = await settled()
  const builder = new TelemetryPayload(1n)
  const snapshot = metrics.snapshot(subscription)
  const codecs = [
    { name: 'gzip', type: 1 },
    ...(zstd.isSupported ? [{ name: 'zstd', type: 4 }] : []),
  ]
  const builds = []
  let rssSamples = 0
  let rssPeakSample = 0
  for (const { name, type } of codecs) {
    const options = {
      telemetryMaxBytes: 1048576,
      acceptedCompressionTypes: [type, 0],
      isCurrent: () => true,
    }
    const { result, samples } = await sampled(() => builder.buildPayload(snapshot, options))
    rssSamples += samples.length
    rssPeakSample = Math.max(rssPeakSample, ...samples)
    builds.push({
      codec: name,
      compressionType: result.compressionType,
      rawBytes: result.rawBytes,
      transmittedBytes: result.transmittedBytes,
      points: result.includedIdentities.length,
    })
    let live = true
    const retired = builder.buildPayload(snapshot, { ...options, isCurrent: () => live })
    live = false
    builds.push({ codec: name, retiredCompletion: (await retired) === null })
  }
  const held = await settled()
  metrics.abandon(snapshot)
  builder.retire()
  metrics.resetTransient()
  const closed = await settled()
  return {
    kind,
    baseline,
    matched,
    held,
    closed,
    builds,
    rssSamples,
    rssPeakSample,
    peakRss: process.resourceUsage().maxRSS * 1024,
    heapLimit: v8.getHeapStatistics().heap_size_limit,
  }
}

const [kind] = process.argv.slice(2)
measure(kind).then(
  (readings) => {
    process.stdout.write(`${JSON.stringify(readings)}\n`)
  },
  (error) => {
    process.stderr.write(`${error.stack}\n`)
    process.exitCode = 1
  }
)
