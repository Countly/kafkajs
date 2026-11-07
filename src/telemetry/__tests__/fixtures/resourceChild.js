'use strict'

/**
 * The supplementary collector-only memory arm, run in its own `node --expose-gc` process by
 * `resources.spec.js`. It builds the maximum legal collector, one persistent payload builder and
 * one sample, compresses with the real built-in codecs, and prints one JSON line of settled
 * readings. It exercises the codecs at the strict 64-node, 257-point maximum, which a public client
 * over a three-node cluster cannot reach, and is never a substitute for the public-client arm. It
 * never calls `process.exit`: a handle it leaves behind keeps the process alive, which the parent
 * fails.
 */
const v8 = require('node:v8')
const TelemetryPayload = require('../../payload')
const zstd = require('../../../protocol/message/compression/zstd')
const { maximum } = require('./maximum')

const SAMPLE_MS = 20

/**
 * Collects twice, a pause apart, and reads heap plus external. `arrayBuffers` is part of
 * `external`, so it is not added again.
 * @returns {Promise<{heapUsed:number, external:number, arrayBuffers:number, retained:number,
 *   rss:number}>}
 */
const settled = async () => {
  globalThis.gc()
  await new Promise((resolve) => setTimeout(resolve, 100))
  globalThis.gc()
  const usage = process.memoryUsage()
  return {
    heapUsed: usage.heapUsed,
    external: usage.external,
    arrayBuffers: usage.arrayBuffers,
    retained: usage.heapUsed + usage.external,
    rss: usage.rss,
  }
}

/** @returns {number} the lifetime peak RSS of this process in bytes */
const lifetimePeak = () => process.resourceUsage().maxRSS * 1024

/**
 * Runs work while reading the current RSS at least every 20 ms, keeping only the count and the
 * maximum so the journal never grows with the run.
 * @template T
 * @param {() => Promise<T>} work every native build of one codec, current and retired
 * @returns {Promise<{result: T, count: number, peak: number}>}
 */
const sampled = async (work) => {
  let count = 1
  let peak = process.memoryUsage.rss()
  const timer = setInterval(() => {
    count += 1
    peak = Math.max(peak, process.memoryUsage.rss())
  }, SAMPLE_MS)
  try {
    const result = await work()
    count += 1
    peak = Math.max(peak, process.memoryUsage.rss())
    return { result, count, peak }
  } finally {
    clearInterval(timer)
  }
}

/**
 * @param {object} metrics the collector @returns {void} binds 64 node pairs with one observation each
 */
const bindNodes = (metrics) => {
  for (let nodeId = 0; nodeId < 64; nodeId++)
    metrics.observeRequest(
      metrics.bindNode({ nodeId, endpoint: `b${nodeId}:9092`, endpointRevision: {} }),
      nodeId
    )
}

/**
 * Builds the readings of one kind.
 * @param {string} kind public client kind @returns {Promise<object>} serializable readings
 */
const measure = async (kind) => {
  const baseline = await settled()
  const baselinePeakRss = lifetimePeak()
  const { metrics, subscription, slots } = maximum(kind)
  slots.forEach((slot) => metrics.retireNode(slot))
  metrics.resetTransient()
  const matched = await settled()
  const builder = new TelemetryPayload(1n)
  bindNodes(metrics)
  let snapshot = metrics.snapshot(subscription)
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
    const { result, count, peak } = await sampled(async () => {
      const current = await builder.buildPayload(snapshot, options)
      let live = true
      const retired = builder.buildPayload(snapshot, { ...options, isCurrent: () => live })
      live = false
      return { current, retiredCompletion: (await retired) === null }
    })
    rssSamples += count
    rssPeakSample = Math.max(rssPeakSample, peak)
    builds.push({
      codec: name,
      compressionType: result.current.compressionType,
      rawBytes: result.current.rawBytes,
      transmittedBytes: result.current.transmittedBytes,
      points: result.current.includedIdentities.length,
      retiredCompletion: result.retiredCompletion,
    })
  }
  const held = await settled()
  metrics.abandon(snapshot)
  snapshot = null
  builder.retire()
  slots.length = 0
  metrics.resetTransient()
  const closed = await settled()
  return {
    kind,
    mode: 'collector',
    zstdSupported: zstd.isSupported,
    baseline,
    baselinePeakRss,
    matched,
    held,
    closed,
    builds,
    rssSamples,
    rssPeakSample,
    peakRss: lifetimePeak(),
    heapLimit: v8.getHeapStatistics().heap_size_limit,
  }
}

const [kind] = process.argv.slice(2)
measure(kind).then(
  (readings) => {
    process.stdout.write(`${JSON.stringify({ readings })}\n`)
  },
  (error) => {
    process.stderr.write(`${error.stack}\n`)
    process.exitCode = 1
  }
)
