'use strict'

/**
 * The supplementary collector-only memory arm, run in its own `node --expose-gc` process by
 * `resources.spec.js`. It exercises the real built-in codecs at the strict 64-node, 257-point
 * collector maximum, which a public client over a one-node wire cannot reach, and is never a
 * substitute for the public-client arm. Like that arm it takes its startup reading before any
 * application import and runs its own same-kind disabled control, in this process, before the
 * collector exists; it does not borrow another child's baseline. It prints one JSON line of
 * readings and never calls `process.exit`: a handle it leaves behind keeps the process alive,
 * which the parent fails.
 */
const path = require('node:path')
const Module = require('node:module')
const inspector = require('node:inspector')

const ROOT = path.join(__dirname, '..', '..', '..', '..')
const { ProcessMemory, PendingSampler, NativeJournal, hostContext, SAMPLE_MS } = require(
  path.join(ROOT, 'testHelpers', 'processMemory.js')
)

const memory = new ProcessMemory()
const startup = { ...memory.current(), modules: process.moduleLoadList.length }
memory.mark('startup')

// Opened before the observer starts, so the stdio pipes are never read as surviving handles.
if (process.stdout.fd < 0 || process.stderr.fd < 0) throw new Error('stdio is not open')

const nativeResolve = Module._resolveFilename
// The helpers resolve this alias through jest's module map; a plain process needs the same one.
Module._resolveFilename = function (request, ...rest) {
  const target = request === 'testHelpers' ? path.join(ROOT, 'testHelpers', 'index.js') : request
  return nativeResolve.call(this, target, ...rest)
}

const { foregroundCatalogue, runControl } = require('./memoryForeground')
const { ResourceRoles } = require(path.join(ROOT, 'testHelpers', 'resourceProbe.js'))
const { phaseCatalogue } = require(
  path.join(ROOT, 'scripts', 'e2e', 'telemetry', 'resourceArtifact.js')
)
const TelemetryPayload = require('../../payload')
const gzipModule = require('../../../protocol/message/compression/gzip')
const zstd = require('../../../protocol/message/compression/zstd')
const { maximum } = require('./maximum')

const loaded = { ...memory.current(), modules: process.moduleLoadList.length }
memory.mark('loaded')

/** @type {import('./memoryForeground').MemoryForeground[]} the clients a failed run still has to close */
const live = []

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
  const codecs = [
    { name: 'gzip', type: 1, module: gzipModule },
    ...(zstd.isSupported ? [{ name: 'zstd', type: 4, module: zstd }] : []),
  ]
  const lifecycles = 1 + 2 * codecs.length
  const capability = memory.gcCapability()
  const manifest = {
    arm: 'collector',
    kind,
    childId: process.env.RESOURCE_CHILD_ID,
    tree: process.env.RESOURCE_TREE,
    runId: process.env.RESOURCE_RUN_ID,
    pid: process.pid,
    node: process.version,
    v8: process.versions.v8,
    platform: process.platform,
    arch: process.arch,
    execArgv: process.execArgv,
    instrumented: {
      inspector: inspector.url() !== undefined,
      coverage: process.env.NODE_V8_COVERAGE !== undefined,
      jest: typeof globalThis.jest !== 'undefined',
    },
    peakSource: memory.source,
    gc: capability,
    codecs: { gzip: true, zstd: zstd.isSupported },
    fixture: { nodes: 64, points: 257 },
    foreground: foregroundCatalogue(kind, lifecycles),
    phases: phaseCatalogue('collector', zstd.isSupported),
    sampleMs: SAMPLE_MS,
  }
  if (!capability.available) {
    const contexts = { control: hostContext(), boundary: hostContext(), final: hostContext() }
    return { manifest, startup, loaded, marks: memory.marks, contexts }
  }

  const roles = new ResourceRoles({ roles: ['control', 'target', 'fixture'] }).start()
  const sampler = new PendingSampler(memory, {
    set: (fn, ms) => roles.run('fixture', () => setInterval(fn, ms)),
  })
  let epoch = 0
  const journal = new NativeJournal({ memory, sampler, epoch: () => epoch })
  for (const codec of codecs) journal.observe(codec.name, codec.module)
  const settled = () => roles.run('fixture', () => memory.settled())
  const contexts = { control: hostContext() }

  try {
    const control = await runControl({ kind, lifecycles, memory, roles, journal, live })
    contexts.boundary = hostContext()

    memory.mark('collector:start')
    const { metrics, subscription, slots } = maximum(kind)
    slots.forEach((slot) => metrics.retireNode(slot))
    metrics.resetTransient()
    const matched = await settled()
    memory.mark('collector:matched')
    const builder = new TelemetryPayload(1n)
    bindNodes(metrics)
    let snapshot = metrics.snapshot(subscription)
    const builds = []
    const actives = []
    for (const codec of codecs) {
      const options = {
        telemetryMaxBytes: 1048576,
        acceptedCompressionTypes: [codec.type, 0],
        isCurrent: () => true,
      }
      const current = await builder.buildPayload(snapshot, options)
      let valid = true
      const retiring = builder.buildPayload(snapshot, { ...options, isCurrent: () => valid })
      epoch += 1
      valid = false
      builds.push({
        codec: codec.name,
        compressionType: current.compressionType,
        rawBytes: current.rawBytes,
        transmittedBytes: current.transmittedBytes,
        points: current.includedIdentities.length,
        retiredCompletion: (await retiring) === null,
      })
      actives.push({ phase: codec.name, ...(await settled()) })
      memory.mark(`collector:${codec.name}`)
    }
    const held = { phase: 'held', ...(await settled()) }
    memory.mark('collector:held')
    actives.push(held)
    metrics.abandon(snapshot)
    snapshot = null
    builder.retire()
    slots.length = 0
    metrics.resetTransient()
    sampler.stop()
    const closed = await settled()
    memory.mark('collector:closed')
    contexts.final = hostContext()
    const readings = {
      manifest,
      startup,
      loaded,
      c0: control.c0,
      c1: control.c1,
      released: control.released,
      control: control.facts,
      matched,
      actives,
      closed,
      builds,
      resources: { released: control.releasedResources },
      samples: sampler.summary(),
      native: journal.summary(),
      contexts,
      marks: memory.marks,
    }
    journal.restoreAll()
    roles.stop()
    memory.stop()
    memory.mark('final')
    return readings
  } finally {
    sampler.stop()
    journal.restoreAll()
    roles.stop()
    memory.stop()
  }
}

const [kind] = process.argv.slice(2)
measure(kind).then(
  (readings) => {
    process.stdout.write(`${JSON.stringify({ readings })}\n`)
  },
  async (error) => {
    process.stderr.write(`${error.stack}\n`)
    process.exitCode = 1
    for (const client of live.splice(0)) await client.close(0).catch(() => {})
  }
)
