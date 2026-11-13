'use strict'

/**
 * The supplementary collector-only memory arm, run in its own `node --expose-gc` process by
 * `resources.spec.js`. It exercises the real built-in codecs at the strict 64-node, 257-point
 * collector maximum, which a public client over a one-node wire cannot reach, and is never a
 * substitute for the public-client arm. Like that arm it takes its startup reading before any
 * application import and runs its own same-kind disabled control, in this process, before the
 * collector exists; it does not borrow another child's baseline. It then runs the collector
 * catalogue once as a measured warm-up (its readings at the matched position, M0, and after its
 * release) before the arm itself. It prints one JSON line of readings and never calls
 * `process.exit`: a handle it leaves behind keeps the process alive, which the parent fails.
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

const { foregroundCatalogue, nativePair, runControl } = require('./memoryForeground')
const { ResourceRoles } = require(path.join(ROOT, 'testHelpers', 'resourceProbe.js'))
const { phaseCatalogue } = require(
  path.join(ROOT, 'scripts', 'e2e', 'telemetry', 'resourceArtifact.js')
)
const TelemetryPayload = require('../../payload')
const gzipModule = require('../../../protocol/message/compression/gzip')
const zstd = require('../../../protocol/message/compression/zstd')
const { maximum, bindNodes } = require('./maximum')

const witnesses =
  process.env.RESOURCE_WITNESSES === '1'
    ? new (require(path.join(ROOT, 'scripts/e2e/telemetry/warmupWitness')).WarmupWitnesses)()
    : null

const loaded = { ...memory.current(), modules: process.moduleLoadList.length }
memory.mark('loaded')

/** @type {import('./memoryForeground').MemoryForeground[]} the clients a failed run still has to close */
const live = []

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
    seed: null,
  }
  if (!capability.available) {
    const contexts = { control: hostContext(), boundary: hostContext(), final: hostContext() }
    return { manifest, startup, loaded, marks: memory.marks, contexts }
  }

  const roles = new ResourceRoles({ roles: ['control', 'target', 'fixture'] }).start()
  const settled = () => roles.run('fixture', () => memory.settled())
  const contexts = { control: hostContext() }

  /**
   * Starts a sampler and a native journal over the codecs for one phase of the arm, so no phase
   * shares rows, windows or an epoch with another.
   * @returns {{sampler: PendingSampler, journal: NativeJournal, hooks: {epoch: number},
   *   stop: () => void}} the observers and how to end them
   */
  const observation = () => {
    const sampler = new PendingSampler(memory, {
      set: (fn, ms) => roles.run('fixture', () => setInterval(fn, ms)),
    })
    const hooks = { epoch: 0 }
    const journal = new NativeJournal({ memory, sampler, epoch: () => hooks.epoch })
    for (const codec of codecs) journal.observe(codec.name, codec.module)
    return {
      sampler,
      journal,
      hooks,
      stop: () => {
        sampler.stop()
        journal.restoreAll()
      },
    }
  }

  /** @returns {Promise<object>} the disabled control, under its own observers */
  const runControlObserved = async () => {
    const watch = observation()
    try {
      return await runControl({ kind, lifecycles, memory, roles, journal: watch.journal, live })
    } finally {
      watch.stop()
    }
  }

  /**
   * Runs the collector catalogue once: the strict maximum population, its matched reading, a
   * current payload and a retired completion per codec, the held state and the release of the
   * collector. The warm-up and the measured arm run this same code; the warm-up marks only its
   * matched position (M0) and returns only that reading.
   * @param {boolean} warm whether this is the warm-up pass
   * @returns {Promise<object>} the matched reading for the warm-up, every reading of the arm
   *   otherwise
   */
  const pass = async (warm) => {
    const watch = observation()
    const { sampler, journal, hooks } = watch
    try {
      if (!warm) memory.mark('collector:start')
      const { metrics, subscription, slots } = maximum(kind)
      if (warm) {
        witnesses?.track('catalogue:metrics', metrics)
        witnesses?.track('catalogue:subscription', subscription)
        witnesses?.track('catalogue:slots', slots)
      }
      slots.forEach((slot) => metrics.retireNode(slot))
      slots.length = 0
      metrics.resetTransient()
      const builder = new TelemetryPayload(1n)
      if (warm) {
        witnesses?.track('catalogue:builder', builder)
        witnesses?.track('observation', watch)
      }
      const matched = await settled()
      if (warm) witnesses?.check('m0', matched)
      else witnesses?.check('matched', matched)
      memory.mark(warm ? 'warmup:matched' : 'collector:matched')
      const active = bindNodes(metrics, subscription)
      let snapshot = metrics.snapshot(active)
      if (warm) {
        witnesses?.track('catalogue:active', active)
        witnesses?.track('catalogue:snapshot', snapshot)
      }
      const builds = []
      const actives = []
      for (const codec of codecs) {
        builds.push(
          await nativePair(builder, snapshot, codec, () => hooks.epoch++, warm ? witnesses : null)
        )
        if (!warm) {
          if (witnesses) {
            const reading = await settled()
            if (actives.length === 0) witnesses?.check('firstActive', reading)
            actives.push({ phase: codec.name, ...reading })
          } else {
            actives.push({ phase: codec.name, ...(await settled()) })
          }
          memory.mark(`collector:${codec.name}`)
        }
      }
      if (!warm) {
        const held = { phase: 'held', ...(await settled()) }
        memory.mark('collector:held')
        actives.push(held)
      }
      metrics.abandon(snapshot)
      snapshot = null
      builder.retire()
      slots.length = 0
      metrics.resetTransient()
      sampler.stop()
      if (warm) {
        witnesses?.track('observation:closed', watch)
        witnesses?.track('catalogue:builds', builds)
        witnesses?.track('catalogue:actives', actives)
        return { m0: matched }
      }
      const closed = await settled()
      witnesses?.check('closed', closed)
      memory.mark('collector:closed')
      return {
        matched,
        actives,
        closed,
        builds,
        samples: sampler.summary(),
        native: journal.summary(),
      }
    } finally {
      watch.stop()
    }
  }

  try {
    const control = await runControlObserved()
    contexts.boundary = hostContext()

    if (witnesses)
      witnesses.exclude(
        memory,
        roles,
        ...Object.values(require.cache).map((entry) => entry.exports)
      )
    let warmed
    try {
      if (witnesses) {
        witnesses.observe(require('../../metrics').prototype, [
          'register',
          'bindNode',
          'select',
          'snapshot',
        ])
        witnesses.observe(require('../../payload').prototype, ['buildPayload'])
      }
      warmed = await pass(true)
    } finally {
      witnesses?.restore()
    }
    const end = await settled()
    witnesses?.check('warmupEnd', end)
    memory.mark('warmup:closed')
    const arm = await pass(false)
    contexts.final = hostContext()
    const readings = {
      manifest,
      ...(witnesses
        ? { diagnostic: true, acceptanceEligible: false, warmupWitnesses: witnesses.summary() }
        : {}),
      startup,
      loaded,
      c0: control.c0,
      c1: control.c1,
      released: control.released,
      control: control.facts,
      warmup: { m0: warmed.m0, end },
      matched: arm.matched,
      actives: arm.actives,
      closed: arm.closed,
      builds: arm.builds,
      resources: { released: control.releasedResources },
      samples: arm.samples,
      native: arm.native,
      contexts,
      marks: memory.marks,
    }
    roles.stop()
    memory.stop()
    memory.mark('final')
    return readings
  } finally {
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
    for (const client of live.splice(0)) {
      await client.close(0).catch(() => {})
      await client.drain().catch(() => {})
    }
  }
)
