'use strict'

/**
 * One single-client memory arm over an actual public client, run in its own `node --expose-gc`
 * process by `resources.spec.js`. It takes its startup reading before any application import,
 * loads everything either mode needs, then runs a genuinely disabled control of the same kind
 * through the fixed foreground workload, releases it, runs the same enabled catalogue once as a
 * measured warm-up (its readings at the matched position, M0, and after its release) and then
 * measures the enabled client: the 128 maximum descriptors through the public API, the assigned
 * UUID, accepted ordinary pushes with real GZIP and detected ZSTD, a retired native completion
 * with an immediate same-object reopen per codec, and a final public close. Each reopen calls
 * connect, and the final close prints its explicit disconnect receipt, in the public
 * disconnect's own resolution continuation, with the observed order recorded for replay; then,
 * once everything is released, it prints one JSON line with every reading. The run, tree and
 * child identity come from the parent. `RESOURCE_SEED` selects a seeded control, which leaks
 * through a product-reachable root and declares itself in the manifest. It never calls
 * `process.exit`: a handle it leaves behind keeps the process alive, which the parent fails.
 */
const path = require('node:path')
const Module = require('node:module')
const zlib = require('node:zlib')
const inspector = require('node:inspector')

const ROOT = path.join(__dirname, '..', '..', '..', '..')
const {
  BoundaryWitness,
  NativeJournal,
  PendingSampler,
  ProcessMemory,
  SAMPLE_MS,
  hostContext,
} = require(path.join(ROOT, 'testHelpers', 'processMemory.js'))

// Everything above is a builtin or the builtin-only probe: the startup reading precedes any import
// of the application.
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

const {
  DESCRIPTORS,
  DESCRIPTOR_BYTES,
  MEMORY_UUID,
  MemoryForeground,
  ProductSeed,
  SEED,
  descriptor,
  foregroundCatalogue,
  runControl,
  until,
  within,
} = require('./memoryForeground')
const { ResourceRoles } = require(path.join(ROOT, 'testHelpers', 'resourceProbe.js'))
const { phaseCatalogue } = require(
  path.join(ROOT, 'scripts', 'e2e', 'telemetry', 'resourceArtifact.js')
)
const gzipModule = require('../../../protocol/message/compression/gzip')
const zstd = require('../../../protocol/message/compression/zstd')

const witnesses =
  process.env.RESOURCE_WITNESSES === '1'
    ? new (require(path.join(ROOT, 'scripts/e2e/telemetry/warmupWitness')).WarmupWitnesses)()
    : null

const loaded = { ...memory.current(), modules: process.moduleLoadList.length }
memory.mark('loaded')

const activeCensus = process.env.ACTIVE_CENSUS_DIRECTORY
  ? new (require(path.join(ROOT, 'scripts/e2e/telemetry/retained/activeCensus')).ActiveCensus)({
      memory,
      variant: process.env.ACTIVE_CENSUS_VARIANT,
      capture: require(path.join(ROOT, 'scripts/e2e/telemetry/retained/liveCensus')).captureSink(
        process.env.ACTIVE_CENSUS_DIRECTORY,
        {
          diagnostic: true,
          acceptanceEligible: false,
          attributionComplete: false,
          childId: process.env.RESOURCE_CHILD_ID,
          tree: process.env.RESOURCE_TREE,
          runId: process.env.RESOURCE_RUN_ID,
          kind: process.argv[2],
          variant: process.env.ACTIVE_CENSUS_VARIANT,
        }
      ),
    })
  : null

const PHASE_PUSHES = 2
const GZIP_MAGIC = [0x1f, 0x8b]
const ZSTD_MAGIC = 0xfd2fb528

/** @type {import('./memoryForeground').MemoryForeground[]} the clients a failed run still has to close */
const live = []

/**
 * Decompresses the first accepted payload of a codec and checks its format, after final public
 * close and before the closed retained reading, inside peak coverage.
 * @param {{name: string, type: number}} codec the codec
 * @param {Buffer} first the transmitted bytes
 * @returns {Promise<{codec: string, format: boolean, rawBytes: number, transmittedBytes: number}>}
 */
const decode = async (codec, first) => {
  const raw = codec.type === 4 ? await zstd.decompress(first) : zlib.gunzipSync(first)
  const format =
    codec.type === 4
      ? first.readUInt32LE(0) === ZSTD_MAGIC
      : first[0] === GZIP_MAGIC[0] && first[1] === GZIP_MAGIC[1]
  return { codec: codec.name, format, rawBytes: raw.length, transmittedBytes: first.length }
}

/**
 * Runs the whole arm for one kind.
 * @param {string} kind public client kind
 * @returns {Promise<object>} serializable readings
 */
const measure = async (kind) => {
  const codecs = [
    { name: 'gzip', type: 1, module: gzipModule },
    ...(zstd.isSupported ? [{ name: 'zstd', type: 4, module: zstd }] : []),
  ]
  const lifecycles = 1 + 2 * codecs.length
  const capability = memory.gcCapability()
  const manifest = {
    arm: 'public',
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
    fixture: {
      descriptors: DESCRIPTORS,
      descriptorBytes: DESCRIPTORS * DESCRIPTOR_BYTES,
      uuid: MEMORY_UUID,
    },
    foreground: foregroundCatalogue(kind, lifecycles),
    phases: phaseCatalogue('public', zstd.isSupported),
    sampleMs: SAMPLE_MS,
    seed: process.env.RESOURCE_SEED
      ? {
          mode: process.env.RESOURCE_SEED,
          closeBytes: SEED.closeBytes,
          warmupBytes: SEED.warmupBytes,
        }
      : null,
  }
  if (!capability.available) {
    const contexts = { control: hostContext(), boundary: hostContext(), final: hostContext() }
    return { manifest, startup, loaded, marks: memory.marks, contexts }
  }

  const roles = new ResourceRoles({ roles: ['control', 'target', 'fixture'] }).start()
  let activeJournal = null
  const settled = () =>
    roles.run('fixture', () =>
      memory.settled(
        activeCensus && activeJournal
          ? (round, stage) => activeCensus.observe(round, stage, activeJournal)
          : null
      )
    )
  const turn = () => new Promise((resolve) => setImmediate(resolve))
  const contexts = { control: hostContext() }

  /**
   * Starts a sampler and a native journal over the codecs for one phase of the arm, so no phase
   * shares rows, windows or an epoch with another.
   * @returns {{sampler: PendingSampler, journal: NativeJournal, hooks: {epoch: number,
   *   onSubmit: (row: object) => void}, stop: () => void}} the observers and how to end them
   */
  const observation = () => {
    const sampler = new PendingSampler(memory, {
      set: (fn, ms) => roles.run('fixture', () => setInterval(fn, ms)),
    })
    const hooks = { epoch: 0, onSubmit: () => {} }
    const journal = new NativeJournal({
      memory,
      sampler,
      epoch: () => hooks.epoch,
      onSubmit: (row) => hooks.onSubmit(row),
    })
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

  /**
   * Runs the enabled catalogue once on a fresh client: the descriptors, the open and the UUID, a
   * close and the settled matched reading, every codec with its retired job and immediate reopen,
   * the held state and the final public close. The warm-up and the measured arm run this same
   * code; the warm-up marks only its matched position (M0), prints no receipt and returns only
   * that reading.
   * @param {boolean} warm whether this is the warm-up pass
   * @returns {Promise<object>} the matched reading for the warm-up, every reading of the arm
   *   otherwise
   */
  const pass = (warm) =>
    roles.run('target', async () => {
      const watch = observation()
      const { sampler, journal, hooks } = watch
      const seed = ProductSeed.of(process.env.RESOURCE_SEED, warm)
      try {
        if (!warm) memory.mark('enabled:start')
        const enabled = new MemoryForeground(kind, {
          enabled: true,
          seed,
          witnesses: warm ? witnesses : null,
        }).build()
        live.push(enabled)
        const reopens = []
        let armed = null
        let reopen = null
        hooks.onSubmit = (row) => {
          activeCensus?.submit(row, journal)
          if (armed !== row.codec) return
          armed = null
          hooks.epoch += 1
          // Called from inside the submission: the close starts while the native job is in flight
          // and the same object's next call is the connect in the close's own continuation.
          const witness = new BoundaryWitness(() => memory.now())
          if (warm) witnesses?.track('catalogue:reopen', witness)
          reopens.push({ codec: row.codec, events: witness.events })
          reopen = enabled
            .close(0)
            .then(() => {
              witness.resolved()
              witness.call('connect')
              return enabled.client.connect()
            })
            .then(() => enabled.drain())
        }
        for (let index = 0; index < DESCRIPTORS; index++) enabled.register(descriptor(index))
        await enabled.open()
        const uuid = await enabled.client.clientInstanceId({ timeout: 30000 })
        if (!warm) memory.mark('enabled:registered')
        hooks.epoch += 1
        await enabled.close(0)
        await enabled.drain()
        if (warm && witnesses) {
          witnesses.track('foreground', enabled)
          witnesses.track('observation', watch)
          witnesses.track('reopens', reopens)
          witnesses.track('seed:root', require(path.join(ROOT, 'index.js')).Kafka[SEED.root])
        }
        const settledMatched = await settled()
        if (warm) witnesses?.check('m0', settledMatched)
        else witnesses?.check('matched', settledMatched)
        const matched = { ...settledMatched, registry: enabled.fingerprint(uuid) }
        memory.mark(warm ? 'warmup:matched' : 'enabled:matched')
        seed?.matched()
        const matchedResources = roles.read()
        if (!warm) activeCensus?.baseline(matched)

        const actives = []
        for (const codec of codecs) {
          enabled.codecs = [codec.type, 0]
          await enabled.open()
          await until(() => enabled.accepted(codec.type) >= PHASE_PUSHES, `${codec.name} pushes`)
          const before = enabled.accepted(codec.type)
          armed = codec.name
          await until(() => reopen !== null, `${codec.name} retired work`)
          await within(reopen, `${codec.name} close and reopen`)
          reopen = null
          await enabled.resume()
          await enabled.work()
          await until(() => journal.live.size === 0, `${codec.name} retired completion`)
          await until(
            () => enabled.accepted(codec.type) > before,
            `${codec.name} push after reopen`
          )
          if (warm) witnesses?.track(`foreground:${codec.name}`, enabled)
          if (!warm) {
            if (activeCensus) {
              activeCensus.phase = codec.name
              activeJournal = journal
            }
            const active = await settled()
            if (activeCensus) activeCensus.phase = null
            activeJournal = null
            if (actives.length === 0) witnesses?.check('firstActive', active)
            actives.push({ phase: codec.name, ...active, registry: enabled.fingerprint(uuid) })
            memory.mark(`enabled:${codec.name}`)
          }
          if (codec !== codecs.at(-1)) {
            hooks.epoch += 1
            await enabled.close(0)
            await enabled.drain()
          }
        }
        if (!warm) {
          const settledHeld = await settled()
          const held = { phase: 'held', ...settledHeld, registry: enabled.fingerprint(uuid) }
          memory.mark('enabled:held')
          actives.push(held)
        }
        const censusCoverage = !warm
          ? activeCensus?.complete(codecs.map((codec) => codec.name))
          : null
        const uuidHeld = await enabled.client.clientInstanceId({ timeout: 0 })
        hooks.epoch += 1
        const disconnect = new BoundaryWitness(() => memory.now())
        const closing = enabled.close(3000).then(() => {
          disconnect.resolved()
          if (warm) return
          disconnect.call('receipt')
          const at = disconnect.events[1].at
          const receipt = { receipt: 'disconnect', kind, uuid, childId: manifest.childId, at }
          process.stdout.write(`${JSON.stringify(receipt)}\n`)
        })
        await within(closing, 'the final public close')
        await enabled.drain()
        await until(() => journal.live.size === 0, 'the final native completion')
        sampler.stop()

        const pushes = {}
        const decoded = []
        for (const codec of codecs) {
          const entry = enabled.pushes[codec.type]
          pushes[codec.name] = {
            accepted: entry.accepted,
            transmittedBytes: entry.transmittedBytes,
          }
          decoded.push(await decode(codec, entry.first))
          entry.first = null
        }
        await turn()
        await turn()
        if (warm) {
          if (witnesses) {
            witnesses.track('foreground:closed', enabled)
            witnesses.track('observation:closed', watch)
            witnesses.track('catalogue:pushes', pushes)
            witnesses.track('catalogue:decoded', decoded)
            witnesses.track('catalogue:disconnect', disconnect)
            witnesses.track('catalogue:closing', closing)
            witnesses.track('seed:root', require(path.join(ROOT, 'index.js')).Kafka[SEED.root])
          }
          enabled.release()
          live.splice(live.indexOf(enabled), 1)
          return { m0: settledMatched, seed: seed?.summary() ?? null }
        }
        const facts = enabled.facts()
        const settledClosed = await settled()
        witnesses?.check('closed', settledClosed)
        const closed = { ...settledClosed, registry: enabled.fingerprint(uuid) }
        memory.mark('enabled:closed')
        const closedResources = roles.read()
        const registry = enabled.registry.map((entry) => ({ ...entry }))
        enabled.release()
        live.splice(live.indexOf(enabled), 1)
        return {
          uuid,
          uuidHeld,
          censusCoverage,
          matched,
          matchedResources,
          actives,
          closed,
          closedResources,
          facts,
          pushes,
          decoded,
          reopens,
          disconnect: disconnect.summary(),
          registry,
          samples: sampler.summary(),
          native: journal.summary(),
          seed: seed?.summary() ?? null,
        }
      } finally {
        watch.stop()
      }
    })

  /** @returns {Promise<object>} the disabled control, under its own observers */
  const runControlObserved = async () => {
    const watch = observation()
    try {
      return await runControl({ kind, lifecycles, memory, roles, journal: watch.journal, live })
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
        witnesses,
        ...Object.values(require.cache).map((entry) => entry.exports)
      )
    if (witnesses) {
      witnesses.observe(require('../../metrics').prototype, [
        'register',
        'bindNode',
        'select',
        'snapshot',
      ])
      witnesses.observe(require('../../payload').prototype, ['buildPayload'])
    }
    let warmed
    try {
      warmed = await pass(true)
    } finally {
      witnesses?.restore()
    }
    const end = await settled()
    witnesses?.check('warmupEnd', end)
    memory.mark('warmup:closed')
    const enabledRun = await pass(false)

    contexts.final = hostContext()
    const { facts, samples, native, seed, ...arm } = enabledRun
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
      matchedState: { connected: false, descriptors: DESCRIPTORS, uuid: arm.uuid },
      actives: arm.actives,
      closed: arm.closed,
      enabled: facts,
      pushes: arm.pushes,
      decoded: arm.decoded,
      reopens: arm.reopens,
      disconnect: arm.disconnect,
      registry: arm.registry,
      uuid: arm.uuid,
      uuidHeld: arm.uuidHeld,
      ...(activeCensus ? { activeCensus: arm.censusCoverage } : {}),
      descriptors: DESCRIPTORS,
      resources: {
        released: control.releasedResources,
        matched: arm.matchedResources,
        closed: arm.closedResources,
      },
      samples,
      native,
      seeds: { warm: warmed.seed, measured: seed },
      contexts,
      marks: memory.marks,
    }
    roles.stop()
    memory.stop()
    memory.mark('final')
    return readings
  } finally {
    ProductSeed.release()
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
