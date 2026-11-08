'use strict'

/**
 * One single-client memory arm over an actual public client, run in its own `node --expose-gc`
 * process by `resources.spec.js`. It takes its startup reading before any application import,
 * loads everything either mode needs, then runs a genuinely disabled control of the same kind
 * through the fixed foreground workload, releases it, and measures the enabled client: the 128
 * maximum descriptors through the public API, the assigned UUID, accepted ordinary pushes with
 * real GZIP and detected ZSTD, a retired native completion with an immediate same-object reopen
 * per codec, and a final public close. Each reopen calls connect, and the final close prints its
 * explicit disconnect receipt, in the public disconnect's own resolution continuation, with the
 * observed order recorded for replay; then, once everything is released, it prints one JSON line
 * with every reading. The run, tree and child identity come from the parent. It never calls
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

const loaded = { ...memory.current(), modules: process.moduleLoadList.length }
memory.mark('loaded')

const PHASE_PUSHES = 2
const GZIP_MAGIC = [0x1f, 0x8b]
const ZSTD_MAGIC = 0xfd2fb528

/** @type {import('./memoryForeground').MemoryForeground[]} the clients a failed run still has to close */
const live = []

/**
 * Decompresses the first accepted payload of a codec and checks its format, after the closed
 * reading's window and counted in the child's own final peak.
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
  let onSubmit = () => {}
  const journal = new NativeJournal({
    memory,
    sampler,
    epoch: () => epoch,
    onSubmit: (row) => onSubmit(row),
  })
  for (const codec of codecs) journal.observe(codec.name, codec.module)
  const settled = () => roles.run('fixture', () => memory.settled())
  const turn = () => new Promise((resolve) => setImmediate(resolve))
  const contexts = { control: hostContext() }

  try {
    const control = await runControl({ kind, lifecycles, memory, roles, journal, live })
    contexts.boundary = hostContext()

    const enabledRun = await roles.run('target', async () => {
      memory.mark('enabled:start')
      const enabled = new MemoryForeground(kind, { enabled: true }).build()
      live.push(enabled)
      const reopens = []
      let armed = null
      let reopen = null
      onSubmit = (row) => {
        if (armed !== row.codec) return
        armed = null
        epoch += 1
        // Called from inside the submission: the close starts while the native job is in flight
        // and the same object's next call is the connect in the close's own continuation.
        const witness = new BoundaryWitness(() => memory.now())
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
      memory.mark('enabled:registered')
      epoch += 1
      await enabled.close(0)
      await enabled.drain()
      const settledMatched = await settled()
      const matched = { ...settledMatched, registry: enabled.fingerprint(uuid) }
      memory.mark('enabled:matched')
      const matchedResources = roles.read()

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
        await until(() => enabled.accepted(codec.type) > before, `${codec.name} push after reopen`)
        const active = await settled()
        actives.push({ phase: codec.name, ...active, registry: enabled.fingerprint(uuid) })
        memory.mark(`enabled:${codec.name}`)
        if (codec !== codecs.at(-1)) {
          epoch += 1
          await enabled.close(0)
          await enabled.drain()
        }
      }
      const settledHeld = await settled()
      const held = { phase: 'held', ...settledHeld, registry: enabled.fingerprint(uuid) }
      memory.mark('enabled:held')
      actives.push(held)
      const uuidHeld = await enabled.client.clientInstanceId({ timeout: 0 })
      epoch += 1
      const disconnect = new BoundaryWitness(() => memory.now())
      const closing = enabled.close(3000).then(() => {
        disconnect.resolved()
        disconnect.call('receipt')
        const at = disconnect.events[1].at
        const receipt = { receipt: 'disconnect', kind, uuid, childId: manifest.childId, at }
        process.stdout.write(`${JSON.stringify(receipt)}\n`)
      })
      await within(closing, 'the final public close')
      await enabled.drain()
      sampler.stop()

      const pushes = {}
      const decoded = []
      for (const codec of codecs) {
        const entry = enabled.pushes[codec.type]
        pushes[codec.name] = { accepted: entry.accepted, transmittedBytes: entry.transmittedBytes }
        decoded.push(await decode(codec, entry.first))
        entry.first = null
      }
      await turn()
      await turn()
      const facts = enabled.facts()
      const settledClosed = await settled()
      const closed = { ...settledClosed, registry: enabled.fingerprint(uuid) }
      memory.mark('enabled:closed')
      const closedResources = roles.read()
      const registry = enabled.registry.map((entry) => ({ ...entry }))
      enabled.release()
      live.splice(live.indexOf(enabled), 1)
      return {
        uuid,
        uuidHeld,
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
      }
    })

    contexts.final = hostContext()
    const { facts, ...arm } = enabledRun
    const readings = {
      manifest,
      startup,
      loaded,
      c0: control.c0,
      c1: control.c1,
      released: control.released,
      control: control.facts,
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
      descriptors: DESCRIPTORS,
      resources: {
        released: control.releasedResources,
        matched: arm.matchedResources,
        closed: arm.closedResources,
      },
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
    for (const client of live.splice(0)) {
      await client.close(0).catch(() => {})
      await client.drain().catch(() => {})
    }
  }
)
