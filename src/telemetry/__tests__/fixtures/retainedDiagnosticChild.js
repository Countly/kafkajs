'use strict'

const path = require('node:path')
const Module = require('node:module')
const zlib = require('node:zlib')
const ROOT = path.join(__dirname, '../../../..')
const { DiagnosticLog, CAPACITY } = require(
  path.join(ROOT, 'scripts/e2e/telemetry/retained/contract')
)
const { DiagnosticMemory, DiagnosticSampler } = require(
  path.join(ROOT, 'scripts/e2e/telemetry/retained/observer')
)
const { NativeJournal, BoundaryWitness } = require(path.join(ROOT, 'testHelpers/processMemory'))
const identity = JSON.parse(
  require('node:fs').readFileSync(process.env.RETAINED_IDENTITY_FILE, 'utf8')
)
const directory = process.env.RETAINED_DIRECTORY
const log = new DiagnosticLog(path.join(directory, 'events.jsonl'))
const memory = new DiagnosticMemory(log, directory, identity.variant)
log.write('manifest', {
  manifest: {
    ...identity,
    node: process.version,
    v8: process.versions.v8,
    flags: process.execArgv,
    capacity: CAPACITY,
    pid: process.pid,
    gc: memory.gcCapability(),
  },
})
memory.mark('startup')
if (process.stdout.fd < 0 || process.stderr.fd < 0) throw new Error('stdio is not open')
const nativeResolve = Module._resolveFilename
Module._resolveFilename = function (request, ...rest) {
  return nativeResolve.call(
    this,
    request === 'testHelpers' ? path.join(ROOT, 'testHelpers/index.js') : request,
    ...rest
  )
}
const {
  MemoryForeground,
  DESCRIPTORS,
  descriptor,
  until,
  within,
  nativePair,
} = require('./memoryForeground')
const { maximum, bindNodes } = require('./maximum')
const { ResourceRoles } = require(path.join(ROOT, 'testHelpers/resourceProbe'))
const TelemetryPayload = require('../../payload')
const { WeakOwners } = require(path.join(ROOT, 'scripts/e2e/telemetry/retained/weakOwners'))
const weak = new WeakOwners(CAPACITY.weakTargets)
weak.observe(require('../../metrics').prototype, 'snapshot', 'snapshot')
weak.observe(require('../../client').prototype, 'close', 'close-promise', ['closing'])
weak.observe(require('../../client').prototype, 'run', 'operation-continuation', ['running'])
weak.observe(TelemetryPayload.prototype, 'buildPayload', 'payload-continuation')
const gzip = require('../../../protocol/message/compression/gzip')
const zstd = require('../../../protocol/message/compression/zstd')
const codecs = [
  { name: 'gzip', type: 1, module: gzip },
  { name: 'zstd', type: 4, module: zstd },
]
memory.mark('loaded')
const roles = new ResourceRoles({ roles: ['control', 'target', 'fixture'] }).start()
const sampler = new DiagnosticSampler(memory)
let epoch = 0
let submitted = () => {}
const journal = new NativeJournal({
  memory,
  sampler,
  capacity: CAPACITY.native,
  epoch: () => epoch,
  onSubmit: (row) => {
    memory.mark(`native:${row.codec}:submitted`)
    submitted(row)
  },
})
journal.idOf = (object) => {
  if (!object || !['object', 'function'].includes(typeof object)) return null
  return weak.issue(
    object,
    Buffer.isBuffer(object)
      ? 'native-view'
      : object instanceof ArrayBuffer
        ? 'native-backing-store'
        : 'native-job'
  )
}
const nativeEnd = journal.end.bind(journal)
journal.end = (row, value) => {
  nativeEnd(row, value)
  memory.mark(`native:${row?.codec}:settled`)
}
for (const codec of codecs) journal.observe(codec.name, codec.module)
const live = new Set()
const turn = () => new Promise((resolve) => setImmediate(resolve))
const settled = () => roles.run('fixture', () => memory.settled())

/** @param {string} name checkpoint name @returns {Promise<object>} scalar reading and ordered capture */
const point = async (name) => {
  const reading = await settled()
  memory.mark(name)
  await memory.checkpoint(name, reading)
  return reading
}

/** Runs exactly five disabled opens with three operations each. @returns {Promise<void>} released scalar proof */
const control = async () => {
  let client = roles.run('control', () =>
    new MemoryForeground(identity.kind, { enabled: false }).build()
  )
  live.add(client)
  let c0
  let c1
  for (let index = 0; index < 5; index++) {
    await roles.run('control', () => client.open())
    if (index === 0) c0 = await point('C0')
    if (index === 4) c1 = await point('C1')
    if (index < 4) {
      await client.close(0)
      await client.drain()
    }
  }
  const probe = await client.client.clientInstanceId({ timeout: 0 }).then(
    () => 'resolved',
    (error) => error.reason
  )
  await client.close(0)
  await client.drain()
  await turn()
  await turn()
  const facts = { ...client.facts(), nativeSubmissions: journal.rows.length, probe }
  client.release()
  live.delete(client)
  client = null
  await turn()
  const released = await point('disabled-released')
  log.write('control', {
    c0,
    c1,
    released,
    retainedDelta: c1.retained - c0.retained,
    rssDelta: c1.rss - c0.rss,
    facts,
    roles: roles.read(),
  })
}

/** @param {object} enabled persistent public fixture @param {object} codec codec definition @returns {Promise<object>} decoded scalar proof */
const decode = async (enabled, codec) => {
  const entry = enabled.pushes[codec.type]
  const first = entry.first
  const raw = codec.type === 4 ? await zstd.decompress(first) : zlib.gunzipSync(first)
  const format =
    codec.type === 4 ? first.readUInt32LE(0) === 0xfd2fb528 : first[0] === 0x1f && first[1] === 0x8b
  entry.first = null
  return {
    codec: codec.name,
    format,
    rawBytes: raw.length,
    transmittedBytes: first.length,
    accepted: entry.accepted,
  }
}

/** Executes the original two-codec retirement/reopen catalogue. @param {object} enabled same returned object @returns {Promise<object>} scalar facts, with all payload scopes released */
const publicCatalogue = async (enabled) => {
  const reopens = []
  let armed = null
  let reopen = null
  submitted = (row) => {
    if (armed !== row.codec) return
    armed = null
    epoch++
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
  const actives = []
  for (const codec of codecs) {
    enabled.codecs = [codec.type, 0]
    await enabled.open()
    await until(() => enabled.accepted(codec.type) >= 2, `${codec.name} pushes`)
    const before = enabled.accepted(codec.type)
    armed = codec.name
    await until(() => reopen !== null, `${codec.name} retired work`)
    await within(reopen, `${codec.name} reopen`)
    reopen = null
    await enabled.resume()
    await enabled.work()
    await until(() => journal.live.size === 0, 'native settlement')
    memory.mark(`native:${codec.name}:settled`)
    await until(() => enabled.accepted(codec.type) > before, 'post-reopen push')
    actives.push({ phase: codec.name, ...(await settled()) })
    memory.mark(`public:${codec.name}`)
    if (codec.type === 1) {
      epoch++
      await enabled.close(0)
      await enabled.drain()
    }
  }
  actives.push({ phase: 'held', ...(await settled()) })
  const uuid = await enabled.client.clientInstanceId({ timeout: 0 })
  epoch++
  const witness = new BoundaryWitness(() => memory.now())
  await enabled.close(3000).then(() => {
    witness.resolved()
    witness.call('intermediate')
  })
  await enabled.drain()
  await until(() => journal.live.size === 0, 'final native settlement')
  const decoded = []
  for (const codec of codecs) decoded.push(await decode(enabled, codec))
  submitted = () => {}
  await turn()
  await turn()
  return { actives, decoded, reopens, uuid, intermediate: witness.summary() }
}

/** Keeps one public object and registry alive at M and all six closed ends. @returns {Promise<void>} joined release */
const publicRun = async () => {
  let enabled = roles.run('target', () =>
    new MemoryForeground(identity.kind, { enabled: true }).build()
  )
  live.add(enabled)
  for (let index = 0; index < DESCRIPTORS; index++) enabled.register(descriptor(index))
  await enabled.open()
  const uuid = await enabled.client.clientInstanceId({ timeout: 30000 })
  await enabled.close(0)
  await enabled.drain()
  const matched = await point('M')
  const originalRegistry = enabled.fingerprint(uuid)
  for (let index = 0; index < 6; index++) {
    memory.reserve()
    const firstNative = journal.rows.length
    const firstOp = index === 0 ? 0 : enabled.ops.length
    if (index > 0) {
      enabled.codecs = [0]
      await enabled.open()
      await enabled.close(0)
      await enabled.drain()
    }
    const facts = await roles.run('target', () => publicCatalogue(enabled))
    const closed = await settled()
    memory.mark(`closed:${index}`)
    if (index === 0 || index === 5) await memory.checkpoint(`closed-${index}`, closed)
    const registry = enabled.fingerprint(uuid)
    const ops = enabled.ops.slice(firstOp)
    if (enabled.ops.length > CAPACITY.operations) throw new Error('diagnostic operation overflow')
    log.write('weak', { index, observations: weak.read() })
    log.write('block', {
      index,
      ...facts,
      closed,
      matchedRetained: matched.retained,
      delta: closed.retained - matched.retained,
      registry,
      originalRegistry,
      ops,
      resources: roles.read(),
      native: { ...journal.summary(), rows: journal.summary().rows.slice(firstNative) },
      samples: sampler.summary(),
    })
    enabled.pushes = {}
  }
  memory.reserve()
  await enabled.close(0).then(() => {
    process.stdout.write(
      `${JSON.stringify({ terminal: true, childId: identity.childId, at: memory.now() })}\n`
    )
  })
  await enabled.drain()
  enabled.release()
  live.delete(enabled)
  enabled = null
}

/** Releases payload and snapshot frames before each closed observation. @param {object} state persistent collector and builder @param {number} index block number @returns {Promise<object>} scalar builds/points */
const collectorCatalogue = async (state, index) => {
  if (index > 0) state.builder.resetTransport(BigInt(index + 1))
  const active = bindNodes(state.metrics, state.subscription)
  state.subscription = active
  let snapshot = state.metrics.snapshot(active)
  const builds = []
  const actives = []
  for (const codec of codecs) {
    builds.push(await nativePair(state.builder, snapshot, codec, () => epoch++))
    memory.mark(`native:${codec.name}:settled`)
    actives.push({ phase: codec.name, ...(await settled()) })
  }
  actives.push({ phase: 'held', ...(await settled()) })
  state.metrics.abandon(snapshot)
  snapshot = null
  state.builder.retire()
  state.metrics.resetTransient()
  const disconnected = state.metrics.snapshot(active)
  const disconnectedPoints = disconnected.points.length
  state.metrics.abandon(disconnected)
  return { builds, actives, disconnectedPoints }
}

/** Keeps the original collector/builder at every baseline and closed reading. @returns {Promise<void>} released owners */
const collectorRun = async () => {
  let state = maximum(identity.kind)
  state.slots.forEach((slot) => state.metrics.retireNode(slot))
  state.slots.length = 0
  state.metrics.resetTransient()
  state.builder = new TelemetryPayload(1n)
  const matched = await point('M')
  for (let index = 0; index < 6; index++) {
    memory.reserve()
    const firstNative = journal.rows.length
    const facts = await collectorCatalogue(state, index)
    const closed = await settled()
    memory.mark(`closed:${index}`)
    if (index === 0 || index === 5) await memory.checkpoint(`closed-${index}`, closed)
    log.write('weak', { index, observations: weak.read() })
    log.write('block', {
      index,
      ...facts,
      closed,
      matchedRetained: matched.retained,
      delta: closed.retained - matched.retained,
      native: { ...journal.summary(), rows: journal.summary().rows.slice(firstNative) },
      samples: sampler.summary(),
    })
  }
  state = null
  process.stdout.write(
    `${JSON.stringify({ terminal: true, childId: identity.childId, at: memory.now() })}\n`
  )
}

/** Runs bounded cleanup independently, retaining primary and cleanup failures. @returns {Promise<void>} evidence flushed before natural exit */
const main = async () => {
  const { joinedCleanup } = require(path.join(ROOT, 'scripts/e2e/telemetry/retained/cleanup'))
  const report = (stage, error) => {
    const facts = { stage, message: error.stack }
    try {
      log.write('error', facts)
    } catch {
      // A full or failed sidecar must not prevent the remaining cleanup actions from joining.
      process.stderr.write(`${JSON.stringify(facts)}\n`)
    }
  }
  const complete = await joinedCleanup(
    async () => {
      if (!process.version.startsWith('v24.') || !zstd.isSupported || !memory.gcCapability().native)
        throw new Error('diagnostic requires Node 24, native GC and gzip+zstd')
      await control()
      if (identity.arm === 'public') await roles.run('target', publicRun)
      else await roles.run('target', collectorRun)
    },
    [
      {
        name: 'clients',
        run: async () => {
          const actions = []
          for (const client of live)
            for (const action of ['close', 'drain', 'release'])
              actions.push({ name: action, run: () => client[action](0) })
          const cleaned = await joinedCleanup(async () => {}, actions, report)
          live.clear()
          if (!cleaned) throw new Error('client cleanup failed; see individual errors')
        },
      },
      {
        name: 'native',
        run: async () => {
          await until(() => journal.live.size === 0, 'cleanup native settlement')
          sampler.stop()
        },
      },
      {
        name: 'restore',
        run: async () => {
          journal.restoreAll()
          weak.restore()
        },
      },
      {
        name: 'released-capture',
        run: async () => {
          await turn()
          await turn()
          await point('all-released')
          log.write('weak', { phase: 'all-released', observations: weak.read() })
          memory.mark('cleanup')
          log.write('terminal', {
            cleanup: true,
            native: journal.summary(),
            resources: roles.read(),
          })
        },
      },
      {
        name: 'observers',
        run: async () => {
          roles.stop()
          memory.stop()
        },
      },
    ],
    report
  )
  if (complete) log.write('complete', { at: memory.now() })
  process.stdout.write(
    `${JSON.stringify({ result: true, childId: identity.childId, failed: !complete })}\n`
  )
  if (!complete) process.exitCode = 1
}
main().catch((error) => {
  process.stderr.write(`${error.stack}\n`)
  process.exitCode = 1
})
