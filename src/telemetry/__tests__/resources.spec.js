const { spawn } = require('node:child_process')
const path = require('node:path')
const TelemetryPayload = require('../payload')
const gzip = require('../../protocol/message/compression/gzip')
const zstd = require('../../protocol/message/compression/zstd')
const oauthBearerAuthenticator = require('../../broker/saslAuthenticator/oauthBearer')
const { maximum } = require('./fixtures/maximum')
const {
  PublicTelemetryRound,
  RELEASE_ACTIONS,
  TELEMETRY_KINDS,
  TELEMETRY_UUID,
} = require('../../../testHelpers/lifecycle')
const { KafkaJSTelemetryError } = require('../../errors')

const KINDS = TELEMETRY_KINDS

describe('bounded telemetry resource structures', () => {
  afterEach(() => jest.restoreAllMocks())

  test.each(KINDS)('caps series and replaces node identities for %s', (kind) => {
    const { metrics, subscription, slots } = maximum(kind)
    let snapshot = metrics.snapshot(subscription)
    expect(snapshot.points).toHaveLength(257)
    expect(new Set(snapshot.points.map((p) => p.identity.key)).size).toBe(257)
    expect(snapshot.points.filter((p) => p.name.startsWith('com.example.'))).toHaveLength(128)
    expect(() =>
      metrics.register({ name: 'com.example.overflow', type: 'gauge', value: 0 })
    ).toThrow(expect.objectContaining({ reason: 'limit' }))
    const retired = snapshot.points
      .filter((p) => p.attributes.some((a) => a.key === 'node_id'))
      .map((p) => p.identity)
    metrics.abandon(snapshot)
    slots.forEach((slot) => metrics.retireNode(slot))
    for (let i = 0; i < 100; i++) {
      const slot = metrics.bindNode({ nodeId: i, endpoint: `new-${i}:9092`, endpointRevision: {} })
      metrics.observeRequest(slot, i)
      slots.forEach((old) => metrics.observeRequest(old, 999))
      snapshot = metrics.snapshot(subscription)
      const nodes = snapshot.points.filter((p) => p.attributes.some((a) => a.key === 'node_id'))
      expect(nodes.length).toBeLessThanOrEqual(128)
      expect(
        nodes.some((p) =>
          retired.some((old) => old.key === p.identity.key && old.revision === p.identity.revision)
        )
      ).toBe(false)
      expect(nodes.some((p) => p.value === 999)).toBe(false)
      expect(snapshot.points.length).toBeLessThanOrEqual(257)
      metrics.abandon(snapshot)
      metrics.releaseNode(slot)
    }
    metrics.resetTransient()
    snapshot = metrics.snapshot(subscription)
    expect(snapshot.points.filter((p) => p.name.startsWith('com.example.'))).toHaveLength(128)
    expect(snapshot.points.filter((p) => p.attributes.some((a) => a.key === 'node_id'))).toEqual([])
    metrics.abandon(snapshot)
  })

  test.each(
    KINDS.flatMap((kind) => [
      [kind, 'resolve'],
      [kind, 'reject'],
    ])
  )(
    'holds one native job across one hundred retirements for %s with late %s',
    async (kind, outcome) => {
      const { metrics, subscription } = maximum(kind)
      const builder = new TelemetryPayload(1n)
      let release
      let fail
      const compress = jest
        .spyOn(gzip, 'compress')
        .mockImplementationOnce(
          () =>
            new Promise((resolve, reject) => {
              release = resolve
              fail = reject
            })
        )
        .mockResolvedValue(Buffer.from('current'))
      const options = {
        telemetryMaxBytes: 1048576,
        acceptedCompressionTypes: [1],
        isCurrent: () => true,
      }
      let snapshot = metrics.snapshot(subscription)
      const pending = builder.buildPayload(snapshot, options)
      const old = pending.then(
        (result) => result,
        (error) => {
          throw error
        }
      )
      old.catch(() => {})
      expect(compress).toHaveBeenCalledTimes(1)
      for (let i = 0; i < 100; i++) {
        builder.retire()
        metrics.resetTransient()
        builder.resetTransport(BigInt(i + 2))
        snapshot = metrics.snapshot(subscription)
        expect(await builder.buildPayload(snapshot, options)).toBeNull()
        expect(compress).toHaveBeenCalledTimes(1)
        metrics.abandon(snapshot)
      }
      if (outcome === 'resolve') release(Buffer.from('obsolete'))
      else fail(new Error('late codec failure'))
      expect(await old).toBeNull()
      snapshot = metrics.snapshot(subscription)
      const current = await builder.buildPayload(snapshot, options)
      expect(current.compressionType).toBe(1)
      expect(current.metrics).toEqual(Buffer.from('current'))
      expect(compress).toHaveBeenCalledTimes(2)
      expect(metrics.isCurrent(snapshot)).toBe(true)
      metrics.abandon(snapshot)
      builder.retire()
    }
  )
})

/**
 * Reads the resources one returned client owns right now, through the round's own wrappers.
 * Every timer id on the deterministic clock must belong to the machine, so an unknown timer fails
 * instead of being excluded.
 * @param {PublicTelemetryRound} round started round with a collector and machine
 * @returns {Readonly<object>} scalar counts and the issued ids of the owned timers
 */
const observe = (round) => {
  const { machine, clock } = round
  const named = {
    scheduler: machine.timer,
    watchdog: machine.watchdog,
    waiter: machine.waiterTimer,
    close: machine.closing ? round.closeTimer : null,
  }
  const owned = Object.entries(named).filter(([, id]) => id != null && clock.pending.has(id))
  const queue = round.dedicatedConnection?.requestQueue
  const sockets = round.dedicatedSockets.filter((socket) => !socket.destroyed)
  return Object.freeze({
    state: machine.state,
    pending: [...clock.pending.keys()].sort(),
    owned: owned.map(([role]) => role).sort(),
    ownedIds: owned.map(([, id]) => id).sort(),
    waiters: machine.waiters.size,
    queue: queue ? queue.pending.length + queue.inflight.size : 0,
    sockets: sockets.length,
    listeners: sockets.reduce(
      (total, socket) =>
        total + socket.eventNames().reduce((sum, event) => sum + socket.listenerCount(event), 0),
      0
    ),
    handles: round.builtHandles.length,
  })
}

/**
 * Asserts the bounds every observation owes, whatever the lifecycle state.
 * @param {object} seen one `observe` reading @returns {void}
 */
const expectBounded = (seen) => {
  expect(seen.pending).toEqual(seen.ownedIds)
  for (const role of ['scheduler', 'watchdog', 'waiter', 'close'])
    expect(seen.owned.filter((owned) => owned === role).length).toBeLessThanOrEqual(1)
  expect(seen.queue).toBeLessThanOrEqual(1)
  expect(seen.sockets).toBeLessThanOrEqual(1)
  expect(seen.waiters).toBeLessThanOrEqual(64)
}

describe('public telemetry resources', () => {
  test.each(KINDS)(
    'bounds timers queues listeners and sockets for %s',
    async (kind) => {
      const seen = {}
      const reading = (name, round) => {
        seen[name] = observe(round)
        expectBounded(seen[name])
      }
      const getting = new PublicTelemetryRound({ kind })
      try {
        const gate = getting.hold('S09', { mode: 'get' })
        await getting.wait(() => getting.start({ drive: 'get' }))
        await getting.wait(() => gate.arrived())
        reading('GETTING', getting)
        expect(seen.GETTING.state).toBe('GETTING')
        expect(seen.GETTING.owned).toContain('watchdog')
        await getting.drainReleased(gate)
        await getting.until(() => getting.machine.subscription !== null)
        await getting.ready()
        reading('READY', getting)
        expect(seen.READY.state).toBe('READY')
        expect(seen.READY.sockets).toBe(1)
        expect(seen.READY.queue).toBe(0)
        expect(seen.READY.owned).toEqual(['scheduler'])
        await getting.wait(() => getting.beginClose(getting.closeOptions(0)))
        await getting.settleWork()
        reading('CLOSED', getting)
        expect(seen.CLOSED).toMatchObject({
          state: 'CLOSED',
          owned: [],
          pending: [],
          waiters: 0,
          queue: 0,
          sockets: 0,
          listeners: 0,
        })
      } finally {
        await getting.dispose()
      }

      const pushing = new PublicTelemetryRound({ kind })
      try {
        const gate = pushing.hold('S13', { mode: 'normal' })
        await pushing.wait(() => pushing.start({ drive: 'normal' }))
        await pushing.wait(() => gate.arrived())
        reading('PUSHING', pushing)
        expect(seen.PUSHING.state).toBe('PUSHING')
        expect(seen.PUSHING.owned).toContain('watchdog')
        pushing.faultDedicated()
        const watchdog = pushing.machine.watchdog
        pushing.clock.jump(pushing.clock.time + 30000)
        pushing.clock.deliver(watchdog)
        await pushing.until(
          () => pushing.machine.state === 'BACKOFF' && pushing.machine.timer !== null
        )
        reading('BACKOFF', pushing)
        expect(seen.BACKOFF.owned).toEqual(['scheduler'])
        await pushing.drainReleased(gate)
      } finally {
        await pushing.dispose()
      }

      const closing = new PublicTelemetryRound({ kind })
      try {
        const gate = closing.hold('S13', { mode: 'final' })
        await closing.start()
        await closing.ready()
        closing.arm()
        closing.closePromise = closing.beginClose(closing.closeOptions(1000))
        await closing.wait(() => gate.arrived())
        reading('CLOSING', closing)
        expect(seen.CLOSING.state).toBe('CLOSING')
        expect(seen.CLOSING.owned).toContain('close')
        await closing.drainReleased(gate)
        await closing.settleClose()
        await closing.settleWork()
        reading('CLOSED after final', closing)
        expect(seen['CLOSED after final']).toMatchObject({ owned: [], sockets: 0, listeners: 0 })
        expect(closing.capture().uuid).toBe(TELEMETRY_UUID)
      } finally {
        await closing.dispose()
      }
    },
    90000
  )

  test.each(KINDS)(
    'bounds independent id waiters for %s',
    async (kind) => {
      const round = new PublicTelemetryRound({ kind })
      try {
        const gate = round.hold('S09', { mode: 'get' })
        await round.wait(() => round.start({ drive: 'get' }))
        await round.wait(() => gate.arrived())
        const settled = []
        const requestsBefore = round.telemetryRequests().length
        const call = (timeout, label) =>
          round.pending.push(
            round.client.clientInstanceId({ timeout }).then(
              (value) => settled.push([label, value]),
              (error) => settled.push([label, error.reason])
            )
          )
        call(1000, 'earliest')
        for (let i = 1; i < 64; i++) call(60000, `waiter-${i}`)
        expect(round.machine.waiters.size).toBe(64)
        expect(
          round.machine.waiterTimer !== null && round.clock.pending.has(round.machine.waiterTimer)
        ).toBe(true)
        const timersBefore = round.clock.pending.size

        await expect(round.client.clientInstanceId({ timeout: 60000 })).rejects.toMatchObject({
          reason: 'limit',
        })
        expect(round.machine.waiters.size).toBe(64)
        expect(round.clock.pending.size).toBe(timersBefore)
        expect(round.telemetryRequests().length).toBe(requestsBefore)

        const withheld = round.clock.callback(round.machine.waiterTimer)
        round.clock.jump(round.clock.time + 1001)
        await round.drainReleased(gate)
        await round.until(() => settled.length === 64)
        expect(settled.find(([label]) => label === 'earliest')).toEqual(['earliest', 'timeout'])
        expect(settled.filter(([, value]) => value === TELEMETRY_UUID)).toHaveLength(63)
        expect(round.machine.waiters.size).toBe(0)
        expect(round.machine.waiterTimer).toBeNull()
        const replacement = round.capture()
        withheld()
        round.assertReplacementUnchanged(replacement)
        await expect(round.client.clientInstanceId({ timeout: 0 })).resolves.toBe(TELEMETRY_UUID)
      } finally {
        await round.dispose()
      }

      const closed = new PublicTelemetryRound({ kind })
      try {
        const gate = closed.hold('S09', { mode: 'get' })
        await closed.wait(() => closed.start({ drive: 'get' }))
        await closed.wait(() => gate.arrived())
        const settled = []
        for (let i = 0; i < 64; i++)
          closed.pending.push(
            closed.client.clientInstanceId({ timeout: 60000 }).then(
              (value) => settled.push(value),
              (error) => settled.push(error.reason)
            )
          )
        expect(closed.machine.waiters.size).toBe(64)
        await closed.wait(() => closed.beginClose(closed.closeOptions(0)))
        await closed.until(() => settled.length === 64)
        expect(settled.every((reason) => reason === 'closed' || reason === 'not-connected')).toBe(
          true
        )
        expect(closed.machine.waiters.size).toBe(0)
        expect(closed.machine.waiterTimer).toBeNull()
        await closed.client.connect()
        const next = closed.client.clientInstanceId({ timeout: 60000 }).catch((error) => error)
        const before = closed.capture()
        await closed.drainReleased(gate)
        expect(closed.capture().waiters).toBe(before.waiters)
        expect(closed.machine.waiters.size).toBe(1)
        closed.tick()
        expect(await next).toBe(TELEMETRY_UUID)
      } finally {
        await closed.dispose()
      }
    },
    90000
  )

  test.each(KINDS)(
    'keeps disabled clients resource free for %s',
    async (kind) => {
      const round = new PublicTelemetryRound({ kind, enabled: false })
      try {
        await round.wait(() => round.start())
        expect(round.machine).toBeNull()
        expect(round.metrics).toBeNull()
        expect(() =>
          round.client.registerMetricForSubscription({
            name: 'com.example.off',
            type: 'gauge',
            value: 1,
          })
        ).toThrow(expect.objectContaining({ reason: 'disabled' }))
        expect(() =>
          round.client.registerMetricForSubscription({
            name: 'com.example.off',
            type: 'gauge',
            value: 1,
          })
        ).toThrow(KafkaJSTelemetryError)
        expect(
          round.client.unregisterMetricFromSubscription({ name: 'com.example.off' })
        ).toBeUndefined()
        await expect(round.client.clientInstanceId({ timeout: 0 })).rejects.toMatchObject({
          reason: 'disabled',
        })
        expect(round.clock.pending.size).toBe(0)
        expect(round.dedicatedCount()).toBe(0)
        expect(round.telemetryRequests()).toEqual([])
        expect(round.builtHandles).toEqual([])
        expect(round.setupCalls).toEqual([])
      } finally {
        await round.dispose()
      }
    },
    60000
  )

  test.each(KINDS)(
    'drops stale work when timer delivery is delayed for %s',
    async (kind) => {
      const round = new PublicTelemetryRound({ kind })
      try {
        await round.start()
        const old = round.machine.timer
        const stale = round.clock.callback(old)
        await round.wait(() => round.beginClose(round.closeOptions(0)))
        await round.settleWork()
        await round.client.connect()
        const before = round.capture()
        expect(before.timer).not.toBe(old)
        expect(round.clock.pending.get(before.timer).due).toBe(round.clock.time)
        stale()
        expect(round.capture().requests).toBe(before.requests)
        round.assertReplacementUnchanged(before)
        await round.establish()
        expect(round.machine.uuid).toBe(TELEMETRY_UUID)
      } finally {
        await round.dispose()
      }

      const deadline = new PublicTelemetryRound({ kind })
      try {
        const gate = deadline.hold('S09', { mode: 'get' })
        await deadline.wait(() => deadline.start({ drive: 'get' }))
        await deadline.wait(() => gate.arrived())
        const withheld = deadline.clock.callback(deadline.machine.watchdog)
        const writes = deadline.socketWrites.length
        deadline.clock.jump(deadline.clock.time + 30001)
        await deadline.drainReleased(gate)
        await deadline.until(
          () => deadline.machine.state === 'BACKOFF' || !deadline.machine.running
        )
        expect(deadline.machine.subscription).toBeNull()
        expect(deadline.machine.uuid).toBeNull()
        expect(deadline.socketWrites.length).toBe(writes)
        withheld()
        await deadline.transition(RELEASE_ACTIONS[5])
        expect(deadline.machine.uuid).toBe(TELEMETRY_UUID)
      } finally {
        await deadline.dispose()
      }
    },
    90000
  )
})

const CYCLES = 100

/** @returns {Promise<void>} resolves after one macrotask, so queued continuations can run */
const turn = () => new Promise((resolve) => setImmediate(resolve))

/**
 * @param {PublicTelemetryRound} round started round @param {string} api wire API name
 * @returns {number} requests of that API the strict wire has answered
 */
const answered = (round, api) =>
  round.wire.requests().filter((r) => r.api === api && r.answeredOrder !== undefined).length

/**
 * Takes one scalar reading of a 100-cycle public schedule. Only numbers, booleans and strings are
 * kept, so the journal never retains a payload, snapshot, socket or transport.
 * @param {PublicTelemetryRound} round started round @param {number} cycle attempt number
 * @param {object} owners the identities that must not change across attempts
 * @returns {Promise<object>} the cycle record
 */
const record = async (round, cycle, owners) => {
  const seen = observe(round)
  return {
    cycle,
    lifecycle: String(round.machine.lifecycle),
    sameBuilder: round.machine.payload === owners.builder,
    nativeJobs: round.codecJobs.length,
    supplierCalls: owners.supplierCalls ? owners.supplierCalls() : 0,
    snapshots: round.snapshots.length,
    handles: seen.handles,
    sockets: seen.sockets,
    timers: seen.pending.length,
    waiters: seen.waiters,
    queue: seen.queue,
    uuid: await round.client.clientInstanceId({ timeout: 0 }),
  }
}

describe('public telemetry resources', () => {
  test.each(KINDS.flatMap((kind) => ['resolve', 'reject'].map((outcome) => [kind, outcome])))(
    'holds the native result through public cycles for %s with late %s',
    async (kind, outcome) => {
      const round = new PublicTelemetryRound({ kind })
      let failNext = false
      round.patch(
        gzip,
        'compress',
        (original) =>
          (...args) =>
            failNext ? Promise.reject(new Error('late codec failure')) : original(...args)
      )
      const gate = round.hold('S12', { mode: 'normal' })
      try {
        await round.wait(() =>
          round.start({ points: 'large', subscription: { acceptedCompressionTypes: [1, 0] } })
        )
        await round.ready()
        const owners = { builder: round.machine.payload }
        failNext = outcome === 'reject'
        round.arm()
        round.tick()
        await round.wait(() => gate.arrived())
        failNext = false
        expect(round.codecJobs).toHaveLength(1)
        const journal = []
        for (let cycle = 0; cycle < CYCLES; cycle++) {
          await round.wait(() => round.beginClose(round.closeOptions(0)))
          await round.wait(() => round.client.connect())
          const gets = answered(round, 'GetTelemetrySubscriptions')
          round.tick()
          await round.until(
            () => answered(round, 'GetTelemetrySubscriptions') > gets && !round.machine.running
          )
          journal.push(await record(round, cycle, owners))
        }
        expect(journal).toHaveLength(CYCLES)
        expect(new Set(journal.map((row) => row.lifecycle)).size).toBe(CYCLES)
        for (const row of journal) {
          expect(row.sameBuilder).toBe(true)
          expect(row.nativeJobs).toBe(1)
          expect(row.snapshots).toBe(journal[0].snapshots)
          expect(row.sockets).toBeLessThanOrEqual(1)
          expect(row.queue).toBeLessThanOrEqual(1)
          expect(row.waiters).toBe(0)
          expect(row.timers).toBeLessThanOrEqual(3)
          expect(row.uuid).toBe(TELEMETRY_UUID)
        }
        expect(
          Math.max(...journal.map((row) => row.handles)) - journal[0].handles
        ).toBeLessThanOrEqual(CYCLES)
        const blocked = round.pushes().length
        round.tick()
        await round.until(() => !round.machine.running)
        expect(round.codecJobs).toHaveLength(1)
        expect(round.pushes().length).toBe(blocked)

        const replacement = round.capture()
        const writes = round.socketWrites.length
        round.armed = false
        await round.drainReleased(gate)
        round.assertReplacementUnchanged(replacement)
        expect(round.socketWrites.length).toBe(writes)
        expect(round.machine.uuid).toBe(TELEMETRY_UUID)
        await round.ready()
        expect(round.pushes().length).toBeGreaterThan(blocked)
        expect(round.codecJobs).toHaveLength(1)
      } finally {
        await round.dispose()
      }
    },
    180000
  )

  const mechanisms = ['built-in OAuth', 'custom provider']
  test.each(
    KINDS.flatMap((kind) =>
      mechanisms.flatMap((mechanism) =>
        ['resolve', 'reject'].map((outcome) => [kind, mechanism, outcome])
      )
    )
  )(
    'holds the auth supplier through public cycles for %s using %s with late %s',
    async (kind, mechanism, outcome) => {
      const round = new PublicTelemetryRound({ kind })
      let calls = 0
      let late = false
      const supplier = async () => {
        calls += 1
        const held = late
        if (held) await round.enter('S19b')
        if (held && outcome === 'reject') throw new Error('late supplier failure')
        return { value: 'telemetry-token' }
      }
      const sasl =
        mechanism === 'built-in OAuth'
          ? { mechanism: 'oauthbearer', oauthBearerProvider: supplier }
          : {
              mechanism: 'oauthbearer',
              authenticationProvider: (args) =>
                oauthBearerAuthenticator({ oauthBearerProvider: supplier })(args),
            }
      const gate = round.hold('S19b', { mode: 'normal' })
      try {
        await round.wait(() => round.start({ sasl: true, kafka: { sasl } }))
        await round.ready()
        const owners = { builder: round.machine.payload, supplierCalls: () => calls }
        const baseline = calls
        round.armRenewal()
        round.arm()
        late = true
        round.tick()
        await round.wait(() => gate.arrived())
        late = false
        expect(calls).toBe(baseline + 1)
        const apiCounts = () =>
          Object.fromEntries(
            ['SaslAuthenticate', 'GetTelemetrySubscriptions', 'PushTelemetry'].map((api) => [
              api,
              round.wire.requests().filter((r) => r.api === api).length,
            ])
          )
        const journal = []
        for (let cycle = 0; cycle < CYCLES; cycle++) {
          await round.wait(() => round.beginClose(round.closeOptions(0)))
          await round.wait(() => round.client.connect())
          if (round.machine.timer !== null) round.tick()
          for (let i = 0; i < 3; i++) await turn()
          journal.push(await record(round, cycle, owners))
        }
        for (const row of journal) {
          expect(row.supplierCalls).toBe(baseline + 1)
          expect(row.sockets).toBeLessThanOrEqual(1)
          expect(row.queue).toBeLessThanOrEqual(1)
          expect(row.uuid).toBe(TELEMETRY_UUID)
        }
        const replacement = round.capture()
        const before = apiCounts()
        const writes = round.socketWrites.length
        await round.wait(() => round.beginClose(round.closeOptions(0)))
        await round.wait(() => round.client.connect())
        const idle = round.capture()
        round.armed = false
        await round.drainReleased(gate)
        expect(apiCounts()).toEqual(before)
        expect(round.socketWrites.length).toBe(writes)
        round.assertReplacementUnchanged(idle)
        expect(replacement.uuid).toBe(TELEMETRY_UUID)
        await round.establish()
        expect(apiCounts().PushTelemetry).toBeGreaterThan(before.PushTelemetry)
        expect(round.machine.uuid).toBe(TELEMETRY_UUID)
      } finally {
        await round.dispose()
      }
    },
    180000
  )
})

/**
 * Runs one single-client memory child under the parent's supervision, with bounded output.
 * @param {string} kind public client kind
 * @returns {Promise<{readings:object, code:number|null, signal:string|null, stderr:string,
 *   closedAfterReceiptMs:number}>} the one reported line and the natural close facts
 */
const supervise = (kind) =>
  new Promise((resolve, reject) => {
    const script = path.join(__dirname, 'fixtures', 'resourceChild.js')
    const child = spawn(process.execPath, ['--expose-gc', script, kind], {
      stdio: ['ignore', 'pipe', 'pipe'],
    })
    let out = ''
    let stderr = ''
    let receipt = null
    const limit = setTimeout(() => {
      child.kill('SIGKILL')
      reject(new Error('Memory arm exceeded its bound'))
    }, 90000)
    child.stdout.on('data', (chunk) => {
      if (out.length < 65536) out += chunk
      if (receipt === null && out.includes('\n')) receipt = Date.now()
    })
    child.stderr.on('data', (chunk) => {
      if (stderr.length < 65536) stderr += chunk
    })
    child.on('error', reject)
    child.on('close', (code, signal) => {
      clearTimeout(limit)
      let readings = null
      try {
        readings = JSON.parse(out.trim())
      } catch (error) {
        return reject(error)
      }
      return resolve({
        readings,
        code,
        signal,
        stderr,
        closedAfterReceiptMs: receipt === null ? Infinity : Date.now() - receipt,
      })
    })
  })

test.each(KINDS)(
  'single client resource memory for %s',
  async (kind) => {
    const { readings, code, signal, stderr, closedAfterReceiptMs } = await supervise(kind)
    expect({ code, signal, stderr }).toEqual({ code: 0, signal: null, stderr: '' })
    expect(closedAfterReceiptMs).toBeLessThanOrEqual(10000)
    const built = readings.builds.filter((row) => row.rawBytes !== undefined)
    expect(built.map((row) => row.codec)).toEqual(zstd.isSupported ? ['gzip', 'zstd'] : ['gzip'])
    for (const row of built) {
      expect(row.points).toBe(257)
      expect(row.rawBytes).toBeGreaterThan(1000)
      expect(row.rawBytes).toBeLessThanOrEqual(1048576)
      expect(row.compressionType).toBe(row.codec === 'zstd' ? 4 : 1)
      expect(row.transmittedBytes).toBeLessThan(row.rawBytes)
    }
    expect(readings.builds.filter((row) => row.retiredCompletion !== undefined)).toEqual(
      built.map((row) => ({ codec: row.codec, retiredCompletion: true }))
    )
    expect(readings.rssSamples).toBeGreaterThanOrEqual(2 * built.length)
    expect(readings.held.retained - readings.matched.retained).toBeLessThanOrEqual(8388608)
    expect(readings.closed.retained - readings.matched.retained).toBeLessThanOrEqual(262144)
    expect(readings.peakRss).toBeLessThanOrEqual(67108864)
    expect(readings.rssPeakSample).toBeLessThanOrEqual(67108864)
  },
  120000
)

describe('public telemetry resources', () => {
  const body = (count, last) => [
    'com.example.',
    ...Array.from({ length: count - 2 }, (_, i) => `p${i}`.padEnd(64, 'x')),
    'z'.repeat(last),
  ]
  const accepted = {
    '256 prefixes totalling 16384 bytes': { requestedMetrics: body(256, 116) },
    'a 256 byte string': { requestedMetrics: ['com.example.', 'x'.repeat(256)] },
    'a 256 byte UTF-8 string of 128 characters': {
      requestedMetrics: ['com.example.', 'é'.repeat(128)],
    },
    '128 codec entries': {
      acceptedCompressionTypes: Array.from({ length: 128 }, (_, i) => (i % 2 ? 1 : 0)),
    },
  }
  const refused = {
    '257 prefixes': {
      requestedMetrics: ['com.example.', ...Array.from({ length: 256 }, (_, i) => `q${i}`)],
    },
    '16385 prefix bytes': { requestedMetrics: body(256, 117) },
    'a 257 byte string': { requestedMetrics: ['com.example.', 'x'.repeat(257)] },
    'a 258 byte UTF-8 string of 129 characters': {
      requestedMetrics: ['com.example.', 'é'.repeat(129)],
    },
    '129 codec entries': { acceptedCompressionTypes: Array.from({ length: 129 }, () => 0) },
  }

  /**
   * Runs one real producer round to completion and always disposes it.
   * @param {Function} work receives the started-less round @returns {Promise<void>}
   */
  const inRound = async (work) => {
    const round = new PublicTelemetryRound({ kind: 'producer' })
    try {
      await work(round)
    } finally {
      await round.dispose()
    }
  }

  test('bounds accepted selection metadata', async () => {
    for (const [name, fields] of Object.entries(accepted)) {
      await inRound(async (round) => {
        await round.wait(() => round.start({ subscription: fields }))
        await round.ready()
        for (const [key, value] of Object.entries(fields))
          expect({ name, [key]: round.machine.subscription[key] }).toEqual({ name, [key]: value })
        expect({ name, state: round.machine.state }).toEqual({ name, state: 'READY' })
        expect(round.machine.uuid).toBe(TELEMETRY_UUID)
      })
    }
    for (const [name, fields] of Object.entries(refused)) {
      await inRound(async (round) => {
        await round.wait(() => round.start({ subscription: fields }))
        round.tick()
        await round.until(() => round.machine.state === 'STOPPED')
        expect({ name, subscription: round.machine.subscription }).toEqual({
          name,
          subscription: null,
        })
        expect(round.machine.uuid).toBeNull()
        expect(round.pushes()).toEqual([])
        await expect(round.client.clientInstanceId({ timeout: 0 })).rejects.toMatchObject({
          reason: 'stopped',
        })
      })
      await inRound(async (round) => {
        await round.wait(() => round.start())
        await round.ready()
        const previous = round.machine.subscription
        await round.wait(() => round.beginClose(round.closeOptions(0)))
        await round.wait(() => round.client.connect())
        round.subscription = { ...round.subscription, ...fields }
        round.tick()
        await round.until(() => round.machine.state === 'STOPPED')
        expect({ name, kept: round.machine.subscription === previous }).toEqual({
          name,
          kept: true,
        })
        expect(round.machine.uuid).toBe(TELEMETRY_UUID)
        expect(round.pushes()).toHaveLength(1)
      })
    }
  }, 300000)
})
