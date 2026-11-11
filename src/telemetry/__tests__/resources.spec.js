const { spawn } = require('node:child_process')
const fs = require('node:fs')
const crypto = require('node:crypto')
const path = require('node:path')
const TelemetryPayload = require('../payload')
const gzip = require('../../protocol/message/compression/gzip')
const zstd = require('../../protocol/message/compression/zstd')
const Connection = require('../../network/connection')
const Decoder = require('../../protocol/decoder')
const { maximum, bindNodes } = require('./fixtures/maximum')
const {
  AckProof,
  BoundedAudit,
  CycleLedger,
  IMMEDIATE,
  IndexOneWatch,
  KindForeground,
  NativeResultFamily,
  SupplierFamily,
} = require('./fixtures/cycleKit')
const { MEMORY_FLAGS, hostContext } = require('../../../testHelpers/processMemory')
const { A: SHARE_TOPIC_ID, B: OTHER_TOPIC_ID } = require('../../../testHelpers/shareHarness')
const {
  MemoryOutput,
  SCHEMA,
  judgeMemoryArm,
} = require('../../../scripts/e2e/telemetry/resourceArtifact')
const {
  BOUNDED_LIMITS,
  PublicTelemetryRound,
  RELEASE_ACTIONS,
  TELEMETRY_KINDS,
  TELEMETRY_UUID,
  telemetryReleaseCells,
} = require('../../../testHelpers/lifecycle')
const { KafkaJSTelemetryError } = require('../../errors')

const KINDS = TELEMETRY_KINDS

describe('bounded telemetry resource structures', () => {
  afterEach(() => jest.restoreAllMocks())

  test.each(KINDS)(
    'restores 257 observed points after the disconnected baseline for %s',
    (kind) => {
      const { metrics, subscription, slots } = maximum(kind)
      slots.forEach((slot) => metrics.retireNode(slot))
      metrics.resetTransient()
      const matched = metrics.snapshot(subscription)
      expect(matched.points).toHaveLength(129)
      metrics.abandon(matched)

      const active = bindNodes(metrics, subscription)
      expect(active.revision).not.toBe(subscription.revision)
      const snapshot = metrics.snapshot(active)
      expect(snapshot.points).toHaveLength(257)
      expect(
        snapshot.points.filter((point) => point.attributes.some((a) => a.key === 'node_id'))
      ).toHaveLength(128)
      metrics.abandon(snapshot)
      metrics.resetTransient()
    }
  )

  test.each(KINDS)(
    'builds current and retired maximum payloads after reset for %s',
    async (kind) => {
      const { metrics, subscription, slots } = maximum(kind)
      slots.forEach((slot) => metrics.retireNode(slot))
      slots.length = 0
      metrics.resetTransient()
      const active = bindNodes(metrics, subscription)
      const snapshot = metrics.snapshot(active)
      const builder = new TelemetryPayload(1n)
      for (const type of [1, ...(zstd.isSupported ? [4] : [])]) {
        const options = { acceptedCompressionTypes: [type, 0], isCurrent: () => true }
        const current = await builder.buildPayload(snapshot, options)
        expect(current.includedIdentities).toHaveLength(257)
        expect(current.compressionType).toBe(type)
        expect(current.rawBytes).toBeLessThanOrEqual(1048576)
        expect(current.transmittedBytes).toBeLessThan(current.rawBytes)
        let valid = true
        const retiring = builder.buildPayload(snapshot, { ...options, isCurrent: () => valid })
        valid = false
        expect(await retiring).toBeNull()
      }
      metrics.abandon(snapshot)
      builder.retire()
      metrics.resetTransient()
    }
  )

  test.each(KINDS)(
    'requires fresh authority for repeated diagnostic native work for %s',
    async (kind) => {
      const { nativePair } = require('./fixtures/memoryForeground')
      const { metrics, subscription, slots } = maximum(kind)
      const builder = new TelemetryPayload(1n)
      const codec = { name: 'gzip', type: 1 }
      const snapshot = metrics.snapshot(subscription)
      await nativePair(builder, snapshot, codec, () => {})
      builder.retire()
      expect(
        await builder.buildPayload(snapshot, {
          acceptedCompressionTypes: [1],
          isCurrent: () => true,
        })
      ).toBeNull()
      builder.resetTransport(2n)
      const proof = await nativePair(builder, snapshot, codec, () => {})
      expect(proof).toMatchObject({
        codec: 'gzip',
        compressionType: 1,
        points: 257,
        retiredCompletion: true,
      })
      expect(proof.transmittedBytes).toBeGreaterThan(0)
      metrics.abandon(snapshot)
      builder.retire()
      slots.forEach((slot) => metrics.retireNode(slot))
      metrics.resetTransient()
    }
  )

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
  const sockets = round.openDedicated()
  return Object.freeze({
    state: machine.state,
    pending: [...clock.pending.keys()].sort(),
    owned: owned.map(([role]) => role).sort(),
    ownedIds: owned.map(([, id]) => id).sort(),
    inherited: [...clock.pending.keys()].filter((id) => !owned.some(([, own]) => own === id))
      .length,
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
  expect(seen.pending).toEqual(expect.arrayContaining(seen.ownedIds))
  expect(seen.inherited).toBeLessThanOrEqual(2)
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
      const owners = {}
      const watches = []
      const watchOf = (round) => {
        const watch = new IndexOneWatch(round)
        watches.push(watch)
        return () => watch.install()
      }
      const reading = async (name, round) => {
        seen[name] = observe(round)
        expectBounded(seen[name])
        owners[name] = await new CycleLedger(round, () => ({})).churnProbe.snapshot()
      }
      const getting = new PublicTelemetryRound({ kind })
      try {
        const gate = getting.hold('S09', { mode: 'get' })
        await getting.wait(() => getting.start({ drive: 'get', beforeConnect: watchOf(getting) }))
        await getting.wait(() => gate.arrived())
        await reading('GETTING', getting)
        expect(seen.GETTING.state).toBe('GETTING')
        expect(seen.GETTING.owned).toContain('watchdog')
        expect(owners.GETTING.identities.operations).toHaveLength(1)
        await getting.drainReleased(gate)
        await getting.until(() => getting.machine.subscription !== null)
        await getting.ready()
        await reading('READY', getting)
        expect(seen.READY.state).toBe('READY')
        expect(seen.READY.sockets).toBe(1)
        expect(seen.READY.queue).toBe(0)
        expect(seen.READY.owned).toEqual(['scheduler'])
        expect(owners.READY.identities.sockets).toHaveLength(1)
        expect(owners.READY.identities.operations).toEqual([])
        expect(owners.READY.registrations['dedicated-socket'].id).toBe(
          owners.READY.identities.sockets[0]
        )
        await getting.wait(() => getting.beginClose(getting.closeOptions(0)))
        await getting.settleWork()
        await reading('CLOSED', getting)
        expect(seen.CLOSED).toMatchObject({
          state: 'CLOSED',
          owned: [],
          pending: [],
          waiters: 0,
          queue: 0,
          sockets: 0,
          listeners: 0,
        })
        expect(Object.values(owners.CLOSED.identities).flat()).toEqual([])
        expect(owners.CLOSED.registrations).toEqual({})
      } finally {
        await getting.dispose()
      }

      const pushing = new PublicTelemetryRound({ kind })
      try {
        const gate = pushing.hold('S13', { mode: 'normal' })
        await pushing.wait(() =>
          pushing.start({ drive: 'normal', beforeConnect: watchOf(pushing) })
        )
        await pushing.wait(() => gate.arrived())
        await reading('PUSHING', pushing)
        expect(seen.PUSHING.state).toBe('PUSHING')
        expect(seen.PUSHING.owned).toContain('watchdog')
        expect(owners.PUSHING.identities.operations).toHaveLength(1)
        pushing.faultDedicated()
        const watchdog = pushing.machine.watchdog
        pushing.clock.jump(pushing.clock.time + 30000)
        pushing.clock.deliver(watchdog)
        await pushing.until(
          () => pushing.machine.state === 'BACKOFF' && pushing.machine.timer !== null
        )
        await reading('BACKOFF', pushing)
        expect(seen.BACKOFF.owned).toEqual(['scheduler'])
        expect(owners.BACKOFF.identities.sockets).toEqual([])
        await pushing.drainReleased(gate)
      } finally {
        await pushing.dispose()
      }

      const closing = new PublicTelemetryRound({ kind })
      try {
        const gate = closing.hold('S13', { mode: 'final' })
        await closing.start({ beforeConnect: watchOf(closing) })
        await closing.ready()
        closing.arm()
        closing.closePromise = closing.beginClose(closing.closeOptions(1000))
        await closing.wait(() => gate.arrived())
        await reading('CLOSING', closing)
        expect(seen.CLOSING.state).toBe('CLOSING')
        expect(seen.CLOSING.owned).toContain('close')
        await closing.drainReleased(gate)
        await closing.settleClose()
        await closing.settleWork()
        await reading('CLOSED after final', closing)
        expect(seen['CLOSED after final']).toMatchObject({ owned: [], sockets: 0, listeners: 0 })
        expect(Object.values(owners['CLOSED after final'].identities).flat()).toEqual([])
        expect(closing.capture().uuid).toBe(TELEMETRY_UUID)
      } finally {
        await closing.dispose()
      }

      expect(watches.map((watch) => watch.pools >= 1)).toEqual([true, true, true])
      expect(watches.reduce((total, watch) => total + watch.opens, 0)).toBe(0)
    },
    90000
  )

  test('counts an attempted open of the second connection of a dedicated pool', async () => {
    const round = new PublicTelemetryRound({ kind: 'producer' })
    const watch = new IndexOneWatch(round)
    try {
      const gate = round.hold('S09', { mode: 'get' })
      await round.wait(() => round.start({ drive: 'get', beforeConnect: () => watch.install() }))
      await round.wait(() => gate.arrived())
      expect(watch.pools).toBeGreaterThanOrEqual(1)
      expect(watch.opens).toBe(0)
      const connected = await round.wait(() => watch.last.getConnection(1))
      expect(connected).toBe(watch.last.pool[1])
      expect(connected.isConnected()).toBe(true)
      expect(watch.opens).toBe(1)
    } finally {
      await round.dispose()
    }
  }, 60000)

  test.each(KINDS)(
    'bounds independent id waiters for %s',
    async (kind) => {
      const round = new PublicTelemetryRound({ kind })
      try {
        const gate = round.hold('S09', { mode: 'get' })
        await round.wait(() => round.start({ drive: 'get' }))
        await round.wait(() => gate.arrived())
        const { machine, clock } = round
        const settled = []
        const call = (timeout, label) =>
          round.pending.push(
            round.client.clientInstanceId({ timeout }).then(
              (value) => settled.push([label, value]),
              (error) => settled.push([label, error.reason])
            )
          )
        call(1000, 'short')
        for (let i = 1; i < 64; i++) call(60000, `long-${i}`)
        expect(machine.waiters.size).toBe(64)
        const firstTimer = machine.waiterTimer
        expect(firstTimer !== null && clock.pending.has(firstTimer)).toBe(true)
        const timersBefore = clock.pending.size
        const requestsBefore = round.telemetryRequests().length

        await expect(round.client.clientInstanceId({ timeout: 60000 })).rejects.toMatchObject({
          reason: 'limit',
        })
        expect(machine.waiters.size).toBe(64)
        expect(machine.waiterTimer).toBe(firstTimer)
        expect(clock.pending.size).toBe(timersBefore)
        expect(round.telemetryRequests().length).toBe(requestsBefore)

        // Only the short deadline passes. No timer is delivered, the Get stays held and nothing
        // settles the waiters before the next caller arrives.
        const obsolete = clock.callback(firstTimer)
        clock.jump(clock.time + 1001)
        const dials = round.dedicatedCount()
        call(60000, 'replacement')
        await round.until(() => settled.length === 1)
        expect(settled).toEqual([['short', 'timeout']])
        expect(machine.waiters.size).toBe(64)
        const deadlines = [...machine.waiters].map((waiter) => waiter.deadline)
        expect(deadlines.filter((deadline) => deadline === clock.now() + 60000)).toHaveLength(1)
        expect(machine.waiterTimer).not.toBe(firstTimer)
        expect(clock.pending.has(firstTimer)).toBe(false)
        expect(clock.pending.get(machine.waiterTimer).due).toBe(Math.min(...deadlines))
        expect(clock.pending.size).toBe(timersBefore)
        expect(round.telemetryRequests().length).toBe(requestsBefore)
        expect(round.dedicatedCount()).toBe(dials)

        await expect(round.client.clientInstanceId({ timeout: 60000 })).rejects.toMatchObject({
          reason: 'limit',
        })
        expect(clock.pending.size).toBe(timersBefore)

        obsolete()
        expect(settled).toHaveLength(1)
        expect(machine.waiters.size).toBe(64)
        expect(clock.pending.size).toBe(timersBefore)
        expect(clock.pending.get(machine.waiterTimer).due).toBe(Math.min(...deadlines))

        await round.wait(() => round.beginClose(round.closeOptions(0)))
        await round.until(() => settled.length === 65)
        expect(
          settled.slice(1).every(([, reason]) => reason === 'closed' || reason === 'not-connected')
        ).toBe(true)
        expect(machine.waiters.size).toBe(0)
        expect(machine.waiterTimer).toBeNull()

        await round.client.connect()
        const next = round.client.clientInstanceId({ timeout: 60000 }).catch((error) => error)
        round.pending.push(next)
        const before = round.capture()
        await round.drainReleased(gate)
        expect(round.capture().waiters).toBe(before.waiters)
        expect(machine.waiters.size).toBe(1)
        round.tick()
        expect(await next).toBe(TELEMETRY_UUID)
      } finally {
        await round.dispose()
      }
    },
    90000
  )

  test.each(KINDS)(
    'keeps disabled clients resource free for %s',
    async (kind) => {
      const arms = [
        ['root false', { enabled: false }, {}],
        [
          'client false over root true',
          {},
          { kafka: { enableMetricsPush: true }, client: { enableMetricsPush: false } },
        ],
      ]
      for (const [arm, variant, options] of arms) {
        const round = new PublicTelemetryRound({ kind, ...variant })
        const foreground = new KindForeground(round).prepare()
        try {
          await round.wait(() => round.start(options))
          expect({ arm, machine: round.machine }).toEqual({ arm, machine: null })
          expect({ arm, metrics: round.metrics }).toEqual({ arm, metrics: null })
          const register = () =>
            round.client.registerMetricForSubscription({
              name: 'com.example.off',
              type: 'gauge',
              value: 1,
            })
          expect(register).toThrow(expect.objectContaining({ reason: 'disabled' }))
          expect(register).toThrow(KafkaJSTelemetryError)
          expect(
            round.client.unregisterMetricFromSubscription({ name: 'com.example.off' })
          ).toBeUndefined()
          await expect(round.client.clientInstanceId({ timeout: 0 })).rejects.toMatchObject({
            reason: 'disabled',
          })
          const identity = await foreground.act(0)
          expect(typeof identity).toBe('object')
          expect(round.clock.pending.size).toBe(0)
          expect(round.dedicatedCount()).toBe(0)
          expect(round.telemetryRequests()).toEqual([])
          expect(round.builtHandles).toEqual([])
          expect(round.setupCalls).toEqual([])
        } finally {
          await round.dispose()
        }
      }
    },
    90000
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

  test.each(KINDS)(
    'refuses late final work once a delayed close deadline has passed for %s',
    async (kind) => {
      const responsive = new PublicTelemetryRound({ kind })
      try {
        const gate = responsive.hold('S13', { mode: 'final' })
        await responsive.start()
        await responsive.ready()
        responsive.arm()
        responsive.beginClose(responsive.closeOptions(30000))
        await responsive.wait(() => gate.arrived())
        const writes = responsive.writeCount
        await responsive.drainReleased(gate)
        await responsive.settleClose()
        expect(responsive.finalFrames()).toHaveLength(1)
        expect(responsive.pushTotals.finalAnswered).toBe(1)
        expect(responsive.writeCount).toBeGreaterThan(writes)
        expect(responsive.machine.hadFinal).toBe(true)
      } finally {
        await responsive.dispose()
      }

      const late = new PublicTelemetryRound({ kind })
      try {
        const gate = late.hold('S13', { mode: 'final' })
        await late.start()
        await late.ready()
        late.arm()
        late.beginClose(late.closeOptions(500))
        await late.wait(() => gate.arrived())
        const { machine, clock } = late
        const { deadline } = machine.closing
        expect(deadline - late.closeEntry).toBeGreaterThan(0)
        expect(deadline - late.closeEntry).toBeLessThanOrEqual(500)
        const closeTimer = late.closeTimer
        expect(clock.pending.get(closeTimer).due).toBe(deadline)
        const withheld = clock.callback(closeTimer)
        const writes = late.writeCount
        clock.jump(deadline + 1)
        await late.drainReleased(gate)
        await late.settleClose()
        expect(late.writeCount).toBe(writes)
        expect(late.finalFrames()).toEqual([])
        expect(late.pushTotals).toMatchObject({ finalRequested: 0, finalAnswered: 0 })
        expect(machine.hadFinal).toBe(false)
        expect(machine.state).toBe('CLOSED')
        expect(clock.pending.has(closeTimer)).toBe(false)

        await late.client.connect()
        const before = late.capture()
        withheld()
        late.assertReplacementUnchanged(before)
        await late.establish()
        expect(machine.uuid).toBe(TELEMETRY_UUID)
        expect(late.pushTotals.finalRequested).toBe(0)
      } finally {
        await late.dispose()
      }
    },
    90000
  )
})

const CYCLES = 100
const CONTROL_CYCLES = 3

/** @returns {Promise<void>} resolves after one macrotask, so queued continuations can run */
const turn = () => new Promise((resolve) => setImmediate(resolve))

const LATE = ['resolve', 'reject']
const KIND_OUTCOMES = KINDS.flatMap((kind) => LATE.map((outcome) => [kind, outcome]))

describe('public telemetry resources', () => {
  test.each(KIND_OUTCOMES)(
    'holds the native result through public cycles for %s with late %s',
    async (kind, outcome) => {
      const family = new NativeResultFamily({ kind, outcome })
      try {
        await family.open()
        for (let cycle = 0; cycle < CYCLES; cycle++) {
          const row = await family.cycle(cycle)
          expect({ cycle, problems: family.audit(row) }).toEqual({ cycle, problems: [] })
        }
        expect(BoundedAudit.journal(family.ledger.rows, CYCLES)).toEqual([])
        await family.finish()
      } finally {
        await family.close()
      }
    },
    240000
  )

  const mechanisms = ['built-in OAuth', 'custom provider']
  test.each(
    KINDS.flatMap((kind) =>
      mechanisms.flatMap((mechanism) => LATE.map((outcome) => [kind, mechanism, outcome]))
    )
  )(
    'holds the auth supplier through public cycles for %s using %s with late %s',
    async (kind, mechanism, outcome) => {
      const family = new SupplierFamily({ kind, mechanism, outcome })
      try {
        await family.open()
        for (let cycle = 0; cycle < CYCLES; cycle++) {
          const row = await family.cycle(cycle)
          expect({ cycle, problems: family.audit(row) }).toEqual({ cycle, problems: [] })
        }
        expect(BoundedAudit.journal(family.ledger.rows, CYCLES)).toEqual([])
        await family.finish()
      } finally {
        await family.close()
      }
    },
    240000
  )
})

describe.each([
  ['native result', (kind, outcome) => new NativeResultFamily({ kind, outcome })],
  ['auth supplier', (kind, outcome) => new SupplierFamily({ kind, outcome })],
])('bounded capture controls for the %s family', (_name, make) => {
  test.each(KIND_OUTCOMES)(
    'fails each deliberate fault with its own diagnostic for %s with late %s',
    async (kind, outcome) => {
      const family = make(kind, outcome)
      try {
        await family.open()
        for (let cycle = 0; cycle < CONTROL_CYCLES; cycle++) {
          const row = await family.cycle(cycle)
          expect({ cycle, problems: family.audit(row) }).toEqual({ cycle, problems: [] })
        }
        const { round, ledger } = family
        const row = ledger.rows.at(-1)
        const rows = ledger.rows.map((entry) => ({ ...entry }))
        expect(BoundedAudit.journal(rows, CONTROL_CYCLES)).toEqual([])
        expect(row.reopen).toEqual(IMMEDIATE)

        // The same number of owners, none of them the held shell.
        const decoy = new CycleLedger(round, () => family.decoyShell())
        expect(family.audit({ ...row, shell: await decoy.baseline() })).toEqual(
          family.roles.map((role) => `identities ${role} changed`)
        )

        // Same-count substitutions of one owner in an otherwise valid row.
        const changed = (change) => {
          const copy = JSON.parse(JSON.stringify(row))
          change(copy)
          return family.audit(copy)
        }
        const previous = ledger.rows.at(-2)
        expect(changed((copy) => (copy.timers.scheduler = [copy.timers.floor]))).toEqual([
          'a timer from a retired lifecycle survives',
        ])
        expect(
          changed((copy) => {
            copy.timers.unknown = copy.timers.scheduler
            copy.timers.scheduler = []
          })
        ).toEqual(['the scheduler timer is not exactly one', 'an unowned timer is pending'])
        const owned = family.roles.includes('nativeInput') ? 'nativeInput' : 'pending'
        expect(
          changed((copy) => (copy.shell.identities[owned] = [Number.MAX_SAFE_INTEGER]))
        ).toEqual([`identities ${owned} changed`])
        expect(
          changed((copy) => (copy.churn.identities.queue = [Number.MAX_SAFE_INTEGER]))
        ).toEqual(['queue owners changed'])
        expect(row.churn.identities.sockets).toHaveLength(family.expected.sockets)
        if (family.expected.sockets) {
          const events = row.churn.registrations['dedicated-socket'].events
          const ids = Object.values(events).flat()
          const event = Object.keys(events).find(
            (name) =>
              events[name].length === 1 && ids.filter((id) => id === events[name][0]).length === 1
          )
          const retiredSocket = previous.churn.identities.sockets[0]
          expect(
            changed((copy) => {
              copy.churn.identities.sockets = [retiredSocket]
              copy.churn.registrations['dedicated-socket'].id = retiredSocket
            })
          ).toEqual([
            'a retired sockets owner is still held',
            'a retired emitter or listener is still registered',
          ])
          expect(
            changed(
              (copy) =>
                (copy.churn.registrations['dedicated-socket'].id = copy.churn.identities.handles[0])
            )
          ).toEqual(['the listeners are on another emitter'])
          expect(
            changed(
              (copy) =>
                (copy.churn.registrations['dedicated-socket'].events[event] =
                  previous.churn.registrations['dedicated-socket'].events[event])
            )
          ).toEqual(['a retired emitter or listener is still registered'])

          // One registration twice on the open dedicated socket.
          const [socket] = round.openDedicated()
          const [listener] = socket.rawListeners(event)
          socket.on(event, listener)
          try {
            ledger.current = [{}]
            const churn = await ledger.churnProbe.snapshot()
            ledger.current = []
            expect(family.audit({ ...row, churn })).toEqual(['dedicated socket listeners changed'])
          } finally {
            socket.off(event, listener)
          }
        }

        // A reconnect deferred by one await is not the disconnect continuation's next call.
        const deferred = await family.cycle(CONTROL_CYCLES, { defer: true })
        expect(deferred.reopen).toEqual(['disconnect', 'yield', 'connect'])
        expect(family.audit(deferred)).toEqual([
          'the reconnect did not run in the disconnect continuation',
        ])

        // A share record whose acknowledgement the broker rejects does not prove progress.
        if (kind === 'shareConsumer') {
          const offset = family.foreground.nextOffset
          await expect(
            round.wait(() => family.foreground.act(CONTROL_CYCLES + 1, { ackError: 121 }), 30000)
          ).rejects.toThrow(`the acknowledgement of offset ${offset} was not confirmed`)
        }

        // A completed owner left in the restore table survives until it is released.
        const retired = {}
        round.owners.add(
          retired,
          () => {},
          () => true
        )
        expect(BoundedAudit.state(round.boundedState(), ledger.base)).toEqual([
          'a retired owner is still in the restore table',
        ])
        round.releaseCompleted()
        expect(BoundedAudit.state(round.boundedState(), ledger.base)).toEqual([])

        // Issued numbers that fall are a reset after pruning, not a continuation.
        const reset = rows.map((entry, index) =>
          index === 1 ? { ...entry, state: { ...entry.state, socketsIssued: 0 } } : entry
        )
        expect(BoundedAudit.journal(reset, CONTROL_CYCLES)).toEqual([
          'issued socket numbers fell at cycle 1',
        ])

        // More live owners than the table admits fails instead of growing.
        const filler = Array.from({ length: BOUNDED_LIMITS.owners + 1 }, () => ({}))
        try {
          expect(() => filler.forEach((owner) => round.owners.add(owner, () => {}))).toThrow(
            `Bounded capture overflow: more than ${BOUNDED_LIMITS.owners} live owners`
          )
        } finally {
          filler.forEach((owner) => round.owners.release(owner))
        }
        expect(BoundedAudit.state(round.boundedState(), ledger.base)).toEqual([])

        // The held late outcome is real: letting it settle early is a failure of the schedule.
        await family.releaseEarly()
        expect(family.held()).toEqual([
          family.roles.includes('builder')
            ? 'the held native job settled during the schedule'
            : 'the held supplier shell settled early',
        ])

        // A write to a socket that already closed is counted and judged.
        const [node] = round.wire.nodes
        const stray = round.wire.fake.factory({
          host: node.host,
          port: node.port,
          onConnect: () => {},
        })
        stray.destroy()
        await turn()
        await turn()
        stray.write(Buffer.alloc(1))
        expect(BoundedAudit.state(round.boundedState(), ledger.base)).toEqual([
          'a write reached a closed socket',
        ])
      } finally {
        await family.close()
      }
    },
    120000
  )
})

describe('bounded capture of the round', () => {
  test.each(['get', 'normal', 'final'])(
    'the %s manifest starts with the zero UUID sentinel',
    (mode) => {
      const cell = telemetryReleaseCells().find(
        (cell) =>
          cell.mode === mode &&
          cell.action === RELEASE_ACTIONS[0] &&
          cell.options.position === 'before'
      )
      expect(cell.expectation({ running: 1n, closeRevision: 2n })[0].uuid).toBeNull()
    }
  )

  test('restores shared clocks when overlapping rounds finish out of order', async () => {
    const imported = require('node:perf_hooks').performance
    const originals = [Date.now, process.hrtime.bigint, imported.now, global.performance.now]
    const first = new PublicTelemetryRound({ kind: 'producer' })
    const second = new PublicTelemetryRound({ kind: 'producer' })
    try {
      first.installClocks()
      first.clock.jump(10)
      second.installClocks()
      second.clock.jump(20)
      await first.dispose()
      expect(Date.now()).toBe(1020)
      expect(process.hrtime.bigint()).toBe(20000000n)
      expect(imported.now()).toBe(20)
    } finally {
      await first.dispose()
      await second.dispose()
    }
    expect([Date.now, process.hrtime.bigint, imported.now, global.performance.now]).toEqual(
      originals
    )
  })

  test('preserves a performance spy owned by the enclosing fake-clock fixture', async () => {
    const imported = require('node:perf_hooks').performance
    const enclosing = jest.spyOn(imported, 'now').mockReturnValue(37)
    const round = new PublicTelemetryRound({ kind: 'producer' })
    try {
      round.installClocks()
      round.clock.jump(20)
      expect(imported.now()).toBe(20)
      await round.dispose()
      expect(imported.now).toBe(enclosing)
      expect(imported.now()).toBe(37)
    } finally {
      await round.dispose()
      enclosing.mockRestore()
    }
  })

  test('restores its clocks even when a collaborator restoration throws', async () => {
    const imported = require('node:perf_hooks').performance
    const originals = [Date.now, process.hrtime.bigint, imported.now]
    const round = new PublicTelemetryRound({ kind: 'producer' })
    const reason = new Error('restore failed')
    round.installClocks()
    round.restores.push(() => {
      throw reason
    })
    await expect(round.dispose()).rejects.toBe(reason)
    expect([Date.now, process.hrtime.bigint, imported.now]).toEqual(originals)
  })

  test.each([false, true])(
    'restores an initially absent socket as writable bounded=%s',
    async (bounded) => {
      const round = new PublicTelemetryRound({ kind: 'producer', bounded })
      const connection = {}
      const socket = { destroyed: false }
      try {
        round.watchSocket(connection)
        connection.socket = socket
        socket.destroyed = true
        if (bounded) expect(round.releaseCompleted()).toBe(1)
      } finally {
        await round.dispose()
      }
      expect(Object.getOwnPropertyDescriptor(connection, 'socket')).toEqual({
        value: socket,
        writable: true,
        enumerable: true,
        configurable: true,
      })
      expect(() => {
        connection.socket = null
      }).not.toThrow()
      expect(connection.socket).toBeNull()
    }
  )

  test.each([false, true])('restores an existing socket descriptor bounded=%s', async (bounded) => {
    const round = new PublicTelemetryRound({ kind: 'producer', bounded })
    const connection = {}
    Object.defineProperty(connection, 'socket', {
      value: null,
      writable: true,
      enumerable: false,
      configurable: true,
    })
    const socket = { destroyed: false }
    try {
      round.watchSocket(connection)
      connection.socket = socket
    } finally {
      await round.dispose()
    }
    expect(Object.getOwnPropertyDescriptor(connection, 'socket')).toEqual({
      value: socket,
      writable: true,
      enumerable: false,
      configurable: true,
    })
  })

  test('keeps the native hold input copy for default callers', async () => {
    const round = new PublicTelemetryRound({ kind: 'producer' })
    const input = Buffer.alloc(1001, 7)
    const codec = jest.spyOn(gzip, 'compress').mockResolvedValue(Buffer.from('compressed'))
    try {
      const gate = round.hold('S12', { mode: 'normal' })
      round.arm()
      const compressing = gzip.compress({ buffer: input })
      round.trackWork(compressing)
      await gate.arrived()
      expect(round.codecJobs[0].raw).toEqual(input)
      expect(round.codecJobs[0].raw).not.toBe(input)
      input[0] = 8
      expect(round.codecJobs[0].raw[0]).toBe(7)
      gate.release()
      await compressing
    } finally {
      await round.dispose()
      codec.mockRestore()
    }
  })

  test('keeps every default round history and counts the same work', async () => {
    const round = new PublicTelemetryRound({ kind: 'producer' })
    try {
      await round.wait(() => round.start())
      await round.ready()
      const state = round.boundedState()
      expect(round.bounded).toBe(false)
      expect(state.history).toBeGreaterThan(0)
      expect(state.restorers).toBeGreaterThan(0)
      expect(state.owners).toBe(0)
      expect(round.pushes()).toHaveLength(round.pushesRequested())
      expect(round.dedicatedCount()).toBe(round.dedicatedSockets.length)
      expect(round.releaseCompleted()).toBe(0)
    } finally {
      await round.dispose()
    }
  }, 60000)

  test.each(KINDS)(
    'holds no history of completed work and refuses what needs it for %s',
    async (kind) => {
      const round = new PublicTelemetryRound({ kind, bounded: true })
      try {
        await round.wait(() => round.start())
        await round.ready()
        await round.wait(() => round.beginClose(round.closeOptions(0)))
        await round.wait(() => round.client.connect())
        await round.establish()
        round.releaseCompleted()
        const state = round.boundedState()
        expect(state.history).toBe(0)
        expect(state.retainedBytes).toBe(0)
        expect(state.ownersCompleted).toBe(0)
        expect(state.socketsIssued).toBeGreaterThan(1)
        expect(round.dedicatedCount()).toBe(state.socketsIssued)
        expect(round.openDedicated()).toHaveLength(1)
        expect(round.pushesAnswered()).toBeGreaterThan(0)
        for (const name of [
          'pushes',
          'telemetryRequests',
          'assertTrace',
          'writtenFor',
          'finalSnapshotIdentity',
          'replacementMetricOrigin',
          'forcedRetirements',
          'newSetupCallsSince',
        ])
          expect(() => round[name]()).toThrow(`${name}() needs the full capture`)
      } finally {
        await round.dispose()
      }
    },
    90000
  )

  test('releases a completed owner and restores its patch, and drains the live ones on disposal', async () => {
    const round = new PublicTelemetryRound({ kind: 'producer', bounded: true })
    const done = { value: false }
    const live = { write: () => 'original' }
    const finished = { write: () => 'original' }
    const original = finished.write
    round.patchFor(
      finished,
      'write',
      () => () => 'patched',
      () => done.value
    )
    round.patchFor(
      live,
      'write',
      () => () => 'patched',
      () => false
    )
    expect(finished.write()).toBe('patched')
    expect(round.owners.size).toBe(2)
    expect(round.releaseCompleted()).toBe(0)
    done.value = true
    expect(round.releaseCompleted()).toBe(1)
    expect(finished.write).toBe(original)
    expect(round.owners.has(finished)).toBe(false)
    expect(live.write()).toBe('patched')
    await round.dispose()
    expect(live.write()).toBe('original')
    expect(round.owners.size).toBe(0)
  })

  test('installs no patch or socket accessor that the full owner table cannot admit', async () => {
    const round = new PublicTelemetryRound({ kind: 'producer', bounded: true })
    const fillers = Array.from({ length: BOUNDED_LIMITS.owners }, () => ({
      write: () => 'original',
    }))
    const original = () => 'original'
    const victim = { write: original }
    const raw = { destroyed: false }
    const connection = { socket: raw }
    const descriptor = Object.getOwnPropertyDescriptor(connection, 'socket')
    try {
      for (const filler of fillers) round.patchFor(filler, 'write', () => () => 'patched')
      const overflow = `Bounded capture overflow: more than ${BOUNDED_LIMITS.owners} live owners`
      expect(() => round.patchFor(victim, 'write', () => () => 'patched')).toThrow(overflow)
      expect(victim.write).toBe(original)
      expect(() => round.watchSocket(connection)).toThrow(overflow)
      expect(Object.getOwnPropertyDescriptor(connection, 'socket')).toEqual(descriptor)
      expect(round.owners.has(victim) || round.owners.has(connection)).toBe(false)
      expect(fillers.every((filler) => filler.write() === 'patched')).toBe(true)
    } finally {
      await round.dispose()
    }
    expect(fillers.every((filler) => filler.write() === 'original')).toBe(true)
    expect(victim.write).toBe(original)
    expect(Object.getOwnPropertyDescriptor(connection, 'socket')).toEqual(descriptor)
    expect(round.owners.size).toBe(0)
  })
})

describe('share acknowledgement proof', () => {
  /** A share request carrying one acknowledgement batch. */
  const request = (api, first, last, types, { topicId = SHARE_TOPIC_ID, partition = 0 } = {}) => ({
    api,
    fields: {
      topics: [
        {
          topicId,
          partitions: [
            {
              partitionIndex: partition,
              acknowledgementBatches: [
                { firstOffset: String(first), lastOffset: String(last), acknowledgeTypes: types },
              ],
            },
          ],
        },
      ],
    },
  })
  /** A public acknowledgement outcome. */
  const outcome = (first, last, type, error = null, topic = 'topic-a', partition = 0) => ({
    topic,
    partition,
    firstOffset: String(first),
    lastOffset: String(last),
    type,
    error,
  })
  const proofFor = (offset) =>
    new AckProof({ topic: 'topic-a', topicId: SHARE_TOPIC_ID, partition: 0, offset })

  test('joins only the Accept of its own record and its successful outcome', () => {
    const proof = proofFor(105)
    proof.wire('request', request('ShareFetch', 104, 104, [1]))
    proof.wire('request', request('ShareFetch', 105, 105, [2]))
    proof.wire('request', request('ShareFetch', 105, 105, [1], { topicId: OTHER_TOPIC_ID }))
    proof.wire('request', request('ShareFetch', 105, 105, [1], { partition: 1 }))
    proof.wire('answered', request('Metadata', 105, 105, [1]))
    proof.settle(outcome(104, 104, 'accept'))
    proof.settle(outcome(105, 105, 'accept', null, 'topic-b'))
    proof.settle(outcome(105, 105, 'accept', null, 'topic-a', 1))
    expect(proof.facts()).toEqual({ written: 0, answered: 0, outcome: null })

    proof.wire('request', request('ShareAcknowledge', 100, 110, [1]))
    proof.wire('answered', request('ShareAcknowledge', 100, 110, [1]))
    proof.settle(outcome(100, 110, 'accept'))
    expect(proof.facts()).toEqual({ written: 1, answered: 1, outcome: 'accept' })
  })

  test('refuses a release, a rejected acknowledgement and a mixed batch that does not accept it', () => {
    const proof = proofFor(102)
    proof.settle(outcome(102, 102, 'release'))
    expect(proof.facts().outcome).toBe('release')
    proof.settle(outcome(102, 102, 'accept', new Error('rejected by the broker')))
    expect(proof.facts().outcome).toBe('error')
    expect(proof.covers(request('ShareFetch', 100, 103, [1, 1, 2, 1]).fields)).toBe(false)
    expect(proofFor(101).covers(request('ShareFetch', 100, 103, [1, 1, 2, 1]).fields)).toBe(true)
    expect(proof.covers(null)).toBe(false)
  })
})

/**
 * Runs one single-client memory child under the parent's supervision. The parent issues the child's
 * identity, run and tree, reads its output incrementally against the fixed message catalogue and
 * records the producing process. A child that overruns is killed and still joined; the facts come
 * back whatever happened, so a failure leaves its evidence.
 * @param {string} script fixture file name @param {string} kind public client kind
 * @param {Record<string, string>} [extraEnv] variables a seeded control adds to the child
 * @returns {Promise<object>} exit, output and receipt facts and the readings line
 */
const supervise = (script, kind, extraEnv = {}) =>
  new Promise((resolve) => {
    const file = path.join(__dirname, 'fixtures', script)
    // The child runs without coverage, inspector or Jest, so none of them can add to its memory.
    const producer = {
      arm: script === 'publicMemoryChild.js' ? 'public' : 'collector',
      kind,
      childId: crypto.randomUUID(),
      tree: process.env.KAFKAJS_TREE || 'unfrozen',
      runId: process.env.GITHUB_RUN_ID || 'local-resource-run',
      node: process.version,
      v8: process.versions.v8,
      platform: process.platform,
      arch: process.arch,
    }
    const output = new MemoryOutput(producer)
    const env = {
      ...process.env,
      RESOURCE_CHILD_ID: producer.childId,
      RESOURCE_TREE: producer.tree,
      RESOURCE_RUN_ID: producer.runId,
      ...extraEnv,
    }
    delete env.NODE_V8_COVERAGE
    const child = spawn(process.execPath, [...MEMORY_FLAGS, file, kind], {
      env,
      stdio: ['ignore', 'pipe', 'pipe'],
    })
    producer.pid = child.pid
    let stderr = ''
    let stdoutBytes = 0
    let stderrBytes = 0
    let timedOut = false
    let failure = null
    const limit = setTimeout(() => {
      timedOut = true
      child.kill('SIGKILL')
    }, 90000)
    child.stdout.on('data', (chunk) => {
      stdoutBytes += chunk.length
      output.write(chunk)
    })
    child.stderr.on('data', (chunk) => {
      stderrBytes += chunk.length
      if (stderr.length < 1024) stderr += chunk
    })
    child.on('error', (error) => {
      failure = error.message
    })
    child.on('close', (code, signal) => {
      clearTimeout(limit)
      const parsed = output.finish()
      resolve({
        code,
        signal,
        timedOut,
        stdoutBytes,
        stderrBytes,
        stderr,
        ...parsed,
        producer,
        failure: failure ?? parsed.failure,
      })
    })
  })

/**
 * Records one measured arm where the resource job collects it, before any assertion runs.
 * @param {string} arm `public` or `collector` @param {string} kind public client kind
 * @param {object} run what `supervise` returned
 * @param {object} before host context before the child @param {object} after after it
 * @param {string|null} [control] the seeded control's name: its record is kept in `controls/`,
 *   never beside the acceptance artifacts the validator reads
 * @returns {object} the record that was judged, also written to the artifact directory when one
 *   is configured
 */
const record = (arm, kind, run, before, after, control = null) => {
  const { readings, failure, ...exit } = run
  const artifact = {
    schema: SCHEMA,
    arm,
    kind,
    stamp: {
      tree: process.env.KAFKAJS_TREE || 'unfrozen',
      node: process.version,
      platform: process.platform,
      arch: process.arch,
      runId: process.env.GITHUB_RUN_ID || 'local-resource-run',
      at: new Date().toISOString(),
    },
    context: { before, after },
    exit: { ...exit, failure },
    readings,
  }
  const directory = process.env.TELEMETRY_RESOURCES_ARTIFACT_DIR
  if (directory) {
    const file = path.join(
      directory,
      control ? path.join('controls', `${control}-${kind}.json`) : `resources-${arm}-${kind}.json`
    )
    fs.mkdirSync(path.dirname(file), { recursive: true })
    fs.writeFileSync(file, JSON.stringify(artifact))
  }
  return artifact
}

// Memory and RSS readings need a quiet host: the unit lane skips them and the dedicated resources
// job, which runs this file alone with the variable set, owns them.
const quiet = process.env.TELEMETRY_RESOURCES_QUIET === '1' ? test.each : test.skip.each
quiet(KINDS)(
  'single client resource memory for %s',
  async (kind) => {
    const before = hostContext()
    const run = await supervise('publicMemoryChild.js', kind)
    const judged = judgeMemoryArm(record('public', kind, run, before, hostContext()))
    expect(judged).toEqual({ verdict: 'pass', problems: [] })
  },
  120000
)

quiet(KINDS)(
  'collector only resource memory for %s',
  async (kind) => {
    const before = hostContext()
    const run = await supervise('resourceChild.js', kind)
    const judged = judgeMemoryArm(record('collector', kind, run, before, hostContext()))
    expect(judged).toEqual({ verdict: 'pass', problems: [] })
  },
  120000
)

const quietOnce = process.env.TELEMETRY_RESOURCES_QUIET === '1' ? test : test.skip

/** The seeded controls of D169: what each leaks, in bytes, and the reasons the judge must give. */
const SEED_CLOSE_BYTES = 131072
const SEED_MIN_TOTAL = 524288
const SEED_WARMUP_BYTES = 1048577
const CLOSED_CEILING = 'closed transient data bytes exceed 256 KiB'
const FIRST_USE_BOUND = 'warm-up first-use data bytes exceed 1 MiB'
const SEEDED = 'seeded control is not acceptance evidence'

/**
 * Runs a public arm seeded with a leak through a product-reachable root, and judges it.
 * @param {'close'|'close-warmup'|'warmup-only'} mode which passes leak
 * @param {string} kind public client kind
 * @returns {Promise<{judged: {verdict: string, problems: string[]}, readings: object}>} the
 *   verdict and the child's own account of what it leaked
 */
const seeded = async (mode, kind) => {
  const before = hostContext()
  const run = await supervise('publicMemoryChild.js', kind, { RESOURCE_SEED: mode })
  const artifact = record('public', kind, run, before, hostContext(), `seeded-${mode}`)
  return { judged: judgeMemoryArm(artifact), readings: artifact.readings }
}

quiet(KINDS)(
  'per-close seeded resource memory for %s',
  async (kind) => {
    const { judged, readings } = await seeded('close', kind)
    expect(readings.manifest.seed).toMatchObject({ mode: 'close', closeBytes: SEED_CLOSE_BYTES })
    expect(readings.seeds.warm).toBeNull()
    expect(readings.seeds.measured.afterMatched).toBeGreaterThanOrEqual(SEED_MIN_TOTAL)
    expect(judged.verdict).toBe('fail')
    expect(judged.problems).toContain(SEEDED)
    expect(judged.problems).toContain(CLOSED_CEILING)
  },
  120000
)

quiet(KINDS)(
  'per-close seeded through the warm-up resource memory for %s',
  async (kind) => {
    const { judged, readings } = await seeded('close-warmup', kind)
    expect(readings.seeds.warm.afterMatched).toBeGreaterThanOrEqual(SEED_MIN_TOTAL)
    expect(readings.seeds.measured.afterMatched).toBeGreaterThanOrEqual(SEED_MIN_TOTAL)
    expect(judged.verdict).toBe('fail')
    expect(judged.problems).toContain(SEEDED)
    expect(judged.problems).toContain(CLOSED_CEILING)
  },
  120000
)

quiet(KINDS)(
  'warm-up only seeded resource memory for %s',
  async (kind) => {
    const { judged, readings } = await seeded('warmup-only', kind)
    expect(readings.seeds.measured).toBeNull()
    expect(readings.seeds.warm).toMatchObject({ bytes: SEED_WARMUP_BYTES })
    expect(readings.seeds.warm.afterMatched).toBe(SEED_WARMUP_BYTES)
    expect(judged.verdict).toBe('fail')
    expect(judged.problems).toContain(SEEDED)
    expect(judged.problems).toContain(FIRST_USE_BOUND)
    expect(judged.problems).not.toContain(CLOSED_CEILING)
  },
  120000
)

/**
 * Runs the code-accounting child on this Node line.
 * @returns {Promise<{code: number|null, signal: string|null, stdout: string, stderr: string}>}
 */
const codeAccounting = () =>
  new Promise((resolve) => {
    const env = { ...process.env }
    delete env.NODE_V8_COVERAGE
    const file = path.join(__dirname, 'fixtures', 'codeAccountingChild.js')
    const child = spawn(process.execPath, [...MEMORY_FLAGS, file], {
      env,
      stdio: ['ignore', 'pipe', 'pipe'],
    })
    let stdout = ''
    let stderr = ''
    const limit = setTimeout(() => child.kill('SIGKILL'), 90000)
    child.stdout.on('data', (chunk) => (stdout += chunk))
    child.stderr.on('data', (chunk) => (stderr += chunk))
    child.on('close', (code, signal) => {
      clearTimeout(limit)
      resolve({ code, signal, stdout, stderr })
    })
  })

quietOnce(
  'code accounting resource memory for the V8 code counters',
  async () => {
    const run = await codeAccounting()
    expect({ code: run.code, signal: run.signal, stderr: run.stderr }).toEqual({
      code: 0,
      signal: null,
      stderr: '',
    })
    const { compile, heapArray, typedArray } = JSON.parse(run.stdout)
    // Compiling and running functions moves the code counters, and they never account for more
    // bytes than the heap grew by, so subtracting them cannot hide a leak.
    expect(compile.code).toBeGreaterThanOrEqual(131072)
    expect(compile.data).toBeGreaterThanOrEqual(-65536)
    // An array of at least 1 MiB moves the data bytes by at least 1 MiB and the code counters little.
    for (const step of [heapArray, typedArray]) {
      expect(step.data).toBeGreaterThanOrEqual(1048576)
      expect(step.code).toBeLessThanOrEqual(262144)
    }
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

  /**
   * Orders, on the real decoder, what one Get response materializes against its refusal. Wrappers
   * only observe: the production decoder runs unchanged and returns or throws what it always did.
   * Array and string materialization is recorded only while a Get response is being decoded.
   * @param {PublicTelemetryRound} round owner, before `start` @returns {string[]} live ordered
   *   events: `decode`, then each materialization, then `decoded` or `refused`
   */
  const witness = (round) => {
    const events = []
    let active = false
    for (const key of ['readUVarIntArray', 'readUVarIntString'])
      round.patch(
        Decoder.prototype,
        key,
        (original) =>
          function (...args) {
            if (active) events.push(key)
            return original.apply(this, args)
          }
      )
    round.patch(
      Connection.prototype,
      'send',
      (original) =>
        function (options) {
          if (!this.authenticationOwner || options.request.apiKey !== 71)
            return original.call(this, options)
          const decode = options.response.decode
          return original.call(this, {
            ...options,
            response: {
              ...options.response,
              decode: (...args) => {
                active = true
                events.push('decode')
                let outcome
                try {
                  outcome = decode(...args)
                } finally {
                  active = false
                }
                return Promise.resolve(outcome).then(
                  (value) => {
                    events.push('decoded')
                    return value
                  },
                  (error) => {
                    events.push('refused')
                    throw error
                  }
                )
              },
            },
          })
        }
    )
    return events
  }

  const materialized = (events) => events.filter((event) => event.startsWith('read'))

  test('bounds accepted selection metadata', async () => {
    for (const [name, fields] of Object.entries(accepted)) {
      await inRound(async (round) => {
        const events = witness(round)
        await round.wait(() => round.start({ subscription: fields }))
        await round.ready()
        for (const [key, value] of Object.entries(fields))
          expect({ name, [key]: round.machine.subscription[key] }).toEqual({ name, [key]: value })
        expect({ name, state: round.machine.state }).toEqual({ name, state: 'READY' })
        expect(round.machine.uuid).toBe(TELEMETRY_UUID)
        expect({ name, first: events[0], last: events.at(-1) }).toEqual({
          name,
          first: 'decode',
          last: 'decoded',
        })
        expect({ name, materialized: materialized(events).length > 0 }).toEqual({
          name,
          materialized: true,
        })
      })
    }
    for (const [name, fields] of Object.entries(refused)) {
      await inRound(async (round) => {
        const events = witness(round)
        await round.wait(() => round.start({ subscription: fields }))
        round.tick()
        await round.until(() => round.machine.state === 'STOPPED')
        expect({ name, events }).toEqual({ name, events: ['decode', 'refused'] })
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
        const events = witness(round)
        await round.wait(() => round.start())
        await round.ready()
        expect({ name, last: events.at(-1) }).toEqual({ name, last: 'decoded' })
        const previous = round.machine.subscription
        events.length = 0
        round.subscription = { ...round.subscription, ...fields }
        round.machine.schedule('get', 0)
        round.tick()
        await round.until(() => round.machine.state === 'STOPPED')
        expect({ name, events }).toEqual({ name, events: ['decode', 'refused'] })
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
