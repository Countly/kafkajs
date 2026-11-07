const assert = require('node:assert/strict')
const {
  PublicTelemetryRound,
  TelemetryClock,
  TelemetryTrace,
  telemetryOutcome,
  assertTelemetryOutcome,
  telemetryReleaseCells,
  RELEASE_ACTIONS,
  TELEMETRY_KINDS,
  TELEMETRY_UUID,
} = require('../../../testHelpers/lifecycle')

const literal = '000000080048000000000007'
const frame = Object.freeze({
  connection: 1n,
  attempt: 2n,
  lifecycle: 3n,
  transport: 4n,
  subscription: 5n,
  request: 6n,
  correlation: 7,
  apiKey: 72,
  hex: literal,
})

describe('helper oracle controls', () => {
  test('same-count different connection is rejected', () => {
    const trace = new TelemetryTrace()
    trace.write(frame)
    assert.throws(() => trace.assertFrames([{ ...frame, connection: 2n }]), assert.AssertionError)
  })
  test('wire correlation must agree with bytes', () => {
    assert.throws(() => new TelemetryTrace().write({ ...frame, correlation: 8 }), TypeError)
  })
  test('same-count different request is rejected', () => {
    const trace = new TelemetryTrace()
    trace.write(frame)
    assert.throws(() => trace.assertFrames([{ ...frame, request: 7n }]), assert.AssertionError)
  })
  test('matching exact frame is accepted', () => {
    const trace = new TelemetryTrace()
    trace.write(frame)
    trace.assertFrames([frame])
  })
  test('cleared callback can be replayed independently of replacement timer', () => {
    const clock = new TelemetryClock()
    let old = 0
    let current = 0
    const id = clock.setTimeout(() => old++, 20)
    const callback = clock.callback(id)
    clock.clearTimeout(id)
    const next = clock.setTimeout(() => current++, 20)
    assert.notEqual(next, id)
    clock.jump(21)
    assert.equal(current, 0)
    callback()
    assert.equal(old, 1)
    assert.equal(current, 0)
    clock.deliver(next)
    assert.equal(current, 1)
    assert.equal(clock.pending.size, 0)
  })
  test('clock is monotonic and no early timer delivery', () => {
    const clock = new TelemetryClock()
    const id = clock.setTimeout(() => {}, 1)
    assert.throws(() => clock.deliver(id))
    clock.jump(2)
    assert.throws(() => clock.jump(1), RangeError)
    clock.deliver(id)
  })
  for (const mode of ['return', 'throw', 'reject'])
    test(`original ${mode} identity and count`, async () => {
      const sentinel = Object.freeze({ object: true })
      let calls = 0
      const actual = await telemetryOutcome(() => {
        calls++
        if (mode === 'throw') throw sentinel
        return mode === 'reject' ? Promise.reject(sentinel) : sentinel
      })
      const expected =
        mode === 'return' ? { kind: mode, value: sentinel } : { kind: mode, reason: sentinel }
      assertTelemetryOutcome(actual, expected, calls)
      const wrong =
        mode === 'return'
          ? { kind: mode, value: { object: true } }
          : { kind: mode, reason: { object: true } }
      assert.throws(() => assertTelemetryOutcome(actual, wrong, calls), assert.AssertionError)
      assert.throws(() => assertTelemetryOutcome(actual, expected, 2), assert.AssertionError)
    })
  test('throw undefined retains its explicit tag', async () => {
    const actual = await telemetryOutcome(() => {
      // eslint-disable-next-line no-throw-literal
      throw undefined
    })
    assertTelemetryOutcome(actual, { kind: 'throw', reason: undefined }, 1)
  })
  test('lazy gate starts original after release once', async () => {
    const trace = new TelemetryTrace()
    const gate = trace.hold({ boundary: 'encode' })
    let calls = 0
    const result = trace.enter({ boundary: 'encode' }, () => ++calls)
    await gate.arrived()
    assert.equal(calls, 0)
    gate.release()
    assert.equal(await result, 1)
    assert.equal(calls, 1)
  })
})

const releaseCells = telemetryReleaseCells()

describe('release catalogue', () => {
  const cells = releaseCells
  const modes = {
    get: [
      'S01',
      'S02',
      'S03',
      'S04',
      'S05',
      'S06',
      'S07',
      'S08',
      'S09',
      'S13',
      'S14',
      'S15',
      'S16',
      'S17',
      'S18',
    ],
    normal: [
      'S10',
      'S11',
      'S12',
      'S13',
      'S14',
      'S15',
      'S16',
      'S17',
      'S18',
      'S19a',
      'S19b',
      'S19c',
      'S19d',
      'S19e',
      'S19f',
    ],
    final: ['S10', 'S11', 'S12', 'S13', 'S14', 'S15', 'S16', 'S17', 'S18'],
  }
  const actions = (_boundary, mode) => {
    return mode === 'final' ? 4 : RELEASE_ACTIONS.length
  }
  const expected = Object.entries(modes).reduce(
    (sum, [mode, ids]) =>
      sum +
      ids.reduce(
        (inner, id) =>
          inner +
          actions(id, mode) *
            (['S01', 'S14', 'S17', 'S18'].includes(id) ? 2 : ['S08', 'S09'].includes(id) ? 3 : 1),
        0
      ),
    0
  )
  test('dimension cardinalities equal the explicit ledger', () => {
    expect(cells).toHaveLength(expected * TELEMETRY_KINDS.length * 2)
    expect(new Set(cells.map((cell) => cell.name)).size).toBe(cells.length)
    for (const kind of TELEMETRY_KINDS)
      for (const enforce of [false, true])
        expect(
          cells.filter((c) => c.kind === kind && c.enforceRequestTimeout === enforce)
        ).toHaveLength(expected)
  })
  test.each([
    ...new Set(cells.map((cell) => `${cell.boundary}/${cell.mode}/${cell.options.position}`)),
  ])('%s resolves to an installable source hook', async (key) => {
    const [boundary, mode, position] = key.split('/')
    const round = new PublicTelemetryRound({ kind: 'producer' })
    try {
      expect(round.hold(boundary, { mode, position })).toBeDefined()
      expect(() => round.hold(boundary, { mode })).toThrow('Duplicate telemetry hold')
    } finally {
      await round.dispose()
    }
  })
})

describe('stale work release matrix', () => {
  test.each(releaseCells)(
    '$name',
    async (cell) => {
      const round = new PublicTelemetryRound(cell)
      try {
        const gate = round.hold(cell.boundary, { mode: cell.mode, ...cell.options })
        await round.wait(() => round.start(cell.start))
        if (!cell.lateArrival) await round.wait(() => gate.arrived())
        const old = round.capture()
        if (cell.boundary === 'S01' && cell.options.position === 'after') {
          expect(round.builtHandles).toHaveLength(1)
          expect(old.handle).toBeNull()
          expect(round.telemetryRequests()).toEqual([])
        }
        await round.wait(() => round.transition(cell.action, cell.options))
        if (cell.lateArrival) await round.wait(() => gate.arrived())
        const replacement = round.capture()
        await round.wait(() => round.drainReleased(gate, cell.releaseValue))
        if (round.deferredReplacement) {
          round.assertReplacementUnchanged(replacement)
          if (round.machine.uuid)
            await expect(round.client.clientInstanceId({ timeout: 0 })).resolves.toBe(
              TELEMETRY_UUID
            )
          await round.establish()
        }
        round.assertTrace(cell.expectation(old, replacement))
        if (
          !round.deferredReplacement &&
          (cell.action.includes('replacement') || cell.action.includes('reopen'))
        )
          round.assertReplacementUnchanged(replacement)
        if (
          (cell.action.includes('replacement') || cell.action.includes('reopen')) &&
          round.machine.connected &&
          round.machine.subscription
        ) {
          const count = round.pushes().length
          round.tick()
          await round.until(() => round.pushes().length === count + 1 && !round.machine.running)
          expect(round.decodedLatestPoint({ name: 'com.example.total' }).value).toBe(0n)
        }
        expect(round.unhandled).toEqual([])
      } finally {
        await round.dispose()
      }
    },
    60000
  )
})

/** Establishes a ready public round and retains cleanup ownership until readiness succeeds.
 * @param {object} options round configuration @param {object} [start] startup options
 * @returns {Promise<PublicTelemetryRound>} caller-owned ready round
 * @throws {Error} original startup/readiness error, after restoring patches and listeners
 */
const established = async (options, start) => {
  const round = new PublicTelemetryRound(options)
  try {
    await round.start(start)
    await round.ready()
    return round
  } catch (error) {
    await round.dispose().catch(() => {})
    throw error
  }
}

describe('public round oracle controls', () => {
  test('factory startup rejection restores clocks prototypes and process listeners', async () => {
    const Connection = require('../../network/connection')
    const Factory = require('../factory').TelemetryFactory
    const originals = [
      Date.now,
      process.hrtime.bigint,
      require('node:perf_hooks').performance.now,
      Connection.prototype.send,
      Factory.prototype.bind,
    ]
    const listeners = process.listeners('unhandledRejection')
    const reason = new Error('startup control')
    await expect(
      established(
        { kind: 'producer' },
        {
          beforeConnect: () => {
            throw reason
          },
        }
      )
    ).rejects.toBe(reason)
    expect([
      Date.now,
      process.hrtime.bigint,
      require('node:perf_hooks').performance.now,
      Connection.prototype.send,
      Factory.prototype.bind,
    ]).toEqual(originals)
    expect(process.listeners('unhandledRejection')).toEqual(listeners)
  })

  test('a throwing remover cannot prevent the remaining restoration', async () => {
    const listeners = process.listeners('unhandledRejection')
    const original = Date.now
    const round = new PublicTelemetryRound({ kind: 'producer' })
    const reason = new Error('remover control')
    round.patch(Date, 'now', () => () => 42)
    round.eventRemovers.push(() => {
      throw reason
    })
    await expect(round.dispose()).rejects.toBe(reason)
    expect(Date.now).toBe(original)
    expect(process.listeners('unhandledRejection')).toEqual(listeners)
  })

  test.each(['request', 'subscription', 'transport', 'hex'])(
    'the public matrix comparator rejects same-count wrong %s',
    async (key) => {
      const round = await established({ kind: 'producer' })
      try {
        const expected = [
          { api: 71, terminating: null, uuid: null, subscriptionId: null, socket: 1n },
          { api: 72, terminating: false, uuid: TELEMETRY_UUID, subscriptionId: 7, socket: 1n },
        ]
        round.assertTrace(expected)
        const original = round.trace.frames[1]
        round.trace.frames[1] = {
          ...original,
          [key]: key === 'hex' ? original.hex.slice(0, -2) + '01' : original[key] + 1n,
        }
        expect(() => round.assertTrace(expected)).toThrow(assert.AssertionError)
        round.trace.frames[1] = original
      } finally {
        await round.dispose()
      }
    },
    60000
  )

  test('the lifecycle oracle rejects lost commit and handler evidence', async () => {
    const round = new PublicTelemetryRound({ kind: 'consumer' })
    round.coreProof = true
    try {
      await round.start()
      round.assertCore(0, 'active')
      const handled = round.core.handlers.pop()
      expect(() => round.assertCore(0, 'active')).toThrow(assert.AssertionError)
      round.core.handlers.push(handled)
      const at = round.wire.log.findIndex((row) => row.api === 'OffsetCommit')
      const [commit] = round.wire.log.splice(at, 1)
      expect(() => round.assertCore(0, 'active')).toThrow(assert.AssertionError)
      round.wire.log.splice(at, 0, commit)
    } finally {
      await round.dispose()
    }
  }, 60000)

  test.each([1, 4])(
    'S12 codec %s retains one raw job across public reopen',
    async (codecType) => {
      const zstd = require('../../protocol/message/compression/zstd')
      const round = new PublicTelemetryRound({ kind: 'producer' })
      round.codecType = codecType
      const gate = round.hold('S12')
      try {
        await round.start({
          points: 'large',
          subscription: { acceptedCompressionTypes: [codecType, 0] },
        })
        await round.ready()
        if (codecType === 4 && !zstd.isSupported) {
          expect(round.pushes().at(-1).fields.compressionType).toBe(0)
          expect(round.codecJobs).toEqual([])
          return
        }
        round.arm()
        round.tick()
        await round.wait(() => gate.arrived())
        expect(round.codecJobs).toHaveLength(1)
        expect(round.codecJobs[0].promise).toBeInstanceOf(Promise)
        expect(round.codecJobs[0].settled).toBe(false)
        await round.wait(() => round.beginClose({ timeout: 0 }))
        await round.client.connect()
        round.tick()
        await round.until(() => round.machine.subscription && !round.machine.running)
        const pushes = round.pushes().length
        round.tick()
        await round.until(() => !round.machine.running)
        expect(round.codecJobs).toHaveLength(1)
        expect(round.pushes()).toHaveLength(pushes)
        await round.drainReleased(gate)
        await round.until(() => round.codecJobs[0].settled)
        round.tick()
        await round.until(() => round.pushes().length === pushes + 1 && !round.machine.running)
        expect(round.codecJobs).toHaveLength(2)
        expect(round.pushes().at(-1).fields.compressionType).toBe(codecType)
      } finally {
        await round.dispose()
      }
    },
    60000
  )
})

test.each(
  TELEMETRY_KINDS.flatMap((kind) =>
    [false, true].map((enforceRequestTimeout) => ({ kind, enforceRequestTimeout }))
  )
)(
  'S18 $kind old operation finally cannot clear a replacement enforce=$enforceRequestTimeout',
  async ({ kind, enforceRequestTimeout }) => {
    const round = await established({ kind, enforceRequestTimeout })
    const schedule = new (require('../../../testHelpers/schedule').Schedule)()
    const gate = schedule.hold({ boundary: 'operation-finally' })
    round.gates.push(gate)
    let enteringRun = false
    let selected = false
    let old
    let abandonCalls = 0
    try {
      round.patch(
        round.machine,
        'abandon',
        (original) =>
          function (op) {
            if (op === old) abandonCalls++
            return original.call(this, op)
          }
      )
      round.patch(
        round.machine,
        'run',
        (original) =>
          function (...args) {
            enteringRun = true
            try {
              return original.apply(this, args)
            } finally {
              enteringRun = false
            }
          }
      )
      // Select the run's own work/cancellation await, before its continuation and finally.
      round.patch(
        Promise,
        'race',
        (original) =>
          function (values) {
            const result = original.call(this, values)
            if (!enteringRun || selected) return result
            selected = true
            old = round.machine.running
            const hold = () => schedule.run({ boundary: 'operation-finally' }, () => undefined)
            return result.then(
              async (value) => {
                await hold()
                return value
              },
              async (error) => {
                await hold()
                throw error
              }
            )
          }
      )
      round.tick()
      await round.wait(() => gate.arrived())
      expect(old).toBe(round.machine.running)
      expect(abandonCalls).toBe(0)
      await round.wait(() => round.beginClose({ timeout: 0 }))
      await round.client.connect()
      await round.establish()
      const replacement = round.capture()
      const before = abandonCalls
      await round.drainReleased(gate)
      expect(abandonCalls).toBe(before + 1)
      round.assertReplacementUnchanged(replacement)
      const count = round.pushes().length
      round.tick()
      await round.until(() => round.pushes().length === count + 1 && !round.machine.running)
      expect(round.decodedLatestPoint({ name: 'com.example.total' }).value).toBe(0n)
    } finally {
      await round.dispose()
    }
  },
  60000
)

describe('timer authority', () => {
  test.each(TELEMETRY_KINDS)(
    '%s cleared scheduler callback cannot run after equal-time rearm',
    async (kind) => {
      const round = await established({ kind })
      try {
        const callback = round.clock.callback(round.machine.timer)
        await round.wait(() => round.beginClose(round.closeOptions(0)))
        await round.settleClose()
        await round.wait(() => round.client.connect())
        const replacement = round.capture()
        callback()
        for (let turn = 0; turn < 50; turn++) await new Promise(setImmediate)
        expect(round.capture().requests).toBe(replacement.requests)
        round.assertReplacementUnchanged(replacement)
        await round.establish()
        expect(round.trace.frames.at(-1).apiKey).toBe(72)
      } finally {
        await round.dispose()
      }
    },
    60000
  )

  test.each(TELEMETRY_KINDS)(
    '%s late interval callback does not catch up',
    async (kind) => {
      const round = await established({ kind })
      try {
        const id = round.machine.timer
        const due = round.clock.pending.get(id).due
        round.clock.jump(due - 1)
        round.clock.pending.get(id).callback()
        expect(round.machine.timer).not.toBe(id)
        expect(round.pushes()).toHaveLength(1)
        round.clock.jump(due + 500)
        const before = round.pushes().length
        round.tick()
        await round.until(() => round.pushes().length > before && !round.machine.running)
        expect(round.pushes()).toHaveLength(before + 1)
        const next = round.clock.pending.get(round.machine.timer)
        expect(next.due).toBe(round.clock.time + 100)
      } finally {
        await round.dispose()
      }
    },
    60000
  )

  test.each(TELEMETRY_KINDS)(
    '%s old watchdog cannot retire replacement transport',
    async (kind) => {
      const round = new PublicTelemetryRound({ kind })
      try {
        const gate = round.hold('S09', { mode: 'get' })
        await round.wait(() => round.start({ drive: 'get' }))
        await round.wait(() => gate.arrived())
        const callback = round.clock.callback(round.machine.watchdog)
        await round.wait(() => round.transition(RELEASE_ACTIONS[5]))
        const replacement = round.capture()
        callback()
        await round.drainReleased(gate)
        expect(round.machine.handle.isConnected()).toBe(true)
        round.assertReplacementUnchanged(replacement)
      } finally {
        await round.dispose()
      }
    },
    60000
  )

  test.each(TELEMETRY_KINDS)(
    '%s earliest waiter timer cannot settle replacement caller',
    async (kind) => {
      const round = new PublicTelemetryRound({ kind })
      try {
        const gate = round.hold('S09', { mode: 'get' })
        await round.wait(() => round.start({ drive: 'get' }))
        await round.wait(() => gate.arrived())
        const settled = []
        for (const timeout of [10, 30])
          round.pending.push(
            round.client.clientInstanceId({ timeout }).then(
              (value) => settled.push(value),
              (error) => settled.push(error.reason)
            )
          )
        const oldTimer = round.clock.callback(round.machine.waiterTimer)
        round.clock.jump(11)
        await round.wait(() => round.suspendForeground())
        await round.until(() => settled.length === 2)
        expect(settled).toEqual([
          'timeout',
          ['consumer', 'shareConsumer'].includes(kind) ? 'not-connected' : 'closed',
        ])
        await round.wait(() => round.restartForeground())
        const third = round.client.clientInstanceId({ timeout: 1000 })
        oldTimer()
        await round.drainReleased(gate)
        expect(round.machine.waiters.size).toBe(1)
        round.tick()
        await expect(third).resolves.toBe(TELEMETRY_UUID)
      } finally {
        await round.dispose()
      }
    },
    60000
  )

  test.each(
    TELEMETRY_KINDS.flatMap((kind) =>
      [false, true].map((enforceRequestTimeout) => ({ kind, enforceRequestTimeout }))
    )
  )(
    '$kind close watchdog uses original deadline under delayed delivery enforce=$enforceRequestTimeout',
    async (cell) => {
      for (const timeout of [20, undefined]) {
        const round = new PublicTelemetryRound(cell)
        try {
          const gate = round.hold('S13', { mode: 'final' })
          await round.start()
          await round.ready()
          round.arm()
          round.closePromise = round.beginClose(timeout === undefined ? undefined : { timeout })
          await round.wait(() => gate.arrived())
          const closing = round.capture()
          expect(closing.closeDeadline - closing.closeEntry).toBe(timeout ?? 1000)
          round.clock.jump(closing.closeDeadline + 1)
          await round.drainReleased(gate)
          await round.deliverCloseWatchdog()
          await round.settleClose()
          expect(round.finalFrames()).toEqual([])
          expect(round.forcedRetirements()).toContain(closing.handle)
        } finally {
          await round.dispose()
        }
      }
    },
    60000
  )

  test.each(TELEMETRY_KINDS)(
    '%s retry preserves uncached caller deadlines',
    async (kind) => {
      const round = new PublicTelemetryRound({ kind })
      try {
        const gate = round.hold('S09', { mode: 'get' })
        await round.wait(() => round.start({ drive: 'get' }))
        await round.wait(() => gate.arrived())
        const settled = []
        for (const timeout of [10, 3000])
          round.pending.push(
            round.client.clientInstanceId({ timeout }).then(
              (value) => settled.push(value),
              (error) => settled.push(error.reason)
            )
          )
        round.clock.jump(5)
        round.faultDedicated()
        await round.drainReleased(gate)
        expect(round.machine.waiters.size).toBe(2)
        round.clock.jump(11)
        round.clock.deliver(round.machine.waiterTimer)
        await round.until(() => settled.length === 1)
        expect(settled).toEqual(['timeout'])
        const survivor = [...round.machine.waiters][0]
        expect(survivor.deadline).toBe(3000)
        expect(round.machine.timer).not.toBeNull()
        round.tick()
        await round.until(() => settled.length === 2)
        expect(round.clock.now()).toBeLessThan(survivor.deadline)
        expect(settled).toEqual(['timeout', TELEMETRY_UUID])
        expect(round.machine.waiters.size).toBe(0)
        expect(round.machine.waiterTimer).toBeNull()
        await round.wait(() => round.suspendForeground())
        await expect(round.client.clientInstanceId({ timeout: 0 })).resolves.toBe(TELEMETRY_UUID)
      } finally {
        await round.dispose()
      }
    },
    60000
  )
})

describe('numeric authority', () => {
  test.each(TELEMETRY_KINDS)(
    '%s subscription A to B to A issues three distinct revisions',
    async (kind) => {
      const round = await established({ kind })
      const gate = round.hold('S11', { mode: 'normal' })
      try {
        round.arm()
        round.tick()
        await round.wait(() => gate.arrived())
        const old = round.capture()
        const revisions = [round.machine.subscription.revision]
        const watchdog = round.machine.watchdog
        round.clock.jump(round.clock.pending.get(watchdog).due)
        round.clock.deliver(watchdog)
        await round.until(() => !round.machine.running && round.machine.timer !== null)
        const answers = [9, 7]
        round.wire.answer('GetTelemetrySubscriptions', (fields) => ({
          ...round.subscription,
          subscriptionId: answers[0],
          clientInstanceId: fields.clientInstanceId ?? TELEMETRY_UUID,
        }))
        for (const next of answers) {
          if (next === 7) {
            round.wire.answer('PushTelemetry', () => ({ errorCode: 117 }))
            round.tick()
            await round.until(() => round.machine.state === 'BACKOFF' && !round.machine.running)
          }
          round.wire.answer('PushTelemetry', () => ({}))
          round.wire.answer('GetTelemetrySubscriptions', (fields) => ({
            ...round.subscription,
            subscriptionId: next,
            clientInstanceId: fields.clientInstanceId ?? TELEMETRY_UUID,
          }))
          const gets = round.wire.requests().filter((r) => r.api === 'GetTelemetrySubscriptions')
          round.tick()
          await round.until(
            () =>
              round.wire.requests().filter((r) => r.api === 'GetTelemetrySubscriptions').length >
                gets.length && round.machine.subscription?.subscriptionId === next
          )
          revisions.push(round.machine.subscription.revision)
        }
        expect(revisions.map((r) => typeof r)).toEqual(['bigint', 'bigint', 'bigint'])
        expect(new Set(revisions).size).toBe(3)
        expect(round.machine.subscription.subscriptionId).toBe(7)
        const replacement = round.capture()
        await round.drainReleased(gate)
        round.assertReplacementUnchanged(replacement)
        expect(
          round.written
            .slice(old.written)
            .some((entry) => entry.snapshot === round.snapshots[old.snapshots - 1]?.snapshot)
        ).toBe(false)
        const count = round.pushes().length
        round.tick()
        await round.until(() => round.pushes().length > count && !round.machine.running)
        expect(round.decodedLatestPoint({ name: 'com.example.total' }).value).toBe(0n)
      } finally {
        await round.dispose()
      }
    },
    60000
  )

  test.each(TELEMETRY_KINDS)(
    '%s unregister and re-register cannot authorize the old sample',
    async (kind) => {
      const round = new PublicTelemetryRound({ kind })
      try {
        const gate = round.hold('S11', { mode: 'normal' })
        await round.wait(() => round.start({ drive: 'normal' }))
        await round.wait(() => gate.arrived())
        const before = round.capture()
        round.client.unregisterMetricFromSubscription({ name: 'com.example.total' })
        round.client.registerMetricForSubscription({
          name: 'com.example.total',
          type: 'sum',
          value: 11n,
        })
        await round.drainReleased(gate)
        expect(round.capture().written).toBe(before.written)
        expect(round.pushes()).toHaveLength(1)
        round.tick()
        await round.until(() => round.pushes().length === 2 && !round.machine.running)
        const point = round.decodedLatestPoint({ name: 'com.example.total' })
        expect(point.value).toBe(11n)
        expect(point.metricRevision).not.toBe(before.metricRevision)
      } finally {
        await round.dispose()
      }
    },
    60000
  )

  test.each(TELEMETRY_KINDS)(
    '%s equal-valued resource replacement revokes the old ordinary sample',
    async (kind) => {
      const round = new PublicTelemetryRound({ kind })
      try {
        const gate = round.hold('S10', { mode: 'normal' })
        await round.wait(() => round.start({ drive: 'normal' }))
        await round.wait(() => gate.arrived())
        const captured = round.binding.captureResources()
        const frames = round.trace.frames.length
        round.binding.updateResources(captured.values)
        expect(captured.isCurrent()).toBe(false)
        await round.drainReleased(gate)
        expect(round.trace.frames).toHaveLength(frames)
        round.tick()
        await round.until(() => round.pushes().length === 2 && !round.machine.running)
      } finally {
        await round.dispose()
      }
    },
    60000
  )

  test.each(TELEMETRY_KINDS)(
    '%s replaced node slot ignores a late old observation',
    async (kind) => {
      const round = await established(
        { kind },
        { subscription: { requestedMetrics: ['com.example.', 'org.apache.kafka.'] } }
      )
      try {
        await round.cluster.refreshMetadata()
        const poolOwner = round.cluster.brokerPool
        const broker = Object.values(poolOwner.brokers)[0]
        const oldPool = broker.connectionPool
        const oldSlot = oldPool.telemetrySlot
        expect(oldSlot).toEqual(expect.any(Object))
        const gate = round.wireSchedule.hold({ api: 'ApiVersions' })
        round.gates.push(gate)
        const observed = []
        round.patch(round.metrics, 'observeRequest', (original) => (slot, duration) => {
          observed.push({ slot, duration })
          return original.call(round.metrics, slot, duration)
        })
        const pending = broker.apiVersions()
        round.pending.push(pending.catch(() => {}))
        await round.wait(() => gate.arrived())
        poolOwner.retireTelemetryNode(oldPool)
        oldPool.retire()
        round.retiredCorePools.push(oldPool)
        const replacementPool = await poolOwner.buildConnectionPool({
          host: oldPool.host,
          port: oldPool.port,
        })
        const replacement = poolOwner.createBroker({
          logger: poolOwner.rootLogger,
          versions: poolOwner.versions,
          connectionPool: replacementPool,
          nodeId: broker.nodeId,
        })
        poolOwner.brokers[broker.nodeId] = replacement
        poolOwner.bindTelemetryNode(replacement)
        const newSlot = replacementPool.telemetrySlot
        expect(newSlot).not.toBe(oldSlot)
        // Rebinding the connection is adversarial: completion must retain its entry-time slot.
        for (const connection of oldPool.pool)
          connection.setTelemetryObserver(round.metrics, newSlot, replacementPool)
        round.clock.jump(round.clock.now() + 9000)
        await replacement.connect()
        await round.drainReleased(gate)
        await pending
        expect(observed).toContainEqual({ slot: oldSlot, duration: 9000 })
        expect(observed).not.toContainEqual({ slot: newSlot, duration: 9000 })
        await oldPool.destroy({ drainTimeout: 0 })
        round.tick()
        const count = round.pushes().length
        await round.until(() => round.pushes().length > count && !round.machine.running)
        const bytes = Buffer.from(round.pushes().at(-1).fields.metrics, 'hex')
        const latency = round
          .decodePoints(bytes)
          .filter((p) => p.name.includes('node.request.latency'))
        expect(latency.length).toBeGreaterThan(0)
        expect(latency.every((p) => p.value === 0)).toBe(true)
      } finally {
        await round.dispose()
      }
    },
    60000
  )
})

describe.each([false, true])('telemetry and public commits enforce=%s', (enforceRequestTimeout) => {
  test.each(['classic-eager', 'classic-cooperative', 'consumer'])(
    '%s refuses an old pending commit and admits the successor offset',
    async (protocol) => {
      const round = new PublicTelemetryRound({ kind: 'consumer', protocol, enforceRequestTimeout })
      const telemetry = round.hold('S10')
      try {
        await round.start({ drive: 'normal' })
        await round.wait(() => telemetry.arrived())
        await round.until(() => round.core.handlers.length === 1)
        const offsets = (offset) => [{ topic: 'test-topic', partition: 0, offset }]
        await round.client.commitOffsets(offsets('0'))
        const schedule = new (require('../../../testHelpers/schedule').Schedule)()
        const gate = schedule.hold({ boundary: 'public-commit' })
        round.gates.push(gate)
        round.patch(
          require('../../network/requestQueue').prototype,
          'sendSocketRequest',
          (original) =>
            function (request) {
              if (request.entry.apiKey !== 8 || gate.call) return original.call(this, request)
              const continuation = schedule
                .run({ boundary: 'public-commit' }, () => undefined)
                .then(() => original.call(this, request))
              round.pending.push(continuation.catch((error) => request.entry.reject(error)))
            }
        )
        const pending = telemetryOutcome(() => round.client.commitOffsets(offsets('1')))
        await round.wait(() => gate.arrived())
        const token = round.runner.consumerGroup.ownership.token()
        const joining = round.rejoin()
        round.pending.push(joining.catch(() => {}))
        await round.until(() => round.runner.consumerGroup.ownership.refusal(token) !== null)
        await round.drainReleased(gate)
        const result = await pending
        expect(result.kind).toBe('reject')
        expect(result.reason).toBeInstanceOf(require('../../errors').KafkaJSCommitFailedError)
        await joining
        await round.drainReleased(telemetry)
        await round.client.commitOffsets(offsets('1'))
        const commits = round.wire.requests().filter((row) => row.api === 'OffsetCommit')
        expect(
          commits.map((row) =>
            row.fields.topics.flatMap((topic) => topic.partitions.map((p) => p.offset))
          )
        ).toEqual([['0'], ['1']])
        expect(round.core.handlers).toEqual(['0'])
        expect(round.runner.running).toBe(true)
      } finally {
        await round.dispose()
      }
    },
    60000
  )
})

describe('corrective public reopen and suspension', () => {
  const answered = (round, api) =>
    round.wire.requests().filter((r) => r.api === api && r.answeredOrder !== undefined)
  /** Reopens while modelling the broker's independent cache retirement order.
   * @param {string} kind public client kind @param {number} failures cache rejections
   * @param {string} [retirement] beforeGet, afterGet or afterPush
   * @returns {Promise<PublicTelemetryRound>} caller-owned replacement round
   * @throws {Error} original startup/close/reconnect failure after cleanup
   */
  const reopened = async (kind, failures, retirement = 'afterGet') => {
    const round = await established({ kind, enforceRequestTimeout: true })
    try {
      let remaining = failures
      await round.wait(() => round.beginClose(round.closeOptions(1000)))
      await round.settleClose()
      round.cache = { present: true, events: [] }
      const retire = () => {
        round.cache.present = false
        round.cache.events.push('retire')
      }
      if (retirement === 'beforeGet') retire()
      round.wire.answer('GetTelemetrySubscriptions', (fields) => {
        round.cache.events.push('Get')
        round.cache.present = true
        if (retirement === 'afterGet' && remaining > 0) retire()
        return { ...round.subscription, clientInstanceId: fields.clientInstanceId }
      })
      round.wire.answer('PushTelemetry', (fields) => {
        if (fields.terminating) return {}
        round.cache.events.push('Push')
        if (retirement === 'afterPush' && remaining > 0) retire()
        const refused = !round.cache.present && remaining-- > 0
        return { errorCode: refused ? 42 : 0 }
      })
      await round.wait(() => round.client.connect())
      return round
    } catch (error) {
      await round.dispose().catch(() => {})
      throw error
    }
  }
  const getThenPush = async (round) => {
    const gets = answered(round, 'GetTelemetrySubscriptions').length
    round.tick()
    await round.until(
      () => answered(round, 'GetTelemetrySubscriptions').length > gets && !round.machine.running
    )
    round.tick()
  }

  test.each(TELEMETRY_KINDS)(
    '%s immediate public reopen works with retirement before Get',
    async (kind) => {
      const round = await reopened(kind, 0, 'beforeGet')
      try {
        const uuid = round.machine.uuid
        expect(round.machine.reopen).toMatchObject({ used: false })
        const dedicated = round.dedicatedCount()
        await getThenPush(round)
        await round.until(() => round.machine.reopen === null)
        expect(round.machine.uuid).toBe(uuid)
        expect(round.dedicatedCount()).toBeGreaterThan(dedicated - 1)
        expect(round.cache.events.slice(0, 3)).toEqual(['retire', 'Get', 'Push'])
        expect(round.finalFrames()).toHaveLength(1)
      } finally {
        await round.dispose()
      }
    },
    60000
  )

  test.each(TELEMETRY_KINDS)(
    '%s immediate public reopen consumes one corrective allowance after Get',
    async (kind) => {
      const round = await reopened(kind, 1)
      try {
        await getThenPush(round)
        await round.until(() => round.machine.reopen?.used === true)
        expect(round.machine.state).toBe('BACKOFF')
        expect(round.cache.events.slice(0, 3)).toEqual(['Get', 'retire', 'Push'])
        await getThenPush(round)
        await round.until(() => round.machine.reopen === null)
        expect(answered(round, 'GetTelemetrySubscriptions').length).toBeGreaterThanOrEqual(3)
        expect(round.finalFrames()).toHaveLength(1)
      } finally {
        await round.dispose()
      }
    },
    60000
  )

  test.each(TELEMETRY_KINDS)(
    '%s immediate public reopen survives retirement after first Push',
    async (kind) => {
      const round = await reopened(kind, 1, 'afterPush')
      try {
        const uuid = round.machine.uuid
        const dedicated = round.dedicatedCount()
        await getThenPush(round)
        await round.until(() => round.machine.reopen?.used === true)
        await getThenPush(round)
        await round.until(() => round.machine.reopen === null)
        expect(round.dedicatedCount()).toBeGreaterThan(dedicated)
        expect(round.cache.events.slice(0, 3)).toEqual(['Get', 'Push', 'retire'])
        expect(round.machine.uuid).toBe(uuid)
        expect(round.unhandled).toEqual([])
      } finally {
        await round.dispose()
      }
    },
    60000
  )

  test.each(TELEMETRY_KINDS)(
    '%s repeated corrective rejection stops without replenishment',
    async (kind) => {
      const round = await reopened(kind, 2)
      try {
        await getThenPush(round)
        await round.until(() => round.machine.reopen?.used === true)
        await getThenPush(round)
        await round.until(() => round.machine.state === 'STOPPED')
        const count = round.wire.requests().length
        for (let turn = 0; turn < 100; turn++) await new Promise(setImmediate)
        expect(round.wire.requests()).toHaveLength(count)
        expect(round.finalFrames()).toHaveLength(1)
      } finally {
        await round.dispose()
      }
    },
    60000
  )

  test.each(TELEMETRY_KINDS.flatMap((kind) => [117, 76].map((code) => ({ kind, code }))))(
    '$kind refresh $code does not replenish the consumed corrective allowance',
    async ({ kind, code }) => {
      const round = await reopened(kind, 1)
      try {
        await getThenPush(round)
        await round.until(() => round.machine.reopen?.used === true)
        const revision = round.machine.reopen.revision
        let refresh = true
        round.wire.answer('PushTelemetry', (fields) => {
          if (fields.terminating) return {}
          const errorCode = refresh ? code : 42
          refresh = false
          return { errorCode }
        })
        await getThenPush(round)
        await round.until(() => !round.machine.running && round.machine.state === 'BACKOFF')
        expect(round.machine.reopen).toMatchObject({ revision, used: true })
        await getThenPush(round)
        await round.until(() => round.machine.state === 'STOPPED')
        expect(round.machine.reopen?.used ?? true).toBe(true)
        expect(round.machine.timer).toBeNull()
      } finally {
        await round.dispose()
      }
    },
    60000
  )

  test.each(
    TELEMETRY_KINDS.flatMap((kind) =>
      ['retirement', 'backoff', 'Get', 'Push'].map((stage) => ({ kind, stage }))
    )
  )(
    '$kind close and reopen revoke corrective $stage',
    async ({ kind, stage }) => {
      const round = await reopened(kind, 1)
      try {
        let gate
        if (stage === 'retirement') {
          gate = round.hold('S18', { mode: 'normal', retirementResultOnly: true })
        }
        if (stage === 'Get') gate = round.hold('S09', { mode: 'get' })
        if (stage === 'Push') gate = round.hold('S13', { mode: 'normal' })
        if (stage === 'retirement') round.arm()
        await getThenPush(round)
        await round.until(() => round.machine.reopen?.used === true)
        const old = round.capture()
        let callback
        if (stage === 'backoff') callback = round.clock.callback(round.machine.timer)
        if (stage === 'Get') {
          round.arm()
          round.tick()
          await round.wait(() => gate.arrived())
        }
        if (stage === 'Push') {
          const gets = answered(round, 'GetTelemetrySubscriptions').length
          round.tick()
          await round.until(
            () =>
              answered(round, 'GetTelemetrySubscriptions').length > gets && !round.machine.running
          )
          round.arm()
          round.tick()
          await round.wait(() => gate.arrived())
        }
        if (stage === 'retirement') await round.wait(() => gate.arrived())
        await round.wait(() => round.beginClose(round.closeOptions(0)))
        await round.settleClose()
        await round.wait(() => round.client.connect())
        const replacement = round.capture()
        expect(replacement.lifecycle).not.toBe(old.lifecycle)
        callback?.()
        if (gate) await round.drainReleased(gate)
        round.assertReplacementUnchanged(replacement)
        expect(round.finalFrames()).toHaveLength(1)
        await round.establish()
        expect(round.machine.reopen).toBeNull()
      } finally {
        await round.dispose()
      }
    },
    60000
  )

  test.each(TELEMETRY_KINDS)(
    '%s established ordinary invalid request has no corrective exception',
    async (kind) => {
      const round = await established({ kind })
      try {
        round.wire.answer('PushTelemetry', () => ({ errorCode: 42 }))
        round.tick()
        await round.until(() => round.machine.state === 'STOPPED')
        expect(round.machine.reopen).toBeNull()
        expect(round.finalFrames()).toEqual([])
      } finally {
        await round.dispose()
      }
    },
    60000
  )

  test.each(['consumer', 'shareConsumer'])(
    '%s suspension settles independent uncached waiters before replacement',
    async (kind) => {
      const round = new PublicTelemetryRound({ kind })
      try {
        const gate = round.hold('S09', { mode: 'get' })
        await round.wait(() => round.start({ drive: 'get' }))
        await round.wait(() => gate.arrived())
        const settled = []
        for (const timeout of [10, 30])
          round.pending.push(
            round.client.clientInstanceId({ timeout }).then(
              (value) => settled.push(value),
              (error) => settled.push(error.reason)
            )
          )
        round.clock.jump(11)
        await round.wait(() => round.crash())
        await round.until(() => settled.length === 2)
        expect(settled).toEqual(['timeout', 'not-connected'])
        expect(round.clock.pending.size).toBe(0)
        await expect(round.client.clientInstanceId({ timeout: 10 })).rejects.toMatchObject({
          reason: 'not-connected',
        })
        await round.wait(() => round.client.connect())
        const caller = round.client.clientInstanceId({ timeout: 1000 })
        await round.drainReleased(gate)
        round.tick()
        await expect(caller).resolves.toBe(TELEMETRY_UUID)
        await expect(round.client.clientInstanceId({ timeout: 0 })).resolves.toBe(TELEMETRY_UUID)
      } finally {
        await round.dispose()
      }
    },
    60000
  )

  test.each(['consumer', 'shareConsumer'])(
    '%s public stop keeps telemetry alive without a final',
    async (kind) => {
      const round = await established({ kind })
      try {
        const uuid = round.machine.uuid
        await round.wait(() => round.client.stop())
        expect(round.machine.connected).toBe(true)
        const pushes = round.pushes().length
        round.tick()
        await round.until(() => round.pushes().length > pushes)
        expect(round.machine.uuid).toBe(uuid)
        expect(round.finalFrames()).toEqual([])
      } finally {
        await round.dispose()
      }
    },
    60000
  )
})

describe('first public resume', () => {
  test.each([false, true])(
    'producer connect resumes telemetry exactly as enabled=%s',
    async (enabled) => {
      const ClientTelemetry = require('../client')
      const resume = jest.spyOn(ClientTelemetry.prototype, 'resume')
      const round = new PublicTelemetryRound({ kind: 'producer', enabled })
      try {
        await round.start()
        expect(resume).toHaveBeenCalledTimes(enabled ? 1 : 0)
        if (!enabled) expect(round.telemetryRequests()).toEqual([])
      } finally {
        resume.mockRestore()
        await round.dispose()
      }
    },
    60000
  )
})

describe('resource and connect continuation authority', () => {
  test.each(['consumer', 'shareConsumer'])(
    'S23 %s replaces the real member while a sample is held',
    async (kind) => {
      const round = await established({ kind })
      const gate = round.hold('S10', { mode: 'normal' })
      try {
        round.arm()
        round.tick()
        await round.wait(() => gate.arrived())
        const resources = round.binding.captureResources()
        const frames = round.trace.frames.length
        await round.rejoin()
        expect(resources.isCurrent()).toBe(false)
        expect(round.binding.captureResources().values).toEqual(resources.values)
        await round.drainReleased(gate)
        expect(round.trace.frames).toHaveLength(frames)
        const count = round.pushes().length
        round.tick()
        await round.until(() => round.pushes().length > count && !round.machine.running)
        round.assertCore(0, 'rejoin')
      } finally {
        await round.dispose()
      }
    },
    60000
  )

  test.each([false, true])(
    'S24 connect/close race preserves the original connect result enabled=%s',
    async (enabled) => {
      const round = new PublicTelemetryRound({ kind: 'producer', enabled })
      const ClientTelemetry = require('../client')
      const resume = jest.spyOn(ClientTelemetry.prototype, 'resume')
      try {
        await round.start()
        await round.wait(() => round.client.disconnect({ timeout: 0 }))
        const Generation = require('../../producer/generation')
        let admitted = false
        let release
        const gate = new Promise((resolve) => {
          release = resolve
        })
        round.patch(
          Generation.prototype,
          'wait',
          (original) =>
            async function (...args) {
              const value = await original.apply(this, args)
              if (!admitted) {
                admitted = true
                await gate
              }
              return value
            }
        )
        const before = resume.mock.calls.length
        const connecting = round.client.connect()
        round.pending.push(connecting.catch(() => {}))
        await round.until(() => admitted)
        const closing = round.client.disconnect({ timeout: 0 })
        release()
        await expect(connecting).resolves.toBeUndefined()
        await closing
        expect(resume.mock.calls.length).toBe(before)
        if (!enabled) expect(round.telemetryRequests()).toEqual([])
      } finally {
        resume.mockRestore()
        await round.dispose()
      }
    },
    60000
  )
})

describe('round clock restoration', () => {
  test.each([false, true])(
    'restores wall and monotonic sources after startup failure=%s',
    async (fail) => {
      const wall = Date.now
      const monotonic = process.hrtime.bigint
      const performanceNow = require('node:perf_hooks').performance.now
      const round = new PublicTelemetryRound({ kind: 'producer' })
      const reason = new Error('fixture startup failure')
      try {
        const starting = round.start({
          beforeConnect: () => {
            if (fail) throw reason
          },
        })
        if (fail) await expect(starting).rejects.toBe(reason)
        else await starting
      } finally {
        await round.dispose()
      }
      expect(Date.now).toBe(wall)
      expect(process.hrtime.bigint).toBe(monotonic)
      expect(require('node:perf_hooks').performance.now).toBe(performanceNow)
    },
    60000
  )
})
