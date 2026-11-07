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
  const actions = (boundary, mode) => {
    if (boundary === 'S18') return mode === 'final' ? 3 : 2
    return mode === 'final' ? 4 : RELEASE_ACTIONS.length
  }
  const expected = Object.entries(modes).reduce(
    (sum, [mode, ids]) => sum + ids.reduce((inner, id) => inner + actions(id, mode), 0),
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
  test.each([...new Set(cells.map((cell) => `${cell.boundary}/${cell.mode}`))])(
    '%s resolves to an installable source hook',
    async (key) => {
      const [boundary, mode] = key.split('/')
      const round = new PublicTelemetryRound({ kind: 'producer' })
      try {
        expect(round.hold(boundary, { mode })).toBeDefined()
        expect(() => round.hold(boundary, { mode })).toThrow('Duplicate telemetry hold')
      } finally {
        await round.dispose()
      }
    }
  )
})

describe('stale work release matrix', () => {
  test.each(releaseCells)(
    '$name',
    async (cell) => {
      const round = new PublicTelemetryRound(cell)
      try {
        const gate = round.hold(cell.boundary, { mode: cell.mode })
        await round.wait(() => round.start(cell.start))
        if (!cell.lateArrival) await round.wait(() => gate.arrived())
        const old = round.capture()
        await round.wait(() => round.transition(cell.action, cell.options))
        if (cell.lateArrival) await round.wait(() => gate.arrived())
        const replacement = round.capture()
        await round.wait(() => round.drainReleased(gate, cell.releaseValue))
        round.assertTrace(cell.expectation(old, replacement))
        if (cell.action.includes('replacement') || cell.action.includes('reopen'))
          round.assertReplacementUnchanged(replacement)
        expect(round.unhandled).toEqual([])
      } finally {
        await round.dispose()
      }
    },
    60000
  )
})

const established = async (options, start) => {
  const round = new PublicTelemetryRound(options)
  await round.start(start)
  await round.ready()
  return round
}

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

  test.each(['consumer', 'shareConsumer'])(
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
        await round.wait(() => round.crash())
        await round.until(() => settled.length === 2)
        expect(settled).toEqual(['timeout', 'not-connected'])
        await round.wait(() => round.client.connect())
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

  test.each(TELEMETRY_KINDS.filter((kind) => kind !== 'admin'))(
    '%s close watchdog uses original deadline under delayed delivery',
    async (kind) => {
      for (const timeout of [20, undefined]) {
        const round = new PublicTelemetryRound({ kind })
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

  test.each(['consumer', 'shareConsumer'])(
    '%s retry preserves uncached caller deadlines',
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
        round.clock.jump(5)
        round.faultDedicated()
        await round.drainReleased(gate)
        expect(round.machine.waiters.size).toBe(2)
        round.clock.jump(11)
        round.clock.deliver(round.machine.waiterTimer)
        await round.until(() => settled.length === 1)
        expect(settled).toEqual(['timeout'])
        round.clock.jump(30)
        round.clock.deliver(round.machine.waiterTimer)
        await round.until(() => settled.length === 2)
        expect(settled).toEqual(['timeout', 'timeout'])
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
      try {
        const revisions = [round.machine.subscription.revision]
        const answers = [9, 7]
        round.wire.answer('GetTelemetrySubscriptions', (fields) => ({
          ...round.subscription,
          subscriptionId: answers[0],
          clientInstanceId: fields.clientInstanceId ?? TELEMETRY_UUID,
        }))
        for (const next of answers) {
          round.wire.answer('PushTelemetry', () => ({ errorCode: 117 }))
          round.tick()
          await round.until(() => round.machine.state === 'BACKOFF')
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
        const slotOf = () =>
          Object.values(round.binding.cluster.brokerPool.brokers)[0]?.connectionPool.telemetrySlot
        const old = slotOf()
        await round.wait(() => round.beginClose(round.closeOptions(0)))
        await round.settleClose()
        await round.wait(() => round.client.connect())
        expect(slotOf()).not.toBe(old)
        round.metrics.observeRequest(old, 9000)
        await round.establish()
        const points = round.snapshots.flatMap((entry) => entry.snapshot?.points ?? [])
        expect(points.some((p) => p.name.includes('node.request.latency'))).toBe(true)
        expect(points.map((p) => p.value)).not.toContain(9000)
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
  const reopened = async (kind, failures) => {
    const round = await established({ kind, enforceRequestTimeout: true })
    let remaining = failures
    await round.wait(() => round.beginClose(round.closeOptions(1000)))
    await round.settleClose()
    round.wire.answer('PushTelemetry', (fields) =>
      !fields.terminating && remaining-- > 0 ? { errorCode: 42 } : {}
    )
    await round.wait(() => round.client.connect())
    return round
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
      const round = await reopened(kind, 0)
      try {
        const uuid = round.machine.uuid
        expect(round.machine.reopen).toMatchObject({ used: false })
        const dedicated = round.dedicatedCount()
        await getThenPush(round)
        await round.until(() => round.machine.reopen === null)
        expect(round.machine.uuid).toBe(uuid)
        expect(round.dedicatedCount()).toBeGreaterThan(dedicated - 1)
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
      const round = await reopened(kind, 1)
      try {
        const uuid = round.machine.uuid
        const dedicated = round.dedicatedCount()
        await getThenPush(round)
        await round.until(() => round.machine.reopen?.used === true)
        await getThenPush(round)
        await round.until(() => round.machine.reopen === null)
        expect(round.dedicatedCount()).toBeGreaterThan(dedicated)
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

  test.each(TELEMETRY_KINDS)(
    '%s close and reopen revoke each corrective continuation',
    async (kind) => {
      const round = await reopened(kind, 1)
      try {
        await getThenPush(round)
        await round.until(() => round.machine.reopen?.used === true)
        const callback = round.clock.callback(round.machine.timer)
        await round.wait(() => round.beginClose(round.closeOptions(0)))
        await round.settleClose()
        await round.wait(() => round.client.connect())
        const replacement = round.capture()
        callback()
        for (let turn = 0; turn < 50; turn++) await new Promise(setImmediate)
        expect(round.capture().requests).toBe(replacement.requests)
        round.assertReplacementUnchanged(replacement)
        expect(round.finalFrames()).toHaveLength(1)
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
