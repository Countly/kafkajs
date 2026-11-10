const {
  PublicTelemetryRound,
  TELEMETRY_KINDS,
  TELEMETRY_UUID,
  DEFAULT_SUBSCRIPTION,
} = require('../../../testHelpers/lifecycle')
const { KafkaJSInvalidConfigurationError } = require('../../errors')

const TIMEOUT_KINDS = TELEMETRY_KINDS
const FINAL_BOUNDARIES = ['S10', 'S11', 'S12', 'S13', 'S14', 'S15', 'S16', 'S17', 'S18']

/**
 * Reads the resource attribute strings of a captured Push with an independent protobuf reader.
 * @param {object} record wire record of a PushTelemetry request @returns {Record<string,string>}
 */
const resourceOf = (record) => {
  const read = (bytes) => {
    let at = 0
    const out = []
    const varint = () => {
      let value = 0
      let factor = 1
      for (;;) {
        const byte = bytes[at++]
        value += (byte & 127) * factor
        if (byte < 128) return value
        factor *= 128
      }
    }
    while (at < bytes.length) {
      const tag = varint()
      const size = tag % 8 === 2 ? varint() : tag % 8 === 1 ? 8 : 0
      out.push({ number: Math.floor(tag / 8), value: size ? bytes.subarray(at, at + size) : 0 })
      at += size
    }
    return out
  }
  const only = (bytes, number) => read(bytes).filter((f) => f.number === number)
  const metrics = Buffer.from(record.fields.metrics, 'hex')
  const resource = only(metrics, 1)[0] && only(only(metrics, 1)[0].value, 1)[0]
  const result = {}
  if (resource)
    for (const { value } of only(resource.value, 1))
      result[only(value, 1)[0].value.toString()] = only(
        only(value, 2)[0].value,
        1
      )[0].value.toString()
  return result
}

/**
 * Establishes a round, arms one hold and begins the actual public close.
 * @param {object} cell round options plus the hold @param {{boundary?:string,mode?:string,timeout?:number,
 *   start?:object}} [hold] @returns {Promise<{round:PublicTelemetryRound,gate:object,before:object}>}
 */
const closing = async (cell, { boundary, mode = 'final', timeout = 1000, start } = {}) => {
  const round = new PublicTelemetryRound(cell)
  try {
    const gate = boundary ? round.hold(boundary, { mode }) : null
    await round.start({
      ...(boundary === 'S12'
        ? { points: 'large', subscription: { acceptedCompressionTypes: [1, 0] } }
        : {}),
      ...start,
    })
    await round.ready()
    round.arm()
    const before = round.capture()
    if (boundary === 'S15') await round.occupyFinalQueue()
    round.closePromise = round.beginClose(
      timeout === undefined ? undefined : round.closeOptions(timeout)
    )
    if (boundary === 'S18') await round.expireForRetirement()
    if (gate) await round.wait(() => gate.arrived())
    if (boundary === 'S15')
      await round.until(() =>
        round.dedicatedConnection.requestQueue.pending.some(
          (request) => request.entry.apiKey === 72
        )
      )
    if (boundary === 'S12') {
      expect(round.codecJobs).toHaveLength(1)
      expect(round.codecJobs[0].raw.length).toBeGreaterThan(1000)
      expect(gate.settle).not.toBeNull()
    }
    return { round, gate, before }
  } catch (error) {
    await round.dispose().catch(() => {})
    throw error
  }
}

describe.each([false, true])('captured close with enforcement %s', (enforceRequestTimeout) => {
  describe.each(TELEMETRY_KINDS)('%s', (kind) => {
    const cell = { kind, enforceRequestTimeout }

    test('C01 distinct closing authority permits one captured final', async () => {
      const { round, gate, before } = await closing(cell, { boundary: 'S13' })
      try {
        const episode = round.capture()
        expect(episode.lifecycle).not.toBe(before.lifecycle)
        expect(episode.closeRevision).toEqual(expect.any(BigInt))
        expect(episode.closeDeadline).toBeLessThanOrEqual(episode.closeEntry + 1000)
        expect(round.savedOrdinaryGuard()).not.toBeNull()
        expect(round.savedClosingGuard()).toBeNull()
        await round.drainReleased(gate)
        await round.settleClose()
        const finals = round.finalFrames()
        expect(finals).toHaveLength(1)
        expect(finals[0].fields).toMatchObject({
          terminating: true,
          clientInstanceId: TELEMETRY_UUID,
          subscriptionId: DEFAULT_SUBSCRIPTION.subscriptionId,
        })
        expect(round.newSetupCallsSince(before)).toEqual([])
        expect(round.capture().written).toBe(before.written + 1)
      } finally {
        await round.dispose()
      }
    }, 60000)

    test('C02 ordinary authority never authorizes a final', async () => {
      const round = new PublicTelemetryRound(cell)
      const gate = round.hold('S13', { mode: 'normal' })
      try {
        await round.wait(() => round.start({ drive: 'normal' }))
        await round.wait(() => gate.arrived())
        round.closePromise = round.beginClose(round.closeOptions(1000))
        expect(round.savedOrdinaryGuard()).not.toBeNull()
        await round.drainReleased(gate)
        expect(round.finalFrames()).toHaveLength(1)
        await round.settleClose()
        expect(round.pushes()).toHaveLength(1)
        expect(round.finalFrames()).toHaveLength(1)
      } finally {
        await round.dispose()
      }
    }, 60000)

    test('C03 zero budget never starts a final', async () => {
      const round = new PublicTelemetryRound(cell)
      try {
        await round.start()
        await round.ready()
        const before = round.capture()
        await round.wait(() => round.beginClose(round.closeOptions(0)))
        expect(round.finalFrames()).toEqual([])
        expect(round.newSetupCallsSince(before)).toEqual([])
        expect(round.machine.uuid).toBe(TELEMETRY_UUID)
      } finally {
        await round.dispose()
      }
    }, 60000)

    test.each([7, 20])(
      'C03 an original T20 budget already spent %s ms is never restarted',
      async (spent) => {
        const round = new PublicTelemetryRound(cell)
        const Payload = require('../payload')
        try {
          await round.start()
          await round.ready()
          const before = round.capture()
          let builds = 0
          round.patch(
            Payload.prototype,
            'buildPayload',
            (original) =>
              function (...args) {
                builds++
                return original.apply(this, args)
              }
          )
          let checked = false
          round.patch(
            round.binding,
            'close',
            (original) =>
              function (budget) {
                if (!checked) {
                  checked = true
                  round.clock.jump(round.clock.now() + spent)
                  expect(budget.remaining()).toBe(20 - spent)
                }
                return original.call(this, budget)
              }
          )
          const entry = round.clock.now()
          await round.wait(() => round.beginClose(round.closeOptions(20)))
          expect(round.machine.closing.deadline).toBe(entry + 20)
          expect(round.machine.closing.deadline - round.closeEntry).toBeLessThanOrEqual(20)
          expect(round.finalFrames()).toHaveLength(spent === 20 ? 0 : 1)
          expect(builds).toBe(spent === 20 ? 0 : 1)
          expect(round.newSetupCallsSince(before)).toEqual([])
          expect(round.machine.uuid).toBe(TELEMETRY_UUID)
          for (const write of round.socketWrites.filter(
            (write) => write.bytes.readInt16BE(4) === 72 && write.time >= entry
          ))
            expect(write.time).toBeLessThan(round.machine.closing.deadline)
        } finally {
          await round.dispose()
        }
      },
      60000
    )

    test.each([
      ['no accepted ID', {}],
      ['empty request array', { subscription: { requestedMetrics: [] } }],
    ])(
      'C03 %s skips the final',
      async (_name, start) => {
        const round = new PublicTelemetryRound(cell)
        try {
          await round.start(start)
          if (start.subscription) {
            round.tick()
            await round.until(() => round.machine.subscription !== null && !round.machine.running)
          }
          await round.wait(() => round.beginClose(round.closeOptions(1000)))
          expect(round.finalFrames()).toEqual([])
        } finally {
          await round.dispose()
        }
      },
      60000
    )

    test('C03 a nonempty unmatched requested prefix sends a zero-blob final', async () => {
      const round = new PublicTelemetryRound(cell)
      try {
        await round.start({ subscription: { requestedMetrics: ['org.unmatched.'] } })
        await round.ready()
        await round.wait(() => round.beginClose(round.closeOptions(1000)))
        const finals = round.finalFrames()
        expect(finals).toHaveLength(1)
        expect(finals[0].fields.metrics).toBe('')
      } finally {
        await round.dispose()
      }
    }, 60000)

    test('C04 a repeated close after settlement sends no second final and reopen sends one more', async () => {
      const round = new PublicTelemetryRound(cell)
      try {
        await round.start()
        await round.ready()
        await round.wait(() => round.beginClose(round.closeOptions(1000)))
        expect(round.finalFrames()).toHaveLength(1)
        await round.wait(() => round.client.disconnect(round.closeOptions(1000)))
        expect(round.finalFrames()).toHaveLength(1)
        await round.wait(() => round.client.connect())
        await round.wait(() => round.beginClose(round.closeOptions(1000)))
        expect(round.finalFrames()).toHaveLength(1)
        await round.wait(() => round.client.connect())
        await round.establish()
        await round.wait(() => round.beginClose(round.closeOptions(1000)))
        expect(round.finalFrames()).toHaveLength(2)
      } finally {
        await round.dispose()
      }
    }, 60000)

    test('C05 an unsent ordinary push is revoked and a written one only delays the final', async () => {
      const { round, gate } = await closing(cell, { boundary: 'S15' })
      try {
        expect(round.finalFrames()).toEqual([])
        const written = round.capture().written
        await round.drainReleased(gate)
        await round.settleClose()
        expect(round.finalFrames()).toHaveLength(1)
        expect(round.capture().written).toBe(written + 1)
      } finally {
        await round.dispose()
      }
    }, 60000)

    test.each([0, 117, 76, 118, 89, 35, 42, 87])(
      'C06 final rejection %p never refreshes retries or reconnects',
      async (code) => {
        const round = new PublicTelemetryRound(cell)
        try {
          await round.start()
          await round.ready()
          round.wire.answer('PushTelemetry', (fields) =>
            fields.terminating ? { errorCode: code } : {}
          )
          const before = round.capture()
          await round.wait(() => round.beginClose(round.closeOptions(1000)))
          expect(round.finalFrames()).toHaveLength(1)
          expect(round.newSetupCallsSince(before)).toEqual([])
          expect(round.pushes()).toHaveLength(1)
          expect(round.machine.state).toBe('CLOSED')
        } finally {
          await round.dispose()
        }
      },
      60000
    )

    test.each(['transport-refused', 'response-lost', 'decode-failure', 'blackhole'])(
      'C06 final %s never retries or reconnects',
      async (fault) => {
        const round = new PublicTelemetryRound(cell)
        const gate = round.hold('S13', { mode: 'final' })
        try {
          await round.start({ beforeConnect: (r) => r.useChaos() })
          await round.ready()
          round.arm()
          const before = round.capture()
          if (fault === 'decode-failure') {
            const Connection = require('../../network/connection')
            round.patch(
              Connection.prototype,
              'send',
              (original) =>
                function (options) {
                  if (this === round.dedicatedConnection && options.request.apiKey === 72)
                    options = {
                      ...options,
                      response: {
                        ...options.response,
                        decode: async () => {
                          throw new Error('private decode')
                        },
                      },
                    }
                  return original.call(this, options)
                }
            )
          }
          round.closePromise = round.beginClose(round.closeOptions(1000))
          await round.wait(() => gate.arrived())
          const socket = round.dedicatedConnection.socket
          if (fault === 'transport-refused') round.faultDedicated()
          if (fault === 'response-lost') socket.dropResponses(1, { apiKey: 72 })
          if (fault === 'blackhole') socket.blackhole()
          await round.drainReleased(
            gate,
            undefined,
            fault === 'response-lost' || fault === 'blackhole'
              ? () => round.until(() => round.machine.closing.attempted)
              : undefined
          )
          if (fault === 'response-lost' || fault === 'blackhole') {
            expect(round.closeDone).toBe(false)
            expect(round.machine.closing.attempted).toBe(true)
            round.clock.jump(round.machine.closing.deadline)
            await round.deliverCloseWatchdog()
          }
          await round.settleClose()
          expect(round.finalFrames()).toHaveLength(
            ['transport-refused', 'blackhole'].includes(fault) ? 0 : 1
          )
          expect(round.written.length - before.written).toBe(fault === 'transport-refused' ? 0 : 1)
          expect(round.newSetupCallsSince(before)).toEqual([])
          expect(round.machine.state).toBe('CLOSED')
          expect(round.unhandled).toEqual([])
        } finally {
          await round.dispose()
        }
      },
      60000
    )

    test.each(FINAL_BOUNDARIES)(
      'C07 a held final expires at %s',
      async (boundary) => {
        const { round, gate } = await closing(cell, { boundary })
        try {
          const episode = round.capture()
          round.clock.jump(episode.closeDeadline + 1)
          await round.drainReleased(gate)
          await round.deliverCloseWatchdog()
          await round.settleClose()
          expect(round.finalFrames()).toHaveLength(['S17', 'S18'].includes(boundary) ? 1 : 0)
          expect(round.forcedRetirements().length).toBeGreaterThan(0)
        } finally {
          await round.dispose()
        }
      },
      60000
    )

    test('C08 reopen before old final completion leaves replacement intact', async () => {
      const { round, gate } = await closing(cell, { boundary: 'S17' })
      try {
        const episode = round.capture()
        round.clock.jump(episode.closeDeadline + 1)
        await round.deliverCloseWatchdog()
        await round.settleClose()
        await round.wait(() => round.client.connect())
        await round.establish()
        const replacement = round.capture()
        await round.drainReleased(gate)
        round.assertReplacementUnchanged(replacement)
        expect(round.finalFrames()).toHaveLength(1)
      } finally {
        await round.dispose()
      }
    }, 60000)

    test('C10 a final never authenticates or adopts a new socket', async () => {
      const round = new PublicTelemetryRound(cell)
      try {
        await round.start({ sasl: true })
        await round.ready()
        round.armRenewal()
        const before = round.capture()
        await round.wait(() => round.beginClose(round.closeOptions(1000)))
        expect(round.finalFrames()).toEqual([])
        expect(round.newSetupCallsSince(before)).toEqual([])
      } finally {
        await round.dispose()
      }
    }, 60000)

    test.each(['disabled', 'dormant', 'stopped', 'disconnected'])(
      'C03 %s skips final encoding and setup',
      async (state) => {
        const round = new PublicTelemetryRound({ ...cell, enabled: state !== 'disabled' })
        try {
          await round.start()
          if (state === 'stopped' || state === 'disconnected') await round.ready()
          if (state === 'stopped') {
            round.wire.answer('PushTelemetry', () => ({ errorCode: 42 }))
            round.tick()
            await round.until(() => round.machine.state === 'STOPPED')
          }
          if (state === 'disconnected') await round.wait(() => round.beginClose({ timeout: 0 }))
          const frames = round.finalFrames().length
          const setup = round.setupCalls.length
          const samples = round.snapshots.length
          await round.wait(() => round.beginClose({ timeout: 1000 }))
          expect(round.finalFrames()).toHaveLength(frames)
          expect(round.setupCalls).toHaveLength(setup)
          expect(round.snapshots).toHaveLength(samples)
        } finally {
          await round.dispose()
        }
      },
      60000
    )

    test.each(['S14', 'S16'])(
      'C10 authentication becomes due after final encoding at %s',
      async (boundary) => {
        const { round, gate, before } = await closing(cell, { boundary, start: { sasl: true } })
        try {
          expect(round.savedClosingGuard()).toBeNull()
          round.armRenewal()
          await round.drainReleased(gate)
          await round.settleClose()
          expect(round.finalFrames()).toEqual([])
          expect(round.newSetupCallsSince(before)).toEqual([])
        } finally {
          await round.dispose()
        }
      },
      60000
    )

    test('C12 close frees all owned work with the original result', async () => {
      const round = new PublicTelemetryRound(cell)
      try {
        await round.start()
        await round.ready()
        await expect(
          round.wait(() => round.beginClose(round.closeOptions(1000)))
        ).resolves.toBeUndefined()
        expect(round.machine.state).toBe('CLOSED')
        expect(round.machine.waiters.size).toBe(0)
        expect(round.clock.pending.size).toBe(0)
        expect(round.forcedRetirements().length).toBeGreaterThan(0)
      } finally {
        await round.dispose()
      }
    }, 60000)
  })

  describe.each(TIMEOUT_KINDS)('%s with a numeric budget', (kind) => {
    const cell = { kind, enforceRequestTimeout }

    test('C04 the first valid close owns concurrent and repeated disconnects', async () => {
      const { round, gate } = await closing(cell, { boundary: 'S13', timeout: 20 })
      try {
        const episode = round.machine.closing
        const second = round.client.disconnect({ timeout: 5000 })
        const third = round.client.disconnect({ timeout: 0 })
        await expect(round.client.disconnect({ timeout: -1 })).rejects.toBeInstanceOf(
          KafkaJSInvalidConfigurationError
        )
        expect(round.machine.closing).toBe(episode)
        expect(episode.deadline - round.closeEntry).toBeLessThanOrEqual(20)
        await round.drainReleased(gate)
        await Promise.all([round.closePromise, second, third])
      } finally {
        await round.dispose()
      }
    }, 60000)

    test('C05 a response after the original cap yields no final', async () => {
      const { round, gate } = await closing(cell, { boundary: 'S15', timeout: 20 })
      try {
        round.clock.jump(round.capture().closeDeadline + 1)
        await round.drainReleased(gate)
        await round.deliverCloseWatchdog()
        await round.settleClose()
        expect(round.finalFrames()).toEqual([])
      } finally {
        await round.dispose()
      }
    }, 60000)
  })

  test.each(
    TELEMETRY_KINDS.flatMap((kind) => {
      const stages =
        kind === 'consumer'
          ? ['handler', 'hook', 'commit', 'leave', 'cluster']
          : kind === 'shareConsumer'
            ? ['handler', 'ack', 'leave', 'cluster']
            : kind === 'producer'
              ? ['delivery', 'transaction', 'cluster']
              : ['request', 'cluster']
      return stages.flatMap((stage) => [false, true].map((enabled) => ({ kind, stage, enabled })))
    })
  )(
    'C09 $kind core $stage drains concurrently enabled=$enabled',
    async ({ kind, stage, enabled }) => {
      const round = new PublicTelemetryRound({ kind, enforceRequestTimeout, enabled })
      const { Schedule } = require('../../../testHelpers/schedule')
      const schedule = new Schedule()
      const core = schedule.hold({ boundary: stage })
      round.gates.push(core)
      let calls = 0
      const hold = (target, key) =>
        round.patch(
          target,
          key,
          (original) =>
            async function (...args) {
              calls++
              await schedule.run({ boundary: stage }, () => undefined)
              return original.apply(this, args)
            }
        )
      let pending
      try {
        const gate = enabled ? round.hold('S13', { mode: 'final' }) : null
        await round.start({
          client: kind === 'producer' ? { transactionalId: 'close-tx', linger: 0 } : {},
          beforeConnect: (r) => {
            if (kind === 'consumer')
              r.runOptions = { rebalanceListener: { onPartitionsRevoked: async () => {} } }
            if (stage === 'handler') {
              r.runOptions = {
                ...r.runOptions,
                eachMessage: async () => {
                  calls++
                  await schedule.run({ boundary: stage }, () => undefined)
                },
              }
              if (kind === 'shareConsumer') {
                const {
                  A,
                  part,
                  acquired,
                  recordsHex,
                  fetchAnswer,
                } = require('../../../testHelpers/shareHarness')
                r.harness.script('ShareFetch', [
                  fetchAnswer([
                    part({
                      topicId: A,
                      partition: 0,
                      acquire: [acquired(0, 0)],
                      records: recordsHex(0, ['held core handler']),
                    }),
                  ]),
                ])
              }
            }
          },
        })
        if (enabled) await round.ready()
        if (stage === 'cluster') hold(round.cluster, 'disconnect')
        else if (kind === 'consumer') {
          const runner = round.runner
          if (stage === 'handler') await round.wait(() => core.arrived())
          if (stage === 'hook') hold(runner, 'invokeCloseHook')
          if (stage === 'commit') {
            runner.autoCommit = true
            runner.consumerGroup.offsetManager.resolveOffset({
              topic: 'test-topic',
              partition: 0,
              offset: '0',
            })
            hold(runner.consumerGroup, 'commitOffsets')
          }
          if (stage === 'leave') hold(runner.consumerGroup, 'leave')
        } else if (kind === 'shareConsumer') {
          if (stage === 'handler') await round.wait(() => core.arrived())
          else if (stage === 'ack') {
            await round.until(() => round.runner.current.nodes.size > 0)
            hold(
              require('../../consumer/share/shareFetchLoop').ShareFetchLoop.prototype,
              'sendFinal'
            )
          } else hold(round.runner.current, 'leave')
        } else {
          const Broker = require('../../broker')
          const method =
            stage === 'transaction' ? 'endTxn' : kind === 'producer' ? 'produce' : 'metadata'
          hold(kind === 'admin' ? round.cluster : Broker.prototype, method)
          if (stage === 'transaction') {
            const transaction = await round.client.transaction()
            await transaction.send({ topic: 'test-topic', messages: [{ value: 'close' }] })
            pending = transaction.commit()
          } else if (kind === 'producer')
            pending = round.client.send({ topic: 'test-topic', messages: [{ value: 'close' }] })
          else pending = round.client.fetchTopicMetadata({ topics: ['test-topic'] })
          round.pending.push(pending.catch(() => {}))
          await round.wait(() => core.arrived())
        }
        round.arm()
        const entry = round.clock.now()
        round.closePromise = round.beginClose({ timeout: 1000, leaveGroup: true })
        if (gate) await round.wait(() => gate.arrived())
        await round.wait(() => core.arrived())
        expect(round.closeDone).toBe(false)
        expect(calls).toBe(1)
        if (enabled) expect(round.machine.closing.deadline).toBe(entry + 1000)
        if (gate) await round.drainReleased(gate)
        expect(round.closeDone).toBe(false)
        core.release()
        await round.settleClose()
        await pending
        expect(calls).toBe(1)
        expect(round.finalFrames()).toHaveLength(enabled ? 1 : 0)
        if (kind === 'consumer' && stage === 'commit') {
          const rows = round.wire.requests()
          const commits = rows.filter((row) => row.api === 'OffsetCommit')
          expect(commits).toHaveLength(1)
          expect(
            commits[0].fields.topics.flatMap((topic) =>
              topic.partitions.map(({ partition, offset }) => ({ partition, offset }))
            )
          ).toEqual([{ partition: 0, offset: '1' }])
          const leave = rows.find((row) => row.api === 'LeaveGroup')
          expect(leave).toBeDefined()
          expect(commits[0].order).toBeLessThan(leave.order)
          expect(commits[0].answeredOrder).toBeLessThan(leave.order)
        }
        expect(round.unhandled).toEqual([])
      } finally {
        await round.dispose()
      }
    },
    60000
  )

  test('C11 the frozen final carries the captured member strings', async () => {
    const round = new PublicTelemetryRound({
      kind: 'consumer',
      enforceRequestTimeout,
      membership: 'static',
    })
    const gate = round.hold('S13', { mode: 'final' })
    try {
      await round.start()
      await round.ready()
      const ordinaryResources = round.binding.captureResources()
      const captured = ordinaryResources.values
      round.arm()
      round.closePromise = round.beginClose({ timeout: 1000, leaveGroup: true })
      await round.wait(() => gate.arrived())
      await round.until(() => round.wire.requests().some((row) => row.api === 'LeaveGroup'))
      await round.until(() => !ordinaryResources.isCurrent())
      await round.drainReleased(gate)
      await round.settleClose()
      const [final] = round.finalFrames()
      expect(resourceOf(final)).toMatchObject({ group_id: 'telemetry-g' })
      expect(resourceOf(final).group_member_id).toBe(captured.group_member_id)
      expect(resourceOf(final).group_instance_id).toBe('static-instance')
    } finally {
      await round.dispose()
    }
    const manual = new PublicTelemetryRound({
      kind: 'consumer',
      enforceRequestTimeout,
      protocol: 'manual',
      membership: 'no-group',
    })
    try {
      await manual.start()
      await manual.ready()
      await manual.wait(() => manual.beginClose({ timeout: 1000 }))
      const keys = Object.keys(resourceOf(manual.finalFrames()[0]))
      expect(keys).not.toContain('group_id')
      expect(keys).not.toContain('group_member_id')
    } finally {
      await manual.dispose()
    }
  }, 60000)
  test('C11 share resources omit static membership and preserve the captured member at close', async () => {
    const round = new PublicTelemetryRound({ kind: 'shareConsumer', enforceRequestTimeout })
    const gate = round.hold('S13', { mode: 'final' })
    try {
      await round.start()
      await round.ready()
      const ordinaryResources = round.binding.captureResources()
      const captured = ordinaryResources.values
      round.arm()
      round.closePromise = round.beginClose({ timeout: 1000 })
      await round.wait(() => gate.arrived())
      await round.until(() =>
        round.wire
          .requests()
          .some((row) => row.api === 'ShareGroupHeartbeat' && row.fields.memberEpoch < 0)
      )
      expect(ordinaryResources.isCurrent()).toBe(false)
      await round.drainReleased(gate)
      await round.settleClose()
      const resource = resourceOf(round.finalFrames()[0])
      expect(resource.group_member_id).toBe(captured.group_member_id)
      expect(resource.group_id).toBe('telemetry-g')
      expect(resource).not.toHaveProperty('group_instance_id')
    } finally {
      await round.dispose()
    }
  }, 60000)

  test.each(TELEMETRY_KINDS)(
    'C11 %s invalid resources at entry authorize no final',
    async (kind) => {
      const round = new PublicTelemetryRound({ kind, enforceRequestTimeout })
      try {
        await round.start()
        await round.ready()
        round.binding.setResourceReader(() => ({ values: {}, isCurrent: () => false }))
        await round.wait(() => round.beginClose(round.closeOptions(1000)))
        expect(round.finalFrames()).toEqual([])
      } finally {
        await round.dispose()
      }
    },
    60000
  )

  test.each(TELEMETRY_KINDS)(
    'C09 %s preserves the original natural core close rejection',
    async (kind) => {
      const reason = new Error('original core close rejection')
      const outcomes = []
      for (const enabled of [false, true]) {
        const round = new PublicTelemetryRound({ kind, enabled, enforceRequestTimeout })
        try {
          await round.start()
          if (enabled) await round.ready()
          let calls = 0
          round.patch(
            round.cluster,
            'disconnect',
            (original) =>
              async function (...args) {
                calls++
                await original.apply(this, args)
                throw reason
              }
          )
          const restoreFailure = round.restores.at(-1)
          const { telemetryOutcome } = require('../../../testHelpers/lifecycle')
          try {
            outcomes.push(await telemetryOutcome(() => round.client.disconnect()))
          } finally {
            restoreFailure()
          }
          expect(calls).toBe(1)
          expect(round.unhandled).toEqual([])
        } finally {
          await round.dispose()
        }
      }
      expect(outcomes[1].kind).toBe(outcomes[0].kind)
      if (outcomes[0].kind === 'return') expect(outcomes[1].value).toBe(outcomes[0].value)
      else {
        expect(outcomes[0].reason).toBe(reason)
        expect(outcomes[1].reason).toBe(reason)
      }
    },
    60000
  )
})
