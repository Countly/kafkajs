const {
  PublicTelemetryRound,
  TELEMETRY_KINDS,
  TELEMETRY_UUID,
  DEFAULT_SUBSCRIPTION,
} = require('../../../testHelpers/lifecycle')
const { KafkaJSInvalidConfigurationError } = require('../../errors')

const TIMEOUT_KINDS = TELEMETRY_KINDS.filter((kind) => kind !== 'admin')
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
  const gate = boundary ? round.hold(boundary, { mode }) : null
  await round.start(start)
  await round.ready()
  round.arm()
  const before = round.capture()
  if (boundary === 'S15') {
    const sent = round.pushes().length
    round.tick()
    await round.until(() => round.pushes().length > sent)
  }
  round.closePromise = round.beginClose(
    timeout === undefined ? undefined : round.closeOptions(timeout)
  )
  if (gate) await round.wait(() => gate.arrived())
  return { round, gate, before }
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
        await round.settleClose()
        expect(round.finalFrames()).toHaveLength(1)
        await round.drainReleased(gate)
        expect(round.pushes()).toHaveLength(1)
        expect(round.finalFrames()).toHaveLength(1)
      } finally {
        await round.dispose()
      }
    }, 60000)

    test('C03 zero and elapsed budgets never start a final', async () => {
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

    test.each([
      ['no accepted ID', {}],
      ['empty request array', { subscription: { requestedMetrics: [] } }],
    ])(
      'C03 %s skips the final',
      async (_name, start) => {
        const round = new PublicTelemetryRound(cell)
        try {
          await round.start(start)
          if (start.subscription) await round.ready().catch(() => {})
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

  test('C09 the core drain starts concurrently with the final work', async () => {
    const round = new PublicTelemetryRound({ kind: 'consumer', enforceRequestTimeout })
    const gate = round.hold('S13', { mode: 'final' })
    try {
      await round.start()
      await round.ready()
      round.arm()
      round.closePromise = round.beginClose({ timeout: 1000, leaveGroup: true })
      await round.wait(() => gate.arrived())
      await round.until(() => round.wire.requests().some((r) => r.api === 'LeaveGroup'))
      expect(round.closeDone).toBe(false)
      await round.drainReleased(gate)
      await round.settleClose()
      expect(round.finalFrames()).toHaveLength(1)
      expect(round.wire.requests().filter((r) => r.api === 'LeaveGroup')).toHaveLength(1)
    } finally {
      await round.dispose()
    }
  }, 60000)

  test('C11 the frozen final carries the captured member strings', async () => {
    const round = new PublicTelemetryRound({ kind: 'consumer', enforceRequestTimeout })
    try {
      await round.start()
      await round.ready()
      await round.wait(() => round.beginClose({ timeout: 1000, leaveGroup: true }))
      const [final] = round.finalFrames()
      expect(resourceOf(final)).toMatchObject({ group_id: 'telemetry-g' })
      expect(resourceOf(final).group_member_id).toEqual(expect.any(String))
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
})
