const { PublicTelemetryRound, TELEMETRY_KINDS, TELEMETRY_UUID } = require('../lifecycle')

describe('public telemetry round setup', () => {
  test.each(TELEMETRY_KINDS)('%s reaches a complete ordinary exchange', async (kind) => {
    const round = new PublicTelemetryRound({ kind })
    try {
      await round.start()
      if (kind === 'shareConsumer') expect(round.runner.current.membership.memberEpoch).toBe(1)
      await round.ready()
      expect(round.machine.state).toBe('READY')
      expect(round.machine.running).toBeNull()
      expect(round.machine.uuid).toBe(TELEMETRY_UUID)
      expect(round.pushesAnswered()).toBe(1)
      expect(round.clock.pending.has(round.machine.timer)).toBe(true)
      expect(round.wire.sockets().every((socket) => !socket.destroyed)).toBe(true)
      round.wire.assertClean()
    } finally {
      await round.dispose()
    }
  })

  test('neutralizes vector throttles and preserves explicit response fields and bytes', async () => {
    const round = new PublicTelemetryRound({ kind: 'producer' })
    try {
      await round.start()
      const pair = { api: 'Metadata' }
      const record = { fields: {} }
      round.wire.answer('Metadata', () => ({ marker: 1 }))
      expect(round.wire.defaultAnswer(pair, record)).toEqual({ throttleTime: 0, marker: 1 })
      round.wire.answer('Metadata', async () => ({ throttleTime: 25, marker: 2 }))
      await expect(round.wire.defaultAnswer(pair, record)).resolves.toEqual({
        throttleTime: 25,
        marker: 2,
      })
      const bytes = Buffer.from([1, 2, 3])
      round.wire.answer('Metadata', () => bytes)
      expect(round.wire.defaultAnswer(pair, record)).toBe(bytes)
    } finally {
      await round.dispose()
    }
  })

  test('reports a broken wire precondition before waiting for a boundary', async () => {
    const round = new PublicTelemetryRound({ kind: 'producer' })
    const reason = new Error('wire precondition')
    round.wire = {
      assertClean: () => {
        throw reason
      },
    }
    try {
      await expect(round.until(() => false)).rejects.toBe(reason)
    } finally {
      round.wire = null
      await round.dispose()
    }
  })

  test('rejects readiness immediately when telemetry construction failed', async () => {
    const round = new PublicTelemetryRound({ kind: 'producer' })
    try {
      await expect(round.ready()).rejects.toThrow(
        'Enabled round must construct its telemetry machine'
      )
    } finally {
      await round.dispose()
    }
  })

  test.each(TELEMETRY_KINDS)(
    '%s restores every global patch after an assertion fails with ordinary work held',
    async (kind) => {
      const crypto = require('node:crypto')
      const Connection = require('../../src/network/connection')
      const { TelemetryFactory } = require('../../src/telemetry/factory')
      const { ShareRun } = require('../../src/consumer/share/shareRun')
      const { ShareRunner } = require('../../src/consumer/share/shareRunner')
      const identities = () => [
        crypto.randomBytes,
        Date.now,
        process.hrtime.bigint,
        require('node:perf_hooks').performance.now,
        global.performance.now,
        setTimeout,
        clearTimeout,
        Connection.prototype.send,
        TelemetryFactory.prototype.bind,
        ShareRun.prototype.createLoop,
        ShareRunner.prototype.api,
      ]
      const originals = identities()
      const listeners = process.listeners('unhandledRejection')
      const reason = new Error('deliberate assertion failure')
      const round = new PublicTelemetryRound({ kind })
      const failingCase = async () => {
        try {
          const gate = round.hold('S10')
          await round.start({ drive: 'normal' })
          await round.wait(() => gate.arrived())
          throw reason
        } finally {
          await round.dispose()
        }
      }
      await expect(failingCase()).rejects.toBe(reason)
      expect(identities()).toEqual(originals)
      expect(process.listeners('unhandledRejection')).toEqual(listeners)
    }
  )
})
