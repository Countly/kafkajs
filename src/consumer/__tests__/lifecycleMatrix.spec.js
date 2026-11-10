const { useSharedFakeClock } = require('./fixtures/sharedFakeClock')
const {
  lifecycleCells,
  coreLifecycleCells,
  runTelemetryLifecycleCell,
} = require('../../../testHelpers/lifecycle')
const committed = require('../../../testHelpers/lifecycleCells.json')
const { runLifecycleCell } = require('./fixtures/lifecycleCase')
const { runManualLifecycleCell } = require('./fixtures/manualLifecycleCase')

describe('Consumer lifecycle matrix', () => {
  test('cell catalogue is exactly the committed reviewed list', () => {
    const prior = coreLifecycleCells()
    const added = committed.slice(prior.length)
    expect(lifecycleCells()).toEqual(committed)
    expect(prior).toHaveLength(690)
    expect(committed.slice(0, prior.length)).toEqual(prior)
    expect(prior.some((cell) => cell.telemetry)).toBe(false)
    expect(committed).toHaveLength(prior.length + (6 * 8 + 2 * 7) * 2)
    expect(new Set(committed.map((cell) => JSON.stringify(cell))).size).toBe(committed.length)
    expect(added.every((cell) => cell.telemetry === true)).toBe(true)
    expect(added.filter((cell) => cell.event === 'rejoin' && cell.protocol === 'manual')).toEqual(
      []
    )
    expect(committed.filter((cell) => cell.protocol === 'manual' && !cell.telemetry)).toHaveLength(
      126
    )
  })
  test('every mandatory manual, hook, force and session cell is in the committed list', () => {
    const has = (event, boundary, protocol, membership) =>
      committed.some(
        (cell) =>
          cell.event === event &&
          cell.boundary === boundary &&
          cell.protocol === protocol &&
          cell.membership === membership
      )
    const events = ['stop', 'disconnect', 'force', 'crash', 'rejoin', 'poll-expiry']
    const protocols = ['classic-eager', 'classic-cooperative', 'consumer']
    const memberships = ['dynamic', 'static']
    const mandatory = []
    for (const boundary of [
      'hook:revoked',
      'hook:assigned',
      'hook:lost',
      'session:initial',
      'session:inflight',
    ])
      for (const event of events)
        for (const protocol of protocols)
          for (const membership of memberships)
            mandatory.push([event, boundary, protocol, membership])
    for (const protocol of protocols)
      for (const membership of memberships)
        for (const boundary of ['before-handler', 'in-handler', 'in-heartbeat', 'in-leave'])
          if (!(boundary === 'in-leave' && protocol !== 'consumer' && membership === 'static'))
            mandatory.push(['force', boundary, protocol, membership])
    for (const event of ['stop', 'disconnect', 'force', 'crash'])
      for (const membership of ['grouped', 'no-group'])
        for (const boundary of ['before-handler', 'in-handler', 'manual:apply', 'idle-wait'])
          mandatory.push([event, boundary, 'manual', membership])
    expect(mandatory.filter((cell) => !has(...cell))).toEqual([])
    expect(
      committed.filter((cell) => cell.protocol === 'manual' && cell.boundary.startsWith('hook:'))
    ).toEqual([])
  })
  describe.each(committed.filter((cell) => cell.telemetry))(
    '$event at $boundary on $protocol/$membership enforce=$enforceRequestTimeout',
    (cell) => {
      test('telemetry authority is fenced and the original close is preserved', async () => {
        await runTelemetryLifecycleCell(cell)
      }, 60000)
    }
  )
  describe.each(committed.filter((cell) => !cell.telemetry))(
    '$event at $boundary on $protocol/$membership',
    (cell) => {
      beforeEach(useSharedFakeClock)
      afterEach(() => {
        jest.useRealTimers()
        jest.restoreAllMocks()
      })
      test('no commit after loss, owed rejoin and settled teardown baseline', async () => {
        await (cell.protocol === 'manual' ? runManualLifecycleCell(cell) : runLifecycleCell(cell))
      })
    }
  )
})
