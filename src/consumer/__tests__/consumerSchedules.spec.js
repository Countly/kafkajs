const { useSharedFakeClock } = require('./fixtures/sharedFakeClock')
const rows = require('./fixtures/consumerSchedules.json')
const { interleavings } = require('../../../testHelpers/schedule')
const { lifecycleCells } = require('../../../testHelpers/lifecycle')
const { ConsumerScheduleEpisode } = require('./fixtures/scheduleEpisode')

/** Closed issue-time window used by the attribution controls. */
class IssueWindow {
  /**
   * @param {{issuedAt: number, commandReturnedAt: number, restoredAt: number, sessionMs: number}} arm
   * @returns {IssueWindow} actual, rather than nominal, outage classification
   */
  constructor(arm) {
    Object.assign(this, arm)
  }

  /** @param {number} at event time @returns {boolean} whether the arm owns the event */
  contains(at) {
    return at >= this.issuedAt && at < this.restoredAt
  }

  /** @returns {boolean} whether actual outage falls outside the inherited short-arm fraction */
  isLong() {
    return this.restoredAt - this.issuedAt >= 0.8 * this.sessionMs
  }
}

describe('Consumer schedule contracts', () => {
  test('declares every protocol and feature before running the transport episodes', () => {
    expect(rows).toHaveLength(31)
    expect(new Set(rows.map((row) => row.name)).size).toBe(31)
    for (const protocol of ['classic-eager', 'classic-cooperative', 'consumer']) {
      expect(rows.filter((row) => row.protocol === protocol)).toHaveLength(9)
    }
    for (const row of rows) {
      expect(row.config.autoCommit).toBe(false)
      expect(row.config.explicitCommit).toBe('processed-current-owned')
      expect(row.replay.log).toBe('n/a-fixed-corpus')
      expect(row.replay.committed).toBe('actual-broker-observed')
      expect(row.steps.at(-1)).toBe('close')
      expect(row.expectedWire.afterLearnedLossOffsetCommit).toBe(0)
    }
  })
  test('generates manual and all callback boundaries without impossible membership cells', () => {
    const cells = lifecycleCells()
    expect(cells.some((cell) => cell.protocol === 'manual')).toBe(true)
    for (const boundary of ['hook:revoked', 'hook:assigned', 'hook:lost']) {
      expect(cells.some((cell) => cell.boundary === boundary)).toBe(true)
    }
    const manual = cells.filter((cell) => cell.protocol === 'manual')
    expect(manual.every((cell) => ['grouped', 'no-group'].includes(cell.membership))).toBe(true)
    expect(manual.some((cell) => cell.boundary.startsWith('hook:'))).toBe(false)
    expect(manual.some((cell) => cell.boundary === 'in-heartbeat')).toBe(false)
    expect(
      cells
        .filter((cell) => cell.membership === 'no-group')
        .every((cell) => cell.protocol === 'manual' && !cell.boundary.startsWith('commit:'))
    ).toBe(true)
  })
  test.each([1, 2])('enumerates all five-action permutations for %i partitions', (partitions) => {
    const actions = ['hook-release', 'answer', 'rebalance', 'stop', 'wake']
    const orders = interleavings(actions, { limit: 120 })
    expect(orders).toHaveLength(120)
    expect(new Set(orders.map((order) => order.join(','))).size).toBe(120)
    expect(partitions).toBeGreaterThan(0)
  })
  test('keeps the inherited short-arm fraction when the outage approaches the session', () => {
    const arm = new IssueWindow({
      issuedAt: 0,
      commandReturnedAt: 4000,
      restoredAt: 5000,
      sessionMs: 6000,
    })
    expect(arm.isLong()).toBe(true)
  })
  test('attributes an event while a bouncer command is held for six seconds', () => {
    const arm = new IssueWindow({
      issuedAt: 1000,
      commandReturnedAt: 7000,
      restoredAt: 8000,
      sessionMs: 6000,
    })
    expect(arm.contains(2000)).toBe(true)
    expect(arm.contains(8000)).toBe(false)
    expect(arm.isLong()).toBe(true)
    expect(arm.contains(999)).toBe(false)
  })
})

/** @returns {Array<[string, object, object]>} every row with each session setting and manual grouping it can run in */
const variants = () =>
  rows.flatMap((row) =>
    [row.config.fetchSessions, !row.config.fetchSessions].flatMap((sessionMode) =>
      (row.protocol === 'manual' ? [true, false] : [true]).map((groupedManual) => [
        `${row.name} sessions=${sessionMode}${row.protocol === 'manual' ? ` grouped=${groupedManual}` : ''}`,
        row,
        { sessionMode, groupedManual },
      ])
    )
  )

describe('Consumer schedule episodes on the real wire', () => {
  beforeEach(useSharedFakeClock)
  afterEach(() => {
    jest.useRealTimers()
    jest.restoreAllMocks()
  })
  test.each(variants())('executes %s', async (_name, row, options) => {
    await new ConsumerScheduleEpisode(row, options).execute()
  })
})

const ACTIONS = ['hook-release', 'answer', 'rebalance', 'stop', 'wake']
const MODES = ['classic-eager', 'classic-cooperative', 'consumer']

/**
 * @param {string} protocol one of MODES
 * @param {number[]} order indices into ACTIONS
 * @returns {object} a schedule row that runs the actions in that order after the first assignment
 *   holds its assigned hook
 */
const interleavingRow = (protocol, order) => ({
  name: `${protocol}/interleaving ${order.map((index) => ACTIONS[index]).join('>')}`,
  faultPoint: 'hook:assigned',
  protocol,
  feature: 'interleaving',
  expectedWire: { afterLearnedLossOffsetCommit: 0, overlappingOwners: 0, convergenceRounds: 6 },
  config: { autoCommit: false, explicitCommit: 'processed-current-owned', fetchSessions: false },
  replay: {
    log: 'n/a-fixed-corpus',
    committed: 'actual-broker-observed',
    handler: 'crash',
    interval: { from: '0', to: '1', incarnation: 'incarnation-a' },
  },
  steps: [
    'acquire',
    'hold',
    ...order.map((index) => ACTIONS[index]),
    'release',
    'quiesce',
    'close',
  ],
})

describe.each(MODES)('Every ordering of the five rebalance actions on %s', (protocol) => {
  beforeEach(useSharedFakeClock)
  afterEach(() => {
    jest.useRealTimers()
    jest.restoreAllMocks()
  })
  const orders = interleavings(ACTIONS, { limit: 120 })
  test.each([1, 2])('runs all 120 orders with %i partitions', async (partitions) => {
    expect(orders).toHaveLength(120)
    for (const order of orders) {
      const row = interleavingRow(protocol, order)
      try {
        await new ConsumerScheduleEpisode(row, { partitions }).execute()
      } catch (error) {
        error.message = `${JSON.stringify({ row, partitions, order })}\n${error.message}`
        throw error
      }
    }
  })
})

describe('Consumer schedule controls', () => {
  beforeEach(useSharedFakeClock)
  afterEach(() => {
    jest.useRealTimers()
    jest.restoreAllMocks()
  })
  const row = (name) => rows.find((candidate) => candidate.name === name)

  test('fails an unknown action instead of skipping it', async () => {
    const episode = new ConsumerScheduleEpisode({
      ...row('classic-eager/stop-during-round'),
      steps: ['acquire', 'vanish', 'close'],
    })
    await expect(episode.execute()).rejects.toThrow('Unknown schedule action: vanish')
  })
  test('fails a row whose hold was never reached', async () => {
    const stale = {
      ...row('classic-eager/reset-seek-commit-resolution'),
      steps: ['acquire', 'hold', 'close'],
    }
    await expect(new ConsumerScheduleEpisode(stale).execute()).rejects.toThrow()
  })
  test('fails a redelivery that the row did not declare', async () => {
    const base = row('classic-eager/epoch-during-handler')
    const undeclared = {
      ...base,
      steps: base.steps.map((step) => (step === 'epoch' ? 'loss' : step)),
    }
    await expect(new ConsumerScheduleEpisode(undeclared).execute()).rejects.toThrow(
      'Undeclared replay of incarnation-a|'
    )
  })
})
