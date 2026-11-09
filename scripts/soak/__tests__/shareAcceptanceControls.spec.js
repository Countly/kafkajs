const { ShareSoak, SHARE_SOAK } = require('../soak')
const { verifyFiles } = require('../../evidence/verify')
const { CleanupStack } = require('../../../src/producer/__tests__/fixtures/liveProbe')
const { LiveRun } = require('../../../src/producer/__tests__/fixtures/bounded')

/** Replays source observations through the original timestamp owner and serializing recorder. */
class AcceptanceRecording {
  /**
   * @param {boolean} short selects the unchanged declared phase spans and sample cadence
   */
  constructor(short = true) {
    this.now = 7654321
    this.origin = this.now
    this.rows = []
    this.owner = new ShareSoak({
      short,
      kafka: {},
      driver: {},
      stack: new CleanupStack(),
      gate: new LiveRun(),
      clock: { now: () => this.now },
      shares: {
        snapshot: async () => {
          await Promise.resolve()
          this.now = this.origin + this.elapsed
          return {
            heapUsed: this.heap,
            queueDepth: 0,
            shareSessions: 0,
            shareRecords: 0,
            shareRanges: 0,
            sockets: { open: 0 },
            activeResources: {},
            activeResourcesBeforeCollection: {},
            namedListeners: {},
          }
        },
        restore: () => {},
      },
      evidence: {
        write: (row) => this.rows.push(JSON.parse(JSON.stringify(row))),
        read: () => this.rows,
      },
    })
    this.owner.startedAt = this.origin
    this.run = { warmupMs: SHARE_SOAK.warmupMs, samples: [], phaseEnds: [] }
  }

  /**
   * Completes one asynchronous source read before assigning the owner's original elapsed time.
   * @param {string} kind original observation kind
   * @param {number} elapsed original elapsed milliseconds
   * @param {number} heap measured retained bytes
   * @param {object} [fields] phase and original periodic times
   * @returns {Promise<object>} original complete snapshot
   */
  async observe(kind, elapsed, heap, fields = {}) {
    this.elapsed = elapsed
    this.heap = heap
    const snapshot = await this.owner.snapshot()
    expect(snapshot.heapAtMs).toBe(elapsed)
    const line = { kind, ...fields, snapshot }
    this.owner.recordResources(line)
    if (kind === 'sample') this.run.samples.push(line)
    else if (kind === 'phase-end') this.run.phaseEnds.push(line)
    else this.run[kind] = snapshot
    return snapshot
  }

  /**
   * Builds complete short/full source-shaped spans, without running a live workload or clock.
   * @param {object} options growth and observation spacing
   * @param {number} options.rate retained bytes per minute
   * @param {boolean} [options.firstOnly] plateau after the first scored phase
   * @param {boolean} [options.irregular] irregular translated elapsed observations
   * @param {number} [options.lastByte] extra byte at the last quiet point
   * @returns {Promise<void>} all observations archived
   */
  async record({ rate, firstOnly = false, irregular = false, lastByte = 0 }) {
    const mode = this.owner.short ? 'short' : 'full'
    const phaseMs = SHARE_SOAK.slotMs[mode] * 10
    const grid = (time) => Math.ceil(time / 1875) * 1875
    let start = irregular ? 1234560000 : 0
    const firstEnd = grid(start + phaseMs + SHARE_SOAK.settleMs)
    const retained = (time) =>
      10000000 + (rate * (firstOnly ? Math.min(time, firstEnd) : time)) / 60000
    await this.observe('baseline', start, 10000000)
    for (const [index, phase] of ['share-steady', 'share-reset', 'share-members'].entries()) {
      for (let ms = SHARE_SOAK.sampleMs[mode]; ms < phaseMs; ms += SHARE_SOAK.sampleMs[mode]) {
        const time = grid(start + ms)
        await this.observe('sample', time, retained(time) + index * 12000000, {
          phase,
          atMs: time,
          phaseMs: time - start,
        })
      }
      const end = grid(start + phaseMs + SHARE_SOAK.settleMs)
      await this.observe('phase-end', end, retained(end) + (index === 2 ? lastByte : 0), { phase })
      start = end + (irregular ? [1875, 5625, 3750][index] : 0)
    }
    await this.observe('teardown', grid(start + 1875), retained(grid(start + 1875)))
  }

  /**
   * Runs the selected owner's unchanged judge and archives its original verdict.
   * @returns {object} original resource verdict
   */
  verdict() {
    const verdict = this.owner.soak().judge(this.run)
    this.owner.recordResources({ kind: 'verdict', ...verdict })
    return verdict
  }

  /**
   * Validates serialized resource rows through the authenticated stamp/scenario entrypoint.
   * @returns {string[]} independently recomputed problems
   */
  verify() {
    const verified = { tree: 'a'.repeat(40), node: process.version, brokerVersion: '4.3.1' }
    const rows = this.rows.map((row) => ({
      ...verified,
      at: '2026-11-09T00:00:00Z',
      lane: 'docker',
      step: 'soak',
      wireVersions: {},
      config: {},
      ...row,
    }))
    return verifyFiles(['share.jsonl'], verified, () =>
      rows.map((row) => JSON.stringify(row)).join('\n')
    ).problems
  }
}

describe('recorded share acceptance sensitivity', () => {
  test.each([true, false])('rejects recorded sustained 128 KiB/min, short=%s', async (short) => {
    const recording = new AcceptanceRecording(short)
    await recording.record({ rate: 131072 })
    const verdict = recording.verdict()
    expect(verdict.measured.quietSlope.bytesPerMinute).toBe(131072)
    expect(verdict.measured.phases.every((phase) => phase.slopeBytesPerMinute < 262144)).toBe(true)
    expect(verdict.measured.phases.every((phase) => phase.endpointBytes < 2097152)).toBe(true)
    expect(verdict.violations).toEqual(
      short
        ? ['phase-end quiet heap slope screen']
        : ['phase-end quiet heap screen', 'phase-end quiet heap slope screen']
    )
    expect(recording.verify().join('\n')).toContain('phase-end quiet heap slope screen')
    expect(recording.owner.startedAt).toBe(recording.origin)
  })

  test('retains recorded first-scored-phase 300 KiB/min failure with a later quiet plateau', async () => {
    const recording = new AcceptanceRecording()
    await recording.record({ rate: 307200, firstOnly: true })
    const verdict = recording.verdict()
    expect(verdict.measured.phases[0].slopeBytesPerMinute).toBeCloseTo(307200, 6)
    expect(verdict.measured.quietSlope.bytesPerMinute).toBe(0)
    expect(verdict.violations).toEqual(['share-steady: retained-heap screen'])
    expect(recording.verify().join('\n')).toContain('share resource verdict is absent or failed')
  })

  test.each([0, 1])(
    'compares recorded exact-bound heaps without rounding, last byte=%s',
    async (lastByte) => {
      const recording = new AcceptanceRecording()
      await recording.record({ rate: 98304, irregular: true, lastByte })
      const verdict = recording.verdict()
      expect(verdict.ok).toBe(lastByte === 0)
      expect(Math.round(verdict.measured.quietSlope.bytesPerMinute)).toBe(98304)
      if (lastByte)
        expect(recording.verify().join('\n')).toContain('phase-end quiet heap slope screen')
      else expect(recording.verify()).toEqual([])
      expect(recording.rows.filter(({ kind }) => kind === 'sample')).toHaveLength(237)
    }
  )

  test.each([
    'missing time',
    'nonfinite time',
    'duplicate time',
    'reversed time',
    'missing phase',
    'duplicate phase',
    'reversed phases',
    'missing heap',
    'invalid heap',
  ])('rejects damaged recorded evidence: %s', async (damage) => {
    const recording = new AcceptanceRecording()
    await recording.record({ rate: 0, irregular: true })
    const verdict = recording.verdict()
    expect(verdict.ok).toBe(true)
    const rows = recording.rows
    const ends = rows.filter(({ kind }) => kind === 'phase-end')
    if (damage === 'missing time') delete ends[1].snapshot.heapAtMs
    if (damage === 'nonfinite time') ends[1].snapshot.heapAtMs = Infinity
    if (damage === 'duplicate time') ends[1].snapshot.heapAtMs = ends[0].snapshot.heapAtMs
    if (damage === 'reversed time') ends[1].snapshot.heapAtMs = ends[0].snapshot.heapAtMs - 1
    if (damage === 'missing phase') rows.splice(rows.indexOf(ends[1]), 1)
    if (damage === 'duplicate phase') rows.splice(rows.indexOf(ends[1]), 0, ends[1])
    if (damage === 'reversed phases')
      [ends[0].phase, ends[1].phase] = [ends[1].phase, ends[0].phase]
    if (damage === 'missing heap') delete ends[1].snapshot.heapUsed
    if (damage === 'invalid heap') ends[1].snapshot.heapUsed = -1
    expect(recording.verify().join('\n')).toContain('phase-end quiet heap slope screen')
  })
})
