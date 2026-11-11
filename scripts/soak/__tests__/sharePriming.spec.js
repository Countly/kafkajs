const { EventEmitter } = require('events')
const Encoder = require('../../../src/protocol/encoder')
const { requestHeaderVersion } = require('../../../src/protocol/headers')
const {
  ShareSoak,
  SharePriming,
  ShareFetchTap,
  SHARE_SOAK,
  SHARE_SOAK_PRIMING,
} = require('../soak')
const { produceCodecs } = require('../shareProtocol')
const { primingReceiptHash } = require('../../evidence/sharePrimingLink')
const { verifySharePriming, verifyShareProtocol } = require('../../evidence/sharePriming')
const { verifyFiles } = require('../../evidence/verify')
const { CleanupStack } = require('../../../src/producer/__tests__/fixtures/liveProbe')
const { LiveRun, delay, liveRun } = require('../../../src/producer/__tests__/fixtures/bounded')
const { createCycleFacts } = require('./fixtures/shareCycleFacts')

/** @returns {object} Complete independent quiet source observation. */
const quiet = () => ({
  heapUsed: 10000000,
  queueDepth: 0,
  ledgerSize: 0,
  shareSessions: 0,
  shareRecords: 0,
  shareRanges: 0,
  shareClients: 0,
  sockets: { created: 0, destroyed: 0, open: 0 },
  listeners: {},
  namedListeners: {},
  activeResources: {},
  activeResourcesBeforeCollection: {},
  timerOwners: {
    beforeCollection: { rows: [], dropped: 0, unattributed: {} },
    afterCollection: { rows: [], dropped: 0, unattributed: {} },
  },
})

/**
 * Encodes an actual Produce v9 batch; other operations retain independently framed headers.
 * @param {number} apiKey Original API key.
 * @param {number} apiVersion Encoded version.
 * @param {number} correlationId Original correlation.
 * @param {number} [compression=0] Actual batch codec.
 * @returns {Promise<Buffer>} Original framed bytes.
 */
const request = async (apiKey, apiVersion, correlationId, compression = 0) => {
  const header = new Encoder()
    .writeInt16(apiKey)
    .writeInt16(apiVersion)
    .writeInt32(correlationId)
    .writeString('priming-control')
  if (requestHeaderVersion(apiKey, apiVersion) === 2) header.writeTaggedFields()
  if (apiKey === 0) {
    const produce = require('../../../src/protocol/requests/produce/v9/request')({
      acks: -1,
      timeout: 3000,
      compression,
      topicData: [
        {
          topic: 'control',
          partitions: [
            {
              partition: 0,
              messages: [
                { key: Buffer.from('k'), value: Buffer.from('v'), timestamp: '1700000000000' },
              ],
            },
          ],
        },
      ],
    })
    header.writeEncoder(await produce.encode())
  }
  return new Encoder().writeInt32(header.size()).writeEncoder(header).buffer
}

/**
 * Sends split original frames through the actual passive socket tap and serializing recorder.
 * @param {ShareFetchTap} tap Original call-through transport observer.
 * @returns {Promise<void>} Complete fixed header/codec coverage.
 */
const coverage = async (tap, endpoint = 'node:1', metadataVersion = 13) => {
  const calls = [
    ['send', 0, 9, 'producer'],
    ['send', 22, 4, 'producer'],
    ['readLog', 1, 12, 'raw'],
    ['fetchTopicOffsets', 2, 7],
    ['fetchTopicMetadata', 3, metadataVersion],
    ['connect', 10, 6],
    ['listGroups', 16, 5],
    ['connect', 18, 5],
    ['connect', 18, 4],
    ['createTopics', 19, 7],
    ['deleteTopics', 20, 6],
    ['describeConfigs', 32, 4],
    ['deleteGroups', 42, 2],
    ['incrementalAlterConfigs:0', 44, 1],
    ['incrementalAlterConfigs:1', 44, 1],
    ['describeCluster', 60, 2],
    ['run', 76, 1, 'share'],
    ['describeShareGroups', 77, 1],
    ['run', 78, 2, 'share'],
    ['disconnect', 79, 2, 'share'],
    ['listShareGroupOffsets:named', 90, 1],
    ['listShareGroupOffsets:all', 90, 1],
    ['listTopics', 3, metadataVersion],
  ]
  const socket = Object.assign(new EventEmitter(), { destroyed: false, write: () => false })
  tap.wrap(socket, endpoint)
  for (const [index, [operation, apiKey, version, role = 'admin']] of calls.entries()) {
    const bytes = await request(apiKey, version, index)
    tap.operation(role, operation, () => {
      expect(socket.write(bytes.subarray(0, 7))).toBe(false)
      expect(socket.write(bytes.subarray(7))).toBe(false)
      const body = new Encoder()
        .writeInt32(index)
        .writeInt16(apiKey === 18 && version === 5 ? 35 : 0)
      const response = new Encoder().writeInt32(body.size()).writeEncoder(body).buffer
      socket.emit('data', response.subarray(0, 5))
      socket.emit('data', response.subarray(5))
    })
  }
  socket.destroyed = true
  socket.emit('close', false)
}

/**
 * Executes the real two-scope owners, original Soak sampler, cycle oracles and completion logic.
 * Only cluster I/O and resource readings are scripted; elapsed schedule uses the Jest clock.
 * @param {object} [options] Selector, source-shaped growth and priming failure injection.
 * @returns {Promise<object>} Original owner, serializing archive and independent verdicts.
 */
const recording = async ({
  short = true,
  rate = 0,
  firstOnly = false,
  failPrime = false,
  failCleanup = false,
  damageBoundary,
  primeRate = 0,
  idleWakeMs = 0,
  heapOffset = 0,
  translatedMs = 0,
  damagePrime,
  separateCoordinators = false,
  scoredMetadataVersion = 13,
} = {}) => {
  const environment = { ...process.env }
  Object.assign(process.env, {
    KAFKAJS_LANE: 'docker',
    KAFKAJS_SOAK_PROFILE: short ? 'short' : 'full',
    [short ? 'SHARE_SOAK_E2E' : 'SHARE_SOAK_FULL']: '1',
  })
  jest.useFakeTimers({ now: 1700000000000, doNotFake: ['setImmediate'] })
  const rows = []
  const gate = new LiveRun()
  const clock = { now: () => Math.floor(performance.now()) + translatedMs }
  const tap = new ShareFetchTap(64, clock)
  let armedAt = null
  let phase = 0
  const shares = {
    snapshot: async () => {
      const point = quiet()
      point.heapUsed += heapOffset
      const now = performance.now()
      const elapsed = armedAt === null ? 0 : now - armedAt
      const limit = SHARE_SOAK.slotMs[short ? 'short' : 'full'] * 10
      point.heapUsed += Math.round(
        (rate * (firstOnly ? Math.min(elapsed, limit) : elapsed)) / 60000
      )
      if (armedAt === null) point.heapUsed += Math.round((primeRate * now) / 60000)
      if (damagePrime && !rows.some((r) => r.event === 'priming-complete')) damagePrime(point)
      return point
    },
    restore: jest.fn(),
  }
  const evidence = { write: (row) => rows.push(JSON.parse(JSON.stringify(row))), read: () => rows }
  const driver = (priming) => {
    const topic = priming ? 'soak-share-prime-control' : 'soak-share-control'
    const group = `${topic}-group`
    const uuid = priming
      ? '00000000-0000-4000-8000-000000000002'
      : '00000000-0000-4000-8000-000000000001'
    const facts = createCycleFacts({
      topic,
      group,
      uuid,
      memberPrefix: priming ? 'prime-' : 'score-',
    })
    return {
      tap,
      brokers: ['node:1'],
      prepare: jest.fn(async function (stack) {
        this.configuration = { topic, groupId: group, topicId: uuid }
        stack.add('joined original scope cleanup', async () => {
          this.completedAt = 100000000 + Math.floor(performance.now())
          this.cleanupClock = { origin: 'hrtime', completedAt: this.completedAt }
          if (priming && failCleanup) throw new Error('priming deletion failed')
        })
        await coverage(
          tap,
          separateCoordinators ? (priming ? 'node-a:9092' : 'node-b:9092') : 'node:1',
          priming ? 13 : scoredMetadataVersion
        )
      }),
      openPhase: async () => {
        if (!priming && phase++ === 0) armedAt = performance.now()
      },
      closePhase: async () => {},
      cycle: async (plan, scope, { clock, due }) => {
        if (priming && failPrime) throw new Error('priming workload failed')
        const observation = facts.cycleFacts(plan.cycle, { startedAt: clock.now(), due })
        await delay(scope, 6300)
        return observation
      },
      final: async (scope, { clock, due }) => {
        const observation = facts.cycleFacts(29, { startedAt: clock.now(), due, kind: 'final' })
        await delay(scope, 6300)
        return observation
      },
    }
  }
  const primeDriver = driver(true)
  const scoredDriver = driver(false)
  let primeRef
  const owner = new ShareSoak({
    driver: scoredDriver,
    kafka: {},
    shares,
    stack: new CleanupStack(),
    short,
    evidence,
    gate,
    clock,
    pause: (scope, ms) => delay(scope, ms + idleWakeMs),
    createPriming: () => {
      const prime = new SharePriming({
        driver: primeDriver,
        kafka: {},
        shares,
        stack: new CleanupStack(),
        short: true,
        evidence,
        gate,
        clock,
      })
      primeRef = new WeakRef(prime)
      return prime
    },
  })
  const snapshot = owner.snapshot.bind(owner)
  owner.snapshot = async () => {
    const point = await snapshot()
    if (damageBoundary && rows.some((r) => r.event === 'priming-complete') && !armedAt)
      damageBoundary(point)
    return point
  }
  let result
  let error
  let done = false
  const work = owner
    .run()
    .then(
      (value) => {
        result = value
      },
      (failure) => {
        error = failure
      }
    )
    .finally(() => {
      done = true
    })
  try {
    for (let i = 0; i < 400; i++) {
      if (done) break
      await jest.advanceTimersByTimeAsync(10000)
      await new Promise((resolve) => setImmediate(resolve))
    }
    if (!done) throw new Error('The fixed source schedule did not settle')
    await work
    await owner.close()
    const stamps = { tree: 'a'.repeat(40), node: process.version, brokerVersion: '4.3.1' }
    const serialized = rows
      .map((row) =>
        JSON.stringify({
          ...stamps,
          at: '2026-11-09T00:00:00Z',
          lane: 'docker',
          step: 'soak',
          wireVersions: {},
          config: {},
          ...row,
        })
      )
      .join('\n')
    return {
      owner,
      rows,
      result,
      error,
      primeRef,
      primeDriver,
      scoredDriver,
      shares,
      problems: verifyFiles(['share.jsonl'], stamps, () => serialized).problems,
    }
  } finally {
    jest.useRealTimers()
    liveRun.reset()
    process.env = environment
  }
}

describe('fixed same-realm share priming', () => {
  test('accepts separate coordinator endpoints with corresponding negotiated versions', async () => {
    const { rows, result, problems } = await recording({ separateCoordinators: true })
    expect(result.ok).toBe(true)
    expect(problems).toEqual([])
    const inventories = rows.filter((r) => r.event === 'soak-protocol-manifest')
    expect(inventories[0].inventory.every((r) => r.endpoint === 'node-a:9092')).toBe(true)
    expect(inventories[1].inventory.every((r) => r.endpoint === 'node-b:9092')).toBe(true)
  }, 120000)

  test('rejects a corresponding negotiated version mismatch across separate coordinators', async () => {
    const { result, problems } = await recording({
      separateCoordinators: true,
      scoredMetadataVersion: 12,
    })
    expect(result.ok).toBe(true)
    expect(problems).toContain('share.jsonl: priming/scored negotiated implementations differ')
  }, 120000)

  test.each([true, false])(
    'executes one complete disjoint pass before scoring, short=%s',
    async (short) => {
      const { owner, rows, result, error, problems, shares } = await recording({ short })
      expect(error).toBeUndefined()
      expect(result).toMatchObject({ ok: true, counts: { cycles: 30, resets: 10 } })
      for (const scope of ['priming', 'scored']) {
        const original = rows.filter((r) => r.scope === scope)
        expect(verifyShareProtocol(original).problems).toEqual([])
        const manifest = original.find((r) => r.event === 'soak-protocol-manifest')
        const completion = original.find((r) =>
          ['priming-complete', 'case-complete'].includes(r.event)
        )
        expect(manifest).toBeDefined()
        expect(completion.protocolSha256).toBe(manifest.sha256)
      }
      expect(problems).toEqual([])
      expect(verifySharePriming(rows)).toEqual([])
      expect(rows.filter((r) => r.event === 'soak-cycle')).toHaveLength(60)
      expect(rows.filter((r) => r.event === 'soak-reset')).toHaveLength(20)
      expect(rows.filter((r) => r.event === 'priming-complete')).toHaveLength(1)
      expect(owner.spent.priming).toBeGreaterThanOrEqual(960000)
      expect(owner.workDue - owner.startedAt).toBe(short ? 2030000 : 2990000)
      expect(owner.profileMarginMs).toBe(10000)
      expect(owner.priming).toMatchObject({ passes: 1, cycles: 30, resets: 10, verifiers: 31 })
      expect(Object.values(owner.priming).every((v) => typeof v !== 'object')).toBe(true)
      expect(shares.restore).toHaveBeenCalledTimes(1)
      const prime = rows.find((r) => r.event === 'priming-complete')
      expect(prime.timing.spent).toMatchObject({ profile: 960000 })
      expect(prime.schedule.slotMs).toBe(32000)
      expect(prime.schedule.boundaryMs).toBe(10000)
    },
    120000
  )

  test.each([true, false])(
    'arms the 300 KiB/min first-phase component only after priming, short=%s',
    async (short) => {
      const { rows, result, error, problems } = await recording({
        short,
        rate: 307200,
        firstOnly: true,
      })
      expect(error).toBeUndefined()
      expect(result.ok).toBe(false)
      expect(result.resources.measured.phases[0].slopeBytesPerMinute).toBeCloseTo(307200, 5)
      expect(result.resources.measured.quietSlope.bytesPerMinute).toBe(0)
      expect(problems.join('\n')).toContain('share-steady: retained-heap screen')
      expect(rows.filter((r) => r.event === 'priming-complete')).toHaveLength(1)
      expect(rows.filter((r) => r.event === 'case-complete')).toHaveLength(0)
    },
    120000
  )

  test.each([true, false])(
    'rejects sustained post-priming 128 KiB/min, short=%s',
    async (short) => {
      const { result, problems } = await recording({ short, rate: 131072 })
      expect(result.ok).toBe(false)
      expect(result.resources.measured.quietSlope.bytesPerMinute).toBeCloseTo(131072, 0)
      expect(result.violations).toContain('phase-end quiet heap slope screen')
      if (!short) expect(result.violations).toContain('phase-end quiet heap screen')
      expect(problems.join('\n')).toContain('phase-end quiet heap slope screen')
    },
    120000
  )

  test.each([true, false])(
    'retains the scored fit with priming, irregular observations and a constant heap offset, short=%s',
    async (short) => {
      const { result, error, owner, problems } = await recording({
        short,
        rate: 131072,
        idleWakeMs: 3,
        heapOffset: 16777216,
        translatedMs: 1234560000,
      })
      expect(error).toBeUndefined()
      expect(owner.startedAt).toBe(1234560000)
      expect(result.resources.measured.quietSlope.bytesPerMinute).toBeCloseTo(131072, 0)
      expect(result.violations).toContain('phase-end quiet heap slope screen')
      expect(problems.join('\n')).toContain('phase-end quiet heap slope screen')
      const ends = result.resources.measured.quietSlope.points
      expect(ends[0].heapAtMs).toBeGreaterThan(960000)
      expect(ends[1].heapAtMs - ends[0].heapAtMs).toBeGreaterThan(short ? 320000 : 640000)
    },
    120000
  )

  test('records first-use heap growth without turning it into a passing scored heap verdict', async () => {
    const { rows, result, error } = await recording({ primeRate: 307200 })
    expect(error).toBeUndefined()
    expect(result.ok).toBe(true)
    const verdict = rows.find((r) => r.scope === 'priming' && r.kind === 'resource-verdict')
    expect(verdict.ok).toBe(true)
    expect(verdict.measurementOnly.ok).toBe(false)
    expect(verdict.measurementOnly.violations).toContain('share-steady: retained-heap screen')
  }, 120000)

  test('joins failed first-use cleanup, archives both errors and never prepares scored resources', async () => {
    const { owner, rows, error, scoredDriver } = await recording({
      failPrime: true,
      failCleanup: true,
    })
    expect(error.message).toBe('priming workload failed')
    expect(error.cleanupErrors[0].errors[0].message).toContain('priming deletion failed')
    expect(owner.gate.tainted).toBe(true)
    expect(scoredDriver.prepare).not.toHaveBeenCalled()
    expect(
      rows
        .filter((r) => r.event === 'case-failed')
        .map((r) => r.error.message)
        .join('\n')
    ).toContain('priming deletion failed')
    expect(rows.filter((r) => r.event === 'priming-complete')).toEqual([])
  }, 120000)

  test.each([
    [
      'timer',
      (p) => {
        p.activeResources.Timeout = 1
        p.activeResourcesBeforeCollection.Timeout = 1
      },
    ],
    [
      'socket',
      (p) => {
        p.sockets = { created: 1, destroyed: 0, open: 1 }
      },
    ],
    [
      'listener',
      (p) => {
        p.namedListeners['prime-socket:data'] = 1
      },
    ],
    [
      'queue',
      (p) => {
        p.queueDepth = 1
      },
    ],
    [
      'ledger',
      (p) => {
        p.shareRecords = 1
      },
    ],
    [
      'lost attribution',
      (p) => {
        p.timerOwners.afterCollection.unattributed.Timeout = 1
      },
    ],
    [
      'overflow',
      (p) => {
        p.timerOwners.afterCollection.dropped = 1
      },
    ],
  ])(
    'a retained priming %s cannot become a new scored baseline',
    async (_name, damageBoundary) => {
      const { error, scoredDriver, rows } = await recording({ damageBoundary })
      expect(error.message).toMatch(/priming boundary/)
      expect(scoredDriver.prepare).not.toHaveBeenCalled()
      expect(rows.some((r) => r.event === 'case-complete')).toBe(false)
    },
    120000
  )

  test.each([
    [
      'missing heap',
      (p) => {
        delete p.heapUsed
      },
    ],
    [
      'invalid heap',
      (p) => {
        p.heapUsed = NaN
      },
    ],
    [
      'missing queue source',
      (p) => {
        delete p.queueDepth
      },
    ],
    [
      'missing timer source',
      (p) => {
        delete p.timerOwners
      },
    ],
    [
      'observer overflow',
      (p) => {
        p.timerOwners.afterCollection.dropped = 1
      },
    ],
  ])(
    'a priming %s failure cannot produce a successful link or start scoring',
    async (_name, damagePrime) => {
      const { error, scoredDriver, rows, owner } = await recording({ damagePrime })
      expect(error).toBeInstanceOf(Error)
      expect(owner.gate.tainted).toBe(true)
      expect(scoredDriver.prepare).not.toHaveBeenCalled()
      expect(rows.some((r) => r.event === 'priming-complete')).toBe(false)
    },
    120000
  )

  test.each([
    [
      'missing heap',
      (p) => {
        delete p.heapUsed
      },
    ],
    [
      'NaN heap',
      (p) => {
        p.heapUsed = NaN
      },
    ],
    [
      'negative heap',
      (p) => {
        p.heapUsed = -1
      },
    ],
    [
      'missing elapsed time',
      (p) => {
        delete p.heapAtMs
      },
    ],
    [
      'NaN elapsed time',
      (p) => {
        p.heapAtMs = NaN
      },
    ],
    [
      'negative elapsed time',
      (p) => {
        p.heapAtMs = -1
      },
    ],
  ])(
    'rejects a boundary-only %s before admitting scored setup',
    async (_name, damageBoundary) => {
      const { error, scoredDriver, rows, owner } = await recording({ damageBoundary })
      expect(error).toBeInstanceOf(Error)
      expect(owner.gate.tainted).toBe(true)
      expect(scoredDriver.prepare).not.toHaveBeenCalled()
      expect(rows.some((r) => r.event === 'priming-complete')).toBe(true)
      expect(rows.some((r) => r.event === 'priming-boundary')).toBe(false)
      expect(rows.some((r) => r.event === 'case-complete')).toBe(false)
    },
    120000
  )

  test('the fixed allocations preserve both distinct idle margins', () => {
    expect(Object.isFrozen(SHARE_SOAK_PRIMING)).toBe(true)
    expect(SHARE_SOAK_PRIMING).toEqual({
      passes: 1,
      cycles: 30,
      cyclesPerPhase: 10,
      resets: 10,
      slotMs: 32000,
      phaseMs: 320000,
      profileMs: 960000,
      setupMs: 15000,
      transitionMs: 5000,
      cleanupMs: 20000,
      idleWakeMarginMs: 10000,
      boundaryMs: 10000,
      totalMs: 1020000,
    })
    for (const mode of ['short', 'full']) expect(SHARE_SOAK.budgets[mode].primingMs).toBe(1020000)
  })
})

describe('streamed share protocol evidence', () => {
  test('preserves partial writes, original return values, matched replies and ApiVersions attempts', async () => {
    const rows = []
    const tap = new ShareFetchTap()
    tap.archive = (fact) => rows.push(JSON.parse(JSON.stringify({ event: 'soak-wire', ...fact })))
    await coverage(tap)
    expect(verifyShareProtocol(rows).problems).toEqual([])
    expect(rows.filter((r) => r.apiKey === 18).map((r) => r.apiVersion)).toEqual([5, 4])
    expect(
      verifyShareProtocol(rows)
        .inventory.filter((r) => r.apiKey === 18)
        .map((r) => [r.apiVersion, r.attempt])
    ).toEqual([
      [4, 'negotiated'],
      [5, 'rejected'],
    ])
    const produce = rows.find((r) => r.apiKey === 0)
    expect(produceCodecs(Buffer.from(produce.bytes, 'base64'))).toEqual([0])
    const response = rows.find((r) => r.kind === 'response' && r.frame === produce.frame)
    response.correlationId++
    expect(verifyShareProtocol(rows).problems).toContain('protocol: response framing or identity')
  })

  test('reads codec attributes from actual batches rather than trusting a claimed codec', async () => {
    expect(produceCodecs(await request(0, 9, 1, 1))).toEqual([1])
  })
})

describe.each([true, false])('original work deadlines, short=%s', (short) => {
  let accepted
  beforeAll(async () => {
    const value = await recording({ short })
    expect(value.problems).toEqual([])
    accepted = value.rows
  }, 120000)

  test.each(
    ['priming', 'scored'].flatMap((scope) =>
      ['cycle', 'verifier', 'final', 'final verifier'].map((kind) => [scope, kind])
    )
  )('binds %s %s actual completion to its unchanged deadline', (scope, kind) => {
    const rows = JSON.parse(JSON.stringify(accepted))
    const cycle = rows.find((r) => r.scope === scope && r.event === 'soak-cycle')
    const done = rows.find(
      (r) => r.scope === scope && ['priming-complete', 'case-complete'].includes(r.event)
    )
    const target = kind.startsWith('final') ? done.final : cycle
    const due = target.timing.due
    target.timing.completedAt = due
    if (kind.includes('verifier')) target.verifier.closedAt = due
    const reseal = () => {
      const prime = rows.find((r) => r.event === 'priming-complete')
      for (const row of rows) if (row.priming) row.priming.receiptSha256 = primingReceiptHash(prime)
    }
    reseal()
    if (kind.startsWith('final')) {
      const { verifyShareWorkTiming } = require('../../evidence/sharePriming')
      done.timing.observedAt = due
      const cycles = rows.filter((r) => r.scenario === done.scenario && r.event === 'soak-cycle')
      expect(
        verifyShareWorkTiming(
          done,
          cycles,
          done.timing.startedAt,
          done.timing.workDue,
          done.timing.workDue + 10000
        )
      ).toEqual([])
    } else expect(verifySharePriming(rows)).toEqual([])
    if (kind.includes('verifier')) target.verifier.closedAt++
    else target.timing.completedAt++
    reseal()
    expect(verifySharePriming(rows)).toContain(
      `${scope}: original ${kind.startsWith('final') ? 'final' : 'cycle 0'} work exceeds its unchanged deadline or scope`
    )
    expect(target.timing.due).toBe(due)
  })

  test.each(['priming', 'scored'])(
    'checks %s actual scope completion against its absolute bound',
    (scope) => {
      const { verifyShareWorkTiming } = require('../../evidence/sharePriming')
      const rows = JSON.parse(JSON.stringify(accepted))
      const done = rows.find(
        (r) => r.scope === scope && ['priming-complete', 'case-complete'].includes(r.event)
      )
      const cycles = rows.filter((r) => r.scenario === done.scenario && r.event === 'soak-cycle')
      const start = rows.find((r) => r.event === 'priming-start').startedAt
      const workDue = start + (scope === 'priming' ? 1010000 : short ? 2030000 : 2990000)
      const childDue = start + (short ? 2040000 : 3000000)
      done.timing.observedAt = workDue
      expect(verifyShareWorkTiming(done, cycles, start, workDue, childDue)).toEqual([])
      done.timing.observedAt++
      expect(verifyShareWorkTiming(done, cycles, start, workDue, childDue)).toContain(
        'original scope completion exceeds its absolute bound'
      )
      expect(done.timing.workDue).toBe(workDue)
      done.timing.observedAt = childDue
      expect(verifyShareWorkTiming(done, cycles, start, childDue, childDue)).toEqual([])
      done.timing.observedAt++
      expect(verifyShareWorkTiming(done, cycles, start, childDue, childDue)).toContain(
        'original scope completion exceeds its absolute bound'
      )
    }
  )
})

describe('recorded priming integrity', () => {
  let accepted
  beforeAll(async () => {
    const recordingResult = await recording()
    expect(recordingResult.problems).toEqual([])
    accepted = recordingResult.rows
  }, 120000)

  test.each([
    [
      'forged boundary resource',
      (rows) => {
        rows.find((r) => r.event === 'priming-boundary').snapshot.queueDepth = 1
      },
    ],
    ...[
      [
        'missing heap',
        (p) => {
          delete p.heapUsed
        },
      ],
      [
        'nonfinite heap',
        (p) => {
          p.heapUsed = Infinity
        },
      ],
      [
        'negative heap',
        (p) => {
          p.heapUsed = -1
        },
      ],
      [
        'missing timestamp',
        (p) => {
          delete p.heapAtMs
        },
      ],
      [
        'negative timestamp',
        (p) => {
          p.heapAtMs = -1
        },
      ],
      [
        'late timestamp',
        (p) => {
          p.heapAtMs += 1020000
        },
      ],
    ].map(([name, damage]) => [
      `boundary ${name}`,
      (rows) => damage(rows.find((r) => r.event === 'priming-boundary').snapshot),
    ]),
    [
      'operation mismatch',
      (rows) => {
        const row = rows.find((r) => r.event === 'soak-wire' && r.apiKey === 19)
        row.operationId = -1
      },
    ],
    [
      'missing prime',
      (rows) =>
        rows.splice(
          rows.findIndex((r) => r.event === 'priming-complete'),
          1
        ),
    ],
    ['duplicate prime', (rows) => rows.push(rows.find((r) => r.event === 'priming-complete'))],
    [
      'extra pass',
      (rows) => {
        rows.find((r) => r.event === 'priming-start').passes = 2
      },
    ],
    [
      'missing cycle',
      (rows) =>
        rows.splice(
          rows.findIndex((r) => r.event === 'soak-cycle'),
          1
        ),
    ],
    ['duplicate cycle', (rows) => rows.push(rows.find((r) => r.event === 'soak-cycle'))],
    [
      'reordered cycles',
      (rows) => {
        const indices = rows
          .map((r, i) => (r.event === 'soak-cycle' ? i : -1))
          .filter((i) => i >= 0)
        ;[rows[indices[0]], rows[indices[1]]] = [rows[indices[1]], rows[indices[0]]]
      },
    ],
    [
      'false cleanup',
      (rows) => {
        rows.find((r) => r.event === 'priming-complete').cleanup.groupsAbsent = false
      },
    ],
    [
      'late priming cutoff',
      (rows) => {
        rows.find((r) => r.event === 'priming-complete').timing.workDue++
      },
    ],
    [
      'changed declared child cutoff',
      (rows) => {
        rows.find((r) => r.event === 'case-complete').timing.workDue++
      },
    ],
    [
      'changed declared original slot',
      (rows) => {
        rows.find((r) => r.event === 'case-complete').timing.slots[0].due++
      },
    ],
    [
      'late original stage',
      (rows) => {
        const stage = rows.find((r) => r.event === 'case-complete').timing.stages[0]
        stage.completedAt = stage.due + 1
      },
    ],
    [
      'late boundary',
      (rows) => {
        const start = rows.find((r) => r.event === 'priming-start')
        rows.find((r) => r.event === 'priming-boundary').priming.boundaryAt =
          start.startedAt + 1020001
      },
    ],
    [
      'scope substitution',
      (rows) => {
        rows.find((r) => r.scope === 'priming' && r.kind === 'baseline').scope = 'scored'
      },
    ],
    [
      'corpus substitution',
      (rows) => {
        const prime = rows.find((r) => r.scope === 'priming' && r.event === 'soak-cycle')
        rows.find((r) => r.scope === 'scored' && r.event === 'soak-cycle').oracles.log.expected =
          prime.oracles.log.expected
      },
    ],
    [
      'topic collision',
      (rows) => {
        const configs = rows.filter((r) => r.event === 'soak-configuration')
        configs[1].config.topic = configs[0].config.topic
      },
    ],
    [
      'group collision',
      (rows) => {
        const configs = rows.filter((r) => r.event === 'soak-configuration')
        configs[1].config.groupId = configs[0].config.groupId
      },
    ],
    [
      'UUID collision',
      (rows) => {
        const configs = rows.filter((r) => r.event === 'soak-configuration')
        configs[1].config.topicId = configs[0].config.topicId
      },
    ],
    [
      'member collision',
      (rows) => {
        const cycles = rows.filter((r) => r.event === 'soak-cycle')
        cycles[30].verifier.memberId = cycles[0].verifier.memberId
      },
    ],
    [
      'codec mismatch',
      (rows) => {
        rows.find((r) => r.event === 'soak-wire' && r.apiKey === 0).codecs = [1]
      },
    ],
    [
      'version mismatch',
      (rows) => {
        const row = rows.find((r) => r.event === 'soak-wire' && r.apiKey === 78)
        const bytes = Buffer.from(row.bytes, 'base64')
        bytes.writeInt16BE(1, 6)
        row.apiVersion = 1
        row.bytes = bytes.toString('base64')
      },
    ],
    [
      'missing Produce operation',
      (rows) => {
        for (let i = rows.length - 1; i >= 0; i--)
          if (rows[i].event === 'soak-wire' && rows[i].apiKey === 0) rows.splice(i, 1)
      },
    ],
  ])('rejects serialized %s evidence', (_name, damage) => {
    const rows = JSON.parse(JSON.stringify(accepted))
    damage(rows)
    const prime = rows.find((r) => r.event === 'priming-complete')
    if (prime)
      for (const row of rows) if (row.priming) row.priming.receiptSha256 = primingReceiptHash(prime)
    expect(verifySharePriming(rows).length).toBeGreaterThan(0)
  })
})

describe('original absolute priming allocations', () => {
  test('refuses owner restart without renewing its original origin', async () => {
    const owner = new ShareSoak({
      driver: {},
      kafka: {},
      shares: {},
      stack: new CleanupStack(),
      short: true,
      gate: new LiveRun(),
      evidence: { read: () => [], write: () => {} },
      clock: { now: () => 9999999 },
    })
    owner.startedAt = 12345
    await expect(owner.run()).rejects.toThrow('cannot be restarted')
    expect(owner.startedAt).toBe(12345)
    expect(owner.gate.tainted).toBe(true)
  })

  test.each([0, 1])(
    'enforces the first-use +1 ms cutoff without moving the original child clock, late=%s',
    async (late) => {
      let now = 12345
      const owner = new ShareSoak({
        driver: {},
        kafka: {},
        shares: {},
        stack: new CleanupStack(),
        short: true,
        gate: new LiveRun(),
        evidence: { read: () => [], write: () => {} },
        clock: { now: () => now },
      })
      owner.startedAt = now
      owner.workDue = now + 2030000
      const work = owner.stage('priming', async () => {
        now += 1020000 + late
      })
      if (late) await expect(work).rejects.toThrow('shared priming deadline exceeded')
      else await expect(work).resolves.toBeUndefined()
      expect(owner.startedAt).toBe(12345)
      expect(owner.workDue).toBe(2042345)
      expect(owner.spent.priming).toBe(1020000 + late)
      expect(owner.profileMarginMs).toBe(10000)
    }
  )
})
