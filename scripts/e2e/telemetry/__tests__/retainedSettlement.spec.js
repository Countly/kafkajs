const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { DiagnosticMemory } = require('../retained/observer')
const { DiagnosticLog, CAPACITY } = require('../retained/contract')
const { validateSettlements, settlementReport, STAGES } = require('../retained/settlement')
const { ProcessMemory } = require('../../../../testHelpers/processMemory')

const dirs = []
afterEach(() => {
  jest.restoreAllMocks()
  for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true })
})

/** Script GC and callback effects without real collections, timers or native work. */
const run = async (
  Meter = DiagnosticMemory,
  drifting = false,
  codec = 'gzip',
  settleNative = false
) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'retained-settlement-'))
  dirs.push(dir)
  const log = new DiagnosticLog(path.join(dir, 'events.jsonl'))
  let heap = 100
  let at = 0
  const actions = []
  let nativeSettled = false
  const sources = {
    read: () => null,
    now: () => ++at,
    gc: () => {
      actions.push('gc')
      heap = drifting ? heap + 20000 : 100
    },
    sleep: async (ms) => actions.push(`sleep:${ms}`),
    settleTurn: async () => {
      actions.push('turn')
      heap += 1024
      if (settleNative && !nativeSettled) {
        nativeSettled = true
        memory.mark(`native:${codec}:settled`, { event: 'settled', id: 7, jobId: 19, codec })
      }
    },
    usage: () => ({ rss: 10000, heapUsed: heap, external: 50, arrayBuffers: 20 }),
  }
  const memory =
    Meter === ProcessMemory ? new Meter(sources) : new Meter(log, dir, 'scalar', sources)
  try {
    if (Meter === DiagnosticMemory) {
      memory.mark(`native:${codec}:submitted`, { event: 'submitted', id: 7, jobId: 19, codec })
    }
    const reading = await memory.settled(
      Meter === DiagnosticMemory ? 'public:block:0:closed' : undefined
    )
    return {
      reading,
      actions,
      events: fs.readFileSync(log.file, 'utf8').trim().split('\n').filter(Boolean).map(JSON.parse),
      storage: memory.settlementStorage,
    }
  } finally {
    memory.stop()
  }
}

test('records ordered post-GC and settlement-turn scalars with phase and native job identities', async () => {
  const { events, reading } = await run()
  const rows = events.filter((row) => row.type === 'settlement')
  expect(rows).toHaveLength(1)
  expect(rows[0]).toMatchObject({ id: reading.gc.diagnosticId, phase: 'public:block:0:closed' })
  expect(rows[0].observations).toHaveLength(10)
  for (let pair = 0; pair < 2; pair++) {
    const points = rows[0].observations.slice(pair * 5, pair * 5 + 5)
    expect(points.map((point) => point.stage)).toEqual(STAGES)
    expect(points.map((point) => point.heapUsed)).toEqual([100, 100, 1124, 1124, 2148])
    expect(points.every((point) => point.round === pair)).toBe(true)
    expect(
      points.every(
        (point) =>
          point.submittedId === 7 && point.submittedJobId === 19 && point.submittedCodec === 1
      )
    ).toBe(true)
  }
  expect(validateSettlements(events, [reading])).toEqual([])
  expect(settlementReport(events).pairs.map((pair) => pair.postGcToReading.retained)).toEqual([
    2048, 2048,
  ])
})

test('adds no collections, settlement turns, waits or timer resources to the read path', async () => {
  const plain = await run(ProcessMemory)
  const interval = jest.spyOn(global, 'setInterval')
  const timeout = jest.spyOn(global, 'setTimeout')
  const immediate = jest.spyOn(global, 'setImmediate')
  const diagnostic = await run()
  expect(diagnostic.actions).toEqual(plain.actions)
  expect(diagnostic.actions).toEqual([
    'gc',
    'sleep:100',
    'gc',
    'turn',
    'turn',
    'gc',
    'sleep:100',
    'gc',
    'turn',
    'turn',
  ])
  expect(interval).not.toHaveBeenCalled()
  expect(timeout).not.toHaveBeenCalled()
  expect(immediate).not.toHaveBeenCalled()
  expect(diagnostic.reading.retained).toBe(plain.reading.retained)
})

test('bounds scalar storage at thirty observations even on six-pair exhaustion', async () => {
  const { events, reading, storage } = await run(DiagnosticMemory, true)
  expect(reading.gc.checkpoints).toHaveLength(6)
  expect(events.find((row) => row.type === 'settlement').observations).toHaveLength(30)
  expect(storage).toBeInstanceOf(Float64Array)
  expect(storage.byteLength).toBe(CAPACITY.settlementPoints * 13 * 8)
})

test.each(['missing', 'reordered', 'duplicate', 'foreign-job', 'overflow'])(
  'independent replay rejects %s settlement evidence',
  async (mode) => {
    const { events, reading } = await run()
    const row = events.find((event) => event.type === 'settlement')
    if (mode === 'missing') row.observations.pop()
    if (mode === 'reordered') row.observations.reverse()
    if (mode === 'duplicate') events.push(row)
    if (mode === 'foreign-job') row.observations[0].submittedJobId = 2000
    if (mode === 'overflow')
      row.observations.push(...row.observations, ...row.observations, ...row.observations)
    expect(validateSettlements(events, [reading]).length).toBeGreaterThan(0)
  }
)

test.each(['gzip', 'zstd'])(
  'joins %s settlement during the existing turn to its native job',
  async (codec) => {
    const { events, reading } = await run(DiagnosticMemory, false, codec, true)
    const points = events.find((event) => event.type === 'settlement').observations
    expect(points.slice(0, 2).map((point) => point.settledId)).toEqual([0, 0])
    expect(
      points
        .slice(2)
        .every(
          (point) =>
            point.settledId === 7 &&
            point.settledJobId === 19 &&
            point.settledCodec === (codec === 'gzip' ? 1 : 4) &&
            point.pending === 0
        )
    ).toBe(true)
    expect(settlementReport(events).pairs[0].nativeEdges).toEqual([
      expect.objectContaining({ event: 'settled', id: 7, jobId: 19, codec }),
    ])
    expect(validateSettlements(events, [reading])).toEqual([])
  }
)

test('invokes scalar hooks immediately after the second GC and around only the original turns', async () => {
  const order = []
  const memory = new ProcessMemory({
    read: () => null,
    gc: () => order.push('gc'),
    sleep: async (ms) => order.push(`sleep:${ms}`),
    settleTurn: async () => order.push('turn'),
    usage: () => ({ rss: 100, heapUsed: 10, external: 7, arrayBuffers: 3 }),
  })
  try {
    await memory.settled((_round, stage) => order.push(stage))
    expect(order).toEqual(
      Array(2)
        .fill([
          'gc',
          'sleep:100',
          'gc',
          'after-second-gc',
          'before-turn-1',
          'turn',
          'after-turn-1',
          'before-turn-2',
          'turn',
          'after-turn-2',
        ])
        .flat()
    )
  } finally {
    memory.stop()
  }
})
