const path = require('node:path')

const load = () => require(path.join(__dirname, '../retained/activeCensus'))
const reading = (at, heapUsed) => ({
  at,
  heapUsed,
  external: 10,
  arrayBuffers: 2,
  retained: heapUsed + 10,
  rss: 1000,
})
const setup = (variant = 'capture') => {
  const capture = jest.fn()
  const memory = {
    current: jest.fn().mockReturnValueOnce(reading(20, 200)).mockReturnValueOnce(reading(21, 80)),
    gc: jest.fn(),
  }
  const census = new (load().ActiveCensus)({ memory, capture, variant })
  census.baseline(reading(10, 50))
  const row = {
    id: 3,
    jobId: 7,
    codec: 'gzip',
    inputBytes: 372000,
    inputBackingBytes: 372000,
    submitted: { at: 19 },
    settled: null,
  }
  const journal = { live: new Set([row]) }
  return { census, memory, capture, row, journal }
}

test('active census pairs raw and explicit major GC with one pending job without retaining its input', () => {
  const { census, memory, capture, row, journal } = setup()
  census.phase = 'gzip'
  census.submit(row, journal)
  expect(memory.gc).toHaveBeenCalledWith({ type: 'major', execution: 'sync' })
  expect(capture.mock.calls.map(([name]) => name)).toEqual(['M', 'gzip'])
  const facts = capture.mock.calls[1][1]
  expect(facts).toMatchObject({
    diagnostic: true,
    acceptanceEligible: false,
    attributionComplete: false,
    stage: 'submission-during-active-settle',
    raw: reading(20, 200),
    afterMajorGc: reading(21, 80),
    rawMinusM: 150,
    afterMajorGcMinusM: 30,
    majorGc: true,
    pending: [
      {
        id: 3,
        jobId: 7,
        codec: 'gzip',
        inputBytes: 372000,
        inputBackingBytes: 372000,
        submittedAt: 19,
      },
    ],
  })
  expect(memory.current.mock.invocationCallOrder[0]).toBeLessThan(
    memory.gc.mock.invocationCallOrder[0]
  )
  expect(memory.gc.mock.invocationCallOrder[0]).toBeLessThan(
    memory.current.mock.invocationCallOrder[1]
  )
  expect(memory.current.mock.invocationCallOrder[1]).toBeLessThan(
    capture.mock.invocationCallOrder[1]
  )
  row.inputBytes = 1
  expect(facts.pending[0].inputBytes).toBe(372000)
  census.submit(row, journal)
  expect(capture).toHaveBeenCalledTimes(2)
  expect(memory.gc).toHaveBeenCalledTimes(1)
})

test('active census scalar companion observes the same seam without GC or captures outside active settlement', () => {
  const { census, memory, capture, row, journal } = setup('scalar')
  census.submit(row, journal)
  census.phase = 'zstd'
  census.submit(row, journal)
  census.phase = 'gzip'
  journal.live.clear()
  census.submit(row, journal)
  expect(capture).toHaveBeenCalledTimes(1)
  journal.live.add(row)
  census.submit(row, journal)
  expect(memory.gc).not.toHaveBeenCalled()
  expect(capture.mock.calls[1][1]).toMatchObject({
    raw: reading(20, 200),
    afterMajorGc: null,
    afterMajorGcMinusM: null,
    majorGc: false,
  })
})

test('active census fails incomplete codec coverage and preserves capture failures', () => {
  const { census, capture, row, journal } = setup()
  expect(() => census.complete(['gzip', 'zstd'])).toThrow('missing active census')
  census.phase = 'gzip'
  capture.mockImplementationOnce(() => {
    throw new Error('snapshot overflow')
  })
  expect(() => census.submit(row, journal)).toThrow('snapshot overflow')
  expect(() => census.complete(['gzip'])).toThrow('missing active census')
  census.submit(row, journal)
  expect(capture).toHaveBeenCalledTimes(2)
})

test('active census top owners carry additive self bytes and named retaining paths including unresolved owners', () => {
  const nodes = [
    { id: 1, name: 'TelemetryPayload', type: 'object', category: 'product', selfBytes: 40 },
    { id: 2, name: 'TelemetryPayload', type: 'object', category: 'product', selfBytes: 60 },
    { id: 3, name: 'UnknownOwner', type: 'object', category: 'unresolved', selfBytes: 200 },
  ]
  const graph = {
    rootPath: jest.fn((v) => [
      { node: { id: 0, name: 'root' } },
      { node: nodes[v], edge: { name: 'currentJob' } },
    ]),
  }
  const summary = load().topOwners({ nodes }, graph)
  expect(summary[0]).toMatchObject({ name: 'UnknownOwner', selfBytes: 200, count: 1 })
  expect(summary[1]).toMatchObject({ name: 'TelemetryPayload', selfBytes: 100, count: 2 })
  expect(summary[1].examples).toHaveLength(2)
  expect(summary[1].examples[0].rootPath[1].edge.name).toBe('currentJob')
  const cohort = load().topOwners({ nodes }, graph, new Set([2]))
  expect(cohort).toHaveLength(1)
  expect(cohort[0]).toMatchObject({ selfBytes: 60, count: 1 })
  expect(cohort[0].examples[0].rootPath[1].node.id).toBe(2)
})

test('active census replay rejects unpaired changed and nonfinite evidence without hiding raw overages', () => {
  const { census, capture, row, journal } = setup()
  census.phase = 'gzip'
  census.submit(row, journal)
  const event = { ...capture.mock.calls[1][1], name: 'gzip', startedAt: 22, endedAt: 23 }
  const options = {
    variant: 'capture',
    matched: reading(10, 50),
    native: { ...row, settled: { at: 24 } },
  }
  expect(() => load().validatePoint(event, options)).not.toThrow()
  const checkpoint = { ...event, name: 'gzip-checkpoint', stage: 'after-turn-2', round: 0 }
  expect(() => load().validatePoint(checkpoint, options)).not.toThrow()
  expect(() => load().validatePoint({ ...checkpoint, round: 6 }, options)).toThrow()
  for (const mutate of [
    (copy) => {
      copy.acceptanceEligible = true
    },
    (copy) => {
      copy.raw.at = NaN
    },
    (copy) => {
      copy.raw.retained++
    },
    (copy) => {
      copy.afterMajorGc.retained++
    },
    (copy) => {
      copy.rawMinusM++
    },
    (copy) => {
      copy.pending[0].jobId++
    },
    (copy) => {
      copy.pending.push(copy.pending[0])
    },
    (copy) => {
      copy.endedAt = 25
    },
    (copy) => {
      copy.startedAt = 19
    },
    (copy) => {
      copy.afterMajorGc = null
    },
  ]) {
    const changed = JSON.parse(JSON.stringify(event))
    mutate(changed)
    expect(() => load().validatePoint(changed, options)).toThrow()
  }
  const large = reading(20, 12 * 1024 * 1024)
  const overage = { ...event, raw: large, rawMinusM: large.retained - options.matched.retained }
  expect(() => load().validatePoint(overage, options)).not.toThrow()
  expect(() =>
    load().validatePoint(
      { ...event, name: 'M', reading: reading(9, 50), processRetainedBytes: 60 },
      options
    )
  ).toThrow('baseline changed')
})

test('active census captures the original pending post-turn checkpoint and reports absent overlaps', () => {
  const { census, memory, capture, row, journal } = setup()
  census.phase = 'gzip'
  census.submit(row, journal)
  memory.current.mockReturnValueOnce(reading(30, 300)).mockReturnValueOnce(reading(31, 90))
  for (const stage of ['after-second-gc', 'before-turn-1', 'after-turn-1', 'before-turn-2'])
    census.observe(0, stage, journal)
  expect(capture).toHaveBeenCalledTimes(2)
  census.observe(0, 'after-turn-2', journal)
  census.observe(1, 'after-turn-2', journal)
  expect(capture.mock.calls.map(([name]) => name)).toEqual(['M', 'gzip', 'gzip-checkpoint'])
  expect(capture.mock.calls[2][1]).toMatchObject({
    stage: 'after-turn-2',
    round: 0,
    raw: reading(30, 300),
    afterMajorGc: reading(31, 90),
  })
  expect(census.complete(['gzip']).checkpointCoverage).toEqual([
    { codec: 'gzip', pendingCheckpointCaptured: true },
  ])
  const absent = setup('scalar')
  absent.census.phase = 'gzip'
  absent.census.submit(absent.row, absent.journal)
  absent.journal.live.clear()
  absent.census.observe(0, 'after-turn-2', absent.journal)
  expect(absent.census.complete(['gzip']).checkpointCoverage).toEqual([
    { codec: 'gzip', pendingCheckpointCaptured: false },
  ])
})

const fs = require('node:fs')
const vm = require('node:vm')
const yaml = require('js-yaml')
const { ProcessMemory } = require('../../../../testHelpers/processMemory')
const snapshotModule = () => require(path.join(__dirname, '../retained/activeSnapshot'))
const MiB = 1024 * 1024

// No heap snapshots or real collections run in these broker-free tests.
test('active snapshot switch off preserves scalar bytes and performs no capture', async () => {
  const capture = jest.fn()
  expect(snapshotModule().createActiveSnapshot({ env: {}, capture })).toBeNull()
  const run = async (hook) => {
    let at = 0
    let collections = 0
    const memory = new ProcessMemory({
      read: () => ({ vmhwm: 100, vmrss: 100 }),
      now: () => ++at,
      gc: () => collections++,
      usage: () => ({ rss: 100, heapUsed: 20, external: 10, arrayBuffers: 2 }),
      sleep: async () => {},
      settleTurn: async () => {},
    })
    try {
      const result = hook ? await memory.settled(null, null, hook) : await memory.settled()
      // Engine code statistics are process-wide and unrelated to the injected scalar sequence.
      for (const point of result.gc.checkpoints) delete point.engine
      return { bytes: JSON.stringify(result), collections }
    } finally {
      memory.stop()
    }
  }
  expect(await run(null)).toEqual(await run(snapshotModule().createActiveSnapshot({ env: {} })))
  expect(capture).not.toHaveBeenCalled()
})

test('active snapshot records M then triggers once strictly after an original checkpoint without GC', async () => {
  const { ActiveSnapshot } = snapshotModule()
  const order = []
  const capture = (name) => order.push(name)
  const memory = new ProcessMemory({
    read: () => ({ vmhwm: 100, vmrss: 100 }),
    now: () => 10,
    gc: jest.fn(),
    usage: () => ({ rss: 100, heapUsed: 9 * MiB, external: 10, arrayBuffers: 2 }),
    sleep: async () => {},
    settleTurn: async () => {},
  })
  try {
    const observer = new ActiveSnapshot({ memory, capture })
    const matched = reading(1, MiB)
    order.push('matched-recorded')
    observer.baseline(matched)
    const result = await memory.settled(null, null, (point, round, checkpoints) => {
      expect(checkpoints[round]).toBe(point)
      order.push('checkpoint-recorded')
      observer.active(point, { phase: 'gzip', round })
    })
    observer.active(result, { phase: 'gzip', round: null })
    observer.active(reading(11, 12 * MiB), { phase: 'held', round: null })
    expect(order.slice(0, 4)).toEqual(['matched-recorded', 'M', 'checkpoint-recorded', 'trigger'])
    expect(order.filter((name) => name === 'trigger')).toHaveLength(1)
    expect(memory.gc).toHaveBeenCalledTimes(4)
    expect(observer.summary()).toMatchObject({
      diagnostic: true,
      acceptanceEligible: false,
      triggered: true,
      records: [
        { name: 'M', reading: matched },
        { name: 'trigger', phase: 'gzip', round: 0, delta: 8 * MiB },
      ],
    })
  } finally {
    memory.stop()
  }
})

test('active snapshot uses a strict four MiB threshold and never retries failed captures', () => {
  const { ActiveSnapshot } = snapshotModule()
  const capture = jest.fn()
  const memory = { current: () => reading(20, MiB), gc: jest.fn() }
  const observer = new ActiveSnapshot({ memory, capture })
  observer.active(reading(2, 12 * MiB), { phase: 'gzip', round: 0 })
  observer.baseline(reading(1, MiB))
  observer.active(reading(2, 5 * MiB), { phase: 'gzip', round: 0 })
  expect(capture).toHaveBeenCalledTimes(1)
  capture.mockImplementationOnce(() => {
    throw new Error('disk full')
  })
  expect(() => observer.active(reading(3, 5 * MiB + 1), { phase: 'held', round: null })).toThrow(
    'disk full'
  )
  observer.active(reading(4, 9 * MiB), { phase: 'held', round: null })
  expect(capture).toHaveBeenCalledTimes(2)
  expect(memory.gc).not.toHaveBeenCalled()
  expect(observer.summary().complete).toBe(false)
})

test('active snapshot pair analysis reports additive growth owners paths and unresolved GC-sensitive bytes', () => {
  const { analysePair } = require(path.join(__dirname, '../retained/analyseActiveSnapshot'))
  const node = (id, name, selfBytes, category, label = '') => ({
    id,
    name,
    type: 'object',
    selfBytes,
    category,
    label,
    semanticLive: true,
    ordinaryLive: true,
  })
  const before = {
    nodes: [node(1, 'TelemetryPayload', 40, 'product')],
    totalSelfBytes: 40,
    totals: { product: 40 },
  }
  const after = {
    nodes: [
      node(1, 'TelemetryPayload', 48, 'product'),
      node(3, 'Object', 100, 'product'),
      node(5, 'Object', 80, 'harness'),
    ],
    totalSelfBytes: 228,
    totals: { product: 148, harness: 80 },
  }
  const graph = {
    rootPath: (index) => [
      { node: { id: 0, name: 'root' } },
      { node: { id: 1, name: 'TelemetryPayload' }, edge: { name: 'payload' } },
      { node: after.nodes[index], edge: { name: '#job' } },
    ],
  }
  const result = analysePair(before, after, graph, {
    reading: reading(2, 9 * MiB),
    afterSnapshot: reading(3, MiB),
  })
  expect(result).toMatchObject({
    diagnostic: true,
    acceptanceEligible: false,
    netSelfBytes: 188,
    positiveSelfBytes: 188,
    gcSensitiveRetainedBytes: 8 * MiB,
    blockAttribution: 'unresolved',
  })
  expect(result.constructors.find((row) => row.name === 'TelemetryPayload').selfBytes).toBe(8)
  expect(result.split.product).toBe(108)
  expect(result.split.harness).toBe(80)
  expect(
    result.owners.some((row) =>
      row.examples.some((entry) => entry.rootPath.at(-1).edge.name === '#job')
    )
  ).toBe(true)
  expect(
    result.productOwners.some(
      (row) =>
        row.field === '#job' &&
        row.source.file === 'src/telemetry/payload.js' &&
        row.source.line > 0
    )
  ).toBe(true)
  expect(result.interpretation).toMatch(/snapshot.*collect/i)
})

test('active snapshot diagnostic is dispatch only bounded and excluded from every verdict', () => {
  const workflow = yaml.load(
    fs.readFileSync(path.join(__dirname, '../../../../.github/workflows/memory-gates.yml'), 'utf8')
  )
  const job = workflow.jobs['resource-active-snapshot']
  expect(job).toBeDefined()
  expect(job.if).toContain("github.event_name == 'workflow_dispatch'")
  expect(job.if).toContain("'resource-active-snapshot'")
  expect(job['timeout-minutes']).toBeLessThanOrEqual(45)
  expect(job.strategy.matrix.node).toEqual([22, 24, 26])
  expect(job.steps.some((step) => step.run?.includes('activeSnapshotDiagnostic.js'))).toBe(true)
  expect(
    job.steps.some(
      (step) => step.run?.includes('analyseActiveSnapshot.js') && step.if === 'always()'
    )
  ).toBe(true)
  for (const [name, verdict] of Object.entries(workflow.jobs)) {
    if (name.includes('verdict'))
      expect(verdict.needs || []).not.toContain('resource-active-snapshot')
  }
  const select = workflow.jobs.select.steps.find((step) => step.id === 'select').run
  const script = select.slice(select.indexOf('const fs ='), select.lastIndexOf('NODE'))
  const outputs = []
  vm.runInNewContext(script, {
    require: () => ({ appendFileSync: (_file, value) => outputs.push(value) }),
    process: { env: { ONLY: 'resource-active-snapshot', GITHUB_OUTPUT: 'output' } },
  })
  expect(outputs.join('')).toContain('jobs=["resource-active-snapshot"]')
})
