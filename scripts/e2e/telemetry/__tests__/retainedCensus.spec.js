const { censusSnapshot, censusComparison } = require('../retained/census')
const { LiveCensus } = require('../retained/liveCensus')
const { prepareEphemerons, ephemeronLiveness } = require('../retained/ownership')
const { analyseSnapshot, prepareReferenceAudits, auditReferences } = require('../retained/heapDiff')

const fixture = (definitions) => {
  const strings = []
  const intern = (name) => {
    if (!strings.includes(name)) strings.push(name)
    return strings.indexOf(name)
  }
  const types = ['synthetic', 'object', 'native', 'code', 'array']
  const edgeTypes = ['property', 'internal', 'weak']
  const edges = []
  const nodes = definitions.flatMap(([type, name, id, bytes, links]) => {
    for (const [kind, label, target] of links)
      edges.push(edgeTypes.indexOf(kind), intern(label), target * 5)
    return [types.indexOf(type), intern(name), id, bytes, links.length]
  })
  return {
    snapshot: {
      node_count: definitions.length,
      meta: {
        node_fields: ['type', 'name', 'id', 'self_size', 'edge_count'],
        node_types: [types],
        edge_fields: ['type', 'name_or_index', 'to_node'],
        edge_types: [edgeTypes],
      },
    },
    strings,
    nodes,
    edges,
  }
}

const population = (released = false) =>
  fixture([
    [
      'synthetic',
      'root',
      1,
      0,
      [
        ['property', 'retainedProduct', 1],
        ['property', 'unknown', 6],
      ],
    ],
    [
      'object',
      'TelemetryPayload',
      3,
      40,
      [
        ['property', 'buffer', 2],
        ['property', 'alias', 3],
      ],
    ],
    ['object', 'Buffer', 5, 32, [['internal', 'buffer', 4]]],
    ['object', 'Uint8Array', 7, 32, [['internal', 'buffer', 4]]],
    ['object', 'ArrayBuffer', 9, 32, [['internal', 'backing_store', 5]]],
    ['native', 'system / JSArrayBufferData', released ? 15 : 11, released ? 64 : 1024, []],
    ['object', 'UnidentifiedOwner', released ? 17 : 13, released ? 80 : 96, []],
  ])

test('owner census counts shared stores and retained product self bytes once', () => {
  const point = censusSnapshot(population())
  expect(point.totals.product).toBe(40)
  expect(point.totals.buffers).toBe(1120)
  expect(point.totals.unresolved).toBe(96)
  expect(point.totalSelfBytes).toBe(1256)
  expect(Object.values(point.totals).reduce((sum, bytes) => sum + bytes, 0)).toBe(1256)
  expect(point.backingStores).toHaveLength(1)
  expect(point.backingStores[0].aliases.map((node) => node.id)).toEqual([5, 7])
  expect(point.nodes.find((node) => node.id === 3)).toMatchObject({
    category: 'product',
    ordinaryLive: true,
    semanticLive: true,
  })
  expect(point.paths.find((row) => row.id === 3)).toMatchObject({
    parentId: 1,
    edge: { name: 'retainedProduct' },
  })
})

test('owner census reports released allocations and absolute unresolved additions and removals', () => {
  const before = censusSnapshot(population())
  const after = censusSnapshot(population(true))
  const comparison = censusComparison(before, after)
  expect(comparison.categories.buffers).toMatchObject({ added: 64, removed: 1024, delta: -960 })
  expect(comparison.categories.unresolved).toMatchObject({
    before: 96,
    after: 80,
    added: 80,
    removed: 96,
    delta: -16,
  })
  expect(comparison.released.map((node) => node.id)).toEqual([11, 13])
  expect(comparison.allocated.map((node) => node.id)).toEqual([15, 17])
  expect(comparison.delta).toBe(-976)
  for (const row of Object.values(comparison.categories))
    expect(row.delta).toBe(row.added - row.removed + row.resized + row.transfer)
  expect(comparison.delta).toBe(after.totalSelfBytes - before.totalSelfBytes)
})

test('live owner census keeps diagnostic flags and captures original M first last and closed only', () => {
  const captures = []
  const census = new LiveCensus({
    sampleAt: [60, 90, 120],
    capture: (name, facts) => captures.push({ name, facts }),
  })
  const snapshot = {
    memory: { at: 10, heapUsed: 8, external: 4, arrayBuffers: 2, gc: { observed: 2 } },
    journals: { bytes: 128, owners: { samples: { cursor: 0 } } },
  }
  census.observe({ kind: 'baseline', snapshot })
  for (const admittedPhaseMs of [60, 90, 120])
    census.observe({ kind: 'sample', admittedPhaseMs, phaseMs: admittedPhaseMs + 1, snapshot })
  census.observe({ kind: 'teardown', snapshot })
  expect(captures.map((row) => row.name)).toEqual(['M', 'first', 'last', 'closed'])
  for (const { facts } of captures) {
    expect(facts).toMatchObject({
      diagnostic: true,
      acceptanceEligible: false,
      attributionComplete: false,
      reading: snapshot.memory,
      journals: snapshot.journals,
    })
    expect(facts.processRetainedBytes).toBe(12)
  }
  expect(captures[1].facts).toMatchObject({ admittedPhaseMs: 60, completedPhaseMs: 61 })
  expect(census.complete()).toBe(true)
  const missing = new LiveCensus({ sampleAt: [60, 90, 120], capture: () => {} })
  missing.observe({ kind: 'sample', admittedPhaseMs: 90, snapshot })
  expect(missing.complete()).toBe(false)
})

test('prepared ephemeron removals preserve AND dependencies without rebuilding indexes', () => {
  const label = '1 / part of key (Object @3) -> value (Object @7) pair in WeakMap (table @5)'
  const raw = fixture([
    [
      'synthetic',
      'root',
      1,
      0,
      [
        ['property', 'key', 1],
        ['property', 'table', 2],
      ],
    ],
    ['object', 'Object', 3, 8, [['internal', label, 3]]],
    [
      'array',
      '',
      5,
      40,
      [
        ['internal', label, 3],
        ['weak', 'value', 3],
      ],
    ],
    ['object', 'TelemetryPayload', 7, 48, []],
  ])
  const prepared = prepareEphemerons(raw)
  for (const excluded of [[], [3], [5], [7]])
    expect(prepared.liveness(excluded)).toEqual(ephemeronLiveness(raw, excluded))
  expect(prepared.liveness().reachableIds.has(7)).toBe(true)
  expect(prepared.liveness([3]).reachableIds.has(7)).toBe(false)
  expect(prepared.liveness([5]).reachableIds.has(7)).toBe(false)
})

test('shared cyclic audits preserve every root target and trace without repeated graph traversal', () => {
  const graph = analyseSnapshot(
    fixture([
      [
        'synthetic',
        'root',
        1,
        0,
        [
          ['property', 'left', 1],
          ['property', 'right', 2],
          ['property', 'asymmetric', 4],
        ],
      ],
      [
        'code',
        'FeedbackVector',
        3,
        16,
        [
          ['internal', 'next', 2],
          ['property', 'client', 3],
        ],
      ],
      ['code', 'FeedbackCell', 5, 16, [['internal', 'back', 1]]],
      ['object', 'TelemetryPayload', 7, 48, []],
      [
        'code',
        'FeedbackVector',
        9,
        16,
        [
          ['property', 'oneway', 1],
          ['property', 'extra', 5],
        ],
      ],
      ['object', 'DistinctOwner', 11, 32, []],
    ])
  )
  const prepared = prepareReferenceAudits(graph)
  const rows = [1, 2, 4].map((start) => prepared.audit(start))
  for (const [i, start] of [1, 2, 4].entries()) {
    const reference = auditReferences(graph, start, { includePaths: false })
    expect(
      prepared.tables[rows[i].table].applicationReferences.map((row) => row.target.id).sort()
    ).toEqual(reference.applicationReferences.map((row) => row.target.id).sort())
    expect(rows[i].ownerTrace).toEqual(reference.ownerTrace)
  }
  expect(rows[0].table).toBe(rows[1].table)
  expect(rows[2].table).not.toBe(rows[0].table)
  expect(prepared.tables).toHaveLength(2)
})

test('live census checkpoints follow the sample grid through eight public churn reopens', () => {
  const { TELEMETRY } = require('../../../soak/soak')
  const { TelemetrySchedule } = require('../../../soak/telemetryChurn')
  const schedule = new TelemetrySchedule(TELEMETRY)
  const captures = []
  const census = new LiveCensus({
    sampleAt: TELEMETRY.sampleAt,
    capture: (name) => captures.push(name),
  })
  const snapshot = { memory: { heapUsed: 8, external: 4 }, journals: {} }
  census.observe({ kind: 'baseline', snapshot })
  const reopens = schedule.ticks.filter((tick) => tick.actions.includes('reopen'))
  expect(reopens).toHaveLength(8)
  for (const tick of reopens) census.observe({ kind: 'reopen', at: tick.at, snapshot })
  for (const admittedPhaseMs of TELEMETRY.sampleAt)
    census.observe({ kind: 'sample', admittedPhaseMs, snapshot })
  expect(census.complete()).toBe(false)
  census.observe({ kind: 'teardown', snapshot })
  expect(captures).toEqual(['M', 'first', 'last', 'closed'])
  expect(census.complete()).toBe(true)
})
