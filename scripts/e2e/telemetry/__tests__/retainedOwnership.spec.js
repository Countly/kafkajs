const { analyseSnapshot, auditReferences } = require('../retained/heapDiff')
const { ephemeronLiveness, refineOwnership, selfLedger } = require('../retained/ownership')

const fixture = (definitions) => {
  const strings = []
  const intern = (s) => {
    if (!strings.includes(s)) strings.push(s)
    return strings.indexOf(s)
  }
  const types = ['synthetic', 'object', 'array', 'code', 'object shape', 'string']
  const edgeTypes = ['property', 'internal', 'weak', 'context']
  const edges = []
  const nodes = definitions.flatMap(([type, name, id, bytes, outgoing]) => {
    for (const [kind, label, target] of outgoing)
      edges.push(edgeTypes.indexOf(kind), intern(label), target * 5)
    return [types.indexOf(type), intern(name), id, bytes, outgoing.length]
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
    nodes,
    edges,
    strings,
  }
}
const pairLabel = (key, value, table) =>
  `1 / part of key (Object @${key}) -> value (Object @${value}) pair in WeakMap (table @${table})`

const conditionalFixture = (rootKey = true, rootTable = true) =>
  fixture([
    [
      'synthetic',
      'root',
      1,
      0,
      [
        ...(rootKey ? [['property', 'key', 1]] : []),
        ...(rootTable ? [['property', 'table', 2]] : []),
      ],
    ],
    ['object', 'Object', 3, 8, [['internal', pairLabel(3, 7, 5), 3]]],
    [
      'array',
      '',
      5,
      40,
      [
        ['weak', 'key', 1],
        ['weak', 'value', 3],
        ['internal', pairLabel(3, 7, 5), 3],
      ],
    ],
    ['object', 'Object', 7, 16, [['property', 'cycle', 1]]],
  ])

test('requires independently live table and key; duplicate pair edges cannot bootstrap cycles', () => {
  const live = ephemeronLiveness(conditionalFixture())
  expect([...live.ordinaryIds]).toEqual([1, 3, 5])
  expect(live.reachableIds.has(7)).toBe(true)
  expect(live.pairs).toEqual([
    { tableId: 5, keyId: 3, valueId: 7, active: true, independentlyReachableValue: false },
  ])
  expect(ephemeronLiveness(conditionalFixture(false)).reachableIds.has(7)).toBe(false)
  expect(ephemeronLiveness(conditionalFixture(true, false)).reachableIds.has(7)).toBe(false)
  expect(ephemeronLiveness(conditionalFixture(), [3]).reachableIds.has(7)).toBe(false)
  expect(ephemeronLiveness(conditionalFixture(), [5]).reachableIds.has(7)).toBe(false)
})

test('iterates conditional chains to a fixed point and preserves independently retained values', () => {
  const raw = fixture([
    [
      'synthetic',
      'root',
      1,
      0,
      [
        ['property', 'key', 1],
        ['property', 'table', 2],
        ['property', 'independent', 5],
      ],
    ],
    ['object', 'Object', 3, 8, []],
    [
      'array',
      '',
      5,
      40,
      [
        ['internal', pairLabel(7, 9, 5), 4],
        ['internal', pairLabel(3, 7, 5), 3],
        ['internal', pairLabel(3, 11, 5), 5],
      ],
    ],
    ['object', 'Object', 7, 16, []],
    ['object', 'Object', 9, 16, []],
    ['object', 'Object', 11, 16, []],
  ])
  expect(ephemeronLiveness(raw).reachableIds.has(9)).toBe(true)
  const removed = ephemeronLiveness(raw, [5])
  expect(removed.reachableIds.has(9)).toBe(false)
  expect(removed.reachableIds.has(11)).toBe(true)
})

test('rejects inconsistent ephemeron labels instead of interpreting them as ordinary strong links', () => {
  const raw = conditionalFixture()
  raw.strings[raw.strings.indexOf(pairLabel(3, 7, 5))] = pairLabel(3, 99, 5)
  expect(() => ephemeronLiveness(raw)).toThrow(/ephemeron dependency/)
})

test('an explicit unlimited audit passes all former depth, node and reference limits', () => {
  const raw = fixture(
    Array.from({ length: 1105 }, (_, i) => [
      i === 0 ? 'synthetic' : 'object',
      i === 0 ? 'root' : 'Object',
      2 * i + 1,
      8,
      i < 1104 ? [['property', 'next', i + 1]] : [],
    ])
  )
  const graph = analyseSnapshot(raw)
  expect(
    auditReferences(graph, 1, { maxNodes: 1000, maxDepth: 6, includePaths: false }).truncated
  ).toBe(true)
  const full = auditReferences(graph, 1, {
    maxNodes: Infinity,
    maxDepth: Infinity,
    maxReferences: Infinity,
    includePaths: false,
  })
  expect(full.truncated).toBe(false)
  expect(full.applicationReferences).toHaveLength(1103)
  expect(full.applicationReferences.at(-1).target.id).toBe(2209)
})

test('does not classify generic feedback-held application objects as compiler storage', () => {
  const raw = fixture([
    [
      'synthetic',
      'root',
      1,
      0,
      [
        ['property', 'feedback', 1],
        ['property', 'literal', 3],
      ],
    ],
    ['code', 'system / FeedbackVector', 3, 32, [['property', 'client', 2]]],
    ['object', 'Object', 5, 48, []],
    ['code', 'system / AllocationSite', 7, 32, [['internal', 'transition_info', 4]]],
    ['object', 'Object', 9, 48, []],
  ])
  const { labels } = refineOwnership(analyseSnapshot(raw))
  expect(labels[2]).toBe('residual:unresolved')
  expect(labels[4]).toBe('runtime:allocation-literal-template')
})

test('reconciles releases, resizing and property category transfers without retained-byte overlap', () => {
  const before = analyseSnapshot(
    fixture([
      [
        'synthetic',
        'root',
        1,
        0,
        [
          ['property', 'a', 1],
          ['property', 'b', 2],
        ],
      ],
      ['object', 'Object', 3, 10, []],
      ['object', 'Object', 5, 20, []],
    ])
  )
  const after = analyseSnapshot(
    fixture([
      [
        'synthetic',
        'root',
        1,
        0,
        [
          ['property', 'a', 1],
          ['property', 'c', 2],
        ],
      ],
      ['object', 'Object', 3, 15, []],
      ['object', 'Object', 7, 40, []],
    ])
  )
  const ledger = selfLedger(before, after, ['root', 'old', 'old'], ['root', 'new', 'new'])
  expect(ledger.old).toMatchObject({ removed: 20, transfer: -10, delta: -30 })
  expect(ledger.new).toMatchObject({ added: 40, transfer: 15, delta: 55 })
  expect(Object.values(ledger).reduce((sum, row) => sum + row.delta, 0)).toBe(25)
  const resized = selfLedger(before, after, ['root', 'same', 'old'], ['root', 'same', 'new'])
  expect(resized.same.resized).toBe(5)
})

test('ignores deleted WeakMap slots labelled with the V8 Hole sentinel', () => {
  const raw = fixture([
    [
      'synthetic',
      'root',
      1,
      0,
      [
        ['property', 'hole', 1],
        ['property', 'table', 2],
      ],
    ],
    ['object', 'system / Hole', 3, 0, [['internal', pairLabel(3, 3, 5), 1]]],
    ['array', '', 5, 40, [['internal', pairLabel(3, 3, 5), 1]]],
  ])
  expect(ephemeronLiveness(raw).pairs).toEqual([])
})

test('indexed incoming edges preserve distinct property labels from the same parent', () => {
  const raw = fixture([
    [
      'synthetic',
      'root',
      1,
      0,
      [
        ['property', 'left', 1],
        ['property', 'right', 1],
      ],
    ],
    ['object', 'Object', 3, 8, []],
  ])
  expect(
    analyseSnapshot(raw)
      .incoming(1)
      .map(({ edge }) => edge.name)
  ).toEqual(['left', 'right'])
})

test('attributes weak observation history without exempting strongly retained product data', () => {
  const raw = fixture([
    [
      'synthetic',
      'root',
      1,
      0,
      [
        ['property', 'observer', 1],
        ['property', 'live', 5],
      ],
    ],
    ['object', 'WeakOwners', 3, 32, [['property', 'rows', 2]]],
    ['object', 'Array', 5, 32, [['property', '0', 3]]],
    [
      'object',
      'Object',
      7,
      48,
      [
        ['property', 'weak', 4],
        ['property', 'accidental', 6],
        ['property', 'kind', 7],
      ],
    ],
    ['object', 'WeakRef', 9, 32, [['weak', 'target', 5]]],
    ['object', 'TelemetryMetrics', 11, 80, []],
    ['object', 'Object', 13, 96, []],
    ['string', 'running:handle', 15, 40, []],
  ])
  const graph = analyseSnapshot(raw)
  const { labels } = refineOwnership(graph)
  expect(labels[2]).toBe('harness:weak-observation-history')
  expect(labels[3]).toBe('harness:weak-observation-history')
  expect(labels[4]).toBe('harness:weak-observation-history')
  expect(labels[5]).not.toBe('harness:weak-observation-history')
  expect(labels[6]).not.toBe('harness:weak-observation-history')
  expect(labels[7]).toBe('harness:weak-observation-history')
})

test('cold backing ledger preserves store identities, aliases, replacements and release paths', () => {
  const { backingInventory, backingComparison } = require('../retained/coldOwnership')
  const snapshot = (id, size, alias = true) => {
    const graph = analyseSnapshot(
      fixture([
        ['synthetic', 'root', 1, 0, [['property', 'client', 1]]],
        [
          'object',
          'TelemetryPayload',
          3,
          40,
          [['property', 'retainedOnce', 2], ...(alias ? [['property', 'alias', 3]] : [])],
        ],
        ['object', 'Buffer', 5, 32, [['internal', 'buffer', 4]]],
        ['object', 'Buffer', 7, 32, alias ? [['internal', 'buffer', 4]] : []],
        ['object', 'ArrayBuffer', 9, 32, [['internal', 'backing_store', 5]]],
        ['object', 'system / JSArrayBufferData', id, size, []],
      ])
    )
    return { graph, labels: refineOwnership(graph).labels }
  }
  const old = backingInventory(snapshot(11, 1048576))
  expect(old).toHaveLength(1)
  expect(old[0]).toMatchObject({ id: 11, selfBytes: 1048576 })
  expect(old[0].aliases.map((entry) => entry.id)).toEqual([5, 7])
  expect(old[0].rootPath.map((entry) => entry.edge?.name)).toContain('retainedOnce')
  const next = backingInventory(snapshot(13, 65536, false))
  const replaced = backingComparison(old, next)
  expect(replaced.removed.map((row) => row.id)).toEqual([11])
  expect(replaced.added.map((row) => row.id)).toEqual([13])
  expect(replaced.delta).toBe(65536 - 1048576)
  expect(backingComparison(next, []).removed.map((row) => row.id)).toEqual([13])
  expect(backingComparison(old, old).surviving[0].id).toBe(11)
})
