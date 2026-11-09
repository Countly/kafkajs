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
