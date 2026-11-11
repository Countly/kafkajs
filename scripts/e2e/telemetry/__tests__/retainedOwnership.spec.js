const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { spawnSync } = require('node:child_process')
const { analyseSnapshot, auditReferences, readSnapshot } = require('../retained/heapDiff')
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

describe('readSnapshot', () => {
  let directory
  let filename

  beforeEach(() => {
    directory = fs.mkdtempSync(path.join(os.tmpdir(), 'read-snapshot-'))
    filename = path.join(directory, 'small.heapsnapshot')
  })

  afterEach(() => fs.rmSync(directory, { recursive: true, force: true }))

  const smallSnapshot = () => ({
    snapshot: {
      node_count: 1,
      edge_count: 0,
      meta: {
        node_fields: ['type', 'name', 'id', 'self_size', 'edge_count'],
        node_types: [['synthetic']],
        edge_fields: ['type', 'name_or_index', 'to_node'],
        edge_types: [['property']],
      },
    },
    nodes: [0, 0, 4294967297, 0, 0],
    edges: [],
    strings: ['root'],
  })

  test('matches JSON parsing when analysing a real V8 heap snapshot', () => {
    const child = spawnSync(
      process.execPath,
      [
        '-e',
        `class SnapshotReaderSentinel {}
         globalThis.sentinel = new SnapshotReaderSentinel()
         require('node:v8').writeHeapSnapshot(process.argv[1])`,
        filename,
      ],
      { encoding: 'utf8', timeout: 30000 }
    )
    expect(child.error).toBeUndefined()
    expect(child.status).toBe(0)
    const raw = JSON.parse(fs.readFileSync(filename, 'utf8'))
    const parsed = readSnapshot(filename)
    expect(parsed.snapshot).toEqual(raw.snapshot)
    expect(ArrayBuffer.isView(parsed.nodes)).toBe(true)
    expect(ArrayBuffer.isView(parsed.edges)).toBe(true)
    expect(Array.from(parsed.nodes)).toEqual(raw.nodes)
    expect(Array.from(parsed.edges)).toEqual(raw.edges)
    expect(parsed.strings).toEqual(raw.strings)
    const expected = analyseSnapshot(raw)
    const actual = analyseSnapshot(parsed)
    expect(actual.n).toBe(expected.n)
    expect(actual.describe(actual.root)).toEqual(expected.describe(expected.root))
    const known = (graph) =>
      graph.groups.find(({ type, name }) => type === 'object' && name === 'SnapshotReaderSentinel')
    expect(known(expected)).toMatchObject({ count: 1 })
    expect(known(actual)).toEqual(known(expected))
  })

  test('preserves escaped and non-ASCII strings while skipping other sections', () => {
    const raw = smallSnapshot()
    raw.nodes[2] = Number.MAX_SAFE_INTEGER
    raw.strings.push(
      'quote " comma , braces } ]',
      'backslash \\',
      '\n\r\t\b\f\u0000',
      'café 雪 😀',
      '\ud800'
    )
    const json = JSON.stringify({
      before: { nested: [1, { label: 'escaped " } ] \\' }] },
      strings: raw.strings,
      edges: raw.edges,
      snapshot: raw.snapshot,
      nodes: raw.nodes,
      after: [[], {}, true, null, -1.5e2],
    })
    fs.writeFileSync(filename, json.replace('café', 'caf\\u00e9'))
    const parsed = readSnapshot(filename)
    expect(parsed.strings).toEqual(raw.strings)
    expect(Array.from(parsed.nodes)).toEqual(raw.nodes)
    expect(Object.keys(parsed).sort()).toEqual(['edges', 'nodes', 'snapshot', 'strings'])
    expect(analyseSnapshot(parsed).describe(0).id).toBe(Number.MAX_SAFE_INTEGER)
  })

  test('rejects truncated or malformed snapshots with a clear error', () => {
    const json = JSON.stringify(smallSnapshot())
    const malformed = [
      '',
      json.slice(0, -1),
      json.slice(0, json.indexOf('"strings"') + 14),
      json.replace('"strings":["root"]', '"strings":["unterminated]'),
      json.replace('"strings":["root"]', '"strings":["bad\\q"]'),
      json.replace('"strings":["root"]', '"strings":[123]'),
      json.replace('"strings":["root"]', '"strings":["root",]'),
      json.replace(',"edges":[]', ''),
      json.replace(',"strings":["root"]', ''),
      json.replace('"nodes":', '"missing_nodes":'),
      json.replace('"snapshot":', '"missing":'),
      json.replace('"node_count":1', '"node_count":2'),
      json.replace('"edge_count":0', '"edge_count":1'),
      json.replace('"nodes":[0,0,4294967297,0,0]', '"nodes":[0,0,1,0]'),
      json.replace('"edges":[]', '"edges":[0]'),
      json.replace('4294967297', '9007199254740992'),
      json.replace('4294967297', '1.5'),
      json.replace('4294967297', '01'),
      json.replace('4294967297', '-1'),
      json.replace('4294967297', 'null'),
      json.replace('4294967297,0,0', '4294967297,0,0,'),
      json.replace('"edge_count":0,', '"edge_count":-1,'),
      json.replace('"node_fields":[', '"missing_fields":['),
      `${json} trailing`,
      json.replace('"nodes":', '"nodes":[],"nodes":'),
    ]
    for (const input of malformed) {
      fs.writeFileSync(filename, input)
      expect(() => readSnapshot(filename)).toThrow(/Malformed heap snapshot:/)
    }
  })
})
