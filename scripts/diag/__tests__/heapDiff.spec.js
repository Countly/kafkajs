const { analyseSnapshot, compareSnapshots } = require('../heapDiff')

/** Builds a tiny graph with shared children, a cycle, nested same-type owners and a weak edge. */
const fixture = (extra = false) => {
  const strings = ['root', 'Owner', 'Payload', 'WeakOnly', 'owner', 'nested', 'payload', 'weak']
  const nodes = [
    [0, 0, 1, 0, 1],
    [1, 1, 3, 10, 3],
    [1, 1, 5, 20, 1],
    [1, 2, 7, 30, 1],
    [1, 3, 9, 1000, 0],
  ]
  const edges = [
    [0, 4, 5],
    [0, 5, 10],
    [0, 6, 15],
    [1, 7, 20],
    [0, 6, 15],
    [0, 4, 5],
  ]
  if (extra) {
    nodes[1][4]++
    nodes.push([1, 2, 11, 40, 0])
    edges.splice(4, 0, [0, 6, 25])
  }
  return {
    snapshot: {
      meta: {
        node_fields: ['type', 'name', 'id', 'self_size', 'edge_count'],
        node_types: [['synthetic', 'object']],
        edge_fields: ['type', 'name_or_index', 'to_node'],
        edge_types: [['property', 'weak']],
      },
      node_count: nodes.length,
      edge_count: edges.length,
    },
    nodes: nodes.flat(),
    edges: edges.flat(),
    strings,
  }
}

test('attributes shared cyclic descendants without double counting nested constructors or weak roots', () => {
  const result = analyseSnapshot(fixture())
  expect(result.groups.find(({ name }) => name === 'Owner')).toMatchObject({
    count: 2,
    selfBytes: 30,
    retainedBytes: 60,
  })
  expect(result.groups.find(({ name }) => name === 'WeakOnly')).toBeUndefined()
  expect(result.unreachable).toEqual({ count: 1, selfBytes: 1000 })
  expect(result.retained[1]).toBe(60)
  expect(result.retained[2]).toBe(20)
  expect(result.idom[3]).toBe(1)
})

test('reports growth and a strong labelled root path with stable snapshot IDs', () => {
  const baseline = analyseSnapshot(fixture())
  const end = analyseSnapshot(fixture(true))
  const diff = compareSnapshots(baseline, end)
  const payload = diff.groups.find(({ name }) => name === 'Payload')
  expect(payload.delta).toEqual({ count: 1, selfBytes: 40, retainedBytes: 40 })
  expect(payload.samples[0]).toMatchObject({ id: 11, presentAtBaseline: false, selfBytes: 40 })
  expect(payload.samples[0].rootPath.map(({ node }) => node.name)).toEqual([
    'root',
    'Owner',
    'Payload',
  ])
  expect(payload.samples[0].rootPath.at(-1).edge).toMatchObject({
    type: 'property',
    name: 'payload',
  })
})

test('rejects malformed snapshot edge targets', () => {
  const snapshot = fixture()
  snapshot.edges[2] = 7
  expect(() => analyseSnapshot(snapshot)).toThrow(/edge target/)
})

test('shows dominator and retained growth for surviving owners', () => {
  const diff = compareSnapshots(analyseSnapshot(fixture()), analyseSnapshot(fixture(true)))
  const owner = diff.groups.find(({ name }) => name === 'Owner').samples.find(({ id }) => id === 3)
  expect(owner).toMatchObject({
    presentAtBaseline: true,
    baselineNode: { retainedBytes: 60 },
    retainedDeltaBytes: 40,
  })
  expect(owner.baselineDominators.map(({ id }) => id)).toEqual([1])
})

test('writes full JSON and readable rankings from raw and gzip synthetic inputs', () => {
  const fs = require('node:fs')
  const os = require('node:os')
  const path = require('node:path')
  const zlib = require('node:zlib')
  const { main } = require('../heapDiff')
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'heap-diff-control-'))
  const baseline = path.join(directory, 'baseline.heapsnapshot')
  const end = path.join(directory, 'end.heapsnapshot.gz')
  const out = path.join(directory, 'analysis')
  const log = jest.spyOn(console, 'log').mockImplementation(() => {})
  try {
    fs.writeFileSync(baseline, JSON.stringify(fixture()))
    fs.writeFileSync(end, zlib.gzipSync(JSON.stringify(fixture(true))))
    main([baseline, end, end, '--out', out])
    const report = JSON.parse(fs.readFileSync(`${out}.json`, 'utf8'))
    expect(report.comparisons).toHaveLength(2)
    expect(report.comparisons[0].groups.find(({ name }) => name === 'Payload').delta.count).toBe(1)
    expect(
      report.comparisons[1].groups.find(({ name }) => name === 'Payload').sincePrevious.count
    ).toBe(0)
    const text = fs.readFileSync(`${out}.txt`, 'utf8')
    expect(text).toContain('Top growing groups by retainedBytes')
    expect(text).toContain('ROOT synthetic/root#1')
    expect(text).toContain('DOMINATORS')
  } finally {
    log.mockRestore()
    fs.rmSync(directory, { recursive: true, force: true })
  }
})

test('matches deletion-based retained sizes on deterministic graphs with joins and back edges', () => {
  let seed = 27
  const random = () => {
    seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0
    return seed
  }
  for (let trial = 0; trial < 20; trial++) {
    const graph = Array.from({ length: 12 }, () => [])
    for (let v = 0; v < 12; v++) {
      if (v < 11) graph[v].push(v + 1)
      for (let edge = 0; edge < 3; edge++) graph[v].push(random() % 12)
    }
    const snapshot = fixture()
    snapshot.nodes = graph.flatMap((targets, v) => [
      v === 0 ? 0 : 1,
      1,
      2 * v + 1,
      v + 1,
      targets.length,
    ])
    snapshot.edges = graph.flatMap((targets) => targets.flatMap((v) => [0, 4, v * 5]))
    snapshot.snapshot.node_count = 12
    snapshot.snapshot.edge_count = snapshot.edges.length / 3
    const actual = analyseSnapshot(snapshot)
    for (let removed = 0; removed < 12; removed++) {
      const reached = new Set()
      const visit = (v) => {
        if (v === removed || reached.has(v)) return
        reached.add(v)
        for (const child of graph[v]) visit(child)
      }
      visit(0)
      const expected = graph.reduce((bytes, _, v) => bytes + (reached.has(v) ? 0 : v + 1), 0)
      expect(actual.retained[removed]).toBe(expected)
    }
  }
})
