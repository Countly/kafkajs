'use strict'

const { CloseSnapshot, createCloseSnapshot } = require('../retained/closeSnapshot')
const { analysePair, validateChild } = require('../retained/analyseCloseSnapshot')

const reading = (at) => ({ at, heapUsed: 1000, external: 200, retained: 1200 })
const node = (id, selfBytes, category, name = 'Object') => ({
  id,
  selfBytes,
  category,
  name,
  type: 'object',
  label: category,
})
const census = (nodes) => ({
  nodes,
  totalSelfBytes: nodes.reduce((sum, entry) => sum + entry.selfBytes, 0),
})
const graph = { rootPath: (index) => [{ node: { id: index + 100 }, edge: null }] }

describe('closed snapshot diagnostic', () => {
  test('does nothing without its environment switch', () => {
    const capture = jest.fn()
    expect(createCloseSnapshot({ env: {}, capture })).toBeNull()
    expect(capture).not.toHaveBeenCalled()
  })

  test('captures exactly the original M then closed without collecting or resettling', () => {
    const capture = jest.fn()
    const current = jest.fn(() => reading(3))
    const counters = jest.fn(() => ({ spaces: [{ space_name: 'old_space' }] }))
    const probe = new CloseSnapshot({ memory: { current }, capture, counters })
    const matched = reading(1)
    const closed = reading(2)
    probe.record('M', matched)
    probe.record('closed', closed)
    expect(capture.mock.calls.map(([name, facts]) => [name, facts.reading])).toEqual([
      ['M', matched],
      ['closed', closed],
    ])
    expect(capture.mock.calls[0][1].reading).toBe(matched)
    expect(current).toHaveBeenCalledTimes(2)
    expect(counters).toHaveBeenCalledTimes(4)
    expect(probe.summary()).toMatchObject({
      diagnostic: true,
      acceptanceEligible: false,
      attributionComplete: false,
      complete: true,
    })
    expect(() => probe.record('closed', closed)).toThrow(/order/)
  })

  test('refuses closed before M and incompatible instrumentation', () => {
    const probe = new CloseSnapshot({ memory: {}, capture: jest.fn() })
    expect(() => probe.record('closed', reading(1))).toThrow(/order/)
    for (const key of ['ACTIVE_CENSUS_DIRECTORY', 'RESOURCE_WITNESSES'])
      expect(() =>
        createCloseSnapshot({
          env: { RESOURCE_CLOSE_SNAPSHOT_DIRECTORY: 'unused', [key]: '1' },
        })
      ).toThrow(/isolated/)
  })

  test('reconciles disappearance, resizing, additions and category transfers by stable ID', () => {
    const before = census([
      node(1, 100, 'product', 'ClientTelemetry'),
      node(2, 200, 'runtime', 'FeedbackVector'),
      node(3, 50, 'harness'),
      node(4, 30, 'unresolved'),
    ])
    const after = census([
      node(2, 120, 'runtime', 'FeedbackVector'),
      node(3, 50, 'product'),
      node(5, 512, 'buffers', 'JSArrayBufferData'),
    ])
    const result = analysePair(before, after, graph, graph)
    expect(result.delta).toBe(302)
    expect(result.releasedSelfBytes).toBe(130)
    expect(result.shrunkSelfBytes).toBe(80)
    expect(result.categories.product).toMatchObject({ removed: 100, transfer: 50, delta: -50 })
    expect(result.categories.runtime.resized).toBe(-80)
    expect(result.disappeared[0]).toMatchObject({ name: 'ClientTelemetry', selfBytes: 100 })
    expect(result.disappeared[0].examples[0].rootPath).toEqual(graph.rootPath(0))
    expect(result.shrunk[0]).toMatchObject({ name: 'FeedbackVector', selfBytes: 80 })
    expect(result.grown[0]).toMatchObject({ name: 'JSArrayBufferData', selfBytes: 512 })
    expect(result.acceptanceEligible).toBe(false)
  })

  test('does not count equal-sized replacement as a surviving object', () => {
    const result = analysePair(
      census([node(1, 100, 'harness')]),
      census([node(2, 100, 'harness')]),
      graph,
      graph
    )
    expect(result.delta).toBe(0)
    expect(result.releasedSelfBytes).toBe(100)
    expect(result.categories.harness).toMatchObject({ added: 100, removed: 100 })
  })

  test('refuses an incomplete or acceptance-eligible child before reading its archive', () => {
    expect(() => validateChild('unused', { complete: false })).toThrow(/incomplete/)
    expect(() =>
      validateChild('unused', { complete: true, readings: { acceptanceEligible: true } })
    ).toThrow(/incomplete/)
  })
})
