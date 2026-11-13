'use strict'

const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { digest } = require('../retained/contract')
const { main } = require('../closeSnapshotDiagnostic')
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
  totals: {
    unresolved: nodes
      .filter((node) => node.category === 'unresolved')
      .reduce((sum, node) => sum + node.selfBytes, 0),
  },
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

describe('closed snapshot evidence authentication', () => {
  let directory
  let result
  let events
  const writeEvents = () =>
    fs.writeFileSync(
      path.join(directory, 'census-events.jsonl'),
      events.map(JSON.stringify).join('\n')
    )

  beforeEach(() => {
    directory = fs.mkdtempSync(path.join(os.tmpdir(), 'closed-snapshot-test-'))
    const identity = {
      childId: 'child',
      tree: 'tree',
      runId: 'run',
      kind: 'producer',
      node: 'node',
      v8: 'v8',
      variant: 'capture',
      seed: null,
      diagnostic: true,
      acceptanceEligible: false,
      attributionComplete: false,
      pid: 123,
      flags: ['--expose-gc'],
    }
    fs.writeFileSync(path.join(directory, 'census-identity.json'), JSON.stringify(identity))
    events = ['M', 'closed'].map((name, index) => {
      const bytes = Buffer.from(`snapshot-${name}`)
      fs.writeFileSync(path.join(directory, `${name}.heapsnapshot`), bytes)
      return {
        diagnostic: true,
        acceptanceEligible: false,
        attributionComplete: false,
        name,
        reading: reading(index * 10),
        processRetainedBytes: 1200,
        countersBefore: { spaces: [] },
        startedAt: index * 10 + 1,
        endedAt: index * 10 + 2,
        file: `${name}.heapsnapshot`,
        bytes: bytes.length,
        sha256: digest(bytes),
      }
    })
    writeEvents()
    result = {
      complete: true,
      identity: { ...identity, seed: 'unseeded' },
      pid: 123,
      readings: {
        diagnostic: true,
        acceptanceEligible: false,
        manifest: { execArgv: identity.flags },
        matched: { ...events[0].reading, registry: {} },
        closed: { ...events[1].reading, registry: {} },
        closeSnapshot: {
          diagnostic: true,
          acceptanceEligible: false,
          attributionComplete: false,
          complete: true,
          records: events.map((event) => ({
            ...event,
            afterSnapshot: reading(event.endedAt + 1),
            countersAfter: { heap: {} },
          })),
        },
      },
    }
  })
  afterEach(() => fs.rmSync(directory, { recursive: true, force: true }))

  test('accepts only joined same-child M and closed captures with original scalar seams', () => {
    expect(validateChild(directory, result).map((event) => event.name)).toEqual(['M', 'closed'])
    result.readings.matched.heapUsed++
    expect(() => validateChild(directory, result)).toThrow(/original reading/)
  })

  test('rejects changed snapshot bytes and foreign child identities', () => {
    fs.appendFileSync(path.join(directory, 'M.heapsnapshot'), 'changed')
    expect(() => validateChild(directory, result)).toThrow(/digest/)
    result.identity.childId = 'foreign'
    expect(() => validateChild(directory, result)).toThrow(/foreign/)
  })

  test('rejects reordered captures and a snapshot preceding its original reading', () => {
    events.reverse()
    writeEvents()
    expect(() => validateChild(directory, result)).toThrow(/original reading/)
    events.reverse()
    events[0].startedAt = -1
    writeEvents()
    expect(() => validateChild(directory, result)).toThrow(/original reading/)
  })

  test('refuses to launch outside a manual isolated CI dispatch', async () => {
    const old = { CI: process.env.CI, GITHUB_EVENT_NAME: process.env.GITHUB_EVENT_NAME }
    try {
      process.env.CI = 'true'
      process.env.GITHUB_EVENT_NAME = 'pull_request'
      await expect(main(['--diagnostic-only', directory, 'capture'])).rejects.toThrow(/isolated CI/)
    } finally {
      for (const [key, value] of Object.entries(old)) {
        if (value === undefined) delete process.env[key]
        else process.env[key] = value
      }
    }
  })
})
