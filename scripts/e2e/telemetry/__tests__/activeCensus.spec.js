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
})
