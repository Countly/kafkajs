const fs = require('fs')
const { ordinaryOwnership } = require('../semantics')
const { verifyScenario } = require('../evidence')
const {
  buildShutdownModel,
  writeShutdownArchive,
} = require('../../../../testHelpers/segmentArchiveFixture')
const { bounded, delay, liveRun } = require('../../../../src/producer/__tests__/fixtures/bounded')

// A refused connection owns only its first, closed, successfully negotiated bootstrap socket.
const refusal = () => ({
  status: 'rejected',
  name: 'KafkaJSShareGroupsNotSupportedError',
  reason: 'api-version-range',
  apiKey: 78,
  shareVersion: 0,
})
const refusedWire = () => ({
  sockets: [{ id: 1, node: '1', kind: 'undecided', closedAt: 5 }],
  records: [
    { type: 'connect', socketId: 1, node: '1', at: 1 },
    {
      type: 'request',
      socketId: 1,
      node: '1',
      clientId: 'share',
      apiKey: 18,
      apiVersion: 5,
      writeAt: 2,
      response: { errorCode: 35, responseAt: 3, ranges: { 18: [0, 4] } },
    },
    {
      type: 'request',
      socketId: 1,
      node: '1',
      clientId: 'share',
      apiKey: 18,
      apiVersion: 4,
      writeAt: 3.5,
      response: { errorCode: 0, responseAt: 4, ranges: { 18: [0, 4], 78: [1, 1] } },
    },
  ],
})

describe('memory-gates refused bootstrap ownership', () => {
  test('accepts only the proven refused first bootstrap after fallback and close', () => {
    expect(ordinaryOwnership(refusedWire(), { refusal: refusal() })).toEqual([
      '1:refused-bootstrap',
    ])
  })
  test('requires the absent advertised capability for the 4.0.2 refusal', () => {
    const t = refusedWire()
    const outcome = { ...refusal(), reason: 'api-not-advertised', apiKey: 76 }
    expect(ordinaryOwnership(t, { refusal: outcome })).toEqual(['1:refused-bootstrap'])
    t.records[2].response.ranges[76] = [0, 0]
    expect(() => ordinaryOwnership(t, { refusal: outcome })).toThrow()
  })
  test.each([
    ['an extra setup socket', (t) => t.sockets.push({ id: 2, node: '1', kind: 'normal' })],
    [
      'a telemetry socket',
      (t) => {
        t.sockets[0].kind = 'telemetry'
      },
    ],
    [
      'an unclosed socket',
      (t) => {
        delete t.sockets[0].closedAt
      },
    ],
    [
      'a failed fallback',
      (t) => {
        t.records[2].response.errorCode = 35
      },
    ],
    [
      'a compatible capability',
      (t) => {
        t.records[2].response.ranges[78] = [0, 1]
      },
    ],
    [
      'a different first physical connect',
      (t) => {
        t.records[0].socketId = 2
      },
    ],
    [
      'extra setup traffic',
      (t) => {
        t.records.push({ ...t.records[2] })
      },
    ],
  ])('rejects %s', (_name, change) => {
    const wire = refusedWire()
    change(wire)
    expect(() => ordinaryOwnership(wire, { refusal: refusal() })).toThrow()
  })
})

describe('memory-gates foreground fetch setup ownership', () => {
  const wire = () => ({
    sockets: [
      { id: 1, node: '1', kind: 'normal', foregroundBootstrap: true },
      { id: 2, node: '2', kind: 'undecided' },
      { id: 3, node: '2', kind: 'normal' },
    ],
    records: [
      {
        type: 'request',
        socketId: 2,
        apiKey: 18,
        clientId: 'share',
        response: {
          errorCode: 0,
          responseAt: 5,
          ranges: { 78: [0, 2] },
        },
      },
      { type: 'request', socketId: 3, apiKey: 78, clientId: 'share', writeAt: 6, response: {} },
    ],
  })
  test('replays setup ownership from an answered sibling fetch without relabelling the archive', () => {
    const t = wire()
    expect(ordinaryOwnership(t)).toEqual(['1:bootstrap', '2:bootstrap', '2:ordinary'])
    expect(t.sockets[1].kind).toBe('undecided')
  })
  test.each([
    ['extra setup socket', (t) => t.sockets.push({ id: 4, node: '2', kind: 'undecided' })],
    [
      'telemetry advertisement',
      (t) => {
        t.records[0].response.ranges[71] = [0, 0]
      },
    ],
    [
      'unanswered fetch',
      (t) => {
        delete t.records[1].response
      },
    ],
    [
      'fetch before negotiation',
      (t) => {
        t.records[1].writeAt = 4
      },
    ],
    [
      'foreign client',
      (t) => {
        t.records[1].clientId = 'other'
      },
    ],
  ])('rejects %s as setup ownership proof', (_name, mutate) => {
    const t = wire()
    mutate(t)
    expect(() => ordinaryOwnership(t)).toThrow()
  })
})

describe('D142 shutdown archive verdicts', () => {
  let archive
  afterEach(() => {
    if (archive) fs.rmSync(archive.directory, { recursive: true, force: true })
  })
  test.each([
    [
      'pending identity',
      (m) => {
        m.results.resources.final.identities.pending = [99]
      },
      'Owned resources survive the quiet window',
    ],
    [
      'listener registration',
      (m) => {
        m.results.resources.final.registrations.process.events.exit.push(99)
      },
      'Listener registrations differ',
    ],
    [
      'owned timer',
      (m) => {
        m.segments.get('producer-ep-0').transcript.cleanup.timers = 1
      },
      'timers',
    ],
    [
      'observer listener',
      (m) => {
        m.segments.get('producer-ep-0').transcript.cleanup.attached = 1
      },
      'attached',
    ],
    [
      'unclosed socket',
      (m) => {
        delete m.segments.get('producer-ep-0').transcript.sockets[0].closedAt
      },
      'A segment socket never closed',
    ],
  ])(
    'keeps a bad %s ledger functional but fails the required memory verdict',
    (_name, mutate, message) => {
      const { verifyMemoryScenario } = require('../memoryVerdicts')
      const model = buildShutdownModel()
      mutate(model)
      archive = writeShutdownArchive(model)
      expect(() => verifyScenario(archive.record, archive.directory)).not.toThrow()
      expect(() => verifyMemoryScenario(archive.record, archive.directory)).toThrow(message)
    }
  )
})

describe('configured share oracle envelope', () => {
  beforeEach(() => {
    jest.useFakeTimers()
    liveRun.reset()
  })
  afterEach(() => {
    jest.useRealTimers()
    liveRun.reset()
  })
  const run = (options) =>
    require('../shareEnvelope').runShareEnvelope({
      signal: new AbortController().signal,
      remainingMs: 30000,
      bounded,
      taint: (e) => liveRun.taint(e),
      ...options,
    })
  test('fits cold recovery and assignment, the full 3000 ms observation and cleanup', async () => {
    const events = []
    const start = Date.now()
    const result = run({
      work: async (scope) => {
        for (const [name, ms] of [
          ['fault recovery', 8500],
          ['assignment', 8000],
          ['verifier startup', 5000],
          ['observation', 3000],
          ['raw oracle', 1000],
        ]) {
          await delay(scope, ms)
          events.push([name, Date.now() - start])
        }
        return 'oracle passed'
      },
      cleanup: async (scope) => {
        await delay(scope, 1900)
        events.push(['cleanup', Date.now() - start])
      },
    })
    await jest.advanceTimersByTimeAsync(29400)
    await expect(result).resolves.toBe('oracle passed')
    expect(events).toEqual([
      ['fault recovery', 8500],
      ['assignment', 16500],
      ['verifier startup', 21500],
      ['observation', 24500],
      ['raw oracle', 25500],
      ['cleanup', 27400],
    ])
  })
  test('rejects over-budget work and still joins local cleanup', async () => {
    let cleaned = false
    const result = run({
      work: (scope) => delay(scope, 29000),
      cleanup: async (scope) => {
        await delay(scope, 1900)
        cleaned = true
      },
    })
    await Promise.all([
      expect(result).rejects.toThrow('28000 ms'),
      jest.advanceTimersByTimeAsync(30000),
    ])
    expect(cleaned).toBe(true)
  })
  test('refuses admission below the full envelope before doing work', async () => {
    let started = false
    await expect(
      run({
        remainingMs: 29999,
        work: () => {
          started = true
        },
        cleanup: async () => {},
      })
    ).rejects.toThrow('remaining budget')
    expect(started).toBe(false)
  })
  test('fails and taints an over-budget cleanup', async () => {
    const result = run({ work: async () => 'ok', cleanup: (scope) => delay(scope, 2100) })
    await Promise.all([
      expect(result).rejects.toThrow('2000 ms'),
      jest.advanceTimersByTimeAsync(2500),
    ])
    expect(liveRun.tainted).toBe(true)
  })
})
