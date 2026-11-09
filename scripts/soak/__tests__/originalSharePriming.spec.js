// eslint-disable-next-line n/no-unsupported-features/node-builtins -- count-only collection checks original weak roots on the supported Node 22+ test lane
const { queryObjects } = require('node:v8')
const { ShareSoakDriver } = require('../soak')
const { liveRun } = require('../../../src/producer/__tests__/fixtures/bounded')
const { verifySharePriming } = require('../../evidence/sharePriming')
const { originalShareSchedule } = require('./fixtures/originalShareSchedule')

/**
 * Checks weak original roots after a new job and a full collection, retaining no returned graph.
 * The count-only query performs collection; it never returns instances to this control.
 * @param {object} roots Weak references created by the first-use factory.
 * @returns {Promise<string[]>} Retained original roots by name.
 */
const retainedRoots = async (roots) => {
  await new Promise(setImmediate)
  queryObjects(ShareSoakDriver, { format: 'count' })
  return Object.entries(roots)
    .filter(([, ref]) => ref.deref() !== undefined)
    .map(([name]) => name)
}

describe('two-scope original share driver', () => {
  afterEach(() => liveRun.reset())

  test('archives decodable negotiation replies from the scripted public boundary', async () => {
    const { SoakTransport } = require('./fixtures/soakTransport')
    const { ShareFetchTap } = require('../soak')
    const tap = new ShareFetchTap()
    const rows = []
    tap.archive = (row) => rows.push(row)
    const transport = new SoakTransport(tap)
    try {
      const admin = transport.admin({ connect: async () => {}, disconnect: async () => {} })
      await admin.connect()
      await admin.disconnect()
      const requests = rows.filter((r) => r.kind === 'request' && r.apiKey === 18)
      expect(requests.map((r) => r.apiVersion)).toEqual([5, 4])
      for (const request of requests) {
        const reply = rows.find((r) => r.kind === 'response' && r.frame === request.frame)
        const codec = require(
          `../../../src/protocol/requests/apiVersions/v${request.apiVersion === 5 ? 0 : 4}/response`
        )
        const decoded = await codec.decode(Buffer.from(reply.bytes, 'base64').subarray(8))
        expect(decoded.errorCode).toBe(request.apiVersion === 5 ? 35 : 0)
        expect(decoded.apiVersions).toEqual(
          expect.arrayContaining([expect.objectContaining({ apiKey: 18, maxVersion: 4 })])
        )
      }
    } finally {
      transport.close()
    }
  })

  test.each([true, false])(
    'derives both complete workloads and cleanup from original operations, short=%s',
    async (short) => {
      let boundaryRoots
      const { owner, rows, result, error, primeRoots } = await originalShareSchedule({
        short,
        twoScopes: true,
        checkBoundaryRoots: async (roots) => {
          boundaryRoots = await retainedRoots(roots)
        },
      })
      expect(error).toBeUndefined()
      expect(result).toMatchObject({ ok: true, violations: [] })
      expect(verifySharePriming(JSON.parse(JSON.stringify(rows)))).toEqual([])
      expect(boundaryRoots).toEqual([])
      expect(await retainedRoots(primeRoots)).toEqual([])
      const workloadSockets = []
      for (const scope of ['priming', 'scored']) {
        const original = rows.filter((r) => r.scope === scope)
        const cycles = original.filter((r) => r.event === 'soak-cycle')
        const done = original.find((r) => ['priming-complete', 'case-complete'].includes(r.event))
        const requests = original.filter((r) => r.event === 'soak-wire' && r.kind === 'request')
        expect(cycles).toHaveLength(30)
        expect(cycles.filter((r) => r.reset)).toHaveLength(10)
        expect(done.cleanup).toMatchObject({ groupsAbsent: true, topicsAbsent: true })
        const verifiers = [...cycles.map((r) => r.verifier), done.final.verifier]
        expect(new Set(verifiers.map((v) => v.memberId)).size).toBe(31)
        const exchanges = [
          ...cycles.flatMap((r) => [...r.provenance.exchanges, ...r.provenance.heartbeats]),
          ...verifiers.flatMap((v) => [...v.provenance.exchanges, ...v.provenance.heartbeats]),
        ]
        const requestFrames = new Map(requests.map((r) => [`${r.socket}:${r.frame}`, r]))
        const responseFrames = new Map(
          original
            .filter((r) => r.event === 'soak-wire' && r.kind === 'response')
            .map((r) => [`${r.socket}:${r.frame}`, r])
        )
        const sockets = new Set()
        for (const exchange of exchanges)
          for (const frame of exchange.frames) {
            sockets.add(frame.socket)
            expect(requestFrames.get(`${frame.socket}:${frame.id}`)?.bytes).toBe(frame.bytes)
            if (frame.responseBytes)
              expect(responseFrames.get(`${frame.socket}:${frame.id}`)?.bytes).toBe(
                frame.responseBytes
              )
          }
        workloadSockets.push(sockets)
        for (const cycle of cycles.filter((r) => r.reset)) {
          expect(cycle.reset.oldSocket).not.toBe(cycle.reset.nextSocket)
          expect(sockets.has(cycle.reset.oldSocket)).toBe(true)
          expect(sockets.has(cycle.reset.nextSocket)).toBe(true)
          expect(cycle.steps.map((r) => r.step)).toEqual(
            expect.arrayContaining(['socket-close', 'SESSION_RESET', 'held-redelivery'])
          )
        }
        expect(cycles.at(-1).positions[1].ends.map((p) => p.high)).toEqual(['600', '600', '600'])
        expect(requests.filter((r) => r.apiKey === 0)).toHaveLength(90)
        expect(requests.filter((r) => r.apiKey === 1)).toHaveLength(31)
      }
      expect([...workloadSockets[0]].some((s) => workloadSockets[1].has(s))).toBe(false)
      expect(owner.priming).toMatchObject({ passes: 1, cycles: 30, resets: 10, verifiers: 31 })
    },
    600000
  )

  test('detects retained orchestration roots for the prime driver and its corpus', async () => {
    let boundaryRoots
    const { owner, primeRoots, result, error } = await originalShareSchedule({
      twoScopes: true,
      retainPrime: 'driver',
      checkBoundaryRoots: async (roots) => {
        boundaryRoots = await retainedRoots(roots)
      },
    })
    expect(error).toBeUndefined()
    expect(result.ok).toBe(true)
    expect(boundaryRoots).toEqual(expect.arrayContaining(['driver', 'corpus']))
    expect(await retainedRoots(primeRoots)).toEqual(expect.arrayContaining(['driver', 'corpus']))
    owner.retainedPrime = owner.retainedPrime.acknowledged
    const corpusOnly = await retainedRoots(primeRoots)
    expect(corpusOnly).toContain('corpus')
    expect(corpusOnly).not.toContain('driver')
    delete owner.retainedPrime
    expect(await retainedRoots(primeRoots)).toEqual([])
  }, 600000)
})
