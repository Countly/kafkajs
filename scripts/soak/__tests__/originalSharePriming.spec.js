jest.mock('../../ci/shareSoakJob', () => ({
  ...jest.requireActual('../../ci/shareSoakJob'),
  sourceIdentity: jest.fn(),
}))
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { sourceIdentity } = require('../../ci/shareSoakJob')
const { validateArchive } = require('../../evidence/shareSoakAcceptance')
const { shareArchive } = require('./fixtures/shareArchive')
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
    'derives both complete workloads, cleanup and sealed archive verdicts from original operations, short=%s',
    async (short) => {
      let boundaryRoots
      const { owner, rows, result, error, primeRoots } = await originalShareSchedule({
        short,
        correlateCleanup: true,
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
      const root = fs.mkdtempSync(path.join(os.tmpdir(), 'share-archive-'))
      const previous = { ...process.env }
      try {
        const archive = shareArchive(root, rows, short)
        sourceIdentity.mockReturnValue(archive.contract.source)
        Object.assign(process.env, archive.platform)
        const first = await validateArchive(archive.contract, root, { sealed: false })
        expect(first.problems).toEqual([])
        archive.finish(first)
        const replay = await validateArchive(archive.contract, root, {
          provenance: archive.provenance,
        })
        expect(replay.problems).toEqual([])
        expect(replay.semanticSha256).toBe(first.semanticSha256)
        const cases = [
          [
            'soak-producer',
            (r) => {
              r.sent[0].baseOffset = '1'
            },
            /producer acknowledgement/,
          ],
          [
            'soak-raw-log',
            (r) => {
              r.records.pop()
            },
            /raw records\/end reads/,
          ],
          [
            'soak-membership',
            (r) => {
              r.members = []
            },
            /assignment barriers|ownership differs/,
          ],
          [
            'soak-reset',
            (r) => {
              r.oldSocket = -1
            },
            /reset rows/,
          ],
          [
            'soak-configuration',
            (r) => {
              r.config.topicMetadata.topics[0].partitions[0].replicas.pop()
            },
            /replication\/retention/,
          ],
          [
            'soak-cleanup',
            (r) => {
              r.after.queueDepth++
            },
            /cleanup populations/,
          ],
        ]
        for (const [event, mutate, expected] of cases) {
          const index = archive.rows.findIndex(
            (r) =>
              r.scope === 'scored' &&
              r.event === event &&
              (event !== 'soak-cleanup' || r.step === 'cleanup-resources')
          )
          expect(index).toBeGreaterThanOrEqual(0)
          const before = archive.rows[index]
          archive.rows[index] = JSON.parse(JSON.stringify(before))
          mutate(archive.rows[index])
          archive.writeRows()
          archive.seal()
          const rejected = await validateArchive(archive.contract, root, {
            provenance: archive.provenance,
          })
          expect(rejected.problems.join('; ')).toMatch(expected)
          expect(rejected.problems).not.toContain('Changed/incomplete artifact seal')
          archive.rows[index] = before
        }
        archive.writeRows()
        archive.seal()
        const foreign = await validateArchive(archive.contract, root, {
          provenance: { ...archive.provenance, attempt: '2' },
        })
        expect(foreign.problems).toContain(
          'Missing or mismatched GitHub producer/artifact provenance'
        )
        sourceIdentity.mockReturnValue({ ...archive.contract.source, commit: 'f'.repeat(40) })
        expect(
          (await validateArchive(archive.contract, root, { provenance: archive.provenance }))
            .problems
        ).toContain('Frozen source identity differs')
      } finally {
        process.env = previous
        fs.rmSync(root, { recursive: true, force: true })
      }
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
