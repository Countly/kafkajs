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
require('node:v8').setFlagsFromString('--expose-gc')
const collect = require('node:vm').runInNewContext('gc')
const { liveRun } = require('../../../src/producer/__tests__/fixtures/bounded')
const { verifySharePriming } = require('../../evidence/sharePriming')
const { originalShareSchedule } = require('./fixtures/originalShareSchedule')
const { diagnosePrimeRoots } = require('./fixtures/primeRetainers')

/**
 * Checks weak original roots after a new job and an asynchronous major collection.
 * Async GC avoids conservative stack roots; only names escape, without heap enumeration.
 * @param {object} roots Weak references created by the first-use factory.
 * @returns {Promise<string[]>} Retained original roots by name.
 */
const retainedRoots = async (roots) => {
  await new Promise(setImmediate)
  await collect({ type: 'major', execution: 'async' })
  return Object.entries(roots)
    .filter(([, ref]) => ref.deref() !== undefined)
    .map(([name]) => name)
}

describe('two-scope original share driver', () => {
  afterEach(() => liveRun.reset())

  test('selects API 60 through the real public admin during share setup', async () => {
    const createAdmin = require('../../../src/admin')
    const { createLogger, LEVELS } = require('../../../src/loggers')
    const { ShareSoakDriver } = require('../soak')
    const brokers = [{ nodeId: 0, host: 'node0', port: 9092 }]
    const broker = {
      negotiatedVersion: () => 2,
      describeCluster: jest.fn(async () => ({
        brokers: [{ brokerId: 0, host: 'node0', port: 9092 }],
        controllerId: 0,
      })),
    }
    const cluster = {
      metadata: jest.fn(async () => ({ brokers, controllerId: 0 })),
      findConnectedBroker: async () => broker,
    }
    const publicAdmin = createAdmin({
      cluster,
      logger: createLogger({ level: LEVELS.NOTHING, logCreator: () => () => {} }),
      retry: { retries: 0 },
    })
    const admin = {
      connect: async () => {},
      disconnect: async () => {},
      createTopics: async () => {},
      fetchTopicMetadata: async ({ topics: [name] }) => ({
        topics: [{ name, topicId: 'topic-id', partitions: [{ partitionId: 0, leader: 0 }] }],
      }),
      describeCluster: publicAdmin.describeCluster,
      listGroups: async () => ({ groups: [] }),
    }
    const driver = new ShareSoakDriver({
      kafka: { admin: () => admin },
      shares: {},
      ports: {
        bounded: { bounded: async (scope, ms, work) => work() },
        fixtures: {
          setShareGroupConfigs: async () => {},
          captureShareCleanupBaseline: async () => ({}),
        },
      },
    })
    const configuration = jest.spyOn(driver, 'readConfiguration').mockResolvedValue({})
    try {
      await driver.prepare({ add: () => {} }, new AbortController().signal)
      expect(broker.describeCluster).toHaveBeenCalledTimes(1)
      expect(broker.describeCluster).toHaveBeenCalledWith({
        includeClusterAuthorizedOperations: true,
        includeFencedBrokers: false,
      })
      expect(cluster.metadata).not.toHaveBeenCalled()
      expect(driver.nodes).toEqual(new Map([['node0:9092', 0]]))
    } finally {
      configuration.mockRestore()
    }
  })

  test('matches scripted cluster discovery to the public admin option semantics', async () => {
    const { SoakTransport } = require('./fixtures/soakTransport')
    const transport = new SoakTransport()
    const legacy = transport.adminRequest('describeCluster', [])
    expect(legacy.apiKey).toBe(3)
    const explicit = transport.adminRequest('describeCluster', [
      { includeAuthorizedOperations: true },
    ])
    expect(explicit.apiKey).toBe(60)
    expect((await explicit.encode()).buffer[0]).toBe(1)
  })

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

  test('keeps scripted reply mutations separate from original metadata and assignment results', () => {
    const { SoakTransport } = require('./fixtures/soakTransport')
    const cases = [
      [
        'fetchTopicMetadata',
        { topics: [{ name: 't', partitions: [{ partitionId: 0, replicas: [0, 1, 2] }] }] },
        (fields) => fields.Topics[0].Partitions[0].ReplicaNodes.pop(),
      ],
      [
        'describeShareGroups',
        {
          groups: [
            { groupId: 'g', members: [{ assignment: [{ topic: 't', partitions: [0, 1, 2] }] }] },
          ],
        },
        (fields) => fields.Groups[0].Members[0].Assignment.TopicPartitions[0].Partitions.pop(),
      ],
    ]
    for (const [name, result, mutate] of cases) {
      const original = JSON.parse(JSON.stringify(result))
      const fields = SoakTransport.prototype.resultFields(name, [], result)
      mutate(fields)
      expect(result).toEqual(original)
      const pristine = SoakTransport.prototype.resultFields(name, [], result)
      expect(fields).not.toEqual(pristine)
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
          if (boundaryRoots.length) await diagnosePrimeRoots(`boundary, short=${short}`)
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
          const writes = cycle.steps.filter((r) => r.step === 'post-reset-api78')
          expect([...new Set(writes.map((r) => r.nodeId))].sort()).toEqual([0, 1, 2])
          const disconnect = cycle.steps.find(
            (r) => r.member === 'A' && r.step === 'disconnect-start'
          )
          for (const write of writes) {
            expect(write.at).toBeLessThanOrEqual(disconnect.at)
            const exchange = cycle.provenance.exchanges.find((r) => r.id === write.exchange)
            const frame = exchange.frames.find((r) => r.id === write.frame)
            expect(frame.socket).toBe(write.socket)
            expect(frame.at).toBe(write.at)
          }
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
            'soak-original-cycle',
            (r) => {
              const observation = r.observation
              const source = observation.provenance.exchanges.find(
                (e) =>
                  e.id ===
                  observation.provenance.acquisitions.find(
                    (a) => a.id === observation.resetTarget.acquisition
                  ).exchange
              )
              const reopen = observation.provenance.exchanges.find(
                (e) =>
                  e.frames[0]?.socket === observation.reset.nextSocket &&
                  e.frames[0].request.shareSessionEpoch === 0
              )
              reopen.pool = source.pool
              reopen.generation = source.generation
            },
            /reset transport\/generation/,
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
              (event !== 'soak-original-cycle' || r.cycle === 10) &&
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
        const { SoakTransport } = require('./fixtures/soakTransport')
        const { reply } = require('./fixtures/soakReplies')
        const topic = archive.rows.find(
          (r) => r.event === 'soak-configuration' && r.scope === 'scored'
        ).config.topic
        const mutations = [
          [
            'send',
            0,
            'Produce',
            () => true,
            (f) => {
              f.Responses[0].PartitionResponses[0].BaseOffset = '1'
            },
          ],
          [
            'describeConfigs',
            32,
            'DescribeConfigs',
            (r) => r.resources[0].resourceType === 32,
            (f) => {
              f.Results[0].Configs.find((e) => e.Name === 'share.record.lock.duration.ms').Value =
                '30001'
            },
          ],
          [
            'describeConfigs',
            32,
            'DescribeConfigs',
            (r) => r.resources[0].resourceType === 2,
            (f) => {
              f.Results[0].Configs.find((e) => e.Name === 'min.insync.replicas').Value = '1'
            },
          ],
          [
            'fetchTopicMetadata',
            3,
            'Metadata',
            () => true,
            (f) => {
              f.Topics[0].Partitions[0].ReplicaNodes.pop()
            },
          ],
          [
            'listShareGroupOffsets:named',
            90,
            'DescribeShareGroupOffsets',
            () => true,
            (f) => {
              f.Groups[0].Topics[0].Partitions[0].StartOffset = '1'
            },
          ],
          [
            'listShareGroupOffsets:all',
            90,
            'DescribeShareGroupOffsets',
            () => true,
            (f) => {
              f.Groups[0].Topics[0].Partitions[0].Lag = '1'
            },
          ],
          [
            'fetchTopicOffsets',
            2,
            'ListOffsets',
            () => true,
            (f) => {
              f.Topics[0].Partitions[0].Offset = '1'
            },
          ],
          [
            'describeShareGroups',
            77,
            'ShareGroupDescribe',
            (r) => r.groups[0].members.length > 0,
            (f) => {
              f.Groups[0].Members[0].Assignment.TopicPartitions[0].Partitions.pop()
            },
          ],
        ]
        for (const [name, apiKey, api, select, mutate] of mutations) {
          const operation = archive.rows.find(
            (r) =>
              r.scope === 'scored' &&
              r.event === 'soak-wire' &&
              r.kind === 'operation' &&
              r.status === 'passed' &&
              r.name === name &&
              select(r.result)
          )
          expect(operation).toBeDefined()
          const request = archive.rows.find(
            (r) =>
              r.scope === 'scored' &&
              r.kind === 'request' &&
              r.operationId === operation.id &&
              r.apiKey === apiKey
          )
          expect(request).toBeDefined()
          const response = archive.rows.find(
            (r) =>
              r.scope === 'scored' &&
              r.kind === 'response' &&
              r.socket === request.socket &&
              r.frame === request.frame
          )
          const before = response.bytes
          const options = name === 'fetchTopicOffsets' ? topic : operation.target
          const fields = SoakTransport.prototype.resultFields(
            name.split(':')[0],
            [options],
            operation.result
          )
          mutate(fields)
          const bytes = Buffer.concat([
            Buffer.from(before, 'base64').subarray(0, 9),
            reply(api, request.apiVersion, fields),
          ])
          bytes.writeInt32BE(bytes.length - 4, 0)
          response.bytes = bytes.toString('base64')
          archive.writeRows()
          archive.seal(first)
          const rejected = await validateArchive(archive.contract, root, {
            provenance: archive.provenance,
          })
          expect(rejected.problems).not.toContain('Changed/incomplete artifact seal')
          expect(rejected.problems.join('; ')).toMatch(
            new RegExp(`Original operation ${operation.id}/${name}: decoded values differ`)
          )
          response.bytes = before
        }
        archive.writeRows()
        archive.seal(first)
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
    if (corpusOnly.includes('driver')) await diagnosePrimeRoots('corpus-only')
    expect(corpusOnly).not.toContain('driver')
    delete owner.retainedPrime
    expect(await retainedRoots(primeRoots)).toEqual([])
  }, 600000)
})
