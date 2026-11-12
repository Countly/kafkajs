const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { ArchivedCycle } = require('../shareSoakCycle')
const { validateRows, cleanupPopulation, validateArchive } = require('../shareSoakAcceptance')
const { verifyProtocolAnswers } = require('../shareSoakProtocol')
const { receiptProblems } = require('../shareSoakReceipts')
const { createCycleFacts } = require('../../soak/__tests__/fixtures/shareCycleFacts')
const { ShareSoakDriver, ShareCycleOracle } = require('../../soak/soak')
const { observeShareSoakDriver } = require('../../soak/shareSoakEvidence')

/** @param {*} value Original recording. @returns {*} JSON-round-tripped original bytes. */
const original = (value) => JSON.parse(JSON.stringify(value))

/** @returns {object[]} Frozen short/full receipt inventory. */
const inventory = () =>
  ['short', 'full'].map((profile) => ({
    type: 'share-soak-ci-v1',
    commit: 'a'.repeat(40),
    runId: '123',
    attempt: '1',
    profile,
    repeat: 'frozen-1',
  }))
/** @returns {object[]} Successful authenticated receipt projections. */
const receipts = () =>
  inventory().map((entry) => ({
    ...entry,
    producer: 'success',
    upload: 'success',
    replay: 'success',
    verdict: 'success',
    exitGate: 'success',
    complete: true,
    contractSha256: 'b'.repeat(64),
    semanticSha256: 'c'.repeat(64),
    replaySemanticSha256: 'c'.repeat(64),
    artifactDigest: `sha256:${'d'.repeat(64)}`,
    downloadDigest: `sha256:${'d'.repeat(64)}`,
  }))

describe('share soak independent acceptance controls', () => {
  test.each([0, 10, 29])(
    'reconstructs original framed acquisitions, accepts, departures and terminal reads for cycle %s',
    (cycle) => {
      const facts = createCycleFacts().cycleFacts(cycle)
      expect(ArchivedCycle.verify(ArchivedCycle.plan(cycle), original(facts))).toEqual([])
    }
  )

  test.each([
    [
      'producer identity',
      (f) => {
        f.produced[0].identity = f.produced[1].identity
      },
    ],
    [
      'raw omission',
      (f) => {
        f.log.pop()
      },
    ],
    [
      'duplicate handler dispatch',
      (f) => {
        f.handler.push(f.handler[0])
      },
    ],
    [
      'terminal listing',
      (f) => {
        f.positions[0].named[0].partitions[0].lag = '1'
      },
    ],
    [
      'response correlation',
      (f) => {
        f.provenance.exchanges[0].frames[0].correlationId++
      },
    ],
    [
      'missing Accept',
      (f) => {
        f.provenance.outcomes.pop()
      },
    ],
    [
      'departure',
      (f) => {
        f.provenance.heartbeats.pop()
      },
    ],
    [
      'inactive verifier',
      (f) => {
        f.verifier.events = []
      },
    ],
    [
      'changed verifier assignment',
      (f) => {
        f.verifier.events[1].payload[0].partitions.pop()
      },
    ],
    [
      'held redelivery',
      (f) => {
        f.handler = f.handler.filter((h) => h.deliveryCount === 1)
      },
    ],
  ])('rejects serialized %s while reported summaries remain green', (name, mutate) => {
    const facts = original(createCycleFacts().cycleFacts(10))
    mutate(facts)
    expect(ArchivedCycle.verify(ArchivedCycle.plan(10), facts).length).toBeGreaterThan(0)
  })

  describe.each([
    ['runner', ShareCycleOracle],
    ['archive', ArchivedCycle],
  ])('%s reset retirement evidence', (_name, judge) => {
    /** @returns {object} A pre-close attempt followed by a new epoch-zero socket. */
    const recording = () => {
      const facts = original(createCycleFacts().cycleFacts(10))
      facts.steps = [{ step: 'socket-close', socket: 3, at: 10, lastFrame: 1000 }]
      facts.provenance.exchanges.push(
        { frames: [{ id: 999, socket: 3, at: 9, apiKey: 78 }] },
        { frames: [{ id: 1002, socket: 7, at: 11, apiKey: 78 }] }
      )
      return facts
    }
    /** @param {object} facts Original cycle frames. @returns {string[]} Membership failures. */
    const problems = (facts) => {
      const failures = []
      judge.membership(judge.plan(10), facts, (message) => failures.push(message))
      return failures
    }

    test('accepts the pre-close race and replacement writes', () => {
      expect(problems(recording())).toEqual([])
    })

    test.each([
      ['later timestamp', { id: 1001, at: 11, apiKey: 78 }, false],
      ['same-millisecond sequence', { id: 1001, at: 10, apiKey: 78 }, false],
      ['acknowledgement frame', { id: 1001, at: 11, apiKey: 79 }, false],
      ['legacy close timestamp', { id: 1001, at: 11, apiKey: 78 }, true],
    ])('rejects raw post-close %s without a diagnostic marker', (_label, fields, legacy) => {
      const facts = recording()
      if (legacy) delete facts.steps[0].lastFrame
      expect(problems(facts)).toEqual([])
      facts.provenance.exchanges.push({ frames: [{ socket: 3, ...fields }] })
      expect(problems(facts)).toEqual(['Retired reset socket 3 was written after close'])
    })
  })

  test.each([0, 1])(
    'rejects an unanswered wire write %s ms after reset socket close',
    async (delay) => {
      const rows = original(require('./fixtures/shareProduceV13.json').rows)
      const request = rows.find((r) => r.kind === 'request' && r.apiKey === 0)
      const at = Math.max(...rows.filter((r) => r.kind === 'request').map((r) => r.at)) + 1
      const frame = Math.max(...rows.filter((r) => r.kind === 'request').map((r) => r.frame)) + 1
      rows.push({
        event: 'soak-cycle-step',
        step: 'socket-close',
        socket: request.socket,
        monotonicAt: at,
        lastFrame: frame - 1,
      })
      const pristine = await verifyProtocolAnswers(rows)
      expect(pristine.problems.filter((p) => p.startsWith('Original'))).toEqual([])
      rows.push({ ...request, frame, at: at + delay })
      const changed = await verifyProtocolAnswers(rows)
      expect(changed.problems.filter((p) => p.startsWith('Original'))).toEqual([
        `Original request after reset socket close: socket ${request.socket}, frame ${frame}`,
      ])
    }
  )

  test.each(['short', 'full'])('refuses empty or removed %s share-identifying rows', (profile) => {
    const contract = { profile, node: 'v24.0.0', source: { commit: 'a'.repeat(40) } }
    expect(validateRows([], contract).length).toBeGreaterThan(0)
    expect(
      validateRows([{ event: 'case-complete', status: 'passed' }], contract).length
    ).toBeGreaterThan(0)
  })

  test('refuses a self-consistent fabricated archive without matching frozen source', async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'share-archive-'))
    try {
      fs.writeFileSync(
        path.join(root, 'contract.json'),
        JSON.stringify({
          schema: 'share-soak-ci-v1',
          profile: 'short',
          source: { commit: 'a'.repeat(40) },
        })
      )
      const result = await validateArchive(
        JSON.parse(fs.readFileSync(path.join(root, 'contract.json'))),
        root
      )
      expect(result.problems.length).toBeGreaterThan(0)
    } finally {
      fs.rmSync(root, { recursive: true, force: true })
    }
  })

  test('requires original cleanup sources without masking equal raw-count retention', () => {
    const point = {
      listeners: {},
      activeResources: { Timeout: 2 },
      sockets: { created: 10, destroyed: 10, open: 0 },
      queueDepth: 0,
      ledgerSize: 0,
    }
    expect(cleanupPopulation(point)).toEqual({
      listeners: {},
      activeResources: { Timeout: 2 },
      sockets: 0,
      queueDepth: 0,
      ledgerSize: 0,
    })
    expect(() => cleanupPopulation({ ...point, queueDepth: undefined })).toThrow(/population/)
    expect(() =>
      cleanupPopulation({ ...point, sockets: { created: 10, destroyed: 9, open: 0 } })
    ).toThrow(/population/)
  })

  test('does not count an answered malformed admin frame as successful operation coverage', async () => {
    const result = await verifyProtocolAnswers([
      {
        event: 'soak-wire',
        kind: 'request',
        apiKey: 19,
        apiVersion: 7,
        socket: 1,
        frame: 1,
        bytes: 'bad',
      },
      { event: 'soak-wire', kind: 'response', socket: 1, frame: 1, bytes: 'bad' },
    ])
    expect(result.successes).toEqual([])
    expect(result.problems).toContain('No decoded successful API 19')
  })

  test('decodes original scripted admin and producer replies rather than placeholder success bytes', async () => {
    const { ShareFetchTap } = require('../../soak/soak')
    const { SoakTransport } = require('../../soak/__tests__/fixtures/soakTransport')
    const tap = new ShareFetchTap()
    const rows = []
    tap.archive = (row) => rows.push(original({ event: 'soak-wire', ...row }))
    const transport = new SoakTransport(tap)
    try {
      const admin = transport.admin({
        describeCluster: async () => ({
          brokers: [{ nodeId: 7, host: 'node7', port: 9092 }],
        }),
        listGroups: async () => ({ groups: [{ groupId: 'unrelated' }] }),
        describeConfigs: async () => ({
          resources: [
            {
              errorCode: 0,
              resourceType: 32,
              resourceName: 'owned',
              configEntries: [
                {
                  configName: 'share.auto.offset.reset',
                  configValue: 'earliest',
                  configSource: 8,
                },
              ],
            },
          ],
        }),
      })
      await admin.describeCluster()
      await admin.describeCluster({ includeAuthorizedOperations: true })
      await admin.listGroups()
      await admin.describeConfigs({ resources: [{ type: 32, name: 'owned' }] })
      const producer = transport.producer({
        send: async () => [{ partition: 0, errorCode: 0, baseOffset: '20' }],
      })
      await producer.send({
        topic: 'topic',
        acks: -1,
        messages: [
          {
            partition: 0,
            key: Buffer.from('k'),
            value: Buffer.from('v'),
            timestamp: '1700000000000',
          },
        ],
      })
      const result = await verifyProtocolAnswers(rows)
      expect(
        result.problems.filter((p) => /^Original (protocol answer|operation)/.test(p))
      ).toEqual([])
      expect(result.groupListings).toEqual([
        expect.objectContaining({ groups: [expect.objectContaining({ groupId: 'unrelated' })] }),
      ])
      expect(result.successes).toEqual(
        expect.arrayContaining(
          [0, 3, 16, 18, 32, 60].map((apiKey) => expect.objectContaining({ apiKey }))
        )
      )
      const discovery = rows.filter(
        (r) => r.kind === 'request' && r.operation === 'describeCluster' && r.apiKey !== 18
      )
      expect(discovery.map(({ apiKey, apiVersion }) => [apiKey, apiVersion])).toEqual([
        [3, 13],
        [60, 2],
      ])
      const { reply } = require('../../soak/__tests__/fixtures/soakReplies')
      for (const request of discovery) {
        const response = rows.find(
          (r) => r.kind === 'response' && r.socket === request.socket && r.frame === request.frame
        )
        const before = response.bytes
        const bytes = Buffer.concat([
          Buffer.from(before, 'base64').subarray(0, 9),
          reply(request.apiKey === 60 ? 'DescribeCluster' : 'Metadata', request.apiVersion, {
            Brokers: [
              { [request.apiKey === 60 ? 'BrokerId' : 'NodeId']: 7, Host: 'changed', Port: 9092 },
            ],
          }),
        ])
        bytes.writeInt32BE(bytes.length - 4, 0)
        response.bytes = bytes.toString('base64')
        const changed = await verifyProtocolAnswers(rows)
        expect(changed.problems).toContain(
          `Original operation ${request.operationId}/describeCluster: decoded values differ`
        )
        response.bytes = before
      }
    } finally {
      transport.close()
    }
  })

  test.each([
    [
      'Produce base offset',
      'send',
      'Produce',
      { Responses: [{ Name: 'topic', PartitionResponses: [{ Index: 0, BaseOffset: '1' }] }] },
    ],
    [
      'group configuration',
      'describeConfigs',
      'DescribeConfigs',
      {
        Results: [
          {
            ResourceType: 32,
            ResourceName: 'owned',
            Configs: [{ Name: 'share.auto.offset.reset', Value: 'latest', ConfigSource: 8 }],
          },
        ],
      },
    ],
    [
      'terminal lag',
      'listShareGroupOffsets',
      'DescribeShareGroupOffsets',
      {
        Groups: [
          {
            GroupId: 'owned',
            Topics: [
              {
                TopicName: 'topic',
                TopicId: '00000000-0000-4000-8000-000000000001',
                Partitions: [{ PartitionIndex: 0, StartOffset: '0', Lag: '1', LeaderEpoch: 1 }],
              },
            ],
          },
        ],
      },
    ],
  ])(
    'rejects changed decoded %s with the original operation result unchanged',
    async (_label, method, api, fields) => {
      const { ShareFetchTap } = require('../../soak/soak')
      const { SoakTransport } = require('../../soak/__tests__/fixtures/soakTransport')
      const { reply } = require('../../soak/__tests__/fixtures/soakReplies')
      const tap = new ShareFetchTap()
      const rows = []
      tap.archive = (row) => rows.push(original({ event: 'soak-wire', ...row }))
      const transport = new SoakTransport(tap)
      try {
        const client =
          method === 'send'
            ? transport.producer({
                send: async () => [{ partition: 0, baseOffset: '0', errorCode: 0 }],
              })
            : transport.admin({
                describeConfigs: async () => ({
                  resources: [
                    {
                      resourceType: 32,
                      resourceName: 'owned',
                      errorCode: 0,
                      configEntries: [
                        {
                          configName: 'share.auto.offset.reset',
                          configValue: 'earliest',
                          configSource: 8,
                        },
                      ],
                    },
                  ],
                }),
                listShareGroupOffsets: async () => [
                  {
                    topic: 'topic',
                    topicId: '00000000-0000-4000-8000-000000000001',
                    partitions: [
                      { partition: 0, startOffset: '0', lag: '0', leaderEpoch: 1, errorCode: 0 },
                    ],
                  },
                ],
              })
        await client[method]({
          topic: 'topic',
          acks: -1,
          groupId: 'owned',
          resources: [{ type: 32, name: 'owned' }],
          messages: [
            {
              partition: 0,
              key: Buffer.from('k'),
              value: Buffer.from('v'),
              timestamp: '1700000000000',
            },
          ],
        })
        const before = await verifyProtocolAnswers(rows)
        expect(before.problems.filter((p) => p.startsWith('Original operation'))).toEqual([])
        const request = rows.find((r) => r.kind === 'request' && r.apiKey !== 18)
        const response = rows.find(
          (r) => r.kind === 'response' && r.socket === request.socket && r.frame === request.frame
        )
        const bytes = Buffer.concat([
          Buffer.from(response.bytes, 'base64').subarray(0, 9),
          reply(api, request.apiVersion, fields),
        ])
        bytes.writeInt32BE(bytes.length - 4, 0)
        response.bytes = bytes.toString('base64')
        const result = await verifyProtocolAnswers(rows)
        expect(result.problems).toContain(
          `Original operation ${request.operationId}/${request.operation}: decoded values differ`
        )
      } finally {
        transport.close()
      }
    }
  )

  test('reconciles absent-group replies without counting them as successful coverage', async () => {
    const { ShareFetchTap } = require('../../soak/soak')
    const { SoakTransport } = require('../../soak/__tests__/fixtures/soakTransport')
    const tap = new ShareFetchTap()
    const rows = []
    tap.archive = (row) => rows.push(original({ event: 'soak-wire', ...row }))
    const transport = new SoakTransport(tap)
    try {
      const admin = transport.admin({
        describeShareGroups: async () => ({
          groups: [{ groupId: 'owned', errorCode: 69, state: 'Empty', members: [] }],
        }),
      })
      await admin.describeShareGroups(['owned'])
      const result = await verifyProtocolAnswers(rows)
      expect(result.problems.filter((p) => p.startsWith('Original operation'))).toEqual([])
      expect(result.successes.some((r) => r.apiKey === 77)).toBe(false)
      expect(result.problems).toContain('No decoded successful API 77')
    } finally {
      transport.close()
    }
  })

  test('observes the real producer adapter and decoded raw-log adapter through serialization', async () => {
    const rows = []
    const sent = [{ partition: 0, errorCode: 0, baseOffset: '0' }]
    const driver = Object.create(ShareSoakDriver.prototype)
    Object.assign(driver, {
      ports: {
        fixtures: {
          shareRecordIdentity: require('../../../testHelpers/shareFixtures').shareRecordIdentity,
        },
        bounded: { bounded: (scope, ms, work) => work() },
      },
      producer: {
        send: jest.fn(async ({ messages }) => [{ ...sent[0], partition: messages[0].partition }]),
      },
      topic: 'topic',
    })
    observeShareSoakDriver(driver, (event, fields) => rows.push(original({ event, ...fields })))
    const produced = await driver.produce(ArchivedCycle.plan(0), {})
    expect(produced).toHaveLength(60)
    expect(rows.filter((r) => r.event === 'soak-producer')).toHaveLength(3)
    expect(rows[0].messages).toHaveLength(20)
    expect(rows[0].sent[0]).toEqual(sent[0])
    const log = new Map([
      [
        0,
        {
          endOffset: '1',
          records: [{ offset: '0', key: Buffer.from('k'), value: Buffer.from('v') }],
        },
      ],
    ])
    driver.readLogs(ArchivedCycle.plan(0), log)
    expect(rows.at(-1)).toMatchObject({
      event: 'soak-raw-log',
      ends: { 0: { end: '1', count: 1 } },
      identities: ['["topic",0,"0","6b","76"]'],
    })
  })
})

describe('share soak CI receipt consumption controls', () => {
  test('requires both profiles for the predeclared immutable repeat', () => {
    expect(receiptProblems(inventory(), receipts())).toEqual([])
    expect(receiptProblems(inventory().slice(0, 1), receipts())).toContain(
      'Both profiles are required for every frozen repeat'
    )
  })

  test.each(['producer', 'upload', 'replay', 'verdict', 'exitGate'])(
    'refuses failed, skipped or cancelled %s despite a green workload elsewhere',
    (field) => {
      for (const result of ['failure', 'skipped', 'cancelled', 'timed_out', undefined]) {
        const facts = receipts()
        facts[0][field] = result
        expect(receiptProblems(inventory(), facts).length).toBeGreaterThan(0)
      }
    }
  )

  test.each([
    'commit',
    'runId',
    'attempt',
    'profile',
    'repeat',
    'artifactDigest',
    'replaySemanticSha256',
  ])('refuses substituted %s and cannot use another run latest green', (field) => {
    const facts = receipts()
    facts[0][field] = 'other'
    expect(receiptProblems(inventory(), facts).length).toBeGreaterThan(0)
  })
})
