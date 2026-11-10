const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { ArchivedCycle } = require('../shareSoakCycle')
const { validateRows, cleanupPopulation, validateArchive } = require('../shareSoakAcceptance')
const { verifyProtocolAnswers } = require('../shareSoakProtocol')
const { receiptProblems } = require('../shareSoakReceipts')
const { createCycleFacts } = require('../../soak/__tests__/fixtures/shareCycleFacts')
const { ShareSoakDriver } = require('../../soak/soak')
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
      expect(result.problems.filter((p) => p.startsWith('Original protocol answer'))).toEqual([])
      expect(result.groupListings).toEqual([
        expect.objectContaining({ groups: [expect.objectContaining({ groupId: 'unrelated' })] }),
      ])
      expect(result.successes).toEqual(
        expect.arrayContaining([0, 16, 18, 32].map((apiKey) => expect.objectContaining({ apiKey })))
      )
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
