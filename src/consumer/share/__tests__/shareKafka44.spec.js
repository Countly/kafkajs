const fs = require('fs')
const os = require('os')
const path = require('path')
const { spawnSync } = require('child_process')
const { EventEmitter } = require('events')
const Encoder = require('../../../protocol/encoder')
const shareFetch = require('../../../../testHelpers/wire/shareFetchV2')
const shareHeartbeat = require('../../../../testHelpers/wire/shareGroupHeartbeatV1')
const expected = require('../../../../testHelpers/wire/expected')
const { A, B, acquired, fetchAnswer, part } = require('../../../../testHelpers/shareHarness')
const {
  acknowledgeTypesByOffset,
  assignedPartitions,
  partitionLifecycle,
  prepareShareKafka44,
  ShareWireTap,
  ShareKafka44Fixture,
} = require('../../../../testHelpers/shareKafka44')

/** @param {Record<string, number>} levels finalized levels by feature name. @returns {object} An admin whose `describeFeatures` reports them. */
const adminWith = (levels) => ({
  describeFeatures: jest.fn(async () => ({
    supportedFeatures: {},
    finalizedFeatures: Object.fromEntries(
      Object.entries(levels).map(([name, level]) => [
        name,
        { minVersionLevel: level, maxVersionLevel: level },
      ])
    ),
    finalizedFeaturesEpoch: '7',
  })),
})

describe('Share Kafka 4.4 setup', () => {
  test('setup rejects an unfinalized level and preserves CLI failures before Jest', () => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'share-feature-'))
    const docker = path.join(directory, 'docker')
    fs.writeFileSync(
      docker,
      `#!/bin/bash
if [ "$1" = ps ]; then echo fake-container; exit 0; fi
if [ "\${!#}" = describe ]; then
  echo "Feature: share.version SupportedMinVersion: 0 SupportedMaxVersion: 2 FinalizedVersionLevel: $TEST_LEVEL Epoch: 7"
  exit "$TEST_STATUS"
fi
exit 0
`,
      { mode: 0o755 }
    )
    try {
      for (const [level, status, expected] of [
        ['1', '0', 1],
        ['0', '0', 1],
        ['2', '0', 0],
        ['2', '7', 7],
      ]) {
        const result = spawnSync('bash', ['scripts/e2e/shareKafka44Setup.sh'], {
          env: {
            ...process.env,
            PATH: `${directory}:${process.env.PATH}`,
            TEST_LEVEL: level,
            TEST_STATUS: status,
          },
          encoding: 'utf8',
        })
        expect({ level, status, exit: result.status }).toEqual({ level, status, exit: expected })
      }
    } finally {
      fs.rmSync(directory, { recursive: true, force: true })
    }
  })

  test('requires finalized share.version two', async () => {
    await expect(prepareShareKafka44({ admin: adminWith({ 'share.version': 2 }) })).resolves.toBe(
      undefined
    )
    for (const [levels, observed] of [
      [{ 'share.version': 0 }, 0],
      [{ 'share.version': 1 }, 1],
      [{ 'share.version': 3 }, 3],
      [{ 'group.version': 2 }, 0],
    ]) {
      await expect(prepareShareKafka44({ admin: adminWith(levels) })).rejects.toThrow(
        `share.version is finalized at level ${observed}, not 2`
      )
    }
  })

  test('fails instead of skipping when feature setup fails', async () => {
    const broker = new Error('describeFeatures refused')
    const refused = { describeFeatures: jest.fn().mockRejectedValue(broker) }
    await expect(prepareShareKafka44({ admin: refused })).rejects.toBe(broker)
    await expect(prepareShareKafka44({ admin: adminWith({ 'share.version': 1 }) })).rejects.toThrow(
      'the Kafka 4.4 share setup did not upgrade it'
    )
  })
})

/** A request frame as a client writes it: size, header v2 and the body. */
const requestFrame = (apiKey, apiVersion, correlationId, body) => {
  const payload = new Encoder()
    .writeInt16(apiKey)
    .writeInt16(apiVersion)
    .writeInt32(correlationId)
    .writeString('tap-test')
    .writeTaggedFields([])
    .writeBuffer(body).buffer
  return Buffer.concat([new Encoder().writeInt32(payload.length).buffer, payload])
}

/** A response frame as a broker writes it: size, correlation id, empty header tags and the body. */
const responseFrame = (correlationId, body) => {
  const payload = Buffer.concat([
    new Encoder().writeInt32(correlationId).writeTaggedFields([]).buffer,
    body,
  ])
  return Buffer.concat([new Encoder().writeInt32(payload.length).buffer, payload])
}

/** A socket that keeps what is written to it and lets the test push bytes back. */
const fakeSocket = () => {
  const socket = new EventEmitter()
  socket.written = []
  socket.write = (data) => {
    socket.written.push(Buffer.from(data))
    return true
  }
  return socket
}

describe('Share Kafka 4.4 wire observation', () => {
  test('records each share request with the answer that carries its correlation id', () => {
    const socket = fakeSocket()
    const tap = new ShareWireTap({ connect: () => socket })
    tap.factory('member-a')({ host: 'h', port: 1 })
    const heartbeat = requestFrame(
      76,
      1,
      11,
      Buffer.from(expected.shareGroupHeartbeatV1.requestHex, 'hex')
    )
    const unrelated = requestFrame(3, 12, 12, Buffer.from([0]))
    const fetch = requestFrame(78, 2, 13, Buffer.from(expected.shareFetchV2.requestHex, 'hex'))
    const bytes = Buffer.concat([heartbeat, unrelated, fetch])
    socket.write(bytes.subarray(0, 9))
    socket.write(bytes.subarray(9))
    const answer = shareFetch.buildResponse()
    socket.emit('data', responseFrame(13, answer).subarray(0, 5))
    socket.emit('data', responseFrame(13, answer).subarray(5))
    expect(socket.written.map(({ length }) => length)).toEqual([9, bytes.length - 9])
    expect(
      tap.exchanges.map(({ apiKey, label, correlationId }) => [apiKey, label, correlationId])
    ).toEqual([
      [76, 'member-a', 11],
      [78, 'member-a', 13],
    ])
    expect(tap.of(76)[0].response).toBe(null)
    expect(tap.of(78, 'member-a')[0].response).toEqual(answer)
    expect(tap.of(78, 'other')).toEqual([])
    expect(shareFetch.readRequest(tap.of(78)[0].request)).toEqual(expected.shareFetchV2.request)
  })

  test('reads a ShareFetch answer with two acquisition ranges of one partition independently', () => {
    const body = shareFetch.buildResponse(
      fetchAnswer([
        part({ partition: 0, acquire: [acquired(0, 2, 2), acquired(13, 15, 2)] }),
        part({ partition: 1, code: 6, leader: { leaderId: 3, leaderEpoch: 12 } }),
      ])
    )
    const read = shareFetch.readResponse(body)
    expect(read.acquisitionLockTimeoutMs).toBe(15000)
    expect(read.responses).toHaveLength(1)
    expect(read.responses[0].topicId).toBe(A)
    expect(read.responses[0].partitions[0].acquiredRecords).toEqual([
      { firstOffset: '0', lastOffset: '2', deliveryCount: 2 },
      { firstOffset: '13', lastOffset: '15', deliveryCount: 2 },
    ])
    expect(read.responses[0].partitions[1]).toMatchObject({
      partitionIndex: 1,
      errorCode: 6,
      currentLeader: { leaderId: 3, leaderEpoch: 12 },
      acquiredRecords: [],
    })
    expect(() => shareFetch.readResponse(Buffer.concat([body, Buffer.from([0])]))).toThrow(
      'its tag buffer'
    )
    expect(() => shareFetch.readResponse(body.subarray(0, -1))).toThrow()
  })

  test('reads the pinned ShareFetch answer vector field by field', () => {
    const vector = expected.shareFetchV2
    const read = shareFetch.readResponse(Buffer.from(vector.responseHex, 'hex'))
    const [first, second] = vector.response.responses[0].partitions
    expect(read).toMatchObject({
      throttleTime: vector.response.throttleTime,
      errorCode: 0,
      acquisitionLockTimeoutMs: vector.response.acquisitionLockTimeoutMs,
      nodeEndpoints: vector.response.nodeEndpoints,
    })
    expect(read.responses[0].partitions[0]).toMatchObject({
      currentLeader: first.currentLeader,
      acquiredRecords: first.acquiredRecords,
    })
    expect(read.responses[0].partitions[1]).toMatchObject({
      errorCode: second.errorCode,
      errorMessage: second.errorMessage,
      acknowledgeErrorCode: second.acknowledgeErrorCode,
      acknowledgeErrorMessage: second.acknowledgeErrorMessage,
    })
  })

  test('reads the pinned ShareGroupHeartbeat answer and an error answer independently', () => {
    const vector = expected.shareGroupHeartbeatV1
    expect(shareHeartbeat.readResponse(Buffer.from(vector.responseHex, 'hex'))).toEqual(
      vector.response
    )
    const failed = shareHeartbeat.readResponse(
      shareHeartbeat.buildResponse({
        errorCode: 69,
        errorMessage: 'Group g is not a share group.',
        memberId: null,
        memberEpoch: -1,
        heartbeatIntervalMs: 0,
        assignment: null,
      })
    )
    expect(failed).toMatchObject({
      errorCode: 69,
      errorMessage: 'Group g is not a share group.',
      assignment: null,
    })
    expect(() =>
      shareHeartbeat.readResponse(
        Buffer.concat([Buffer.from(vector.responseHex, 'hex'), Buffer.from([1])])
      )
    ).toThrow('its tag buffer')
  })

  test('lists the acknowledge types of every offset in request order', () => {
    const batch = (firstOffset, lastOffset, acknowledgeTypes) => ({
      firstOffset,
      lastOffset,
      acknowledgeTypes,
    })
    const request = (partitionIndex, acknowledgementBatches, topicId = A) => ({
      topics: [{ topicId, partitions: [{ partitionIndex, acknowledgementBatches }] }],
    })
    const types = acknowledgeTypesByOffset(
      [
        request(0, [batch('0', '2', [2]), batch('5', '6', [1, 3])]),
        request(1, [batch('0', '0', [1])]),
        request(0, [batch('0', '1', [1]), batch('7', '7', [0])], B),
        request(0, [batch('0', '2', [1])]),
      ],
      A,
      0
    )
    expect(types).toEqual(
      new Map([
        ['0', [2, 1]],
        ['1', [2, 1]],
        ['2', [2, 1]],
        ['5', [1]],
        ['6', [3]],
      ])
    )
  })

  test('finds the request that carries the last acknowledgements and forgets the partition', () => {
    const fetch = (listed, forgotten = []) => ({
      topics: [
        {
          topicId: A,
          partitions: listed.map(([partitionIndex, acknowledged]) => ({
            partitionIndex,
            acknowledgementBatches: acknowledged
              ? [{ firstOffset: '0', lastOffset: '0', acknowledgeTypes: [1] }]
              : [],
          })),
        },
      ],
      forgottenTopicsData: forgotten.length ? [{ topicId: A, partitions: forgotten }] : [],
    })
    const requests = [fetch([[0], [1]]), fetch([[1]]), fetch([[0, true], [1]], [0]), fetch([[1]])]
    expect(partitionLifecycle(requests, A, 0)).toEqual({
      firstAcknowledged: 2,
      forgotten: [2],
      lastListed: 2,
    })
    expect(partitionLifecycle(requests, A, 1)).toEqual({
      firstAcknowledged: -1,
      forgotten: [],
      lastListed: 3,
    })
    expect(partitionLifecycle(requests, B, 0)).toEqual({
      firstAcknowledged: -1,
      forgotten: [],
      lastListed: -1,
    })
  })

  test('lists each distinct stated assignment and ignores answers that state none', () => {
    const answer = (partitions) => ({
      assignment: partitions && {
        topicPartitions: [
          { topicId: A, partitions },
          { topicId: B, partitions: [9] },
        ],
      },
    })
    expect(
      assignedPartitions(
        [answer([1, 0]), answer(null), answer([0, 1]), answer([1]), answer(null), answer([1])],
        A
      )
    ).toEqual([[0, 1], [1]])
    expect(assignedPartitions([answer(null)], A)).toEqual([])
  })
})

describe('Share Kafka 4.4 archived evidence', () => {
  test('preserves raw requests and responses when a scenario fails before observation', async () => {
    const fixture = new ShareKafka44Fixture('forget')
    const rows = []
    fixture.row = (event, fields) => rows.push({ event, ...fields })
    fixture.tap.exchanges.push({
      apiKey: 78,
      apiVersion: 2,
      label: 'A',
      requestedAt: 10,
      answeredAt: 11,
      request: Buffer.from('0102', 'hex'),
      response: Buffer.from('0304', 'hex'),
    })
    const failure = new Error('acknowledgement deadline')
    await expect(
      fixture.run(async () => {
        throw failure
      })
    ).rejects.toBe(failure)
    expect(rows).toEqual([
      {
        event: 'case-failed',
        error: { message: failure.message },
        cleanup: { groupsAbsent: false, topicsAbsent: false, adminDisconnected: false },
        wire: [
          {
            apiKey: 78,
            apiVersion: 2,
            label: 'A',
            requestedAt: 10,
            answeredAt: 11,
            requestHex: '0102',
            responseHex: '0304',
          },
        ],
      },
    ])
  })

  test('requires all scenarios and cleanup instead of accepting a stamped success line', () => {
    const { verifyFiles } = require('../../../../scripts/evidence/verify')
    const verified = { tree: 'a'.repeat(40), node: process.version, brokerVersion: '4.4.0' }
    const stamp = {
      ...verified,
      at: new Date().toISOString(),
      lane: 'docker',
      config: {},
      wireVersions: {},
    }
    for (const name of ['share-kafka44', 'share-dlq-kafka44']) {
      const result = verifyFiles([`${name}.jsonl`], verified, () =>
        JSON.stringify({ ...stamp, status: 'passed' })
      )
      expect(result.problems.length).toBe(name === 'share-kafka44' ? 5 : 4)
      expect(
        result.problems.every((message) => message.includes('needs one observation row'))
      ).toBe(true)
    }
  })

  test('validates DLQ copy and metadata receipts and rejects duplicate records headers and failed acknowledgements', async () => {
    const {
      assertScenario,
      verifyShareKafka44,
    } = require('../../../../scripts/evidence/shareKafka44')
    const acknowledge = require('../../../../testHelpers/wire/shareAcknowledgeV2')
    const rows = []
    for (const release of [false, true]) {
      for (const copy of [false, true]) {
        const scenario = `dlq-${release ? 'release' : 'reject'}-${copy ? 'copy' : 'metadata'}`
        const fields = {
          groupId: 'group',
          memberId: A,
          topics: [
            {
              topicId: A,
              partitions: [
                {
                  partitionIndex: 0,
                  acknowledgementBatches: [
                    { firstOffset: '0', lastOffset: '0', acknowledgeTypes: [release ? 2 : 3] },
                  ],
                },
              ],
            },
          ],
        }
        const proof = {
          deliveryCounts: release ? [1, 2] : [1],
          identities: release ? ['identity', 'identity'] : ['identity'],
          group: 'group',
          topicId: A,
          quietMs: 3000,
          crashes: [],
          source: {
            topic: 'source',
            identity: 'identity',
            key: '6b',
            value: '76',
            headers: { 'source-only': Buffer.from('must-not-copy').toString('hex') },
          },
          records: [
            {
              key: copy ? '6b' : null,
              value: copy ? '76' : null,
              headers: Object.fromEntries(
                Object.entries({
                  '__dlq.errors.topic': 'source',
                  '__dlq.errors.partition': '0',
                  '__dlq.errors.offset': '0',
                  '__dlq.errors.group': 'group',
                  '__dlq.errors.delivery.count': release ? '2' : '1',
                  '__dlq.errors.message': release
                    ? 'Offset delivery count exceeded the threshold.'
                    : 'Offset rejected by client.',
                }).map(([key, value]) => [key, Buffer.from(value).toString('hex')])
              ),
            },
          ],
        }
        const wire = []
        for (const apiKey of release ? [78, 79] : [79]) {
          const codec = apiKey === 78 ? shareFetch : acknowledge
          const request = require(
            `../../../protocol/requests/${apiKey === 78 ? 'shareFetch' : 'shareAcknowledge'}/v2/request`
          )(fields)
          wire.push({
            apiKey,
            label: 'member',
            requestHex: (await request.encode()).buffer.toString('hex'),
            responseHex: codec
              .buildResponse({
                responses: [
                  {
                    topicId: A,
                    partitions: [
                      {
                        partitionIndex: 0,
                        errorCode: 0,
                        acknowledgeErrorCode: 0,
                        errorMessage: null,
                        acknowledgeErrorMessage: null,
                        currentLeader: { leaderId: 0, leaderEpoch: 1 },
                        records: '',
                        acquiredRecords: [],
                      },
                    ],
                  },
                ],
              })
              .toString('hex'),
          })
        }
        expect(() => assertScenario(scenario, proof, wire)).not.toThrow()
        for (const mutate of [
          (p) => p.records.push(p.records[0]),
          (p) => {
            p.records[0].headers['source-only'] = '78'
          },
          (p) => {
            delete p.records[0].headers['__dlq.errors.offset']
          },
          (p) => {
            p.records[0].key = copy ? null : '6b'
          },
          (p) => {
            p.deliveryCounts = [1, 1]
          },
        ]) {
          const broken = JSON.parse(JSON.stringify(proof))
          mutate(broken)
          expect(() => assertScenario(scenario, broken, wire)).toThrow()
        }
        const failed = wire.map((entry) => ({
          ...entry,
          responseHex: (entry.apiKey === 78 ? shareFetch : acknowledge)
            .buildResponse({ errorCode: 30 })
            .toString('hex'),
        }))
        expect(() => assertScenario(scenario, proof, failed)).toThrow()
        rows.push(
          {
            case: scenario,
            scenario,
            event: 'observations',
            proof,
            wire,
            wireVersions: { 78: 2 },
            features: { finalizedFeatures: { 'share.version': { maxVersionLevel: 2 } } },
          },
          {
            case: scenario,
            scenario,
            event: 'case-complete',
            status: 'passed',
            groups: ['group'],
            topics: ['source', 'dlq.source'],
            cleanup: { groupsAbsent: true, topicsAbsent: true, adminDisconnected: true },
          }
        )
      }
    }
    expect(verifyShareKafka44('share-dlq-kafka44', rows)).toEqual([])
    for (const mutate of [
      (rows) => rows.pop(),
      (rows) => {
        rows[1].cleanup.groupsAbsent = false
      },
      (rows) => {
        rows[0].features.finalizedFeatures['share.version'].maxVersionLevel = 1
      },
      (rows) => {
        rows[0].wireVersions = {}
      },
      (rows) => {
        rows[0].wire[0].requestHex = '00'
      },
      (rows) => rows.push(rows[0]),
    ]) {
      const broken = JSON.parse(JSON.stringify(rows))
      mutate(broken)
      expect(verifyShareKafka44('share-dlq-kafka44', broken).length).toBeGreaterThan(0)
    }
  })
})

describe('Share Kafka 4.4 forget evidence', () => {
  test('requires settled acknowledgements and a later successful fetch without reintroducing lost partitions', async () => {
    const { assertScenario } = require('../../../../scripts/evidence/shareKafka44')
    const request = require('../../../protocol/requests/shareFetch/v2/request')
    const frame = async (partitions, forgotten, time, response = shareFetch.buildResponse()) => ({
      apiKey: 78,
      label: 'A',
      requestedAt: time,
      answeredAt: time + 1,
      requestHex: (
        await request({
          groupId: 'group',
          memberId: A,
          shareSessionEpoch: 1,
          topics: [{ topicId: A, partitions }],
          forgottenTopicsData: forgotten,
        }).encode()
      ).buffer.toString('hex'),
      responseHex: response.toString('hex'),
    })
    const keep = { partitionIndex: 1, acknowledgementBatches: [] }
    const response = (partitions) => shareFetch.buildResponse(fetchAnswer(partitions))
    const lost = {
      partitionIndex: 0,
      acknowledgementBatches: [{ firstOffset: '0', lastOffset: '1', acknowledgeTypes: [2] }],
    }
    const wire = [
      await frame([lost, keep], [], 1, response([part({ partition: 0 })])),
      await frame(
        [keep],
        [],
        3,
        response([
          part({ partition: 0, acquire: [acquired(0, 1)] }),
          part({ partition: 1, acquire: [acquired(0, 1)] }),
        ])
      ),
      await frame(
        [lost, keep],
        [{ topicId: A, partitions: [0] }],
        10,
        response([part({ partition: 0 })])
      ),
      await frame([keep], [], 30),
    ]
    const proof = {
      topicId: A,
      lost: [0],
      kept: [1],
      heldAt: 4,
      settledAt: 20,
      crashes: [],
      acknowledgements: [{ partition: 0, type: 'release', error: null }],
    }
    expect(() => assertScenario('forget', proof, wire)).not.toThrow()
    expect(() => assertScenario('forget', proof, wire.slice(0, -1))).toThrow(
      'no later successful fetch'
    )
    expect(() => assertScenario('forget', { ...proof, acknowledgements: [] }, wire)).toThrow()
    expect(() => assertScenario('forget', { ...proof, settledAt: 40 }, wire)).toThrow(
      'no later successful fetch'
    )
    expect(() => assertScenario('forget', proof, [...wire, wire[2]])).toThrow()
    const reintroduced = await frame(
      [{ partitionIndex: 0, acknowledgementBatches: [] }, keep],
      [],
      40
    )
    expect(() => assertScenario('forget', proof, [...wire, reintroduced])).toThrow(
      'lost partition reappeared'
    )
    expect(() =>
      assertScenario('forget', proof, [
        ...wire.slice(0, -1),
        {
          ...wire.at(-1),
          responseHex: shareFetch.buildResponse({ errorCode: 30 }).toString('hex'),
        },
      ])
    ).toThrow('no later successful fetch')
    const damaged = (index, responseHex) =>
      wire.map((entry, i) => (i === index ? { ...entry, responseHex } : entry))
    expect(() =>
      assertScenario(
        'forget',
        proof,
        damaged(1, response([part({ partition: 1, acquire: [acquired(0, 1)] })]).toString('hex'))
      )
    ).toThrow('held fetch did not acquire partition 0')
    for (const fields of [
      { errorCode: 122 },
      { errorCode: 123 },
      fetchAnswer([part({ partition: 0, ack: 6 })]),
    ]) {
      expect(() =>
        assertScenario(
          'forget',
          proof,
          damaged(2, shareFetch.buildResponse(fields).toString('hex'))
        )
      ).toThrow()
    }
  })
})
