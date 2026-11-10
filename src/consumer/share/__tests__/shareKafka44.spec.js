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
