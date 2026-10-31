const { topLevelFetchAction } = require('../fetchErrorContract')
const OLD = '01020304-0506-0708-090a-0b0c0d0e0f10'
const NEW = '11121314-1516-1718-191a-1b1c1d1e1f20'

test.each([13, 14, 15, 16, 17, 18])(
  'Fetch v%i forgets the recorded old topic ID after cache replacement',
  async (version) => {
    const request = require(`../../protocol/requests/fetch/v${version}/request`)({
      replicaId: -1,
      maxWaitTime: 500,
      minBytes: 1,
      maxBytes: 10485760,
      topics: [],
      topicIds: new Map([['retired', NEW]]),
      sessionId: 42,
      sessionEpoch: 1,
      forgottenTopics: [{ topic: 'retired', topicId: OLD, partitions: [0] }],
    })
    const { buffer } = await request.encode()
    const start = (version <= 14 ? 25 : 21) + 2
    expect(buffer.subarray(start, start + 16)).toEqual(
      Buffer.from('0102030405060708090a0b0c0d0e0f10', 'hex')
    )
  }
)
test.each([13, 15])(
  'Fetch v%i keeps current-ID lookup for callers without a recorded ID',
  async (version) => {
    const request = require(`../../protocol/requests/fetch/v${version}/request`)({
      replicaId: -1,
      maxWaitTime: 500,
      minBytes: 1,
      maxBytes: 10485760,
      topics: [],
      topicIds: new Map([['retired', NEW]]),
      forgottenTopics: [{ topic: 'retired', partitions: [0] }],
    })
    const { buffer } = await request.encode()
    const start = (version <= 14 ? 25 : 21) + 2
    expect(buffer.subarray(start, start + 16)).toEqual(
      Buffer.from('1112131415161718191a1b1c1d1e1f20', 'hex')
    )
  }
)
test.each([70, 71])(
  'session error %i does not become stale metadata for tracked requests',
  (code) => {
    expect(topLevelFetchAction(code, { session: true })).toBe('session-reset')
    expect(topLevelFetchAction(code)).toBe('stale-metadata')
  }
)
test('topic-ID session errors still request metadata and unrelated top-level errors retain their classifier', () => {
  expect(topLevelFetchAction(106, { session: true })).toBe('stale-metadata')
  expect(topLevelFetchAction(29, { session: true })).toBe('throw')
})

const {
  NODE_ID,
  TOPIC,
  TOPIC_ID,
  createFetchGroup,
  fetchResponse,
  prepareRecordBatches,
} = require('./fixtures/fetchHarness')
const { KafkaJSRequestTimeoutError } = require('../../errors')

let harness
beforeAll(() => prepareRecordBatches())
afterEach(async () => {
  jest.useRealTimers()
  if (harness) await harness.broker.disconnect()
  harness = null
})

/** @returns {Promise<void>} yields one event-loop turn without arming a timer */
const turn = () => new Promise(setImmediate)

/** @returns {object} a Fetch group with sessions on, all partitions committed at offset 5 */
const open = (options = {}) => createFetchGroup({ fetchSessions: true, position: '5', ...options })

/**
 * @param {number} sessionId the session ID the broker answers with
 * @param {(number|object)[]} [partitions] the rows the reply carries; none is an empty reply
 * @param {object} [options] `fetchResponse` options
 * @returns {Buffer} one scripted Fetch response
 */
const reply = (sessionId, partitions = [], options = {}) =>
  fetchResponse(
    partitions.map((entry) =>
      typeof entry === 'number'
        ? { partition: entry, errorCode: 0, records: true }
        : { errorCode: 0, records: true, ...entry }
    ),
    { sessionId, includeTopic: partitions.length > 0, ...options }
  )

/**
 * @param {object} frame a decoded Fetch frame
 * @returns {{sessionId: number, sessionEpoch: number, topics: number[], forgotten: number[]}}
 *   the session fields and the partitions it sends and removes
 */
const shape = (frame) => ({
  sessionId: frame.sessionId,
  sessionEpoch: frame.sessionEpoch,
  topics: frame.topics.flatMap(({ partitions }) => partitions.map(({ partition }) => partition)),
  forgotten: frame.forgottenTopics.flatMap(({ partitions }) => partitions),
})

/**
 * Holds every completed response or failure of the broker's Fetch until released, which is a late
 * reply from the point of view of the group.
 * @param {object} broker the harness broker
 * @returns {{release: () => void, failNext: (error: Error) => void, pending: () => number}} the
 *   release of every held call, a failure for the next call delivered when released, and how many
 *   calls are held now
 */
const holdFetches = (broker) => {
  const real = broker.fetch.bind(broker)
  const waiting = []
  let failure = null
  jest.spyOn(broker, 'fetch').mockImplementation(async (fields) => {
    const failing = failure
    failure = null
    const response = failing ? null : await real(fields)
    await new Promise((resolve) => waiting.push(resolve))
    if (failing) throw failing
    return response
  })
  return {
    release: () => waiting.splice(0).forEach((resolve) => resolve()),
    failNext: (error) => {
      failure = error
    },
    pending: () => waiting.length,
  }
}

/**
 * @param {() => boolean} condition what the schedule is waiting for
 * @returns {Promise<void>} resolves once it holds, after at most a few hundred event-loop turns
 * @throws {Error} when it never holds
 */
const until = async (condition) => {
  for (let turns = 0; turns < 500 && !condition(); turns++) await turn()
  if (!condition()) throw new Error('the schedule never reached its hold point')
}

const nextRound = (group, answer, node = NODE_ID) => {
  harness.setResponse(answer)
  return group.fetch(node)
}

describe('Fetch sessions through the consumer group', () => {
  test('creates one session per node and polls an unchanged active set', async () => {
    harness = await open({
      partitions: [0, 1, 2],
      partitionLeaders: { 0: 0, 1: 1, 2: 2 },
      knownNodes: [0, 1, 2],
    })
    const ids = [11, 12, 13]
    for (const node of [0, 1, 2]) {
      harness.setResponse(reply(ids[node], [node]))
      const batches = await harness.group.fetch(node)
      expect(batches.map(({ partition }) => partition)).toEqual([node])
    }
    for (const epoch of [1, 2, 3]) {
      for (const node of [0, 1, 2]) {
        harness.setResponse(reply(ids[node], [node]))
        const batches = await harness.group.fetch(node)
        expect(batches.map(({ partition, fetchedOffset }) => [partition, fetchedOffset])).toEqual([
          [node, '5'],
        ])
        expect(epoch).toBeGreaterThan(0)
      }
    }

    const frames = harness.fetchFrames().map(shape)
    expect(frames.slice(0, 3)).toEqual(
      [0, 1, 2].map((node) => ({
        sessionId: 0,
        sessionEpoch: 0,
        topics: [node],
        forgotten: [],
      }))
    )
    expect(frames.slice(3)).toEqual(
      [1, 2, 3].flatMap((sessionEpoch) =>
        ids.map((sessionId) => ({ sessionId, sessionEpoch, topics: [], forgotten: [] }))
      )
    )
    expect(harness.findBrokerCalls.map(({ nodeId }) => nodeId)).toEqual([
      0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1, 2,
    ])
  })

  test('falls back to full requests at cache capacity without losing records', async () => {
    harness = await open()
    for (let round = 0; round < 3; round++) {
      harness.setResponse(reply(0, [0]))
      const batches = await harness.group.fetch(NODE_ID)
      expect(batches.map(({ partition }) => partition)).toEqual([0])
    }
    harness.setResponse(reply(77, [0]))
    expect((await harness.group.fetch(NODE_ID)).length).toBe(1)
    harness.setResponse(reply(77, []))
    expect(await harness.group.fetch(NODE_ID)).toEqual([])

    expect(harness.fetchFrames().map(shape)).toEqual([
      { sessionId: 0, sessionEpoch: 0, topics: [0], forgotten: [] },
      { sessionId: 0, sessionEpoch: 0, topics: [0], forgotten: [] },
      { sessionId: 0, sessionEpoch: 0, topics: [0], forgotten: [] },
      { sessionId: 0, sessionEpoch: 0, topics: [0], forgotten: [] },
      { sessionId: 77, sessionEpoch: 1, topics: [], forgotten: [] },
    ])
    expect(harness.cluster.refreshMetadata).not.toHaveBeenCalled()
    expect(harness.requestRejoin).not.toHaveBeenCalled()
  })

  test('resets session epochs without refreshing metadata for session errors', async () => {
    harness = await open()
    harness.setResponse(reply(42, [0]))
    await harness.group.fetch(NODE_ID)

    const next = async (answer) => {
      harness.setResponse(answer)
      return harness.group.fetch(NODE_ID)
    }
    expect(await next(reply(42, [], { topLevel: 70 }))).toEqual([])
    expect(await next(reply(43, [0]))).toHaveLength(1)
    expect(await next(reply(43, [], { topLevel: 71 }))).toEqual([])
    expect(harness.cluster.refreshMetadata).not.toHaveBeenCalled()
    expect(await next(reply(43, [0]))).toHaveLength(1)
    expect(await next(reply(43, [], { topLevel: 106 }))).toEqual([])
    expect(harness.cluster.refreshMetadata).toHaveBeenCalledTimes(1)
    expect(await next(reply(43, [0]))).toHaveLength(1)

    expect(harness.fetchFrames().map(shape)).toEqual([
      { sessionId: 0, sessionEpoch: 0, topics: [0], forgotten: [] },
      { sessionId: 42, sessionEpoch: 1, topics: [], forgotten: [] },
      { sessionId: 0, sessionEpoch: 0, topics: [0], forgotten: [] },
      { sessionId: 43, sessionEpoch: 1, topics: [], forgotten: [] },
      { sessionId: 43, sessionEpoch: 0, topics: [0], forgotten: [] },
      { sessionId: 43, sessionEpoch: 1, topics: [], forgotten: [] },
      { sessionId: 43, sessionEpoch: 0, topics: [0], forgotten: [] },
    ])
    expect(harness.requestRejoin).not.toHaveBeenCalled()
  })

  test('keeps fatal top-level codes fatal and the sessionless classifier unchanged', async () => {
    harness = await open()
    harness.setResponse(reply(42, [0]))
    await harness.group.fetch(NODE_ID)
    harness.setResponse(reply(42, [], { topLevel: 29 }))
    await expect(harness.group.fetch(NODE_ID)).rejects.toMatchObject({ code: 29 })
    await harness.broker.disconnect()

    harness = await createFetchGroup({ position: '5' })
    harness.setResponse(reply(0, [], { topLevel: 70 }))
    expect(await harness.group.fetch(NODE_ID)).toEqual([])
    expect(harness.cluster.refreshMetadata).toHaveBeenCalledTimes(1)
  })

  test('decodes sparse topic-ID replies with the complete request map', async () => {
    harness = await open({ partitions: [0, 1] })
    harness.setResponse(reply(42, [0, 1]))
    await harness.group.fetch(NODE_ID)
    harness.group.offsetManager.seek({ topic: TOPIC, partition: 1, offset: '9' })

    harness.setResponse(reply(42, [0]))
    const batches = await harness.group.fetch(NODE_ID)

    const [frame] = harness.fetchFrames().slice(-1)
    expect(shape(frame)).toEqual({ sessionId: 42, sessionEpoch: 1, topics: [1], forgotten: [] })
    expect(frame.topics[0].partitions[0].fetchOffset).toBe('9')
    expect(
      batches.map(({ topic, partition, fetchedOffset }) => [topic, partition, fetchedOffset])
    ).toEqual([[TOPIC, 0, '5']])
  })

  test('refuses an unknown topic ID in a sparse reply and starts the session over', async () => {
    harness = await open()
    harness.setResponse(reply(42, [0]))
    await harness.group.fetch(NODE_ID)

    const body = Buffer.from(reply(42, [0]))
    Buffer.from('99999999999999999999999999999999', 'hex').copy(
      body,
      body.indexOf(Buffer.from(TOPIC_ID.replaceAll('-', ''), 'hex'))
    )
    harness.setResponse(body)
    expect(await harness.group.fetch(NODE_ID)).toEqual([])
    expect(harness.cluster.refreshMetadata).toHaveBeenCalledTimes(1)

    harness.setResponse(reply(42, [0]))
    await harness.group.fetch(NODE_ID)
    expect(shape(harness.fetchFrames().slice(-1)[0])).toMatchObject({
      sessionId: 42,
      sessionEpoch: 0,
      topics: [0],
    })
  })

  const removals = {
    pause: (group) => group.pause([{ topic: TOPIC, partitions: [0] }]),
    revoke: (group) => group.subscriptionState.assign([]),
    halt: (group) =>
      group.haltedPartitions.set(`${TOPIC}|0`, {
        offset: '5',
        reason: 'checksum',
        error: new Error('halted'),
      }),
    validation: (group) =>
      group.positionValidator.request(TOPIC, 0, {
        offset: '5',
        trackedEpoch: 1,
        currentLeaderEpoch: 1,
      }),
  }

  test('forgets the final desired partition before waiting and wakes only its own run', async () => {
    for (const cause of Object.keys(removals)) {
      harness = await open()
      await nextRound(harness.group, reply(42, [0]))
      removals[cause](harness.group)
      jest.useFakeTimers({ doNotFake: ['setImmediate', 'nextTick'] })

      expect(await nextRound(harness.group, reply(42, []))).toEqual([])
      const removal = harness.fetchFrames().slice(-1)[0]
      expect(shape(removal)).toEqual({ sessionId: 42, sessionEpoch: 1, topics: [], forgotten: [0] })
      expect(removal.forgottenTopics[0].topicId).toBe(TOPIC_ID)

      const written = harness.writes().length
      for (let idle = 0; idle < 2; idle++) {
        const round = harness.group.fetch(NODE_ID)
        await jest.advanceTimersByTimeAsync(100)
        expect(await round).toEqual([])
      }
      expect(harness.writes()).toHaveLength(written)
      expect(jest.getTimerCount()).toBe(0)

      const oldRun = harness.group.idleWait
      const first = harness.group.fetch(NODE_ID)
      await turn()
      const newRun = harness.group.startIdleWait()
      expect(await first).toEqual([])

      let done = false
      const second = harness.group.fetch(NODE_ID).then((batches) => {
        done = true
        return batches
      })
      await turn()
      oldRun.wake()
      await turn()
      await turn()
      expect(done).toBe(cause === 'pause')

      newRun.wake()
      await second
      expect(jest.getTimerCount()).toBe(0)
      expect(harness.writes()).toHaveLength(written + (cause === 'pause' ? 1 : 0))
      jest.useRealTimers()
      await harness.broker.disconnect()
      harness = null
    }
  })

  test('retains active polling while the wire diff is empty', async () => {
    harness = await open()
    await nextRound(harness.group, reply(42, [0]))

    const batches = await nextRound(harness.group, reply(42, [0]))

    const [frame] = harness.fetchFrames().slice(-1)
    expect(shape(frame)).toEqual({ sessionId: 42, sessionEpoch: 1, topics: [], forgotten: [] })
    expect(frame).toMatchObject({ maxWaitMs: 100, minBytes: 1 })
    expect(batches.map(({ partition, fetchedOffset }) => [partition, fetchedOffset])).toEqual([
      [0, '5'],
    ])
  })

  test('fences late full and incremental replies across rebuild and generation failure', async () => {
    harness = await open()
    await nextRound(harness.group, reply(42, [0]))
    const held = holdFetches(harness.broker)

    harness.setResponse(reply(42, [0]))
    const lateSuccess = harness.group.fetch(NODE_ID)
    await until(() => held.pending() === 1)
    harness.group.startIdleWait()
    held.release()
    expect(await lateSuccess).toEqual([])

    held.failNext(new Error('late failure'))
    const lateFailure = harness.group.fetch(NODE_ID)
    await until(() => held.pending() === 1)
    harness.group.startIdleWait()
    held.release()
    expect(await lateFailure).toEqual([])

    harness.broker.fetch.mockRestore()
    harness.setResponse(reply(42, [0]))
    await harness.group.fetch(NODE_ID)
    expect(harness.fetchFrames().map(shape)).toEqual([
      { sessionId: 0, sessionEpoch: 0, topics: [0], forgotten: [] },
      { sessionId: 42, sessionEpoch: 1, topics: [], forgotten: [] },
      { sessionId: 42, sessionEpoch: 0, topics: [0], forgotten: [] },
    ])
    expect(harness.cluster.refreshMetadata).not.toHaveBeenCalled()
    expect(harness.cluster.removeBroker).not.toHaveBeenCalled()
  })

  test('a retired idle run failing late keeps the replacement run routing', async () => {
    harness = await open()
    await nextRound(harness.group, reply(42, [0]))
    const held = holdFetches(harness.broker)
    held.failNext(new Error('old idle run transport failure'))
    const late = harness.group.fetch(NODE_ID)
    await until(() => held.pending() === 1)
    harness.group.startIdleWait()
    const hint = { nodeId: NODE_ID, expireAt: Date.now() + 60000 }
    harness.group.preferredReadReplicasPerTopicPartition[TOPIC] = { 0: hint }

    held.release()

    expect(await late).toEqual([])
    expect(harness.group.preferredReadReplicasPerTopicPartition[TOPIC][0]).toBe(hint)
    harness.broker.fetch.mockRestore()
    harness.setResponse(reply(42, [0]))
    await harness.group.fetch(NODE_ID)
    expect(shape(harness.fetchFrames().at(-1))).toEqual({
      sessionId: 42,
      sessionEpoch: 0,
      topics: [0],
      forgotten: [],
    })
  })

  test('drops a reply that arrives after its sessions were retired', async () => {
    harness = await open()
    await nextRound(harness.group, reply(42, [0]))
    const held = holdFetches(harness.broker)
    harness.setResponse(reply(42, [0]))
    const late = harness.group.fetch(NODE_ID)
    await until(() => held.pending() === 1)

    harness.group.retireFetchSessions()
    held.release()

    expect(await late).toEqual([])
    expect(harness.group.fetchSessions.sessions.size).toBe(0)
    const written = harness.writes().length
    await harness.group.closeFetchSessions({ deadlineAt: performance.now() + 1000 })
    expect(harness.writes()).toHaveLength(written)
    expect(harness.cluster.refreshMetadata).not.toHaveBeenCalled()
  })

  test('preserves current addressing mode while the final ID-backed partition is removed', async () => {
    harness = await open()
    await nextRound(harness.group, reply(42, [0]))
    harness.cluster.topicId = () => undefined
    harness.group.pause([{ topic: TOPIC, partitions: [0] }])
    await nextRound(harness.group, reply(42, []))
    let [frame] = harness.fetchFrames().slice(-1)
    expect(frame).toMatchObject({ apiVersion: 18, sessionId: 42, sessionEpoch: 1 })
    expect(frame.forgottenTopics).toEqual([{ topic: null, topicId: TOPIC_ID, partitions: [0] }])
    await harness.broker.disconnect()

    harness = await open({ fetchMaxVersion: 12 })
    await nextRound(harness.group, reply(42, [0], { apiVersion: 12 }))
    harness.group.pause([{ topic: TOPIC, partitions: [0] }])
    await nextRound(harness.group, reply(42, [], { apiVersion: 12 }))
    ;[frame] = harness.fetchFrames().slice(-1)
    expect(frame).toMatchObject({ apiVersion: 12, sessionId: 42, sessionEpoch: 1 })
    expect(frame.forgottenTopics).toEqual([{ topic: TOPIC, topicId: null, partitions: [0] }])
    await harness.broker.disconnect()

    harness = await open()
    harness.cluster.topicId = () => undefined
    await nextRound(harness.group, reply(42, [0], { apiVersion: 12 }))
    harness.group.pause([{ topic: TOPIC, partitions: [0] }])
    await nextRound(harness.group, reply(42, [], { apiVersion: 12 }))
    ;[frame] = harness.fetchFrames().slice(-1)
    expect(frame).toMatchObject({ apiVersion: 12, sessionId: 42, sessionEpoch: 1 })
    expect(frame.forgottenTopics).toEqual([{ topic: TOPIC, topicId: null, partitions: [0] }])
    await harness.broker.disconnect()

    harness = await open()
    await nextRound(harness.group, reply(42, [0]))
    harness.cluster.topicId = () => undefined
    await nextRound(harness.group, reply(43, [0], { apiVersion: 12 }))
    harness.cluster.topicId = () => TOPIC_ID
    await nextRound(harness.group, reply(43, [0]))
    expect(
      harness.fetchFrames().map((sent) => [sent.apiVersion, sent.sessionId, sent.sessionEpoch])
    ).toEqual([
      [18, 0, 0],
      [12, 42, 0],
      [18, 43, 0],
    ])
  })

  test('keeps fallback and same-offset seeks safe across ID mode changes', async () => {
    harness = await open()
    expect(await nextRound(harness.group, reply(0, [0]))).toHaveLength(1)

    harness.setResponse(() => {
      harness.group.pause([{ topic: TOPIC, partitions: [0] }])
      harness.group.seek({ topic: TOPIC, partition: 0, offset: '5' })
      return reply(0, [0])
    })
    expect(await harness.group.fetch(NODE_ID)).toEqual([])
    harness.group.resume([{ topic: TOPIC, partitions: [0] }])

    harness.cluster.topicId = () => undefined
    const named = await nextRound(harness.group, reply(55, [0], { apiVersion: 12 }))
    harness.cluster.topicId = () => TOPIC_ID
    const keyed = await nextRound(harness.group, reply(55, [0]))
    const incremental = await nextRound(harness.group, reply(55, []))

    expect(named.map(({ fetchedOffset }) => fetchedOffset)).toEqual(['5'])
    expect(keyed.map(({ fetchedOffset }) => fetchedOffset)).toEqual(['5'])
    expect(incremental).toEqual([])
    expect(
      harness
        .fetchFrames()
        .map((frame) => [
          frame.apiVersion,
          frame.sessionId,
          frame.sessionEpoch,
          shape(frame).topics,
        ])
    ).toEqual([
      [18, 0, 0, [0]],
      [18, 0, 0, [0]],
      [12, 0, 0, [0]],
      [18, 55, 0, [0]],
      [18, 55, 1, []],
    ])
    expect(harness.cluster.refreshMetadata).not.toHaveBeenCalled()
    expect(harness.requestRejoin).not.toHaveBeenCalled()
  })

  test('clears preferred replicas only for a current failed session send', async () => {
    const { KafkaJSConnectionClosedError } = require('../../errors')
    const hint = () => ({ 0: { nodeId: NODE_ID, expireAt: Date.now() + 60000 } })
    const families = [
      new KafkaJSRequestTimeoutError('Request timed out', {}),
      new Error('socket reset'),
      new TypeError('cannot encode'),
    ]

    for (const error of families) {
      harness = await open()
      await nextRound(harness.group, reply(42, [0]))
      harness.group.preferredReadReplicasPerTopicPartition[TOPIC] = hint()
      jest.spyOn(harness.broker, 'fetch').mockRejectedValueOnce(error)

      await expect(harness.group.fetch(NODE_ID)).rejects.toBe(error)

      expect(harness.group.preferredReadReplicasPerTopicPartition[TOPIC][0]).toBeUndefined()
      await nextRound(harness.group, reply(42, [0]))
      const [retry] = harness.fetchFrames().slice(-1)
      expect(shape(retry)).toEqual({ sessionId: 42, sessionEpoch: 0, topics: [0], forgotten: [] })
      expect(retry.topics[0].partitions[0].fetchOffset).toBe('5')
      await harness.broker.disconnect()
    }

    harness = await open()
    await nextRound(harness.group, reply(42, [0]))
    jest
      .spyOn(harness.broker, 'fetch')
      .mockRejectedValueOnce(new KafkaJSConnectionClosedError('closed', { host: 'h', port: 1 }))
    expect(await harness.group.fetch(NODE_ID)).toEqual([])
    expect(harness.cluster.removeBroker).toHaveBeenCalledTimes(1)
    await nextRound(harness.group, reply(42, [0]))
    expect(shape(harness.fetchFrames().slice(-1)[0]).sessionEpoch).toBe(0)
    await harness.broker.disconnect()

    harness = await open()
    await nextRound(harness.group, reply(42, [0]))
    const held = holdFetches(harness.broker)
    held.failNext(new Error('late transport failure'))
    const late = harness.group.fetch(NODE_ID)
    await until(() => held.pending() === 1)
    harness.group.retireFetchSessions()
    harness.group.preferredReadReplicasPerTopicPartition[TOPIC] = hint()
    held.release()
    expect(await late).toEqual([])
    expect(harness.group.preferredReadReplicasPerTopicPartition[TOPIC][0]).toMatchObject({
      nodeId: NODE_ID,
    })
    expect(harness.cluster.removeBroker).not.toHaveBeenCalled()
  })

  test('re-requests a dropped answer from the unchanged offset', async () => {
    harness = await open()
    await nextRound(harness.group, reply(42, [0]))
    harness.setResponse(() => {
      harness.group.pause([{ topic: TOPIC, partitions: [0] }])
      return reply(42, [0])
    })
    expect(await harness.group.fetch(NODE_ID)).toEqual([])
    harness.group.resume([{ topic: TOPIC, partitions: [0] }])

    const delivered = await nextRound(harness.group, reply(42, [0]))

    expect(harness.offsetManager.nextOffset(TOPIC, 0).toString()).toBe('5')
    expect(harness.offsetCommit).not.toHaveBeenCalled()
    expect(delivered.map(({ fetchedOffset }) => fetchedOffset)).toEqual(['5'])
    expect(harness.fetchFrames().map(shape)).toEqual([
      { sessionId: 0, sessionEpoch: 0, topics: [0], forgotten: [] },
      { sessionId: 42, sessionEpoch: 1, topics: [], forgotten: [] },
      { sessionId: 42, sessionEpoch: 2, topics: [], forgotten: [] },
    ])
  })

  test('moves the next effective Fetch to the eligible hinted follower with sparse responses', async () => {
    harness = await open({ knownNodes: [0, 1], rackId: 'rack-a' })
    await nextRound(harness.group, reply(42, [0]))
    const hinted = await nextRound(
      harness.group,
      reply(42, [{ partition: 0, preferredReadReplica: 1 }])
    )
    expect(hinted).toHaveLength(1)

    expect(await nextRound(harness.group, reply(42, []))).toEqual([])
    expect(await nextRound(harness.group, reply(7, [0]), 1)).toHaveLength(1)

    const frames = harness.fetchFrames()
    expect(frames.map(shape)).toEqual([
      { sessionId: 0, sessionEpoch: 0, topics: [0], forgotten: [] },
      { sessionId: 42, sessionEpoch: 1, topics: [], forgotten: [] },
      { sessionId: 42, sessionEpoch: 2, topics: [], forgotten: [0] },
      { sessionId: 0, sessionEpoch: 0, topics: [0], forgotten: [] },
    ])
    expect(harness.findBrokerCalls.map(({ nodeId }) => nodeId)).toEqual([0, 0, 0, 1])
    expect(frames.map(({ rackId }) => rackId)).toEqual(['rack-a', 'rack-a', 'rack-a', 'rack-a'])
  })

  describe('closing', () => {
    const threeNodes = async () => {
      harness = await open({
        partitions: [0, 1, 2],
        partitionLeaders: { 0: 0, 1: 1, 2: 2 },
        knownNodes: [0, 1, 2],
      })
      for (const node of [0, 1, 2]) {
        await nextRound(harness.group, reply([11, 12, 0][node], [node]), node)
      }
      harness.setResponse(fetchResponse([], { sessionId: 0, includeTopic: false }))
    }

    test('bounds parallel session closes under the original captured close scope', async () => {
      await threeNodes()
      const held = holdFetches(harness.broker)
      const before = harness.writes().length
      const { fetchSessions } = harness.group

      const close = harness.group.closeFetchSessions({
        deadlineAt: performance.now() + 10000,
        guard: () => null,
      })
      await until(() => held.pending() === 2)
      expect(fetchSessions.closeAll()).toBe(fetchSessions.closing)

      const closes = harness
        .fetchFrames()
        .slice(3)
        .map((frame) => [frame.sessionId, frame.sessionEpoch, frame.maxWaitMs, frame.apiVersion])
      expect(closes.sort()).toEqual([
        [11, -1, 0, 18],
        [12, -1, 0, 18],
      ])
      expect(harness.fetchFrames().slice(3).map(shape)).toEqual([
        expect.objectContaining({ topics: [], forgotten: [] }),
        expect.objectContaining({ topics: [], forgotten: [] }),
      ])
      expect(harness.writes().length).toBeGreaterThan(before)
      held.release()
      await close
      for (const { fence } of harness.findBrokerCalls.slice(3)) {
        expect(fence.remaining()).toBeLessThanOrEqual(10000)
      }
      expect(fetchSessions.sessions.size).toBe(0)
    })

    test('a spent budget starts no close and a failed close is only logged', async () => {
      await threeNodes()
      const debug = jest.spyOn(harness.group.logger, 'debug')
      const written = harness.writes().length

      await harness.group.closeFetchSessions({ deadlineAt: performance.now() })

      expect(harness.writes()).toHaveLength(written)
      expect(debug).toHaveBeenCalledWith('Fetch session close failed', expect.any(Object))
    })

    test('a close failure never rejects', async () => {
      await threeNodes()
      jest.spyOn(harness.broker, 'fetch').mockRejectedValue(new Error('refused'))
      const debug = jest.spyOn(harness.group.logger, 'debug')

      await expect(harness.group.closeFetchSessions()).resolves.toBeUndefined()

      expect(
        debug.mock.calls.filter(([message]) => message === 'Fetch session close failed')
      ).toHaveLength(2)
    })

    test('an unbounded close is capped by the smaller of the request timeout and two seconds', async () => {
      await threeNodes()

      await harness.group.closeFetchSessions()

      const remaining = harness.findBrokerCalls.slice(3).map(({ fence }) => fence.remaining())
      expect(remaining).toHaveLength(2)
      for (const left of remaining) expect(left).toBeLessThanOrEqual(2000)
    })

    test('a smaller request timeout lowers the unbounded cap', async () => {
      harness = await open({ requestTimeout: 500 })
      await nextRound(harness.group, reply(42, [0]))
      harness.setResponse(fetchResponse([], { sessionId: 0, includeTopic: false }))

      await harness.group.closeFetchSessions()

      expect(harness.findBrokerCalls.slice(-1)[0].fence.remaining()).toBeLessThanOrEqual(500)
    })

    test('a group without sessions has nothing to close and constructs no registry', async () => {
      harness = await createFetchGroup()

      expect(harness.group.fetchSessions).toBeNull()
      await harness.group.closeFetchSessions()
      harness.group.retireFetchSessions()
      expect(harness.writes()).toHaveLength(1)
    })
  })

  test('preserves reset obligations until a confirmed concrete commit', async () => {
    harness = await open()
    harness.group.obligations.owe(TOPIC, TOPIC_ID)

    await nextRound(harness.group, reply(42, [0]))
    await nextRound(harness.group, reply(42, [0]))
    await nextRound(harness.group, reply(42, []))
    harness.group.retireFetchSessions()

    expect(harness.group.obligations.isOwed(TOPIC, 0)).toBe(true)
    expect(harness.offsetCommit).not.toHaveBeenCalled()
  })

  test('a rebuilt group builds a fresh registry and never reuses a session', async () => {
    harness = await open()
    await nextRound(harness.group, reply(42, [0]))
    const { fetchSessions: previous } = harness.group
    const session = previous.sessions.get(NODE_ID)
    const rebuilt = await open()
    try {
      rebuilt.setResponse(reply(7, [0]))
      await rebuilt.group.fetch(NODE_ID)

      expect(rebuilt.group.fetchSessions).not.toBe(previous)
      expect(rebuilt.group.fetchSessions.sessions.get(NODE_ID)).not.toBe(session)
      expect(shape(rebuilt.fetchFrames()[0])).toEqual({
        sessionId: 0,
        sessionEpoch: 0,
        topics: [0],
        forgotten: [],
      })
    } finally {
      await rebuilt.broker.disconnect()
    }
  })

  test('sends the same full requests as before when sessions are off', async () => {
    harness = await createFetchGroup({ partitions: [0, 1], position: '5' })
    expect(harness.group.fetchSessions).toBeNull()

    for (let round = 0; round < 2; round++) {
      harness.setResponse(reply(0, [0, 1]))
      await harness.group.fetch(NODE_ID)
    }

    for (const frame of harness.fetchFrames()) {
      expect(frame.sessionId).toBe(0)
      expect(frame.sessionEpoch).toBe(-1)
      expect(frame.forgottenTopics).toEqual([])
      expect(shape(frame).topics.sort()).toEqual([0, 1])
    }
  })

  test('keeps below-v7 traffic sessionless with no node session', async () => {
    harness = await open({ fetchMaxVersion: 6 })

    await nextRound(
      harness.group,
      fetchResponse([{ partition: 0, errorCode: 0 }], { apiVersion: 6 })
    )

    expect(harness.group.fetchSessions.sessions.size).toBe(0)
  })
})
