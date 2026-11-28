const apiKeys = require('../../protocol/requests/apiKeys')
const { decode } = require('../../protocol/requests/offsetForLeaderEpoch/v4/response')
const { readRequestHeader } = require('testHelpers')
const {
  NODE_ID,
  TOPIC,
  apiVersionsBody,
  createFakeBroker,
  createFetchGroup,
  decodeOffsetForLeaderEpochRequests,
  offsetForLeaderEpochRawResponse,
  offsetForLeaderEpochResponse,
  withheldAnswer,
  scriptedEpochLog,
} = require('./fixtures/fetchHarness')

/**
 * The broker's answer to OffsetForLeaderEpoch comes from `LeaderEpochFileCache#endOffsetFor`
 * (storage/src/main/java/org/apache/kafka/storage/internals/epoch/LeaderEpochFileCache.java).
 * When it finds nothing (end offset -1), `UnifiedLog#endOffsetForEpoch` returns no value
 * (core/.../log/UnifiedLog.scala 4.0.0:944-951; storage/.../log/UnifiedLog.java 4.4.0:1316-1325)
 * and `Partition#lastOffsetForLeaderEpoch` sets only the partition and code NONE
 * (core/.../cluster/Partition.scala 4.0.0:1729-1731, 4.4.0:1598-1600), so the row carries the
 * message defaults: LeaderEpoch -1, EndOffset -1 (OffsetForLeaderEpochResponse.json).
 */
const CACHE = {
  epochs: [
    [3, 20],
    [5, 40],
    [8, 100],
  ],
  logEnd: 150,
}
const FOLLOWER = { ...CACHE, leader: false }
const FOLLOWER_AHEAD = { ...FOLLOWER, leaderEpoch: 9 }
const EMPTY_LEADER = { epochs: [], logEnd: 0, leaderEpoch: 5 }
const UNDEFINED = { leaderEpoch: -1, endOffset: '-1' }
const FENCED = { errorCode: 74, ...UNDEFINED }
const UNKNOWN = { errorCode: 75, ...UNDEFINED }
const NOT_LEADER = { errorCode: 6, ...UNDEFINED }

/**
 * @param {number} leaderEpoch
 * @param {string} endOffset
 * @returns {{ errorCode: 0, leaderEpoch: number, endOffset: string }} a successful lookup row
 */
const lookupRow = (leaderEpoch, endOffset) => ({ errorCode: 0, leaderEpoch, endOffset })

/**
 * A request for one topic, in the shape the consumer group sends.
 * @param {number} partition
 * @param {number} leaderEpoch the epoch asked about
 * @param {number} [currentLeaderEpoch] the epoch the client believes the leader is at
 * @returns {{ topics: object[] }}
 */
const validationRequest = (partition, leaderEpoch, currentLeaderEpoch = 9) => ({
  topics: [{ topic: TOPIC, partitions: [{ partition, currentLeaderEpoch, leaderEpoch }] }],
})

const harnesses = []

/**
 * `createFetchGroup`, torn down after the test whatever its outcome.
 * @param {object} [options] the harness options
 * @returns {Promise<object>} the harness
 */
const fetchGroup = async (options) => {
  const harness = await createFetchGroup(options)
  harnesses.push(harness)
  return harness
}

afterEach(async () => {
  await Promise.all(harnesses.splice(0).map((harness) => harness.teardown()))
})

/** @returns {Promise<void>} once every queued immediate has run */
const nextTurn = () => new Promise(setImmediate)

/**
 * Lets promise reactions run without any timer or immediate, as under a frozen clock.
 * @returns {Promise<void>}
 */
const reactions = async () => {
  for (let hop = 0; hop < 1000; hop += 1) await Promise.resolve()
}

/** The clock of the frozen-clock cases: timers and immediates faked, promise jobs real. */
const freezeClock = () => jest.useFakeTimers({ doNotFake: ['nextTick', 'queueMicrotask'] })

describe('Test harness > scripted OffsetForLeaderEpoch answers', () => {
  // Source lines are LeaderEpochFileCache.java at 4.0.0 / 4.4.0.
  test.each([
    // the latest cached epoch: that epoch and the log end (4.0.0:293-298, 4.4.0:292-297)
    ['the current epoch', 8, CACHE, { leaderEpoch: 8, endOffset: '150' }],
    // same branch for a leader that wrote nothing in its epoch yet (start = log end)
    [
      'the current epoch before any record of it',
      8,
      {
        epochs: [
          [3, 20],
          [8, 150],
        ],
        logEnd: 150,
      },
      { leaderEpoch: 8, endOffset: '150' },
    ],
    // floor and next start (4.0.0:300,306,314-317; 4.4.0:299,305,313-316)
    [
      'an epoch present in the cache below the current one',
      5,
      CACHE,
      { leaderEpoch: 5, endOffset: '100' },
    ],
    ['an epoch between two cached epochs', 6, CACHE, { leaderEpoch: 5, endOffset: '100' }],
    // no floor: the requested epoch and the first cached start (4.0.0:307-313, 4.4.0:306-312)
    ['an epoch below the first cached one', 2, CACHE, { leaderEpoch: 2, endOffset: '20' }],
    // no higher entry: undefined (4.0.0:300-304, 4.4.0:299-303)
    ['an epoch above the current one', 9, CACHE, UNDEFINED],
    // no latest epoch and no higher entry: undefined (4.0.0:293,300-304; 4.4.0:292,299-303)
    ['any epoch of an empty cache', 5, { epochs: [], logEnd: 0 }, UNDEFINED],
    // the undefined epoch itself (4.0.0:289-292, 4.4.0:288-291)
    ['the undefined epoch', -1, CACHE, UNDEFINED],
  ])('scriptedEpochLog answers %s as the broker does', (_case, requested, log, expected) => {
    expect(scriptedEpochLog(log).endOffsetFor(requested)).toEqual(expected)
  })

  // ReplicaManager#lastOffsetForLeaderEpoch treats a current epoch of -1 as absent and asks only
  // the leader (ReplicaManager.scala 4.0.0:2593-2602, 4.4.0:2266-2275). The epoch check
  // (Partition.scala checkCurrentLeaderEpoch 4.0.0:509-522, 4.4.0:369-382) runs before the leader
  // check (getLocalLog 4.0.0:524-541, 4.4.0:384-401), and an error row carries only partition and
  // code (lastOffsetForLeaderEpoch 4.0.0:1716-1738, 4.4.0:1585-1607).
  test.each([
    ['a current epoch below the broker one', CACHE, 7, 5, FENCED],
    ['a current epoch above the broker one', CACHE, 9, 5, UNKNOWN],
    ['a follower at the current epoch', FOLLOWER, 8, 5, NOT_LEADER],
    ['a follower whose epoch is ahead of its cache', FOLLOWER_AHEAD, 9, 5, NOT_LEADER],
    ['a current epoch below a follower one, checked before leadership', FOLLOWER, 7, 5, FENCED],
    ['a current epoch above a follower one, checked before leadership', FOLLOWER, 9, 5, UNKNOWN],
    ['no current epoch at a follower', FOLLOWER, -1, 5, NOT_LEADER],
    ['no current epoch at the leader', CACHE, -1, 5, lookupRow(5, '100')],
    ['the latest epoch', CACHE, 8, 8, lookupRow(8, '150')],
    ['a cached epoch below the latest', CACHE, 8, 5, lookupRow(5, '100')],
    ['an epoch between cached epochs', CACHE, 8, 6, lookupRow(5, '100')],
    ['an epoch below the first cached one', CACHE, 8, 2, lookupRow(2, '20')],
    ['an epoch above the latest', CACHE, 8, 9, lookupRow(-1, '-1')],
    ['an empty cache', EMPTY_LEADER, 5, 5, lookupRow(-1, '-1')],
    ['the undefined epoch', CACHE, 8, -1, lookupRow(-1, '-1')],
  ])(
    'the encoded answer to %s carries the code and fields the broker sends',
    async (_case, log, currentLeaderEpoch, leaderEpoch, expected) => {
      const body = scriptedEpochLog(log).answer([3], {
        nodeId: NODE_ID,
        partitions: [{ partition: 3, currentLeaderEpoch, leaderEpoch }],
      })

      expect((await decode(body)).topics).toEqual([
        { topic: TOPIC, partitions: [{ partition: 3, ...expected }] },
      ])
    }
  )

  test('scriptedEpochLog writes one row per requested partition, each for the epoch it asked about', async () => {
    const body = scriptedEpochLog(CACHE).answer([0, 1, 2], {
      nodeId: NODE_ID,
      partitions: [
        { partition: 0, currentLeaderEpoch: 8, leaderEpoch: 5 },
        { partition: 1, currentLeaderEpoch: 8, leaderEpoch: 2 },
        { partition: 2, currentLeaderEpoch: 8, leaderEpoch: 9 },
      ],
    })

    expect((await decode(body)).topics).toEqual([
      {
        topic: TOPIC,
        partitions: [
          { errorCode: 0, partition: 0, leaderEpoch: 5, endOffset: '100' },
          { errorCode: 0, partition: 1, leaderEpoch: 2, endOffset: '20' },
          { errorCode: 0, partition: 2, leaderEpoch: -1, endOffset: '-1' },
        ],
      },
    ])
  })

  test('scriptedEpochLog refuses to answer a row without a current leader epoch', () => {
    const { answer } = scriptedEpochLog(CACHE)

    expect(() => answer([0], { partitions: [{ partition: 0, leaderEpoch: 5 }] })).toThrow(
      /partition 0 carries no current leader epoch/
    )
  })

  test.each([
    [
      'epochs that do not rise',
      [
        [5, 0],
        [5, 10],
      ],
      20,
    ],
    [
      'starts that fall',
      [
        [3, 10],
        [5, 0],
      ],
      20,
    ],
    [
      'a start past the log end',
      [
        [3, 0],
        [5, 30],
      ],
      20,
    ],
  ])('scriptedEpochLog refuses a cache with %s', (_case, epochs, logEnd) => {
    expect(() => scriptedEpochLog({ epochs, logEnd })).toThrow(/epoch 5/)
  })

  // LeaderEpochFileCache#assign refuses a negative epoch or start and removes every entry at or
  // above the new epoch or start (4.0.0:125-139,151-154; 4.4.0:124-138,150-153). A leader assigns
  // its epoch at its log end on election (Partition.scala 4.0.0:775-788, 4.4.0:636-649).
  test.each([
    [
      'an epoch that starts where the one before it started, which the broker removes',
      {
        epochs: [
          [3, 20],
          [5, 40],
          [6, 40],
        ],
        logEnd: 50,
      },
      /epoch 6 at 40 does not follow epoch 5/,
    ],
    [
      'a negative cached epoch',
      {
        epochs: [
          [-1, 0],
          [5, 10],
        ],
        logEnd: 20,
      },
      /cached epoch -1 is not an integer/,
    ],
    [
      'a negative cached start',
      { epochs: [[5, -1]], logEnd: 20 },
      /epoch 5 start -1 is not an exact int64/,
    ],
    [
      'a leader whose epoch is not its latest cached one',
      {
        epochs: [
          [3, 0],
          [5, 10],
        ],
        logEnd: 20,
        leaderEpoch: 6,
      },
      /leader epoch 6/,
    ],
    [
      'a follower whose epoch is below its latest cached one',
      {
        epochs: [
          [3, 0],
          [5, 10],
        ],
        logEnd: 20,
        leader: false,
        leaderEpoch: 4,
      },
      /leader epoch 4/,
    ],
    ['a negative leader epoch', { epochs: [], logEnd: 0, leaderEpoch: -1 }, /leader epoch -1/],
    // LeaderEpoch is an int32 and EndOffset an int64 (OffsetForLeaderEpochResponse.json, 4.0.0
    // and 4.4.0, lines 38-47); a log end, like a start, is 0 or more.
    [
      'a log end beyond the int64 range',
      { epochs: [[5, '0']], logEnd: '9223372036854775808' },
      /log end 9223372036854775808 is not an exact int64/,
    ],
    ['a negative log end of an empty cache', { epochs: [], logEnd: '-1' }, /log end -1/],
    ['an unsafe numeric log end', { epochs: [], logEnd: 2 ** 53 + 2 }, /log end 9007199254740994/],
    ['a fractional cached epoch', { epochs: [[5.5, 0]], logEnd: 10 }, /cached epoch 5.5/],
    ['a cached epoch that is not a number', { epochs: [[NaN, 0]], logEnd: 10 }, /cached epoch NaN/],
    [
      'a cached epoch beyond the int32 range',
      { epochs: [[2 ** 31, 0]], logEnd: 10 },
      /cached epoch 2147483648/,
    ],
    ['a fractional cached start', { epochs: [[5, 1.5]], logEnd: 10 }, /epoch 5 start 1.5/],
    [
      'a fractional leader epoch',
      { epochs: [], logEnd: 0, leaderEpoch: 5.5 },
      /leader epoch 5.5 is not an integer/,
    ],
    [
      'a leader epoch beyond the int32 range',
      { epochs: [], logEnd: 0, leaderEpoch: 2 ** 31 },
      /leader epoch 2147483648 is not an integer/,
    ],
  ])('scriptedEpochLog refuses %s', (_case, log, message) => {
    expect(() => scriptedEpochLog(log)).toThrow(message)
  })

  test('scriptedEpochLog refuses a requested epoch that is not an int32', () => {
    expect(() => scriptedEpochLog(CACHE).endOffsetFor(1.5)).toThrow(/requested epoch 1.5/)
  })

  test('scriptedEpochLog keeps offsets beyond the safe integer range exact in the encoded answer', async () => {
    const body = scriptedEpochLog({
      epochs: [
        [4, '9007199254740993'],
        [5, '9223372036854775806'],
      ],
      logEnd: '9223372036854775807',
    }).answer([0, 1], {
      partitions: [
        { partition: 0, currentLeaderEpoch: 5, leaderEpoch: 5 },
        { partition: 1, currentLeaderEpoch: 5, leaderEpoch: 4 },
      ],
    })

    expect((await decode(body)).topics[0].partitions).toEqual([
      { errorCode: 0, partition: 0, leaderEpoch: 5, endOffset: '9223372036854775807' },
      { errorCode: 0, partition: 1, leaderEpoch: 4, endOffset: '9223372036854775806' },
    ])
  })

  test.each([
    ['a leader epoch below -1', { partition: 0, leaderEpoch: -2 }, /leader epoch -2/],
    ['a fractional leader epoch', { partition: 0, leaderEpoch: 4.5 }, /leader epoch 4.5/],
    ['an end offset below -1', { partition: 0, endOffset: '-2' }, /end offset -2/],
    [
      'an end offset beyond the int64 range',
      { partition: 0, endOffset: '9223372036854775808' },
      /end offset 9223372036854775808/,
    ],
    [
      'an unsafe numeric end offset',
      { partition: 0, endOffset: 2 ** 53 + 2 },
      /end offset 9007199254740994/,
    ],
    ['a negative partition', { partition: -1 }, /partition -1/],
    ['an error code beyond int16', { partition: 0, errorCode: 2 ** 15 }, /error code 32768/],
    // An error row carries partition and code alone, so both fields keep their -1 defaults; a
    // successful lookup is a found pair or -1 with -1 (Partition.scala 4.0.0:1716-1738,
    // 4.4.0:1585-1607; UnifiedLog 4.0.0:944-951, 4.4.0:1316-1325).
    [
      'an error row with a defined epoch and end offset',
      { partition: 0, errorCode: 6, leaderEpoch: 4, endOffset: '20' },
      /error 6 with LeaderEpoch 4 and EndOffset 20 is no broker answer/,
    ],
    [
      'an undefined end offset with a defined epoch',
      { partition: 0, leaderEpoch: 4, endOffset: '-1' },
      /LeaderEpoch 4 and EndOffset -1 is no broker answer/,
    ],
    [
      'an undefined epoch with a defined end offset',
      { partition: 0, leaderEpoch: -1, endOffset: '20' },
      /LeaderEpoch -1 and EndOffset 20 is no broker answer/,
    ],
  ])('offsetForLeaderEpochResponse refuses %s', (_case, row, message) => {
    expect(() => offsetForLeaderEpochResponse([row])).toThrow(message)
  })

  test('offsetForLeaderEpochResponse gives an error row LeaderEpoch -1 and EndOffset -1 by default', async () => {
    const body = offsetForLeaderEpochResponse([{ partition: 0, errorCode: 6 }])

    expect((await decode(body)).topics).toEqual([
      { topic: TOPIC, partitions: [{ errorCode: 6, partition: 0, ...UNDEFINED }] },
    ])
  })

  test('offsetForLeaderEpochRawResponse encodes a malformed answer exactly as scripted', async () => {
    const row = { partition: 0, errorCode: 0, leaderEpoch: 4, endOffset: '-1' }

    expect((await decode(offsetForLeaderEpochRawResponse([row]))).topics).toEqual([
      { topic: TOPIC, partitions: [row] },
    ])
  })

  test('scriptedEpochLog refuses to answer a request for an epoch below -1', () => {
    const { answer } = scriptedEpochLog(CACHE)

    expect(() =>
      answer([0], { partitions: [{ partition: 0, currentLeaderEpoch: 8, leaderEpoch: -5 }] })
    ).toThrow(/asks for epoch -5, which no client sends/)
  })
})

describe('Test harness > withheld broker answers', () => {
  test('a withheld answer lets a later answer on the same connection through, and arrives when released', async () => {
    const withheld = withheldAnswer()
    let validations = 0
    const { broker } = createFakeBroker((header) => {
      if (header.apiKey === apiKeys.ApiVersions) return apiVersionsBody(18)
      validations += 1
      return validations === 1
        ? withheld.promise
        : offsetForLeaderEpochResponse([{ partition: 1, leaderEpoch: 4, endOffset: '30' }])
    })
    await broker.connect()

    try {
      const first = broker.offsetForLeaderEpoch(validationRequest(0, 4))
      const firstSettled = jest.fn()
      first.then(firstSettled, () => {})

      const second = await broker.offsetForLeaderEpoch(validationRequest(1, 4))
      expect(second.topics[0].partitions).toEqual([
        { errorCode: 0, partition: 1, leaderEpoch: 4, endOffset: '30' },
      ])
      expect(validations).toBe(2)
      expect(firstSettled).not.toHaveBeenCalled()

      withheld.resolve(
        offsetForLeaderEpochResponse([{ partition: 0, leaderEpoch: 4, endOffset: '20' }])
      )
      expect((await first).topics[0].partitions).toEqual([
        { errorCode: 0, partition: 0, leaderEpoch: 4, endOffset: '20' },
      ])
    } finally {
      await broker.disconnect({ drainTimeout: 0 })
    }
  })

  test('createFakeBroker hands each answer the frame it answers, even when requests are queued together', async () => {
    const answered = []
    const { broker } = createFakeBroker((header, frame) => {
      if (header.apiKey === apiKeys.ApiVersions) return apiVersionsBody(18)
      answered.push({ header: header.correlationId, frame: readRequestHeader(frame).correlationId })
      return offsetForLeaderEpochResponse([{ partition: 0 }])
    })
    await broker.connect()

    try {
      await Promise.all([
        broker.offsetForLeaderEpoch(validationRequest(0, 4)),
        broker.offsetForLeaderEpoch(validationRequest(1, 4)),
      ])

      expect(answered).toHaveLength(2)
      expect(answered[0].header).not.toBe(answered[1].header)
      for (const { header, frame } of answered) expect(frame).toBe(header)
    } finally {
      await broker.disconnect({ drainTimeout: 0 })
    }
  })

  test('a rejected withheld answer fails the request it answers with the rejection', async () => {
    const cancelled = new Error('answer cancelled')
    const { broker } = createFakeBroker((header) =>
      header.apiKey === apiKeys.ApiVersions ? apiVersionsBody(18) : Promise.reject(cancelled)
    )
    await broker.connect()

    try {
      await expect(broker.offsetForLeaderEpoch(validationRequest(0, 4))).rejects.toBe(cancelled)
      // The failed request left the connection's queue, so an ordinary disconnect drains.
      await broker.disconnect()
    } finally {
      await broker.disconnect({ drainTimeout: 0 })
    }
  })

  test('a withheld answer rejected before the fake broker asks for it fails its request', async () => {
    const withheld = withheldAnswer()
    const cancelled = new Error('cancelled before the answer was asked for')
    const { broker } = createFakeBroker((header) =>
      header.apiKey === apiKeys.ApiVersions ? apiVersionsBody(18) : withheld.promise
    )
    await broker.connect()

    try {
      const request = broker.offsetForLeaderEpoch(validationRequest(0, 4))
      // Same turn: the fake broker answers the frame only a turn after it is written.
      withheld.reject(cancelled)

      await expect(request).rejects.toBe(cancelled)
      await broker.disconnect()
    } finally {
      await broker.disconnect({ drainTimeout: 0 })
    }
  })

  test('an assertion that throws while a withheld answer is played back fails the request it answers', async () => {
    const withheld = withheldAnswer()
    const layout = new Error('the response frame has the wrong layout')
    const { broker } = createFakeBroker(
      (header) => (header.apiKey === apiKeys.ApiVersions ? apiVersionsBody(18) : withheld.promise),
      (frame, header) => {
        if (header.apiKey === apiKeys.OffsetForLeaderEpoch) throw layout
        return [frame]
      }
    )
    await broker.connect()

    try {
      const outcome = broker.offsetForLeaderEpoch(validationRequest(0, 4)).then(
        () => 'answered',
        (error) => error
      )
      withheld.resolve(offsetForLeaderEpochResponse([{ partition: 0 }]))

      expect(await outcome).toBe(layout)
      await broker.disconnect()
    } finally {
      await broker.disconnect({ drainTimeout: 0 })
    }
  })

  test('an answer that throws fails the request it answers alone, and the connection stays usable and drains', async () => {
    const refused = new Error('no answer for partition 1')
    const { broker } = createFakeBroker((header, frame) => {
      if (header.apiKey === apiKeys.ApiVersions) return apiVersionsBody(18)
      const [{ topics }] = decodeOffsetForLeaderEpochRequests([frame])
      if (topics[0].partitions[0].partition === 1) throw refused
      return offsetForLeaderEpochResponse([{ partition: 0, leaderEpoch: 4, endOffset: '20' }])
    })
    await broker.connect()

    try {
      const [first, second] = await Promise.allSettled([
        broker.offsetForLeaderEpoch(validationRequest(0, 4)),
        broker.offsetForLeaderEpoch(validationRequest(1, 4)),
      ])

      expect(first.status).toBe('fulfilled')
      expect(first.value.topics[0].partitions).toEqual([
        { errorCode: 0, partition: 0, leaderEpoch: 4, endOffset: '20' },
      ])
      expect(second.status).toBe('rejected')
      expect(second.reason).toBe(refused)

      const after = await broker.offsetForLeaderEpoch(validationRequest(0, 4))
      expect(after.topics[0].partitions).toEqual([
        { errorCode: 0, partition: 0, leaderEpoch: 4, endOffset: '20' },
      ])
      await broker.disconnect()
    } finally {
      await broker.disconnect({ drainTimeout: 0 })
    }
  })
})

describe('Test harness > fake broker close', () => {
  afterEach(() => jest.useRealTimers())

  test('close fails a request whose answer is withheld at once, answers no frame written before or after it, and ignores the withheld answer released after it', async () => {
    const withheld = withheldAnswer()
    const asked = []
    const { broker, state, close } = createFakeBroker((header) => {
      if (header.apiKey === apiKeys.ApiVersions) return apiVersionsBody(18)
      asked.push(header.correlationId)
      return withheld.promise
    })
    await broker.connect()

    try {
      const outcome = jest.fn()
      broker.offsetForLeaderEpoch(validationRequest(0, 4)).then(
        () => outcome('answered'),
        (error) => outcome(error.name)
      )
      while (asked.length === 0) await nextTurn()
      const [socket] = state.sockets
      const [frame] = socket.writes.slice(-1)

      // Written in the same turn as the close: its answer is queued for a later turn.
      socket.write(frame)
      await close()
      socket.write(frame)
      withheld.resolve(offsetForLeaderEpochResponse([{ partition: 0 }]))
      await nextTurn()
      await nextTurn()

      expect(outcome.mock.calls).toEqual([['KafkaJSConnectionClosedError']])
      expect(asked).toHaveLength(1)
      expect(socket.destroyed).toBe(true)
      expect(socket.writes.slice(-3)).toEqual([frame, frame, frame])
      // Nothing is left in flight, so an ordinary disconnect has nothing to wait for.
      await broker.disconnect()
    } finally {
      await close()
    }
  })

  test('a closed fake broker refuses a later connect and request before it opens a socket or writes anything', async () => {
    const { broker, state, close } = createFakeBroker((header) =>
      header.apiKey === apiKeys.ApiVersions
        ? apiVersionsBody(18)
        : offsetForLeaderEpochResponse([{ partition: 0 }])
    )
    await broker.connect()

    try {
      await close()
      const writes = state.writes.length
      const connected = jest.fn()
      const requested = jest.fn()
      broker.connect().then(
        () => connected('connected'),
        (error) => connected(error.name, error.message)
      )
      broker.offsetForLeaderEpoch(validationRequest(0, 4)).then(
        () => requested('answered'),
        (error) => requested(error.name, error.message)
      )
      await nextTurn()
      await nextTurn()

      expect(connected.mock.calls).toEqual([
        ['KafkaJSConnectionError', 'The fake broker was closed'],
      ])
      expect(requested.mock.calls).toEqual([
        ['KafkaJSConnectionError', 'The fake broker was closed'],
      ])
      expect(state.writes).toHaveLength(writes)
      expect(state.sockets).toHaveLength(1)
      expect(state.sockets[0].destroyed).toBe(true)
    } finally {
      await close()
    }
  })

  test('an ordinary disconnect cancels the answer queued on its socket, and the fake broker answers again on the next socket', async () => {
    const answered = []
    const { broker, state, close } = createFakeBroker((header) => {
      if (header.apiKey === apiKeys.ApiVersions) return apiVersionsBody(18)
      answered.push(header.correlationId)
      return offsetForLeaderEpochResponse([{ partition: 0 }])
    })
    await broker.connect()

    try {
      await broker.offsetForLeaderEpoch(validationRequest(0, 4))
      const [socket] = state.sockets
      const [frame] = socket.writes.slice(-1)

      // Its answer is queued for a later turn, after the socket is gone.
      socket.write(frame)
      await broker.disconnect({ drainTimeout: 0 })
      await nextTurn()
      await nextTurn()

      expect(socket.destroyed).toBe(true)
      expect(answered).toHaveLength(1)

      const after = await broker.offsetForLeaderEpoch(validationRequest(0, 4))
      expect(after.topics[0].partitions).toEqual([
        { errorCode: 0, partition: 0, leaderEpoch: 4, endOffset: '20' },
      ])
      expect(state.sockets).toHaveLength(2)
      expect(answered).toHaveLength(2)
    } finally {
      await close()
    }
  })

  test('a frame the fake broker cannot read closes its connection and fails the request in flight on it', async () => {
    const asked = []
    const { broker, state, close } = createFakeBroker((header) => {
      if (header.apiKey === apiKeys.ApiVersions) return apiVersionsBody(18)
      asked.push(header.correlationId)
      return new Promise(() => {})
    })
    await broker.connect()

    try {
      const outcome = jest.fn()
      broker.offsetForLeaderEpoch(validationRequest(0, 4)).then(
        () => outcome('answered'),
        (error) => outcome(error.name, error.message)
      )
      while (asked.length === 0) await nextTurn()
      const [socket] = state.sockets

      socket.write(Buffer.alloc(2))
      await nextTurn()
      await nextTurn()

      expect(socket.destroyed).toBe(true)
      expect(outcome.mock.calls).toEqual([
        [
          'KafkaJSConnectionError',
          'Connection error: The fake broker could not read a request frame',
        ],
      ])
    } finally {
      await close()
    }
  })

  test('a frame whose size prefix disagrees with its length closes its connection without an answer and fails the request in flight on it', async () => {
    const asked = []
    const { broker, state, close } = createFakeBroker((header) => {
      if (header.apiKey === apiKeys.ApiVersions) return apiVersionsBody(18)
      asked.push(header.correlationId)
      return new Promise(() => {})
    })
    await broker.connect()

    try {
      const outcome = jest.fn()
      broker.offsetForLeaderEpoch(validationRequest(0, 4)).then(
        () => outcome('answered'),
        (error) => outcome(error.name, error.message)
      )
      while (asked.length === 0) await nextTurn()
      const [socket] = state.sockets
      const malformed = Buffer.from(socket.writes[socket.writes.length - 1])
      malformed.writeInt32BE(0, 0)
      // The header alone still reads: only the envelope is wrong.
      expect(readRequestHeader(malformed).correlationId).toBe(asked[0])

      socket.write(malformed)
      await nextTurn()
      await nextTurn()

      expect(asked).toHaveLength(1)
      expect(socket.destroyed).toBe(true)
      expect(outcome.mock.calls).toEqual([
        [
          'KafkaJSConnectionError',
          'Connection error: The fake broker could not read a request frame',
        ],
      ])
    } finally {
      await close()
    }
  })

  /**
   * A fake broker whose OffsetForLeaderEpoch playback closes it from the callback `closeIn` names,
   * with the data its socket emits after connecting recorded.
   * @param {'answer'|'answerThrows'|'chunks'|'chunkIterator'} closeIn
   * @returns {Promise<{ broker: object, socket: object, data: Buffer[], closing: () => Promise<void>,
   *   close: () => Promise<void> }>}
   */
  const closingFrom = async (closeIn) => {
    let closing = null
    const closeNow = () => {
      closing = fake.close()
    }
    const fake = createFakeBroker(
      (header) => {
        if (header.apiKey === apiKeys.ApiVersions) return apiVersionsBody(18)
        if (closeIn === 'answer') closeNow()
        if (closeIn === 'answerThrows') {
          closeNow()
          throw new Error('the answer closed the fake broker, then threw')
        }
        return offsetForLeaderEpochResponse([{ partition: 0 }])
      },
      (frame, header) => {
        if (header.apiKey !== apiKeys.OffsetForLeaderEpoch) return [frame]
        if (closeIn === 'chunks') {
          closeNow()
          return [frame]
        }
        if (closeIn !== 'chunkIterator') return [frame]
        return (function* () {
          yield frame.subarray(0, 6)
          closeNow()
          yield frame.subarray(6)
        })()
      }
    )
    await fake.broker.connect()
    const [socket] = fake.state.sockets
    const data = []
    socket.on('data', (chunk) => data.push(chunk))
    return { broker: fake.broker, socket, data, closing: () => closing, close: fake.close }
  }

  test.each([
    ['an answer that returns a body', 'answer', 0],
    ['an answer that throws', 'answerThrows', 0],
    ['the chunks callback', 'chunks', 0],
    ['a chunk iterator between two chunks', 'chunkIterator', 1],
  ])(
    'close from %s rejects the request through the shutdown and emits nothing more',
    async (_where, closeIn, chunksBeforeClose) => {
      const { broker, socket, data, closing, close } = await closingFrom(closeIn)

      try {
        const outcome = jest.fn()
        await broker.offsetForLeaderEpoch(validationRequest(0, 4)).then(
          () => outcome('answered'),
          (error) => outcome(error.name)
        )
        await closing()

        expect(outcome.mock.calls).toEqual([['KafkaJSConnectionClosedError']])
        expect(data).toHaveLength(chunksBeforeClose)
        expect(socket.destroyed).toBe(true)
      } finally {
        await close()
      }
    }
  )

  test('close aborts a connect still waiting for its socket and refuses the connect queued behind it, with the clock frozen', async () => {
    freezeClock()
    const answer = jest.fn(() => apiVersionsBody(18))
    const { broker, state, close } = createFakeBroker(answer)
    const first = jest.fn()
    const second = jest.fn()
    broker.connect().then(
      () => first('connected'),
      (error) => first(error.name, error.message)
    )
    broker.connect().then(
      () => second('connected'),
      (error) => second(error.name, error.message)
    )

    try {
      await reactions()
      expect(state.sockets).toHaveLength(1)

      const closed = jest.fn()
      Promise.resolve(close()).then(closed)
      await reactions()

      expect(closed).toHaveBeenCalledTimes(1)
      expect(first.mock.calls).toEqual([
        ['KafkaJSConnectionError', 'Connection closed while connecting'],
      ])
      expect(second.mock.calls).toEqual([['KafkaJSConnectionError', 'The fake broker was closed']])
      expect(state.sockets[0].destroyed).toBe(true)

      jest.advanceTimersByTime(0)
      await reactions()
      expect(state.sockets).toHaveLength(1)
      expect(state.writes).toEqual([])
      expect(answer).not.toHaveBeenCalled()
    } finally {
      await close()
    }
  })

  test('close aborts a connect whose ApiVersions answer is still queued, and that answer never plays back, with the clock frozen', async () => {
    freezeClock()
    const answer = jest.fn(() => apiVersionsBody(18))
    const { broker, state, close } = createFakeBroker(answer)
    const outcome = jest.fn()
    broker.connect().then(
      () => outcome('connected'),
      (error) => outcome(error.name)
    )

    try {
      await reactions()
      // The socket connects, and ApiVersions is written with its answer queued for a later turn.
      jest.advanceTimersByTime(0)
      await reactions()
      expect(state.writes.map((write) => readRequestHeader(write).apiKey)).toEqual([
        apiKeys.ApiVersions,
      ])
      expect(answer).not.toHaveBeenCalled()

      const closed = jest.fn()
      Promise.resolve(close()).then(closed)
      await reactions()

      expect(closed).toHaveBeenCalledTimes(1)
      expect(outcome.mock.calls).toEqual([['KafkaJSConnectionClosedError']])
      expect(state.sockets[0].destroyed).toBe(true)

      jest.advanceTimersByTime(0)
      await reactions()
      expect(answer).not.toHaveBeenCalled()
      expect(state.sockets).toHaveLength(1)
    } finally {
      await close()
    }
  })
})

describe('Test harness > fetch group routing', () => {
  test('by default the group sees a constant leader epoch and a forget only records the call', async () => {
    const { cluster, installRouting } = await fetchGroup({ knownNodes: [0, 1] })

    installRouting({ 0: 1 })
    expect(cluster.leaderEpoch(TOPIC, 0)).toBe(5)
    expect(cluster.cachedLeader(TOPIC, 0)).toBe(1)
    expect(cluster.forgetLeaderEpoch(TOPIC, 0)).toBe(true)
    expect(cluster.leaderEpoch(TOPIC, 0)).toBe(5)
  })

  test('with liveRouting the group sees the epoch the metadata cache holds, and a forget clears it there', async () => {
    const { cluster, installRouting, metadataCache } = await fetchGroup({
      knownNodes: [0, 1],
      liveRouting: true,
    })
    expect(cluster.leaderEpoch(TOPIC, 0)).toBe(5)

    installRouting({ 0: 1 })
    expect(cluster.leaderEpoch(TOPIC, 0)).toBe(6)
    expect(cluster.cachedLeader(TOPIC, 0)).toBe(1)

    await cluster.applyLeaderHint({ topic: TOPIC, partition: 0, leaderId: 0, leaderEpoch: 9 })
    expect(cluster.leaderEpoch(TOPIC, 0)).toBe(9)
    expect(cluster.cachedLeader(TOPIC, 0)).toBe(0)

    expect(cluster.forgetLeaderEpoch(TOPIC, 0)).toBe(true)
    expect(cluster.forgetLeaderEpoch).toHaveBeenCalledWith(TOPIC, 0)
    expect(cluster.leaderEpoch(TOPIC, 0)).toBe(-1)
    expect(metadataCache.leaderEpoch(TOPIC, 0)).toBe(-1)
  })

  test('with liveRouting an accepted leader hint moves routing, validation and Fetch to the new leader', async () => {
    const h = await fetchGroup({
      knownNodes: [0, 1],
      partitionLeaders: { 0: 1 },
      liveRouting: true,
      validationBrokers: true,
      committedEpoch: 4,
      position: '10',
    })
    // Node 0 leads at epoch 9, which it entered at offset 20.
    h.setValidationResponse(
      scriptedEpochLog({
        epochs: [
          [4, 0],
          [9, 20],
        ],
        logEnd: 20,
      }).answer
    )
    expect(h.cluster.findLeaderForPartitions(TOPIC, [0])).toEqual({ 1: [0] })

    await expect(
      h.cluster.applyLeaderHint({ topic: TOPIC, partition: 0, leaderId: 0, leaderEpoch: 9 })
    ).resolves.toBe(true)
    expect(h.cluster.findLeaderForPartitions(TOPIC, [0])).toEqual({ 0: [0] })
    expect(h.cluster.findTopicPartitionMetadata(TOPIC)).toEqual([{ partitionId: 0, leader: 0 }])

    await h.group.fetch(0)

    expect(h.validationRequestsTo(1)).toEqual([])
    expect(h.validationRequestsTo(0)).toEqual([
      {
        replicaId: -1,
        topics: [
          { topic: TOPIC, partitions: [{ partition: 0, currentLeaderEpoch: 9, leaderEpoch: 4 }] },
        ],
      },
    ])
    expect(h.fetchRequestPartitions()).toEqual([0])
  })

  test('with validationBrokers each node answers on its own connection and is told which node it is', async () => {
    const { cluster, setValidationResponse, validationRequestsTo, writesTo } = await fetchGroup({
      partitions: [0, 1],
      knownNodes: [0, 1],
      validationBrokers: true,
    })
    const asked = []
    setValidationResponse((partitions, { nodeId, partitions: rows }) => {
      asked.push({ nodeId, partitions, epochs: rows.map(({ leaderEpoch }) => leaderEpoch) })
      return offsetForLeaderEpochResponse(partitions.map((partition) => ({ partition })))
    })

    const node0 = await cluster.findBroker({ nodeId: 0 })
    const node1 = await cluster.findBroker({ nodeId: 1 })
    expect(node1).not.toBe(node0)
    expect(await cluster.findBroker({ nodeId: 1 })).toBe(node1)

    await node1.offsetForLeaderEpoch(validationRequest(1, 3))

    expect(asked).toEqual([{ nodeId: 1, partitions: [1], epochs: [3] }])
    expect(validationRequestsTo(1)).toEqual([
      {
        replicaId: -1,
        topics: [
          { topic: TOPIC, partitions: [{ partition: 1, currentLeaderEpoch: 9, leaderEpoch: 3 }] },
        ],
      },
    ])
    expect(validationRequestsTo(0)).toEqual([])
    expect(writesTo(2)).toEqual([])
  })

  test('validation requests queued together each receive the rows of their own frame', async () => {
    const { broker, setValidationResponse, validationRequests } = await fetchGroup({
      partitions: [0, 1],
    })
    setValidationResponse((_partitions, { partitions: rows }) =>
      offsetForLeaderEpochResponse(
        rows.map(({ partition, leaderEpoch }) => ({
          partition,
          leaderEpoch,
          endOffset: String(10 + partition),
        }))
      )
    )

    const [first, second] = await Promise.all([
      broker.offsetForLeaderEpoch(validationRequest(0, 4)),
      broker.offsetForLeaderEpoch(validationRequest(1, 3)),
    ])

    expect(validationRequests()).toHaveLength(2)
    expect(first.topics[0].partitions).toEqual([
      { errorCode: 0, partition: 0, leaderEpoch: 4, endOffset: '10' },
    ])
    expect(second.topics[0].partitions).toEqual([
      { errorCode: 0, partition: 1, leaderEpoch: 3, endOffset: '11' },
    ])
  })

  test('a held broker lookup waits until released while other nodes are found', async () => {
    const { cluster, holdBrokerLookup } = await fetchGroup({
      knownNodes: [0, 1],
      validationBrokers: true,
    })
    const release = holdBrokerLookup(1)
    const found = jest.fn()
    const held = cluster.findBroker({ nodeId: 1 }).then(found)

    await cluster.findBroker({ nodeId: 0 })
    await nextTurn()
    expect(found).not.toHaveBeenCalled()

    release()
    await held
    expect(found).toHaveBeenCalledTimes(1)
    expect(found.mock.calls[0][0]).not.toBe(await cluster.findBroker({ nodeId: 0 }))
  })

  test('a lookup held twice on one node waits for both releases', async () => {
    const { cluster, holdBrokerLookup } = await fetchGroup({
      knownNodes: [0, 1],
      validationBrokers: true,
    })
    // Node 1's broker is connected first, so a lookup let through resolves within a turn.
    await cluster.findBroker({ nodeId: 1 })
    const releaseFirst = holdBrokerLookup(1)
    const found = jest.fn()
    const held = cluster.findBroker({ nodeId: 1 }).then(found)
    const releaseSecond = holdBrokerLookup(1)

    releaseFirst()
    await nextTurn()
    expect(found).not.toHaveBeenCalled()

    releaseSecond()
    await held
    expect(found).toHaveBeenCalledTimes(1)
  })
})

describe('Test harness > fetch group teardown', () => {
  test('teardown fails a validation request whose answer is withheld', async () => {
    const { broker, setValidationResponse, validationRequests, teardown } = await fetchGroup()
    setValidationResponse(() => new Promise(() => {}))

    const outcome = broker.offsetForLeaderEpoch(validationRequest(0, 4)).then(
      () => 'answered',
      (error) => error.name
    )
    while (validationRequests().length === 0) await nextTurn()

    await teardown()

    expect(await outcome).toBe('KafkaJSConnectionClosedError')
  })

  test('teardown cancels the answer of a frame written in the same turn, so no answer runs after it', async () => {
    const { broker, setValidationResponse, validationRequests, teardown } = await fetchGroup()
    await broker.offsetForLeaderEpoch(validationRequest(0, 4))
    const answer = jest.fn(() => new Promise(() => {}))
    setValidationResponse(answer)
    const { socket } = await broker.connectionPool.getConnection()
    const [frame] = socket.writes.slice(-1)

    // Written just before the test ends, as by a request the test stops right after: its answer
    // is queued for a later turn.
    socket.write(frame)
    expect(validationRequests()).toHaveLength(2)
    await teardown()
    socket.write(frame)
    await nextTurn()
    await nextTurn()

    expect(validationRequests()).toHaveLength(3)
    expect(answer).not.toHaveBeenCalled()
  })

  test('teardown ends a held broker lookup with an error', async () => {
    const { cluster, holdBrokerLookup, teardown } = await fetchGroup({
      knownNodes: [0, 1],
      validationBrokers: true,
    })
    holdBrokerLookup(1)
    const lookup = cluster.findBroker({ nodeId: 1 }).then(
      () => 'found',
      (error) => error.message
    )

    await teardown()

    expect(await lookup).toBe('fetch harness torn down')
  })

  test('teardown refuses a lookup whose node was still connecting', async () => {
    const { cluster, teardown } = await fetchGroup({ knownNodes: [0, 1], validationBrokers: true })
    // Node 1's broker is created by this lookup and connects over later turns.
    const lookup = cluster.findBroker({ nodeId: 1 }).then(
      () => 'found',
      (error) => error.message
    )

    await teardown()

    expect(await lookup).toBe('fetch harness torn down')
  })

  test('teardown aborts a node still waiting for its socket without waiting for its handshake, with the clock frozen', async () => {
    const { cluster, teardown, socketsTo, writesTo } = await fetchGroup({
      knownNodes: [0, 1],
      validationBrokers: true,
    })
    freezeClock()

    try {
      const lookup = jest.fn()
      cluster.findBroker({ nodeId: 1 }).then(
        () => lookup('found'),
        (error) => lookup(error.message)
      )
      await reactions()

      const done = jest.fn()
      teardown().then(done)
      await reactions()

      expect(done).toHaveBeenCalledTimes(1)
      expect(lookup.mock.calls).toEqual([['fetch harness torn down']])
      expect(socketsTo(1)).toHaveLength(1)
      expect([...socketsTo(0), ...socketsTo(1)].map(({ destroyed }) => destroyed)).toEqual([
        true,
        true,
      ])

      jest.advanceTimersByTime(0)
      await reactions()
      expect(writesTo(1)).toEqual([])
      expect(socketsTo(1)).toHaveLength(1)
    } finally {
      jest.useRealTimers()
    }
  })

  test('concurrent teardown callers wait for the same cleanup, which runs once', async () => {
    const { broker, cluster, teardown } = await fetchGroup({
      knownNodes: [0, 1],
      validationBrokers: true,
    })
    const disconnect = broker.disconnect.bind(broker)
    let closed = false
    const disconnects = jest.spyOn(broker, 'disconnect').mockImplementation(async (options) => {
      await disconnect(options)
      closed = true
    })
    const lookup = cluster.findBroker({ nodeId: 1 }).then(
      () => 'found',
      (error) => error.message
    )

    const first = teardown()
    const second = teardown()
    expect(second).toBe(first)
    await second

    expect(closed).toBe(true)
    expect(disconnects).toHaveBeenCalledTimes(1)
    expect(disconnects).toHaveBeenCalledWith({ drainTimeout: 0 })
    expect(await lookup).toBe('fetch harness torn down')
  })
})
