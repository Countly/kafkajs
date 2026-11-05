const createAdmin = require('../../admin')
const { createErrorFromCode } = require('../../protocol/error')
const { newLogger } = require('testHelpers')
const {
  MOVE_WAIT_MS,
  ROUND_ALLOWANCE_MS,
  UNHINTED_OUTAGE,
  moveProblems,
  partitionLeader,
  pollAtRoute,
  siblingProblems,
} = require('./fixtures/fetchRoutingOracle')

const LEADER_NOT_AVAILABLE = 5

const busy = {
  kind: 'busy',
  hinted: true,
  newNode: 2,
  hint: { leaderId: 2 },
  anyHint: true,
  refreshed: true,
  priorFetches: 1,
  lossPollWriteAt: 9000,
  lossPollDoneAt: 10500,
  pollWriteAt: 9000,
  errorAt: 10000,
  routeAt: 10000,
  pollDoneAt: 10500,
  nextAt: 10600,
}
const idle = {
  ...busy,
  kind: 'idle',
  priorFetches: 0,
  lossPollWriteAt: null,
  lossPollDoneAt: null,
  pollWriteAt: null,
  pollDoneAt: null,
}

describe('Consumer > Fetch routing oracle (offline controls)', () => {
  it('passes a long poll that is outstanding at the loss and the route', () => {
    expect(moveProblems(busy)).toEqual([])
  })

  it('fails a long poll that completed before the move', () => {
    const done = {
      ...busy,
      errorAt: 10000,
      routeAt: 10000,
      lossPollDoneAt: 9900,
      pollDoneAt: 9900,
      nextAt: 10100,
    }
    expect(moveProblems(done)).toContain('the long poll was not outstanding at the loss')
  })

  it('fails a poll answered at the route instant', () => {
    expect(
      moveProblems({ ...busy, lossPollDoneAt: 10000, pollDoneAt: 10000, nextAt: 10100 })
    ).not.toEqual([])
  })

  it('fails a poll written after the route', () => {
    expect(moveProblems({ ...busy, pollWriteAt: 10001 })).not.toEqual([])
  })

  describe('an unhinted busy move, routed once the round it was in has ended', () => {
    const unhinted = {
      ...busy,
      hinted: false,
      hint: null,
      anyHint: false,
      pollWriteAt: 9000,
      errorAt: 9800,
      pollDoneAt: 10500,
      routeAt: 10503,
      nextAt: 10510,
    }

    it('passes a poll held at the loss whose end the route follows', () => {
      expect(moveProblems(unhinted)).toEqual([])
    })

    it('passes the numbers of the live run that failed on the route anchor', () => {
      const live = {
        ...unhinted,
        lossPollWriteAt: 73325.834949,
        lossPollDoneAt: 78328.846169,
        pollWriteAt: 73325.834949,
        errorAt: 74108.908003,
        pollDoneAt: 78328.846169,
        routeAt: 78332.333731,
        nextAt: 78337.735026,
      }
      expect(moveProblems(live)).toEqual([])
    })

    it('fails a poll that was answered before the loss', () => {
      expect(
        moveProblems({ ...unhinted, errorAt: 10600, nextAt: 10700, routeAt: 10650 })
      ).toContain('the long poll was not outstanding at the loss')
    })

    it('fails a poll written after the loss', () => {
      expect(moveProblems({ ...unhinted, errorAt: 8900 })).toContain(
        'the long poll was not outstanding at the loss'
      )
    })

    it('fails a Fetch that waits after the later of the poll end and the route', () => {
      const slow = { ...unhinted, nextAt: unhinted.routeAt + ROUND_ALLOWANCE_MS + 1 }
      expect(moveProblems(slow)).toContain('the route waited after the long poll')
    })

    it('does not charge a late route to the client when the Fetch follows it at once', () => {
      const elected = { ...unhinted, routeAt: 12000, nextAt: 12010 }
      expect(moveProblems(elected)).toEqual([])
    })
  })

  it('fails a busy move with no prior Fetch, never an idle pass', () => {
    const none = {
      ...busy,
      priorFetches: 0,
      lossPollWriteAt: null,
      lossPollDoneAt: null,
      pollWriteAt: null,
      pollDoneAt: null,
      nextAt: 10100,
    }
    expect(moveProblems(none)).toContain('the new node had no long poll outstanding')
  })

  it('fails a late route', () => {
    const late = { ...busy, nextAt: busy.nextAt + MOVE_WAIT_MS + 2 * ROUND_ALLOWANCE_MS }
    expect(moveProblems(late)).not.toEqual([])
  })

  it('requires the hint when hinted and none when not', () => {
    expect(moveProblems({ ...busy, hint: null })).not.toEqual([])
    const unhinted = { ...busy, hinted: false, hint: null, anyHint: false }
    expect(moveProblems(unhinted)).toEqual([])
    expect(moveProblems({ ...unhinted, anyHint: true })).not.toEqual([])
    expect(moveProblems({ ...unhinted, refreshed: false })).not.toEqual([])
  })

  it('keeps the idle bounds', () => {
    expect(moveProblems(idle)).toEqual([])
    expect(moveProblems({ ...idle, priorFetches: 1 })).not.toEqual([])
    expect(moveProblems({ ...idle, nextAt: idle.routeAt + ROUND_ALLOWANCE_MS + 1 })).not.toEqual([])
  })
})

describe('Consumer > Fetch routing oracle > a move around a node-set change', () => {
  // The numbers of a CI run whose unhinted move started a long poll after the route. The
  // observations a recreation leaves are diagnostics: they earn no allowance.
  const observation = { nodeSetChanged: true, drainEndAt: 74046.1 }
  const unhintedIdle = {
    kind: 'idle',
    hinted: false,
    newNode: 1,
    errorAt: 59227.729882,
    routeAt: 68995.814434,
    nextAt: 74051.248941,
    priorFetches: 0,
    lossPollWriteAt: null,
    lossPollDoneAt: null,
    pollWriteAt: null,
    pollDoneAt: null,
    hint: null,
    anyHint: false,
    refreshed: true,
  }
  const unhintedBusy = {
    ...unhintedIdle,
    kind: 'busy',
    newNode: 2,
    nextAt: 74048.183506,
    priorFetches: 4,
    lossPollWriteAt: 58982.758603,
    lossPollDoneAt: 63985.03353,
    pollWriteAt: 58982.758603,
    pollDoneAt: 63985.03353,
  }

  it('fails an idle start a long poll after the route whatever a recreation observed', () => {
    expect(moveProblems(unhintedIdle)).toEqual(['the idle route took longer than a round'])
    expect(moveProblems({ ...unhintedIdle, ...observation })).toEqual(moveProblems(unhintedIdle))
  })

  it('fails a busy start a long poll after the later of the poll end and the route', () => {
    expect(moveProblems(unhintedBusy)).toEqual(['the route waited after the long poll'])
    expect(moveProblems({ ...unhintedBusy, ...observation })).toEqual(moveProblems(unhintedBusy))
  })

  it('gives a move that meets the bounds the same verdict with or without the observation', () => {
    const meets = { ...unhintedIdle, nextAt: unhintedIdle.routeAt + 10 }
    expect(moveProblems(meets)).toEqual([])
    expect(moveProblems({ ...meets, ...observation })).toEqual([])
  })
})

describe('Consumer > Fetch routing oracle > the poll the route met', () => {
  const first = { at: 100, answer: { at: 900 } }
  const second = { at: 950, answer: { at: 2000 } }
  const third = { at: 2100 }

  it('passes separate polls outstanding at the loss and the route', () => {
    const poll = pollAtRoute([first, second, third], 1500)
    expect(
      moveProblems({
        ...busy,
        errorAt: 500,
        routeAt: 1500,
        nextAt: 2010,
        lossPollWriteAt: first.at,
        lossPollDoneAt: first.answer.at,
        pollWriteAt: poll.at,
        pollDoneAt: poll.answer.at,
      })
    ).toEqual([])
  })

  it('fails separate polls when none was outstanding at the loss', () => {
    const poll = pollAtRoute([first, second, third], 1500)
    expect(
      moveProblems({
        ...busy,
        errorAt: 500,
        routeAt: 1500,
        nextAt: 2010,
        lossPollWriteAt: first.at,
        lossPollDoneAt: 400,
        pollWriteAt: poll.at,
        pollDoneAt: poll.answer.at,
      })
    ).toEqual(['the long poll was not outstanding at the loss'])
  })

  it('is the Fetch written last by the route, not the one outstanding at the loss', () => {
    expect(pollAtRoute([first, second, third], 1500)).toBe(second)
  })

  it('counts a Fetch written at the route instant and none written after it', () => {
    expect(pollAtRoute([first, second, third], 950)).toBe(second)
    expect(pollAtRoute([first, second, third], 949)).toBe(first)
  })

  it('is nothing when the node had written no Fetch by the route', () => {
    expect(pollAtRoute([third], 1500)).toBeUndefined()
  })
})

describe('Consumer > Fetch routing oracle > the unhinted outage', () => {
  it('kills the broker: a clean stop moves the leaders first and the old leader hints', () => {
    expect(UNHINTED_OUTAGE).toBe('kill')
  })
})

describe('Consumer > Fetch routing oracle > the leader of a partition', () => {
  const leaderless = { topics: [{ partitions: [{ leader: -1 }] }] }

  it('asks through DescribeTopicPartitions, which reports no leader as -1', async () => {
    const admin = {
      fetchTopicMetadata: jest.fn(async ({ useDescribeTopicPartitions }) => {
        if (!useDescribeTopicPartitions) throw createErrorFromCode(LEADER_NOT_AVAILABLE)
        return leaderless
      }),
    }

    await expect(partitionLeader(admin, 't')).resolves.toBe(-1)
    expect(admin.fetchTopicMetadata).toHaveBeenCalledWith({
      topics: ['t'],
      useDescribeTopicPartitions: true,
    })
  })

  describe('why not a plain fetchTopicMetadata', () => {
    beforeEach(() => jest.useFakeTimers())
    afterEach(() => jest.useRealTimers())

    it('waits for the leader and then rejects with the timeout', async () => {
      const cluster = {
        allowAutoTopicCreation: false,
        metadata: jest.fn(async () => {
          throw createErrorFromCode(LEADER_NOT_AVAILABLE)
        }),
      }
      const admin = createAdmin({
        logger: newLogger(),
        retry: { retries: 0, initialRetryTime: 1, maxRetryTime: 1 },
        cluster,
      })
      const settled = admin.fetchTopicMetadata({ topics: ['t'] }).then(
        () => null,
        (error) => error
      )

      await jest.advanceTimersByTimeAsync(10100)

      expect(await settled).toMatchObject({ name: 'KafkaJSTimeout', message: 'Timeout' })
      expect(cluster.metadata.mock.calls.length).toBeGreaterThan(1)
    })
  })
})

describe('Consumer > Fetch routing oracle > the sibling window', () => {
  const during = [{ topic: 't', partition: 0, value: 'during-0' }]
  const calm = { joins: 0, rebalances: 0, crashes: 0 }

  it('counts a record handled right after the leader was seen gone, before the stop returned', () => {
    const handled = [{ ...during[0], at: 234028 }]
    const window = { leaderlessAt: 234000, tRestored: 258252 }

    expect(siblingProblems({ outage: during, handled, window, events: calm })).toEqual([])
  })

  it('fails a record handled before the leader was gone', () => {
    const handled = [{ ...during[0], at: 233900 }]
    const window = { leaderlessAt: 234000, tRestored: 258252 }

    expect(siblingProblems({ outage: during, handled, window, events: calm })).toEqual([
      't|0|during-0 was not handled while the sibling had no leader',
    ])
  })

  it('fails a record handled only after the broker was back, or never', () => {
    const window = { leaderlessAt: 234000, tRestored: 258252 }
    const late = [{ ...during[0], at: 258253 }]

    expect(siblingProblems({ outage: during, handled: late, window, events: calm })).toHaveLength(1)
    expect(siblingProblems({ outage: during, handled: [], window, events: calm })).toHaveLength(1)
  })

  it('fails a join, a rebalance or a crash in the window', () => {
    const handled = [{ ...during[0], at: 234028 }]
    const window = { leaderlessAt: 234000, tRestored: 258252 }

    expect(
      siblingProblems({ outage: during, handled, window, events: { ...calm, crashes: 1 } })
    ).toEqual(['1 crashes in the stable window'])
  })
})

describe('Consumer > Fetch routing wire measurement', () => {
  const { RoutingWire, MAX_TRACE_ROWS } = require('./fixtures/routingWire')
  const Encoder = require('../../protocol/encoder')
  const createRequest = require('../../protocol/request')
  const { requests } = require('../../protocol/requests')
  const { fetchResponse, TOPIC } = require('./fixtures/fetchHarness')
  let wire

  beforeEach(() => {
    jest.useFakeTimers()
    wire = new RoutingWire()
  })

  afterEach(() => jest.useRealTimers())

  /**
   * Writes a real v12 Fetch asking for partition 0 of one topic, at the current transcript clock.
   * @param {number} connection socket identity, also the node id the transcript maps it to
   * @param {number} correlationId request identity on that socket
   * @param {string[]} topics the topics asked for
   * @returns {Promise<void>}
   */
  const fetchOf = async (connection, correlationId, topics) => {
    const { request } = requests.Fetch.protocol({ version: 12 })({
      maxWaitTime: MOVE_WAIT_MS,
      minBytes: 1,
      maxBytes: 1024,
      topics: topics.map((topic) => ({
        topic,
        partitions: [{ partition: 0, fetchOffset: '0', maxBytes: 1024 }],
      })),
      topicIds: new Map(),
      rackId: '',
    })
    const { buffer } = await createRequest({ correlationId, clientId: 'routing-wire', request })
    wire.nodes.set(`broker:${connection}`, connection)
    wire.write(connection, `broker:${connection}`, buffer)
  }

  /**
   * Reads a framed v12 answer that gives partition 0 of the fake harness topic an error code.
   * @param {number} connection socket identity
   * @param {number} correlationId request identity
   * @param {number} errorCode
   * @returns {void}
   */
  const answerWith = (connection, correlationId, errorCode) => {
    const body = fetchResponse([{ partition: 0, errorCode }], { apiVersion: 12 })
    wire.read(
      connection,
      new Encoder()
        .writeInt32(5 + body.length)
        .writeInt32(correlationId)
        .writeInt8(0)
        .writeBuffer(body).buffer
    )
  }

  it('traces the Fetches of each node in a window with what they asked for and what came back', async () => {
    const targets = [
      { label: 'moved', topic: TOPIC },
      { label: 'other', topic: 'other-topic' },
    ]
    await fetchOf(1, 1, [TOPIC])
    jest.advanceTimersByTime(100)
    answerWith(1, 1, 6)
    await fetchOf(2, 1, ['other-topic'])
    jest.advanceTimersByTime(900)
    await fetchOf(1, 2, [TOPIC, 'other-topic'])

    expect(await wire.fetchTrace({ from: 50, to: 1000, targets })).toEqual({
      rows: [
        { node: 2, at: 100, answeredAt: null, asked: ['other'], errors: {} },
        { node: 1, at: 1000, answeredAt: null, asked: ['moved', 'other'], errors: {} },
      ],
      dropped: 0,
    })
    const [first] = (await wire.fetchTrace({ from: 0, to: 0, targets })).rows
    expect(first).toEqual({
      node: 1,
      at: 0,
      answeredAt: 100,
      asked: ['moved'],
      errors: { moved: 6 },
    })
  })

  it('keeps a bounded number of trace rows and counts the rest', async () => {
    for (let n = 1; n <= MAX_TRACE_ROWS + 3; n++) await fetchOf(1, n, [TOPIC])

    const { rows, dropped } = await wire.fetchTrace({ from: 0, to: 0, targets: [] })

    expect(rows).toHaveLength(MAX_TRACE_ROWS)
    expect(dropped).toBe(3)
  })

  it('selects the first Metadata route when answers arrive out of request order', async () => {
    for (const correlationId of [1, 2]) {
      const { request } = requests.Metadata.protocol({ version: 0 })({ topics: [] })
      const { buffer } = await createRequest({ correlationId, clientId: 'routing-wire', request })
      wire.write(1, 'broker:1', buffer)
    }
    for (const [correlationId, nodeId] of [
      [2, 2],
      [1, 1],
    ]) {
      jest.advanceTimersByTime(100)
      const body = new Encoder()
        .writeInt32(1)
        .writeInt32(nodeId)
        .writeString('broker')
        .writeInt32(9092)
        .writeArray([
          new Encoder()
            .writeInt16(0)
            .writeString('moved-topic')
            .writeArray([
              new Encoder()
                .writeInt16(0)
                .writeInt32(0)
                .writeInt32(2)
                .writeArray([2], 'int32')
                .writeArray([2], 'int32'),
            ]),
        ])
      wire.read(
        1,
        new Encoder()
          .writeInt32(4 + body.size())
          .writeInt32(correlationId)
          .writeEncoder(body).buffer
      )
    }

    const METADATA = require('../../protocol/requests/apiKeys').Metadata
    const errorAt = 50
    const topic = 'moved-topic'
    const newNode = 2
    expect((await wire.answers(METADATA)).map(({ at }) => at)).toEqual([200, 100])
    const known = (await wire.answers(METADATA))
      .sort((a, b) => a.at - b.at)
      .find(
        ({ at, decoded }) =>
          at >= errorAt &&
          decoded.topicMetadata
            .find((t) => t.topic === topic)
            ?.partitionMetadata.some((p) => p.partitionId === 0 && p.leader === newNode)
      )
    expect(known.at).toBe(100)
    expect(known.decoded.brokers[0].nodeId).toBe(2)
  })
})
