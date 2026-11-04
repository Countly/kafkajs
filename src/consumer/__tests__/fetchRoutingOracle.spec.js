const createAdmin = require('../../admin')
const { createErrorFromCode } = require('../../protocol/error')
const { newLogger } = require('testHelpers')
const {
  MOVE_WAIT_MS,
  ROUND_ALLOWANCE_MS,
  UNHINTED_OUTAGE,
  moveProblems,
  partitionLeader,
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
  pollWriteAt: 9000,
  errorAt: 10000,
  routeAt: 10000,
  pollDoneAt: 10500,
  nextAt: 10600,
}
const idle = { ...busy, kind: 'idle', priorFetches: 0, pollWriteAt: null, pollDoneAt: null }

describe('Consumer > Fetch routing oracle (offline controls)', () => {
  it('passes a long poll that is outstanding at the loss and the route', () => {
    expect(moveProblems(busy)).toEqual([])
  })

  it('fails a long poll that completed before the move', () => {
    const done = { ...busy, errorAt: 10000, routeAt: 10000, pollDoneAt: 9900, nextAt: 10100 }
    expect(moveProblems(done)).toContain('the long poll was not outstanding at the route')
  })

  it('fails a poll answered at the route instant', () => {
    expect(moveProblems({ ...busy, pollDoneAt: 10000, nextAt: 10100 })).not.toEqual([])
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
      ).toContain('the long poll was not outstanding at the route')
    })

    it('fails a poll written after the loss', () => {
      expect(moveProblems({ ...unhinted, errorAt: 8900 })).toContain(
        'the long poll was not outstanding at the route'
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
    const none = { ...busy, priorFetches: 0, pollWriteAt: null, pollDoneAt: null, nextAt: 10100 }
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

describe('Consumer > Fetch routing oracle > a move that changes the node set', () => {
  // CI measured the route and destination starts, but no rebalance log.
  // This replay assumes a shared rebalance-log time promptly before the first recreated Fetch.
  const unhintedIdle = {
    kind: 'idle',
    hinted: false,
    newNode: 1,
    errorAt: 59227.729882,
    routeAt: 68995.814434,
    nextAt: 74051.248941,
    priorFetches: 0,
    pollWriteAt: null,
    pollDoneAt: null,
    hint: null,
    anyHint: false,
    refreshed: true,
    nodeSetChanged: true,
    drainEndAt: 74046.1,
  }
  const unhintedBusy = {
    ...unhintedIdle,
    kind: 'busy',
    newNode: 2,
    nextAt: 74048.183506,
    priorFetches: 4,
    pollWriteAt: 58982.758603,
    pollDoneAt: 63985.03353,
  }

  it('passes the CI route/start timestamps assuming a prompt shared drain', () => {
    expect(moveProblems(unhintedIdle)).toEqual([])
    expect(moveProblems(unhintedBusy)).toEqual([])
  })

  it('fails a start a full round after the drain end', () => {
    const slow = (move) => ({ ...move, nextAt: move.drainEndAt + ROUND_ALLOWANCE_MS + 1 })

    expect(moveProblems(slow(unhintedIdle))).toContain(
      'the fetchers started more than a round after the drain'
    )
    expect(moveProblems(slow(unhintedBusy))).toContain(
      'the fetchers started more than a round after the drain'
    )
  })

  it('fails a drain end that was not measured', () => {
    expect(moveProblems({ ...unhintedIdle, drainEndAt: null })).toContain(
      'no drain end was measured for the node-set change'
    )
  })

  it('fails a start later than the wait and a round after the route', () => {
    const late = {
      ...unhintedIdle,
      routeAt: unhintedIdle.nextAt - MOVE_WAIT_MS - 100 - ROUND_ALLOWANCE_MS,
    }

    expect(moveProblems(late)).toContain('the idle route took longer than the wait and a round')
    expect(moveProblems({ ...unhintedBusy, routeAt: late.routeAt })).toContain(
      'the busy route took longer than the wait and a round'
    )
  })

  it('still fails a busy move whose long poll was not outstanding at the loss', () => {
    expect(moveProblems({ ...unhintedBusy, errorAt: 64000 })).toContain(
      'the long poll was not outstanding at the route'
    )
  })

  it('keeps the strict bounds when the node set is unchanged', () => {
    const kept = { ...unhintedIdle, nodeSetChanged: false }

    expect(moveProblems(kept)).toContain('the idle route took longer than a round')
    expect(moveProblems({ ...unhintedBusy, nodeSetChanged: false })).toContain(
      'the route waited after the long poll'
    )
  })

  it('keeps the strict bounds for the hinted arm of the first CI run', () => {
    const hintedIdle = {
      kind: 'idle',
      hinted: true,
      newNode: 1,
      errorAt: 47393.632323,
      routeAt: 47393.632323,
      nextAt: 47400.097163,
      priorFetches: 0,
      pollWriteAt: null,
      pollDoneAt: null,
      hint: { leaderId: 1, leaderEpoch: 1 },
      anyHint: true,
      refreshed: false,
      nodeSetChanged: false,
      drainEndAt: 47399.9,
    }
    const hintedBusy = {
      ...hintedIdle,
      kind: 'busy',
      newNode: 2,
      nextAt: 52401.254665,
      priorFetches: 3,
      pollWriteAt: 47391.856825,
      pollDoneAt: 52394.258539,
      hint: { leaderId: 2, leaderEpoch: 1 },
    }

    expect(moveProblems(hintedIdle)).toEqual([])
    expect(moveProblems(hintedBusy)).toEqual([])
    expect(moveProblems({ ...hintedIdle, nextAt: hintedIdle.routeAt + 5055 })).not.toEqual([])
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
  const { RoutingWire } = require('./fixtures/routingWire')
  const { FetcherRecreations } = require('./fixtures/fetcherRecreations')
  const Encoder = require('../../protocol/encoder')
  const createRequest = require('../../protocol/request')
  const { requests } = require('../../protocol/requests')
  const { fetchResponse } = require('./fixtures/fetchHarness')
  let wire

  beforeEach(() => {
    jest.useFakeTimers()
    wire = new RoutingWire()
  })

  afterEach(() => jest.useRealTimers())

  /**
   * Writes a real Fetch frame at the current transcript clock, without opening a socket.
   * @param {number} connection socket identity
   * @param {number} correlationId request identity on that socket
   * @returns {Promise<void>}
   */
  const fetch = async (connection, correlationId) => {
    const { request } = requests.Fetch.protocol({ version: 18 })({
      maxWaitTime: MOVE_WAIT_MS,
      minBytes: 1,
      maxBytes: 1024,
      topics: [],
      topicIds: new Map(),
      rackId: '',
    })
    const { buffer } = await createRequest({ correlationId, clientId: 'routing-wire', request })
    wire.write(connection, `broker:${connection}`, buffer)
  }

  /**
   * Reads a framed Fetch answer at the current clock.
   * @param {number} connection socket identity
   * @param {number} correlationId request identity
   * @returns {void}
   */
  const answer = (connection, correlationId) => {
    const body = fetchResponse([], { apiVersion: 18 })
    wire.read(
      connection,
      new Encoder()
        .writeInt32(5 + body.length)
        .writeInt32(correlationId)
        .writeInt8(0)
        .writeBuffer(body).buffer
    )
  }

  it('passes an old fetcher writing after the route with an explicit rebalance boundary', async () => {
    const observation = new FetcherRecreations()
    const log = observation.logCreator(5)
    await fetch(1, 1)
    await fetch(2, 1)
    jest.advanceTimersByTime(105)
    answer(1, 1)
    jest.advanceTimersByTime(5)
    await fetch(1, 2)
    jest.advanceTimersByTime(2890)
    answer(1, 2)
    answer(2, 1)
    log({ namespace: 'FetchManager', log: { message: 'Rebalancing fetchers...' } })
    jest.advanceTimersByTime(10)
    await fetch(1, 3)
    await fetch(2, 2)

    const moves = [1, 2].map((connection) => ({
      ...idle,
      errorAt: 100,
      routeAt: 100,
      nextAt: wire.exchanges.filter((x) => x.connection === connection).at(-1).at,
    }))
    const boundary = observation.boundary(moves)
    expect(boundary).toEqual({ nodeSetChanged: true, drainEndAt: 3000 })
    for (const move of moves) expect(moveProblems({ ...move, ...boundary })).toEqual([])
  })

  it('rejects a late destination using the shared rebalance log', async () => {
    const observation = new FetcherRecreations()
    await fetch(1, 1)
    await fetch(2, 1)
    jest.advanceTimersByTime(200)
    answer(1, 1)
    answer(2, 1)
    observation.logCreator(5)({
      namespace: 'FetchManager',
      log: { message: 'Rebalancing fetchers...' },
    })
    jest.advanceTimersByTime(10)
    await fetch(1, 2)
    jest.advanceTimersByTime(2790)
    answer(1, 2)
    jest.advanceTimersByTime(100)
    await fetch(2, 2)

    const moves = [210, 3100].map((nextAt) => ({ ...idle, routeAt: 100, errorAt: 100, nextAt }))
    const boundary = observation.boundary(moves)
    expect(boundary).toEqual({ nodeSetChanged: true, drainEndAt: 200 })
    for (const move of moves) {
      expect(moveProblems({ ...move, ...boundary })).toEqual(
        move.nextAt === 210 ? [] : ['the fetchers started more than a round after the drain']
      )
    }
  })

  it('keeps strict bounds when no rebalance log was observed', () => {
    const moves = [{ ...idle, errorAt: 100, routeAt: 100, nextAt: 3010 }]
    const boundary = new FetcherRecreations().boundary(moves)
    expect(boundary).toEqual({ nodeSetChanged: false, drainEndAt: null })
    expect(moveProblems({ ...moves[0], ...boundary })).toEqual([
      'the idle route took longer than a round',
    ])
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
