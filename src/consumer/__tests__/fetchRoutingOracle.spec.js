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
