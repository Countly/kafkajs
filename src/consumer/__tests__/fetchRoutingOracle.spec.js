const createAdmin = require('../../admin')
const { createErrorFromCode } = require('../../protocol/error')
const { newLogger } = require('testHelpers')
const {
  MOVE_WAIT_MS,
  ROUND_ALLOWANCE_MS,
  UNHINTED_OUTAGE,
  moveProblems,
  partitionLeader,
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
  routeAt: 10000,
  pollDoneAt: 10500,
  nextAt: 10600,
}
const idle = { ...busy, kind: 'idle', priorFetches: 0, pollWriteAt: null, pollDoneAt: null }

describe('Consumer > Fetch routing oracle (offline controls)', () => {
  it('passes a long poll that is outstanding at the route', () => {
    expect(moveProblems(busy)).toEqual([])
  })

  it('fails a long poll that completed before the move', () => {
    const done = { ...busy, routeAt: 10000, pollDoneAt: 9900, nextAt: 10100 }
    expect(moveProblems(done)).toContain('the long poll was not outstanding at the route')
  })

  it('fails a poll answered at the route instant', () => {
    expect(moveProblems({ ...busy, pollDoneAt: 10000, nextAt: 10100 })).not.toEqual([])
  })

  it('fails a poll written after the route', () => {
    expect(moveProblems({ ...busy, pollWriteAt: 10001 })).not.toEqual([])
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
