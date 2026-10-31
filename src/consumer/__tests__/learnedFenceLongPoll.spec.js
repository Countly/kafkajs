const { performance } = require('node:perf_hooks')
const { KafkaJSCommitFailedError } = require('../../errors')
const { ClassicFaultEpisode, batchAt } = require('./fixtures/classicFaultEpisode')
const { TOPIC } = require('./fixtures/fetchHarness')

const EPISODE_TIMEOUT_MS = 25000
const WAIT_MS = 12000

/** @param {number} ms @returns {Promise<void>} resolves after the real delay */
const pause = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

/**
 * @param {() => boolean} predicate
 * @param {string} label what is awaited, for the failure
 * @returns {Promise<void>} when the predicate holds
 * @throws {Error} naming `label` when it does not hold within the budget
 */
const until = async (predicate, label) => {
  const deadline = Date.now() + WAIT_MS
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`Did not happen in time: ${label}`)
    await pause(5)
  }
}

/** A healthy classic episode whose heartbeat runs every 50 ms. */
const row = {
  api: 'Heartbeat',
  fault: 'none',
  step: 'healthy',
  expectedState: 'healthy',
  expectedWire: { afterLossOffsetCommits: 0, duplicateAssignments: 0 },
  maxRounds: 0,
  config: { groupProtocol: 'classic', retry: { retries: 6, initialRetryTime: 100, factor: 0 } },
}

describe.each([22, 25])(
  'Consumer > a heartbeat that learns code %i behind a held long poll',
  (code) => {
    it(
      'hands no handler or commit to the old membership and rejoins once the poll settles',
      async () => {
        const episode = new ClassicFaultEpisode(row)
        try {
          await episode.open()
          await episode.execute()
          const { schedule } = episode.cluster
          const poll = schedule.hold({ api: 'Fetch' })
          const held = await poll.arrived()
          let learnedAt = null
          schedule.once({ api: 'Heartbeat' }, () => {
            learnedAt = performance.now()
            return { errorCode: code }
          })
          await until(() => learnedAt !== null, 'the heartbeat to learn the loss')
          await pause(200)

          const outcome = await episode.consumer
            .commitOffsets([{ topic: TOPIC, partition: 0, offset: '3' }])
            .then(
              () => null,
              (error) => error
            )
          const [{ partition, fetchOffset }] = held.fields.topics[0].partitions
          poll.release({
            partitions: [
              {
                partition,
                errorCode: 0,
                records: await batchAt(partition, Number(fetchOffset)),
                abortedTransactions: [],
              },
            ],
          })
          await until(() => episode.joins.length >= 2, 'the rejoin to settle')
          const rejoinedAt = episode.joins[1].at
          await pause(200)

          expect(outcome === null || outcome instanceof KafkaJSCommitFailedError).toBe(true)
          const [, replacementSync] = episode.frames('SyncGroup')
          for (const frame of episode.frames('OffsetCommit'))
            expect(frame.at).toBeGreaterThanOrEqual(replacementSync.at)
          const late = episode.deliveries.filter(({ at }) => at > learnedAt)
          expect(late.length).toBeGreaterThan(0)
          expect(late.every(({ at }) => at >= rejoinedAt)).toBe(true)
          expect(episode.deliveries.filter(({ id }) => id.endsWith(`-${fetchOffset}`))).toEqual([])
          expect(episode.overlaps).toBe(0)
        } finally {
          await episode.close()
        }
      },
      EPISODE_TIMEOUT_MS
    )
  }
)
