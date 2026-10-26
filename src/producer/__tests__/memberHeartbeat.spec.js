const { Kafka } = require('../../../index')
const { RecordBatch } = require('../../protocol/recordBatch/v0')
const RecordEncoder = require('../../protocol/recordBatch/record/v0')
const { createScriptedCluster, groupScript } = require('../../../testHelpers/scriptedCluster')
const { prepareRecordBatches, TOPIC } = require('../../consumer/__tests__/fixtures/fetchHarness')
const { waitForCondition } = require('../../consumer/__tests__/fixtures/liveScenario')
const sleep = require('../../utils/sleep')
const { heartbeatInBatch } = require('./fixtures/memberHeartbeat')

const REBALANCE_IN_PROGRESS = 27
const NODES = [{ id: 0, host: 'node0', port: 9092 }]
const MEMBER_ID = 'member-1'
const PARTITIONS = 3
const RECORDS = 60
const CHUNK = 3
const CHUNKS = RECORDS / CHUNK
const PACE_MS = 40
const HEARTBEAT_MS = 50

/** Polls until `predicate` holds, failing with `what` when it never does. */
const until = async (predicate, what) => {
  if (!(await waitForCondition(predicate, { attempts: 800, delay: 5 }))) {
    throw new Error(`${what} did not happen`)
  }
}

/**
 * One classic member alone in its group, handed a batch of `RECORDS` records for each of
 * `PARTITIONS` partitions in one fetch, whose handler works through each in paced chunks, as a
 * transactional copier does. After the first chunk the coordinator starts a rebalance: it answers
 * every heartbeat with REBALANCE_IN_PROGRESS until the member joins again.
 * @param {boolean} beat whether the handler heartbeats after each chunk, as the exactly-once copier
 *   does (`heartbeatInBatch`)
 * @returns {Promise<{ rejoinedAtChunk: number, ended: Array<string|null>, flags: object }>}
 *   `rejoinedAtChunk` is how many chunks the handlers had done in all when the second JoinGroup was
 *   written; `ended` how each handler call ended, the type of its error or null when it ran to the
 *   end; `flags` what `isRunning` and `isStale` said while the rebalance was pending
 */
const rebalanceUnderHandler = async (beat) => {
  const cluster = groupScript(createScriptedCluster({ nodes: NODES }), { partitions: PARTITIONS })
  let rebalancing = false
  let joins = 0
  let chunks = 0
  let fetches = 0
  let active = 0
  const state = { rejoinedAtChunk: null, ended: [], flags: { running: true, stale: false } }
  const records = Array.from({ length: RECORDS }, (_, offsetDelta) =>
    RecordEncoder({ offsetDelta, key: Buffer.from('key'), value: Buffer.from(`${offsetDelta}`) })
  )
  const batch = Buffer.from(
    (
      await RecordBatch({
        firstTimestamp: 1700000000000,
        maxTimestamp: 1700000000000,
        lastOffsetDelta: RECORDS - 1,
        records,
      })
    ).buffer
  )
  cluster
    .answer('JoinGroup', ({ protocolType, groupProtocols: [protocol] }) => {
      joins += 1
      if (joins === 2) {
        state.rejoinedAtChunk = chunks
        rebalancing = false
      }
      return {
        protocolType,
        memberId: MEMBER_ID,
        generationId: joins,
        leaderId: MEMBER_ID,
        groupProtocol: protocol.name,
        members: [
          { memberId: MEMBER_ID, groupInstanceId: null, memberMetadata: protocol.metadata },
        ],
      }
    })
    .answer('Heartbeat', () => (rebalancing ? { errorCode: REBALANCE_IN_PROGRESS } : {}))
    .answer('Fetch', async ({ maxWaitMs, topics }) => {
      fetches += 1
      if (fetches > 1) await sleep(maxWaitMs)
      return {
        partitions: topics[0].partitions.map(({ partition }) => ({
          partition,
          errorCode: 0,
          records: fetches === 1 ? batch : false,
        })),
      }
    })

  const consumer = new Kafka(cluster.kafkaOptions()).consumer({
    groupId: 'handler-rebalance',
    autoCommit: false,
    heartbeatInterval: HEARTBEAT_MS,
    maxWaitTimeInMs: 20,
  })
  try {
    await consumer.connect()
    await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
    await consumer.run({
      eachBatchAutoResolve: false,
      eachBatch: async ({ batch: fetched, resolveOffset, heartbeat, isRunning, isStale }) => {
        active += 1
        try {
          for (let at = 0; at < fetched.messages.length; at += CHUNK) {
            await sleep(PACE_MS)
            resolveOffset(fetched.messages[at + CHUNK - 1].offset)
            chunks += 1
            if (chunks === 1) rebalancing = true
            if (rebalancing) {
              state.flags.running &&= isRunning()
              state.flags.stale ||= isStale()
            }
            if (beat) await heartbeatInBatch(heartbeat)
          }
          state.ended.push(null)
        } catch (error) {
          state.ended.push(error.type)
          throw error
        } finally {
          active -= 1
        }
      },
    })
    await until(
      () => joins >= 2 && active === 0,
      'the member joining again and its handlers ending'
    )
    return state
  } finally {
    await consumer.disconnect()
  }
}

describe('Producer > a rebalance under the paced eachBatch handlers of a copier', () => {
  beforeAll(() => prepareRecordBatches())

  it('waits for every fetched batch of handlers that never heartbeat, which see no sign of it in isRunning or isStale', async () => {
    const outcome = await rebalanceUnderHandler(false)

    expect(outcome.rejoinedAtChunk).toBe(PARTITIONS * CHUNKS)
    expect(outcome.ended).toEqual(Array(PARTITIONS).fill(null))
    expect(outcome.flags).toEqual({ running: true, stale: false })
  }, 20000)

  it('rejoins within a few chunks for handlers that heartbeat between them, which end with the rebalance error', async () => {
    const outcome = await rebalanceUnderHandler(true)

    expect(outcome.rejoinedAtChunk).toBeLessThan(CHUNKS)
    expect(outcome.ended.length).toBeGreaterThan(0)
    expect(new Set(outcome.ended)).toEqual(new Set(['REBALANCE_IN_PROGRESS']))
  }, 20000)
})
