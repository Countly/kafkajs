const { Kafka, PartitionAssigners } = require('../../../index')
const Runner = require('../runner')
const { createScriptedCluster, groupScript } = require('../../../testHelpers/scriptedCluster')
const { prepareRecordBatches, TOPIC } = require('./fixtures/fetchHarness')
const { waitForCondition } = require('./fixtures/liveScenario')

const ILLEGAL_GENERATION = 22
const REBALANCE_IN_PROGRESS = 27
const NODES = [{ id: 0, host: 'node0', port: 9092 }]

/** Polls until `predicate` holds, and fails naming `what` when it never does. */
const until = async (predicate, what) => {
  if (!(await waitForCondition(predicate, { attempts: 400, delay: 5 }))) {
    throw new Error(`${what} did not happen`)
  }
}

/**
 * A cooperative consumer whose handler commits once released and lets the commit's error escape, as
 * a handler that does not catch it does. The commit is the only request this scenario answers by
 * hand: the test decides when, and with what, the coordinator answers it.
 */
describe('Consumer > an escaping commit error from a generation the member has left', () => {
  let consumer
  let release

  beforeAll(() => prepareRecordBatches())

  afterEach(async () => {
    release?.()
    await consumer?.disconnect()
    jest.restoreAllMocks()
  })

  /**
   * Holds a commit sent while a rejoin is pending, answers the rejoin with generation 2 and then
   * the commit with ILLEGAL_GENERATION.
   * @param {{ rejoin: boolean, escape?: boolean }} options `rejoin` false leaves the member at its
   *   generation: no rejoin runs; `escape` false has the handler catch the commit's error
   * @returns {Promise<{ lost: number, joins: number, deliveries: number, fetchOffsets: string[] }>}
   *   the lost-membership rounds the runner resolved, the JoinGroup requests sent, the handler
   *   calls, and the offset each Fetch asked for
   */
  const answerIllegalGeneration = async ({ rejoin, escape = true }) => {
    const cluster = groupScript(createScriptedCluster({ nodes: NODES }), { batches: 2 })
    const resolved = jest.spyOn(Runner.prototype, 'resolveRebalanceVerdict')
    consumer = new Kafka(cluster.kafkaOptions({ requestTimeout: 20000 })).consumer({
      groupId: 'escaping-commit',
      autoCommit: false,
      heartbeatInterval: 100,
      maxWaitTimeInMs: 20,
      partitionAssigners: [PartitionAssigners.cooperativeSticky],
    })
    const requests = (api) => cluster.requests().filter((request) => request.api === api)
    let commit
    let deliveries = 0
    const handlerMayCommit = new Promise((resolve) => {
      release = resolve
    })
    await consumer.connect()
    await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
    await consumer.run({
      eachMessage: async () => {
        deliveries += 1
        await handlerMayCommit
        commit = consumer.commitOffsets([{ topic: TOPIC, partition: 0, offset: '1' }])
        commit.catch(() => {})
        await (escape ? commit : commit.catch(() => {}))
      },
    })
    await until(() => requests('Fetch').length > 0, 'the first fetch')

    const commitGate = cluster.schedule.hold({ api: 'OffsetCommit' })
    if (rejoin) {
      const join = cluster.schedule.hold({ api: 'JoinGroup' })
      cluster.schedule.once({ api: 'Heartbeat' }, { errorCode: REBALANCE_IN_PROGRESS })
      const { fields } = await join.arrived()
      release()
      await commitGate.arrived()
      join.release({ ...cluster.answerers.get('JoinGroup')(fields), generationId: 2 })
    } else {
      release()
      await commitGate.arrived()
    }
    commitGate.release({
      topics: [{ topic: TOPIC, partitions: [{ partition: 0, errorCode: ILLEGAL_GENERATION }] }],
    })
    await commit.catch(() => {})
    await new Promise((resolve) => setTimeout(resolve, 300))

    return {
      lost: resolved.mock.calls.filter(([verdict]) => verdict.kind === 'lost').length,
      joins: requests('JoinGroup').length,
      deliveries,
      fetchOffsets: requests('Fetch').map(
        ({ fields }) => fields.topics[0].partitions[0].fetchOffset
      ),
    }
  }

  it('does not lose the membership it already replaced by joining again', async () => {
    expect(await answerIllegalGeneration({ rejoin: true })).toMatchObject({ lost: 0, joins: 2 })
  }, 20000)

  it('still loses the membership when the generation is the one the member holds (control)', async () => {
    const { lost } = await answerIllegalGeneration({ rejoin: false })

    expect(lost).toBe(1)
  }, 20000)

  describe('a handler that fails on the commit error', () => {
    it('is retried from the record it failed on, in the same membership', async () => {
      const { lost, deliveries, fetchOffsets } = await answerIllegalGeneration({ rejoin: true })

      expect({ lost, deliveries }).toEqual({ lost: 0, deliveries: 2 })
      expect(fetchOffsets.slice(0, 3)).toEqual(['0', '0', '1'])
    }, 20000)

    it('is not retried when the handler caught the error (control)', async () => {
      const { lost, deliveries, fetchOffsets } = await answerIllegalGeneration({
        rejoin: true,
        escape: false,
      })

      expect({ lost, deliveries }).toEqual({ lost: 0, deliveries: 1 })
      expect(fetchOffsets.slice(0, 2)).toEqual(['0', '1'])
    }, 20000)
  })
})
