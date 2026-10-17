const { Kafka } = require('../../../index')
const { KafkaJSCommitFailedError } = require('../../errors')
const RebalanceCoordinator = require('../rebalanceCoordinator')
const { createScriptedCluster, groupScript } = require('../../../testHelpers/scriptedCluster')
const { prepareRecordBatches, TOPIC } = require('./fixtures/fetchHarness')
const { waitForCondition } = require('./fixtures/liveScenario')

const UNKNOWN_MEMBER_ID = 25
const NODES = [
  { id: 0, host: 'node0', port: 9092 },
  { id: 1, host: 'node1', port: 9092 },
]

/** Lets the answer just written reach the client and what it wakes run. */
const flush = async () => {
  for (let turn = 0; turn < 10; turn++) {
    await new Promise(setImmediate)
  }
}

/** Polls until `predicate` holds, and fails naming `what` when it never does. */
const until = async (predicate, what) => {
  if (!(await waitForCondition(predicate, { attempts: 400, delay: 5 }))) {
    throw new Error(`${what} did not happen`)
  }
}

/** Waits for a promise, failing with `what` after `ms` instead of leaving the test to time out. */
const within = (promise, ms, what) => {
  let timer
  const timeout = new Promise((resolve, reject) => {
    timer = setTimeout(() => reject(new Error(`${what} did not happen within ${ms} ms`)), ms)
  })
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer))
}

/**
 * Wraps a gate so that `release` is safe at any moment: the held call goes once, as soon as it is
 * there, however often and however early `release` is called.
 * @param {{ arrived: () => Promise<object>, release: () => void }} gate
 * @returns {{ arrived: () => Promise<object>, release: () => void }}
 */
const releasableOnce = (gate) => {
  let arrived = false
  let wanted = false
  let released = false
  const go = () => {
    if (arrived && wanted && !released) {
      released = true
      gate.release()
    }
  }
  gate.arrived().then(() => {
    arrived = true
    go()
  })

  return {
    arrived: () => gate.arrived(),
    release: () => {
      wanted = true
      go()
    },
  }
}

/**
 * A member that is lost may not commit: the coordinator has handed its partitions on, so an
 * OffsetCommit it sends could write over the new owner's progress. These cases run the real
 * consumer over the scripted cluster and hold the request a commit waits on.
 *
 * The invariant case is marked `it.failing` until the membership fence prevents the late commit;
 * setup and settlement are checked by ordinary hooks and a guard case.
 */
describe('Consumer > commits under a lost membership', () => {
  let consumer
  let rejoin
  let scenario
  let beforeRelease
  let outcome
  /** What teardown lets go of, oldest first; each is safe in any state the scenario reached. */
  const drains = []

  /**
   * A commit that waits for the coordinator lookup while the coordinator tells the member it is
   * lost. Resolves once both are true: the lookup is held and the member has been told. Whatever
   * it creates is registered in `drains` at once, so a setup that fails midway still lets go.
   * @returns {Promise<{ cluster: object, lookup: object, commitOf: () => Promise<void> }>}
   *   `commitOf()` is the pending commit, which settles once the lookup is released
   */
  const commitWaitingForLookupOfLostMember = async () => {
    const cluster = groupScript(createScriptedCluster({ nodes: NODES }), { coordinator: 1 })
    drains.push(() => cluster.up(1))
    // Gives the hook bounds margin; unrelated commit errors must still reject the hook.
    consumer = new Kafka(cluster.kafkaOptions({ requestTimeout: 20000 })).consumer({
      groupId: 'lost-group',
      autoCommit: false,
      heartbeatInterval: 100,
      maxWaitTimeInMs: 20,
    })
    const inHandler = jest.fn()
    let finishHandler
    const handlerMayFinish = new Promise((resolve) => {
      finishHandler = resolve
    })
    drains.push(finishHandler)
    let commit
    await consumer.connect()
    await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
    await consumer.run({
      eachMessage: async () => {
        inHandler()
        await handlerMayFinish
        // The coordinator's connection resets and the node stays down, so nothing reconnects it
        // before the commit starts and the commit has to look the coordinator up again.
        cluster.down(1)
        cluster
          .sockets()
          .filter(({ endpoint }) => endpoint.host === 'node1')
          .forEach((socket) => socket.emit('error', new Error('connection reset')))
        await flush()
        commit = consumer.commitOffsets([{ topic: TOPIC, partition: 0, offset: '1' }])
        await commit.catch(() => {})
      },
    })
    await until(() => inHandler.mock.calls.length > 0, 'the handler starting')

    const lookup = releasableOnce(cluster.schedule.hold({ api: 'FindCoordinator' }))
    drains.push(lookup.release)
    finishHandler()
    await within(lookup.arrived(), 5000, 'the held coordinator lookup')
    cluster.up(1)

    const heartbeats = () => cluster.requests().filter(({ api }) => api === 'Heartbeat').length
    const before = heartbeats()
    cluster.schedule.once({ api: 'Heartbeat' }, { errorCode: UNKNOWN_MEMBER_ID })
    await until(() => heartbeats() > before, 'the heartbeat that tells the member it was lost')
    await until(
      () =>
        rejoin.mock.calls.some(
          ([verdict]) => verdict.kind === 'lost' && verdict.error?.code === UNKNOWN_MEMBER_ID
        ),
      'the client receiving the lost-membership verdict'
    )

    return { cluster, lookup, commitOf: () => commit }
  }

  beforeAll(() => prepareRecordBatches())

  beforeEach(async () => {
    rejoin = jest.spyOn(RebalanceCoordinator.prototype, 'requestRejoin')
    scenario = await within(commitWaitingForLookupOfLostMember(), 10000, 'the lost-member scenario')
    const { cluster, lookup, commitOf } = scenario
    beforeRelease = {
      commits: cluster.requests().filter(({ api }) => api === 'OffsetCommit'),
      lookups: cluster.requests().filter(({ api }) => api === 'FindCoordinator').length,
      lost: rejoin.mock.calls.some(
        ([verdict]) => verdict.kind === 'lost' && verdict.error?.code === UNKNOWN_MEMBER_ID
      ),
    }
    expect(beforeRelease).toEqual({ commits: [], lookups: 2, lost: true })
    lookup.release()
    outcome = await within(
      commitOf().then(
        () => null,
        (error) => {
          if (!(error instanceof KafkaJSCommitFailedError)) {
            throw error
          }
          return error
        }
      ),
      5000,
      'the released commit settling'
    )
    cluster.assertClean()
  })

  afterEach(async () => {
    try {
      drains.splice(0).forEach((drain) => drain())
      await within(consumer?.disconnect(), 5000, 'consumer disconnect')
    } finally {
      rejoin?.mockRestore()
      consumer = rejoin = scenario = beforeRelease = outcome = undefined
    }
  })

  it('reaches the held lookup and the loss before it releases the lookup', () => {
    expect(beforeRelease).toEqual({ commits: [], lookups: 2, lost: true })
  })

  it.failing(
    'sends no OffsetCommit for a commit that was waiting for the coordinator lookup when the member was lost',
    () => {
      expect(scenario.cluster.requests().filter(({ api }) => api === 'OffsetCommit')).toEqual([])
      expect(outcome).toBeInstanceOf(KafkaJSCommitFailedError)
    }
  )
})
