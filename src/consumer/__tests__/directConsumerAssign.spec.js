const { Kafka, logLevel } = require('../../../index')
const { createScriptedCluster, groupScript } = require('../../../testHelpers/scriptedCluster')
const Cluster = require('../../cluster')
const createRetry = require('../../retry')
const ResetObligations = require('../offsetManager/resetObligations')
const { CLUSTER_TOPIC_RECREATED } = require('../../cluster/instrumentationEvents')
const { TOPIC, TOPIC_ID } = require('./fixtures/fetchHarness')

const NODE = { id: 0, host: 'node-0', port: 9092 }
const OTHER = 'other-topic'
const OTHER_ID = '11121314-1516-1718-191a-1b1c1d1e1f20'
const BUDGET_MS = 10000

/**
 * @param {object} lock a `Lock`, whose state sits behind a private symbol
 * @returns {boolean} whether it is held
 */
const isLocked = (lock) =>
  lock[
    Object.getOwnPropertySymbols(lock).find(
      (symbol) => symbol.description === 'private:Lock:locked'
    )
  ]

/**
 * A scripted cluster that lists `TOPIC` (three partitions) and `OTHER` (two), so a selection can
 * move from one topic to the other.
 * @returns {{ cluster: object, kafka: Kafka }}
 */
const scripted = () => {
  const cluster = groupScript(createScriptedCluster({ nodes: [NODE] }), { partitions: 3 }).answer(
    'Metadata',
    () => ({
      brokers: [{ nodeId: NODE.id, host: NODE.host, port: NODE.port, rack: null }],
      controllerId: NODE.id,
      topics: [
        [TOPIC, TOPIC_ID, 3],
        [OTHER, OTHER_ID, 2],
      ].map(([name, topicId, count]) => ({
        errorCode: 0,
        name,
        topicId,
        isInternal: false,
        topicAuthorizedOperations: -2147483648,
        partitions: Array.from({ length: count }, (_, partitionIndex) => ({
          errorCode: 0,
          partitionIndex,
          leaderId: NODE.id,
          leaderEpoch: 5,
          replicas: [NODE.id],
          isr: [NODE.id],
          offlineReplicas: [],
        })),
      })),
    })
  )
  const kafka = new Kafka(
    cluster.kafkaOptions({
      logLevel: logLevel.NOTHING,
      retry: { retries: 1, initialRetryTime: 10, maxRetryTime: 20 },
    })
  )
  return { cluster, kafka }
}

/** @param {() => boolean} predicate @param {string} label @returns {Promise<void>} resolves when it holds */
const until = async (predicate, label) => {
  const deadline = Date.now() + BUDGET_MS
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`Did not happen in time: ${label}`)
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
}

describe('Manual assignment replaces its state in one step', () => {
  let wire
  let stage
  const consumers = []

  /** @returns {object} a consumer the suite disconnects afterwards */
  const track = (consumer) => {
    consumers.push(consumer)
    return consumer
  }

  /** @returns {object} the Cluster the consumer under test built, found through its first staged read */
  const clusterOf = () => stage.mock.contexts[0]

  beforeEach(() => {
    wire = scripted()
    stage = jest.spyOn(Cluster.prototype, 'stageMetadata')
  })

  afterEach(async () => {
    for (const consumer of consumers.splice(0)) await consumer.disconnect().catch(() => {})
    jest.restoreAllMocks()
  })

  test('replaces the cluster targets and drops the old selection from the metadata cache', async () => {
    const consumer = track(wire.kafka.consumer({ groupId: 'swap-group' }))
    await consumer.connect()
    await consumer.assign([{ topic: TOPIC, partitions: [0] }])
    const cluster = clusterOf()
    expect([...cluster.targetTopics]).toEqual([TOPIC])
    await cluster.refreshMetadata()
    expect(cluster.metadataCache.topics.has(TOPIC)).toBe(true)

    await consumer.assign([{ topic: OTHER, partitions: [1] }])

    expect([...cluster.targetTopics]).toEqual([OTHER])
    expect(cluster.metadataCache.topics.has(TOPIC)).toBe(false)
    expect(cluster.brokerPool.metadata.topicMetadata.map(({ topic }) => topic)).not.toContain(TOPIC)
  })

  test('validates without installing: a rejected candidate leaves targets, cache and selection', async () => {
    const consumer = track(wire.kafka.consumer({ groupId: 'reject-group' }))
    await consumer.connect()
    await consumer.assign([{ topic: TOPIC, partitions: [0] }])
    const cluster = clusterOf()
    const before = cluster.brokerPool.metadata

    await expect(consumer.assign([{ topic: OTHER, partitions: [0, 5] }])).rejects.toThrow(
      'has no partition 5'
    )

    expect([...cluster.targetTopics]).toEqual([TOPIC])
    expect(cluster.brokerPool.metadata).toBe(before)
    expect(cluster.metadataCache.topics.has(OTHER)).toBe(false)
    await consumer.run({ autoCommit: false, eachMessage: async () => {} })
    expect(consumer.assignment()).toEqual([{ topic: TOPIC, partitions: [0] }])
  })

  test('a candidate overtaken while its metadata is read never publishes', async () => {
    const consumer = track(wire.kafka.consumer({ groupId: 'race-group' }))
    await consumer.connect()
    const gate = wire.cluster.schedule.hold({ api: 'Metadata' })
    const slow = consumer.assign([{ topic: TOPIC, partitions: [0] }])
    const slowOutcome = slow.then(
      () => 'published',
      (error) => error.message
    )
    await gate.arrived()
    await consumer.assign([{ topic: OTHER, partitions: [1] }])
    gate.release()

    expect(await slowOutcome).toContain('superseded')
    expect([...clusterOf().targetTopics]).toEqual([OTHER])
    await consumer.run({ autoCommit: false, eachMessage: async () => {} })
    expect(consumer.assignment()).toEqual([{ topic: OTHER, partitions: [1] }])
  })

  test('a candidate that waits for the target lock is refused when a run or an unsubscribe wins', async () => {
    const consumer = track(wire.kafka.consumer({ groupId: 'lock-group' }))
    await consumer.connect()
    await consumer.assign([{ topic: TOPIC, partitions: [0] }])
    const cluster = clusterOf()
    await cluster.mutatingTargetTopics.acquire()
    const waiting = consumer.assign([{ topic: OTHER, partitions: [1] }]).then(
      () => 'published',
      (error) => error.message
    )
    await until(
      () => stage.mock.calls.length === 2 && isLocked(cluster.mutatingTargetTopics),
      'the stage'
    )
    await new Promise((resolve) => setTimeout(resolve, 50))
    await consumer.run({ autoCommit: false, eachMessage: async () => {} })
    await cluster.mutatingTargetTopics.release()

    expect(await waiting).toContain('running')
    expect([...cluster.targetTopics]).toEqual([TOPIC])
    expect(consumer.assignment()).toEqual([{ topic: TOPIC, partitions: [0] }])
  })

  test('the targets and the policy change together under the lock, with no await between them', async () => {
    const consumer = track(wire.kafka.consumer({ groupId: 'atomic-group' }))
    await consumer.connect()
    await consumer.assign([{ topic: TOPIC, partitions: [0] }], { offsetReset: 'earliest' })
    const cluster = clusterOf()
    const observed = []
    const original = cluster.resetTargetTopics.bind(cluster)
    jest.spyOn(cluster, 'resetTargetTopics').mockImplementation((names, options) =>
      original(names, {
        ...options,
        publish: () => {
          observed.push({
            locked: isLocked(cluster.mutatingTargetTopics),
            before: [...cluster.targetTopics],
          })
          options.publish()
          observed.push({ after: [...cluster.targetTopics] })
        },
      })
    )

    await consumer.assign([{ topic: OTHER, partitions: [1] }], { offsetReset: 'latest' })

    expect(observed).toEqual([{ locked: true, before: [OTHER] }, { after: [OTHER] }])
  })

  test('keeps the recreation debt of a topic that stays selected and drops the rest', async () => {
    const owe = jest.spyOn(ResetObligations.prototype, 'owe')
    const retain = jest.spyOn(ResetObligations.prototype, 'retain')
    const consumer = track(wire.kafka.consumer({ groupId: 'debt-group' }))
    await consumer.connect()
    await consumer.assign([
      { topic: TOPIC, partitions: [0] },
      { topic: OTHER, partitions: [0] },
    ])
    const cluster = clusterOf()
    for (const [topic, topicId] of [
      [TOPIC, TOPIC_ID],
      [OTHER, OTHER_ID],
    ]) {
      cluster.instrumentationEmitter.emit(CLUSTER_TOPIC_RECREATED, { topic, topicId })
    }
    const [obligations] = owe.mock.contexts
    expect(obligations.size()).toBe(2)

    await consumer.assign([{ topic: TOPIC, partitions: [1] }])

    expect(obligations.size()).toBe(1)
    expect(obligations.isOwed(TOPIC, 0)).toBe(true)
    expect(obligations.isOwed(OTHER, 0)).toBe(false)
    expect(retain).toHaveBeenLastCalledWith(new Set([TOPIC]))
    await expect(consumer.assign([{ topic: TOPIC, partitions: [9] }])).rejects.toThrow()
    expect(obligations.isOwed(TOPIC, 0)).toBe(true)
    expect(retain).toHaveBeenCalledTimes(2)
  })

  test('an unsubscribe clears a pattern subscription, its scope, targets and debts', async () => {
    const retain = jest.spyOn(ResetObligations.prototype, 'retain')
    const useScope = jest.spyOn(Cluster.prototype, 'useConsumerMetadataScope')
    const consumer = track(wire.kafka.consumer({ groupId: 'pattern-group' }))
    await consumer.connect()
    await consumer.subscribe({ topics: [/^other-/] })
    const cluster = useScope.mock.contexts[0]
    expect(consumer.subscription()).toEqual([OTHER])
    expect(cluster.brokerPool.consumerMetadataScope).not.toBeNull()
    await expect(consumer.assign([{ topic: TOPIC, partitions: [0] }])).rejects.toThrow(
      'unsubscribe'
    )

    await consumer.unsubscribe()

    expect(consumer.subscription()).toEqual([])
    expect(cluster.brokerPool.consumerMetadataScope).toBeNull()
    expect([...cluster.targetTopics]).toEqual([])
    expect(retain).toHaveBeenLastCalledWith(new Set())
    await consumer.assign([{ topic: TOPIC, partitions: [0] }])
    expect(consumer.subscription()).toEqual([])
    expect([...cluster.targetTopics]).toEqual([TOPIC])
  })

  describe.each([
    ['matches a topic', /^other-/, [OTHER]],
    ['matches no topic', /^nomatch-/, []],
  ])('a client pattern that %s refuses assign', (_title, pattern, matched) => {
    const refuseAndKeepState = async ({ consumer, obligations, cluster }) => {
      const targets = [...cluster.targetTopics]
      const scope = cluster.brokerPool.consumerMetadataScope
      const retain = jest.spyOn(ResetObligations.prototype, 'retain')
      const owed = obligations?.size()

      await expect(consumer.assign([{ topic: TOPIC, partitions: [0] }])).rejects.toThrow(
        'unsubscribe'
      )

      expect(retain).not.toHaveBeenCalled()
      expect(obligations?.size()).toBe(owed)
      expect(consumer.subscription()).toEqual(matched)
      expect([...cluster.targetTopics]).toEqual(targets)
      expect(cluster.brokerPool.consumerMetadataScope).toBe(scope)
      expect(scope).not.toBeNull()
    }

    /** @returns {Promise<{consumer: object, obligations: object, cluster: Cluster}>} a connected pattern subscriber; `obligations` is undefined when it owes nothing */
    const subscribed = async () => {
      const owe = jest.spyOn(ResetObligations.prototype, 'owe')
      const useScope = jest.spyOn(Cluster.prototype, 'useConsumerMetadataScope')
      const consumer = track(wire.kafka.consumer({ groupId: 'pattern-mode-group' }))
      await consumer.connect()
      await consumer.subscribe({ topics: [pattern] })
      const cluster = useScope.mock.contexts[0]
      if (matched.length > 0) {
        cluster.instrumentationEmitter.emit(CLUSTER_TOPIC_RECREATED, {
          topic: OTHER,
          topicId: OTHER_ID,
        })
      }
      return { consumer, obligations: owe.mock.contexts[0], cluster }
    }

    test('before run, then unsubscribe lets assign through', async () => {
      const state = await subscribed()
      const { consumer, obligations, cluster } = state
      await refuseAndKeepState(state)
      expect(obligations?.isOwed(OTHER, 0) ?? false).toBe(matched.length > 0)

      await consumer.unsubscribe()
      await consumer.assign([{ topic: TOPIC, partitions: [0] }])

      expect([...cluster.targetTopics]).toEqual([TOPIC])
      await consumer.run({ autoCommit: false, eachMessage: async () => {} })
      expect(consumer.assignment()).toEqual([{ topic: TOPIC, partitions: [0] }])
    })

    test('after stop, then unsubscribe lets assign through', async () => {
      const state = await subscribed()
      const { consumer, cluster } = state
      await consumer.run({ autoCommit: false, eachMessage: async () => {} })
      await consumer.stop()
      await refuseAndKeepState(state)

      await consumer.unsubscribe()
      await consumer.assign([{ topic: TOPIC, partitions: [0] }])

      expect([...cluster.targetTopics]).toEqual([TOPIC])
    })
  })

  test('a client pattern shows the names it matches and a literal beside it stays listed', async () => {
    const consumer = track(wire.kafka.consumer({ groupId: 'listed-group' }))
    await consumer.connect()
    await consumer.subscribe({ topics: [/^other-/, TOPIC] })
    expect(consumer.subscription()).toEqual([OTHER, TOPIC])
  })
})

describe('A pending manual assignment does not outlive a forced close', () => {
  let wire
  let stage
  let held
  const consumers = []

  /** @returns {object} a consumer the suite disconnects afterwards */
  const track = (consumer) => {
    consumers.push(consumer)
    return consumer
  }

  /** @returns {Promise<string|Error>} the outcome of an assignment, as a value */
  const outcomeOf = (pending) =>
    pending.then(
      () => 'published',
      (error) => error
    )

  beforeEach(() => {
    wire = scripted()
    stage = jest.spyOn(Cluster.prototype, 'stageMetadata')
  })

  afterEach(async () => {
    held?.release()
    held = null
    for (const consumer of consumers.splice(0)) await consumer.disconnect().catch(() => {})
    jest.restoreAllMocks()
    wire.cluster.assertClean()
  })

  test('a Metadata read held on the wire neither reconnects nor publishes after the close', async () => {
    const consumer = track(wire.kafka.consumer({ groupId: 'close-wire-group' }))
    await consumer.connect()
    await consumer.assign([{ topic: TOPIC, partitions: [0] }])
    const cluster = stage.mock.contexts[0]
    held = wire.cluster.schedule.hold({ api: 'Metadata' })
    const old = outcomeOf(consumer.assign([{ topic: OTHER, partitions: [1] }]))
    await held.arrived()
    await consumer.disconnect({ timeout: 0 })
    const frames = wire.cluster.requests().length
    held.release()
    held = null

    expect(await old).toMatchObject({ type: 'CONSUMER_STOPPING' })
    await new Promise((resolve) => setTimeout(resolve, 100))
    expect(wire.cluster.requests().slice(frames)).toEqual([])
    expect([...cluster.targetTopics]).toEqual([TOPIC])
    expect(cluster.isConnected()).toBe(false)
  })

  test('a candidate waiting for the target lock never publishes after the close', async () => {
    const consumer = track(wire.kafka.consumer({ groupId: 'close-lock-group' }))
    await consumer.connect()
    await consumer.assign([{ topic: TOPIC, partitions: [0] }])
    const cluster = stage.mock.contexts[0]
    const reset = jest.spyOn(cluster, 'resetTargetTopics')
    await cluster.mutatingTargetTopics.acquire()
    const old = outcomeOf(consumer.assign([{ topic: OTHER, partitions: [1] }]))
    await until(() => reset.mock.calls.length === 1, 'the publication')
    await consumer.disconnect({ timeout: 0 })
    await cluster.mutatingTargetTopics.release()

    expect(await old).toMatchObject({ type: 'CONSUMER_STOPPING' })
    await new Promise((resolve) => setTimeout(resolve, 50))
    expect([...cluster.targetTopics]).toEqual([TOPIC])
  })

  test('a candidate waiting for the target lock never publishes after a close that was not forced', async () => {
    const consumer = track(wire.kafka.consumer({ groupId: 'plain-close-group' }))
    await consumer.connect()
    await consumer.assign([{ topic: TOPIC, partitions: [0] }])
    const cluster = stage.mock.contexts[0]
    const reset = jest.spyOn(cluster, 'resetTargetTopics')
    await cluster.mutatingTargetTopics.acquire()
    const old = outcomeOf(consumer.assign([{ topic: OTHER, partitions: [1] }]))
    await until(() => reset.mock.calls.length === 1, 'the publication')
    await consumer.disconnect()
    await cluster.mutatingTargetTopics.release()

    expect(await old).toMatchObject({ type: 'CONSUMER_STOPPING' })
    expect([...cluster.targetTopics]).toEqual([TOPIC])
  })

  test('assign has no retry.maxTotalTime deadline of its own', async () => {
    const kafka = new Kafka(
      wire.cluster.kafkaOptions({
        logLevel: logLevel.NOTHING,
        retry: { retries: 1, initialRetryTime: 10, maxRetryTime: 20, maxTotalTime: 100 },
      })
    )
    const consumer = track(kafka.consumer({ groupId: 'no-deadline-group' }))
    await consumer.connect()
    await consumer.assign([{ topic: TOPIC, partitions: [0] }])
    const cluster = stage.mock.contexts[0]
    const reset = jest.spyOn(cluster, 'resetTargetTopics')
    await cluster.mutatingTargetTopics.acquire()
    const pending = outcomeOf(consumer.assign([{ topic: OTHER, partitions: [1] }]))
    await until(() => reset.mock.calls.length === 1, 'the publication')
    await new Promise((resolve) => setTimeout(resolve, 400))
    await cluster.mutatingTargetTopics.release()

    expect(await pending).toBe('published')
    expect([...cluster.targetTopics]).toEqual([OTHER])
  })

  test.each([
    ['a Metadata read held on the wire', 'wire'],
    ['a candidate waiting for the target lock', 'lock'],
  ])('%s stays refused when a replacement lifetime assigns meanwhile', async (_label, where) => {
    const consumer = track(wire.kafka.consumer({ groupId: `replacement-${where}-group` }))
    await consumer.connect()
    await consumer.assign([{ topic: TOPIC, partitions: [0] }])
    const cluster = stage.mock.contexts[0]
    const reset = jest.spyOn(cluster, 'resetTargetTopics')
    if (where === 'wire') held = wire.cluster.schedule.hold({ api: 'Metadata' })
    else await cluster.mutatingTargetTopics.acquire()
    const old = outcomeOf(consumer.assign([{ topic: OTHER, partitions: [1] }]))
    if (where === 'wire') await held.arrived()
    else await until(() => reset.mock.calls.length === 1, 'the publication')
    await consumer.disconnect({ timeout: 0 })

    await consumer.connect()
    if (where === 'lock') await cluster.mutatingTargetTopics.release()
    await consumer.assign([{ topic: TOPIC, partitions: [2] }])
    held?.release()
    held = null

    expect(await old).toMatchObject({ type: 'CONSUMER_STOPPING' })
    await new Promise((resolve) => setTimeout(resolve, 50))
    expect([...cluster.targetTopics]).toEqual([TOPIC])
    await consumer.run({ autoCommit: false, eachMessage: async () => {} })
    expect(consumer.assignment()).toEqual([{ topic: TOPIC, partitions: [2] }])
  })

  test('an assignment made while a close is running waits for it and then publishes', async () => {
    const consumer = track(wire.kafka.consumer({ groupId: 'during-close-group' }))
    await consumer.connect()
    await consumer.assign([{ topic: TOPIC, partitions: [0] }])
    const cluster = stage.mock.contexts[0]
    const frames = wire.cluster.requests().length
    let closed = false
    const closing = consumer.disconnect({ timeout: 0 }).then(() => {
      closed = true
    })
    let wrote = null
    const assigning = consumer.assign([{ topic: OTHER, partitions: [1] }])
    stage.mockImplementationOnce(function (...args) {
      wrote = closed
      return Cluster.prototype.stageMetadata.apply(this, args)
    })
    await Promise.all([closing, assigning])

    expect(wrote).toBe(true)
    expect(wire.cluster.requests().length).toBeGreaterThan(frames)
    expect([...cluster.targetTopics]).toEqual([OTHER])
  })

  test('an assignment made after a completed close starts a lifetime of its own and publishes', async () => {
    const consumer = track(wire.kafka.consumer({ groupId: 'after-close-group' }))
    await consumer.connect()
    await consumer.assign([{ topic: TOPIC, partitions: [0] }])
    await consumer.disconnect({ timeout: 0 })

    await consumer.assign([{ topic: OTHER, partitions: [1] }])

    expect([...stage.mock.contexts[0].targetTopics]).toEqual([OTHER])
  })
})

describe('A pending subscription does not outlive a forced close', () => {
  let wire
  let consumer
  let cluster

  beforeEach(async () => {
    wire = scripted()
    const add = jest.spyOn(Cluster.prototype, 'addMultipleTargetTopics')
    consumer = wire.kafka.consumer({ groupId: 'close-subscribe-group' })
    await consumer.connect()
    await consumer.subscribe({ topics: [TOPIC] })
    cluster = add.mock.contexts[0]
  })

  afterEach(async () => {
    await consumer.disconnect({ timeout: 0 }).catch(() => {})
    jest.restoreAllMocks()
    wire.cluster.assertClean()
  })

  /**
   * Starts a subscription that waits for the target lock, then force-closes the consumer.
   * @param {object} subscription the `subscribe` argument
   * @param {() => Promise<void>} [between] runs after the close, before the lock is released
   * @returns {Promise<{ outcome: Error|string, frames: number }>} what the call settled with and the
   *   frames written after the close
   */
  const subscribeAcrossClose = async (subscription, between = async () => {}) => {
    await cluster.mutatingTargetTopics.acquire()
    const pending = consumer.subscribe(subscription).then(
      () => 'resolved',
      (error) => error
    )
    await new Promise((resolve) => setTimeout(resolve, 100))
    await consumer.disconnect({ timeout: 0 })
    await between()
    const frames = wire.cluster.requests().length
    await cluster.mutatingTargetTopics.release()
    const outcome = await pending
    await new Promise((resolve) => setTimeout(resolve, 100))
    return { outcome, frames: wire.cluster.requests().length - frames }
  }

  test('a name subscription waiting for the target lock neither reconnects nor adds its topics', async () => {
    const { outcome, frames } = await subscribeAcrossClose({ topics: [OTHER] })

    expect(outcome).toMatchObject({ type: 'CONSUMER_STOPPING' })
    expect(frames).toBe(0)
    expect([...cluster.targetTopics]).toEqual([TOPIC])
    expect(cluster.isConnected()).toBe(false)
  })

  test('a pattern subscription waiting for the target lock does not replace the targets', async () => {
    const { outcome, frames } = await subscribeAcrossClose({ topics: [/^other/] })

    expect(outcome).toMatchObject({ type: 'CONSUMER_STOPPING' })
    expect(frames).toBe(0)
    expect([...cluster.targetTopics]).toEqual([TOPIC])
    expect(cluster.isConnected()).toBe(false)
  })

  test('a name subscription of the closed lifetime stays refused beside a replacement lifetime', async () => {
    const { outcome } = await subscribeAcrossClose({ topics: [OTHER] }, () => consumer.connect())

    expect(outcome).toMatchObject({ type: 'CONSUMER_STOPPING' })
    expect([...cluster.targetTopics]).toEqual([TOPIC])
  })

  test('a subscription made after a completed close starts a lifetime of its own and applies', async () => {
    await consumer.disconnect({ timeout: 0 })

    await consumer.subscribe({ topics: [OTHER] })

    expect([...cluster.targetTopics].sort()).toEqual([OTHER, TOPIC].sort())
  })
})

describe('Cluster#resetTargetTopics with a publication', () => {
  /** @returns {Cluster} a cluster that never connects */
  const bare = () => {
    const { kafka } = scripted()
    const consumer = kafka.consumer({ groupId: 'unit-group' })
    return { consumer, kafka }
  }

  test('runs the publication after the replacement, under the lock, and reports it ran', async () => {
    const { consumer } = bare()
    const spy = jest.spyOn(Cluster.prototype, 'resetTargetTopics')
    await consumer.connect()
    await consumer.unsubscribe()
    const [cluster] = spy.mock.contexts
    const seen = []

    const ran = await cluster.resetTargetTopics(['a'], {
      publish: () => seen.push([isLocked(cluster.mutatingTargetTopics), [...cluster.targetTopics]]),
    })

    expect(ran).toBe(true)
    expect(seen).toEqual([[true, ['a']]])
    expect(isLocked(cluster.mutatingTargetTopics)).toBe(false)
    await consumer.disconnect()
    jest.restoreAllMocks()
  })

  test('skips the publication and reports it when the caller is no longer current', async () => {
    const { consumer } = bare()
    const spy = jest.spyOn(Cluster.prototype, 'resetTargetTopics')
    await consumer.connect()
    await consumer.unsubscribe()
    const [cluster] = spy.mock.contexts
    const publish = jest.fn()
    let current = true
    await cluster.mutatingTargetTopics.acquire()
    const pending = cluster.resetTargetTopics(['a'], { isCurrent: () => current, publish })
    current = false
    await cluster.mutatingTargetTopics.release()

    expect(await pending).toBe(false)
    expect(publish).not.toHaveBeenCalled()
    expect([...cluster.targetTopics]).toEqual([])
    await consumer.disconnect()
    jest.restoreAllMocks()
  })
})

describe('Cluster#stageMetadata with a refusing guard', () => {
  /**
   * @param {object} broker the one broker the cluster can reach
   * @returns {{ stage: (guard: () => Error|null) => Promise<object>, attempts: () => number }}
   *   the staged read, and how many times the broker was asked to connect
   */
  const stagedOver = (broker) => {
    let attempts = 0
    const cluster = Object.create(Cluster.prototype)
    cluster.retrier = createRetry({ retries: 0 })
    cluster.brokerPool = {
      brokers: {},
      seedBroker: {
        ...broker,
        connect: (...args) => {
          attempts++
          return broker.connect(...args)
        },
      },
    }
    return {
      stage: (guard) => cluster.stageMetadata({ topics: [], guard }),
      attempts: () => attempts,
    }
  }

  test('a refusal that arrives during an attempt replaces the failure and ends the retries', async () => {
    const refusal = new Error('refused')
    let refused = false
    const { stage, attempts } = stagedOver({
      isConnected: () => false,
      connect: async () => {
        refused = true
        throw new Error('Closed connection')
      },
    })

    await expect(stage(() => (refused ? refusal : null))).rejects.toBe(refusal)
    expect(attempts()).toBe(1)
  })

  test('the broker connection is guarded by the caller', async () => {
    const seen = []
    const { stage } = stagedOver({
      isConnected: () => false,
      connect: async ({ guard }) => {
        seen.push(guard())
        return Promise.reject(new Error('stop'))
      },
    })

    await expect(stage(() => null)).rejects.toThrow('stop')
    expect(seen[0]).toBeNull()
  })
})

describe('A pattern subscription waiting to connect does not outlive a forced close', () => {
  test('pattern broker acquisition refuses connect resumed after close', async () => {
    const Broker = require('../../broker')
    const wire = scripted()
    const consumer = wire.kafka.consumer({ groupId: 'pattern-connect-group' })
    let release
    const gate = new Promise((resolve) => {
      release = resolve
    })
    let arrived = false
    const original = Broker.prototype.connect
    jest.spyOn(Broker.prototype, 'connect').mockImplementationOnce(async function (...args) {
      arrived = true
      await gate
      return original.apply(this, args)
    })
    try {
      const pending = consumer.subscribe({ topics: [/./] }).then(
        () => 'published',
        (error) => error
      )
      await until(() => arrived, 'broker connect gate')
      await consumer.disconnect({ timeout: 0 })
      const count = wire.cluster.requests().length
      release()
      const outcome = await pending

      expect(wire.cluster.requests().slice(count)).toEqual([])
      expect(outcome).toMatchObject({ type: 'CONSUMER_STOPPING' })
    } finally {
      release()
      await consumer.disconnect({ timeout: 0 })
      jest.restoreAllMocks()
      wire.cluster.assertClean()
    }
  })
})
