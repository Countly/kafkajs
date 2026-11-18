const Broker = require('../../broker')
const RequestQueue = require('../../network/requestQueue')
const ConsumerGroup = require('../consumerGroup')
const { FetchPreparation } = require('../fetchPreparation')
const handlerContext = require('../handlerContext')
const { KafkaJSCommitFailedError } = require('../../errors')
const { TOPIC, CloseRig, batchAt, gate } = require('./fixtures/closeRig')

const BEFORE = ['value-2', 'value-3', 'value-4']

/**
 * A consumer whose first batch of partition 0 (records 2..4) is still being handled when the test
 * seeks the partition. Every later delivery of partition 0 runs `current` and then waits until
 * disposal, so the partition's position stays where the earlier handlers left it. Partition 1, when
 * there is one, runs `other` on its first delivery and resolves what it was given.
 */
class SeekScenario {
  /**
   * @param {object} options
   * @param {'classic'|'consumer'} options.protocol
   * @param {object} options.run run options; a truthy `eachBatch` selects the batch handler
   * @param {(payload: object, scenario: SeekScenario) => unknown} [options.before] runs in the old
   *   handler before its gate
   * @param {(payload: object) => Promise<unknown>} [options.old] runs in the old handler once its
   *   gate opens; for `eachMessage` the payload is the message payload
   * @param {(payload: object) => Promise<unknown>} [options.current] runs in the next delivery's
   *   handler before it waits
   * @param {(payload: object) => Promise<unknown>} [options.other] runs in partition 1's first handler
   * @param {boolean} [options.handOff=false] the old handler returns at once, keeping its payload
   * @param {number} [options.partitions=1]
   * @returns {Promise<SeekScenario>} the old batch's handler has been entered
   */
  static async open({
    protocol,
    run,
    before = () => {},
    old = async () => {},
    current = async () => {},
    other = async () => {},
    handOff = false,
    partitions = 1,
  }) {
    const committed = { 0: '2', 1: '0' }
    const log = { '0|2': Buffer.concat([await batchAt(2), await batchAt(3), await batchAt(4)]) }
    for (const offset of [0, 1, 5, 6, 7]) log[`0|${offset}`] = await batchAt(offset)
    log['1|0'] = await batchAt(0)
    const rig = await CloseRig.open({ protocol, last: -1, log, committed, partitions })
    const scenario = new SeekScenario(rig, committed)
    const commit = rig.cluster.answerers.get('OffsetCommit')
    rig.cluster.answer('OffsetCommit', (fields, record) => {
      for (const entry of fields.topics)
        for (const cell of entry.partitions) committed[cell.partition] = String(cell.offset)
      return commit(fields, record)
    })

    const guarded = (work, payload) =>
      Promise.resolve()
        .then(() => work(payload, scenario))
        .catch((error) => scenario.errors.push(error))
    const handle = async (partition, values, payload) => {
      if (partition !== 0) {
        const first = scenario.otherSeen++ === 0
        if (first) await guarded(other, payload)
        for (const { offset } of payload.batch?.messages ?? []) payload.resolveOffset(offset)
        if (first) scenario.otherDone.resolve()
        return
      }
      scenario.deliveries.push(values)
      if (scenario.deliveries.length === 1) {
        scenario.old = payload
        await guarded(before, payload)
        scenario.entered.resolve()
        if (handOff) return
        await scenario.gate.promise
        await guarded(old, payload)
        scenario.finished.resolve()
        return
      }
      await guarded(current, payload)
      scenario.next.resolve(values)
      await scenario.hold.promise
    }
    const handlers = run.eachBatch
      ? {
          eachMessage: undefined,
          eachBatch: (payload) =>
            handle(
              payload.batch.partition,
              payload.batch.messages.map(({ value }) => value.toString()),
              payload
            ),
        }
      : {
          eachMessage: (payload) =>
            handle(payload.partition, [payload.message.value.toString()], payload),
        }
    try {
      const started = await rig.started({ run: { ...run, ...handlers }, delivered: 0 })
      scenario.consumer = started.consumer
      await scenario.entered.promise
    } catch (error) {
      await scenario.dispose()
      throw error
    }
    return scenario
  }

  /**
   * @param {CloseRig} rig
   * @param {Record<number, string>} committed the broker's committed offsets, kept current
   */
  constructor(rig, committed) {
    this.rig = rig
    this.committed = committed
    this.consumer = null
    this.old = null
    this.otherSeen = 0
    this.entered = gate()
    this.gate = gate()
    this.finished = gate()
    this.next = gate()
    this.otherDone = gate()
    this.hold = gate()
    /** @type {string[][]} */
    this.deliveries = []
    /** @type {Error[]} */
    this.errors = []
  }

  /**
   * Seeks a partition and waits until the consumer has applied the seek.
   * @param {string} offset
   * @param {number} [partition=0]
   * @returns {Promise<string>} the position the consumer reports afterwards
   */
  seek(offset, partition = 0) {
    this.consumer.seek({ topic: TOPIC, partition, offset })
    return this.position(partition)
  }

  /** @returns {Promise<void>} lets the old handler finish its work */
  async release() {
    this.gate.resolve()
    await this.finished.promise
  }

  /** @returns {Promise<string[]>} the values of the delivery after the old batch */
  nextDelivery() {
    return this.next.promise
  }

  /**
   * @param {number} [partition=0]
   * @returns {string[]} the offset of every OffsetCommit cell for the partition, in order
   */
  commits(partition = 0) {
    return this.rig
      .frames('OffsetCommit')
      .flatMap(({ fields }) => fields.topics.flatMap(({ partitions }) => partitions))
      .filter((cell) => cell.partition === partition)
      .map(({ offset }) => String(offset))
  }

  /**
   * @param {number} [partition=0]
   * @returns {Promise<string>} the consumer's next position for the partition
   */
  position(partition = 0) {
    return this.consumer.position({ topic: TOPIC, partition })
  }

  /** @returns {Promise<void>} */
  async dispose() {
    this.gate.resolve()
    this.hold.resolve()
    await this.rig.dispose()
  }
}

/**
 * Holds the next `Broker#offsetCommit` call before the broker checks its guard or writes anything.
 * @returns {{ reached: Promise<void>, release: () => void, restore: () => void }}
 */
const holdNextCommitCall = () => {
  const reached = gate()
  const opened = gate()
  const original = Broker.prototype.offsetCommit
  let held = false
  const spy = jest.spyOn(Broker.prototype, 'offsetCommit').mockImplementation(async function (
    ...args
  ) {
    if (!held) {
      held = true
      reached.resolve()
      await opened.promise
    }
    return original.apply(this, args)
  })
  return { reached: reached.promise, release: opened.resolve, restore: () => spy.mockRestore() }
}

/**
 * Holds the next OffsetCommit at the request queue's dispatch, before its final refusal check and
 * its write, so whatever the request was built from is already fixed.
 * @returns {{ reached: Promise<void>, release: () => void, restore: () => void }}
 */
const holdNextCommitWrite = () => {
  const reached = gate()
  const opened = gate()
  const original = RequestQueue.prototype.sendSocketRequest
  let held = false
  const spy = jest
    .spyOn(RequestQueue.prototype, 'sendSocketRequest')
    .mockImplementation(function (request) {
      if (held || request.entry.apiName !== 'OffsetCommit') return original.call(this, request)
      held = true
      reached.resolve()
      opened.promise.then(() => original.call(this, request))
    })
  return { reached: reached.promise, release: opened.resolve, restore: () => spy.mockRestore() }
}

/**
 * Holds the answers of an API after the broker applied the request, with a gate on the first
 * arrival.
 * @param {import('./fixtures/closeRig').CloseRig} rig
 * @param {string} api
 * @returns {{ arrived: Promise<void>, release: () => void }}
 */
const holdAnswerOnArrival = (rig, api) => {
  const held = rig.hold(api)
  const arrived = gate()
  const answer = rig.cluster.answerers.get(api)
  rig.cluster.answer(api, (fields, record) => {
    arrived.resolve()
    return answer(fields, record)
  })
  return { arrived: arrived.promise, release: held.release }
}

/**
 * @param {{topics: {partitions: {partition: number}[]}[]}} fields an OffsetCommit request
 * @returns {object} the answer that refuses every cell with REQUEST_TIMED_OUT, recording nothing
 */
const timedOut = (fields) => ({
  topics: fields.topics.map((topic) => ({
    ...topic,
    partitions: topic.partitions.map(({ partition }) => ({ partition, errorCode: 7 })),
  })),
})

/**
 * Answers the next OffsetCommit with REQUEST_TIMED_OUT for every cell, recording nothing.
 * @param {SeekScenario} scenario
 * @returns {void}
 */
const failNextCommit = (scenario) => {
  const answer = scenario.rig.cluster.answerers.get('OffsetCommit')
  let failed = false
  scenario.rig.cluster.answer('OffsetCommit', (fields, record) => {
    if (failed) return answer(fields, record)
    failed = true
    return timedOut(fields)
  })
}

/**
 * Holds the answers of the first OffsetCommits that reach the broker, one hold per kind: an
 * `applied` request is applied by the broker before its answer is held, a `failed` one is answered
 * REQUEST_TIMED_OUT and recorded nowhere. Later requests are answered as usual.
 * @param {SeekScenario} scenario
 * @param {('applied'|'failed')[]} kinds
 * @returns {{ holds: {arrived: Promise<void>, release: () => void}[], restore: () => void }}
 */
const holdCommitAnswers = (scenario, kinds) => {
  const holds = kinds.map(() => ({ arrived: gate(), opened: gate() }))
  const answer = scenario.rig.cluster.answerers.get('OffsetCommit')
  let arrivals = 0
  scenario.rig.cluster.answer('OffsetCommit', async (fields, record) => {
    const hold = holds[arrivals++]
    if (!hold) return answer(fields, record)
    const result =
      kinds[arrivals - 1] === 'failed' ? timedOut(fields) : await answer(fields, record)
    hold.arrived.resolve()
    await hold.opened.promise
    return result
  })
  return {
    holds: holds.map(({ arrived, opened }) => ({
      arrived: arrived.promise,
      release: opened.resolve,
    })),
    restore: () => holds.forEach(({ opened }) => opened.resolve()),
  }
}

/** The run options and first handlers of two partitions whose partition 0 progress is resolved. */
const TWO_PARTITIONS = {
  partitions: 2,
  run: {
    autoCommit: true,
    autoCommitInterval: 60000,
    partitionsConsumedConcurrently: 2,
    eachBatch: true,
  },
  before: ({ resolveOffset }) => resolveOffset('4'),
}

/**
 * Captures the consumer group of the scenario when it applies its first assignment.
 * @returns {{ group: () => ConsumerGroup, restore: () => void }}
 */
const captureGroup = () => {
  const apply = ConsumerGroup.prototype.applyRebalanceResult
  let captured = null
  const spy = jest
    .spyOn(ConsumerGroup.prototype, 'applyRebalanceResult')
    .mockImplementation(function (...args) {
      captured = this
      return apply.apply(this, args)
    })
  return { group: () => captured, restore: () => spy.mockRestore() }
}

/**
 * Resolves with the request entries of the next fetch built after the call.
 * @returns {{ next: () => Promise<object[]>, restore: () => void }}
 */
const watchFetchBuilds = () => {
  const build = ConsumerGroup.prototype.buildFetchRequests
  let waiter = null
  const spy = jest
    .spyOn(ConsumerGroup.prototype, 'buildFetchRequests')
    .mockImplementation(function (...args) {
      const requests = build.apply(this, args)
      waiter?.resolve(requests)
      waiter = null
      return requests
    })
  return {
    next: () => {
      waiter = gate()
      return waiter.promise
    },
    restore: () => spy.mockRestore(),
  }
}

/**
 * @param {object[]} requests fetch request entries
 * @returns {string[]} the fetch offsets asked for partition 0
 */
const partitionZeroOffsets = (requests) =>
  requests.flatMap(({ partitions }) =>
    partitions.filter(({ partition }) => partition === 0).map(({ fetchOffset }) => fetchOffset)
  )

/**
 * @param {Promise<unknown>} promise
 * @returns {Promise<Error|null>} the error the promise rejected with, or null when it resolved
 */
const failureOf = (promise) =>
  promise.then(
    () => null,
    (error) => error
  )

const MANUAL = { autoCommit: false, autoCommitThreshold: 1, eachBatchAutoResolve: false }
const PAYLOAD_5 = { topics: [{ topic: TOPIC, partitions: [{ partition: 0, offset: '5' }] }] }

describe.each(['classic', 'consumer'])('a seek, %s protocol', (protocol) => {
  /** @type {SeekScenario|undefined} */
  let scenario
  let restore = () => {}
  afterEach(async () => {
    restore()
    restore = () => {}
    await scenario?.dispose()
    scenario = undefined
  })
  const using = (handle) => {
    const previous = restore
    restore = () => {
      handle.restore()
      previous()
    }
    return handle
  }
  const open = async (options) => {
    scenario = await SeekScenario.open({ protocol, ...options })
    return scenario
  }
  const resolveAndCommit = async ({ resolveOffset, commitOffsetsIfNecessary }) => {
    resolveOffset('4')
    await commitOffsetsIfNecessary()
  }

  test('an older batch awaiting across seek() and position() neither resolves nor commits', async () => {
    await open({ run: { ...MANUAL, eachBatch: true }, old: resolveAndCommit })
    scenario.consumer.seek({ topic: TOPIC, partition: 0, offset: '0' })
    expect(scenario.old.isStale()).toBe(true)
    expect(await scenario.position()).toBe('0')
    expect(scenario.old.isStale()).toBe(false)

    await scenario.release()

    expect(await scenario.nextDelivery()).toEqual(['value-0'])
    expect(await scenario.position()).toBe('0')
    expect(scenario.commits()).toEqual([])
    expect(scenario.errors).toEqual([])
  })

  test('an older batch neither resolves nor commits while its seek is still pending', async () => {
    await open({ run: { ...MANUAL, eachBatch: true }, old: resolveAndCommit })
    scenario.consumer.seek({ topic: TOPIC, partition: 0, offset: '0' })

    await scenario.release()

    expect(await scenario.nextDelivery()).toEqual(['value-0'])
    expect(await scenario.position()).toBe('0')
    expect(scenario.commits()).toEqual([])
    expect(scenario.errors).toEqual([])
  })

  test('a seek to the offset the older batch started at still fences that batch', async () => {
    await open({ run: { ...MANUAL, eachBatch: true }, old: resolveAndCommit })
    expect(await scenario.seek('2')).toBe('2')

    await scenario.release()

    expect(await scenario.nextDelivery()).toEqual(BEFORE)
    expect(await scenario.position()).toBe('2')
    expect(scenario.commits()).toEqual([])
    expect(scenario.errors).toEqual([])
  })

  test('a forward seek refuses the explicit commit callback of an older batch', async () => {
    await open({
      run: { ...MANUAL, eachBatch: true },
      old: async ({ resolveOffset, commitOffsetsIfNecessary }) => {
        resolveOffset('4')
        await commitOffsetsIfNecessary(PAYLOAD_5)
      },
    })
    expect(await scenario.seek('6')).toBe('6')

    await scenario.release()

    expect(await scenario.nextDelivery()).toEqual(['value-6'])
    expect(await scenario.position()).toBe('6')
    expect(scenario.commits()).toEqual([])
    expect(scenario.errors).toHaveLength(1)
    expect(scenario.errors[0]).toBeInstanceOf(KafkaJSCommitFailedError)
    expect(scenario.errors[0].reason).toBe('stale-assignment')
    expect(scenario.errors[0].message).toBe('A seek superseded the handler batch')
  })

  test('callbacks an older batch handed off before returning are refused after a seek', async () => {
    await open({ run: { ...MANUAL, eachBatch: true }, handOff: true })
    await scenario.nextDelivery()
    const { resolveOffset, commitOffsetsIfNecessary } = scenario.old
    expect(await scenario.seek('0')).toBe('0')

    resolveOffset('4')
    await commitOffsetsIfNecessary()
    await expect(commitOffsetsIfNecessary(PAYLOAD_5)).rejects.toBeInstanceOf(
      KafkaJSCommitFailedError
    )

    expect(await scenario.position()).toBe('0')
    expect(scenario.commits()).toEqual([])
  })

  test('eachBatchAutoResolve does not resolve or commit an older batch after a seek', async () => {
    await open({ run: { autoCommit: true, autoCommitThreshold: 1, eachBatch: true } })
    expect(await scenario.seek('0')).toBe('0')

    await scenario.release()

    expect(await scenario.nextDelivery()).toEqual(['value-0'])
    expect(await scenario.position()).toBe('0')
    expect(scenario.commits()).toEqual(['0'])
    expect(scenario.committed[0]).toBe('0')
    expect(scenario.errors).toEqual([])
  })

  test('eachMessage stops an older batch after a seek and does not resolve or commit it', async () => {
    await open({ run: { autoCommit: true, autoCommitThreshold: 1 } })
    expect(await scenario.seek('0')).toBe('0')

    await scenario.release()

    expect(await scenario.nextDelivery()).toEqual(['value-0'])
    expect(await scenario.position()).toBe('0')
    expect(scenario.commits()).toEqual(['0'])
    expect(scenario.deliveries.flat()).not.toContain('value-3')
    expect(scenario.errors).toEqual([])
  })

  test('consumer.commitOffsets from work an older batch handler started is refused after a seek', async () => {
    await open({
      run: { ...MANUAL, eachBatch: true },
      old: () =>
        new Promise((resolve) => setImmediate(resolve)).then(() =>
          scenario.consumer.commitOffsets([{ topic: TOPIC, partition: 0, offset: '5' }])
        ),
    })
    expect(await scenario.seek('0')).toBe('0')

    await scenario.release()

    expect(await scenario.nextDelivery()).toEqual(['value-0'])
    expect(await scenario.position()).toBe('0')
    expect(scenario.commits()).toEqual([])
    expect(scenario.errors).toHaveLength(1)
    expect(scenario.errors[0]).toBeInstanceOf(KafkaJSCommitFailedError)
  })

  test('an older batch commit not yet written when the seek is admitted is never sent', async () => {
    const held = holdNextCommitCall()
    restore = held.restore
    let committing
    await open({
      run: { ...MANUAL, eachBatch: true },
      before: ({ resolveOffset, commitOffsetsIfNecessary }) => {
        resolveOffset('4')
        committing = commitOffsetsIfNecessary()
      },
      old: () => committing,
    })
    await held.reached
    expect(await scenario.seek('0')).toBe('0')

    held.release()
    await scenario.release()

    expect(await scenario.nextDelivery()).toEqual(['value-0'])
    expect(await scenario.position()).toBe('0')
    expect(scenario.commits()).toEqual([])
    expect(scenario.errors).toEqual([])
  })

  test('an older batch commit already written applies on the broker but not to the local position', async () => {
    let committing
    const answer = { hold: null }
    await open({
      run: { ...MANUAL, eachBatch: true },
      before: ({ resolveOffset, commitOffsetsIfNecessary }, opened) => {
        answer.hold = holdAnswerOnArrival(opened.rig, 'OffsetCommit')
        resolveOffset('4')
        committing = commitOffsetsIfNecessary()
      },
      old: () => committing,
    })
    await answer.hold.arrived
    expect(await scenario.seek('0')).toBe('0')

    answer.hold.release()
    await scenario.release()

    expect(scenario.errors).toEqual([])
    expect(scenario.commits()).toEqual(['5'])
    expect(scenario.committed[0]).toBe('5')
    expect(await scenario.nextDelivery()).toEqual(['value-0'])
    expect(await scenario.position()).toBe('0')
  })

  test('the final automatic commit leaves out progress a pending seek made obsolete', async () => {
    await open({
      run: { autoCommit: true, autoCommitInterval: 60000, eachBatch: true },
      before: ({ resolveOffset }) => resolveOffset('4'),
    })
    scenario.consumer.seek({ topic: TOPIC, partition: 0, offset: '0' })

    const closing = scenario.consumer.disconnect()
    await scenario.release()
    await closing

    expect(scenario.commits()).toEqual([])
    expect(scenario.committed[0]).toBe('2')
    expect(scenario.errors).toEqual([])
  })

  test('a healthy partition commits its progress without the sought partition', async () => {
    const sought = gate()
    await open({
      partitions: 2,
      run: { ...MANUAL, partitionsConsumedConcurrently: 2, eachBatch: true },
      before: ({ resolveOffset }) => resolveOffset('4'),
      other: async ({ resolveOffset, commitOffsetsIfNecessary }) => {
        await sought.promise
        resolveOffset('0')
        await commitOffsetsIfNecessary()
      },
    })
    scenario.consumer.seek({ topic: TOPIC, partition: 0, offset: '0' })
    sought.resolve()

    await scenario.release()

    expect(await scenario.nextDelivery()).toEqual(['value-0'])
    expect(scenario.commits(1)).toEqual(['1'])
    expect(scenario.commits(0)).toEqual([])
    expect(await scenario.position()).toBe('0')
    expect(scenario.errors).toEqual([])
  })

  test('a seek of another partition leaves an older batch free to resolve and commit', async () => {
    let stale, identity
    await open({
      partitions: 2,
      run: { ...MANUAL, partitionsConsumedConcurrently: 2, eachBatch: true },
      before: () => {
        identity = handlerContext.current()
      },
      old: async (payload) => {
        stale = payload.isStale()
        await resolveAndCommit(payload)
      },
    })
    scenario.consumer.seek({ topic: TOPIC, partition: 1, offset: '0' })
    expect(handlerContext.isSuperseded(identity)).toBe(false)

    await scenario.release()

    expect(stale).toBe(false)
    expect(await scenario.nextDelivery()).toEqual(['value-5'])
    expect(await scenario.position()).toBe('5')
    expect(scenario.commits()).toEqual(['5'])
    expect(scenario.errors).toEqual([])
  })

  test('the batch fetched after a seek resolves and commits as usual', async () => {
    let stale
    await open({
      run: { ...MANUAL, eachBatch: true },
      current: async ({ isStale, resolveOffset, commitOffsetsIfNecessary }) => {
        stale = isStale()
        resolveOffset('6')
        await commitOffsetsIfNecessary()
      },
    })
    expect(await scenario.seek('6')).toBe('6')

    await scenario.release()

    expect(await scenario.nextDelivery()).toEqual(['value-6'])
    expect(stale).toBe(false)
    expect(await scenario.position()).toBe('7')
    expect(scenario.commits()).toEqual(['7'])
    expect(scenario.errors).toEqual([])
  })

  test('consumer.commitOffsets outside any handler still commits after a seek', async () => {
    await open({ run: { ...MANUAL, eachBatch: true } })
    expect(await scenario.seek('0')).toBe('0')

    await scenario.consumer.commitOffsets([{ topic: TOPIC, partition: 0, offset: '5' }])

    expect(scenario.commits()).toEqual(['5'])
    expect(scenario.committed[0]).toBe('5')
  })
  test('consumer.commitOffsets from work an older batch handler left running after it returned is refused after a seek', async () => {
    const later = gate()
    let detached
    await open({
      run: { ...MANUAL, eachBatch: true },
      handOff: true,
      before: () => {
        detached = later.promise.then(() =>
          scenario.consumer.commitOffsets([{ topic: TOPIC, partition: 0, offset: '5' }])
        )
      },
    })
    expect(await scenario.nextDelivery()).toEqual(BEFORE)
    expect(await scenario.seek('0')).toBe('0')

    later.resolve()

    await expect(detached).rejects.toBeInstanceOf(KafkaJSCommitFailedError)
    expect(await scenario.position()).toBe('0')
    expect(scenario.commits()).toEqual([])
    expect(scenario.errors).toEqual([])
  })

  test('a default commit selected before a seek is written without the obsolete offset of the sought partition', async () => {
    const write = using(holdNextCommitWrite())
    const allow = gate()
    await open({
      partitions: 2,
      run: { ...MANUAL, partitionsConsumedConcurrently: 2, eachBatch: true },
      before: ({ resolveOffset }) => resolveOffset('4'),
      other: async ({ resolveOffset, commitOffsetsIfNecessary }) => {
        await allow.promise
        resolveOffset('0')
        await commitOffsetsIfNecessary()
      },
    })
    allow.resolve()
    await write.reached
    expect(await scenario.seek('0')).toBe('0')

    write.release()
    await scenario.otherDone.promise

    expect(scenario.commits(0)).toEqual(['0'])
    expect(scenario.commits(1)).toEqual(['1'])
    expect(scenario.committed).toEqual({ 0: '0', 1: '1' })
    expect(await scenario.position()).toBe('0')
    expect(scenario.errors).toEqual([])
  })

  test('a final commit selected before a pending seek is written without the sought partition', async () => {
    const write = using(holdNextCommitWrite())
    await open({
      partitions: 2,
      run: {
        autoCommit: true,
        autoCommitInterval: 60000,
        partitionsConsumedConcurrently: 2,
        eachBatch: true,
      },
      before: ({ resolveOffset }) => resolveOffset('4'),
    })
    await scenario.otherDone.promise
    const closing = scenario.consumer.disconnect()
    await scenario.release()
    await write.reached
    scenario.consumer.seek({ topic: TOPIC, partition: 0, offset: '0' })

    write.release()
    await closing

    expect(scenario.commits(0)).toEqual([])
    expect(scenario.commits(1)).toEqual(['1'])
    expect(scenario.committed).toEqual({ 0: '2', 1: '1' })
    expect(scenario.errors).toEqual([])
  })

  test('a fetch prepared while a seek is still being applied does not read from the old position', async () => {
    const builds = using(watchFetchBuilds())
    await open({
      run: {
        autoCommit: true,
        autoCommitThreshold: 1,
        eachBatchAutoResolve: false,
        eachBatch: true,
      },
    })
    const answer = holdAnswerOnArrival(scenario.rig, 'OffsetCommit')
    const seeking = scenario.seek('0')
    await answer.arrived
    const built = builds.next()
    await scenario.release()

    expect(partitionZeroOffsets(await built)).toEqual([])

    answer.release()
    expect(await seeking).toBe('0')
    expect(await scenario.nextDelivery()).toEqual(['value-0'])
    expect(await scenario.position()).toBe('0')
    expect(scenario.commits()).toEqual(['0'])
    expect(scenario.errors).toEqual([])
  })

  test('a resolve callback of an older batch stays fenced after its assignment is rebuilt', async () => {
    const captured = using(captureGroup())
    await open({ run: { ...MANUAL, eachBatch: true }, handOff: true })
    await scenario.nextDelivery()
    expect(await scenario.seek('0')).toBe('0')
    scenario.old.resolveOffset('4')
    expect(await scenario.position()).toBe('0')

    await captured.group().applyRebalanceResult({ assignment: { [TOPIC]: [0] } })
    expect(await scenario.position()).toBe('2')
    scenario.old.resolveOffset('4')

    expect(await scenario.position()).toBe('2')
    await captured.group().commitOffsets({}, { auto: true })
    expect(scenario.commits()).toEqual([])
  })

  test('a resolve callback of an older batch stays fenced after its partition is revoked and assigned again', async () => {
    const captured = using(captureGroup())
    await open({ run: { ...MANUAL, eachBatch: true }, handOff: true })
    await scenario.nextDelivery()
    expect(await scenario.seek('0')).toBe('0')
    const group = captured.group()

    await group.applyRebalanceResult({ assignment: {} }, { rebuildOffsets: false })
    await group.applyRebalanceResult({ assignment: { [TOPIC]: [0] } }, { rebuildOffsets: false })
    expect(await scenario.position()).toBe('2')
    scenario.old.resolveOffset('4')

    expect(await scenario.position()).toBe('2')
    await group.commitOffsets({}, { auto: true })
    expect(scenario.commits()).toEqual([])
  })

  test('a seek whose commit failed keeps the obsolete progress out of the final commit', async () => {
    await open({
      partitions: 2,
      run: {
        autoCommit: true,
        autoCommitInterval: 60000,
        partitionsConsumedConcurrently: 2,
        eachBatch: true,
      },
      before: ({ resolveOffset }) => resolveOffset('4'),
    })
    await scenario.otherDone.promise
    failNextCommit(scenario)
    await expect(scenario.seek('0')).rejects.toMatchObject({ type: 'REQUEST_TIMED_OUT' })

    const closing = scenario.consumer.disconnect()
    await scenario.release()
    await closing

    expect(scenario.commits(0)).toEqual(['0'])
    expect(scenario.commits(1)).toEqual(['1'])
    expect(scenario.committed).toEqual({ 0: '2', 1: '1' })
    expect(scenario.errors).toEqual([])
  })

  test('a seek whose commit failed is applied by the next resolution', async () => {
    await open({
      run: { autoCommit: true, autoCommitInterval: 60000, eachBatch: true },
      before: ({ resolveOffset }) => resolveOffset('4'),
    })
    failNextCommit(scenario)
    await expect(scenario.seek('0')).rejects.toMatchObject({ type: 'REQUEST_TIMED_OUT' })

    expect(await scenario.position()).toBe('0')
    await scenario.release()

    expect(await scenario.nextDelivery()).toEqual(['value-0'])
    expect(scenario.commits()).toEqual(['0', '0'])
    expect(scenario.committed[0]).toBe('0')
    expect(scenario.errors).toEqual([])
  })

  test('a commit with no seek admitted skips the sought-partition checks', async () => {
    const captured = using(captureGroup())
    await open({ run: { ...MANUAL, eachBatch: true }, old: resolveAndCommit })
    const group = captured.group()
    const scans = jest.spyOn(group, 'isSoughtAfter')
    const lookups = jest.spyOn(group.unappliedSeeks, 'has')
    using({ restore: () => [scans, lookups].forEach((spy) => spy.mockRestore()) })

    await scenario.release()

    expect(scenario.commits()).toEqual(['5'])
    expect(scans).not.toHaveBeenCalled()
    expect(lookups).not.toHaveBeenCalled()
    expect(scenario.errors).toEqual([])
  })

  test('a handler identity carries its acquisition, which a seek admitted after the fetch supersedes', async () => {
    let identity, currentIdentity
    await open({
      run: { ...MANUAL, eachBatch: true },
      before: () => {
        identity = handlerContext.current()
      },
      current: async () => {
        currentIdentity = handlerContext.current()
      },
    })
    expect(identity.acquisition.retired).toBe(false)
    expect(handlerContext.isSuperseded(identity)).toBe(false)

    scenario.consumer.seek({ topic: TOPIC, partition: 0, offset: '0' })
    expect(handlerContext.isSuperseded(identity)).toBe(true)
    expect(await scenario.position()).toBe('0')
    expect(handlerContext.isSuperseded(identity)).toBe(true)

    await scenario.release()
    expect(await scenario.nextDelivery()).toEqual(['value-0'])
    expect(currentIdentity.acquisition).toBe(identity.acquisition)
    expect(handlerContext.isSuperseded(currentIdentity)).toBe(false)
  })

  test('rebuilding the assignment retires the acquisition an older identity carries', async () => {
    const captured = using(captureGroup())
    let identity
    await open({
      run: { ...MANUAL, eachBatch: true },
      before: () => {
        identity = handlerContext.current()
      },
    })
    const group = captured.group()

    await group.applyRebalanceResult({ assignment: { [TOPIC]: [0] } })

    expect(identity.acquisition.retired).toBe(true)
    expect(handlerContext.isSuperseded(identity)).toBe(true)
    const replacement = group.acquired.get(`${TOPIC}|0`)
    expect(replacement).not.toBe(identity.acquisition)
    expect(replacement.retired).toBe(false)
  })

  test('revoking a partition retires its acquisition, and assigning it again does not revive it', async () => {
    const captured = using(captureGroup())
    let identity
    await open({
      run: { ...MANUAL, eachBatch: true },
      before: () => {
        identity = handlerContext.current()
      },
    })
    const group = captured.group()

    await group.applyRebalanceResult({ assignment: {} }, { rebuildOffsets: false })
    expect(identity.acquisition.retired).toBe(true)
    await group.applyRebalanceResult({ assignment: { [TOPIC]: [0] } }, { rebuildOffsets: false })

    expect(identity.acquisition.retired).toBe(true)
    expect(handlerContext.isSuperseded(identity)).toBe(true)
    expect(group.acquired.get(`${TOPIC}|0`).retired).toBe(false)
  })

  test('stopping the run retires the acquisition a handler identity carries', async () => {
    let identity
    await open({
      run: { ...MANUAL, eachBatch: true },
      before: () => {
        identity = handlerContext.current()
      },
    })
    expect(handlerContext.isSuperseded(identity)).toBe(false)

    const closing = scenario.consumer.disconnect()
    await scenario.release()
    await closing

    expect(identity.acquisition.retired).toBe(true)
    expect(handlerContext.isSuperseded(identity)).toBe(true)
  })

  describe('a default commit the broker already accepted', () => {
    test('a default commit sends the resolved offset again after a deliberate commit moved the partition ahead, with no seek involved', async () => {
      const captured = using(captureGroup())
      await open(TWO_PARTITIONS)
      await scenario.otherDone.promise
      const group = captured.group()

      await group.commitOffsets({}, { auto: true })
      await scenario.consumer.commitOffsets([{ topic: TOPIC, partition: 1, offset: '9' }])
      await group.commitOffsets({}, { auto: true })

      expect(scenario.commits(1)).toEqual(['1', '9', '1'])
      expect(scenario.commits(0)).toEqual(['5'])
      expect(scenario.rig.frames('OffsetCommit')).toHaveLength(3)
    })

    test('is not sent again when a seek reaches its partition before the answer, and a later deliberate commit of another partition stays', async () => {
      const captured = using(captureGroup())
      await open(TWO_PARTITIONS)
      await scenario.otherDone.promise
      const {
        holds: [accepted],
      } = using(holdCommitAnswers(scenario, ['applied']))
      const committing = captured.group().commitOffsets({}, { auto: true })
      await accepted.arrived
      expect(await scenario.seek('0')).toBe('0')
      await scenario.consumer.commitOffsets([{ topic: TOPIC, partition: 1, offset: '9' }])

      accepted.release()
      await committing

      expect(scenario.commits(0)).toEqual(['5', '0'])
      expect(scenario.commits(1)).toEqual(['1', '9'])
      expect(scenario.rig.frames('OffsetCommit')).toHaveLength(3)
      expect(scenario.committed).toEqual({ 0: '0', 1: '9' })
      expect(String(captured.group().offsetManager.committedOffsets()[TOPIC][1])).toBe('9')
      expect(scenario.errors).toEqual([])
    })

    test('reaches the caller as the broker error it was, with no commit sent after it, while a seek is pending', async () => {
      const captured = using(captureGroup())
      await open(TWO_PARTITIONS)
      await scenario.otherDone.promise
      const {
        holds: [failing],
      } = using(holdCommitAnswers(scenario, ['failed']))
      const committing = captured
        .group()
        .commitOffsets({}, { auto: true })
        .then(
          () => null,
          (error) => error
        )
      await failing.arrived
      scenario.consumer.seek({ topic: TOPIC, partition: 0, offset: '0' })

      failing.release()
      const error = await committing

      expect(error).not.toBeNull()
      expect(error).not.toBeInstanceOf(KafkaJSCommitFailedError)
      expect(error.type).toBe('REQUEST_TIMED_OUT')
      expect(scenario.rig.frames('OffsetCommit')).toHaveLength(1)
      expect(scenario.errors).toEqual([])
    })

    test('is published to the committed cache cell by cell, leaving out a partition a listener sought', async () => {
      const captured = using(captureGroup())
      const allow = gate()
      await open({
        ...TWO_PARTITIONS,
        run: { ...MANUAL, partitionsConsumedConcurrently: 2, eachBatch: true },
        other: async ({ resolveOffset, commitOffsetsIfNecessary }) => {
          await allow.promise
          resolveOffset('0')
          await commitOffsetsIfNecessary()
        },
      })
      let admitted = false
      scenario.consumer.on(scenario.consumer.events.COMMIT_OFFSETS, () => {
        if (admitted) return
        admitted = true
        scenario.consumer.seek({ topic: TOPIC, partition: 0, offset: '0' })
      })
      allow.resolve()
      await scenario.otherDone.promise
      const group = captured.group()

      expect(group.unappliedSeeks.has(`${TOPIC}|0`)).toBe(true)
      expect(group.offsetManager.committedOffsets()[TOPIC]).toEqual({ 1: '1' })
      expect(scenario.commits(0)).toEqual(['5'])
      expect(await scenario.position()).toBe('0')
      expect(scenario.errors).toEqual([])
    })

    test('selects its offsets at most three times while seeks keep reaching them before the write, then an automatic commit skips and a plain one is refused', async () => {
      const captured = using(captureGroup())
      await open(TWO_PARTITIONS)
      await scenario.otherDone.promise
      const group = captured.group()
      const selections = jest.spyOn(group.offsetManager, 'selectUncommitted')
      const sought = jest.spyOn(group, 'isSoughtAfter').mockReturnValue(true)
      group.lastSoughtAt = Number.MAX_SAFE_INTEGER
      using({
        restore: () => {
          group.lastSoughtAt = 0
          selections.mockRestore()
          sought.mockRestore()
        },
      })

      await group.commitOffsets({}, { auto: true })
      expect(selections).toHaveBeenCalledTimes(3)

      selections.mockClear()
      const refusal = await failureOf(group.commitOffsets({}, {}))
      expect(refusal).toBeInstanceOf(KafkaJSCommitFailedError)
      expect(refusal.reason).toBe('stale-assignment')
      expect(refusal.message).toBe('A seek superseded the selected offsets')
      expect(selections).toHaveBeenCalledTimes(3)
      expect(scenario.rig.frames('OffsetCommit')).toHaveLength(0)
      expect(scenario.errors).toEqual([])
    })

    test('is committed again by the next default commit when a seek back to the cached offset reached it before the answer, with autoCommit off', async () => {
      const captured = using(captureGroup())
      await open({
        ...TWO_PARTITIONS,
        run: { ...MANUAL, partitionsConsumedConcurrently: 2, eachBatch: true },
      })
      await scenario.otherDone.promise
      const group = captured.group()
      const {
        holds: [accepted],
      } = using(holdCommitAnswers(scenario, ['applied']))
      const committing = group.commitOffsets({}, { auto: true })
      await accepted.arrived
      scenario.consumer.seek({ topic: TOPIC, partition: 0, offset: '2' })

      accepted.release()
      await committing
      expect(await scenario.position()).toBe('2')
      await group.commitOffsets({}, { auto: true })

      expect(scenario.commits(0)).toEqual(['5', '2'])
      expect(scenario.commits(1)).toEqual(['1'])
      expect(scenario.committed).toEqual({ 0: '2', 1: '1' })
      expect(scenario.errors).toEqual([])
    })
  })

  describe('a seek whose commit the broker accepted', () => {
    test('drops the progress it replaced when a deliberate commit superseded it, and the final commit sends neither', async () => {
      const captured = using(captureGroup())
      await open(TWO_PARTITIONS)
      await scenario.otherDone.promise
      const {
        holds: [accepted],
      } = using(holdCommitAnswers(scenario, ['applied']))
      const seeking = scenario.seek('0')
      await accepted.arrived
      await scenario.consumer.commitOffsets([{ topic: TOPIC, partition: 0, offset: '8' }])
      expect(scenario.committed[0]).toBe('8')

      accepted.release()

      expect(await seeking).toBe('8')
      expect(captured.group().unappliedSeeks.has(`${TOPIC}|0`)).toBe(false)
      const closing = scenario.consumer.disconnect()
      await scenario.release()
      await closing
      expect(scenario.commits(0)).toEqual(['0', '8'])
      expect(scenario.commits(1)).toEqual(['1'])
      expect(scenario.committed).toEqual({ 0: '8', 1: '1' })
      expect(scenario.errors).toEqual([])
    })
    test('a newer resolution with the same decimal offset survives the late answer of an older seek', async () => {
      await open({
        run: {
          autoCommit: true,
          autoCommitInterval: 60000,
          eachBatchAutoResolve: false,
          eachBatch: true,
        },
        before: ({ resolveOffset }) => resolveOffset('4'),
        current: ({ resolveOffset }) => resolveOffset('4'),
      })
      const {
        holds: [accepted],
      } = using(holdCommitAnswers(scenario, ['applied']))
      const older = scenario.seek('0')
      await accepted.arrived
      expect(await scenario.seek('2')).toBe('2')
      await scenario.release()
      expect(await scenario.nextDelivery()).toEqual(BEFORE)
      expect(await scenario.position()).toBe('5')

      accepted.release()

      expect(await older).toBe('5')
      expect(await scenario.position()).toBe('5')
      const closing = scenario.consumer.disconnect()
      scenario.hold.resolve()
      await closing
      expect(scenario.commits()).toEqual(['0', '2', '5'])
      expect(scenario.committed[0]).toBe('5')
      expect(scenario.errors).toEqual([])
    })
  })

  describe('a seek whose application ended without applying', () => {
    const SEEKING = {
      run: { autoCommit: true, autoCommitInterval: 60000, eachBatch: true },
      before: ({ resolveOffset }) => resolveOffset('4'),
    }
    const rebuildIncrementally = async (group) => {
      scenario.consumer.pause([{ topic: TOPIC, partitions: [0] }])
      await scenario.release()
      await group.rebalanceCoordinator.runner.waitForHandlers()
      await group.applyRebalanceResult({ assignment: { [TOPIC]: [0] } }, { rebuildOffsets: false })
    }
    const disconnect = async () => {
      const closing = scenario.consumer.disconnect()
      scenario.hold.resolve()
      await closing
    }

    test('after the broker accepted it and an incremental rebuild refused the applying call, leaves the replaced progress out and reads the accepted offset', async () => {
      const captured = using(captureGroup())
      await open(SEEKING)
      const group = captured.group()
      const {
        holds: [accepted],
      } = using(holdCommitAnswers(scenario, ['applied']))
      const seeking = failureOf(scenario.seek('0'))
      await accepted.arrived
      await rebuildIncrementally(group)

      accepted.release()

      expect((await seeking).type).toBe('CONSUMER_STOPPING')
      expect(group.unappliedSeeks.has(`${TOPIC}|0`)).toBe(false)
      expect(await scenario.position()).toBe('0')
      expect(group.offsetManager.nextOffset(TOPIC, 0).toString()).toBe('0')
      await disconnect()
      expect(scenario.commits()).toEqual(['0'])
      expect(scenario.committed[0]).toBe('0')
      expect(scenario.errors).toEqual([])
    })

    test('after an incremental rebuild refused its commit before it was written, leaves the replaced progress out of the position and the final commit', async () => {
      const captured = using(captureGroup())
      const held = using(holdNextCommitCall())
      await open(SEEKING)
      const group = captured.group()
      const seeking = failureOf(scenario.seek('0'))
      await held.reached
      await rebuildIncrementally(group)

      held.release()

      expect((await seeking).type).toBe('CONSUMER_STOPPING')
      expect(await scenario.position()).toBe('2')
      expect(group.offsetManager.nextOffset(TOPIC, 0).toString()).toBe('2')
      await disconnect()
      expect(scenario.commits()).toEqual([])
      expect(scenario.committed[0]).toBe('2')
      expect(scenario.errors).toEqual([])
    })

    test('after its commit raised a commit failure the seek swallowed, leaves the replaced progress out of the position and the final commit', async () => {
      const captured = using(captureGroup())
      await open(SEEKING)
      const send = jest
        .spyOn(captured.group().offsetManager, 'sendOffsetCommit')
        .mockRejectedValueOnce(new KafkaJSCommitFailedError('lost', { reason: 'stale-assignment' }))
      using({ restore: () => send.mockRestore() })

      expect(await scenario.seek('0')).toBe('2')

      await scenario.release()
      await disconnect()
      expect(scenario.commits()).toEqual([])
      expect(scenario.committed[0]).toBe('2')
      expect(scenario.errors).toEqual([])
    })
  })

  describe('a seek whose commit failed', () => {
    const FAILING_SEEK = {
      run: { autoCommit: true, autoCommitInterval: 60000, eachBatch: true },
      before: ({ resolveOffset }) => resolveOffset('4'),
    }
    test('is not applied again after a newer seek of the partition was applied', async () => {
      await open(FAILING_SEEK)
      const {
        holds: [failing],
      } = using(holdCommitAnswers(scenario, ['failed']))
      const older = failureOf(scenario.seek('0'))
      await failing.arrived
      expect(await scenario.seek('7')).toBe('7')

      failing.release()

      expect((await older).type).toBe('REQUEST_TIMED_OUT')
      expect(await scenario.position()).toBe('7')
      expect(scenario.commits()).toEqual(['0', '7'])
      expect(scenario.errors).toEqual([])
    })

    test('leaves a newer seek of the partition that is still queued to apply alone', async () => {
      await open(FAILING_SEEK)
      const {
        holds: [failing],
      } = using(holdCommitAnswers(scenario, ['failed']))
      const older = failureOf(scenario.seek('0'))
      await failing.arrived
      scenario.consumer.seek({ topic: TOPIC, partition: 0, offset: '7' })

      failing.release()

      expect((await older).type).toBe('REQUEST_TIMED_OUT')
      expect(await scenario.position()).toBe('7')
      expect(scenario.commits()).toEqual(['0', '7'])
      expect(scenario.errors).toEqual([])
    })

    test('is not applied again over a newer seek of the partition that is still being applied', async () => {
      await open(FAILING_SEEK)
      const {
        holds: [failing, applying],
      } = using(holdCommitAnswers(scenario, ['failed', 'applied']))
      const older = failureOf(scenario.seek('0'))
      await failing.arrived
      const newer = scenario.seek('7')
      await applying.arrived

      failing.release()
      expect((await older).type).toBe('REQUEST_TIMED_OUT')
      applying.release()

      expect(await newer).toBe('7')
      expect(await scenario.position()).toBe('7')
      expect(scenario.commits()).toEqual(['0', '7'])
      expect(scenario.errors).toEqual([])
    })

    test('is not applied again once its assignment was rebuilt', async () => {
      const captured = using(captureGroup())
      await open(FAILING_SEEK)
      const group = captured.group()
      const {
        holds: [failing],
      } = using(holdCommitAnswers(scenario, ['failed']))
      const parts = [{ topic: TOPIC, partitions: [0] }]
      const round = new FetchPreparation(
        group,
        group.idleWait,
        parts,
        Infinity,
        group.ownership.token()
      )
      scenario.consumer.seek({ topic: TOPIC, partition: 0, offset: '0' })
      const older = failureOf(group.seekOffsets(parts, round))
      await failing.arrived
      await group.applyRebalanceResult({ assignment: { [TOPIC]: [0] } })

      failing.release()

      expect(await older).not.toBeNull()
      expect(group.hasSeekOffset({ topic: TOPIC, partition: 0 })).toBe(false)
      expect(group.unappliedSeeks.has(`${TOPIC}|0`)).toBe(false)
      expect(await scenario.position()).toBe('2')
      expect(scenario.commits()).toEqual(['0'])
      expect(scenario.errors).toEqual([])
    })

    test('is not applied again once its topic was recreated', async () => {
      const captured = using(captureGroup())
      await open(FAILING_SEEK)
      const {
        holds: [failing],
      } = using(holdCommitAnswers(scenario, ['failed']))
      const older = failureOf(scenario.seek('0'))
      await failing.arrived
      captured.group().invalidateTopic(TOPIC)

      failing.release()

      expect((await older).type).toBe('REQUEST_TIMED_OUT')
      expect(captured.group().hasSeekOffset({ topic: TOPIC, partition: 0 })).toBe(false)
      expect(captured.group().unappliedSeeks.has(`${TOPIC}|0`)).toBe(false)
      expect(scenario.commits()).toEqual(['0'])
      expect(await scenario.position()).toBe('2')
      expect(scenario.committed[0]).toBe('2')
    })
  })
})
