const Broker = require('../../broker')
const RequestQueue = require('../../network/requestQueue')
const ConsumerGroup = require('../consumerGroup')
const { FetchPreparation } = require('../fetchPreparation')
const handlerContext = require('../handlerContext')
const OffsetManager = require('../offsetManager')
const { KafkaJSCommitFailedError, KafkaJSNonRetriableError } = require('../../errors')
const { TOPIC, GROUP, CloseRig, batchAt, gate, until } = require('./fixtures/closeRig')

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
 * Holds each of the next OffsetCommits at its dispatch, as `holdNextCommitWrite` holds one.
 * @param {number} count
 * @returns {{ holds: { reached: Promise<void>, release: () => void }[], restore: () => void }}
 */
const holdCommitWrites = (count) => {
  const holds = Array.from({ length: count }, () => ({ reached: gate(), opened: gate() }))
  const original = RequestQueue.prototype.sendSocketRequest
  let next = 0
  const spy = jest
    .spyOn(RequestQueue.prototype, 'sendSocketRequest')
    .mockImplementation(function (request) {
      const hold = request.entry.apiName === 'OffsetCommit' && holds[next]
      if (!hold) return original.call(this, request)
      next++
      hold.reached.resolve()
      hold.opened.promise.then(() => original.call(this, request))
    })
  return {
    holds: holds.map(({ reached, opened }) => ({
      reached: reached.promise,
      release: opened.resolve,
    })),
    restore: () => {
      holds.forEach(({ opened }) => opened.resolve())
      spy.mockRestore()
    },
  }
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

/**
 * @param {CloseRig} rig
 * @returns {string[][]} the `partition:offset` cells of every OffsetCommit frame, in order
 */
const frameCells = (rig) =>
  rig
    .frames('OffsetCommit')
    .map(({ fields }) =>
      fields.topics.flatMap(({ partitions }) =>
        partitions.map(({ partition, offset }) => `${partition}:${offset}`)
      )
    )

/**
 * @param {CloseRig} rig
 * @param {number} partition
 * @returns {string[]} the offset every Fetch frame asked for the partition, in order
 */
const fetchOffsetsOf = (rig, partition) =>
  rig
    .frames('Fetch')
    .flatMap(({ fields }) => fields.topics.flatMap(({ partitions }) => partitions))
    .filter((cell) => cell.partition === partition)
    .map(({ fetchOffset }) => String(fetchOffset))

/**
 * Answers the next Fetch that asks partition 0 at `offset` with OFFSET_OUT_OF_RANGE for it, held
 * until released when `hold` is set.
 * @param {CloseRig} rig
 * @param {string} offset
 * @param {boolean} hold
 * @returns {{ reached: Promise<void>, release: () => void, restore: () => void }}
 */
const outOfRangeAt = (rig, offset, hold) => {
  const reached = gate()
  const released = gate()
  if (!hold) released.resolve()
  const fetch = rig.cluster.answerers.get('Fetch')
  let answered = false
  rig.cluster.answer('Fetch', async (fields, record) => {
    const p0 = fields.topics[0].partitions.find(({ partition }) => partition === 0)
    if (answered || !p0 || String(p0.fetchOffset) !== offset) return fetch(fields, record)
    answered = true
    reached.resolve()
    await released.promise
    return {
      partitions: fields.topics[0].partitions.map(({ partition }) => ({
        partition,
        errorCode: partition === 0 ? 1 : 0,
        records: false,
        abortedTransactions: [],
      })),
    }
  })
  return { reached: reached.promise, release: released.resolve, restore: released.resolve }
}

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
    expect(scenario.old.isStale()).toBe(true)

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

    expect(scenario.rig.frames('OffsetCommit')).toHaveLength(1)
    expect(scenario.commits(0)).toEqual([])
    expect(scenario.commits(1)).toEqual(['1'])
    expect(scenario.committed).toEqual({ 0: '2', 1: '1' })
    expect(await scenario.position()).toBe('0')
    expect(scenario.errors).toEqual([])
  })

  describe('a default commit pruned at its write', () => {
    const PRUNED = (allow) => ({
      partitions: 2,
      run: { ...MANUAL, partitionsConsumedConcurrently: 2, eachBatch: true },
      before: ({ resolveOffset }) => resolveOffset('4'),
      other: async ({ resolveOffset, commitOffsetsIfNecessary }) => {
        await allow.promise
        resolveOffset('0')
        await commitOffsetsIfNecessary()
      },
    })
    const cellsOf = ({ topics }) =>
      topics.flatMap(({ partitions }) =>
        partitions.map(({ partition, offset }) => `${partition}:${offset}`)
      )

    test('reports in COMMIT_OFFSETS exactly the cells it sent', async () => {
      const write = using(holdNextCommitWrite())
      const allow = gate()
      await open(PRUNED(allow))
      const reported = []
      scenario.consumer.on(scenario.consumer.events.COMMIT_OFFSETS, ({ payload }) =>
        reported.push(cellsOf(payload))
      )
      allow.resolve()
      await write.reached
      expect(await scenario.seek('0')).toBe('0')

      write.release()
      await scenario.otherDone.promise

      expect(frameCells(scenario.rig)).toEqual([['1:1']])
      expect(reported).toEqual([['1:1']])
      expect(scenario.errors).toEqual([])
    })

    test('publishes what it sent even when a COMMIT_OFFSETS listener replaces the reported cells', async () => {
      const captured = using(captureGroup())
      const allow = gate()
      await open(PRUNED(allow))
      scenario.consumer.on(scenario.consumer.events.COMMIT_OFFSETS, ({ payload }) => {
        payload.topics = [{ topic: TOPIC, partitions: [{ partition: 0, offset: '9' }] }]
      })

      allow.resolve()
      await scenario.otherDone.promise

      expect(frameCells(scenario.rig)).toEqual([['0:5', '1:1']])
      expect(captured.group().offsetManager.committedOffsets()[TOPIC]).toEqual({ 0: '5', 1: '1' })
      expect(scenario.errors).toEqual([])
    })
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

  test('isStale() of an older batch reports no seek when its partition is revoked without one', async () => {
    const captured = using(captureGroup())
    await open({ run: { ...MANUAL, eachBatch: true } })
    const group = captured.group()
    expect(scenario.old.isStale()).toBe(false)

    await group.applyRebalanceResult({ assignment: {} }, { rebuildOffsets: false })

    expect(scenario.old.isStale()).toBe(false)
  })

  test('isStale() of an older batch reports no seek when its topic is recreated without one', async () => {
    const captured = using(captureGroup())
    await open({ run: { ...MANUAL, eachBatch: true } })
    expect(scenario.old.isStale()).toBe(false)

    captured.group().invalidateTopic(TOPIC)

    expect(scenario.old.isStale()).toBe(false)
  })

  test('isStale() of an older batch stays true after a seek of its partition, also once its partition is revoked', async () => {
    const captured = using(captureGroup())
    await open({ run: { ...MANUAL, eachBatch: true } })
    expect(await scenario.seek('0')).toBe('0')
    expect(scenario.old.isStale()).toBe(true)

    await captured.group().applyRebalanceResult({ assignment: {} }, { rebuildOffsets: false })

    expect(scenario.old.isStale()).toBe(true)
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

  test('a seek whose commit failed leaves the progress made before it in the final commit', async () => {
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

    expect(scenario.commits(0)).toEqual(['0', '5'])
    expect(scenario.commits(1)).toEqual(['1'])
    expect(scenario.committed).toEqual({ 0: '5', 1: '1' })
    expect(scenario.errors).toEqual([])
  })

  test('a seek whose commit failed is not sent again, and the partition continues from its position before the seek', async () => {
    await open({
      run: { autoCommit: true, autoCommitInterval: 60000, eachBatch: true },
      before: ({ resolveOffset }) => resolveOffset('4'),
    })
    failNextCommit(scenario)
    await expect(scenario.seek('0')).rejects.toMatchObject({ type: 'REQUEST_TIMED_OUT' })

    expect(await scenario.position()).toBe('5')
    await scenario.release()

    expect(await scenario.nextDelivery()).toEqual(['value-5'])
    expect(scenario.commits()).toEqual(['0'])
    expect(scenario.committed[0]).toBe('2')
    expect(scenario.errors).toEqual([])
  })

  test('a commit with no seek admitted skips the sought-partition checks', async () => {
    const captured = using(captureGroup())
    await open({ run: { ...MANUAL, eachBatch: true }, old: resolveAndCommit })
    const group = captured.group()
    const scans = jest.spyOn(group, 'isStaleSince')
    const lookups = jest.spyOn(group, 'isSeekGated')
    const forgetResolved = jest.spyOn(group.offsetManager, 'forgetResolved')
    const forgetCommitted = jest.spyOn(group.offsetManager, 'forgetCommitted')
    const spies = [scans, lookups, forgetResolved, forgetCommitted]
    using({ restore: () => spies.forEach((spy) => spy.mockRestore()) })

    await scenario.release()

    expect(scenario.commits()).toEqual(['5'])
    for (const spy of spies) expect(spy).not.toHaveBeenCalled()
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

    test('is published to the committed cache cell by cell, also for a partition a listener sought', async () => {
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

      expect(group.isSeekGated(TOPIC, 0)).toBe(true)
      expect(group.offsetManager.committedOffsets()[TOPIC]).toEqual({ 0: '5', 1: '1' })
      expect(scenario.commits(0)).toEqual(['5'])
      expect(await scenario.position()).toBe('0')
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
      expect(captured.group().isSeekGated(TOPIC, 0)).toBe(false)
      const closing = scenario.consumer.disconnect()
      await scenario.release()
      await closing
      expect(scenario.commits(0)).toEqual(['0', '8'])
      expect(scenario.commits(1)).toEqual(['1'])
      expect(scenario.committed).toEqual({ 0: '8', 1: '1' })
      expect(scenario.errors).toEqual([])
    })
    test('a newer seek waits for the answer of an older one, then applies, and the progress made after it stands', async () => {
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
      const newer = scenario.seek('2')
      await new Promise(setImmediate)
      expect(scenario.commits()).toEqual(['0'])

      accepted.release()

      expect(await older).toBe('2')
      expect(await newer).toBe('2')
      await scenario.release()
      expect(await scenario.nextDelivery()).toEqual(BEFORE)
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
    const applyIncrementally = async (group) => {
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

    test('after the broker accepted it and an incremental apply refused the applying call, leaves the replaced progress out and reads the accepted offset', async () => {
      const captured = using(captureGroup())
      await open(SEEKING)
      const group = captured.group()
      const {
        holds: [accepted],
      } = using(holdCommitAnswers(scenario, ['applied']))
      const seeking = failureOf(scenario.seek('0'))
      await accepted.arrived
      await applyIncrementally(group)

      accepted.release()

      expect((await seeking).type).toBe('CONSUMER_STOPPING')
      expect(group.isSeekGated(TOPIC, 0)).toBe(false)
      expect(await scenario.position()).toBe('0')
      expect(group.offsetManager.nextOffset(TOPIC, 0).toString()).toBe('0')
      await disconnect()
      expect(scenario.commits()).toEqual(['0'])
      expect(scenario.committed[0]).toBe('0')
      expect(scenario.errors).toEqual([])
    })

    test('after an incremental apply refused its commit before it was written, keeps the cached offset and its position and is not sent again', async () => {
      const captured = using(captureGroup())
      const held = using(holdNextCommitCall())
      await open(SEEKING)
      const group = captured.group()
      const apply = group.offsetManager.seek
      let cachedAfterRefusal
      const seek = jest.spyOn(group.offsetManager, 'seek').mockImplementationOnce(async function (
        ...args
      ) {
        try {
          return await apply.apply(this, args)
        } finally {
          cachedAfterRefusal = group.offsetManager.committedOffsets()[TOPIC][0]
        }
      })
      using({ restore: () => seek.mockRestore() })
      const seeking = failureOf(scenario.seek('0'))
      await held.reached
      await applyIncrementally(group)

      held.release()

      expect((await seeking).type).toBe('CONSUMER_STOPPING')
      expect(cachedAfterRefusal).toBe('2')
      expect(group.isSeekGated(TOPIC, 0)).toBe(false)
      expect(group.hasSeekOffset({ topic: TOPIC, partition: 0 })).toBe(false)
      expect(await scenario.position()).toBe('5')
      await disconnect()
      expect(scenario.commits()).toEqual(['5'])
      expect(scenario.committed[0]).toBe('5')
      expect(scenario.errors).toEqual([])
    })

    test('after an incremental apply refused its commit before it was written, continues from an automatic commit the broker accepted while the seek was admitted', async () => {
      const captured = using(captureGroup())
      await open(SEEKING)
      const group = captured.group()
      const {
        holds: [automatic],
      } = using(holdCommitAnswers(scenario, ['applied']))
      const committing = group.commitOffsets(undefined, { auto: true })
      await automatic.arrived
      expect(scenario.committed[0]).toBe('5')
      scenario.consumer.seek({ topic: TOPIC, partition: 0, offset: '0' })
      automatic.release()
      await committing
      const held = using(holdNextCommitCall())
      const seeking = failureOf(scenario.position())
      await held.reached
      await applyIncrementally(group)

      held.release()

      expect((await seeking).type).toBe('CONSUMER_STOPPING')
      expect(await scenario.position()).toBe('5')
      expect(scenario.commits()).toEqual(['5'])
      scenario.consumer.resume([{ topic: TOPIC, partitions: [0] }])
      expect(await scenario.nextDelivery()).toEqual(['value-5'])
      expect(scenario.errors).toEqual([])
    })

    test('after its commit raised a commit failure, surfaces it and keeps the position before the seek', async () => {
      const captured = using(captureGroup())
      await open(SEEKING)
      const lost = new KafkaJSCommitFailedError('lost', { reason: 'stale-assignment' })
      const send = jest
        .spyOn(captured.group().offsetManager, 'sendOffsetCommit')
        .mockRejectedValueOnce(lost)
      using({ restore: () => send.mockRestore() })

      expect(await failureOf(scenario.seek('0'))).toBe(lost)
      expect(await scenario.position()).toBe('5')

      await scenario.release()
      await disconnect()
      expect(scenario.commits()).toEqual(['5'])
      expect(scenario.committed[0]).toBe('5')
      expect(scenario.errors).toEqual([])
    })
  })

  describe('a seek that position validation reaches while it is being applied', () => {
    const SEEKING = {
      run: { autoCommit: true, autoCommitInterval: 60000, eachBatch: true },
      before: ({ resolveOffset }) => resolveOffset('4'),
    }
    /**
     * Moves partition 0's leader epoch and waits until a validation read of the fetch loop has run
     * while the seek is still being applied.
     * @param {ConsumerGroup} group
     * @returns {Promise<void>}
     */
    const validateWhileApplying = async (group) => {
      const validated = gate()
      const validate = group.validatePositions
      const epoch = jest.spyOn(group.cluster, 'leaderEpoch').mockReturnValue(6)
      const validation = jest.spyOn(group, 'validatePositions').mockImplementation(async function (
        ...args
      ) {
        const result = await validate.apply(this, args)
        if (group.isSeekGated(TOPIC, 0)) validated.resolve()
        return result
      })
      using({
        restore: () => {
          epoch.mockRestore()
          validation.mockRestore()
        },
      })
      scenario.consumer.pause([{ topic: TOPIC, partitions: [0] }])
      await scenario.release()
      await validated.promise
    }
    const disconnect = async () => {
      const closing = scenario.consumer.disconnect()
      scenario.hold.resolve()
      await closing
    }

    test('keeps a deliberate commit that superseded it as the position and leaves the committed offset out of the final commit', async () => {
      const captured = using(captureGroup())
      await open(SEEKING)
      const group = captured.group()
      const {
        holds: [accepted],
      } = using(holdCommitAnswers(scenario, ['applied']))
      const seeking = scenario.seek('0')
      await accepted.arrived
      await validateWhileApplying(group)
      await scenario.consumer.commitOffsets([{ topic: TOPIC, partition: 0, offset: '8' }])
      expect(scenario.committed[0]).toBe('8')

      accepted.release()

      expect(await seeking).toBe('8')
      expect(await scenario.position()).toBe('8')
      await disconnect()
      expect(scenario.commits()).toEqual(['0', '8'])
      expect(scenario.committed[0]).toBe('8')
      expect(scenario.deliveries).toEqual([BEFORE])
      expect(scenario.errors).toEqual([])
    })

    test('reads the accepted offset after an incremental apply refused the applying call, and leaves the committed offset out of the final commit', async () => {
      const captured = using(captureGroup())
      await open(SEEKING)
      const group = captured.group()
      const {
        holds: [accepted],
      } = using(holdCommitAnswers(scenario, ['applied']))
      const seeking = failureOf(scenario.seek('0'))
      await accepted.arrived
      await validateWhileApplying(group)
      await group.rebalanceCoordinator.runner.waitForHandlers()
      await group.applyRebalanceResult({ assignment: { [TOPIC]: [0] } }, { rebuildOffsets: false })

      accepted.release()

      expect((await seeking).type).toBe('CONSUMER_STOPPING')
      expect(await scenario.position()).toBe('0')
      await disconnect()
      expect(scenario.commits()).toEqual(['0'])
      expect(scenario.committed[0]).toBe('0')
      expect(scenario.deliveries).toEqual([BEFORE])
      expect(scenario.errors).toEqual([])
    })

    test('an answer to a validation requested before the seek does not set the position when it arrives while the seek is applied', async () => {
      const captured = using(captureGroup())
      await open(SEEKING)
      const group = captured.group()
      const asked = gate()
      const answered = gate()
      const reply = gate()
      // The leader at epoch 6 says the tracked epoch 5 ended at offset 1, below the position: the
      // answer would rewind the partition if it still applied.
      scenario.rig.cluster.answer('OffsetForLeaderEpoch', async ({ topics }) => {
        asked.resolve()
        await reply.promise
        return {
          partitions: topics[0].partitions.map(({ partition }) => ({
            partition,
            errorCode: 0,
            leaderEpoch: 5,
            endOffset: '1',
          })),
        }
      })
      const truncations = []
      scenario.consumer.on(scenario.consumer.events.LOG_TRUNCATION, ({ payload }) =>
        truncations.push(payload)
      )
      expect(await scenario.position()).toBe('5')
      expect(group.offsetManager.lastConsumedEpoch(TOPIC, 0)).toBe(5)
      const apply = group.applyValidationResponse
      const epoch = jest.spyOn(group.cluster, 'leaderEpoch').mockReturnValue(6)
      const applying = jest
        .spyOn(group, 'applyValidationResponse')
        .mockImplementation(async function (...args) {
          try {
            return await apply.apply(this, args)
          } finally {
            answered.resolve()
          }
        })
      using({
        restore: () => {
          reply.resolve()
          epoch.mockRestore()
          applying.mockRestore()
        },
      })
      await scenario.release()
      await asked.promise
      const {
        holds: [accepted],
      } = using(holdCommitAnswers(scenario, ['applied']))
      const seeking = scenario.seek('0')
      await accepted.arrived

      reply.resolve()
      await answered.promise

      expect(group.isSeekGated(TOPIC, 0)).toBe(true)
      expect(group.offsetManager.resolvedOffsetAt(TOPIC, 0)).toBe('5')
      expect(truncations).toEqual([])
      await scenario.consumer.commitOffsets([{ topic: TOPIC, partition: 0, offset: '8' }])
      accepted.release()
      expect(await seeking).toBe('8')
      await disconnect()
      expect(scenario.commits()).toEqual(['0', '8'])
      expect(scenario.committed[0]).toBe('8')
      expect(scenario.deliveries).toEqual([BEFORE])
      expect(scenario.errors).toEqual([])
    })
  })

  describe('an older handler around a seek', () => {
    const AUTO = { autoCommit: true, autoCommitInterval: 60000, eachBatch: true }
    /**
     * The old batch's work once its gate opens: resolve further, then commit through each of its
     * own paths, recording what each did.
     * @param {object} seen collects the outcomes
     * @returns {(payload: object) => Promise<void>}
     */
    const oldWork =
      (seen) =>
      async ({ resolveOffset, commitOffsetsIfNecessary, isStale }) => {
        resolveOffset('4')
        await commitOffsetsIfNecessary()
        seen.explicit = await failureOf(commitOffsetsIfNecessary(PAYLOAD_5))
        seen.stale = isStale()
      }

    test('cannot restore progress after an applied seek: its resolve is ignored and none of its commits is sent', async () => {
      const seen = {}
      await open({
        run: AUTO,
        before: ({ resolveOffset }) => resolveOffset('2'),
        old: oldWork(seen),
      })
      expect(await scenario.seek('1')).toBe('1')

      await scenario.release()

      expect(seen.explicit).toBeInstanceOf(KafkaJSCommitFailedError)
      expect(seen.stale).toBe(true)
      expect(scenario.commits()).toEqual(['1'])
      expect(scenario.committed[0]).toBe('1')
      expect(await scenario.position()).toBe('1')
      expect(await scenario.nextDelivery()).toEqual(['value-1'])
    })

    test('stays fenced after the seek fails, and the partition keeps its position before the seek', async () => {
      const seen = {}
      await open({
        run: AUTO,
        before: ({ resolveOffset }) => resolveOffset('2'),
        old: oldWork(seen),
      })
      failNextCommit(scenario)
      expect((await failureOf(scenario.seek('6'))).type).toBe('REQUEST_TIMED_OUT')
      expect(await scenario.position()).toBe('3')

      await scenario.release()

      expect(seen.explicit).toBeInstanceOf(KafkaJSCommitFailedError)
      expect(seen.stale).toBe(true)
      expect(await scenario.position()).toBe('3')
      expect(scenario.commits()).toEqual(['6'])
      expect(scenario.committed[0]).toBe('2')
      const closing = scenario.consumer.disconnect()
      scenario.hold.resolve()
      await closing
      expect(scenario.commits()).toEqual(['6', '3'])
      expect(scenario.committed[0]).toBe('3')
    })

    test('a non-retriable seek failure on the fetch loop warns once and crashes the consumer, with no retry', async () => {
      const captured = using(captureGroup())
      await open({ run: AUTO })
      const warn = jest.spyOn(captured.group().logger, 'warn')
      using({ restore: () => warn.mockRestore() })
      const crash = gate()
      scenario.consumer.on(scenario.consumer.events.CRASH, ({ payload }) =>
        crash.resolve({ type: payload.error.type, restart: payload.restart })
      )
      const answer = scenario.rig.cluster.answerers.get('OffsetCommit')
      scenario.rig.cluster.answer('OffsetCommit', (fields, record) => {
        if (String(fields.topics[0].partitions[0].offset) !== '5') return answer(fields, record)
        return {
          topics: fields.topics.map(({ partitions, ...entry }) => ({
            ...entry,
            partitions: partitions.map(({ partition }) => ({ partition, errorCode: 29 })),
          })),
        }
      })

      scenario.consumer.seek({ topic: TOPIC, partition: 0, offset: '5' })
      await scenario.release()

      expect(await crash.promise).toEqual({ type: 'TOPIC_AUTHORIZATION_FAILED', restart: false })
      expect(
        warn.mock.calls.filter(([message]) => message.startsWith('Seek failed')).map(([m]) => m)
      ).toEqual(['Seek failed; the partition continues from its position before the seek'])
      expect(scenario.commits()).toEqual(['5'])
      expect(scenario.committed[0]).toBe('2')
    })
  })

  describe('a seek whose commit failed', () => {
    const FAILING_SEEK = {
      run: { autoCommit: true, autoCommitInterval: 60000, eachBatch: true },
      before: ({ resolveOffset }) => resolveOffset('4'),
    }
    test('is not applied again, and a newer seek of the partition waits for its answer and then applies', async () => {
      await open(FAILING_SEEK)
      const {
        holds: [failing],
      } = using(holdCommitAnswers(scenario, ['failed']))
      const older = failureOf(scenario.seek('0'))
      await failing.arrived
      const newer = scenario.seek('7')
      await new Promise(setImmediate)
      expect(scenario.commits()).toEqual(['0'])

      failing.release()

      expect((await older).type).toBe('REQUEST_TIMED_OUT')
      expect(await newer).toBe('7')
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
      await new Promise(setImmediate)
      expect(scenario.commits()).toEqual(['0'])

      failing.release()
      expect((await older).type).toBe('REQUEST_TIMED_OUT')
      await applying.arrived
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
      expect(group.isSeekGated(TOPIC, 0)).toBe(false)
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
      expect(captured.group().isSeekGated(TOPIC, 0)).toBe(false)
      expect(scenario.commits()).toEqual(['0'])
      expect(await scenario.position()).toBe('2')
      expect(scenario.committed[0]).toBe('2')
    })

    test('is not applied again over a deliberate commit of the partition made while its commit was in flight, which stands', async () => {
      await open(FAILING_SEEK)
      const {
        holds: [failing, deliberate],
      } = using(holdCommitAnswers(scenario, ['failed', 'applied']))
      const older = failureOf(scenario.seek('0'))
      await failing.arrived
      const committing = scenario.consumer.commitOffsets([
        { topic: TOPIC, partition: 0, offset: '8' },
      ])
      await deliberate.arrived

      failing.release()
      expect((await older).type).toBe('REQUEST_TIMED_OUT')
      deliberate.release()
      await committing

      expect(await scenario.position()).toBe('5')
      expect(scenario.commits()).toEqual(['0', '8'])
      expect(scenario.committed[0]).toBe('8')
      expect(scenario.errors).toEqual([])
    })

    test('is not applied again over a deliberate commit of the partition made while it was queued, which stands', async () => {
      await open(FAILING_SEEK)
      const {
        holds: [deliberate, failing],
      } = using(holdCommitAnswers(scenario, ['applied', 'failed']))
      scenario.consumer.seek({ topic: TOPIC, partition: 0, offset: '0' })
      const committing = scenario.consumer.commitOffsets([
        { topic: TOPIC, partition: 0, offset: '8' },
      ])
      await deliberate.arrived
      deliberate.release()
      await committing
      const applying = failureOf(scenario.position())
      await failing.arrived

      failing.release()

      expect((await applying).type).toBe('REQUEST_TIMED_OUT')
      expect(await scenario.position()).toBe('5')
      expect(scenario.commits()).toEqual(['8', '0'])
      expect(scenario.committed[0]).toBe('8')
      expect(scenario.errors).toEqual([])
    })
  })

  describe('a seek popped before a newer seek of its partition', () => {
    const SEEKING = {
      run: { autoCommit: true, autoCommitInterval: 60000, eachBatch: true },
      before: ({ resolveOffset }) => resolveOffset('4'),
    }

    test('is not written once a newer seek was admitted', async () => {
      await open(SEEKING)
      const write = using(holdNextCommitWrite())
      const older = scenario.seek('0')
      await write.reached
      const newer = scenario.seek('7')
      await new Promise(setImmediate)
      expect(scenario.commits()).toEqual([])

      write.release()

      expect(await newer).toBe('7')
      expect(await older).toBe('7')
      expect(await scenario.position()).toBe('7')
      expect(scenario.commits()).toEqual(['7'])
      expect(scenario.committed[0]).toBe('7')
      expect(scenario.errors).toEqual([])
    })

    test.each(['acquired again', 'recreated'])(
      'leaves the halt of its partition alone when its commit gets no answer after the partition was %s',
      async (change) => {
        const captured = using(captureGroup())
        await open(SEEKING)
        const group = captured.group()
        const key = `${TOPIC}|0`
        const answer = scenario.rig.cluster.answerers.get('OffsetCommit')
        const arrived = gate()
        const lose = gate()
        let lost = false
        scenario.rig.cluster.answer('OffsetCommit', async (fields, record) => {
          if (lost) return answer(fields, record)
          lost = true
          arrived.resolve()
          await lose.promise
          throw new Error('connection lost after the write')
        })
        using({ restore: () => lose.resolve() })
        const older = failureOf(scenario.seek('0'))
        await arrived.promise
        if (change === 'recreated') group.invalidateTopic(TOPIC)
        else {
          await group.applyRebalanceResult(
            { assignment: { [TOPIC]: [] } },
            { rebuildOffsets: false }
          )
          await group.applyRebalanceResult(
            { assignment: { [TOPIC]: [0] } },
            { rebuildOffsets: false }
          )
        }
        const halt = { offset: '3', reason: 'checksum', error: new Error('a later halt') }
        group.haltedPartitions.set(key, halt)

        lose.resolve()

        expect(await older).not.toBeNull()
        expect(group.haltedPartitions.get(key)).toBe(halt)
        expect(scenario.commits()).toEqual(['0'])
      }
    )
  })
  describe('a seek reservation on the wire', () => {
    /**
     * Records every request queue a request is pushed to, and optionally keeps the first
     * OffsetCommit queued, unsent, behind a long throttle.
     * @param {{ queueFirstCommit?: boolean }} [options]
     * @returns {{ queues: Set<RequestQueue>, queued: Promise<RequestQueue>, restore: () => void }}
     */
    const trackQueues = ({ queueFirstCommit = false } = {}) => {
      const queues = new Set()
      const queued = gate()
      let armed = queueFirstCommit
      const push = RequestQueue.prototype.push
      const spy = jest.spyOn(RequestQueue.prototype, 'push').mockImplementation(function (request) {
        queues.add(this)
        if (armed && request.entry.apiName === 'OffsetCommit') {
          armed = false
          this.throttledUntil = Date.now() + 60000
          const result = push.call(this, request)
          queued.resolve(this)
          return result
        }
        return push.call(this, request)
      })
      return { queues, queued: queued.promise, restore: () => spy.mockRestore() }
    }

    /**
     * Disconnects the scenario and checks it left no reservation, ledger cell, release
     * registration, position waiter or correlation behind.
     * @param {ConsumerGroup} group
     * @param {Set<RequestQueue>} queues
     * @returns {Promise<void>}
     */
    const disposeAndAudit = async (group, queues) => {
      await scenario.dispose()
      scenario = undefined
      expect(group.operations.reserved).toBe(0)
      expect(group.operations.size()).toBe(0)
      expect(group.operations.releaseWakes.size).toBe(0)
      expect(group.idleWait.waiters.size).toBe(0)
      for (const queue of queues) expect(queue.inflight.size + queue.pending.length).toBe(0)
    }

    test('a default commit made while a seek commit is unanswered and a newer seek waits sends only the sibling', async () => {
      const captured = using(captureGroup())
      const { queues } = using(trackQueues())
      await open(TWO_PARTITIONS)
      await scenario.otherDone.promise
      const group = captured.group()
      const {
        holds: [accepted],
      } = using(holdCommitAnswers(scenario, ['applied']))
      const older = scenario.seek('0')
      await accepted.arrived
      scenario.consumer.seek({ topic: TOPIC, partition: 0, offset: '6' })

      await group.commitOffsets({}, { auto: true })

      expect(scenario.commits(0)).toEqual(['0'])
      expect(scenario.commits(1)).toEqual(['1'])
      accepted.release()
      expect(await older).toBe('6')
      expect(scenario.commits(0)).toEqual(['0', '6'])
      expect(scenario.committed[0]).toBe('6')
      await disposeAndAudit(group, queues)
    })

    test('a seek whose commit is still queued unsent when its connection closes releases its cell, and a newer seek is then sent', async () => {
      const captured = using(captureGroup())
      const { queues, queued } = using(trackQueues({ queueFirstCommit: true }))
      await open({
        run: { autoCommit: true, autoCommitInterval: 60000, eachBatch: true },
        before: ({ resolveOffset }) => resolveOffset('4'),
      })
      const group = captured.group()
      const older = failureOf(scenario.seek('0'))
      const queue = await queued
      expect(queue.pending.map(({ entry }) => entry.apiName)).toContain('OffsetCommit')
      scenario.consumer.seek({ topic: TOPIC, partition: 0, offset: '7' })
      const newer = scenario.position()
      await new Promise(setImmediate)
      expect(scenario.commits()).toEqual([])
      expect(group.operations.reserved).toBe(1)

      for (const socket of scenario.rig.cluster.sockets()) {
        if (!socket.destroyed) socket.emit('end')
      }

      expect(await older).toBeInstanceOf(Error)
      expect(await newer).toBe('7')
      expect(scenario.commits()).toEqual(['7'])
      expect(scenario.committed[0]).toBe('7')
      await disposeAndAudit(group, queues)
    })
  })

  describe('a seek of a halted partition', () => {
    const SEEKING = {
      run: { autoCommit: true, autoCommitInterval: 60000, eachBatch: true },
      before: ({ resolveOffset }) => resolveOffset('4'),
    }
    const FAILURES = {
      'refused before its write': (group) => {
        const refusal = new Error('refused before the write')
        const seek = jest.spyOn(group.offsetManager, 'seek').mockImplementationOnce(async () => {
          throw refusal
        })
        using({ restore: () => seek.mockRestore() })
        return { commits: [] }
      },
      'rejected by the broker': () => {
        failNextCommit(scenario)
        return { commits: ['0'] }
      },
      'written and left unanswered': () => {
        const answer = scenario.rig.cluster.answerers.get('OffsetCommit')
        let lost = false
        scenario.rig.cluster.answer('OffsetCommit', async (fields, record) => {
          if (lost) return answer(fields, record)
          lost = true
          throw new Error('connection lost after the write')
        })
        return { commits: ['0'] }
      },
    }

    test.each(Object.keys(FAILURES))(
      'keeps the halt, the position and the tracked epoch when its commit was %s',
      async (failure) => {
        const captured = using(captureGroup())
        await open(SEEKING)
        const group = captured.group()
        const key = `${TOPIC}|0`
        expect(await scenario.position()).toBe('5')
        const epoch = group.offsetManager.lastConsumedEpoch(TOPIC, 0)
        const halt = { offset: '5', reason: 'checksum', error: new Error('crc') }
        group.haltedPartitions.set(key, halt)
        const { commits } = FAILURES[failure](group)

        scenario.consumer.seek({ topic: TOPIC, partition: 0, offset: '0' })
        expect(await failureOf(scenario.position())).toBeInstanceOf(Error)

        expect(group.haltedPartitions.get(key)).toBe(halt)
        expect(await scenario.position()).toBe('5')
        expect(group.offsetManager.lastConsumedEpoch(TOPIC, 0)).toBe(epoch)
        expect(scenario.commits()).toEqual(commits)
        expect(scenario.committed[0]).toBe('2')
      }
    )

    test('clears the halt once the broker accepted its commit', async () => {
      const captured = using(captureGroup())
      await open(SEEKING)
      const group = captured.group()
      const key = `${TOPIC}|0`
      group.haltedPartitions.set(key, { offset: '5', reason: 'checksum', error: new Error('crc') })

      expect(await scenario.seek('0')).toBe('0')

      expect(group.haltedPartitions.has(key)).toBe(false)
      expect(scenario.commits()).toEqual(['0'])
      expect(scenario.committed[0]).toBe('0')
    })
  })

  describe('a seek a fetch round applied', () => {
    const SEEKING = {
      run: { autoCommit: true, autoCommitInterval: 60000, eachBatch: true },
      before: ({ resolveOffset }) => resolveOffset('4'),
    }
    /**
     * Admits a seek to 0 without applying it, lets the fetch loop apply it, and refuses that
     * round by an incremental assignment that keeps the partition.
     * @param {ConsumerGroup} group
     * @param {Promise<void>} reached resolves once the seek's commit got as far as the test holds it
     */
    const applyByFetchThenRetain = async (group, reached) => {
      const applying = jest.spyOn(group, 'seekOffsets')
      using({ restore: () => applying.mockRestore() })
      scenario.consumer.seek({ topic: TOPIC, partition: 0, offset: '0' })
      await scenario.release()
      await reached
      expect(applying.mock.calls.some(([, round]) => round instanceof FetchPreparation)).toBe(true)
      await group.rebalanceCoordinator.runner.waitForHandlers()
      await group.applyRebalanceResult({ assignment: { [TOPIC]: [0] } }, { rebuildOffsets: false })
    }

    test('is not sent again after the broker accepted its commit and the round was refused', async () => {
      const captured = using(captureGroup())
      await open(SEEKING)
      const {
        holds: [accepted],
      } = using(holdCommitAnswers(scenario, ['applied']))
      await applyByFetchThenRetain(captured.group(), accepted.arrived)

      accepted.release()

      expect(await scenario.nextDelivery()).toEqual(['value-0'])
      expect(await scenario.position()).toBe('0')
      expect(scenario.commits()).toEqual(['0'])
      expect(scenario.committed[0]).toBe('0')
      expect(captured.group().isSeekGated(TOPIC, 0)).toBe(false)
      expect(scenario.errors).toEqual([])
    })

    test('keeps a deliberate commit made after the broker accepted its commit and the round was refused', async () => {
      const captured = using(captureGroup())
      await open(SEEKING)
      const {
        holds: [accepted],
      } = using(holdCommitAnswers(scenario, ['applied']))
      await applyByFetchThenRetain(captured.group(), accepted.arrived)
      await scenario.consumer.commitOffsets([{ topic: TOPIC, partition: 0, offset: '8' }])

      accepted.release()

      await until(() => !captured.group().isSeekGated(TOPIC, 0), 'the seek settled')
      expect(await scenario.position()).toBe('8')
      expect(scenario.commits()).toEqual(['0', '8'])
      expect(scenario.committed[0]).toBe('8')
      expect(scenario.errors).toEqual([])
    })

    test('clears the halt of its partition when the broker accepted its commit, even though the round was refused', async () => {
      const captured = using(captureGroup())
      await open(SEEKING)
      const group = captured.group()
      const key = `${TOPIC}|0`
      const refusal = new Error('the round was refused')
      const seek = jest
        .spyOn(group.offsetManager, 'seek')
        .mockImplementationOnce(async (entry, preparation, attempt) => {
          attempt.accepted = true
          attempt.settle()
          throw refusal
        })
      using({ restore: () => seek.mockRestore() })
      group.haltedPartitions.set(key, { offset: '2', reason: 'checksum', error: new Error('x') })
      scenario.consumer.seek({ topic: TOPIC, partition: 0, offset: '0' })

      await expect(group.seekOffsets([{ topic: TOPIC, partitions: [0] }])).rejects.toBe(refusal)

      expect(group.haltedPartitions.has(key)).toBe(false)
      expect(group.isSeekGated(TOPIC, 0)).toBe(false)
      expect(group.hasSeekOffset({ topic: TOPIC, partition: 0 })).toBe(false)
    })

    test('is not sent again by the next round when the round was refused before its commit was written, and the partition continues from its position before the seek', async () => {
      const captured = using(captureGroup())
      const held = using(holdNextCommitCall())
      await open(SEEKING)
      await applyByFetchThenRetain(captured.group(), held.reached)

      held.release()

      expect(await scenario.nextDelivery()).toEqual(['value-5'])
      expect(await scenario.position()).toBe('5')
      expect(captured.group().hasSeekOffset({ topic: TOPIC, partition: 0 })).toBe(false)
      expect(scenario.commits()).toEqual([])
      expect(scenario.committed[0]).toBe('2')
      expect(scenario.errors).toEqual([])
    })
  })

  describe('a seek whose commit was written and got no answer', () => {
    test('is not sent again, and the live position stays where it was although the broker stored the seek', async () => {
      await open({
        run: { autoCommit: true, autoCommitInterval: 60000, eachBatch: true },
        before: ({ resolveOffset }) => resolveOffset('4'),
      })
      const answer = scenario.rig.cluster.answerers.get('OffsetCommit')
      let lost = false
      scenario.rig.cluster.answer('OffsetCommit', async (fields, record) => {
        const result = await answer(fields, record)
        if (lost) return result
        lost = true
        throw new Error('connection lost after the write')
      })

      expect((await failureOf(scenario.seek('0'))).name).toBe('KafkaJSConnectionError')

      expect(await scenario.position()).toBe('5')
      await scenario.release()
      expect(await scenario.nextDelivery()).toEqual(['value-5'])
      expect(scenario.commits()).toEqual(['0'])
      expect(scenario.committed[0]).toBe('0')
      expect(scenario.errors).toEqual([])
    })

    /** Drops the connection of the next OffsetCommit before the broker applies it. */
    const loseNextCommitUnapplied = () => {
      const answer = scenario.rig.cluster.answerers.get('OffsetCommit')
      let lost = false
      scenario.rig.cluster.answer('OffsetCommit', (fields, record) => {
        if (lost) return answer(fields, record)
        lost = true
        throw new Error('connection lost before the broker applied it')
      })
    }
    const SEEKING = {
      run: { autoCommit: true, autoCommitInterval: 60000, eachBatch: true },
      before: ({ resolveOffset }) => resolveOffset('4'),
    }

    test('is not taken as applied by position() when the broker did not apply it: the position and the next delivery stay where they were', async () => {
      await open(SEEKING)
      loseNextCommitUnapplied()

      expect((await failureOf(scenario.seek('0'))).name).toBe('KafkaJSConnectionError')

      expect(await scenario.position()).toBe('5')
      await scenario.release()
      expect(await scenario.nextDelivery()).toEqual(['value-5'])
      expect(scenario.commits()).toEqual(['0'])
      expect(scenario.committed[0]).toBe('2')
      expect(scenario.errors).toEqual([])
    })

    test('is not taken as applied by a fetch round when the broker did not apply it: the next delivery stays where it was', async () => {
      const captured = using(captureGroup())
      await open(SEEKING)
      const applying = jest.spyOn(captured.group(), 'seekOffsets')
      using({ restore: () => applying.mockRestore() })
      loseNextCommitUnapplied()

      scenario.consumer.seek({ topic: TOPIC, partition: 0, offset: '0' })
      await scenario.release()

      expect(await scenario.nextDelivery()).toEqual(['value-5'])
      expect(applying.mock.calls.some(([, round]) => round instanceof FetchPreparation)).toBe(true)
      expect(await scenario.position()).toBe('5')
      expect(scenario.commits()).toEqual(['0'])
      expect(scenario.committed[0]).toBe('2')
      expect(captured.group().hasSeekGates()).toBe(false)
      expect(scenario.errors).toEqual([])
    })
  })

  describe('a default commit held at its write after a seek', () => {
    const SIBLING = {
      partitions: 2,
      run: { ...MANUAL, partitionsConsumedConcurrently: 2, eachBatch: true },
      before: ({ resolveOffset }) => resolveOffset('4'),
    }

    test('completes without a frame when every selected cell is sought before the write', async () => {
      const captured = using(captureGroup())
      await open(SIBLING)
      await scenario.otherDone.promise
      const {
        holds: [first, second],
      } = using(holdCommitWrites(2))
      const committing = captured.group().commitOffsets({}, { auto: true })
      await first.reached
      scenario.consumer.seek({ topic: TOPIC, partition: 0, offset: '0' })

      first.release()
      await second.reached
      scenario.consumer.seek({ topic: TOPIC, partition: 1, offset: '0' })
      second.release()
      await committing

      expect(scenario.rig.frames('OffsetCommit')).toHaveLength(0)
      expect(scenario.committed).toEqual({ 0: '2', 1: '0' })
      expect(scenario.errors).toEqual([])
    })

    test('still writes an unsought cell at its selected offset after a newer deliberate commit of that cell, as kafkajs 2.2.4 does', async () => {
      const captured = using(captureGroup())
      await open(SIBLING)
      await scenario.otherDone.promise
      const group = captured.group()
      const write = using(holdNextCommitWrite())
      const committing = group.commitOffsets({}, { auto: true })
      await write.reached
      expect(await scenario.seek('0')).toBe('0')
      await scenario.consumer.commitOffsets([{ topic: TOPIC, partition: 1, offset: '9' }])

      write.release()
      await committing
      await group.commitOffsets({}, { auto: true })

      expect(frameCells(scenario.rig)).toEqual([['1:9'], ['1:1'], ['0:0', '1:1']])
      expect(scenario.committed).toEqual({ 0: '0', 1: '1' })
      expect(group.offsetManager.committedOffsets()[TOPIC]).toEqual({ 0: '0', 1: '1' })
      expect(scenario.errors).toEqual([])
    })
  })

  describe('a fetch round prepared before a seek another caller applied', () => {
    test('delivers at the sought offset from the request it builds after the seek, fetched once', async () => {
      const captured = using(captureGroup())
      await open({ run: { ...MANUAL, eachBatch: true } })
      const group = captured.group()
      const reached = gate()
      const opened = gate()
      const validate = group.validatePositions
      let held = false
      const spy = jest.spyOn(group, 'validatePositions').mockImplementation(async function (
        ...args
      ) {
        if (!held && args[1] instanceof FetchPreparation) {
          held = true
          reached.resolve()
          await opened.promise
        }
        return validate.apply(this, args)
      })
      using({
        restore: () => {
          opened.resolve()
          spy.mockRestore()
        },
      })
      await scenario.release()
      await reached.promise
      expect(await scenario.seek('0')).toBe('0')

      opened.resolve()

      expect(await scenario.nextDelivery()).toEqual(['value-0'])
      expect(fetchOffsetsOf(scenario.rig, 0).filter((offset) => offset === '0')).toEqual(['0'])
      expect(scenario.errors).toEqual([])
    })
  })

  describe('a seek a START_BATCH_PROCESS listener admits', () => {
    test('keeps the older batch from eachBatch and pairs START with END', async () => {
      await open({ run: { ...MANUAL, eachBatch: true } })
      const starts = []
      const ends = []
      let sought = false
      scenario.consumer.on(scenario.consumer.events.START_BATCH_PROCESS, ({ payload }) => {
        starts.push(payload.firstOffset)
        if (sought) return
        sought = true
        scenario.consumer.seek({ topic: TOPIC, partition: 0, offset: '0' })
      })
      scenario.consumer.on(scenario.consumer.events.END_BATCH_PROCESS, ({ payload }) =>
        ends.push(payload.firstOffset)
      )

      await scenario.release()

      expect(await scenario.nextDelivery()).toEqual(['value-0'])
      expect(scenario.deliveries).toEqual([BEFORE, ['value-0']])
      expect(starts).toEqual(['2', '0'])
      expect(ends).toEqual(['2', '2'])
      expect(scenario.commits()).toEqual([])
      expect(scenario.errors).toEqual([])
    })
  })

  describe('a seek with a partition that is not a number', () => {
    test('throws before any state changes, and fetching goes on', async () => {
      const captured = using(captureGroup())
      await open({
        run: { ...MANUAL, eachBatch: true },
        before: ({ resolveOffset }) => resolveOffset('4'),
      })
      const group = captured.group()
      const unprintable = {
        toString() {
          throw new Error('not printable')
        },
      }
      const inputs = [
        ['0', '0'],
        [null, 'null'],
        ['', ''],
        [undefined, 'undefined'],
        [true, 'true'],
        [NaN, 'NaN'],
        [1.5, '1.5'],
        [-1, '-1'],
        [Infinity, 'Infinity'],
        [2 ** 31, String(2 ** 31)],
        [Symbol('p'), 'Symbol(p)'],
        [unprintable, 'object'],
      ]

      for (const [partition, printed] of inputs) {
        let thrown = null
        try {
          scenario.consumer.seek({ topic: TOPIC, partition, offset: '0' })
        } catch (error) {
          thrown = error
        }
        expect(thrown).toBeInstanceOf(KafkaJSNonRetriableError)
        expect(thrown.message).toBe(`Invalid partition, expected a number received ${printed}`)
      }

      expect(scenario.old.isStale()).toBe(false)
      expect(group.hasSeekGates()).toBe(false)
      expect(group.seekOffset.size).toBe(0)
      expect(await scenario.position()).toBe('5')
      await scenario.release()
      expect(await scenario.nextDelivery()).toEqual(['value-5'])
      expect(scenario.errors).toEqual([])
    })

    test('throws for a manual assignment before run', async () => {
      const rig = await CloseRig.open({ protocol })
      const consumer = rig.consumer()
      try {
        await consumer.connect()
        await consumer.assign([{ topic: TOPIC, partitions: [0] }])

        for (const partition of ['0', -1, 1.5]) {
          const seeking = () => consumer.seek({ topic: TOPIC, partition, offset: '1' })
          expect(seeking).toThrow(KafkaJSNonRetriableError)
          expect(seeking).toThrow(`Invalid partition, expected a number received ${partition}`)
        }
        expect(() => consumer.seek({ topic: TOPIC, partition: 0, offset: '1' })).not.toThrow()
      } finally {
        await rig.dispose()
      }
    })
  })

  test("a string-keyed onWrite option of a broker's offsetCommit is not the commit's write hook", async () => {
    const captured = using(captureGroup())
    await open({ run: { ...MANUAL, eachBatch: true } })
    const broker = await captured.group().cluster.findGroupCoordinator({ groupId: GROUP })
    const onWrite = jest.fn()
    const write = jest.spyOn(RequestQueue.prototype, 'sendSocketRequest')
    using({ restore: () => write.mockRestore() })
    const { groupId, generationId, memberId } = captured.group()

    await broker.offsetCommit(
      {
        groupId,
        groupGenerationId: generationId,
        memberId,
        topics: [
          {
            topic: TOPIC,
            topicId: captured.group().cluster.topicId(TOPIC),
            partitions: [{ partition: 0, offset: '3' }],
          },
        ],
      },
      { onWrite }
    )

    const commits = write.mock.calls.filter(([request]) => request.entry.apiName === 'OffsetCommit')
    expect(commits.map(([request]) => request.onWrite)).toEqual([null])
    expect(onWrite).not.toHaveBeenCalled()
    expect(scenario.committed[0]).toBe('3')
  })

  describe('an answer requested before a seek', () => {
    test('a truncation answer does not reposition the partition over a newer seek, and no validation is asked while it applies', async () => {
      const captured = using(captureGroup())
      await open({
        run: { autoCommit: true, autoCommitInterval: 60000, eachBatch: true },
        before: ({ resolveOffset }) => resolveOffset('4'),
      })
      const group = captured.group()
      const asked = gate()
      const reply = gate()
      scenario.rig.cluster.answer('OffsetForLeaderEpoch', async ({ topics }) => {
        asked.resolve()
        await reply.promise
        return {
          partitions: topics[0].partitions.map(({ partition }) => ({
            partition,
            errorCode: 0,
            leaderEpoch: 5,
            endOffset: '1',
          })),
        }
      })
      const epoch = jest.spyOn(group.cluster, 'leaderEpoch')
      let validations = 0
      const validate = group.validatePositions
      const validating = jest.spyOn(group, 'validatePositions').mockImplementation(async function (
        ...args
      ) {
        try {
          return await validate.apply(this, args)
        } finally {
          validations++
        }
      })
      using({
        restore: () => {
          reply.resolve()
          epoch.mockRestore()
          validating.mockRestore()
        },
      })
      expect(await scenario.position()).toBe('5')
      expect(group.offsetManager.lastConsumedEpoch(TOPIC, 0)).toBe(5)
      scenario.consumer.pause([{ topic: TOPIC, partitions: [0] }])
      await scenario.release()
      epoch.mockReturnValue(6)
      await asked.promise
      const {
        holds: [accepted],
      } = using(holdCommitAnswers(scenario, ['applied']))
      scenario.consumer.seek({ topic: TOPIC, partition: 0, offset: '0' })
      const newer = scenario.position()
      await accepted.arrived

      reply.resolve()
      await until(() => !group.positionValidator.isAwaiting(TOPIC, 0), 'the validation answered')
      const answered = validations
      await until(() => validations >= answered + 2, 'two more rounds while the seek applies')
      expect(group.positionValidator.isAwaiting(TOPIC, 0)).toBe(false)
      accepted.release()

      expect(await newer).toBe('0')
      expect(await scenario.position()).toBe('0')
      expect(scenario.commits()).toEqual(['0'])
      expect(scenario.committed[0]).toBe('0')
      expect(scenario.errors).toEqual([])
    })

    test.each([true, false])(
      'an out-of-range answer does not reset the partition over a seek applied after the fetch was built (autoCommit %s)',
      async (autoCommit) => {
        await open({
          run: autoCommit
            ? { autoCommit: true, autoCommitInterval: 60000, eachBatch: true }
            : { autoCommit: false, eachBatch: true },
          before: ({ resolveOffset }) => resolveOffset('4'),
        })
        const reached = gate()
        const released = gate()
        const fetch = scenario.rig.cluster.answerers.get('Fetch')
        let held = false
        scenario.rig.cluster.answer('Fetch', async (fields, record) => {
          const p0 = fields.topics[0].partitions.find(({ partition }) => partition === 0)
          if (held || !p0 || String(p0.fetchOffset) !== '5') return fetch(fields, record)
          held = true
          reached.resolve()
          await released.promise
          return {
            partitions: fields.topics[0].partitions.map(({ partition }) => ({
              partition,
              errorCode: partition === 0 ? 1 : 0,
              records: false,
              abortedTransactions: [],
            })),
          }
        })
        using({ restore: () => released.resolve() })
        await scenario.release()
        await reached.promise
        expect(await scenario.seek('6')).toBe('6')

        released.resolve()

        expect(await scenario.nextDelivery()).toEqual(['value-6'])
        expect(await scenario.position()).toBe('6')
        expect(scenario.commits()).toEqual(autoCommit ? ['6'] : [])
        expect(scenario.errors).toEqual([])
      }
    )

    test.each([true, false])(
      'an out-of-range answer does not reset the partition while a seek admitted after the fetch was built is still queued (autoCommit %s)',
      async (autoCommit) => {
        await open({
          run: autoCommit
            ? { autoCommit: true, autoCommitInterval: 60000, eachBatch: true }
            : { autoCommit: false, eachBatch: true },
          before: ({ resolveOffset }) => resolveOffset('4'),
        })
        const fetch = using(outOfRangeAt(scenario.rig, '5', true))
        await scenario.release()
        await fetch.reached
        scenario.consumer.seek({ topic: TOPIC, partition: 0, offset: '6' })

        fetch.release()

        expect(await scenario.nextDelivery()).toEqual(['value-6'])
        expect(await scenario.position()).toBe('6')
        expect(scenario.commits()).toEqual(autoCommit ? ['6'] : [])
        expect(scenario.committed[0]).toBe(autoCommit ? '6' : '2')
        expect(scenario.errors).toEqual([])
      }
    )

    test('a by-duration reset a seek overtakes before its commit is written is dropped, and fetching goes on at the seek', async () => {
      const captured = using(captureGroup())
      await open({
        run: { autoCommit: true, autoCommitInterval: 60000, eachBatch: true },
        before: ({ resolveOffset }) => resolveOffset('4'),
      })
      captured.group().topicConfigurations[TOPIC].resetPolicy = {
        kind: 'by_duration',
        durationMs: 3600000,
      }
      const crashes = []
      scenario.consumer.on(scenario.consumer.events.CRASH, ({ payload }) =>
        crashes.push(payload.error)
      )
      using(outOfRangeAt(scenario.rig, '5', false))
      const reached = gate()
      const opened = gate()
      const send = OffsetManager.prototype.sendOffsetCommit
      const spy = jest
        .spyOn(OffsetManager.prototype, 'sendOffsetCommit')
        .mockImplementationOnce(async function (...args) {
          reached.resolve()
          await opened.promise
          return send.apply(this, args)
        })
      using({
        restore: () => {
          opened.resolve()
          spy.mockRestore()
        },
      })
      await scenario.release()
      await reached.promise
      scenario.consumer.seek({ topic: TOPIC, partition: 0, offset: '6' })

      opened.resolve()

      expect(await scenario.nextDelivery()).toEqual(['value-6'])
      expect(await scenario.position()).toBe('6')
      expect(scenario.commits()).toEqual(['6'])
      expect(scenario.committed[0]).toBe('6')
      expect(crashes).toEqual([])
      expect(scenario.errors).toEqual([])
    })

    test('an out-of-range answer to a current request still resets the partition', async () => {
      await open({
        run: { autoCommit: true, autoCommitInterval: 60000, eachBatch: true },
        before: ({ resolveOffset }) => resolveOffset('4'),
      })
      const fetch = scenario.rig.cluster.answerers.get('Fetch')
      let answered = false
      scenario.rig.cluster.answer('Fetch', async (fields, record) => {
        const p0 = fields.topics[0].partitions.find(({ partition }) => partition === 0)
        if (answered || !p0 || String(p0.fetchOffset) !== '5') return fetch(fields, record)
        answered = true
        return {
          partitions: fields.topics[0].partitions.map(({ partition }) => ({
            partition,
            errorCode: partition === 0 ? 1 : 0,
            records: false,
            abortedTransactions: [],
          })),
        }
      })

      await scenario.release()

      expect(await scenario.nextDelivery()).toEqual(['value-0'])
      expect(scenario.commits()).toEqual(['-2'])
      expect(scenario.errors).toEqual([])
    })
  })

  describe('a seek made while its partition is not assigned', () => {
    test('a seek made right after run, before the first assignment, is applied before the first delivery', async () => {
      const log = {}
      for (let offset = 0; offset <= 7; offset++) log[`0|${offset}`] = await batchAt(offset)
      const committed = { 0: '2' }
      const rig = await CloseRig.open({ protocol, last: -1, log, committed })
      const commit = rig.cluster.answerers.get('OffsetCommit')
      rig.cluster.answer('OffsetCommit', (fields, record) => {
        for (const entry of fields.topics)
          for (const cell of entry.partitions) committed[cell.partition] = String(cell.offset)
        return commit(fields, record)
      })
      try {
        const consumer = rig.consumer()
        const seen = []
        await consumer.connect()
        await consumer.subscribe({ topic: TOPIC })
        const running = consumer.run({
          eachMessage: async ({ message }) => {
            seen.push(message.value.toString())
          },
        })
        consumer.seek({ topic: TOPIC, partition: 0, offset: '5' })
        await running
        await until(() => seen.length >= 1, 'the first delivery')

        expect(seen[0]).toBe('value-5')
        expect(fetchOffsetsOf(rig, 0)[0]).toBe('5')
      } finally {
        await rig.dispose()
      }
    })

    test('a seek made while its partition is revoked is kept across an assignment without it and applied once the partition is assigned again', async () => {
      const captured = using(captureGroup())
      await open({ run: { ...MANUAL, eachBatch: true }, handOff: true })
      await scenario.nextDelivery()
      const group = captured.group()
      await group.applyRebalanceResult({ assignment: {} }, { rebuildOffsets: false })

      scenario.consumer.seek({ topic: TOPIC, partition: 0, offset: '7' })
      scenario.consumer.seek({ topic: TOPIC, partition: 0, offset: '5' })
      await group.applyRebalanceResult({ assignment: { [TOPIC]: [] } }, { rebuildOffsets: false })
      await group.applyRebalanceResult({ assignment: { [TOPIC]: [0] } }, { rebuildOffsets: false })

      expect(await scenario.position()).toBe('5')
      expect(group.hasSeekOffset({ topic: TOPIC, partition: 0 })).toBe(false)
      expect(scenario.errors).toEqual([])
    })

    test('a seek queued before its partition is revoked is dropped with the revocation', async () => {
      const captured = using(captureGroup())
      await open({ run: { ...MANUAL, eachBatch: true }, handOff: true })
      await scenario.nextDelivery()
      const group = captured.group()
      scenario.consumer.seek({ topic: TOPIC, partition: 0, offset: '5' })

      await group.applyRebalanceResult({ assignment: {} }, { rebuildOffsets: false })
      await group.applyRebalanceResult({ assignment: { [TOPIC]: [0] } }, { rebuildOffsets: false })

      expect(group.hasSeekOffset({ topic: TOPIC, partition: 0 })).toBe(false)
      expect(await scenario.position()).toBe('2')
      expect(scenario.commits()).toEqual([])
    })
  })

  describe('a seek left open by a later commit', () => {
    const SEEKING = {
      run: { autoCommit: true, autoCommitInterval: 60000, eachBatch: true },
      before: ({ resolveOffset }) => resolveOffset('4'),
    }

    test('a failed seek is not sent again over a deliberate commit that succeeded after its failure', async () => {
      const captured = using(captureGroup())
      await open(SEEKING)
      failNextCommit(scenario)
      expect((await failureOf(scenario.seek('0'))).type).toBe('REQUEST_TIMED_OUT')
      expect(captured.group().isSeekGated(TOPIC, 0)).toBe(false)

      await scenario.consumer.commitOffsets([{ topic: TOPIC, partition: 0, offset: '7' }])

      expect(await scenario.position()).toBe('5')
      expect(scenario.commits()).toEqual(['0', '7'])
      expect(scenario.committed[0]).toBe('7')
      expect(captured.group().isSeekGated(TOPIC, 0)).toBe(false)
      await scenario.release()
      expect(await scenario.nextDelivery()).toEqual(['value-5'])
      expect(scenario.errors).toEqual([])
    })

    test('a COMMIT_OFFSETS listener that changes a reported offset in place changes nothing recorded', async () => {
      const captured = using(captureGroup())
      await open(SEEKING)
      expect(await scenario.seek('5')).toBe('5')
      scenario.consumer.on(scenario.consumer.events.COMMIT_OFFSETS, ({ payload }) => {
        payload.topics[0].partitions[0].offset = '7'
      })

      await scenario.consumer.commitOffsets([{ topic: TOPIC, partition: 0, offset: '5' }])

      expect(scenario.commits()).toEqual(['5', '5'])
      expect(scenario.committed[0]).toBe('5')
      expect(String(captured.group().offsetManager.committedOffsets()[TOPIC][0])).toBe('5')
      await scenario.release()
      expect(await scenario.nextDelivery()).toEqual(['value-5'])
      expect(scenario.errors).toEqual([])
    })

    test('an accepted seek clears the halt of its partition even when a newer deliberate commit owns the offset', async () => {
      const captured = using(captureGroup())
      await open(SEEKING)
      const group = captured.group()
      const key = `${TOPIC}|0`
      group.haltedPartitions.set(key, { offset: '2', reason: 'checksum', error: new Error('crc') })
      const {
        holds: [accepted],
      } = using(holdCommitAnswers(scenario, ['applied']))
      const applying = scenario.seek('5')
      await accepted.arrived
      await scenario.consumer.commitOffsets([{ topic: TOPIC, partition: 0, offset: '7' }])

      accepted.release()

      expect(await applying).toBe('7')
      expect(scenario.commits()).toEqual(['5', '7'])
      expect(scenario.committed[0]).toBe('7')
      expect(group.haltedPartitions.has(key)).toBe(false)
      await scenario.release()
      expect(await scenario.nextDelivery()).toEqual(['value-7'])
      expect(scenario.errors).toEqual([])
    })

    test('a seek refused before its write is dropped with its error, and the partition continues from its position before the seek when the deliberate commit made while it was pending was never written', async () => {
      const captured = using(captureGroup())
      await open(SEEKING)
      const group = captured.group()
      scenario.consumer.seek({ topic: TOPIC, partition: 0, offset: '1' })
      const { holds, restore: release } = using(holdCommitWrites(2))
      const committing = failureOf(
        scenario.consumer.commitOffsets([{ topic: TOPIC, partition: 0, offset: '3' }])
      )
      await holds[0].reached
      const applying = failureOf(scenario.position())
      await holds[1].reached
      await group.applyRebalanceResult({ assignment: { [TOPIC]: [0] } }, { rebuildOffsets: false })

      release()

      expect(await committing).toBeInstanceOf(KafkaJSCommitFailedError)
      expect(await applying).toBeInstanceOf(Error)
      expect(group.hasSeekOffset({ topic: TOPIC, partition: 0 })).toBe(false)
      expect(group.isSeekGated(TOPIC, 0)).toBe(false)
      expect(scenario.commits()).toEqual([])
      expect(scenario.committed[0]).toBe('2')
      expect(await scenario.position()).toBe('5')
      await scenario.release()
      expect(await scenario.nextDelivery()).toEqual(['value-5'])
      expect(scenario.commits()).toEqual([])
      expect(scenario.errors).toEqual([])
    })
  })
})
