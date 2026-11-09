const { KafkaJSDeliveryTimeout } = require('../../errors')
const { ConsumerOperation } = require('../groupProtocol/interface')
const { createGroupDescriptions } = require('../../admin/groupDescriptions')
const Connection = require('../../network/connection')
const Encoder = require('../../protocol/encoder')
const apiKeys = require('../../protocol/requests/apiKeys')
const {
  newLogger,
  createFakeSocketFactory,
  respondWithFrame,
  readRequestHeader,
} = require('testHelpers')
const { CloseRig, TOPIC, gate, pause, until } = require('./fixtures/closeRig')
const { pump } = require('./fixtures/lifecycleCase')
const { useSharedFakeClock } = require('./fixtures/sharedFakeClock')
const {
  TOPIC: HARNESS_TOPIC,
  NEW_ID,
  createScheduledOffsetManager,
  groupOver,
  listOffsetsAnswer,
} = require('../offsetManager/__tests__/fixtures/scheduledOffsetManager')

const BOUND_MS = 500
const CELL = { topic: TOPIC, partition: 0 }
const bounded = { maxTotalTime: BOUND_MS }

/**
 * @template T
 * @param {Promise<T>} promise work driven by fake timers and real socket turns
 * @param {string} label the completion being awaited
 * @returns {Promise<T>} the work's result
 */
const drive = async (promise, label) => {
  let outcome
  promise.then(
    (value) => {
      outcome = { value }
    },
    (error) => {
      outcome = { error }
    }
  )
  await pump(() => outcome !== undefined, label)
  if ('error' in outcome) throw outcome.error
  return outcome.value
}

describe.each([true, false])(
  'a public API call under request timeout enforcement %p',
  (enforce) => {
    /** @type {CloseRig} */
    let rig
    let fakeClock = false
    /** @param {object} [options] rig options @returns {Promise<void>} a fresh rig, read as `rig` */
    const open = async (options) => {
      rig = await CloseRig.open({ enforce, retry: bounded, ...options })
    }
    afterEach(async () => {
      try {
        if (fakeClock) await drive(rig.dispose(), 'the rig to dispose')
        else await rig.dispose()
      } finally {
        if (fakeClock) {
          jest.useRealTimers()
          jest.restoreAllMocks()
          fakeClock = false
        }
      }
    })

    describe('whose first coordinator lookup is held', () => {
      test('commitOffsets settles at its deadline and sends nothing once the lookup answers', async () => {
        await open()
        const lookup = rig.lookup()
        const { consumer } = await rig.started({ run: { autoCommit: false } })
        const committed = []
        consumer.on(consumer.events.COMMIT_OFFSETS, ({ payload }) => committed.push(payload))
        lookup.makeStale()
        const held = lookup.hold()
        const startedAt = Date.now()
        const settled = consumer
          .commitOffsets([{ topic: TOPIC, partition: 0, offset: '2' }])
          .catch((error) => error)
        await held.entered
        expect(await settled).toBeInstanceOf(KafkaJSDeliveryTimeout)
        expect(Date.now() - startedAt).toBeGreaterThanOrEqual(BOUND_MS - 50)
        expect(Date.now() - startedAt).toBeLessThan(BOUND_MS + 1500)
        held.release()
        await pause(150)
        expect(rig.frames('OffsetCommit')).toHaveLength(0)
        expect(committed).toEqual([])
      })

      test('position settles at its deadline and applies nothing once the lookup answers', async () => {
        await open()
        const lookup = rig.lookup()
        const { consumer } = await rig.started()
        const before = await consumer.position(CELL)
        const committed = []
        consumer.on(consumer.events.COMMIT_OFFSETS, ({ payload }) => committed.push(payload))
        const commitsBefore = rig.frames('OffsetCommit').length
        lookup.makeStale()
        const held = lookup.hold()
        consumer.seek({ ...CELL, offset: '1' })
        const settled = consumer.position(CELL).catch((error) => error)
        await held.entered
        expect(await settled).toBeInstanceOf(KafkaJSDeliveryTimeout)
        held.release()
        await pause(150)
        expect(rig.frames('OffsetCommit')).toHaveLength(commitsBefore)
        expect(committed).toEqual([])
        expect(await consumer.position(CELL)).toBe(before)
      })

      test('committed settles at its deadline and reads nothing once the lookup answers', async () => {
        await open()
        const consumer = rig.consumer()
        await consumer.connect()
        const held = rig.lookup().hold()
        const settled = consumer.committed([CELL]).catch((error) => error)
        await held.entered
        expect(await settled).toBeInstanceOf(KafkaJSDeliveryTimeout)
        held.release()
        await pause(150)
        expect(rig.frames('OffsetFetch')).toHaveLength(0)
      })
    })

    describe('whose one deadline spans the lookup, its retry and the response', () => {
      /**
       * @param {number} spent ms the early stages take in all
       * @param {number} startedAt when the call began
       * @returns {void}
       */
      const expectAtOriginalDeadline = (spent, startedAt) => {
        const elapsed = Date.now() - startedAt
        expect(elapsed).toBeGreaterThanOrEqual(BOUND_MS - 50)
        expect(elapsed).toBeLessThan(BOUND_MS + spent * 0.75)
      }

      test('committed expires at the entry deadline, not a new duration from the final stage', async () => {
        await open()
        const consumer = rig.consumer()
        await consumer.connect()
        const lookup = rig.lookup()
        lookup.failAfter(150)
        lookup.answerAfter(150)
        const response = rig.hold('OffsetFetch')
        const startedAt = Date.now()
        await expect(consumer.committed([CELL])).rejects.toBeInstanceOf(KafkaJSDeliveryTimeout)
        expectAtOriginalDeadline(300, startedAt)
        expect(response.hits).toBe(1)
      })

      test('commitOffsets expires at the entry deadline, not a new duration from the final stage', async () => {
        fakeClock = true
        useSharedFakeClock()
        await open()
        const lookup = rig.lookup()
        const { consumer } = await drive(
          rig.started({ run: { autoCommit: false } }),
          'the consumer to start'
        )
        lookup.makeStale()
        lookup.failAfter(150)
        lookup.answerAfter(150)
        const response = rig.hold('OffsetCommit')
        const startedAt = Date.now()
        let settled = false
        const committing = consumer
          .commitOffsets([{ topic: TOPIC, partition: 0, offset: '2' }])
          .then(
            () => {
              settled = true
            },
            (error) => {
              settled = true
              return error
            }
          )
        await pump(() => response.hits === 1 || settled, 'the held OffsetCommit response')
        expect(response.hits).toBe(1)
        expect(Date.now() - startedAt).toBeLessThan(BOUND_MS)
        await jest.advanceTimersByTimeAsync(BOUND_MS - 1 - (Date.now() - startedAt))
        expect(settled).toBe(false)
        await jest.advanceTimersByTimeAsync(1)
        expect(await committing).toBeInstanceOf(KafkaJSDeliveryTimeout)
        expectAtOriginalDeadline(300, startedAt)
        expect(Date.now() - startedAt).toBe(BOUND_MS)
        expect(response.hits).toBe(1)
      })

      test('an unbounded call keeps waiting for the same held response', async () => {
        await open({ retry: { maxTotalTime: undefined } })
        const consumer = rig.consumer()
        await consumer.connect()
        const lookup = rig.lookup()
        lookup.failAfter(150)
        lookup.answerAfter(150)
        const response = rig.hold('OffsetFetch')
        let settled = false
        const reading = consumer.committed([CELL]).then((rows) => {
          settled = true
          return rows
        })
        await until(() => response.hits === 1, 'the held response')
        await pause(BOUND_MS + 100)
        expect(settled).toBe(false)
        response.release()
        await expect(reading).resolves.toEqual([{ ...CELL, offset: null, metadata: null }])
      })
    })
  }
)

describe('describeGroup under one operation spanning its stages', () => {
  const unknown = { groups: [{ groupId: 'g', errorCode: 69, errorMessage: 'unknown' }] }
  const classic = {
    groups: [
      { groupId: 'g', errorCode: 0, members: [], state: 'Empty', protocolType: '', protocol: '' },
    ],
  }
  /**
   * Stubs whose lookups and first describe each spend `stageMs`, falling back to a classic
   * describe that is held.
   * @param {number} stageMs
   * @returns {{ cluster: object, coordinator: object, response: ReturnType<typeof gate> }}
   */
  const stages = (stageMs) => {
    const response = gate()
    const coordinator = {
      nodeId: 0,
      consumerGroupDescribe: jest.fn(async () => {
        await pause(stageMs)
        return unknown
      }),
      describeGroups: jest.fn(() => response.promise),
    }
    const cluster = {
      findGroupCoordinator: jest.fn(async () => {
        await pause(stageMs)
        return coordinator
      }),
      refreshMetadata: jest.fn(),
    }
    return { cluster, coordinator, response }
  }
  const describer = (cluster) =>
    createGroupDescriptions({
      cluster,
      retry: { retries: 2, initialRetryTime: 5, maxRetryTime: 10 },
      logger: { namespace: () => ({ debug: () => {}, error: () => {} }) },
    })

  test('expires at the original deadline while the fallback response is held', async () => {
    const { cluster, coordinator, response } = stages(100)
    const operation = new ConsumerOperation({ deadlineAt: performance.now() + BOUND_MS })
    const startedAt = Date.now()
    await expect(describer(cluster).describe(['g'], { operation })).rejects.toBeInstanceOf(
      KafkaJSDeliveryTimeout
    )
    const elapsed = Date.now() - startedAt
    expect(elapsed).toBeGreaterThanOrEqual(BOUND_MS - 50)
    expect(elapsed).toBeLessThan(BOUND_MS + 200)
    expect(coordinator.describeGroups).toHaveBeenCalledTimes(1)
    response.resolve(classic)
    operation.finish()
  })

  test('an ordinary admin call waits for the same held response', async () => {
    const { cluster, response } = stages(100)
    let settled = false
    const described = describer(cluster)
      .describe(['g'])
      .then((result) => {
        settled = true
        return result
      })
    await pause(BOUND_MS + 100)
    expect(settled).toBe(false)
    response.resolve(classic)
    expect((await described).groups).toHaveLength(1)
  })
})

describe('position over a real offset manager and ledger', () => {
  const KEY = `${HARNESS_TOPIC}|0`
  const POSITION_CELL = { topic: HARNESS_TOPIC, partition: 0 }

  /**
   * One partition with no committed offset, so its first position is a ListOffsets read, and a
   * real group over the real manager, ledger and reset obligations.
   * @returns {object} the scheduled manager fixture plus `group`, `halts` and `call`
   */
  const positionGroup = () => {
    const ctx = createScheduledOffsetManager({ committed: '-1', listOffset: '7' })
    const group = groupOver(ctx)
    group.assignmentEpoch = 1
    group.acquired.set(KEY, { acquiredAt: 1, changedAt: 1 })
    group.acquireClock = 1
    const halts = []
    group.instrumentationEmitter.addListener(
      require('../instrumentationEvents').events.PARTITION_HALTED,
      (event) => halts.push(event.payload)
    )
    const operations = []
    const call = (deadlineMs = 150) => {
      const operation = new ConsumerOperation({ deadlineAt: performance.now() + deadlineMs })
      operations.push(operation)
      return operation.wait(() => group.position(POSITION_CELL, { operation }))
    }
    return { ...ctx, group, halts, call, finish: () => operations.forEach((op) => op.finish()) }
  }

  /** @param {object} h the fixture @returns {void} nothing of an expired call survived */
  const expectUntouched = (h) => {
    expect(h.cache[HARNESS_TOPIC]?.[0]).not.toBe('7')
    expect(h.manager.resolvedOffsetAt(HARNESS_TOPIC, 0)).toBeUndefined()
    expect(h.group.haltedPartitions.size).toBe(0)
    expect(h.halts).toHaveLength(0)
    expect(h.coordinator.offsetCommit).not.toHaveBeenCalled()
    expect(h.operations.openTickets).toBe(0)
    expect(h.operations.cells).toBe(0)
  }

  test('an expired first coordinator lookup sends nothing and changes nothing', async () => {
    const h = positionGroup()
    const lookup = gate()
    h.coordinator.isConnected = () => false
    h.cluster.findGroupCoordinator = jest.fn(() => lookup.promise)
    const settled = h.call().catch((error) => error)
    expect(await settled).toBeInstanceOf(KafkaJSDeliveryTimeout)
    expect(h.cluster.findGroupCoordinator).toHaveBeenCalledTimes(1)
    lookup.resolve({ ...h.coordinator, isConnected: () => true })
    await pause(30)
    expect(h.coordinator.offsetFetch).not.toHaveBeenCalled()
    expect(h.cluster.fetchTopicsOffset).not.toHaveBeenCalled()
    expectUntouched(h)
    h.finish()
  })

  test('an expired reset answer fills no position, cache, debt or halt', async () => {
    const h = positionGroup()
    h.recreate(NEW_ID)
    const reset = h.schedule.hold({ api: 'ListOffsets' })
    const settled = h.call().catch((error) => error)
    await reset.arrived()
    expect(await settled).toBeInstanceOf(KafkaJSDeliveryTimeout)
    reset.release(listOffsetsAnswer({ 0: '7' }))
    await pause(30)
    expectUntouched(h)
    expect(h.owed()).toBe(true)
    h.finish()
  })

  test('a same-offset seek keeps the same acquisition and the call resolves', async () => {
    const h = positionGroup()
    const reset = h.schedule.hold({ api: 'ListOffsets' })
    const pending = h.call(2000)
    await reset.arrived()
    h.group.seek({ ...POSITION_CELL, offset: '7' })
    reset.release(listOffsetsAnswer({ 0: '7' }))
    await expect(pending).resolves.toBe('7')
    expect(h.group.acquired.get(KEY).acquiredAt).toBe(1)
    h.finish()
  })

  test('an equal-valued reacquisition under a distinct identity refuses the old answer', async () => {
    const h = positionGroup()
    h.recreate(NEW_ID)
    const reset = h.schedule.hold({ api: 'ListOffsets' })
    const pending = h.call(2000).catch((error) => error)
    await reset.arrived()
    h.manager.updateAssignment({
      revoked: [{ topic: HARNESS_TOPIC, partitions: [0] }],
      memberAssignment: { [HARNESS_TOPIC]: [0] },
      generationId: 2,
      memberId: 'member-a',
      coordinator: h.coordinator,
    })
    h.group.acquired.delete(KEY)
    h.group.acquired.set(KEY, { acquiredAt: 2, changedAt: 2 })
    reset.release(listOffsetsAnswer({ 0: '7' }))
    expect(await pending).toMatchObject({ type: 'CONSUMER_STOPPING' })
    expectUntouched(h)
    expect(h.owed()).toBe(true)

    await expect(h.group.position(POSITION_CELL)).resolves.toBe('7')
    h.finish()
  })
})

describe.each([true, false])(
  'a real connection under an API operation with request timeout enforcement %p',
  (enforce) => {
    const settle = () => new Promise((resolve) => setImmediate(resolve))
    /** @type {{connection: Connection, state: object}|null} */
    let wire
    let operation
    const clock = { now: 0 }

    beforeEach(async () => {
      answered = new Set()
      clock.now = 0
      operation = new ConsumerOperation({ deadlineAt: 1000, now: () => clock.now })
      const { factory, state } = createFakeSocketFactory()
      const connection = new Connection({
        host: '127.0.0.1',
        port: 9092,
        logger: newLogger(),
        socketFactory: factory,
        clientId: 'test-client',
        connectionTimeout: 1000,
        requestTimeout: 5000,
        enforceRequestTimeout: enforce,
        maxInFlightRequests: 1,
      })
      await connection.connect()
      wire = { connection, state }
    })
    afterEach(async () => {
      operation.finish()
      await wire.connection.disconnect()
    })

    /**
     * @param {() => Promise<Encoder>} [encode] how the request is encoded
     * @returns {{request: object, response: object}} a Metadata-shaped call with a one-int answer
     */
    const call = (encode = async () => new Encoder().writeInt32(0)) => ({
      request: {
        apiKey: apiKeys.Metadata,
        apiVersion: 1,
        apiName: 'Metadata',
        expectResponse: () => true,
        encode,
      },
      response: { decode: async (buffer) => buffer, parse: async (decoded) => decoded },
    })
    let answered
    /** @returns {void} answers every frame written so far, once, as a broker would */
    const answerWritten = () => {
      for (const frame of wire.state.writes) {
        const { correlationId } = readRequestHeader(frame)
        if (answered.has(correlationId)) continue
        answered.add(correlationId)
        respondWithFrame(wire.state, correlationId, 0, Buffer.alloc(4))
      }
    }
    const queue = () => wire.connection.requestQueue

    test('a request whose encoding finishes after the deadline writes no frame and leaves no entry', async () => {
      const encoding = gate()
      const sent = wire.connection
        .send({ ...call(() => encoding.promise), guard: operation.guard })
        .catch((error) => error)
      await settle()
      expect(queue().pending).toHaveLength(0)

      clock.now = 1000
      encoding.resolve(new Encoder().writeInt32(0))
      await settle()
      answerWritten()

      expect(await sent).toBeInstanceOf(KafkaJSDeliveryTimeout)
      expect(wire.state.writes).toEqual([])
      expect(queue().pending).toHaveLength(0)
      expect(queue().inflight.size).toBe(0)
    })

    test('the same request writes its one frame while the deadline holds', async () => {
      const encoding = gate()
      const sent = wire.connection.send({ ...call(() => encoding.promise), guard: operation.guard })
      await settle()
      encoding.resolve(new Encoder().writeInt32(0))
      await settle()
      expect(wire.state.writes).toHaveLength(1)
      answerWritten()
      await expect(sent).resolves.toBeDefined()
      expect(queue().inflight.size).toBe(0)
    })

    test('a queued unsent request is refused at transmission without a frame or an in-flight slot', async () => {
      const first = wire.connection.send(call())
      await settle()
      expect(wire.state.writes).toHaveLength(1)
      const queued = wire.connection.send({ ...call(), guard: operation.guard }).catch((e) => e)
      const control = wire.connection.send(call())
      await settle()
      expect(queue().pending).toHaveLength(2)

      clock.now = 1000
      answerWritten()
      await first
      await settle()
      answerWritten()

      expect(await queued).toBeInstanceOf(KafkaJSDeliveryTimeout)
      expect(wire.state.writes).toHaveLength(2)
      expect(queue().pending).toHaveLength(0)
      await expect(control).resolves.toBeDefined()
      expect(queue().inflight.size).toBe(0)
    })

    test('retiring the operation refuses the queued request exactly as an expired clock does', async () => {
      const first = wire.connection.send(call())
      await settle()
      const queued = wire.connection.send({ ...call(), guard: operation.guard }).catch((e) => e)
      await settle()
      expect(queue().pending).toHaveLength(1)

      const retired = new KafkaJSDeliveryTimeout('The call was retired')
      operation.retire(retired)
      answerWritten()
      await first
      await settle()
      answerWritten()

      expect(await queued).toBe(retired)
      expect(wire.state.writes).toHaveLength(1)
      expect(queue().pending).toHaveLength(0)
      expect(queue().inflight.size).toBe(0)
    })
  }
)
