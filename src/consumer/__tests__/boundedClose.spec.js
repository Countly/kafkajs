const { Kafka, logLevel } = require('../../../index')
const Runner = require('../runner')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const Generation = require('../../producer/generation')
const { newLogger } = require('testHelpers')
const { ConsumerOperation } = require('../groupProtocol/interface')
const createRetry = require('../../retry')
const { createScriptedCluster, groupScript } = require('../../../testHelpers/scriptedCluster')
const { RecordBatch } = require('../../protocol/recordBatch/v0')
const RecordEncoder = require('../../protocol/recordBatch/record/v0')
const { createGroupDescriptions } = require('../../admin/groupDescriptions')
const { TOPIC } = require('./fixtures/fetchHarness')
const {
  KafkaJSDeliveryTimeout,
  KafkaJSCommitFailedError,
  KafkaJSInvalidConfigurationError,
  KafkaJSNonRetriableError,
} = require('../../errors')

/** @returns {{promise:Promise<unknown>,resolve:Function,reject:Function}} controlled boundary */
const gate = () => {
  let resolve, reject
  const promise = new Promise((ok, fail) => {
    resolve = ok
    reject = fail
  })
  return { promise, resolve, reject }
}
/** @returns {Promise<void>} finite local promise chain */
const flush = async () => {
  for (let n = 0; n < 12; n++) await Promise.resolve()
}

describe('consumer control operation lifetime', () => {
  let operations
  beforeEach(() => {
    jest.useFakeTimers()
    operations = []
  })
  afterEach(() => {
    for (const operation of operations) operation.finish()
    expect(jest.getTimerCount()).toBe(0)
    jest.useRealTimers()
  })
  /** @param {object} [options] context @returns {ConsumerOperation} tracked scope */
  const scope = (options) => {
    const op = new ConsumerOperation(options)
    operations.push(op)
    return op
  }

  test('a first held await expires without waiting for a failed attempt', async () => {
    const held = gate()
    const op = scope({ deadlineAt: performance.now() + 1000 })
    let settled = false
    const observed = op
      .wait(() => held.promise)
      .catch((e) => {
        settled = true
        return e
      })
    await jest.advanceTimersByTimeAsync(1000)
    expect(settled).toBe(true)
    expect(await observed).toBeInstanceOf(KafkaJSDeliveryTimeout)
    expect(op.waiters.size).toBe(0)
    expect(jest.getTimerCount()).toBe(0)
    held.reject(new Error('late response'))
    await flush()
  })
  test('actual time refuses a write before a delayed timer callback', async () => {
    let now = 0
    const op = scope({ deadlineAt: 1000, now: () => now })
    const write = jest.fn()
    now = 1000
    await expect(op.wait(write)).rejects.toBeInstanceOf(KafkaJSDeliveryTimeout)
    expect(write).not.toHaveBeenCalled()
    expect(jest.getTimerCount()).toBe(0)
  })
  test('retirement refuses equal-valued replacement work', async () => {
    const run = Symbol('run')
    const held = gate()
    const replacement = { position: '8' }
    let active = run
    const error = new KafkaJSCommitFailedError('Run retired')
    const op = scope({ guard: () => (active === run ? null : error) })
    const pending = op
      .wait(() => held.promise)
      .then(() => {
        op.check()
        replacement.position = '99'
      })
      .catch((e) => e)
    active = Symbol('run')
    op.retire(error)
    expect(jest.getTimerCount()).toBe(0)
    expect(await pending).toBe(error)
    held.resolve('99')
    await flush()
    expect(replacement.position).toBe('8')
  })
  test('the original identity guard refuses a new step', async () => {
    const error = new KafkaJSCommitFailedError('Original identity expired')
    const op = scope({ guard: () => error })
    const action = jest.fn()
    await expect(op.wait(action)).rejects.toBe(error)
    expect(action).not.toHaveBeenCalled()
  })
  test('retirement clears an owned backoff timer and settles its caller', async () => {
    const op = scope()
    const error = new KafkaJSCommitFailedError('Run retired')
    const pending = op.sleep(5000).catch((e) => e)
    expect(jest.getTimerCount()).toBe(1)
    op.retire(error)
    expect(await pending).toBe(error)
    expect(op.disposers.size).toBe(0)
    expect(jest.getTimerCount()).toBe(0)
  })
  test('retirement removes every owned listener despite a cleanup error', () => {
    const op = scope()
    const disposed = jest.fn()
    op.register(() => {
      throw new Error('cleanup failed')
    })
    op.register(disposed)
    op.finish()
    op.finish()
    expect(disposed).toHaveBeenCalledTimes(1)
    expect(op.disposers.size).toBe(0)
  })
  test('unregistering a resource does not dispose a later owner', () => {
    const op = scope()
    const old = jest.fn()
    const current = jest.fn()
    const unregister = op.register(old)
    unregister()
    op.register(current)
    op.finish()
    expect(old).not.toHaveBeenCalled()
    expect(current).toHaveBeenCalledTimes(1)
  })
  test('a registered cancel receives the retirement error, also when already retired', () => {
    const op = scope()
    const cancel = jest.fn()
    const error = new KafkaJSCommitFailedError('Run retired')
    op.register(cancel)
    op.retire(error)
    expect(cancel).toHaveBeenCalledWith(error)
    const late = jest.fn()
    expect(op.register(late)).toEqual(expect.any(Function))
    expect(late).toHaveBeenCalledWith(error)
    expect(op.disposers.size).toBe(0)
  })
  test('the guard works detached, keeps the first refusal and matches check', () => {
    let now = 0
    const op = scope({ deadlineAt: 1000, now: () => now })
    const { guard } = op
    expect(guard()).toBeNull()
    expect(() => op.check()).not.toThrow()
    now = 1000
    expect(guard()).toBeInstanceOf(KafkaJSDeliveryTimeout)
    expect(op.guard()).toBe(guard())
    expect(() => op.check()).toThrow(KafkaJSDeliveryTimeout)
  })
  test('a fenced retry stops at retirement and removes its backoff timer', async () => {
    const op = scope()
    const attempt = jest.fn(async () => {
      throw new Error('moved')
    })
    const pending = createRetry({ fence: op, initialRetryTime: 2000, factor: 0, retries: 2 })(
      attempt
    ).catch((e) => e)
    await flush()
    expect(jest.getTimerCount()).toBe(1)
    const retired = new KafkaJSCommitFailedError('Run retired')
    op.retire(retired)
    expect(await pending).toBe(retired)
    expect(jest.getTimerCount()).toBe(0)
    expect(attempt).toHaveBeenCalledTimes(1)
  })
  test('one deadline spans two successful waits', async () => {
    const op = scope({ deadlineAt: performance.now() + 1000 })
    expect(await op.wait(() => 1)).toBe(1)
    await jest.advanceTimersByTimeAsync(800)
    const held = gate()
    let settled = false
    const second = op
      .wait(() => held.promise)
      .catch((e) => {
        settled = true
        return e
      })
    await jest.advanceTimersByTimeAsync(200)
    expect(settled).toBe(true)
    expect(await second).toBeInstanceOf(KafkaJSDeliveryTimeout)
    held.resolve(2)
    await flush()
  })
  test('zero remaining budget starts no commit or leave thunk', async () => {
    const op = scope({ deadlineAt: performance.now() })
    const commit = jest.fn()
    const leave = jest.fn()
    await expect(op.wait(commit)).rejects.toBeInstanceOf(KafkaJSDeliveryTimeout)
    await expect(op.wait(leave)).rejects.toBeInstanceOf(KafkaJSDeliveryTimeout)
    expect(commit).not.toHaveBeenCalled()
    expect(leave).not.toHaveBeenCalled()
  })
  test('an omitted deadline keeps the natural wait', async () => {
    const op = scope()
    const held = gate()
    let settled = false
    const pending = op
      .wait(() => held.promise)
      .then(() => {
        settled = true
      })
    await jest.advanceTimersByTimeAsync(60000)
    expect(settled).toBe(false)
    expect(jest.getTimerCount()).toBe(0)
    held.resolve()
    await pending
    expect(settled).toBe(true)
  })
  test('a delayed successful response is refused before local publication', async () => {
    let now = 0
    const op = scope({ deadlineAt: 1000, now: () => now })
    const held = gate()
    const apply = jest.fn()
    const pending = op
      .wait(() => held.promise)
      .then(apply)
      .catch((e) => e)
    now = 1001
    held.resolve('7')
    expect(await pending).toBeInstanceOf(KafkaJSDeliveryTimeout)
    expect(apply).not.toHaveBeenCalled()
  })
  test('a retry that cannot fit creates no backoff timer', async () => {
    const op = scope({ deadlineAt: performance.now() + 1000 })
    const attempt = jest.fn(async () => {
      throw new Error('moved')
    })
    let observed
    const pending = op
      .retry(attempt, { initialRetryTime: 2000, factor: 0, retries: 2 })
      .catch((error) => {
        observed = error
      })
    await flush()
    expect(observed).toBeInstanceOf(KafkaJSDeliveryTimeout)
    expect(attempt).toHaveBeenCalledTimes(1)
    await pending
    op.finish()
    expect(jest.getTimerCount()).toBe(0)
  })
  test('stop during retry backoff removes its timer before a second attempt', async () => {
    const op = scope()
    const attempt = jest.fn(async () => {
      throw new Error('moved')
    })
    const pending = op
      .retry(attempt, { initialRetryTime: 2000, factor: 0, retries: 2 })
      .catch((e) => e)
    await flush()
    expect(jest.getTimerCount()).toBe(1)
    const retired = new KafkaJSCommitFailedError('Run retired')
    op.retire(retired)
    expect(jest.getTimerCount()).toBe(0)
    expect(await pending).toBe(retired)
    await jest.advanceTimersByTimeAsync(3000)
    expect(attempt).toHaveBeenCalledTimes(1)
    expect(jest.getTimerCount()).toBe(0)
  })
  test('deadline expiry retains the last failed attempt as its cause', async () => {
    const cause = new Error('coordinator moved')
    const op = scope({ deadlineAt: performance.now() + 1000 })
    const held = gate()
    let calls = 0
    const pending = op
      .retry(
        () => {
          if (calls++ === 0) throw cause
          return held.promise
        },
        { initialRetryTime: 10, factor: 0, retries: 2 }
      )
      .catch((error) => error)
    await flush()
    await jest.advanceTimersByTimeAsync(1000)
    expect(await pending).toMatchObject({ name: 'KafkaJSDeliveryTimeout', cause })
    held.resolve()
    await flush()
  })
  test('bail stays terminal and keeps its original error', async () => {
    const op = scope()
    const error = new Error('bail')
    const attempt = jest.fn((bail) => bail(error))
    await expect(op.retry(attempt)).rejects.toBe(error)
    expect(attempt).toHaveBeenCalledTimes(1)
  })
  test('retry records the existing exhaustion shape', async () => {
    const op = scope()
    const attempt = jest.fn(async () => {
      throw new Error('retry')
    })
    let settled = false
    const pending = op
      .retry(attempt, { initialRetryTime: 10, factor: 0, retries: 1 })
      .catch((e) => {
        settled = true
        return e
      })
    await flush()
    await jest.advanceTimersByTimeAsync(10)
    expect(settled).toBe(true)
    expect(await pending).toMatchObject({
      name: 'KafkaJSNumberOfRetriesExceeded',
      retryCount: 1,
    })
    expect(attempt).toHaveBeenCalledTimes(2)
  })

  describe('as the fence of a pool install', () => {
    const BrokerPool = require('../../cluster/brokerPool')
    const { createLogger } = require('../../loggers')
    const sasl = Object.assign(new Error('illegal sasl state'), {
      type: 'ILLEGAL_SASL_STATE',
      retriable: true,
    })
    /** @returns {{pool:BrokerPool,built:object,release:Function}} a pool whose builder is held */
    const heldBuilder = () => {
      const held = gate()
      const built = { retire: jest.fn(), destroy: jest.fn(async () => {}) }
      const pool = new BrokerPool({
        connectionPoolBuilder: { build: jest.fn(() => held.promise) },
        logger: createLogger({ level: 0, logCreator: require('../../loggers/console') }),
      })
      return { pool, built, release: () => held.resolve(built) }
    }
    /** @param {BrokerPool} pool owner @param {object} built unused pool @returns {void} */
    const expectDisposed = (pool, built) => {
      expect(built.retire).toHaveBeenCalledTimes(1)
      expect(built.destroy).toHaveBeenCalledTimes(1)
      expect(pool.brokers).toEqual({})
    }

    test('closed reports retirement and original identity refusal', () => {
      const error = new KafkaJSCommitFailedError('Run retired')
      const retired = scope()
      expect(retired.closed).toBe(false)
      retired.retire(error)
      expect(retired.closed).toBe(true)
      let refusal = null
      const refused = scope({ guard: () => refusal })
      expect(refused.closed).toBe(false)
      refusal = error
      expect(refused.closed).toBe(true)
      expect(refused.error).toBe(error)
    })
    test('closed reports actual expiry before the deadline timer is delivered', () => {
      let now = 0
      const op = scope({ deadlineAt: 1000, now: () => now })
      expect(op.closed).toBe(false)
      now = 1000
      expect(op.closed).toBe(true)
      expect(op.error).toBeInstanceOf(KafkaJSDeliveryTimeout)
    })
    test('a seed pool built after retirement is disposed and the previous seed stays', async () => {
      const error = new KafkaJSCommitFailedError('Run retired')
      const op = scope()
      const h = heldBuilder()
      const previous = { retire: jest.fn(async () => {}) }
      h.pool.seedBroker = previous
      const pending = h.pool.createSeedBroker({ fence: op }).catch((e) => e)
      op.retire(error)
      h.release()
      expect(await pending).toBe(error)
      expectDisposed(h.pool, h.built)
      expect(h.pool.seedBroker).toBe(previous)
      expect(previous.retire).not.toHaveBeenCalled()
    })
    test('a seed pool built after actual expiry is refused without a delivered deadline wake', async () => {
      let now = 0
      const op = scope({ deadlineAt: 1000, now: () => now })
      const h = heldBuilder()
      const previous = { retire: jest.fn(async () => {}) }
      h.pool.seedBroker = previous
      const pending = h.pool.createSeedBroker({ fence: op }).catch((e) => e)
      now = 1000
      h.release()
      expect(await pending).toBeInstanceOf(KafkaJSDeliveryTimeout)
      expectDisposed(h.pool, h.built)
      expect(h.pool.seedBroker).toBe(previous)
      expect(previous.retire).not.toHaveBeenCalled()
    })
    test('an endpoint pool built after retirement is disposed and installs no broker', async () => {
      const error = new KafkaJSCommitFailedError('Run retired')
      const op = scope()
      const h = heldBuilder()
      const pending = h.pool
        .applyNodeEndpoints([{ nodeId: 1, host: 'broker', port: 9092 }], { fence: op })
        .catch((e) => e)
      op.retire(error)
      h.release()
      expect(await pending).toBe(error)
      expectDisposed(h.pool, h.built)
    })
    test('a replacement pool built after retirement is disposed and attached to no broker', async () => {
      const error = new KafkaJSCommitFailedError('Run retired')
      const op = scope()
      const h = heldBuilder()
      h.pool.replacePool = jest.fn()
      const broker = {
        connectionPool: { host: 'broker', port: 9092 },
        isConnected: () => false,
        connect: jest.fn(async () => {
          throw sasl
        }),
        disconnect: jest.fn(async () => {}),
      }
      const pending = h.pool.connectBroker(broker, { fence: op }).catch((e) => e)
      await flush()
      op.retire(error)
      h.release()
      expect(await pending).toBe(error)
      await flush()
      expect(h.built.retire).toHaveBeenCalledTimes(1)
      expect(h.pool.replacePool).not.toHaveBeenCalled()
    })
    test('a metadata refresh failing after retirement starts no recovery', async () => {
      const error = new KafkaJSCommitFailedError('Run retired')
      const op = scope()
      const h = heldBuilder()
      h.pool.onMetadataFailure = jest.fn(async () => {})
      h.pool.findConnectedBroker = jest.fn(async () => ({
        allowAutoTopicCreation: false,
        metadata: () => gate().promise,
      }))
      const pending = h.pool.refreshMetadata([], { fence: op }).catch((e) => e)
      await flush()
      op.retire(error)
      expect(await pending).toBe(error)
      expect(h.pool.onMetadataFailure).not.toHaveBeenCalled()
    })
  })
})

describe('disconnect validation before cleanup', () => {
  const createConsumer = require('../index')
  const { createLogger } = require('../../loggers')
  /** @returns {{consumer:object,disconnect:Function,remove:Function}} stopped consumer with no sockets */
  const fixture = () => {
    const disconnect = jest.fn(async () => {})
    const remove = jest.fn()
    const consumer = createConsumer({
      groupId: 'g',
      partitionAssigners: [],
      logger: createLogger({
        level: 0,
        logCreator: require('../../loggers/console'),
      }),
      cluster: { onTopicRecreated: () => remove, disconnect },
    })
    return { consumer, disconnect, remove }
  }
  test('invalid timeout uses the configuration error before cleanup', async () => {
    const h = fixture()
    const failure = await h.consumer.disconnect({ timeout: -1 }).catch((error) => error)
    expect(h.disconnect).not.toHaveBeenCalled()
    expect(h.remove).not.toHaveBeenCalled()
    expect(failure).toBeInstanceOf(KafkaJSInvalidConfigurationError)
    expect(failure).toMatchObject({ category: 'invalidConfiguration', retriable: false })
    await h.consumer.disconnect()
  })
  test.each([-1, 0.5, NaN, Infinity, 2147483648, '1', null, true])(
    'invalid timeout %p leaves the consumer untouched',
    async (timeout) => {
      const h = fixture()
      await expect(h.consumer.disconnect({ timeout })).rejects.toMatchObject({
        name: 'KafkaJSInvalidConfigurationError',
        category: 'invalidConfiguration',
      })
      expect(h.disconnect).not.toHaveBeenCalled()
      expect(h.remove).not.toHaveBeenCalled()
      await h.consumer.disconnect()
    }
  )
  test.each([0, 2147483647, undefined])('valid timeout %p reaches cleanup', async (timeout) => {
    const h = fixture()
    await h.consumer.disconnect({ timeout })
    expect(h.disconnect).toHaveBeenCalledTimes(1)
    expect(h.remove).toHaveBeenCalledTimes(1)
  })
  test.each(['SIGTERM', true, null])('legacy nonobject %p remains accepted', async (argument) => {
    const h = fixture()
    await h.consumer.disconnect(argument)
    expect(h.disconnect).toHaveBeenCalledTimes(1)
  })
})

const NODE = { id: 0, host: 'node-0', port: 9092 }
const WAIT_MS = 10000
const BOUND_MS = 300
const GROUP = 'close-group'

/** @param {() => boolean} predicate @param {string} label @returns {Promise<void>} resolves when it holds */
const until = async (predicate, label) => {
  const deadline = Date.now() + WAIT_MS
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`Did not happen in time: ${label}`)
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
}
/** @param {number} ms @returns {Promise<void>} resolves after the real delay */
const pause = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

/**
 * One write of the production encoder, so the decoder reads it as it reads a broker's.
 * @param {number} offset first and only offset of the batch
 * @param {boolean} [corrupt=false] flip one byte inside the CRC-covered region
 * @returns {Promise<Buffer>} a one-record batch whose value names its offset
 */
const batchAt = async (offset, corrupt = false) => {
  const batch = Buffer.from(
    (
      await RecordBatch({
        firstOffset: String(offset),
        firstTimestamp: 1700000000000,
        maxTimestamp: 1700000000000,
        partitionLeaderEpoch: 5,
        records: [
          RecordEncoder({
            key: Buffer.from(`key-${offset}`),
            value: Buffer.from(`value-${offset}`),
          }),
        ],
      })
    ).buffer
  )
  // The byte after the producer id is covered by the CRC and otherwise inert.
  if (corrupt) batch[8 + 4 + 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8] ^= 0x01
  return batch
}

/**
 * Holds the answer of one API, after the request was received and recorded. The broker has applied
 * what a held request carried; only its answer waits.
 * @param {object} cluster the scripted cluster
 * @param {string} api the API name
 * @returns {{ hits: number, received: object[], release: () => void }} the hold
 */
const holdAnswer = (cluster, api) => {
  const original = cluster.answerers.get(api)
  const opened = gate()
  const hold = {
    hits: 0,
    received: [],
    release: () => {
      hold.open = true
      opened.resolve()
    },
    open: false,
  }
  cluster.answer(api, async (fields, record) => {
    hold.hits += 1
    hold.received.push(fields)
    if (!hold.open) await opened.promise
    return original(fields, record)
  })
  return hold
}

/**
 * A scripted one-partition topic whose log holds one record per batch at offsets `0..last`.
 * @param {object} [options]
 * @param {number} [options.last=2]
 * @param {boolean} [options.enforce=true] whether the transport enforces request timeouts
 * @param {object} [options.retry] client retry options
 * @returns {Promise<{ cluster: object, kafka: Kafka }>}
 */
const closeRig = async ({ last = 2, enforce = true, retry = {} } = {}) => {
  const batches = {}
  for (let offset = 0; offset <= last; offset++) batches[offset] = await batchAt(offset)
  const cluster = groupScript(createScriptedCluster({ nodes: [NODE] }), { partitions: 1 }).answer(
    'Fetch',
    async ({ maxWaitMs, topics }) => {
      const partitions = topics[0].partitions.map(({ partition, fetchOffset }) => ({
        partition,
        errorCode: 0,
        records: batches[fetchOffset] ?? false,
        abortedTransactions: [],
      }))
      if (partitions.every(({ records }) => records === false)) await pause(maxWaitMs)
      return { partitions }
    }
  )
  const kafka = new Kafka(
    cluster.kafkaOptions({
      logLevel: logLevel.NOTHING,
      enforceRequestTimeout: enforce,
      retry: { retries: 1, initialRetryTime: 10, maxRetryTime: 20, ...retry },
    })
  )
  return { cluster, kafka }
}

/** @param {object} cluster @param {string} api @returns {object[]} the frames of one API */
const frames = (cluster, api) => cluster.requests().filter((request) => request.api === api)

describe('a consumer close over a scripted broker', () => {
  const consumers = []
  const holds = []
  let rig

  /**
   * @param {object} [options] `rig` options
   * @returns {Promise<void>} a fresh rig the test reads as `rig`
   */
  const open = async (options) => {
    rig = await closeRig(options)
  }
  /**
   * Starts a consumer on the rig and waits for its first deliveries.
   * @param {object} [options]
   * @param {object} [options.consumer] consumer options
   * @param {object} [options.run] run options
   * @param {number} [options.delivered=3] records to wait for
   * @returns {Promise<{ consumer: object, delivered: string[], events: string[] }>}
   */
  const started = async ({ consumer: consumerOptions = {}, run = {}, delivered = 3 } = {}) => {
    const consumer = rig.kafka.consumer({ groupId: GROUP, maxWaitTimeInMs: 20, ...consumerOptions })
    consumers.push(consumer)
    const seen = []
    const events = []
    for (const name of ['STOP', 'DISCONNECT'])
      consumer.on(consumer.events[name], () => events.push(name))
    await consumer.connect()
    await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
    await consumer.run({
      autoCommit: true,
      autoCommitInterval: 60000,
      eachMessage: async ({ message }) => {
        seen.push(message.value.toString())
      },
      ...run,
    })
    await until(() => seen.length >= delivered, 'the deliveries')
    return { consumer, delivered: seen, events }
  }

  afterEach(async () => {
    for (const hold of holds.splice(0)) hold.release()
    for (const consumer of consumers.splice(0)) await consumer.disconnect().catch(() => {})
    rig?.cluster.assertClean()
    rig = null
  })

  /** @param {string} api @returns {object} a hold of that API's answer, released after the test */
  const hold = (api) => {
    const held = holdAnswer(rig.cluster, api)
    holds.push(held)
    return held
  }

  test('the first disconnect bounds the entire shared cleanup', async () => {
    await open()
    const { consumer, events } = await started()
    const leave = hold('LeaveGroup')
    const startedAt = Date.now()
    const first = consumer.disconnect({ timeout: BOUND_MS, leaveGroup: true })
    const second = consumer.disconnect({ timeout: 9000, leaveGroup: false })
    await expect(Promise.all([first, second])).resolves.toBeDefined()
    expect(Date.now() - startedAt).toBeGreaterThanOrEqual(BOUND_MS - 50)
    expect(Date.now() - startedAt).toBeLessThan(3000)
    expect(leave.hits).toBe(1)
    expect(events).toEqual(['STOP', 'DISCONNECT'])
  })

  test('a second close with an invalid timeout rejects without changing the first', async () => {
    await open()
    const { consumer } = await started()
    hold('LeaveGroup')
    const startedAt = Date.now()
    const first = consumer.disconnect({ timeout: BOUND_MS })
    await expect(consumer.disconnect({ timeout: -1 })).rejects.toBeInstanceOf(
      KafkaJSInvalidConfigurationError
    )
    await first
    expect(Date.now() - startedAt).toBeGreaterThanOrEqual(BOUND_MS - 50)
  })

  test('zero timeout starts no final commit or leave', async () => {
    await open()
    const { consumer, events } = await started()
    await consumer.disconnect({ timeout: 0 })
    expect(frames(rig.cluster, 'OffsetCommit')).toHaveLength(0)
    expect(frames(rig.cluster, 'LeaveGroup')).toHaveLength(0)
    expect(events).toEqual(['STOP', 'DISCONNECT'])
  })

  test('omitted timeout waits naturally for a running handler', async () => {
    await open()
    const release = gate()
    let entered = false
    const { consumer } = await started({
      delivered: 0,
      run: {
        eachMessage: async () => {
          entered = true
          await release.promise
        },
      },
    })
    await until(() => entered, 'the handler')
    let settled = false
    const closing = consumer.disconnect().then(() => {
      settled = true
    })
    await pause(4 * BOUND_MS)
    expect(settled).toBe(false)
    release.resolve()
    await closing
    expect(frames(rig.cluster, 'LeaveGroup')).toHaveLength(1)
  })

  test('classic stop attempts the eligible final prefix once', async () => {
    await open()
    const { consumer } = await started()
    await consumer.stop()
    await consumer.disconnect()
    const commits = frames(rig.cluster, 'OffsetCommit')
    expect(commits).toHaveLength(1)
    const order = rig.cluster.requests().map(({ api }) => api)
    expect(order.indexOf('OffsetCommit')).toBeLessThan(order.indexOf('LeaveGroup'))
  })

  test('final commit is independent of the leave choice', async () => {
    await open()
    const { consumer } = await started()
    await consumer.disconnect({ leaveGroup: false })
    expect(frames(rig.cluster, 'OffsetCommit')).toHaveLength(1)
    expect(frames(rig.cluster, 'LeaveGroup')).toHaveLength(0)
  })

  test('final commit skips disabled progress', async () => {
    await open()
    const { consumer } = await started({ run: { autoCommit: false } })
    await consumer.disconnect()
    expect(frames(rig.cluster, 'OffsetCommit')).toHaveLength(0)
  })

  test('a written final commit may apply after local close expiry', async () => {
    await open()
    const { consumer } = await started()
    const commit = hold('OffsetCommit')
    const startedAt = Date.now()
    await consumer.disconnect({ timeout: BOUND_MS })
    expect(Date.now() - startedAt).toBeLessThan(3000)
    expect(commit.hits).toBe(1)
    commit.release()
    await pause(100)
    expect(frames(rig.cluster, 'OffsetCommit')).toHaveLength(1)
    expect(frames(rig.cluster, 'LeaveGroup')).toHaveLength(0)
  })

  test('a held handler is detached without accepting its later progress', async () => {
    await open()
    const release = gate()
    const outcome = []
    let entered = false
    const { consumer } = await started({
      delivered: 0,
      run: {
        eachMessage: async () => {
          if (entered) return
          entered = true
          await release.promise
          outcome.push(
            await consumer.commitOffsets([{ topic: TOPIC, partition: 0, offset: '9' }]).then(
              () => 'accepted',
              (error) => error.type ?? error.name
            )
          )
        },
      },
    })
    await until(() => entered, 'the handler')
    await consumer.disconnect({ timeout: BOUND_MS })
    const before = frames(rig.cluster, 'OffsetCommit').length
    release.resolve()
    await until(() => outcome.length === 1, 'the detached commit')
    expect(outcome[0]).toBe('CONSUMER_STOPPING')
    expect(frames(rig.cluster, 'OffsetCommit')).toHaveLength(before)
  })

  test('a replacement run starts after a held handler and a held hook are each detached', async () => {
    await open()
    const handlerGate = gate()
    const hookGate = gate()
    const detached = []
    const delivered = []
    let handlerHeld = false
    let hookHeld = false
    const consumer = rig.kafka.consumer({ groupId: GROUP, maxWaitTimeInMs: 20 })
    consumers.push(consumer)
    const runWith = ({ holdHandler = false, holdHook = false } = {}) => {
      let entered = false
      return consumer.run({
        autoCommit: true,
        autoCommitInterval: 60000,
        rebalanceListener: holdHook
          ? {
              // Every eager round revokes the empty assignment first; only an owned one is held.
              onPartitionsRevoked: async ({ partitions }) => {
                if (partitions.length === 0) return
                hookHeld = true
                await hookGate.promise
                detached.push(
                  await consumer.commitOffsets([{ topic: TOPIC, partition: 0, offset: '1' }]).then(
                    () => 'accepted',
                    (error) => error.type ?? error.name
                  )
                )
              },
            }
          : undefined,
        eachMessage: async ({ message }) => {
          delivered.push(message.value.toString())
          if (holdHandler && !entered) {
            entered = true
            handlerHeld = true
            await handlerGate.promise
            detached.push(
              await Promise.allSettled([
                consumer.commitOffsets([{ topic: TOPIC, partition: 0, offset: '99' }]),
              ]).then((results) => results.map(({ status }) => status))
            )
          }
        },
      })
    }
    const restart = async (options) => {
      await consumer.connect()
      await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
      await runWith(options)
    }

    await restart({ holdHandler: true })
    await until(() => handlerHeld, 'the handler')
    await consumer.disconnect({ timeout: BOUND_MS })

    const before = delivered.length
    await restart({ holdHook: true })
    await until(() => delivered.length > before, 'the replacement deliveries')
    handlerGate.resolve()
    await until(() => detached.length === 1, 'the released handler')
    expect(detached[0]).toEqual(['rejected'])

    await consumer.disconnect({ timeout: BOUND_MS })
    expect(hookHeld).toBe(true)
    await restart()
    hookGate.resolve()
    await until(() => detached.length === 2, 'the released hook')
    expect(detached[1]).not.toBe('accepted')
  })

  test('hook disconnect acknowledges before its own cleanup completes', async () => {
    await open()
    const acknowledged = []
    const consumer = rig.kafka.consumer({ groupId: GROUP, maxWaitTimeInMs: 20 })
    consumers.push(consumer)
    await consumer.connect()
    await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
    await consumer.run({
      eachMessage: async () => {},
      rebalanceListener: {
        onPartitionsAssigned: async () => {
          const startedAt = Date.now()
          await consumer.disconnect({ timeout: 1000 })
          acknowledged.push(Date.now() - startedAt)
        },
      },
    })
    await until(() => acknowledged.length === 1, 'the acknowledgement')
    expect(acknowledged[0]).toBeLessThan(900)
    await until(() => frames(rig.cluster, 'LeaveGroup').length >= 1, 'the leave')
    await pause(100)
    expect(frames(rig.cluster, 'LeaveGroup')).toHaveLength(1)
  })

  test('a close callback commits explicitly while the stop is in progress', async () => {
    await open()
    const commit = hold('OffsetCommit')
    commit.release()
    const outcome = []
    const owner = {}
    const { consumer } = await started({
      run: {
        autoCommit: false,
        rebalanceListener: {
          onPartitionsRevoked: async () => {
            if (!owner.consumer) return
            outcome.push(
              await owner.consumer
                .commitOffsets([{ topic: TOPIC, partition: 0, offset: '2' }])
                .then(
                  () => 'accepted',
                  (error) => error.name
                )
            )
          },
        },
      },
    })
    owner.consumer = consumer
    await consumer.disconnect({ timeout: 5000 })
    expect(outcome).toEqual(['accepted'])
    expect(commit.hits).toBe(1)
  })

  test('graceful close commits the valid prefix below a corrupt batch', async () => {
    await open()
    const prefix = Buffer.concat([await batchAt(0), await batchAt(1, true)])
    rig.cluster.answer('Fetch', async ({ maxWaitMs, topics }) => {
      const [{ partition, fetchOffset }] = topics[0].partitions
      if (String(fetchOffset) === '0') {
        return { partitions: [{ partition, errorCode: 0, records: prefix }] }
      }
      await pause(maxWaitMs)
      return { partitions: [{ partition, errorCode: 0, records: false }] }
    })
    const commit = hold('OffsetCommit')
    commit.release()
    const halts = []
    const consumer = rig.kafka.consumer({ groupId: GROUP, maxWaitTimeInMs: 20 })
    consumers.push(consumer)
    consumer.on(consumer.events.PARTITION_HALTED, ({ payload }) => halts.push(payload))
    await consumer.connect()
    await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
    await consumer.run({
      autoCommit: true,
      autoCommitInterval: 60000,
      eachMessage: async () => {},
    })
    await until(() => halts.length === 1, 'the halt')
    expect(commit.hits).toBe(0)
    await consumer.disconnect({ timeout: 5000 })
    const offsets = commit.received.flatMap(({ topics }) =>
      topics.flatMap(({ partitions }) => partitions.map(({ offset }) => Number(offset)))
    )
    expect(halts[0]).toEqual(expect.objectContaining({ partition: 0, reason: 'checksum' }))
    expect(offsets).toEqual([1])
  })

  describe.each([true, false])('with request timeout enforcement %p', (enforce) => {
    const bounded = { maxTotalTime: BOUND_MS }
    const connected = async (retry = bounded) => {
      await open({ enforce, retry })
      const consumer = rig.kafka.consumer({ groupId: GROUP, maxWaitTimeInMs: 20 })
      consumers.push(consumer)
      await consumer.connect()
      return consumer
    }
    const cell = [{ topic: TOPIC, partition: 0 }]

    test('committed settles at its deadline when the first lookup is held', async () => {
      const consumer = await connected()
      const lookup = hold('FindCoordinator')
      const startedAt = Date.now()
      await expect(consumer.committed(cell)).rejects.toBeInstanceOf(KafkaJSDeliveryTimeout)
      expect(Date.now() - startedAt).toBeLessThan(3000)
      expect(lookup.hits).toBeGreaterThan(0)
      lookup.release()
      await pause(100)
      expect(frames(rig.cluster, 'OffsetFetch')).toHaveLength(0)
    })

    test('committed settles at its deadline when the first response is held', async () => {
      const consumer = await connected()
      const fetch = hold('OffsetFetch')
      await expect(consumer.committed(cell)).rejects.toBeInstanceOf(KafkaJSDeliveryTimeout)
      expect(fetch.hits).toBe(1)
      fetch.release()
      await pause(100)
      expect(frames(rig.cluster, 'OffsetFetch')).toHaveLength(1)
    })

    test('commitOffsets settles at its deadline when the first response is held', async () => {
      await open({ enforce, retry: bounded })
      const { consumer } = await started()
      const commit = hold('OffsetCommit')
      await expect(
        consumer.commitOffsets([{ topic: TOPIC, partition: 0, offset: '2' }])
      ).rejects.toBeInstanceOf(KafkaJSDeliveryTimeout)
      expect(commit.hits).toBe(1)
    })

    test('position settles at its deadline when the first response is held', async () => {
      await open({ enforce, retry: bounded })
      const { consumer } = await started({ delivered: 1, run: { autoCommit: false } })
      const list = hold('ListOffsets')
      consumer.seek({ topic: TOPIC, partition: 0, offset: '-2' })
      await expect(consumer.position(cell[0])).rejects.toBeInstanceOf(KafkaJSDeliveryTimeout)
      expect(list.hits).toBeGreaterThan(0)
    })
  })

  test('an unset API deadline retains a held first call', async () => {
    await open({ retry: { maxTotalTime: undefined } })
    const consumer = rig.kafka.consumer({ groupId: GROUP, maxWaitTimeInMs: 20 })
    consumers.push(consumer)
    await consumer.connect()
    const fetch = hold('OffsetFetch')
    let settled = false
    const reading = consumer.committed([{ topic: TOPIC, partition: 0 }]).then((rows) => {
      settled = true
      return rows
    })
    await until(() => fetch.hits === 1, 'the held response')
    await pause(2 * BOUND_MS)
    expect(settled).toBe(false)
    fetch.release()
    await expect(reading).resolves.toEqual([
      { topic: TOPIC, partition: 0, offset: null, metadata: null },
    ])
  })

  test('empty committed input validates its group during close', async () => {
    await open()
    const { consumer } = await started()
    hold('LeaveGroup')
    const closing = consumer.disconnect({ timeout: BOUND_MS })
    await expect(consumer.committed([])).resolves.toEqual([])
    await expect(consumer.committed('x')).rejects.toBeInstanceOf(KafkaJSNonRetriableError)
    await closing
    const noGroup = rig.kafka.consumer({ maxWaitTimeInMs: 20 })
    await expect(noGroup.committed([])).rejects.toBeInstanceOf(KafkaJSNonRetriableError)
    expect(frames(rig.cluster, 'OffsetFetch')).toHaveLength(1)
  })

  test('committed remains a RequireStable live group read', async () => {
    await open()
    const consumer = rig.kafka.consumer({ groupId: GROUP, maxWaitTimeInMs: 20 })
    consumers.push(consumer)
    await consumer.connect()
    const fetch = hold('OffsetFetch')
    fetch.release()
    await consumer.committed([{ topic: TOPIC, partition: 0 }])
    expect(fetch.received[0].requireStable).toBe(true)
  })
})

describe('describeGroup under a consumer operation', () => {
  const batch = (ids) => ({
    groups: ids.map((groupId) => ({ groupId, errorCode: 0, members: [] })),
  })
  const describer = (cluster, retry = { retries: 1, initialRetryTime: 5, maxRetryTime: 10 }) =>
    createGroupDescriptions({
      cluster,
      retry,
      logger: { namespace: () => ({ debug: () => {}, error: () => {} }) },
    })

  test('an expired operation sends nothing and installs nothing', async () => {
    const coordinator = { nodeId: 0, consumerGroupDescribe: jest.fn(async () => batch(['g'])) }
    const cluster = {
      findGroupCoordinator: jest.fn(async () => coordinator),
      refreshMetadata: jest.fn(),
    }
    let now = 0
    const operation = new ConsumerOperation({ deadlineAt: 1000, now: () => now })
    now = 1000
    await expect(describer(cluster).describe(['g'], { operation })).rejects.toBeInstanceOf(
      KafkaJSDeliveryTimeout
    )
    expect(coordinator.consumerGroupDescribe).not.toHaveBeenCalled()
    operation.finish()
  })

  test('a held lookup settles at the operation deadline', async () => {
    const held = gate()
    const cluster = {
      findGroupCoordinator: jest.fn(() => held.promise),
      refreshMetadata: jest.fn(),
    }
    const operation = new ConsumerOperation({ deadlineAt: performance.now() + BOUND_MS })
    await expect(describer(cluster).describe(['g'], { operation })).rejects.toBeInstanceOf(
      KafkaJSDeliveryTimeout
    )
    held.resolve({ nodeId: 0, consumerGroupDescribe: jest.fn() })
    operation.finish()
  })

  test('the fallback uses the original operation and an ordinary call is unchanged', async () => {
    const unknown = { groups: [{ groupId: 'g', errorCode: 69, errorMessage: 'unknown' }] }
    const coordinator = {
      nodeId: 0,
      consumerGroupDescribe: jest.fn(async () => unknown),
      describeGroups: jest.fn(async () => ({
        groups: [
          {
            groupId: 'g',
            errorCode: 0,
            members: [],
            state: 'Empty',
            protocolType: '',
            protocol: '',
          },
        ],
      })),
    }
    const cluster = {
      findGroupCoordinator: jest.fn(async () => coordinator),
      refreshMetadata: jest.fn(),
    }
    const operation = new ConsumerOperation({ deadlineAt: performance.now() + 5000 })
    const { groups } = await describer(cluster).describe(['g'], { operation })
    expect(groups).toHaveLength(1)
    expect(coordinator.describeGroups).toHaveBeenCalledWith(
      expect.objectContaining({ guard: operation.guard })
    )
    expect(cluster.findGroupCoordinator).toHaveBeenCalledWith(
      expect.objectContaining({ fence: operation })
    )
    operation.finish()
    coordinator.describeGroups.mockClear()
    await describer(cluster).describe(['g'])
    expect(coordinator.describeGroups).toHaveBeenCalledWith({ groupIds: ['g'] })
    expect(cluster.findGroupCoordinator).toHaveBeenLastCalledWith({ groupId: 'g' })
  })
})

describe('a runner stopped while its connection is held', () => {
  /**
   * @param {object} connect the held connect
   * @returns {{ runner: Runner, group: object, onCrash: jest.Mock }} a runner over a minimal group
   */
  const buildRunner = (connect) => {
    const group = {
      groupId: 'close-group',
      memberId: 'm',
      groupInstanceId: null,
      kind: 'classic',
      groupMembership: {},
      attach: jest.fn(),
      getNodeIds: () => [1],
      connect: jest.fn(() => connect.promise),
      onStopRequested: jest.fn(),
      startIdleWait: jest.fn(() => ({ fence: new Generation() })),
      heartbeatInterval: () => 3000,
      beginRebalanceRound: jest.fn(),
      clearAssignerState: jest.fn(),
      leave: jest.fn(),
    }
    const onCrash = jest.fn()
    const runner = new Runner({
      logger: newLogger(),
      consumerGroup: group,
      instrumentationEmitter: new InstrumentationEventEmitter(),
      concurrency: 1,
      heartbeatInterval: 3000,
      maxPollInterval: 300000,
      eachBatch: async () => {},
      onCrash,
    })
    runner.rebalanceCoordinator.runRound = jest.fn()
    return { runner, group, onCrash }
  }

  test('a held connect cannot resurrect an initial run', async () => {
    const connect = gate()
    const { runner, group, onCrash } = buildRunner(connect)
    const starting = runner.start()
    await flush()
    expect(group.connect).toHaveBeenCalledTimes(1)
    await runner.stop()
    expect(group.onStopRequested).toHaveBeenCalledTimes(1)
    connect.resolve()
    await starting
    expect(runner.running).toBe(false)
    expect(runner.rebalanceCoordinator.runRound).not.toHaveBeenCalled()
    expect(onCrash).not.toHaveBeenCalled()
    expect(group.leave).not.toHaveBeenCalled()
  })

  test('a stop that closes the run fence ends a held connect without a crash', async () => {
    const fence = new Generation()
    const connect = gate()
    const { runner, group, onCrash } = buildRunner(connect)
    group.startIdleWait.mockReturnValue({ fence })
    group.connect.mockImplementation(({ fence: given }) => {
      given.register((error) => connect.reject(error))
      return connect.promise
    })
    const starting = runner.start()
    await flush()
    await runner.stop()
    fence.close(Object.assign(new Error('stopping'), { type: 'CONSUMER_STOPPING' }))
    await starting
    expect(onCrash).not.toHaveBeenCalled()
    expect(runner.rebalanceCoordinator.runRound).not.toHaveBeenCalled()
  })
})
