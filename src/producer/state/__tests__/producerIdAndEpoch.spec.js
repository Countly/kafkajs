const ProducerIdAndEpoch = require('../producerIdAndEpoch')
const { transactionStateFor } = require('../errorClassifier')
const { createErrorFromCode } = require('../../../protocol/error')
const { requests } = require('../../../protocol/requests')
const apiKeys = require('../../../protocol/requests/apiKeys')
const {
  KafkaJSApplicationRecoverableError,
  KafkaJSInvalidConfigurationError,
  KafkaJSInvariantViolation,
  KafkaJSNonRetriableError,
} = require('../../../errors')
const { newLogger } = require('testHelpers')

const TRANSACTIONAL_ID = 'test-transactional-id'
const TRANSACTION_TIMEOUT = 60000
const CLUSTER_AUTHORIZATION_FAILED = 31
const PRODUCER_FENCED = 90
const REQUEST_TIMED_OUT = 7
const REFUSED_MESSAGE =
  'The broker refused InitProducerId: the principal needs IDEMPOTENT_WRITE on the cluster ' +
  '(or WRITE on any topic, from Kafka 2.8), or create the producer with idempotent: false'

/**
 * A broker that negotiates InitProducerId at `maxVersion` and answers each InitProducerId from a
 * script: the next `{ producerId, producerEpoch }` pair, or the next error, thrown.
 * @param {{ maxVersion?: number, answers: Array<{ producerId: string, producerEpoch: number }|Error> }} script
 */
const createCoordinator = ({ maxVersion = 5, answers }) => ({
  negotiatedVersion: jest.fn((apiKey, definition) =>
    apiKey === apiKeys.InitProducerId && definition === requests.InitProducerId ? maxVersion : null
  ),
  initProducerId: jest.fn(async () => {
    const answer = answers.shift()
    if (answer instanceof Error) {
      throw answer
    }
    return { throttleTime: 0, errorCode: 0, ...answer }
  }),
})

/**
 * @param {{ transactionalId?: string|null, coordinator: object }} options
 * @returns {{ identity: ProducerIdAndEpoch, onReset: jest.Mock, findBroker: jest.Mock, logger: object }}
 */
const build = ({ transactionalId = TRANSACTIONAL_ID, coordinator }) => {
  const onReset = jest.fn()
  const findBroker = jest.fn(async () => coordinator)
  const logger = newLogger()
  const identity = new ProducerIdAndEpoch({
    logger,
    transactionalId,
    transactionTimeout: TRANSACTION_TIMEOUT,
    findBroker,
    onReset,
  })
  return { identity, onReset, findBroker, logger }
}

/** The InitProducerId requests the coordinator received, in order. */
const sent = (coordinator) => coordinator.initProducerId.mock.calls.map(([request]) => request)

const pairOf = (identity) => ({
  producerId: identity.producerId,
  producerEpoch: identity.producerEpoch,
})

describe('ProducerIdAndEpoch', () => {
  test('holds no pair before its first initialization', () => {
    const { identity } = build({ coordinator: createCoordinator({ answers: [] }) })

    expect(pairOf(identity)).toEqual({ producerId: -1, producerEpoch: -1 })
    expect(identity.isValid()).toBe(false)
    expect(identity.pendingInitialization()).toBeNull()
  })

  describe('initialize()', () => {
    test('sends -1/-1 with the transactional id, keeps the answer and restarts every partition', async () => {
      const coordinator = createCoordinator({ answers: [{ producerId: '1000', producerEpoch: 0 }] })
      const { identity, onReset } = build({ coordinator })

      await identity.initialize()

      expect(sent(coordinator)).toEqual([
        {
          transactionalId: TRANSACTIONAL_ID,
          transactionTimeout: TRANSACTION_TIMEOUT,
          producerId: -1,
          producerEpoch: -1,
        },
      ])
      expect(pairOf(identity)).toEqual({ producerId: '1000', producerEpoch: 0 })
      expect(identity.isValid()).toBe(true)
      expect(onReset).toHaveBeenCalledTimes(1)
    })

    test('an idempotent-only producer, created with no transactional id, sends null', async () => {
      const coordinator = createCoordinator({ answers: [{ producerId: '1000', producerEpoch: 0 }] })
      const identity = new ProducerIdAndEpoch({
        logger: newLogger(),
        transactionTimeout: TRANSACTION_TIMEOUT,
        findBroker: async () => coordinator,
        onReset: () => {},
      })

      await identity.initialize()

      expect(sent(coordinator)[0].transactionalId).toBeNull()
    })

    test('CLUSTER_AUTHORIZATION_FAILED becomes a configuration error naming both remedies', async () => {
      const refusal = createErrorFromCode(CLUSTER_AUTHORIZATION_FAILED)
      const coordinator = createCoordinator({ answers: [refusal] })
      const { identity, onReset } = build({ transactionalId: null, coordinator })

      const error = await identity.initialize().catch((e) => e)

      expect(error).toBeInstanceOf(KafkaJSInvalidConfigurationError)
      expect(error).toBeInstanceOf(KafkaJSNonRetriableError)
      expect(error.message).toEqual(REFUSED_MESSAGE)
      expect(error).toMatchObject({ retriable: false, category: 'invalidConfiguration' })
      expect(error.cause).toBe(refusal)
      expect(transactionStateFor(error)).toEqual('FATAL_ERROR')
      expect(identity.isValid()).toBe(false)
      expect(onReset).not.toHaveBeenCalled()
    })

    test('after a refusal, the next initialize() - what a later send re-runs - acquires a pair', async () => {
      const coordinator = createCoordinator({
        answers: [
          createErrorFromCode(CLUSTER_AUTHORIZATION_FAILED),
          { producerId: '1000', producerEpoch: 0 },
        ],
      })
      const { identity } = build({ transactionalId: null, coordinator })

      await expect(identity.initialize()).rejects.toThrow(KafkaJSInvalidConfigurationError)
      await identity.initialize()

      expect(identity.isValid()).toBe(true)
      expect(sent(coordinator)).toHaveLength(2)
    })

    test('any other error rejects as the same object and changes nothing', async () => {
      const timedOut = createErrorFromCode(REQUEST_TIMED_OUT)
      const coordinator = createCoordinator({ answers: [timedOut] })
      const { identity, onReset } = build({ coordinator })

      await expect(identity.initialize()).rejects.toBe(timedOut)
      expect(pairOf(identity)).toEqual({ producerId: -1, producerEpoch: -1 })
      expect(onReset).not.toHaveBeenCalled()
    })
  })

  describe('reinitialize()', () => {
    test('sends the pair it holds and takes the bumped epoch (KIP-360)', async () => {
      const coordinator = createCoordinator({
        answers: [
          { producerId: '1000', producerEpoch: 0 },
          { producerId: '1000', producerEpoch: 1 },
        ],
      })
      const { identity, onReset, findBroker } = build({ coordinator })
      await identity.initialize()

      await identity.reinitialize()

      expect(sent(coordinator)[1]).toEqual({
        transactionalId: TRANSACTIONAL_ID,
        transactionTimeout: TRANSACTION_TIMEOUT,
        producerId: '1000',
        producerEpoch: 0,
      })
      expect(pairOf(identity)).toEqual({ producerId: '1000', producerEpoch: 1 })
      expect(onReset).toHaveBeenCalledTimes(2)
      expect(findBroker).toHaveBeenCalledTimes(2)
    })

    test('accepts a new producer id: the coordinator allocates one when the epoch is exhausted', async () => {
      const coordinator = createCoordinator({
        answers: [
          { producerId: '1000', producerEpoch: 32767 },
          { producerId: '2000', producerEpoch: 0 },
        ],
      })
      const { identity } = build({ coordinator })
      await identity.initialize()

      await identity.reinitialize()

      expect(pairOf(identity)).toEqual({ producerId: '2000', producerEpoch: 0 })
    })

    test('accepts a different producer id even when the epoch did not move', async () => {
      const coordinator = createCoordinator({
        answers: [
          { producerId: '1000', producerEpoch: 3 },
          { producerId: '2000', producerEpoch: 3 },
        ],
      })
      const { identity, onReset } = build({ coordinator })
      await identity.initialize()

      await identity.reinitialize()

      expect(pairOf(identity)).toEqual({ producerId: '2000', producerEpoch: 3 })
      expect(onReset).toHaveBeenCalledTimes(2)
    })

    test('an answer that bumped nothing is an application-recoverable error, and the pair is kept', async () => {
      const coordinator = createCoordinator({
        answers: [
          { producerId: '1000', producerEpoch: 3 },
          { producerId: '1000', producerEpoch: 3 },
        ],
      })
      const { identity, onReset } = build({ coordinator })
      await identity.initialize()

      const error = await identity.reinitialize().catch((e) => e)

      expect(error).toBeInstanceOf(KafkaJSApplicationRecoverableError)
      expect(error.message).toContain('did not bump the epoch')
      expect(error.category).toEqual('applicationRecoverable')
      expect(transactionStateFor(error)).toEqual('FATAL_ERROR')
      expect(pairOf(identity)).toEqual({ producerId: '1000', producerEpoch: 3 })
      expect(onReset).toHaveBeenCalledTimes(1)
    })

    test('a producer id held as a number and answered as the same decimal string is no bump', async () => {
      const coordinator = createCoordinator({ answers: [{ producerId: '1000', producerEpoch: 3 }] })
      const { identity } = build({ coordinator })
      identity.adopt({ producerId: 1000, producerEpoch: 3 })

      await expect(identity.reinitialize()).rejects.toThrow(KafkaJSApplicationRecoverableError)
    })

    test('a fenced epoch rejects as the same object and leaves the pair alone', async () => {
      const fenced = createErrorFromCode(PRODUCER_FENCED)
      const coordinator = createCoordinator({
        answers: [{ producerId: '1000', producerEpoch: 3 }, fenced],
      })
      const { identity, onReset } = build({ coordinator })
      await identity.initialize()

      await expect(identity.reinitialize()).rejects.toBe(fenced)
      expect(fenced.category).toEqual('applicationRecoverable')
      expect(pairOf(identity)).toEqual({ producerId: '1000', producerEpoch: 3 })
      expect(onReset).toHaveBeenCalledTimes(1)
    })

    test('a coordinator at InitProducerId v3 takes the KIP-360 path', async () => {
      const coordinator = createCoordinator({
        maxVersion: 3,
        answers: [
          { producerId: '1000', producerEpoch: 3 },
          { producerId: '1000', producerEpoch: 4 },
        ],
      })
      const { identity } = build({ coordinator })
      await identity.initialize()

      await identity.reinitialize()

      expect(sent(coordinator)[1]).toMatchObject({ producerId: '1000', producerEpoch: 3 })
      expect(pairOf(identity)).toEqual({ producerId: '1000', producerEpoch: 4 })
    })

    test.each([2, null])(
      'a coordinator whose InitProducerId version is %p gets a plain first initialization: no pair sent, no bump check',
      async (maxVersion) => {
        const coordinator = createCoordinator({
          maxVersion,
          answers: [
            { producerId: '1000', producerEpoch: 3 },
            { producerId: '1000', producerEpoch: 3 },
          ],
        })
        const { identity, onReset } = build({ coordinator })
        await identity.initialize()

        await identity.reinitialize()

        expect(coordinator.negotiatedVersion).toHaveBeenCalledWith(
          apiKeys.InitProducerId,
          requests.InitProducerId
        )
        expect(sent(coordinator)[1]).toMatchObject({ producerId: -1, producerEpoch: -1 })
        expect(pairOf(identity)).toEqual({ producerId: '1000', producerEpoch: 3 })
        expect(onReset).toHaveBeenCalledTimes(2)
      }
    )

    test('CLUSTER_AUTHORIZATION_FAILED on a re-initialization is the same configuration error', async () => {
      const coordinator = createCoordinator({
        answers: [
          { producerId: '1000', producerEpoch: 3 },
          createErrorFromCode(CLUSTER_AUTHORIZATION_FAILED),
        ],
      })
      const { identity } = build({ coordinator })
      await identity.initialize()

      await expect(identity.reinitialize()).rejects.toThrow(REFUSED_MESSAGE)
      expect(pairOf(identity)).toEqual({ producerId: '1000', producerEpoch: 3 })
    })
  })

  describe('bumpEpoch()', () => {
    const initialized = async (producerEpoch, answers = [], producerId = '1000') => {
      const coordinator = createCoordinator({
        answers: [{ producerId, producerEpoch }, ...answers],
      })
      const built = build({ transactionalId: null, coordinator })
      await built.identity.initialize()
      return { ...built, coordinator }
    }

    test('increments the epoch locally: same producer id, no request, no full restart', async () => {
      const { identity, coordinator, onReset } = await initialized(3)

      identity.bumpEpoch()

      expect(pairOf(identity)).toEqual({ producerId: '1000', producerEpoch: 4 })
      expect(sent(coordinator)).toHaveLength(1)
      expect(onReset).toHaveBeenCalledTimes(1)
      expect(identity.pendingInitialization()).toBeNull()
    })

    test('reaches 32767 by a plain increment', async () => {
      const { identity, coordinator } = await initialized(32766)

      identity.bumpEpoch()

      expect(pairOf(identity)).toEqual({ producerId: '1000', producerEpoch: 32767 })
      expect(sent(coordinator)).toHaveLength(1)
    })

    test('at 32767 it discards the pair and acquires a fresh one instead of overflowing the int16', async () => {
      const { identity, coordinator, onReset } = await initialized(32767, [
        { producerId: '2000', producerEpoch: 0 },
      ])

      identity.bumpEpoch()

      expect(identity.isValid()).toBe(false)
      expect(pairOf(identity)).toEqual({ producerId: -1, producerEpoch: -1 })
      const pending = identity.pendingInitialization()
      expect(pending).toBeInstanceOf(Promise)

      await pending

      expect(pairOf(identity)).toEqual({ producerId: '2000', producerEpoch: 0 })
      expect(identity.pendingInitialization()).toBeNull()
      expect(sent(coordinator)[1]).toEqual({
        transactionalId: null,
        transactionTimeout: TRANSACTION_TIMEOUT,
        producerId: -1,
        producerEpoch: -1,
      })
      expect(sent(coordinator).every(({ producerEpoch }) => producerEpoch <= 32767)).toBe(true)
      // The caller's narrow reset follows the bump; the fresh pair does not restart every partition.
      expect(onReset).toHaveBeenCalledTimes(1)
    })

    test('a failed fresh acquisition rejects pendingInitialization(), never as an unhandled rejection', async () => {
      const timedOut = createErrorFromCode(REQUEST_TIMED_OUT)
      const { identity } = await initialized(32767, [timedOut])

      identity.bumpEpoch()
      await expect(identity.pendingInitialization()).rejects.toBe(timedOut)
      // A leaked rejection reaches jest's own handler on the next macrotask and fails this test.
      await new Promise(setImmediate)

      expect(identity.isValid()).toBe(false)
      expect(identity.pendingInitialization()).toBeNull()
    })

    test('a failed fresh acquisition is logged as a warning naming the producer and the error', async () => {
      const timedOut = createErrorFromCode(REQUEST_TIMED_OUT)
      const { identity, logger } = await initialized(32767, [timedOut])
      const warn = jest.spyOn(logger, 'warn')

      identity.bumpEpoch()
      await identity.pendingInitialization().catch(() => {})
      await new Promise(setImmediate)

      expect(warn).toHaveBeenCalledTimes(1)
      expect(warn).toHaveBeenCalledWith(expect.stringContaining('producer 1000'), {
        producerId: '1000',
        error: timedOut.message,
      })
    })

    test('a producer id of 0, the first a fresh cluster issues, is a real one: valid, and a bump increments it', async () => {
      const { identity } = await initialized(0, [], '0')

      expect(identity.isValid()).toBe(true)
      identity.bumpEpoch()

      expect(pairOf(identity)).toEqual({ producerId: '0', producerEpoch: 1 })
    })

    test('an adopted producer id of "-1" is no pair: isValid() is false and a bump does nothing', () => {
      const { identity } = build({
        transactionalId: null,
        coordinator: createCoordinator({ answers: [] }),
      })
      identity.adopt({ producerId: '-1', producerEpoch: -1 })

      expect(identity.isValid()).toBe(false)
      identity.bumpEpoch()

      expect(pairOf(identity)).toEqual({ producerId: '-1', producerEpoch: -1 })
      expect(identity.pendingInitialization()).toBeNull()
    })

    test('a bump while no pair is held does nothing', () => {
      const coordinator = createCoordinator({ answers: [] })
      const { identity } = build({ transactionalId: null, coordinator })

      identity.bumpEpoch()

      expect(pairOf(identity)).toEqual({ producerId: -1, producerEpoch: -1 })
      expect(identity.pendingInitialization()).toBeNull()
      expect(sent(coordinator)).toHaveLength(0)
    })

    test('a transactional producer never bumps locally', async () => {
      const coordinator = createCoordinator({ answers: [{ producerId: '1000', producerEpoch: 3 }] })
      const { identity } = build({ coordinator })
      await identity.initialize()

      expect(() => identity.bumpEpoch()).toThrow(KafkaJSInvariantViolation)
      expect(pairOf(identity)).toEqual({ producerId: '1000', producerEpoch: 3 })
    })

    test('a transactional producer that holds no pair still throws', () => {
      const { identity } = build({ coordinator: createCoordinator({ answers: [] }) })

      expect(() => identity.bumpEpoch()).toThrow(KafkaJSInvariantViolation)
    })
  })

  describe('adopt()', () => {
    test('takes the pair an EndTxn v5 response carries and restarts every partition, with no request', async () => {
      const coordinator = createCoordinator({ answers: [{ producerId: '1000', producerEpoch: 3 }] })
      const { identity, onReset } = build({ coordinator })
      await identity.initialize()

      identity.adopt({ producerId: '1000', producerEpoch: 4 })

      expect(pairOf(identity)).toEqual({ producerId: '1000', producerEpoch: 4 })
      expect(onReset).toHaveBeenCalledTimes(2)
      expect(sent(coordinator)).toHaveLength(1)
    })
  })
})
