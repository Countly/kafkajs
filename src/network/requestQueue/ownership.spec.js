const { channel } = require('node:diagnostics_channel')
const { newLogger } = require('testHelpers')
const {
  currentDeadline,
  runWithDeadline,
  shareMetadata,
  sharedSetup,
} = require('../../admin/deadline')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { NETWORK_REQUEST_QUEUE_SIZE } = require('../instrumentationEvents')
const RequestQueue = require('./index')

/** @returns {{promise: Promise<void>, resolve: Function}} An independently released gate. */
const held = () => {
  let resolve
  const promise = new Promise((yes) => {
    resolve = yes
  })
  return { promise, resolve }
}

/**
 * Starts a real scoped invocation without changing the queue's ownership rules.
 * @param {string} scope admin or discovery
 * @param {Function} work Invocation body.
 * @returns {Promise<*>} Original invocation result.
 */
const runScoped = (scope, work) =>
  scope === 'admin' ? runWithDeadline('queue test', 1000, work) : shareMetadata(work)()

/**
 * Enqueues a request whose wire write is observed without a physical socket.
 * @param {RequestQueue} queue Real request queue.
 * @param {number} correlationId Unique request identity.
 * @param {number[]} sent Written request identities.
 * @param {Function} [guard] Original pre-write guard.
 * @returns {Promise<object>} Original request result or refusal.
 */
const enqueue = (queue, correlationId, sent, guard) =>
  new Promise((resolve, reject) => {
    queue.push({
      entry: { correlationId, resolve, reject },
      expectResponse: false,
      sendRequest: () => sent.push(correlationId),
      guard,
    })
  })

describe('Network > RequestQueue > observer ownership', () => {
  let queue

  afterEach(() => {
    queue?.destroy()
    jest.restoreAllMocks()
  })

  it.each([
    ['admin', 'listener'],
    ['discovery', 'listener'],
    ['admin', 'diagnostics subscriber'],
    ['discovery', 'diagnostics subscriber'],
  ])('a %s request does not own work started by a queue-size %s', async (scope, observer) => {
    const diagnostic = channel('kafkajs.test.queueOwnership')
    const emitter = new InstrumentationEventEmitter({
      channels: { [NETWORK_REQUEST_QUEUE_SIZE]: diagnostic },
    })
    queue = new RequestQueue({
      instrumentationEmitter: emitter,
      maxInFlightRequests: 1,
      requestTimeout: 1000,
      enforceRequestTimeout: false,
      clientId: 'queue-listener',
      broker: 'localhost:9092',
      logger: newLogger(),
    })
    queue.maybeThrottle(1000)
    const gate = held()
    const sent = []
    const listenerScopes = []
    let request, owner, ownedRequest
    /** @returns {void} Holds independent observer work until its publisher has retired. */
    const listener = () => {
      if (request) return
      listenerScopes.push(currentDeadline())
      request = gate.promise.then(() => {
        const pending = enqueue(queue, 2, sent)
        queue.throttledUntil = -1
        queue.checkPendingRequests()
        return pending
      })
    }
    let unsubscribe
    if (observer === 'listener') {
      unsubscribe = emitter.addListener(NETWORK_REQUEST_QUEUE_SIZE, listener)
    } else {
      diagnostic.subscribe(listener)
      unsubscribe = () => diagnostic.unsubscribe(listener)
    }

    try {
      await runScoped(scope, () => {
        owner = currentDeadline()
        const cleanups = owner.cleanups.size
        ownedRequest = enqueue(queue, 1, sent)
        ownedRequest.catch(() => {})
        expect(currentDeadline()).toBe(owner)
        expect(owner.cleanups.size).toBe(cleanups + 1)
        owner.check()
      })
      await expect(ownedRequest).rejects.toBe(owner.reason)
      expect(owner.reason).toBeDefined()
      expect(queue.pending).toEqual([])
      expect(request).toBeDefined()
      gate.resolve()
      await expect(request).resolves.toMatchObject({ size: 0, payload: null })
      expect(listenerScopes).toEqual([undefined])
      expect(sent).toEqual([2])
      expect(queue.isIdle()).toBe(true)
    } finally {
      gate.resolve()
      await request?.catch(() => {})
      unsubscribe()
    }
  })

  it.each(['admin', 'discovery'])(
    'arms and runs the pending-check timer outside its %s requester',
    async (scope) => {
      const emitter = new InstrumentationEventEmitter()
      queue = new RequestQueue({
        instrumentationEmitter: emitter,
        maxInFlightRequests: 1,
        requestTimeout: 1000,
        enforceRequestTimeout: false,
        clientId: 'queue-timer',
        broker: 'localhost:9092',
        logger: newLogger(),
      })
      queue.maybeThrottle(100)
      const gate = held()
      const sent = []
      const armedScopes = []
      const timerScopes = []
      const listenerScopes = []
      const guardScopes = []
      let owner, ownedRequest, independentRequest, request
      const checkPendingRequests = queue.checkPendingRequests
      jest.spyOn(queue, 'checkPendingRequests').mockImplementation(() => {
        timerScopes.push(currentDeadline())
        return checkPendingRequests.call(queue)
      })
      const unsubscribe = emitter.addListener(NETWORK_REQUEST_QUEUE_SIZE, ({ payload }) => {
        if (payload.queueSize !== 0 || request) return
        listenerScopes.push(currentDeadline())
        request = gate.promise.then(() => {
          queue.maybeThrottle(1000)
          const pending = enqueue(queue, 3, sent)
          queue.throttledUntil = -1
          queue.checkPendingRequests()
          return pending
        })
      })
      const armTimeout = global.setTimeout

      try {
        await runScoped(scope, () => {
          owner = currentDeadline()
          const cleanups = owner.cleanups.size
          jest.spyOn(global, 'setTimeout').mockImplementation((...args) => {
            armedScopes.push(currentDeadline())
            return armTimeout(...args)
          })
          ownedRequest = enqueue(queue, 1, sent)
          ownedRequest.catch(() => {})
          sharedSetup(() => {
            independentRequest = enqueue(queue, 2, sent, () => {
              guardScopes.push(currentDeadline())
              return null
            })
          })
          expect(currentDeadline()).toBe(owner)
          expect(owner.cleanups.size).toBe(cleanups + 1)
          owner.check()
        })
        global.setTimeout.mockRestore()
        await expect(ownedRequest).rejects.toBe(owner.reason)
        expect(owner.reason).toBeDefined()
        expect(queue.pending.map(({ correlationId }) => correlationId)).toEqual([2])
        queue.throttledUntil = -1
        await expect(independentRequest).resolves.toMatchObject({ size: 0, payload: null })
        expect(armedScopes).toEqual([undefined])
        expect(timerScopes).toEqual([undefined])
        expect(guardScopes).toEqual([undefined])
        expect(request).toBeDefined()
        gate.resolve()
        await expect(request).resolves.toMatchObject({ size: 0, payload: null })
        expect(listenerScopes).toEqual([undefined])
        expect(sent).toEqual([2, 3])
        expect(queue.isIdle()).toBe(true)
      } finally {
        gate.resolve()
        await request?.catch(() => {})
        unsubscribe()
      }
    }
  )
})
