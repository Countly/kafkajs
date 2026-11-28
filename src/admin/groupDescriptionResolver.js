const {
  KafkaJSBrokerNotFound,
  KafkaJSNonRetriableError,
  KafkaJSNumberOfRetriesExceeded,
  isStaleCoordinator,
} = require('../errors')
const { isRetriable } = require('../retry')
const { AdminDeadline, currentDeadline, endsCall, isAnsweredError } = require('./deadline')

/**
 * Resolves both group description APIs for one call. An id's retrier owns its only outstanding
 * attempt. Attempts that have a node join the next dispatch for that node and API.
 */
class GroupDescriptionResolver {
  /**
   * @param {object} options
   * @param {import('../cluster')} options.cluster coordinator discovery and dispatch
   * @param {object} options.retry existing retry settings
   * @param {object} [options.operation] original consumer operation, when called by the consumer
   * @param {{ask: Function, outcomes: Function}} options.consumer native API and row reader
   * @param {{ask: Function, outcomes: Function}} options.classic classic API and row reader
   */
  constructor({ cluster, retry, operation, consumer, classic }) {
    this.cluster = cluster
    this.consumerOperation = operation
    this.parent = currentDeadline()
    this.scope = new AdminDeadline(
      'describeGroups',
      null,
      this.parent ? { guard: this.parent.guard } : null
    )
    this.retrier = operation ? (work) => operation.retry(work, retry) : this.scope.retrier(retry)
    this.consumer = { ...consumer, ready: new Map() }
    this.classic = { ...classic, ready: new Map() }
    this.results = { consumer: [], classic: [] }
    this.flushHandle = null
    this.scope.own(() => {
      clearImmediate(this.flushHandle)
      this.consumer.ready.clear()
      this.classic.ready.clear()
    })
  }

  /**
   * Runs each wire id through its own retry budget and transitions fallback ids as they are ready.
   * The original deadline owns the resolver; its terminal refusal also fences transport work.
   * @param {string[]} ids distinct wire ids
   * @returns {Promise<{consumer: object[], classic: object[]}>} resolved rows by API
   */
  async resolve(ids) {
    const entries = new Map(ids.map((id) => [id, { id, nodeId: null, relocating: false }]))
    this.batchRequests = entries.size > 1
    const unown = this.parent?.own((error) => this.scope.stop(error))
    try {
      await this.scope.wait(() =>
        Promise.all([...entries.values()].map((entry) => this.#resolveId(entry)))
      )
      this.parent?.check()
      return this.results
    } finally {
      unown?.()
      this.scope.stop(new KafkaJSNonRetriableError('Group descriptions completed'))
    }
  }

  /**
   * Resolves one wire id. A classic fallback starts a fresh budget and retains the route by node id.
   * @param {object} entry call-owned state for one wire id
   * @returns {Promise<void>}
   */
  async #resolveId(entry) {
    try {
      const result = await this.#resolveApi(entry, this.consumer)
      this.scope.check()
      if (result.fallback) {
        const classic = await this.#resolveApi(entry, this.classic)
        this.scope.check()
        if (classic?.row) this.results.classic.push({ row: classic.row, missing: result.missing })
      } else {
        this.results.consumer.push(result.row)
      }
    } catch (error) {
      this.scope.stop(error)
      throw error
    }
  }

  /**
   * Uses the existing counted retry executor for one id on one API. A refresh failure leaves
   * relocation pending, including when the refresh succeeded earlier but discovery failed.
   * @param {object} entry wire id and its route state
   * @param {object} api request and outcome readers
   * @returns {Promise<object|undefined>} the outcome, or a classic response with no row for the id
   */
  #resolveApi(entry, api) {
    return this.retrier(async () => {
      try {
        this.scope.check()
        const fence = this.consumerOperation
        if (entry.relocating) {
          await this.cluster.refreshMetadata(...(fence ? [{ fence }] : []))
          this.scope.check()
        }
        if (entry.nodeId === null || entry.relocating) {
          const broker = await this.cluster.findGroupCoordinator({
            groupId: entry.id,
            ...(fence && { fence }),
            guard: this.scope.guard,
          })
          this.scope.check()
          entry.nodeId = broker.nodeId
        }
        return await this.#enqueue(api, entry)
      } catch (error) {
        if (!this.#stopOnTerminal(error)) {
          if (isStaleCoordinator(error)) entry.relocating = true
          if (error instanceof KafkaJSBrokerNotFound) entry.nodeId = null
        }
        throw error
      }
    })
  }

  /**
   * Queues one located attempt. The next turn batches ready ids without waiting for other
   * discovery or request promises. A call for one id can dispatch right away.
   * @param {object} api owning API
   * @param {object} entry located id and node
   * @returns {Promise<object|undefined>} this id's outcome from its batch
   */
  #enqueue(api, entry) {
    return new Promise((resolve, reject) => {
      const unown = this.scope.own(reject)
      if (this.scope.guard()) return
      const { nodeId } = entry
      let batch = api.ready.get(nodeId)
      if (!batch) api.ready.set(nodeId, (batch = new Map()))
      batch.set(entry.id, {
        entry,
        resolve: (value) => {
          unown()
          resolve(value)
        },
        reject: (error) => {
          unown()
          reject(error)
        },
      })
      if (!this.batchRequests) {
        this.#flush()
      } else if (this.flushHandle === null) {
        this.flushHandle = setImmediate(() => this.#flush())
      }
    })
  }

  /**
   * Hands off all ready batches to independent requests and leaves later arrivals a fresh queue.
   * @returns {void}
   */
  #flush() {
    this.flushHandle = null
    if (this.scope.guard()) return
    for (const api of [this.consumer, this.classic]) {
      for (const [nodeId, batch] of api.ready) {
        api.ready.delete(nodeId)
        this.#send(api, nodeId, batch)
      }
    }
  }

  /**
   * Dispatches through the pool's current broker and settles only ids carried in this request.
   * Checks terminal results before releasing any sibling to another attempt or API.
   * @param {object} api owning API
   * @param {number} nodeId coordinator node id
   * @param {Map<string, object>} batch the in-flight attempts by wire id
   * @returns {Promise<void>}
   */
  async #send(api, nodeId, batch) {
    try {
      const ids = [...batch.keys()]
      const rows = await this.cluster.withCoordinatorBroker(
        nodeId,
        (broker) => {
          this.scope.check()
          for (const { entry } of batch.values()) entry.relocating = false
          return api.ask(broker, ids)
        },
        { fence: this.consumerOperation }
      )
      this.scope.check()
      const outcomes = api.outcomes(rows, new Set(ids))
      for (const { error } of outcomes.values()) {
        if (error !== undefined) this.#stopOnTerminal(error)
      }
      this.scope.check()
      for (const [id, outcome] of outcomes) {
        const attempt = batch.get(id)
        batch.delete(id)
        if (outcome.error === undefined) attempt.resolve(outcome)
        else attempt.reject(outcome.error)
      }
      for (const { resolve } of batch.values()) resolve()
    } catch (error) {
      this.#stopOnTerminal(error)
      for (const { reject } of batch.values()) reject(error)
    }
  }

  /**
   * Refuses queued writes and late continuations as soon as an error cannot be retried.
   * @param {Error} error original lookup, request or row failure
   * @returns {boolean} whether the call is closed
   */
  #stopOnTerminal(error) {
    if (this.scope.guard()) return true
    if (
      endsCall(error) ||
      isAnsweredError(error) ||
      error instanceof KafkaJSNumberOfRetriesExceeded
    ) {
      this.scope.stop(error)
    } else if (error == null || !isRetriable(error)) {
      this.scope.stop(new KafkaJSNonRetriableError(error, { cause: error?.cause || error }))
    } else {
      return false
    }
    return true
  }
}

module.exports = GroupDescriptionResolver
