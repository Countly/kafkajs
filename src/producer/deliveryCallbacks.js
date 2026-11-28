const { AsyncLocalStorage } = require('node:async_hooks')
const { types } = require('node:util')
const { KafkaJSNonRetriableError } = require('../errors')
const { ownedBuffer } = require('./state/recordSize')

/** Preserves codec errors and gives non-error throws the ordinary encoding-failure path. */
const codecError = (error) =>
  error instanceof Error
    ? error
    : new Error('A custom codec threw a non-Error value', { cause: error })

/** Copies the Buffer view while keeping its shared bytes. */
const routingValue = (value) => (Buffer.isBuffer(value) ? ownedBuffer(value) : value)

/** Isolates a custom partitioner's record, including repeated header values. */
const routingRecord = (message) => {
  const { headers } = message
  const copy = { ...message }
  if (Buffer.isBuffer(copy.key)) copy.key = ownedBuffer(copy.key)
  if (Buffer.isBuffer(copy.value)) copy.value = ownedBuffer(copy.value)
  if (headers != null) {
    const headerCopy = { ...headers }
    for (const name of Object.keys(headerCopy)) {
      const header = headerCopy[name]
      headerCopy[name] = Array.isArray(header)
        ? Object.freeze(header.map(routingValue))
        : routingValue(header)
    }
    copy.headers = Object.freeze(headerCopy)
  }
  return Object.freeze(copy)
}

/** Tracks custom delivery code for one producer, including its transactions. */
class DeliveryCallbacks {
  #partitionerDepth = 0
  #codec = new AsyncLocalStorage()

  /** Throws before a public producer or transaction operation can do work. */
  check = () => {
    if (this.#partitionerDepth !== 0 || this.#codec.getStore()?.active) {
      throw new KafkaJSNonRetriableError(
        'A custom partitioner or compression codec cannot call its own producer or transaction'
      )
    }
  }

  /**
   * Gives the partitioner private routing inputs and refuses reentry while it runs.
   * @param {Function} partitioner
   * @returns {Function}
   */
  partitioner(partitioner) {
    return (routing) => {
      routing.message = routingRecord(routing.message)
      this.#partitionerDepth += 1
      try {
        const partition = partitioner(routing)
        if (types.isPromise(partition)) partition.catch(() => {})
        return partition
      } finally {
        this.#partitionerDepth -= 1
      }
    }
  }

  /**
   * Runs a custom codec factory and compressor with the same per-batch context.
   * @param {Function} factory
   * @param {object} receiver
   * @returns {{ compress: Function }}
   */
  codec(factory, receiver) {
    const context = { active: true }
    let implementation
    try {
      implementation = this.#codec.run(context, () => Reflect.apply(factory, receiver, []))
    } catch (error) {
      throw codecError(error)
    } finally {
      context.active = false
    }
    return {
      compress: async (...args) => {
        context.active = true
        try {
          return await this.#codec.run(context, async () => implementation.compress(...args))
        } catch (error) {
          throw codecError(error)
        } finally {
          context.active = false
        }
      },
    }
  }
}

module.exports = DeliveryCallbacks
