const Fifo = require('../../utils/fifo')
const {
  KafkaJSBufferExhaustedError,
  KafkaJSDeliveryTimeout,
  KafkaJSRecordTooLargeError,
} = require('../../errors')

/**
 * One admitted send's bytes in the buffer. `release(n)` gives `n` of them back and `releaseAll()`
 * the rest; neither gives back more than was reserved, however often it is called.
 */
class Reservation {
  #remaining
  #give

  /**
   * @param {number} bytes
   * @param {(bytes: number) => void} give returns bytes to the buffer
   */
  constructor(bytes, give) {
    this.#remaining = bytes
    this.#give = give
  }

  /**
   * Gives back `bytes` of the reservation, or what is left of it when that is less.
   * @param {number} bytes
   * @returns {void}
   */
  release(bytes) {
    const returned = Math.min(bytes, this.#remaining)
    if (returned > 0) {
      this.#remaining -= returned
      this.#give(returned)
    }
  }

  /**
   * Gives back what is left of the reservation.
   * @returns {void}
   */
  releaseAll() {
    this.release(this.#remaining)
  }
}

/**
 * @typedef {object} Waiter a send waiting for room
 * @property {number} bytes its charge
 * @property {number} since when it began to wait, epoch ms
 * @property {ReturnType<typeof setTimeout>} timer its `maxBlockMs` or deadline timer
 * @property {(reservation: Reservation) => void} resolve
 * @property {(error: Error) => void} reject
 */

/**
 * Fair, bounded admission of a producer's sends against its `bufferMemory`, as the Java client's
 * `BufferPool` admits batches against `buffer.memory` within `max.block.ms`. A send that fits
 * while nobody waits is granted at once, synchronously: no promise and no timer. Otherwise it waits
 * in a strict FIFO, and a small send behind a large waiter waits too; each waiter has one timer, at
 * `min(maxBlockMs, the time left before its delivery deadline)`. Room comes back when reservations
 * are released and debits repaid, and is granted to the waiters at the head while the head fits.
 * Three terms are counted apart: the reservations of admitted sends (A, at most `capacity`, as a
 * grant needs them and the debits to fit), the charges of waiting sends (W, at most `capacity`:
 * a send that would take them past it is refused at once) and the debits of batches (D), taken
 * without waiting, because an admitted send already holds its records and waiting for more could
 * stall admitted sends on each other. A charge, not a limit on the process's memory. One
 * `Admission` serves a producer's own sends and its transactions'.
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/java/org/apache/kafka/clients/producer/internals/BufferPool.java
 */
class Admission {
  #capacity
  #maxBlockMs
  /** reservations and debits */
  #used = 0
  #debts = 0
  #waitingBytes = 0
  /** @type {Fifo<Waiter>} */
  #waiters = new Fifo()
  #give = (bytes) => {
    this.#used -= bytes
    this.#grant()
  }

  /**
   * @param {object} options
   * @param {number} options.capacity `bufferMemory`, in bytes
   * @param {number} options.maxBlockMs how long a send may wait for room; 0 refuses it at once
   */
  constructor({ capacity, maxBlockMs }) {
    this.#capacity = capacity
    this.#maxBlockMs = maxBlockMs
  }

  /** @returns {number} `bufferMemory` */
  get capacity() {
    return this.#capacity
  }

  /** @returns {number} bytes neither reserved nor debited; never below 0 */
  get available() {
    return Math.max(0, this.#capacity - this.#used)
  }

  /** @returns {number} the bytes the admitted sends hold (A) */
  get reserved() {
    return this.#used - this.#debts
  }

  /** @returns {number} the charges of the sends waiting for room (W) */
  get waitingBytes() {
    return this.#waitingBytes
  }

  /** @returns {number} the debits of the batches not yet retired (D) */
  get debts() {
    return this.#debts
  }

  /**
   * Reserves a send's charge: at once when it fits and nobody waits, otherwise in turn, first come
   * first served.
   * @param {number} bytes the send's charge
   * @param {import('../deadline').Deadline} deadline the send's delivery deadline
   * @returns {Reservation|Promise<Reservation>} a reservation when it is granted at once, else a
   *   promise of one, which rejects when the wait runs out
   * @throws {KafkaJSRecordTooLargeError} `bytes` above `capacity`; nothing waits
   * @throws {KafkaJSBufferExhaustedError} `reason: 'timeout'` with `maxBlockMs` 0, and
   *   `reason: 'queue-full'` when the waiting sends would hold more than `capacity`
   * @throws {KafkaJSDeliveryTimeout} `outcome: 'unsent'`, when the deadline has passed and the send
   *   would have to wait
   */
  acquire(bytes, deadline) {
    if (bytes > this.#capacity) {
      throw new KafkaJSRecordTooLargeError(
        `A send of ${bytes} bytes can never fit in bufferMemory (${this.#capacity} bytes)`,
        { reason: 'bufferMemory', scope: 'send', size: bytes, limit: this.#capacity }
      )
    }
    if (this.#waiters.size === 0 && this.#used + bytes <= this.#capacity) {
      this.#used += bytes
      return new Reservation(bytes, this.#give)
    }

    const remaining = deadline.remaining()
    const byDeadline = remaining < this.#maxBlockMs
    const wait = byDeadline ? remaining : this.#maxBlockMs
    if (wait === 0) {
      throw this.#refusal(byDeadline, bytes, deadline, 0)
    }
    if (this.#waitingBytes + bytes > this.#capacity) {
      throw new KafkaJSBufferExhaustedError(
        `A send of ${bytes} bytes cannot wait for buffer memory: the sends already waiting hold ` +
          `${this.#waitingBytes} of the ${this.#capacity} bytes of bufferMemory`,
        this.#fields('queue-full', bytes, 0)
      )
    }

    return new Promise((resolve, reject) => {
      const waiter = { bytes, since: Date.now(), timer: null, resolve, reject }
      waiter.timer = setTimeout(() => {
        this.#waiters.remove(waiter)
        this.#waitingBytes -= bytes
        reject(this.#refusal(byDeadline, bytes, deadline, Date.now() - waiter.since))
        this.#grant()
      }, wait)
      this.#waiters.push(waiter)
      this.#waitingBytes += bytes
    })
  }

  /**
   * Charges a batch's debit, without waiting: `available` may reach 0, and admission waits until
   * the debit is repaid.
   * @param {number} bytes
   * @returns {void}
   */
  debit(bytes) {
    this.#used += bytes
    this.#debts += bytes
  }

  /**
   * Repays a batch's debit, once, when the batch retires.
   * @param {number} bytes
   * @returns {void}
   */
  repay(bytes) {
    this.#debts -= bytes
    this.#give(bytes)
  }

  /**
   * Grants the waiters at the head of the queue while the head fits.
   * @returns {void}
   */
  #grant() {
    for (
      let waiter = this.#waiters.peek();
      waiter !== undefined && this.#used + waiter.bytes <= this.#capacity;
      waiter = this.#waiters.peek()
    ) {
      this.#waiters.shift()
      clearTimeout(waiter.timer)
      this.#waitingBytes -= waiter.bytes
      this.#used += waiter.bytes
      waiter.resolve(new Reservation(waiter.bytes, this.#give))
    }
  }

  /**
   * @param {boolean} byDeadline whether the delivery deadline, not `maxBlockMs`, ended the wait
   * @param {number} bytes
   * @param {import('../deadline').Deadline} deadline
   * @param {number} waitedMs
   * @returns {KafkaJSDeliveryTimeout|KafkaJSBufferExhaustedError}
   */
  #refusal(byDeadline, bytes, deadline, waitedMs) {
    return byDeadline
      ? new KafkaJSDeliveryTimeout(
          `Gave up waiting for buffer memory: the send's ${deadline.budgetMs} ms delivery deadline passed`,
          { outcome: 'unsent' }
        )
      : new KafkaJSBufferExhaustedError(
          `No buffer memory for a send of ${bytes} bytes within maxBlockMs (${this.#maxBlockMs} ms): ` +
            `${this.available} of the ${this.#capacity} bytes of bufferMemory are free`,
          this.#fields('timeout', bytes, waitedMs)
        )
  }

  /**
   * @param {'timeout'|'queue-full'} reason
   * @param {number} bytes
   * @param {number} waitedMs
   * @returns {object} the fields of a `KafkaJSBufferExhaustedError`
   */
  #fields(reason, bytes, waitedMs) {
    return {
      reason,
      requested: bytes,
      available: this.available,
      capacity: this.#capacity,
      waitedMs,
    }
  }
}

module.exports = { Admission, Reservation }
