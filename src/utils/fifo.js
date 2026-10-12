/**
 * A first-in first-out queue whose `shift()` costs O(1) at any size.
 *
 * `Array#shift` is cheap only while V8 can trim the array in place; past about 16 000 elements it
 * moves every remaining element on each call. `Fifo` keeps the items in one array and a head
 * index instead: `shift()` reads the slot at the head and clears it, so a settled item is not
 * retained. When the head passes half of the array the live items are copied out once with
 * `Array#slice`, which is the only place the backing array is rebuilt, so the cost of a shift
 * stays amortised O(1) and the array never holds more than twice the live items.
 *
 * Not safe to change while it is being iterated. Items are compared by identity.
 * @template T
 */
class Fifo {
  /** @type {(T|undefined)[]} cleared slots in front of `#head`, then the live items */
  #items = []
  #head = 0

  /**
   * The number of items in the queue.
   * @returns {number}
   */
  get size() {
    return this.#items.length - this.#head
  }

  /**
   * Adds an item at the back.
   * @param {T} item
   * @returns {void}
   */
  push(item) {
    this.#items.push(item)
  }

  /**
   * Removes the item at the front and returns it.
   * @returns {T|undefined} undefined when the queue is empty
   */
  shift() {
    if (this.size === 0) {
      return undefined
    }

    const item = this.#items[this.#head]
    this.#items[this.#head] = undefined
    this.#head += 1
    if (this.#head === this.#items.length) {
      this.clear()
    } else if (this.#head * 2 > this.#items.length) {
      this.#items = this.#items.slice(this.#head)
      this.#head = 0
    }

    return item
  }

  /**
   * The item at the front, left in place.
   * @returns {T|undefined} undefined when the queue is empty
   */
  peek() {
    return this.at(0)
  }

  /**
   * The item `index` places from the front.
   * @param {number} index
   * @returns {T|undefined} undefined when `index` is outside the queue; a slot in front of the head
   *   is always cleared, so a negative index reads undefined
   */
  at(index) {
    return index < this.size ? this.#items[this.#head + index] : undefined
  }

  /**
   * Inserts an item so that it is `index` places from the front, ahead of what was there.
   * O(size): for the rare re-queue in the middle, not for the hot path.
   * @param {number} index 0 inserts at the front, `size` at the back; clamped to that range
   * @param {T} item
   * @returns {void}
   */
  insertAt(index, item) {
    const position = Math.min(Math.max(index, 0), this.size)
    this.#items.splice(this.#head + position, 0, item)
  }

  /**
   * Removes the first occurrence of an item, by identity. O(size).
   * @param {T} item
   * @returns {boolean} whether the item was in the queue
   */
  remove(item) {
    const index = this.#items.indexOf(item, this.#head)
    if (index === -1) {
      return false
    }

    this.#items.splice(index, 1)
    return true
  }

  /**
   * Empties the queue.
   * @returns {void}
   */
  clear() {
    this.#items.length = 0
    this.#head = 0
  }

  /**
   * The items from front to back. A hand-rolled iterator rather than a generator: the drain loop
   * iterates one queue per partition per pass, and a generator object costs noticeably more to
   * create and step.
   * @returns {Iterator<T>}
   */
  [Symbol.iterator]() {
    let index = this.#head
    return {
      next: () =>
        index < this.#items.length
          ? { value: this.#items[index++], done: false }
          : { value: undefined, done: true },
    }
  }
}

module.exports = Fifo
