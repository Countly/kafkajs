/**
 * Orders the writes to one group's committed-offset cache and resolved positions.
 *
 * An operation takes a ticket over the cells (topic and partition) it will write. The ledger
 * remembers a cell only while some open ticket covers it, so its size follows the outstanding
 * work and never the number of topic names touched over a run.
 *
 * Reads (offset resolution) fill a cell and never supersede anything. Mutations (seek, commit,
 * transactional publication) are ordered by when they began. Barriers (revoke, assignment
 * rebuild, reset, topic recreation) fence every operation of the covered cells that began before
 * they were applied. State shared across consumers of one group stays here; assignment epochs,
 * ownership tokens and resolved positions stay with the consumer.
 */
class OffsetOperations {
  constructor() {
    /** @type {Map<string, Map<number, {open: number, appliedAt: number, barrierAt: number, appliedIntent: number}>>} */
    this.topics = new Map()
    this.cells = 0
    this.clock = 0
    this.intent = 0
    this.openTickets = 0
    this.onIdle = null
    /** @type {Set<{discharge: (topic: string, partition: number) => void}>} */
    this.attached = new Set()
  }

  /**
   * Opens a ticket over the given cells.
   * @param {{topic: string, partition: number|string}[]} cells the cells the operation will write;
   *   a partition given as a string (an object key) is the same cell as its number
   * @param {{ mutation?: boolean }} [options] a mutation ticket also takes the next intent number
   * @returns {object} a ticket, open until `end`
   */
  begin(cells, { mutation = false } = {}) {
    this.openTickets += 1
    const ticket = {
      began: ++this.clock,
      intent: mutation ? ++this.intent : 0,
      mutation,
      cells: [],
      ended: false,
    }

    for (const { topic, partition } of cells) {
      const number = Number(partition)
      let partitions = this.topics.get(topic)
      if (!partitions) {
        partitions = new Map()
        this.topics.set(topic, partitions)
      }

      let state = partitions.get(number)
      if (!state) {
        state = { open: 0, appliedAt: 0, barrierAt: 0, appliedIntent: 0 }
        partitions.set(number, state)
        this.cells += 1
      }

      state.open += 1
      ticket.cells.push({ topic, partition: number })
    }

    return ticket
  }

  /**
   * Closes a ticket and releases the cells and topics no other open ticket covers. Idempotent.
   * @param {object} ticket
   * @returns {void}
   */
  end(ticket) {
    if (ticket.ended) {
      return
    }

    ticket.ended = true
    this.openTickets -= 1
    for (const { topic, partition } of ticket.cells) {
      const partitions = this.topics.get(topic)
      const state = partitions.get(partition)
      state.open -= 1

      if (state.open === 0) {
        partitions.delete(partition)
        this.cells -= 1
        if (partitions.size === 0) {
          this.topics.delete(topic)
        }
      }
    }
    this.notifyIdle()
  }

  /**
   * Whether the ticket's completion is stale for a cell. A read is stale when a mutation or a
   * barrier was applied to the cell after the ticket began. A mutation is stale when a mutation
   * that began later was already applied to the cell, or a barrier was applied after it began;
   * so the outcome depends on the order the operations began, not on which answer arrived first.
   * @param {object} ticket
   * @param {string} topic
   * @param {number} partition
   * @returns {boolean}
   */
  isSuperseded(ticket, topic, partition) {
    const state = this.state(topic, partition)
    if (!state) {
      return false
    }

    if (state.barrierAt > ticket.began) {
      return true
    }

    return ticket.mutation ? state.appliedIntent > ticket.intent : state.appliedAt > ticket.began
  }

  /**
   * Records that a mutation ticket's result was applied to a cell, and settles the matching reset
   * obligation of every attached consumer. A recreation is a barrier, so a mutation begun under
   * an earlier incarnation was already fenced and never reaches here. Callers check
   * `isSuperseded` first.
   * @param {object} ticket a mutation ticket
   * @param {string} topic
   * @param {number} partition
   * @returns {void}
   */
  apply(ticket, topic, partition) {
    const state = this.state(topic, partition)
    if (state) {
      state.appliedAt = ++this.clock
      state.appliedIntent = Math.max(state.appliedIntent, ticket.intent)
    }

    for (const obligations of this.attached) {
      obligations.discharge(topic, Number(partition))
    }
  }

  /**
   * A lifecycle barrier was applied to one cell: a revoke, an assignment rebuild or a reset.
   * Retains nothing for a cell no open ticket covers.
   * @param {string} topic
   * @param {number} partition
   * @returns {void}
   */
  touch(topic, partition) {
    const state = this.state(topic, partition)
    if (state) {
      state.barrierAt = ++this.clock
    }
  }

  /**
   * A barrier over every covered cell of a topic: a recreation or a topic reset.
   * @param {string} topic
   * @returns {void}
   */
  touchTopic(topic) {
    const partitions = this.topics.get(topic)
    if (!partitions) {
      return
    }

    const at = ++this.clock
    for (const state of partitions.values()) {
      state.barrierAt = at
    }
  }

  /**
   * Links a consumer's reset obligations so an applied mutation can settle them.
   * @param {{discharge: Function}} obligations
   * @returns {() => void} unlinks them
   */
  attach(obligations) {
    this.attached.add(obligations)
    return () => {
      this.attached.delete(obligations)
      this.notifyIdle()
    }
  }

  /**
   * Whether no ticket (including an empty ticket) or attached table still owns this ledger.
   * @returns {boolean} true only after every lifetime has ended
   */
  isIdle() {
    return this.openTickets === 0 && this.attached.size === 0
  }

  /**
   * Notifies the registry at settlement boundaries; standalone ledgers have no callback.
   * @returns {void}
   */
  notifyIdle() {
    if (this.isIdle()) this.onIdle?.()
  }

  /**
   * @returns {number} the cells an open ticket covers, for the resource probe and the tests
   */
  size() {
    return this.cells
  }

  /**
   * @private
   * @param {string} topic
   * @param {number} partition
   * @returns {object|undefined}
   */
  state(topic, partition) {
    const partitions = this.topics.get(topic)
    return partitions && partitions.get(Number(partition))
  }
}

module.exports = OffsetOperations
