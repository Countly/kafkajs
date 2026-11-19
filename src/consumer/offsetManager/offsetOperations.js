const CommittedOffsetRevisions = require('../../cluster/committedOffsetRevisions')

const NO_TOPICS = Object.freeze([])

/**
 * Orders the writes to one group's committed-offset cache and resolved positions, and is the one
 * writer of that cache: every publication, read fill, eviction and topic invalidation goes
 * through the ledger bound to it, so each write advances the cell's revision and the ledger sees
 * every published mutation.
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
 *
 * A ticket may name an owner, and an owner barrier fences only the tickets of that owner, so one
 * participant can cancel its own pending work without invalidating anybody else's.
 *
 * Barriers come in two kinds. An ordinary eviction (revoke, assignment rebuild) clears cached
 * cells; a hard barrier (topic recreation or reset) ends the incarnation. Both fence older work
 * alike. Each covered cell also keeps its durable winner, the highest-intent mutation published
 * to it: an eviction keeps it, a hard barrier drops it.
 *
 * The ledger also answers who owns a topic of the group (an open ticket, or an attached consumer
 * table that holds it) and tells its registry when a topic's last ticket ends or ownership changed,
 * so the group's committed state can retire topics nobody owns.
 */
class OffsetOperations {
  /**
   * @param {object|null} [cache=null] the committed-offset cache this ledger writes, a topic to
   *   partition to offset dictionary; a ledger built without one is bound by `bind`
   */
  constructor(cache = null) {
    this.cache = cache
    /** @type {Map<string, Map<number, {open: number, appliedAt: number, barrierAt: number, hardAt: number, appliedIntent: number, durable: {intent: number, offset: string}|null, ownerBarriers: WeakMap<object, number>|null}>>} */
    this.topics = new Map()
    this.cells = 0
    this.clock = 0
    this.intent = 0
    this.openTickets = 0
    this.onIdle = null
    /** @type {((topic: string) => void)|null} told when the last open ticket of a topic ends */
    this.onTopicIdle = null
    /** @type {(() => void)|null} told when an owner gave up topics */
    this.onRetire = null
    /** @type {Set<{discharge: (topic: string, partition: number) => void}>} */
    this.attached = new Set()
  }

  /**
   * Opens a ticket over the given cells.
   * @param {{topic: string, partition: number|string}[]} cells the cells the operation will write;
   *   a partition given as a string (an object key) is the same cell as its number
   * @param {{ mutation?: boolean, owner?: object|null }} [options] a mutation ticket also takes the
   *   next intent number; an `owner` makes the ticket fenceable by `touchOwned` of that owner
   * @returns {object} a ticket, open until `end`
   */
  begin(cells, { mutation = false, owner = null } = {}) {
    this.openTickets += 1
    const ticket = {
      began: ++this.clock,
      intent: mutation ? ++this.intent : 0,
      mutation,
      owner,
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
        state = {
          open: 0,
          appliedAt: 0,
          barrierAt: 0,
          hardAt: 0,
          appliedIntent: 0,
          durable: null,
          ownerBarriers: null,
        }
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
   * Once the whole ticket is released, each topic it left uncovered is reported to `onTopicIdle`,
   * then idleness to `onIdle`.
   * @param {object} ticket
   * @returns {void}
   */
  end(ticket) {
    if (ticket.ended) {
      return
    }

    ticket.ended = true
    this.openTickets -= 1
    let released = null
    for (const { topic, partition } of ticket.cells) {
      const partitions = this.topics.get(topic)
      const state = partitions.get(partition)
      state.open -= 1

      if (state.open === 0) {
        partitions.delete(partition)
        this.cells -= 1
        if (partitions.size === 0) {
          this.topics.delete(topic)
          released ??= []
          released.push(topic)
        }
      }
    }
    for (const topic of released ?? NO_TOPICS) this.onTopicIdle?.(topic)
    this.notifyIdle()
  }

  /**
   * Whether the ticket's completion is stale for a cell. A read is stale when a mutation or a
   * barrier was applied to the cell after the ticket began. A mutation is stale when a mutation
   * that began later was already applied to the cell, or a barrier was applied after it began;
   * so the outcome depends on the order the operations began, not on which answer arrived first.
   * Either kind is also stale when a barrier of its own owner was applied after it began.
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

    const ownerBarrierAt = ticket.owner === null ? 0 : (state.ownerBarriers?.get(ticket.owner) ?? 0)
    if (state.barrierAt > ticket.began || ownerBarrierAt > ticket.began) {
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
    this.fence(ticket, topic, partition)
    this.discharge(topic, partition)
  }

  /**
   * Records a mutation's intent on a cell without settling any reset obligation, for a mutation
   * whose effect on the cell is immediate but whose settlement waits for a later step (a sentinel
   * seek, settled when its boundary is published). Reads that began earlier and mutations with a
   * lower intent become stale, exactly as for `apply`. Retains nothing for a cell no open ticket
   * covers.
   * @param {object} ticket a mutation ticket
   * @param {string} topic
   * @param {number} partition
   * @returns {void}
   */
  fence(ticket, topic, partition) {
    const state = this.state(topic, partition)
    if (state) {
      state.appliedAt = ++this.clock
      state.appliedIntent = Math.max(state.appliedIntent, ticket.intent)
    }
  }

  /**
   * Settles the matching reset obligation of every attached consumer, without recording anything
   * on the cell: the settlement half of `apply`, for a mutation already fenced by `fence`.
   * @param {string} topic
   * @param {number} partition
   * @returns {void}
   */
  discharge(topic, partition) {
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
   * A barrier over the tickets of one owner on one cell: the owner cancelled its own pending work
   * there. Fences only tickets begun with that owner, never another participant's. Retains nothing
   * for a cell no open ticket covers, and the owner's mark leaves with the cell; it never keeps a
   * retired owner alive. A null owner is ignored.
   * @param {object|null} owner the `owner` its tickets were begun with
   * @param {string} topic
   * @param {number} partition
   * @returns {void}
   */
  touchOwned(owner, topic, partition) {
    const state = this.state(topic, partition)
    if (state && owner !== null) {
      state.ownerBarriers ??= new WeakMap()
      state.ownerBarriers.set(owner, ++this.clock)
    }
  }

  /**
   * A hard barrier over every covered cell of a topic: a recreation or a topic reset. It fences
   * like `touch` and also drops each cell's durable winner.
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
      state.hardAt = at
      state.durable = null
    }
  }

  /**
   * Binds this ledger to its cache once; a bound ledger keeps its own.
   * @param {object} cache the cache a participant would otherwise use
   * @returns {object} the bound cache, which every writer and reader of the pair uses
   */
  bind(cache) {
    this.cache ??= cache
    return this.cache
  }

  /**
   * Publishes a mutation's committed offset: writes the cell, advances its revision and, when the
   * mutation outranks the cell's durable winner, records it as the new one. Callers check
   * `isSuperseded` first and record `apply` or `fence` themselves.
   * @param {object|null} ticket the publishing mutation's ticket; null records no winner
   * @param {string} topic
   * @param {number|string} partition
   * @param {string} offset
   * @returns {void}
   */
  publish(ticket, topic, partition, offset) {
    this.write(topic, partition, offset)
    const state = this.state(topic, partition)
    if (state && ticket?.mutation && ticket.intent >= (state.durable?.intent ?? 0)) {
      state.durable = { intent: ticket.intent, offset }
    }
  }

  /**
   * Fills a cell from a read (offset resolution): writes it and advances its revision, never
   * recording a durable winner.
   * @param {string} topic
   * @param {number|string} partition
   * @param {string} offset
   * @returns {void}
   */
  fill(topic, partition, offset) {
    this.write(topic, partition, offset)
  }

  /**
   * Forgets a cached cell without a barrier and advances its revision, so the next resolution
   * reads the coordinator.
   * @param {string} topic
   * @param {number|string} partition
   * @returns {void}
   */
  forget(topic, partition) {
    CommittedOffsetRevisions.forCache(this.cache).advance(topic, partition)
    if (this.cache[topic]) delete this.cache[topic][partition]
  }

  /**
   * Ordinary eviction of one cell (revoke, assignment rebuild): forgets it and fences older work
   * like `touch`, keeping the cell's durable winner.
   * @param {string} topic
   * @param {number|string} partition
   * @returns {void}
   */
  evict(topic, partition) {
    this.topicCells(topic)
    this.forget(topic, partition)
    this.touch(topic, partition)
  }

  /**
   * Hard invalidation of a topic's cache (topic reset): the barrier of `touchTopic`, a topic
   * revision stamp and an empty topic entry.
   * @param {string} topic
   * @returns {void}
   */
  invalidateTopic(topic) {
    this.touchTopic(topic)
    CommittedOffsetRevisions.forCache(this.cache).clearTopic(topic)
    this.cache[topic] = {}
  }

  /**
   * The cached cells of a topic, created empty when missing: readers index a topic's cells
   * without a guard.
   * @param {string} topic
   * @returns {object} the topic's partition to offset table
   */
  topicCells(topic) {
    this.cache[topic] ??= {}
    return this.cache[topic]
  }

  /**
   * @param {string} topic
   * @param {number|string} partition
   * @returns {{intent: number, offset: string}|null} the highest-intent mutation published to the
   *   cell since its last hard barrier, while a ticket covers the cell
   */
  durable(topic, partition) {
    return this.state(topic, partition)?.durable ?? null
  }

  /**
   * @param {object} ticket
   * @param {string} topic
   * @param {number|string} partition
   * @returns {boolean} whether a hard barrier reached the cell after the ticket began
   */
  isHardBarred(ticket, topic, partition) {
    const state = this.state(topic, partition)
    return state !== undefined && state.hardAt > ticket.began
  }

  /**
   * @private
   * @param {string} topic
   * @param {number|string} partition
   * @param {string} offset
   * @returns {void}
   */
  write(topic, partition, offset) {
    CommittedOffsetRevisions.forCache(this.cache).advance(topic, partition)
    this.topicCells(topic)[partition] = offset
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
   * Whether a topic of this group is owned: an open ticket covers it, or an attached consumer table
   * holds it (subscription, assignment or owed reset). Reads only.
   * @param {string} topic
   * @returns {boolean}
   */
  isOwned(topic) {
    if (this.topics.has(topic)) return true
    for (const table of this.attached) {
      if (table.holds?.(topic)) return true
    }
    return false
  }

  /**
   * Asks the registry to retire the topics nobody owns any longer; a standalone ledger has none.
   * @returns {void}
   */
  retireUnowned() {
    this.onRetire?.()
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
