const { compareMembers } = require('../memberInfo')

/** What a member reports when it has never held a generation. */
const DEFAULT_GENERATION = -1

/** Separator for the flat partition key. A Kafka topic name cannot contain a NUL. */
const SEP = '\u0000'

/**
 * @param {string} topic
 * @param {number} partition
 * @returns {string} the flat key a partition is tracked by inside this module
 */
const keyOf = (topic, partition) => `${topic}${SEP}${partition}`

/** @param {string} key @returns {string} */
const topicOf = (key) => key.slice(0, key.indexOf(SEP))

/** @param {string} key @returns {number} */
const partitionOf = (key) => Number(key.slice(key.indexOf(SEP) + 1))

/**
 * Groups flat partition keys back into the `{ topic: partitions }` shape the wire codec wants,
 * with topics and partitions both in ascending order so the result is reproducible.
 *
 * @param {Iterable<string>} keys
 * @returns {{[topic: string]: number[]}}
 */
const groupByTopic = (keys) => {
  const byTopic = new Map()

  for (const key of keys) {
    const topic = topicOf(key)
    const partitions = byTopic.get(topic)

    if (partitions) {
      partitions.push(partitionOf(key))
    } else {
      byTopic.set(topic, [partitionOf(key)])
    }
  }

  const result = {}
  for (const topic of [...byTopic.keys()].sort()) {
    result[topic] = byTopic.get(topic).sort((a, b) => a - b)
  }

  return result
}

/**
 * The members kept in Java's `sortedCurrentSubscriptions` order: fewest assigned partitions
 * first, ties broken by member id.
 *
 * The order depends on a mutable assignment, so - exactly as with Java's `TreeSet` - a member is
 * removed before its assignment changes and added back afterwards.
 */
class MembersByLoad {
  /** @param {Map<string, Set<string>>} assignment the live assignment this order reads */
  constructor(assignment) {
    this.assignment = assignment
    /** @type {string[]} */
    this.members = []
  }

  /**
   * Inserts a member at its place in the order. A binary search finds the first member that sorts
   * after it, the index a front-to-back scan stops at, since the array is kept sorted.
   * @param {string} memberId
   * @returns {void}
   */
  add(memberId) {
    const size = this.assignment.get(memberId).size
    let low = 0
    let high = this.members.length

    while (low < high) {
      const middle = (low + high) >>> 1
      const other = this.members[middle]
      const otherSize = this.assignment.get(other).size

      if (otherSize > size || (otherSize === size && other > memberId)) {
        high = middle
      } else {
        low = middle + 1
      }
    }

    this.members.splice(low, 0, memberId)
  }

  /** @param {string} memberId @returns {void} */
  remove(memberId) {
    const index = this.members.indexOf(memberId)

    if (index !== -1) {
      this.members.splice(index, 1)
    }
  }

  /** @returns {string} the least loaded member */
  first() {
    return this.members[0]
  }

  /** @returns {string} the most loaded member */
  last() {
    return this.members[this.members.length - 1]
  }
}

/**
 * Resolves what each member still owns, and whether every member has the same subscription.
 *
 * A partition claimed by two members is decided by generation: the higher generation keeps it,
 * and at the same generation neither does - a stale zombie would otherwise win a claim purely by
 * iteration order.
 *
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractStickyAssignor.java
 * @param {object} input
 * @param {StickyMember[]} input.members already in member-info order
 * @param {Set<string>} input.allTopics topics the group has metadata for
 * @returns {{ subscriptionsEqual: boolean, owned: Map<string, Set<string>>,
 *   doublyClaimed: Set<string>, maxGeneration: number }}
 */
const resolveOwnership = ({ members, allTopics }) => {
  const owned = new Map()
  const doublyClaimed = new Set()
  const owner = new Map()
  const generations = new Map()

  let subscriptionsEqual = true
  let subscribedTopics = null
  let maxGeneration = DEFAULT_GENERATION

  for (const member of members) {
    const { memberId, topics } = member
    const generation = member.generationId == null ? DEFAULT_GENERATION : member.generationId

    if (subscribedTopics === null) {
      subscribedTopics = new Set(topics)
    } else if (
      subscriptionsEqual &&
      !(topics.length === subscribedTopics.size && topics.every((t) => subscribedTopics.has(t)))
    ) {
      subscriptionsEqual = false
    }

    generations.set(memberId, generation)
    maxGeneration = Math.max(maxGeneration, generation)

    const mine = new Set()
    owned.set(memberId, mine)

    for (const { topic, partitions } of member.ownedPartitions) {
      if (!allTopics.has(topic)) {
        continue
      }

      for (const partition of partitions) {
        const key = keyOf(topic, partition)
        const other = owner.get(key)

        if (other === undefined) {
          mine.add(key)
          owner.set(key, memberId)
          continue
        }

        const otherGeneration = generations.get(other)

        if (generation === otherGeneration) {
          doublyClaimed.add(key)
          owned.get(other).delete(key)
          owner.set(key, memberId)
        } else if (generation > otherGeneration) {
          mine.add(key)
          owned.get(other).delete(key)
          owner.set(key, memberId)
        }
      }
    }
  }

  return { subscriptionsEqual, owned, doublyClaimed, maxGeneration }
}

/**
 * The optimised path Java takes when every member subscribes to exactly the same topics.
 *
 * Each member keeps as many of its owned partitions as its quota allows, then the leftovers go
 * round-robin to the members still under quota. `minQuota` and `maxQuota` differ by at most one,
 * so the result is always balanced.
 *
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractStickyAssignor.java
 */
class ConstrainedAssignmentBuilder {
  /**
   * @param {object} input
   * @param {{[topic: string]: number[]}} input.partitionsPerTopic
   * @param {StickyMember[]} input.members in member-info order
   * @param {Map<string, Set<string>>} input.owned
   * @param {Set<string>} input.doublyClaimed
   */
  constructor({ partitionsPerTopic, members, owned, doublyClaimed }) {
    this.partitionsPerTopic = partitionsPerTopic
    this.members = members
    this.owned = owned
    this.doublyClaimed = doublyClaimed

    const total = Object.values(partitionsPerTopic).reduce((sum, p) => sum + p.length, 0)
    this.minQuota = Math.floor(total / members.length)
    this.maxQuota = Math.ceil(total / members.length)
    this.expectedNumMembersWithOverMinQuota = total % members.length
    this.currentNumMembersWithOverMinQuota = 0

    /** @type {Map<string, string[]>} */
    this.assignment = new Map(members.map(({ memberId }) => [memberId, []]))
    /** @type {Set<string>} */
    this.assignedPartitions = new Set()
    /** @type {string[]} */
    this.unfilledUnderMinQuota = []
    /** @type {string[]} */
    this.unfilledExactlyMinQuota = []
  }

  /** @returns {Map<string, string[]>} member id to the partition keys it was assigned */
  build() {
    this.assignOwnedPartitions()

    const unassigned = this.unassignedPartitions()

    this.unfilledUnderMinQuota.sort()
    this.unfilledExactlyMinQuota.sort()
    this.assignRoundRobin(unassigned)

    return this.assignment
  }

  /**
   * Step 1: every member keeps the owned partitions its quota allows, in `(partition, topic)`
   * order - the order that keeps co-partitioned topics together.
   * @private
   * @returns {void}
   */
  assignOwnedPartitions() {
    const { minQuota, maxQuota } = this

    for (const { memberId } of this.members) {
      const owned = [...this.owned.get(memberId)]
        .filter((key) => !this.doublyClaimed.has(key))
        .sort(
          (a, b) =>
            partitionOf(a) - partitionOf(b) ||
            (topicOf(a) < topicOf(b) ? -1 : topicOf(a) > topicOf(b) ? 1 : 0)
        )

      if (owned.length < minQuota) {
        this.keep(memberId, owned)
        this.unfilledUnderMinQuota.push(memberId)
        continue
      }

      if (
        owned.length >= maxQuota &&
        this.currentNumMembersWithOverMinQuota < this.expectedNumMembersWithOverMinQuota
      ) {
        this.currentNumMembersWithOverMinQuota++

        if (this.currentNumMembersWithOverMinQuota === this.expectedNumMembersWithOverMinQuota) {
          this.unfilledExactlyMinQuota.length = 0
        }

        this.keep(memberId, owned.slice(0, maxQuota))
        continue
      }

      this.keep(memberId, owned.slice(0, minQuota))

      if (this.currentNumMembersWithOverMinQuota < this.expectedNumMembersWithOverMinQuota) {
        this.unfilledExactlyMinQuota.push(memberId)
      }
    }
  }

  /**
   * @private
   * @param {string} memberId
   * @param {string[]} keys
   * @returns {void}
   */
  keep(memberId, keys) {
    const assigned = this.assignment.get(memberId)

    for (const key of keys) {
      assigned.push(key)
      this.assignedPartitions.add(key)
    }
  }

  /**
   * @private
   * @returns {string[]} every partition nobody kept, topics sorted, partitions ascending
   */
  unassignedPartitions() {
    const unassigned = []

    for (const topic of Object.keys(this.partitionsPerTopic).sort()) {
      for (const partition of this.partitionsPerTopic[topic]) {
        const key = keyOf(topic, partition)

        if (!this.assignedPartitions.has(key)) {
          unassigned.push(key)
        }
      }
    }

    return unassigned
  }

  /**
   * Step 2: fill the members still under `minQuota`, then the ones sitting exactly on it, up to
   * `maxQuota`. The quotas sum to the partition count, so an unfilled member always exists while
   * partitions remain.
   * @private
   * @param {string[]} unassigned
   * @returns {void}
   */
  assignRoundRobin(unassigned) {
    const underMin = this.unfilledUnderMinQuota
    const exactlyMin = this.unfilledExactlyMinQuota
    let cursor = 0

    for (const key of unassigned) {
      let fromUnderMin = true
      let memberId

      if (cursor < underMin.length) {
        memberId = underMin[cursor++]
      } else if (underMin.length > 0) {
        cursor = 1
        memberId = underMin[0]
      } else {
        memberId = exactlyMin.shift()
        fromUnderMin = false
      }

      const assigned = this.assignment.get(memberId)
      assigned.push(key)

      if (fromUnderMin && assigned.length === this.minQuota) {
        underMin.splice(--cursor, 1)
        exactlyMin.push(memberId)
      } else if (assigned.length === this.maxQuota) {
        this.currentNumMembersWithOverMinQuota++
      }
    }
  }
}

/**
 * Java's general (KIP-54) path, taken as soon as two members subscribe to different topics.
 *
 * It keeps every still-valid previous assignment, hands the rest to the least loaded subscriber,
 * and then repeatedly moves single partitions towards a more balanced assignment.
 *
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractStickyAssignor.java
 */
class GeneralAssignmentBuilder {
  /**
   * @param {object} input
   * @param {{[topic: string]: number[]}} input.partitionsPerTopic
   * @param {StickyMember[]} input.members in member-info order
   * @param {Map<string, Set<string>>} input.owned conflict-resolved ownership
   * @param {number} input.maxGeneration highest generation any member reported
   * @param {boolean} [input.handBackToSubscribersOnly] whether the balancing pass hands a partition
   *   back to its previous owner only while that owner still subscribes to the partition's topic,
   *   which Java's pass does not ask
   */
  constructor({ partitionsPerTopic, members, owned, maxGeneration, handBackToSubscribersOnly }) {
    this.partitionsPerTopic = partitionsPerTopic
    this.members = members
    this.maxGeneration = maxGeneration
    this.handBackToSubscribersOnly = handBackToSubscribersOnly === true
    this.revocationRequired = false

    /** topic to every member that could take one of its partitions, in member-info order */
    this.topicMembers = new Map(Object.keys(partitionsPerTopic).map((topic) => [topic, []]))
    /** the same, as sets, for membership tests inside the hot loops */
    this.topicMemberSet = new Map()
    /** member to every topic it subscribes to that the group has metadata for */
    this.memberTopics = new Map()
    this.memberTopicSet = new Map()
    /** @type {Map<string, Set<string>>} */
    this.assignment = new Map()
    /** @type {Map<string, string>} partition key to the member holding it */
    this.partitionOwner = new Map()
    /** topic to the flat keys of its partitions, built once so the hot loops allocate none */
    this.topicKeys = new Map(
      Object.keys(partitionsPerTopic).map((topic) => [
        topic,
        partitionsPerTopic[topic].map((partition) => keyOf(topic, partition)),
      ])
    )
    /**
     * The last move a full scan found possible, with its member's capacity.
     * @type {{ memberId: string, key: string, capacity: number }|null}
     */
    this.witness = null

    for (const { memberId, topics } of members) {
      const subscribed = topics.filter((topic) => this.topicMembers.has(topic))
      this.memberTopics.set(memberId, subscribed)
      this.memberTopicSet.set(memberId, new Set(subscribed))
      subscribed.forEach((topic) => this.topicMembers.get(topic).push(memberId))
      this.assignment.set(memberId, new Set(owned.get(memberId)))
    }

    this.topicMembers.forEach((ids, topic) => this.topicMemberSet.set(topic, new Set(ids)))

    for (const [memberId, keys] of this.assignment) {
      keys.forEach((key) => this.partitionOwner.set(key, memberId))
    }

    this.sortedPartitions = this.allPartitionsInTopicOrder()
    this.byLoad = new MembersByLoad(this.assignment)
    this.movements = new PartitionMovements()
  }

  /**
   * @private
   * @returns {string[]} every partition, topics ordered by how few members can take them and
   *   then by name - the order the balancing pass walks
   */
  allPartitionsInTopicOrder() {
    const topics = [...this.topicMembers.keys()].sort(
      (a, b) =>
        this.topicMembers.get(a).length - this.topicMembers.get(b).length ||
        (a < b ? -1 : a > b ? 1 : 0)
    )

    return topics.flatMap((topic) => this.topicKeys.get(topic))
  }

  /** @returns {Map<string, Set<string>>} member id to the partition keys it was assigned */
  build() {
    const previousOwner = this.previousOwnersBelowMaxGeneration()

    this.dropInvalidOwnership()
    const unassigned = this.sortedPartitions.filter((key) => !this.partitionOwner.has(key))

    this.members.forEach(({ memberId }) => this.byLoad.add(memberId))
    this.balance(previousOwner, unassigned)

    return this.assignment
  }

  /**
   * @returns {boolean} whether every partition of the built assignment is held by a member that
   *   subscribes to its topic
   */
  assignsOnlyToSubscribers() {
    for (const [memberId, keys] of this.assignment) {
      const subscribed = this.memberTopicSet.get(memberId)

      for (const key of keys) {
        if (!subscribed.has(topicOf(key))) {
          return false
        }
      }
    }

    return true
  }

  /**
   * The owner a partition had in the newest generation *below* the group's maximum. That is where
   * the balancing pass prefers to send a partition back to, which is what makes it sticky.
   * @private
   * @returns {Map<string, { memberId: string, generation: number }>}
   */
  previousOwnersBelowMaxGeneration() {
    const previous = new Map()

    for (const member of this.members) {
      const generation = member.generationId == null ? DEFAULT_GENERATION : member.generationId

      if (generation >= this.maxGeneration) {
        continue
      }

      for (const { topic, partitions } of member.ownedPartitions) {
        for (const partition of partitions) {
          const key = keyOf(topic, partition)
          const known = previous.get(key)

          if (!known || generation > known.generation) {
            previous.set(key, { memberId: member.memberId, generation })
          }
        }
      }
    }

    return previous
  }

  /**
   * Drops every owned partition whose owner no longer subscribes to its topic. Such a revocation
   * is what stops the balancing pass from trying the "only move newly added partitions" round
   * first.
   * @private
   * @returns {void}
   */
  dropInvalidOwnership() {
    for (const [memberId, keys] of this.assignment) {
      const subscribed = this.memberTopicSet.get(memberId)

      for (const key of [...keys]) {
        if (!subscribed.has(topicOf(key))) {
          keys.delete(key)
          this.partitionOwner.delete(key)
          this.revocationRequired = true
        }
      }
    }
  }

  /**
   * @private
   * @param {string[]} unassigned
   * @returns {void} hands every unassigned partition to the least loaded subscriber
   */
  assignUnassigned(unassigned) {
    for (const key of unassigned) {
      const candidates = this.topicMemberSet.get(topicOf(key))
      const memberId = this.byLoad.members.find((id) => candidates.has(id))

      if (memberId !== undefined) {
        this.byLoad.remove(memberId)
        this.assignment.get(memberId).add(key)
        this.partitionOwner.set(key, memberId)
        this.byLoad.add(memberId)
      }
    }
  }

  /**
   * @private
   * @param {string} memberId
   * @returns {number} how many partitions this member could hold at most
   */
  maxAssignmentSize(memberId) {
    return this.memberTopics
      .get(memberId)
      .reduce((sum, topic) => sum + this.partitionsPerTopic[topic].length, 0)
  }

  /**
   * Whether no single move would improve the balance: no member could take a partition of one of
   * its topics from a member holding at least two more.
   *
   * A move found by the last full scan is kept as a witness and checked first: while it is still
   * possible the answer is still false, so the scan, which costs every member's partitions, runs
   * only once the witness has gone. The scan skips a member that holds as many partitions as it
   * could hold, so the witness is no longer possible once its member does: such a member can hold
   * partitions of a topic it left, and the scan never offers it one more. The answer is the same
   * either way.
   * @private
   * @returns {boolean}
   */
  isBalanced() {
    const min = this.assignment.get(this.byLoad.first()).size
    const max = this.assignment.get(this.byLoad.last()).size

    if (min >= max - 1) {
      return true
    }

    const witness = this.witness

    if (
      witness !== null &&
      this.assignment.get(witness.memberId).size !== witness.capacity &&
      this.canMove(witness.memberId, witness.key)
    ) {
      return false
    }

    for (const memberId of this.byLoad.members) {
      const held = this.assignment.get(memberId)
      const capacity = this.maxAssignmentSize(memberId)

      if (held.size === capacity) {
        continue
      }

      for (const topic of this.memberTopics.get(memberId)) {
        for (const key of this.topicKeys.get(topic)) {
          if (this.canMove(memberId, key)) {
            this.witness = { memberId, key, capacity }
            return false
          }
        }
      }
    }

    return true
  }

  /**
   * @private
   * @param {string} memberId a member subscribed to the partition's topic
   * @param {string} key
   * @returns {boolean} whether the member could take the partition from an owner holding at least
   *   two more partitions than it does
   */
  canMove(memberId, key) {
    const held = this.assignment.get(memberId)
    const other = this.assignment.get(this.partitionOwner.get(key))

    return !held.has(key) && other !== undefined && held.size + 1 < other.size
  }

  /**
   * @private
   * @param {string} topic
   * @returns {boolean} a topic only one member can take never needs rebalancing
   */
  topicCanMove(topic) {
    return this.topicMembers.get(topic).length >= 2
  }

  /**
   * @private
   * @param {string} memberId
   * @returns {boolean} whether this member's assignment can still change
   */
  memberCanMove(memberId) {
    const held = this.assignment.get(memberId)

    if (held.size < this.maxAssignmentSize(memberId)) {
      return true
    }

    return [...held].some((key) => this.topicCanMove(topicOf(key)))
  }

  /**
   * Places the unassigned partitions, then moves partitions around until no move improves the
   * balance.
   * @private
   * @param {Map<string, { memberId: string, generation: number }>} previousOwner
   * @param {string[]} unassigned
   * @returns {void}
   */
  balance(previousOwner, unassigned) {
    this.assignUnassigned(unassigned)

    const fixedPartitions = new Set(
      [...this.topicMembers.keys()]
        .filter((topic) => !this.topicCanMove(topic))
        .flatMap((topic) => this.partitionsPerTopic[topic].map((p) => keyOf(topic, p)))
    )

    this.sortedPartitions = this.sortedPartitions.filter((key) => !fixedPartitions.has(key))
    const movable = unassigned.filter((key) => !fixedPartitions.has(key))

    const fixedAssignments = new Map()
    for (const { memberId } of this.members) {
      if (!this.memberCanMove(memberId)) {
        this.byLoad.remove(memberId)
        fixedAssignments.set(memberId, this.assignment.get(memberId))
        this.assignment.delete(memberId)
      }
    }

    if (!this.revocationRequired) {
      this.performReassignments(movable, previousOwner)
    }

    this.performReassignments(this.sortedPartitions, previousOwner)

    fixedAssignments.forEach((keys, memberId) => {
      this.assignment.set(memberId, keys)
      this.byLoad.add(memberId)
    })
  }

  /**
   * @private
   * @param {string} memberId the previous owner of a partition
   * @param {string} key
   * @returns {boolean} whether the partition may go back to `memberId`
   */
  canHandBackTo(memberId, key) {
    return !this.handBackToSubscribersOnly || this.memberTopicSet.get(memberId).has(topicOf(key))
  }

  /**
   * Moves single partitions until a full pass changes nothing or the assignment is balanced. A
   * partition whose owner holds at least two more than its previous owner goes back to that
   * previous owner, when `canHandBackTo` allows it; otherwise it goes to the least loaded
   * subscriber when some subscriber holds at least two fewer than its owner.
   * @private
   * @param {string[]} reassignable
   * @param {Map<string, { memberId: string, generation: number }>} previousOwner
   * @returns {void}
   */
  performReassignments(reassignable, previousOwner) {
    let modified
    this.witness = null

    do {
      modified = false

      for (const key of reassignable) {
        if (this.isBalanced()) {
          break
        }

        // Every reassignable partition belongs to a topic at least two members can take, so its
        // owner passed `memberCanMove` and is still in `assignment`.
        const memberId = this.partitionOwner.get(key)
        const held = this.assignment.get(memberId)
        const previous = previousOwner.get(key)
        const previousHeld =
          previous && this.canHandBackTo(previous.memberId, key)
            ? this.assignment.get(previous.memberId)
            : undefined

        if (previousHeld !== undefined && held.size > previousHeld.size + 1) {
          this.reassign(key, previous.memberId)
          modified = true
          continue
        }

        const better = this.topicMembers
          .get(topicOf(key))
          .some(
            (other) => this.assignment.has(other) && held.size > this.assignment.get(other).size + 1
          )

        if (better) {
          this.reassign(key, this.leastLoadedSubscriber(topicOf(key)))
          modified = true
        }
      }
    } while (modified)
  }

  /**
   * @private
   * @param {string} topic
   * @returns {string} the least loaded member subscribed to `topic`
   */
  leastLoadedSubscriber(topic) {
    const candidates = this.topicMemberSet.get(topic)
    return this.byLoad.members.find((memberId) => candidates.has(memberId))
  }

  /**
   * Moves a partition, asking `PartitionMovements` which partition to move so that a pair of
   * members never keeps swapping the same partition back and forth.
   * @private
   * @param {string} key
   * @param {string} newMemberId
   * @returns {void}
   */
  reassign(key, newMemberId) {
    const toMove = this.movements.actualPartitionToMove(
      key,
      this.partitionOwner.get(key),
      newMemberId
    )
    const from = this.partitionOwner.get(toMove)

    this.byLoad.remove(from)
    this.byLoad.remove(newMemberId)
    this.movements.record(toMove, from, newMemberId)
    this.assignment.get(from).delete(toMove)
    this.assignment.get(newMemberId).add(toMove)
    this.partitionOwner.set(toMove, newMemberId)
    this.byLoad.add(newMemberId)
    this.byLoad.add(from)
  }
}

/**
 * Runs the general path so that its assignment is Java's wherever that assignment is valid, and a
 * corrected one where it is not.
 *
 * Java's balancing pass hands a partition back to its previous owner without asking whether that
 * owner still subscribes to the partition's topic, so its assignment can give a partition to a
 * member that drops it. Asking changes which partition moves where, and with it every later move,
 * even on a shape where Java's answer was valid. So the pass runs first as Java's does, and when
 * every partition lands with a subscriber that assignment is returned as it is. Only otherwise
 * does a second pass run with the subscription checked, so only an invalid shape pays for two.
 *
 * @param {object} input
 * @param {{[topic: string]: number[]}} input.partitionsPerTopic
 * @param {StickyMember[]} input.members in member-info order
 * @param {Map<string, Set<string>>} input.owned conflict-resolved ownership, not changed
 * @param {number} input.maxGeneration highest generation any member reported
 * @returns {Map<string, Set<string>>} member id to the partition keys it was assigned
 */
const buildGeneral = (input) => {
  const unchecked = new GeneralAssignmentBuilder(input)
  const assigned = unchecked.build()

  if (unchecked.assignsOnlyToSubscribers()) {
    return assigned
  }

  return new GeneralAssignmentBuilder({ ...input, handBackToSubscribersOnly: true }).build()
}

/**
 * Tracks which member each partition came from, so that a partition already moved from A to B is
 * preferred over an untouched one when something has to move from B back to A.
 *
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractStickyAssignor.java
 */
class PartitionMovements {
  constructor() {
    /** @type {Map<string, { from: string, to: string }>} */
    this.byPartition = new Map()
    /** @type {Map<string, Map<string, Set<string>>>} topic to "from->to" to partition keys */
    this.byTopic = new Map()
  }

  /**
   * @param {string} key
   * @param {string} from
   * @param {string} to
   * @returns {void} records that `key` moved, collapsing a chain A->B->C into A->C and dropping
   *   the record entirely when the partition returns to where it started
   */
  record(key, from, to) {
    const existing = this.byPartition.get(key)

    if (existing === undefined) {
      this.add(key, from, to)
      return
    }

    this.remove(key)

    if (existing.from !== to) {
      this.add(key, existing.from, to)
    }
  }

  /**
   * @param {string} key the partition the caller wants to move
   * @param {string} from
   * @param {string} to
   * @returns {string} the partition that should move instead - one already travelling the
   *   opposite way, when there is one
   */
  actualPartitionToMove(key, from, to) {
    const forTopic = this.byTopic.get(topicOf(key))

    if (forTopic === undefined) {
      return key
    }

    const existing = this.byPartition.get(key)
    const source = existing === undefined ? from : existing.from
    const reverse = forTopic.get(`${to}${SEP}${source}`)

    return reverse === undefined ? key : reverse.values().next().value
  }

  /**
   * @private
   * @param {string} key
   * @param {string} from
   * @param {string} to
   * @returns {void}
   */
  add(key, from, to) {
    const topic = topicOf(key)
    this.byPartition.set(key, { from, to })

    if (!this.byTopic.has(topic)) {
      this.byTopic.set(topic, new Map())
    }

    const forTopic = this.byTopic.get(topic)
    const pair = `${from}${SEP}${to}`

    if (!forTopic.has(pair)) {
      forTopic.set(pair, new Set())
    }

    forTopic.get(pair).add(key)
  }

  /**
   * @private
   * @param {string} key
   * @returns {void}
   */
  remove(key) {
    const { from, to } = this.byPartition.get(key)
    const topic = topicOf(key)
    const forTopic = this.byTopic.get(topic)
    const pair = `${from}${SEP}${to}`

    this.byPartition.delete(key)
    forTopic.get(pair).delete(key)

    if (forTopic.get(pair).size === 0) {
      forTopic.delete(pair)
    }

    if (forTopic.size === 0) {
      this.byTopic.delete(topic)
    }
  }
}

/**
 * @typedef {Object} StickyMember
 * @property {string} memberId
 * @property {string|null} groupInstanceId
 * @property {string[]} topics
 * @property {Array<{topic: string, partitions: number[]}>} ownedPartitions what the member says
 *   it held in `generationId`
 * @property {number} generationId `-1` when the member has never been assigned anything
 */

/**
 * The sticky assignment algorithm, ported from Java's `AbstractStickyAssignor`.
 *
 * Pure: no cluster, no codecs, no I/O. The caller decodes subscriptions and encodes assignments;
 * this decides only who gets what, so the whole algorithm is testable arm by arm.
 *
 * Two paths, exactly as in Java: a quota-driven one when every member has an identical
 * subscription, and the general KIP-54 balance-and-move loop otherwise. Members are processed in
 * member-info order rather than Java's hash order, so the outcome is reproducible where Java's
 * depends on its `HashMap` iteration.
 *
 * @param {object} input
 * @param {{[topic: string]: number[]}} input.partitionsPerTopic every partition of every topic
 *   any member subscribes to
 * @param {StickyMember[]} input.members
 * @param {Set<string>} [input.contested] filled with the flat keys of the partitions two members
 *   of the same generation both claimed. They belong to nobody: the cooperative caller withholds
 *   them for a round, the eager one has no use for them and passes nothing.
 * @returns {Map<string, {[topic: string]: number[]}>} one entry per member, including members
 *   that end up with nothing
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractStickyAssignor.java
 */
const assignSticky = ({ partitionsPerTopic, members, contested }) => {
  const ordered = [...members].sort(compareMembers)
  const subscribed = new Set(ordered.flatMap(({ topics }) => topics))
  const partitions = {}

  // A topic nobody subscribes to is dropped rather than divided up. Java is never handed one -
  // its caller passes the group's subscription - and its quota-driven path would otherwise hand
  // those partitions to members that cannot fetch them.
  for (const topic of Object.keys(partitionsPerTopic)) {
    if (subscribed.has(topic)) {
      partitions[topic] = [...partitionsPerTopic[topic]].sort((a, b) => a - b)
    }
  }

  const { subscriptionsEqual, owned, doublyClaimed, maxGeneration } = resolveOwnership({
    members: ordered,
    allTopics: new Set(Object.keys(partitions)),
  })

  if (contested) {
    for (const key of doublyClaimed) {
      contested.add(key)
    }
  }

  const assigned = subscriptionsEqual
    ? new ConstrainedAssignmentBuilder({
        partitionsPerTopic: partitions,
        members: ordered,
        owned,
        doublyClaimed,
      }).build()
    : buildGeneral({ partitionsPerTopic: partitions, members: ordered, owned, maxGeneration })

  return new Map(ordered.map(({ memberId }) => [memberId, groupByTopic(assigned.get(memberId))]))
}

module.exports = { assignSticky, DEFAULT_GENERATION, keyOf, topicOf, partitionOf }
