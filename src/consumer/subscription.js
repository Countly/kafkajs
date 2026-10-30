const dictionary = require('../utils/dictionary')

/**
 * Retains subscription declarations and separates desired names from an applied round.
 * Metadata tickets fence installation before evaluation. Owns no timer or network work.
 */
class Subscription {
  /** Creates empty provenance, desired state and metadata identity.
   * @returns {Subscription}
   */
  constructor() {
    this.literals = new Map()
    this.patterns = []
    this.order = 0
    this.revision = 0
    this.generation = 0
    this.sequence = 0
    this.desired = { revision: 0, names: [], topicConfigurations: new Map() }
    this.applied = this.desired
    this.round = null
    this.observed = new Map()
    this.initialized = false
    this.sharedKey = null
  }

  /**
   * Adds validated declarations in call order. Clones patterns without stateful flags.
   * @param {{topics: Array<string|RegExp>, policy: {kind: string, durationMs?: number}}} subscription
   * @returns {void}
   */
  add({ topics, policy }) {
    const firstPattern = !this.hasPatterns() && topics.some((topic) => topic instanceof RegExp)
    const configuration = Object.freeze({
      fromBeginning: policy.kind === 'earliest',
      offsetReset: Object.freeze({ ...policy }),
    })
    for (const topic of topics) {
      const entry = { order: ++this.order, configuration }
      if (topic instanceof RegExp) {
        this.patterns.push({
          ...entry,
          pattern: new RegExp(topic.source, topic.flags.replace(/[gy]/g, '')),
        })
      } else {
        this.literals.set(topic, entry)
      }
    }
    if (firstPattern) this.retireMetadata()
  }

  /** Reports whether discovery needs all topics.
   * @returns {boolean}
   */
  hasPatterns() {
    return this.patterns.length > 0
  }

  /**
   * Captures the declarations and the desired snapshot, for a call that may fail after `add`.
   * @returns {{literals: Map, patterns: object[], desired: object}} opaque value for `restore`
   */
  checkpoint() {
    return { literals: new Map(this.literals), patterns: [...this.patterns], desired: this.desired }
  }

  /**
   * Puts the declarations and the desired snapshot back and retires metadata issued meanwhile.
   * Counters stay monotonic, so no reply of the failed attempt can look current again.
   * @param {{literals: Map, patterns: object[], desired: object}} checkpoint what `checkpoint` returned
   * @returns {void}
   */
  restore({ literals, patterns, desired }) {
    this.literals = literals
    this.patterns = patterns
    this.desired = desired
    this.retireMetadata()
  }

  /**
   * Captures one actual full/named refresh before acquisition or any await.
   * Newer issued work supersedes older replies even if the newer work fails.
   * @returns {{generation: number, sequence: number, declarationRevision: number, isCurrent: () => boolean}}
   */
  captureMetadata() {
    const generation = this.generation
    const sequence = ++this.sequence
    const declarationRevision = this.order
    return {
      generation,
      sequence,
      declarationRevision,
      isCurrent: () =>
        this.generation === generation &&
        this.sequence === sequence &&
        this.order === declarationRevision,
    }
  }

  /**
   * Captures scope for a partial by-id lookup, composed with its membership/target guard.
   * A partial lookup neither supersedes nor evaluates a full discovery response.
   * @returns {() => boolean}
   */
  captureScope() {
    const generation = this.generation
    return () => this.generation === generation
  }

  /**
   * Scope for sharing one in-flight refresh: callers with the same key may join one promise. The
   * key object is replaced when the generation or the declaration revision moves, so a refresh
   * begun before a pattern, an unsubscribe or a later declaration is never joined by newer work.
   * The request sequence is not part of it: a newer issue still fences an older reply by ticket.
   * @returns {{generation: number, declarationRevision: number}} the same object until either changes
   */
  shareKey() {
    if (
      this.sharedKey?.generation !== this.generation ||
      this.sharedKey.declarationRevision !== this.order
    ) {
      this.sharedKey = { generation: this.generation, declarationRevision: this.order }
    }
    return this.sharedKey
  }

  /** Retires metadata on an accepted mode change. Counters are never reset.
   * @returns {void}
   */
  retireMetadata() {
    this.generation += 1
  }

  /**
   * Evaluates accepted metadata synchronously. Literals survive absence/internal flags;
   * topic errors exclude pattern matches, while leaderless partitions do not exclude a name.
   * @param {{topicMetadata: Array<{topic: string, topicErrorCode?: number, isInternal?: boolean}>}} metadata
   * @returns {{revision: number, names: string[], topicConfigurations: Map}} defensive desired snapshot
   */
  evaluate(metadata) {
    const selected = new Map(this.literals)
    for (const row of metadata.topicMetadata) {
      if (row.isInternal || (row.topicErrorCode != null && row.topicErrorCode !== 0)) continue
      for (const entry of this.patterns) {
        if (entry.pattern.test(row.topic) && entry.order > (selected.get(row.topic)?.order ?? -1)) {
          selected.set(row.topic, entry)
        }
      }
    }
    const names = [...selected.keys()].sort()
    const configurations = new Map(names.map((name) => [name, selected.get(name).configuration]))
    const previous = this.desired.topicConfigurations
    const changed =
      names.length !== this.desired.names.length ||
      names.some(
        (name, index) =>
          name !== this.desired.names[index] || previous.get(name) !== configurations.get(name)
      )
    if (changed)
      this.desired = { revision: ++this.revision, names, topicConfigurations: configurations }
    return this.copy(this.desired)
  }

  /**
   * Records only currently observed selected incarnations after accepted full evaluation.
   * The first observation keeps ordinary group-offset behavior. Later additions without a
   * known equal id need the inherited reset path. Unknown ids require proof before offset use.
   * This reports debts; it never discharges or settles the consumer's reset obligations.
   * @param {{topicMetadata: object[]}} metadata the accepted full response
   * @returns {Array<{topic: string, topicId: string|null}>} resets to owe before offset resolution
   */
  observe(metadata) {
    const next = new Map()
    const resets = []
    for (const row of metadata.topicMetadata) {
      if (
        !this.desired.topicConfigurations.has(row.topic) ||
        (row.topicErrorCode != null && row.topicErrorCode !== 0)
      )
        continue
      const topicId =
        row.topicId && row.topicId !== '00000000-0000-0000-0000-000000000000' ? row.topicId : null
      if (this.initialized && (topicId === null || this.observed.get(row.topic) !== topicId)) {
        resets.push({ topic: row.topic, topicId })
      }
      next.set(row.topic, topicId)
    }
    this.observed = next
    this.initialized = true
    return resets
  }

  /** Pins desired once at round entry. Returned state stays private to the round owner.
   * @returns {{revision: number, names: string[], topicConfigurations: Map}}
   */
  pin() {
    if (!this.round) this.round = this.copy(this.desired)
    return this.round
  }

  /**
   * Publishes after old-owner final progress and revoke work. Retired rounds cannot publish.
   * @param {object} snapshot the exact object returned by pin
   * @returns {boolean} whether publication was accepted
   */
  apply(snapshot) {
    if (snapshot !== this.round || snapshot == null) return false
    this.applied = this.copy(snapshot)
    this.round = null
    return true
  }

  /** Releases a failed round without claiming that its desired revision applied.
   * @param {object} snapshot the exact pinned round
   * @returns {void}
   */
  abandon(snapshot) {
    if (snapshot === this.round) this.round = null
  }

  /** Reports an unapplied revision at the next existing round/tick opportunity.
   * @returns {boolean}
   */
  needsApply() {
    return this.desired.revision !== this.applied.revision
  }

  /**
   * Retires run publication, keeping declarations and desired names.
   * Reset obligations and incarnation continuity remain owned by the inherited ledger.
   * @returns {void}
   */
  retireRun() {
    this.retireMetadata()
    this.round = null
    this.applied = { revision: -1, names: [], topicConfigurations: new Map() }
  }

  /** Ends a subscription after stop. Counters remain monotonic.
   * @returns {void}
   */
  clear() {
    this.retireRun()
    this.literals.clear()
    this.patterns = []
    this.observed.clear()
    this.initialized = false
    this.desired = { revision: ++this.revision, names: [], topicConfigurations: new Map() }
    this.applied = this.desired
  }

  /**
   * Converts a snapshot's configurations to the name-keyed dictionary the offset manager reads.
   * @param {{topicConfigurations: Map<string, {fromBeginning: boolean, offsetReset: object}>}} snapshot
   * @returns {Record<string, {fromBeginning: boolean, resetPolicy: object}>} prototype-free, so any topic name is an own key
   */
  static configurationsOf({ topicConfigurations }) {
    const configurations = dictionary()
    for (const [name, { fromBeginning, offsetReset }] of topicConfigurations) {
      configurations[name] = { fromBeginning, resetPolicy: offsetReset }
    }
    return configurations
  }

  /** Copies containers; configuration values are recursively frozen at add time.
   * @param {{revision: number, names: string[], topicConfigurations: Map}} snapshot
   * @returns {{revision: number, names: string[], topicConfigurations: Map}}
   */
  copy(snapshot) {
    return {
      revision: snapshot.revision,
      names: [...snapshot.names],
      topicConfigurations: new Map(snapshot.topicConfigurations),
    }
  }
}

module.exports = Subscription
