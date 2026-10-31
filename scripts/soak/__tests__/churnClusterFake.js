const { EventEmitter } = require('node:events')
const { KafkaJSCommitFailedError } = require('../../../index')

const OWN = (topic, partition) => `${topic}|${partition}`

/** A socket the probe's factory wraps; `destroy` closes it on the next tick, as a real one does. */
class FakeSocket extends EventEmitter {
  constructor() {
    super()
    this.destroyed = false
  }

  write() {
    return true
  }

  setKeepAlive() {}

  destroy() {
    if (this.destroyed) return
    this.destroyed = true
    setImmediate(() => this.emit('close'))
  }
}

/**
 * A cluster the public client fakes share: topics with their logs, per-group committed offsets and
 * the groups' live members, with a coordinator that hands each partition to one member.
 */
class FakeCluster {
  constructor() {
    this.topics = new Map()
    this.committed = new Map()
    this.members = new Map()
    this.rebalances = Promise.resolve()
    this.recreations = []
  }

  createTopic(name, partitions) {
    if (this.topics.has(name)) throw new Error(`topic ${name} exists`)
    this.topics.set(
      name,
      Array.from({ length: partitions }, () => [])
    )
    this.#rebalanceAll()
  }

  deleteTopic(name) {
    if (!this.topics.delete(name)) throw new Error(`topic ${name} is unknown`)
    for (const offsets of this.committed.values()) {
      for (const key of [...offsets.keys()]) if (key.startsWith(`${name}|`)) offsets.delete(key)
    }
    for (const member of this.#all()) member.forget(name)
  }

  append(topic, partition, value) {
    const log = this.topics.get(topic)?.[partition]
    if (!log) throw new Error(`no partition ${topic}-${partition}`)
    log.push(value)
    for (const member of this.#all()) member.wake()
    return log.length - 1
  }

  commit(group, topic, partition, offset) {
    if (!this.committed.has(group)) this.committed.set(group, new Map())
    this.committed.get(group).set(OWN(topic, partition), offset)
  }

  committedOffset(group, topic, partition) {
    return this.committed.get(group)?.get(OWN(topic, partition))
  }

  join(member) {
    if (!this.members.has(member.group)) this.members.set(member.group, new Set())
    this.members.get(member.group).add(member)
    this.#rebalance(member.group)
  }

  leave(member) {
    this.members.get(member.group)?.delete(member)
    this.#rebalance(member.group)
  }

  #all() {
    return [...this.members.values()].flatMap((set) => [...set])
  }

  #rebalanceAll() {
    for (const group of this.members.keys()) this.#rebalance(group)
  }

  #rebalance(group) {
    this.rebalances = this.rebalances.then(() => this.#assign(group))
    return this.rebalances
  }

  async #assign(group) {
    const members = [...(this.members.get(group) ?? [])]
    const wanted = new Map(members.map((member) => [member, new Map()]))
    for (const [topic, logs] of this.topics) {
      const interested = members.filter((member) => member.wants(topic))
      for (let partition = 0; partition < logs.length && interested.length > 0; partition++) {
        const owned = wanted.get(interested[partition % interested.length])
        if (!owned.has(topic)) owned.set(topic, [])
        owned.get(topic).push(partition)
      }
    }
    for (const member of members) await member.revokeLost(wanted.get(member))
    for (const member of members) await member.assignGained(wanted.get(member))
  }
}

/** A public client: it opens a socket through the probe's factory on connect and closes it on disconnect. */
class FakeClient {
  constructor(cluster, socketFactory) {
    this.cluster = cluster
    this.socketFactory = socketFactory
    this.events = {}
    this.socket = null
  }

  on() {
    return () => {}
  }

  async connect() {
    this.socket ??= this.socketFactory({ host: 'broker', port: 9092 })
  }

  async disconnect() {
    this.socket?.destroy()
    this.socket = null
  }
}

class FakeAdmin extends FakeClient {
  async createTopics({ topics }) {
    for (const { topic, numPartitions } of topics) this.cluster.createTopic(topic, numPartitions)
  }

  async deleteTopics({ topics }) {
    for (const topic of topics) this.cluster.deleteTopic(topic)
  }

  async fetchOffsets({ groupId, topics }) {
    return topics.map((topic) => ({
      topic,
      partitions: (this.cluster.topics.get(topic) ?? []).map((_, partition) => ({
        partition,
        offset: this.cluster.committedOffset(groupId, topic, partition) ?? '-1',
      })),
    }))
  }
}

class FakeProducer extends FakeClient {
  async send({ topic, messages }) {
    await Promise.resolve()
    const written = messages.map(({ partition, value }) => ({
      topicName: topic,
      partition,
      baseOffset: String(this.cluster.append(topic, partition, value)),
    }))
    if (this.cluster.ackLag) await new Promise(setImmediate)
    return written
  }
}

/** A group member that owns what the coordinator gives it and delivers each owned log from its position. */
class FakeConsumer extends FakeClient {
  constructor(cluster, socketFactory, { groupId }) {
    super(cluster, socketFactory)
    this.group = groupId
    this.subscriptions = []
    this.owned = new Map()
    this.positions = new Map()
    this.paused = new Set()
    this.pumping = false
    this.joined = false
  }

  async subscribe({ topic }) {
    this.subscriptions.push(topic)
  }

  wants(topic) {
    return this.subscriptions.some((s) => (s instanceof RegExp ? s.test(topic) : s === topic))
  }

  async run({ rebalanceListener, eachMessage }) {
    this.listener = { onPartitionsAssigned() {}, onPartitionsRevoked() {}, ...rebalanceListener }
    this.eachMessage = eachMessage
    this.joined = true
    this.cluster.join(this)
    await this.cluster.rebalances
  }

  #target(wanted) {
    const target = new Map()
    for (const [topic, partitions] of wanted ?? []) {
      for (const partition of partitions) target.set(OWN(topic, partition), { topic, partition })
    }
    return target
  }

  #group(list) {
    const topics = new Map()
    for (const { topic, partition } of list) {
      if (!topics.has(topic)) topics.set(topic, [])
      topics.get(topic).push(partition)
    }
    return [...topics].map(([topic, partitions]) => ({ topic, partitions }))
  }

  async revokeLost(wanted) {
    const target = this.#target(wanted)
    const lost = [...this.owned].filter(([key]) => !target.has(key)).map(([, value]) => value)
    if (lost.length === 0) return
    await this.listener.onPartitionsRevoked({ partitions: this.#group(lost) })
    for (const { topic, partition } of lost) this.owned.delete(OWN(topic, partition))
  }

  async assignGained(wanted) {
    const target = this.#target(wanted)
    const gained = [...target].filter(([key]) => !this.owned.has(key)).map(([, value]) => value)
    if (gained.length > 0) {
      for (const { topic, partition } of gained) {
        this.owned.set(OWN(topic, partition), { topic, partition })
        const committed = this.cluster.committedOffset(this.group, topic, partition)
        this.positions.set(OWN(topic, partition), committed === undefined ? 0 : Number(committed))
      }
      await this.listener.onPartitionsAssigned({ partitions: this.#group(gained) })
    }
    this.wake()
  }

  forget(topic) {
    for (const key of this.positions.keys())
      if (key.startsWith(`${topic}|`)) this.positions.set(key, 0)
  }

  pause(list) {
    for (const { topic, partitions } of list) {
      for (const partition of partitions) this.paused.add(OWN(topic, partition))
    }
  }

  resume(list) {
    for (const { topic, partitions } of list) {
      for (const partition of partitions) this.paused.delete(OWN(topic, partition))
    }
    this.wake()
  }

  async commitOffsets([{ topic, partition, offset }]) {
    if (!this.owned.has(OWN(topic, partition)))
      throw new KafkaJSCommitFailedError('not owned', { reason: 'membership-lost' })
    this.cluster.commit(this.group, topic, partition, offset)
  }

  wake() {
    if (this.pumping || !this.joined) return
    this.pumping = true
    setImmediate(() => this.#pump())
  }

  async #pump() {
    try {
      for (let again = true; again && this.joined;) {
        again = false
        for (const [key, { topic, partition }] of [...this.owned]) {
          const log = this.cluster.topics.get(topic)?.[partition] ?? []
          while (this.joined && this.owned.has(key) && !this.paused.has(key)) {
            const at = this.positions.get(key) ?? 0
            if (at >= log.length) break
            this.positions.set(key, at + 1)
            again = true
            if (
              this.group.includes('-reader-') &&
              this.cluster.hideFromReaders?.(topic, partition, at)
            )
              continue
            await this.eachMessage({
              topic,
              partition,
              message: { offset: String(at), value: log[at] },
            })
          }
        }
      }
    } finally {
      this.pumping = false
    }
  }

  async disconnect() {
    if (this.joined) {
      this.joined = false
      this.cluster.leave(this)
      await this.cluster.rebalances
    }
    this.owned.clear()
    await super.disconnect()
  }
}

/**
 * The public `Kafka` surface the churn workloads use, over a `FakeCluster`; every client opens its
 * socket through `socketFactory`, so a probe wrapping that factory sees what stays open.
 */
class FakeKafka {
  constructor({ cluster = new FakeCluster(), socketFactory }) {
    this.cluster = cluster
    this.socketFactory = socketFactory
    this.built = []
  }

  #build(client) {
    this.built.push(client)
    return client
  }

  admin() {
    return this.#build(new FakeAdmin(this.cluster, this.socketFactory))
  }

  producer() {
    return this.#build(new FakeProducer(this.cluster, this.socketFactory))
  }

  consumer(options) {
    return this.#build(new FakeConsumer(this.cluster, this.socketFactory, options))
  }
}

module.exports = { FakeCluster, FakeKafka, FakeSocket }
