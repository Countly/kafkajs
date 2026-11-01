const { bounded } = require('../../../producer/__tests__/fixtures/bounded')

/**
 * A public consumer of the topic and everything it was given and told.
 */
class Member {
  /**
   * @param {object} options
   * @param {import('../../../types').Consumer} options.consumer
   * @param {string} options.topic
   * @param {object} options.subscribe the options of `subscribe` or, with an assignment, of `assign`
   * @param {AbortSignal} options.scope the case's; each startup step runs under it
   * @param {number[]} [options.assignment] the partitions to assign manually instead of subscribing
   */
  constructor({ consumer, topic, subscribe, scope, assignment }) {
    this.assignment = assignment
    this.scope = scope
    this.consumer = consumer
    this.topic = topic
    this.subscribeOptions = subscribe
    /** @type {Array<{ topic: string, partition: number, offset: string, value: string }>} */
    this.delivered = []
    this.halts = []
    this.recreated = []
    this.crashes = []
  }

  /**
   * Connects, subscribes and runs with manual commits.
   * @returns {Promise<void>}
   */
  async start() {
    const { consumer } = this
    consumer.on(consumer.events.PARTITION_HALTED, ({ payload }) => this.halts.push(payload))
    consumer.on(consumer.events.TOPIC_RECREATED, ({ payload }) => this.recreated.push(payload))
    consumer.on(consumer.events.CRASH, ({ payload }) => this.crashes.push(payload.error))
    await bounded(this.scope, Infinity, () => consumer.connect())
    await bounded(this.scope, Infinity, () =>
      this.assignment
        ? consumer.assign(
            [{ topic: this.topic, partitions: this.assignment }],
            this.subscribeOptions
          )
        : consumer.subscribe({ topic: this.topic, ...this.subscribeOptions })
    )
    await bounded(this.scope, Infinity, () => this.run())
  }

  /** @returns {Promise<void>} */
  run() {
    return this.consumer.run({
      autoCommit: false,
      eachMessage: async ({ topic, partition, message }) => {
        this.delivered.push({
          topic,
          partition,
          offset: message.offset,
          value: message.value.toString(),
        })
      },
    })
  }

  /**
   * @param {number} partition
   * @param {number} [from=0] skips the first deliveries
   * @returns {string[]} the offsets delivered for the partition, in order
   */
  offsetsOf(partition, from = 0) {
    return this.delivered
      .slice(from)
      .filter((entry) => entry.partition === partition)
      .map(({ offset }) => offset)
  }
}

module.exports = { Member }
