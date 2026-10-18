/**
 * The clients one workload builds. Each is owned from the moment it is built, so a failure anywhere
 * in the workload (in its setup, or half way through a replacement) still leaves every client to
 * disconnect.
 */
class Clients {
  /**
   * @param {object} kafka a `Kafka`
   */
  constructor(kafka) {
    this.kafka = kafka
    this.owned = new Set()
  }

  /**
   * Runs `work` and then disconnects every client it built, whether it ended or failed.
   * @param {object} kafka a `Kafka`
   * @param {(clients: Clients) => Promise<void>} work
   * @returns {Promise<void>}
   * @throws {Error} the failure of `work`, after the disconnects, whatever they did; when `work`
   *   ended well, the first failure of a disconnect, after all were tried
   */
  static async use(kafka, work) {
    const clients = new Clients(kafka)
    try {
      await work(clients)
    } catch (error) {
      await clients.disconnectAll().catch(() => {})
      throw error
    }
    await clients.disconnectAll()
  }

  /** @returns {object} a new producer, owned */
  producer() {
    return this.own(this.kafka.producer())
  }

  /**
   * @param {object} config consumer options
   * @returns {object} a new consumer, owned
   */
  consumer(config) {
    return this.own(this.kafka.consumer(config))
  }

  /** @returns {object} a new admin, owned */
  admin() {
    return this.own(this.kafka.admin())
  }

  /**
   * Disconnects a client the workload is done with, and stops owning it.
   * @param {object} client one this object built
   * @returns {Promise<void>}
   * @throws {Error} the failure of the disconnect; the client stays owned and is tried again
   */
  async release(client) {
    await client.disconnect()
    this.owned.delete(client)
  }

  /**
   * @private
   * @param {object} client
   * @returns {object} the client
   */
  own(client) {
    this.owned.add(client)
    return client
  }

  /**
   * Disconnects every owned client, the last built first, and tries all of them though one rejects.
   * @private
   * @returns {Promise<void>}
   * @throws {Error} the first failure, after the last attempt
   */
  async disconnectAll() {
    const failures = []
    for (const client of [...this.owned].reverse()) {
      try {
        await client.disconnect()
      } catch (error) {
        failures.push(error)
      }
    }
    this.owned.clear()

    if (failures.length > 0) {
      throw failures[0]
    }
  }
}

module.exports = { Clients }
