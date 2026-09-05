module.exports = class SeekOffsets extends Map {
  getKey(topic, partition) {
    return JSON.stringify([topic, partition])
  }

  set(topic, partition, offset) {
    const key = this.getKey(topic, partition)
    super.set(key, offset)
  }

  has(topic, partition) {
    const key = this.getKey(topic, partition)
    return super.has(key)
  }

  pop(topic, partition) {
    if (this.size === 0 || !this.has(topic, partition)) {
      return
    }

    const key = this.getKey(topic, partition)
    const offset = this.get(key)

    this.delete(key)
    return { topic, partition, offset }
  }

  /**
   * Drops every pending seek for one topic. Used when a topic is recreated: a seek against the
   * previous incarnation means nothing against the new one.
   * @param {string} topic
   * @returns {void}
   */
  deleteTopic(topic) {
    for (const key of [...super.keys()]) {
      if (JSON.parse(key)[0] === topic) {
        this.delete(key)
      }
    }
  }
}
