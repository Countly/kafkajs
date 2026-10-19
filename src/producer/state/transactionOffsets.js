/**
 * Holds immutable transactional offset calls until a confirmed commit. Fences belong to the
 * outstanding calls, then to their coalesced entries; release permanently retires both.
 */
class TransactionOffsets {
  /**
   * @param {import('../../../types').Logger} logger reports a contained publication failure
   */
  constructor(logger) {
    this.logger = logger
    this.entries = new Map()
    this.snapshots = new Set()
    this.tail = Promise.resolve()
  }

  /**
   * Copies every wire field and takes the mutation fence synchronously, before any await.
   * Empty input owns no fence. Neither wire retries nor staging reads the caller's arrays.
   * @param {import('../../consumer/offsetManager/offsetOperations')} ledger
   * @param {string} groupId
   * @param {object[]} topics
   * @returns {object} the call and its lifetime permit
   * @throws {TypeError} for malformed offset arrays, before transmission
   */
  snapshot(ledger, groupId, topics) {
    const copied = Object.freeze(
      topics.map(({ topic, partitions }) =>
        Object.freeze({
          topic,
          partitions: Object.freeze(
            partitions.map(({ partition, offset, leaderEpoch, metadata }) =>
              Object.freeze({ partition, offset, leaderEpoch, metadata })
            )
          ),
        })
      )
    )
    const cells = copied.flatMap(({ topic, partitions }) =>
      partitions.map(({ partition }) => ({ topic, partition }))
    )
    const call = {
      ledger,
      groupId,
      topics: copied,
      ticket: cells.length ? ledger.begin(cells, { mutation: true }) : null,
      remaining: 0,
      ended: false,
    }
    this.snapshots.add(call)
    return call
  }

  /**
   * Runs calls in invocation order, including each call's retries. Failure frees the slot.
   * @param {object} snapshot
   * @param {(snapshot: object) => Promise<void>} work
   * @returns {Promise<void>} the result of this call only
   * @throws {Error} the work's error, unchanged
   */
  enqueue(snapshot, work) {
    const pending = this.tail.then(() => work(snapshot))
    this.tail = pending.catch(() => {})
    return pending
  }

  /**
   * Hands an accepted call's fence to one entry per cell, replacing earlier calls' entries.
   * A late acceptance cannot restore a released permit.
   * @param {object} snapshot
   * @returns {void}
   */
  stage(snapshot) {
    if (snapshot.ended) return
    for (const { topic, partitions } of snapshot.topics) {
      for (const fields of partitions) {
        const key = JSON.stringify([snapshot.groupId, topic, Number(fields.partition)])
        const previous = this.entries.get(key)
        snapshot.remaining += 1
        this.entries.set(key, { snapshot, topic, fields })
        if (previous) {
          previous.snapshot.remaining -= 1
          if (previous.snapshot.remaining === 0) this.abandon(previous.snapshot)
        }
      }
    }
    if (snapshot.remaining === 0) this.abandon(snapshot)
  }

  /**
   * Ends a failed, replaced or empty call's fence. Idempotent.
   * @param {object} snapshot
   * @returns {void}
   */
  abandon(snapshot) {
    if (snapshot.ended) return
    snapshot.ended = true
    if (snapshot.ticket) snapshot.ledger.end(snapshot.ticket)
    this.snapshots.delete(snapshot)
  }

  /**
   * Publishes each still-current cell once, after EndTxn confirmation. Each cell's apply is
   * independent, so one transaction cannot supersede itself. Errors cannot undo a broker commit.
   * @param {import('../../cluster')} cluster
   * @returns {void} never throws, including when logging fails
   */
  publish(cluster) {
    try {
      for (const { snapshot, topic, fields } of this.entries.values()) {
        try {
          const { ledger, ticket, groupId } = snapshot
          const { partition, offset } = fields
          if (!ledger.isSuperseded(ticket, topic, partition)) {
            cluster.markOffsetAsCommitted({ groupId, topic, partition, offset })
            ledger.apply(ticket, topic, partition)
          }
        } catch (error) {
          try {
            this.logger.error('Committed transaction offset cache publication failed', {
              error: error.message,
            })
          } catch {}
        }
      }
    } finally {
      this.release()
    }
  }

  /**
   * Forfeits outstanding and staged permits permanently; never publishes. Idempotent.
   * @returns {void}
   */
  release() {
    for (const snapshot of this.snapshots) this.abandon(snapshot)
    this.entries.clear()
  }
}

module.exports = TransactionOffsets
