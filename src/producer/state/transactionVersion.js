/** The finalized feature that carries the transaction version (KIP-890). */
const TRANSACTION_VERSION_FEATURE = 'transaction.version'

/**
 * The transaction version in force: 2 when the transaction coordinator's ApiVersions response
 * finalizes `transaction.version` at level 2 or above with a valid features epoch, 1 otherwise.
 * It is never inferred from the request versions a broker offers.
 *
 * Read at `begin()` - once per transaction - and fixed until the next one. The coordinator's
 * table is read again only when it reports a higher finalized-features epoch than the one the
 * version came from, so a feature change shows up at the next transaction after the coordinator
 * renegotiated. A move from 1 to 2 asks for one epoch bump before the first TV2 transaction; the
 * first read and a move from 2 to 1 ask for none.
 *
 * The Java client reads the version at `initTransactions` (never asking for a bump) and again in
 * `beginCompletingTransaction`, where a move from 1 to 2 sets the bump and chains a
 * re-initialization after the `EndTxn`; its `beginTransaction()` does not read it. This client
 * reads at every `begin()` instead, so a new transaction never starts on a stale version.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-890%3A+Transactions+Server-Side+Defense
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java
 */
class TransactionVersion {
  #version = 1
  #epoch = null

  /**
   * Fixes the version for the transaction being begun.
   * @param {{ finalizedFeatures?: { [name: string]: { minVersionLevel: number, maxVersionLevel: number } },
   *   finalizedFeaturesEpoch?: number }} coordinator the transaction coordinator, as it negotiated
   * @returns {{ version: 1|2, bumpRequired: boolean }} `bumpRequired` when the version moved from
   *   1 to 2 since the previous transaction
   */
  begin({ finalizedFeatures = {}, finalizedFeaturesEpoch = -1 }) {
    if (this.#epoch !== null && !(finalizedFeaturesEpoch > this.#epoch)) {
      return { version: this.#version, bumpRequired: false }
    }

    const feature = finalizedFeatures[TRANSACTION_VERSION_FEATURE]
    const level = feature === undefined ? 0 : feature.maxVersionLevel
    const version = finalizedFeaturesEpoch >= 0 && level >= 2 ? 2 : 1
    const bumpRequired = this.#epoch !== null && this.#version === 1 && version === 2

    this.#epoch = finalizedFeaturesEpoch
    this.#version = version
    return { version, bumpRequired }
  }

  /**
   * @returns {1|2} the version of the current transaction; 1 before the first `begin()`
   */
  current() {
    return this.#version
  }
}

module.exports = TransactionVersion
