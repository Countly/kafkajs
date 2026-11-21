const { createErrorFromCode } = require('../../../protocol/error')

const CONCURRENT_TRANSACTIONS = 51
const PRODUCER_FENCED = 90
const INVALID_TXN_STATE = 48
const COORDINATOR_LOAD_IN_PROGRESS = 14

/**
 * A transaction coordinator for one transactional id, following the rules of Kafka 4.4.0's
 * `TransactionCoordinator.scala` (`prepareInitProducerIdTransit`, `endTransactionWithTV1`,
 * `endTransaction` for version 2) and `TransactionMetadata.java` (`prepareFenceProducerEpoch`,
 * `prepareIncrementProducerEpoch`, `prepareAbortOrCommit`). Transaction markers complete at once
 * unless `markerRounds` holds a transaction in PREPARE_*; producer id rotation, timeouts and
 * coordinator moves are not modelled.
 * @see https://github.com/apache/kafka/blob/4.4.0/core/src/main/scala/kafka/coordinator/transaction/TransactionCoordinator.scala
 * @see https://github.com/apache/kafka/blob/4.4.0/transaction-coordinator/src/main/java/org/apache/kafka/coordinator/transaction/TransactionMetadata.java
 */
class TransactionCoordinatorModel {
  /**
   * @param {object} options
   * @param {1|2} options.version the finalized transaction version
   * @param {string} [options.producerId='1000']
   */
  constructor({ version, producerId = '1000' }) {
    this.version = version
    this.producerId = producerId
    this.epoch = -1
    this.lastEpoch = -1
    this.state = 'EMPTY'
    /** CONCURRENT_TRANSACTIONS answers InitProducerId gets from a PREPARE_* state before its markers complete. */
    this.markerRounds = 0
  }

  /**
   * InitProducerId: -1/-1 asks for no expected epoch. An ONGOING transaction is fenced and
   * aborted, answering CONCURRENT_TRANSACTIONS; version 1 forgets the last epoch on that fence,
   * version 2 keeps it.
   * @param {{ producerId?: string|number, producerEpoch?: number }} request
   * @returns {{ producerId: string, producerEpoch: number }}
   * @throws {Error} CONCURRENT_TRANSACTIONS (51) or PRODUCER_FENCED (90)
   */
  initProducerId({ producerId = -1, producerEpoch = -1 }) {
    const expected = String(producerId) === '-1' ? null : producerEpoch
    if (expected !== null && this.epoch !== -1 && String(producerId) !== this.producerId) {
      throw createErrorFromCode(PRODUCER_FENCED)
    }
    if (this.state.startsWith('PREPARE_')) {
      this.#completeMarkersAfterRound()
      throw createErrorFromCode(CONCURRENT_TRANSACTIONS)
    }
    if (this.state === 'ONGOING') {
      if (this.version === 1) {
        this.epoch += 1
        this.lastEpoch = -1
      } else {
        this.lastEpoch = this.epoch
        this.epoch += 1
      }
      this.state = 'COMPLETE_ABORT'
      throw createErrorFromCode(CONCURRENT_TRANSACTIONS)
    }

    if (expected === null) {
      this.lastEpoch = -1
      this.epoch += 1
    } else if (this.epoch === -1 || expected === this.epoch) {
      this.lastEpoch = this.epoch
      this.epoch += 1
    } else if (expected !== this.lastEpoch) {
      throw createErrorFromCode(PRODUCER_FENCED)
    }
    this.state = 'EMPTY'
    return { producerId: this.producerId, producerEpoch: this.epoch }
  }

  /**
   * AddPartitionsToTxn (version 1), or the partition a version 2 Produce adds implicitly.
   * @param {{ producerEpoch: number }} request
   * @returns {void}
   * @throws {Error} PRODUCER_FENCED (90) for another epoch
   */
  addPartitions({ producerEpoch }) {
    if (producerEpoch !== this.epoch) throw createErrorFromCode(PRODUCER_FENCED)
    this.state = 'ONGOING'
  }

  /**
   * EndTxn from the client. Version 2 bumps the epoch on completion and answers with it; a retry
   * carrying the epoch before that bump is answered with success.
   * @param {{ producerEpoch: number, transactionResult: boolean }} request
   * @returns {{ producerId: string, producerEpoch: number }}
   * @throws {Error} PRODUCER_FENCED (90), CONCURRENT_TRANSACTIONS (51) or INVALID_TXN_STATE (48)
   */
  endTxn({ producerEpoch, transactionResult }) {
    const target = transactionResult ? 'COMMIT' : 'ABORT'
    const retry = this.version === 2 && this.epoch === producerEpoch + 1
    const validEpoch =
      this.version === 1 || this.state === 'ONGOING' || this.state === 'EMPTY'
        ? producerEpoch === this.epoch
        : retry || (this.state.startsWith('COMPLETE_') && producerEpoch === this.epoch)
    if (!validEpoch) throw createErrorFromCode(PRODUCER_FENCED)

    if (this.state === 'ONGOING') {
      if (this.version === 2) {
        this.lastEpoch = this.epoch
        this.epoch += 1
      }
      this.state = `PREPARE_${target}`
      if (this.markerRounds === 0) this.state = `COMPLETE_${target}`
      return this.#answer()
    }
    if (this.state === `PREPARE_${target}`) throw createErrorFromCode(CONCURRENT_TRANSACTIONS)
    if (this.state === `COMPLETE_${target}` && (this.version === 1 || retry)) {
      return this.#answer()
    }
    throw createErrorFromCode(INVALID_TXN_STATE)
  }

  /**
   * Installs the model behind a fake broker's InitProducerId (for the transactional id; one
   * without it keeps the fake's answer), AddPartitionsToTxn, Produce and EndTxn mocks, logging every request as `<api>:<epoch>` in order. `endTxn` decides what
   * happens to each EndTxn: `'answer'` applies and answers it, `'lost'` never delivers it,
   * `'unanswered'` applies it and never answers, `'refuse'` answers COORDINATOR_LOAD_IN_PROGRESS
   * (14, retriable) without applying it.
   * @param {object} broker a fake cluster broker (`fixtures/fakeCluster.js`)
   * @returns {{ log: string[], setEndTxn: (mode: 'answer'|'lost'|'unanswered'|'refuse') => void,
   *   holdAddPartitions: (hold: boolean) => void }}
   */
  install(broker) {
    const log = []
    let endTxnMode = 'answer'
    let addPartitionsHeld = false
    const never = () => new Promise(() => {})
    const initProducerId = broker.initProducerId.getMockImplementation()
    broker.initProducerId.mockImplementation(async (request) => {
      if (request.transactionalId == null) return initProducerId(request)
      log.push(`initProducerId:${request.producerEpoch ?? -1}`)
      return this.initProducerId(request)
    })
    const addPartitions = broker.addPartitionsToTxn.getMockImplementation()
    broker.addPartitionsToTxn.mockImplementation(async (request) => {
      log.push(`addPartitionsToTxn:${request.producerEpoch}`)
      this.addPartitions(request)
      if (addPartitionsHeld) await never()
      return addPartitions(request)
    })
    const produce = broker.produce.getMockImplementation()
    broker.produce.mockImplementation(async (request) => {
      log.push(`produce:${request.producerEpoch}`)
      if (this.version === 2) this.addPartitions(request)
      return produce(request)
    })
    broker.endTxn.mockImplementation(async (request) => {
      log.push(`endTxn:${request.transactionResult ? 'commit' : 'abort'}:${request.producerEpoch}`)
      if (endTxnMode === 'lost') await never()
      if (endTxnMode === 'refuse') throw createErrorFromCode(COORDINATOR_LOAD_IN_PROGRESS)
      const answer = this.endTxn(request)
      if (endTxnMode === 'unanswered') await never()
      return answer
    })
    return {
      log,
      setEndTxn: (mode) => (endTxnMode = mode),
      holdAddPartitions: (hold) => (addPartitionsHeld = hold),
    }
  }

  /** @returns {{ producerId: string, producerEpoch: number }} version 2's pair, -1/-1 for version 1 */
  #answer() {
    return this.version === 2
      ? { producerId: this.producerId, producerEpoch: this.epoch }
      : { producerId: '-1', producerEpoch: -1 }
  }

  #completeMarkersAfterRound() {
    this.markerRounds -= 1
    if (this.markerRounds <= 0) {
      this.markerRounds = 0
      this.state = this.state.replace('PREPARE_', 'COMPLETE_')
    }
  }
}

module.exports = { TransactionCoordinatorModel }
