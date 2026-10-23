const { requests } = require('../../protocol/requests')
const apiKeys = require('../../protocol/requests/apiKeys')
const {
  KafkaJSApplicationRecoverableError,
  KafkaJSInvalidConfigurationError,
  KafkaJSInvariantViolation,
} = require('../../errors')

const NO_PRODUCER_ID = -1
const NO_PRODUCER_EPOCH = -1

// The producer epoch is an int16 on the wire.
const MAX_PRODUCER_EPOCH = 32767

// InitProducerId v3 adds ProducerId and ProducerEpoch to the request (KIP-360).
const FIRST_EPOCH_BUMPING_VERSION = 3

const CLUSTER_AUTHORIZATION_FAILED = 31
/** @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-679%3A+Producer+will+enable+the+strongest+delivery+guarantee+by+default */
const CLUSTER_AUTHORIZATION_MESSAGE =
  'The broker refused InitProducerId: the principal needs IDEMPOTENT_WRITE on the cluster ' +
  '(or WRITE on any topic, from Kafka 2.8)'
const IDEMPOTENT_REMEDY = ', or create the producer with idempotent: false'

/**
 * The producer's identity - its producer id and epoch - and the four ways it changes:
 *
 * - `initialize()` acquires a fresh pair: InitProducerId with -1/-1.
 * - `reinitialize()` asks the coordinator to bump the epoch of the pair it holds (KIP-360,
 *   InitProducerId v3+) and accepts the answer only if it moved; against a coordinator below v3
 *   it is a plain `initialize()`, the pre-KIP-360 fallback.
 * - `bumpEpoch()` increments the epoch locally, an idempotent-only producer's recovery; at the
 *   int16 ceiling it discards the pair and acquires a fresh one instead of overflowing.
 * - `adopt()` takes the pair an EndTxn v5 response carries (KIP-890).
 *
 * Each InitProducerId is one attempt: retries, the ordering against batches in flight and the
 * transaction state a failure arms belong to the caller. `initialize()`, `reinitialize()` and
 * `adopt()` end in `onReset()`, which restarts every partition's sequences; a bump does not,
 * because only the partition that triggered it restarts at once - the caller's narrow reset - and
 * the others once nothing of theirs is in flight. Before the first initialization, and after the
 * ceiling discards the pair, the producer holds no pair: -1/-1.
 * @see https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=89068820
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java
 */
class ProducerIdAndEpoch {
  #logger
  #transactionalId
  #transactionalProducer
  #transactionTimeout
  #findBroker
  #onReset
  #getGeneration
  #producerId = NO_PRODUCER_ID
  #producerEpoch = NO_PRODUCER_EPOCH
  /** @type {?{ generation: ?import('../generation'), promise: Promise<void> }} */
  #pending = null

  /**
   * @param {object} options
   * @param {import('../../../types').Logger} options.logger
   * @param {string|null} [options.transactionalId=null] null for an idempotent-only producer
   * @param {boolean} [options.transactionalProducer=false] whether the producer this identity
   *   belongs to has a transactional id, though this identity may send none: such a producer is
   *   idempotent by necessity, so a refusal is not answered with `idempotent: false`
   * @param {number} options.transactionTimeout sent as InitProducerId's TransactionTimeoutMs
   * @param {(generation?: import('../generation')) => Promise<import('../../../types').Broker>} options.findBroker
   *   the broker every InitProducerId goes to - the transaction coordinator, or any broker for an
   *   idempotent-only producer - asked again for every request, with the producer lifecycle the
   *   call captured
   * @param {() => void} options.onReset restarts every partition's sequences
   * @param {() => (import('../generation')|undefined)} [options.getGeneration] the producer's
   *   current lifecycle, read once when each operation starts: a pair is taken, and `onReset`
   *   runs, only while that lifecycle is open
   */
  constructor({
    logger,
    transactionalId = null,
    transactionalProducer = false,
    transactionTimeout,
    findBroker,
    onReset,
    getGeneration = () => undefined,
  }) {
    this.#logger = logger
    this.#transactionalId = transactionalId
    this.#transactionalProducer = transactionalProducer
    this.#transactionTimeout = transactionTimeout
    this.#findBroker = findBroker
    this.#onReset = onReset
    this.#getGeneration = getGeneration
  }

  /** @returns {string|number} the producer id - the decimal string a response carries - or -1 while none is held */
  get producerId() {
    return this.#producerId
  }

  /** @returns {number} the producer epoch, or -1 while no producer id is held */
  get producerEpoch() {
    return this.#producerEpoch
  }

  /**
   * Whether a producer id is held (Java's `hasProducerId()`): false before the first
   * initialization and while the epoch ceiling's fresh acquisition runs.
   * @returns {boolean}
   */
  isValid() {
    return Number(this.#producerId) >= 0
  }

  /**
   * The fresh InitProducerId the epoch ceiling started, while it runs. A caller that needs a
   * producer id awaits it rather than starting another initialization; it rejects with the
   * request's error, which is never also reported as an unhandled rejection and is logged as a
   * warning. After a rejection no producer id is held: the caller re-initializes.
   * @returns {Promise<void>|null} null when there is none, or it began under a lifecycle that is
   *   not the current one
   */
  pendingInitialization() {
    const pending = this.#pending
    return pending !== null && pending.generation === this.#getGeneration() ? pending.promise : null
  }

  /**
   * Acquires a fresh producer id and epoch - one InitProducerId with -1/-1 and the transactional
   * id, null for an idempotent-only producer - then restarts every partition's sequences.
   * @returns {Promise<void>}
   * @throws {KafkaJSInvalidConfigurationError} on CLUSTER_AUTHORIZATION_FAILED (31), naming both
   *   remedies; its `cause` is the broker's error
   * @throws {import('../../errors').KafkaJSProtocolError} any other error of the request, unchanged
   * @throws {Error} the close error of the producer lifecycle, when it closed meanwhile; nothing
   *   is taken and nothing is reset
   */
  async initialize() {
    const generation = this.#getGeneration()
    await this.#acquire(generation)
    generation?.check()
    this.#onReset()
  }

  /**
   * Bumps the epoch of the pair held (KIP-360): one InitProducerId carrying the current producer
   * id and epoch, accepted only when the answer moved the epoch or carries a new producer id (the
   * coordinator allocates one when the epoch is exhausted); then restarts every partition's
   * sequences. Against a coordinator whose InitProducerId stops below v3, which cannot bump, it is
   * a plain `initialize()` - the caller has already aborted, so a fresh pair is safe.
   * @returns {Promise<void>}
   * @throws {KafkaJSApplicationRecoverableError} when the answer bumped nothing; the pair is kept
   * @throws {KafkaJSInvalidConfigurationError} on CLUSTER_AUTHORIZATION_FAILED (31)
   * @throws {import('../../errors').KafkaJSProtocolError} any other error, unchanged -
   *   PRODUCER_FENCED (90) included
   * @throws {Error} the close error of the producer lifecycle, when it closed meanwhile; the pair
   *   is kept and nothing is reset
   * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java
   */
  async reinitialize() {
    const generation = this.#getGeneration()
    const broker = await this.#findBroker(generation)
    generation?.check()
    const version = broker.negotiatedVersion(apiKeys.InitProducerId, requests.InitProducerId)
    if (!(version >= FIRST_EPOCH_BUMPING_VERSION)) {
      return this.initialize()
    }

    const sent = { producerId: this.#producerId, producerEpoch: this.#producerEpoch }
    const received = await this.#request(broker, sent, generation)
    generation?.check()
    if (
      String(received.producerId) === String(sent.producerId) &&
      received.producerEpoch === sent.producerEpoch
    ) {
      throw new KafkaJSApplicationRecoverableError(
        `InitProducerId did not bump the epoch of producer ${sent.producerId} (epoch ${sent.producerEpoch}); create a new producer`
      )
    }

    this.#set(received)
    this.#onReset()
  }

  /**
   * Increments the epoch locally - an idempotent-only producer's recovery from a sequence it can
   * no longer resolve - with no request. At the int16 ceiling it discards the pair instead and
   * starts a fresh InitProducerId with no transactional id, observable through
   * `pendingInitialization()`. It never restarts sequences: the caller restarts the partition that
   * triggered the bump and marks the others stale. A bump while no pair is held does nothing; the
   * next pair starts at its own epoch.
   * @returns {void}
   * @throws {KafkaJSInvariantViolation} for a transactional producer, whose epoch moves only
   *   through `reinitialize()` and `adopt()`
   * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java
   */
  bumpEpoch() {
    if (this.#transactionalId != null) {
      throw new KafkaJSInvariantViolation('bumpEpoch() is only for an idempotent-only producer')
    }

    if (!this.isValid()) {
      return
    }

    if (this.#producerEpoch < MAX_PRODUCER_EPOCH) {
      this.#set({ producerId: this.#producerId, producerEpoch: this.#producerEpoch + 1 })
      return
    }

    const exhaustedProducerId = this.#producerId
    this.#set({ producerId: NO_PRODUCER_ID, producerEpoch: NO_PRODUCER_EPOCH })
    const generation = this.#getGeneration()
    const pending = { generation, promise: this.#acquire(generation) }
    pending.promise.then(
      () => {
        if (this.#pending === pending) this.#pending = null
      },
      (e) => {
        if (this.#pending === pending) this.#pending = null
        this.#logger.warn(
          `Could not acquire a producer id after producer ${exhaustedProducerId} reached the maximum epoch`,
          { producerId: exhaustedProducerId, error: e.message }
        )
      }
    )
    this.#pending = pending
  }

  /**
   * Takes the producer id and epoch an EndTxn v5 response carries - under transaction version 2
   * the coordinator bumps the epoch at the end of every transaction (KIP-890) - then restarts
   * every partition's sequences. A state change, not a request: it accepts any pair silently, so
   * the caller guarantees nothing is in flight and that the producer id is not -1.
   * @param {{ producerId: string|number, producerEpoch: number }} pair
   * @returns {void}
   */
  adopt({ producerId, producerEpoch }) {
    this.#set({ producerId, producerEpoch })
    this.#onReset()
  }

  /**
   * One InitProducerId for a fresh pair, kept on success unless the producer lifecycle it began
   * under closed meanwhile.
   * @param {import('../generation')} [generation] the lifecycle captured when the operation began
   * @returns {Promise<void>}
   * @throws {Error} the lifecycle's close error
   */
  async #acquire(generation) {
    const broker = await this.#findBroker(generation)
    generation?.check()
    const pair = await this.#request(
      broker,
      {
        producerId: NO_PRODUCER_ID,
        producerEpoch: NO_PRODUCER_EPOCH,
      },
      generation
    )
    generation?.check()
    this.#set(pair)
  }

  /**
   * Sends one InitProducerId. CLUSTER_AUTHORIZATION_FAILED (31) becomes a configuration error
   * naming the ACL to grant - and, for a producer without a transactional id, the option that turns
   * idempotence off - its `cause` the broker's error; every other error is rethrown as is.
   * @param {import('../../../types').Broker} broker
   * @param {{ producerId: string|number, producerEpoch: number }} pair
   * @param {import('../generation')} [generation] its `guard` refuses the write once it closed
   * @returns {Promise<{ producerId: string, producerEpoch: number }>}
   */
  async #request(broker, { producerId, producerEpoch }, generation) {
    try {
      return await broker.initProducerId({
        transactionalId: this.#transactionalId,
        transactionTimeout: this.#transactionTimeout,
        producerId,
        producerEpoch,
        ...(generation ? { guard: generation.guard } : {}),
      })
    } catch (e) {
      if (e.code === CLUSTER_AUTHORIZATION_FAILED) {
        throw new KafkaJSInvalidConfigurationError(
          this.#transactionalProducer
            ? CLUSTER_AUTHORIZATION_MESSAGE
            : CLUSTER_AUTHORIZATION_MESSAGE + IDEMPOTENT_REMEDY,
          { cause: e }
        )
      }

      throw e
    }
  }

  /**
   * @param {{ producerId: string|number, producerEpoch: number }} pair
   * @returns {void}
   */
  #set({ producerId, producerEpoch }) {
    this.#producerId = producerId
    this.#producerEpoch = producerEpoch
    this.#logger.debug('Producer id and epoch set', { producerId, producerEpoch })
  }
}

module.exports = ProducerIdAndEpoch
