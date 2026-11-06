/**
 * The KIP-1050 code → category table. A leaf module that requires nothing, so `src/errors.js` can
 * stamp a protocol error's category in its constructor and the producer's classifier can read the
 * same table without a require cycle.
 */

/**
 * What the caller should do about an error (KIP-1050): retry it; refresh metadata or the
 * coordinator and retry it; abort the transaction and keep the producer; create a new producer;
 * or fix the configuration, the record or the ACL.
 * @typedef {'retriable'|'refreshRetriable'|'abortable'|'applicationRecoverable'|'invalidConfiguration'} Category
 */

/**
 * The request family an error arrived on: `produce` (Produce, the send path), `txn`
 * (InitProducerId, AddPartitionsToTxn, AddOffsetsToTxn, EndTxn, FindCoordinator) or `offsets`
 * (TxnOffsetCommit).
 * @typedef {'produce'|'txn'|'offsets'} Surface
 */

/**
 * The two categories the client retries: an error's `retriable` flag is true exactly for these.
 * @type {Set<Category>}
 */
const RETRIABLE_CATEGORIES = new Set(['retriable', 'refreshRetriable'])

/** A row that holds on every surface — the only kind that applies before the surface is known. */
const ANY = null

/**
 * The code → category table. Rows are tried in order; the first one naming both the code and the
 * surface answers. A code no row names takes its `retriable` flag: `retriable` when true,
 * `applicationRecoverable` otherwise. A `{ transactional, idempotent }` category depends on the
 * producer kind, because an idempotent-only producer has no transaction to abort. Codes 14, 15 and
 * 16 still carry this client's pre-0.11 type names, which is why rows match on the code.
 *
 * The refresh-retriable rows are the Java client's `RefreshRetriableException` subclasses. One
 * group differs from KIP-1050's own table on purpose: ILLEGAL_GENERATION, UNKNOWN_MEMBER_ID,
 * FENCED_INSTANCE_ID, STALE_MEMBER_EPOCH and, from TxnOffsetCommit v6, GROUP_ID_NOT_FOUND are
 * abortable — the application aborts, the consumer rejoins and the batch is reprocessed — where
 * KIP-1050 would have the producer re-created on every rebalance race. (The Java 4.3 client has no
 * branch for STALE_MEMBER_EPOCH there at all.) GROUP_ID_NOT_FOUND is abortable on that surface
 * only: it keeps the category its flag gives it everywhere else.
 *
 * Two codes hold on more than the request one would expect. GROUP_AUTHORIZATION_FAILED is a
 * configuration error on every surface: FindCoordinator, AddOffsetsToTxn and TxnOffsetCommit all
 * answer it with an abortable error, so a missing group ACL is fixed, not met with a new
 * producer. UNKNOWN_PRODUCER_ID is abortable on Produce and on the transaction requests for a
 * transactional producer (the Java client's `abortableErrorIfPossible`); the idempotent kind never
 * sends those requests, and its row keeps the Produce row's shape.
 * MESSAGE_TOO_LARGE and INVALID_TIMESTAMP, like INVALID_RECORD, reject the records of one send: they
 * are configuration errors on Produce, so a transaction that met one can be aborted and the producer
 * carries on once the record is fixed.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-1050:+Consistent+error+handling+for+Transactions
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/common/errors/RefreshRetriableException.java
 * @type {{ codes: number[], surfaces: Surface[]|null, category: Category|{ transactional: Category, idempotent: Category } }[]}
 */
const ROWS = [
  { codes: [2, 7, 14], surfaces: ANY, category: 'retriable' },
  { codes: [19, 20], surfaces: ['produce'], category: 'retriable' },
  { codes: [51], surfaces: ['txn'], category: 'retriable' },
  { codes: [88], surfaces: ['offsets'], category: 'retriable' },
  { codes: [3, 5, 6, 9, 13, 56, 74, 75, 100, 103], surfaces: ANY, category: 'refreshRetriable' },
  { codes: [15, 16], surfaces: ['txn', 'offsets'], category: 'refreshRetriable' },
  { codes: [120], surfaces: ANY, category: 'abortable' },
  { codes: [48], surfaces: ['produce'], category: 'abortable' },
  {
    codes: [45, 59],
    surfaces: ['produce'],
    category: { transactional: 'abortable', idempotent: 'applicationRecoverable' },
  },
  {
    codes: [59],
    surfaces: ['txn'],
    category: { transactional: 'abortable', idempotent: 'applicationRecoverable' },
  },
  { codes: [22, 25, 69, 82, 113], surfaces: ['offsets'], category: 'abortable' },
  { codes: [47, 49, 90], surfaces: ANY, category: 'applicationRecoverable' },
  { codes: [48], surfaces: ['txn', 'offsets'], category: 'applicationRecoverable' },
  { codes: [22, 25, 82], surfaces: ['produce', 'txn'], category: 'applicationRecoverable' },
  { codes: [17, 29, 30, 31, 35, 53, 58], surfaces: ANY, category: 'invalidConfiguration' },
  { codes: [10, 18, 21, 32, 43, 87], surfaces: ['produce'], category: 'invalidConfiguration' },
  { codes: [50], surfaces: ['txn'], category: 'invalidConfiguration' },
]

/** @type {Map<number, typeof ROWS>} */
const ROWS_BY_CODE = new Map()
for (const row of ROWS) {
  for (const code of row.codes) {
    ROWS_BY_CODE.set(code, [...(ROWS_BY_CODE.get(code) || []), row])
  }
}

/**
 * The table's category for a Kafka error code. The answer never contradicts `retriable`: a row
 * whose category disagrees with the flag is ignored and the flag decides, so `retriable === true`
 * holds exactly for the two retriable categories on every error built from this table.
 * @param {number} code
 * @param {object} options
 * @param {boolean} options.retriable the code's flag in the protocol error table; decides a code
 *   no row names
 * @param {Surface|null} [options.surface=null] `null` before the request is known: only the rows
 *   that hold on every surface apply
 * @param {boolean} [options.transactional=false] the producer kind, for the codes that depend on it
 * @returns {Category}
 */
const categoryOf = (code, { retriable, surface = null, transactional = false }) => {
  const fromFlag = retriable === true ? 'retriable' : 'applicationRecoverable'
  const row = (ROWS_BY_CODE.get(code) || []).find(
    ({ surfaces }) => surfaces === ANY || surfaces.includes(surface)
  )

  if (row === undefined) {
    return fromFlag
  }

  const category =
    typeof row.category === 'string'
      ? row.category
      : row.category[transactional ? 'transactional' : 'idempotent']

  return RETRIABLE_CATEGORIES.has(category) === (retriable === true) ? category : fromFlag
}

module.exports = { categoryOf, RETRIABLE_CATEGORIES }
