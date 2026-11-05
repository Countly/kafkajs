/** The coordinator answers this while it ends the open transaction of a fenced id. */
const CONCURRENT_TRANSACTIONS = 51
/** A broker answers this for a credential it has not applied yet, as for one that never existed. */
const RESOURCE_NOT_FOUND = 91
/** The requests a transactional producer sends to its transaction coordinator. */
const TRANSACTION_COORDINATOR_APIS = [
  'InitProducerId',
  'AddPartitionsToTxn',
  'AddOffsetsToTxn',
  'EndTxn',
  'TxnOffsetCommit',
]

/**
 * The live admin probes make one attempt per call (`retry: { retries: 0 }`), so the repeats the
 * Java admin makes on its own are made here, inside the probe's own deadline.
 * @typedef {object} CatchUpProbe
 * @property {(work: () => Promise<any>) => Promise<any>} invoke runs one guarded call
 * @property {(check: () => Promise<boolean>) => Promise<void>} poll repeats `check` until it holds
 */

/**
 * Fences a transactional id and waits until the coordinator has ended its open transaction. The
 * first request for an id with an open transaction is always answered CONCURRENT_TRANSACTIONS
 * while the coordinator aborts it; the Java admin repeats the request on that code, and so does
 * this. Any other outcome ends the wait, and the caller judges it.
 * @param {CatchUpProbe} probe the case's probe
 * @param {object} admin a connected admin
 * @param {string} transactionalId the id to fence
 * @returns {Promise<object[]>} the item results of the first answer that is not CONCURRENT_TRANSACTIONS
 * @throws {Error} the call's failure, or the probe's deadline
 */
const fenceUntilSettled = async (probe, admin, transactionalId) => {
  let rows
  await probe.poll(async () => {
    rows = await probe.invoke(() => admin.fenceProducers({ transactionalIds: [transactionalId] }))
    return rows[0].errorCode !== CONCURRENT_TRANSACTIONS
  })
  return rows
}

/**
 * Describes one user's SCRAM credentials once the broker that answers has applied the change. The
 * controller commits an alteration before every broker applies it, and DescribeUserScramCredentials
 * is answered from the broker's own metadata, so a read right after an upsertion may still say
 * RESOURCE_NOT_FOUND. Any other outcome ends the wait, and the caller judges it.
 * @param {CatchUpProbe} probe the case's probe
 * @param {object} admin a connected admin
 * @param {string} user the user whose credentials were just changed
 * @returns {Promise<object[]>} the item results of the first answer that is not RESOURCE_NOT_FOUND
 * @throws {Error} the call's failure, or the probe's deadline
 */
const describeScramUntilApplied = async (probe, admin, user) => {
  let rows
  await probe.poll(async () => {
    rows = await probe.invoke(() => admin.describeUserScramCredentials({ users: [user] }))
    return rows[0].errorCode !== RESOURCE_NOT_FOUND
  })
  return rows
}

module.exports = { fenceUntilSettled, describeScramUntilApplied, TRANSACTION_COORDINATOR_APIS }
