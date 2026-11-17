const Broker = require('../src/broker')
const Pool = require('../src/network/connectionPool')

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

/**
 * @param {Error} error a login failure
 * @returns {boolean} whether it, or an error in its cause chain, is an authentication refusal
 */
const isRefusal = (error) => {
  for (let cause = error; cause; cause = cause.cause) {
    if (cause.name === 'KafkaJSSASLAuthenticationError') return true
    if (cause.type === 'SASL_AUTHENTICATION_FAILED') return true
  }
  return false
}

/**
 * One fresh SASL login against a broker address, through the probe's sockets; the connection is
 * closed whatever the outcome.
 * @param {{logger: object, socket: Function}} probe the case's probe
 * @param {{ssl?: object, sasl: object}} security the credential and TLS settings to log in with
 * @returns {(address: string) => Promise<void>} login for `scramLoginSettled`
 */
const brokerLogin =
  (probe, { ssl, sasl }) =>
  async (address) => {
    const [host, port] = address.split(':')
    const broker = new Broker({
      logger: probe.logger,
      connectionPool: new Pool({
        host,
        port: Number(port),
        ssl,
        sasl,
        logger: probe.logger,
        socketFactory: (args) => probe.socket(args),
        connectionTimeout: 3000,
        requestTimeout: 10000,
      }),
    })
    try {
      await broker.connect()
    } finally {
      await broker.disconnect({ drainTimeout: 0 })
    }
  }

/**
 * Waits until a SCRAM login gives the same answer on every broker. Each broker applies a SCRAM
 * change from the metadata log on its own schedule and authenticates from its own copy, so neither
 * the controller's acknowledgement nor one broker's DescribeUserScramCredentials shows that another
 * broker accepts, or refuses, the credential yet. Only an authentication refusal is repeated.
 * @param {CatchUpProbe} probe the case's probe
 * @param {object} options
 * @param {string[]} options.addresses every broker's SASL listener, `host:port`
 * @param {boolean} options.accepted true to wait for the login to succeed, false for its refusal
 * @param {(address: string) => Promise<void>} options.login one fresh login at that address
 * @returns {Promise<void>} once every broker gives the expected answer
 * @throws {Error} any other login failure, or the probe's deadline naming the broker and its last
 *   outcome
 */
const scramLoginSettled = async (probe, { addresses, accepted, login }) => {
  for (const address of addresses) {
    let last = 'none'
    let unexpected
    try {
      await probe.poll(async () => {
        try {
          await probe.invoke(() => login(address))
          last = 'accepted'
          return accepted
        } catch (error) {
          if (!isRefusal(error)) throw (unexpected = error)
          last = `${error.name} ${error.message}`
          return !accepted
        }
      })
    } catch (error) {
      if (error === unexpected) throw error
      throw new Error(
        `SCRAM login at ${address} was never ${accepted ? 'accepted' : 'refused'} (last outcome: ${last}): ${error.message}`,
        { cause: error }
      )
    }
  }
}

module.exports = {
  fenceUntilSettled,
  describeScramUntilApplied,
  scramLoginSettled,
  brokerLogin,
  TRANSACTION_COORDINATOR_APIS,
}
