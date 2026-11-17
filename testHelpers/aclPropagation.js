const { createCluster } = require('./index')

/** The fields that identify one ACL binding. */
const BINDING_FIELDS = [
  'resourceType',
  'resourceName',
  'resourcePatternType',
  'principal',
  'host',
  'operation',
  'permissionType',
]

/**
 * A test-only view of the cluster that reads ACLs from one chosen broker. Public
 * `admin.describeAcls` always asks the broker Metadata names as controller, which in KRaft is any
 * live broker, so it cannot show whether every broker's authorizer has applied a change.
 * @typedef {object} AclPort
 * @property {() => Promise<number[]>} brokerIds node ids of the live brokers
 * @property {(nodeId: number, filter: object) => Promise<object[]>} describeAclsOn DescribeAcls
 *   resources (grouped rows with `acls`) or flat bindings served by one broker
 * @property {() => Promise<void>} close releases the port's connections
 */

/**
 * Normalizes DescribeAcls resources to one row per binding; rows without `acls` are already flat.
 * @param {object[]} rows grouped resources or flat bindings
 * @returns {string[]} canonical JSON of each binding, fields in `BINDING_FIELDS` order
 */
const bindingKeys = (rows) =>
  rows
    .flatMap((row) => (row.acls ? row.acls.map((acl) => ({ ...row, ...acl })) : [row]))
    .map((row) => JSON.stringify(Object.fromEntries(BINDING_FIELDS.map((f) => [f, row[f]]))))

/**
 * @param {Set<string>} want expected binding keys
 * @param {object[]} rows what one broker served
 * @returns {{missing: string[], extra: string[]}|null} the difference, or null when equal
 */
const compare = (want, rows) => {
  const have = new Set(bindingKeys(rows))
  const missing = [...want].filter((key) => !have.has(key))
  const extra = [...have].filter((key) => !want.has(key))
  return missing.length || extra.length ? { missing, extra } : null
}

/**
 * Polls every broker until each one serves exactly `expected` under `filter`. All brokers are
 * read in every round, and the wait ends only in a round where all of them match, so a broker
 * that converged early is checked again. One overall deadline bounds the whole wait, including
 * a read that never answers; every timer is cleared on every exit.
 * @param {object} options
 * @param {AclPort} options.port per-broker reader
 * @param {object} options.filter complete DescribeAcls filter
 * @param {object[]} options.expected bindings, flat or grouped; `[]` waits for removal
 * @param {number} [options.timeout] overall deadline in ms
 * @param {number} [options.interval] pause between rounds in ms
 * @param {AbortSignal} [options.signal] stops polling and rejects with its reason
 * @returns {Promise<void>} once every broker serves exactly the expected bindings
 * @throws {Error} at the deadline, naming each broker's latest missing and extra bindings and
 *   each broker whose read is unanswered; the abort reason; a read failure; or no broker to read
 */
const waitForAclBindings = async ({
  port,
  filter,
  expected,
  timeout = 30000,
  interval = 100,
  signal,
}) => {
  signal?.throwIfAborted()
  const want = new Set(bindingKeys(expected))
  /** @type {Map<number, {diff?: {missing: string[], extra: string[]}|null, pending: boolean}>} */
  const brokers = new Map()
  let timer
  let pause
  let onAbort
  const stopped = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(timeoutMessage(timeout, brokers))), timeout)
    onAbort = () => reject(signal.reason)
    signal?.addEventListener('abort', onAbort, { once: true })
  })
  stopped.catch(() => {})
  const until = (work) => Promise.race([work, stopped])
  try {
    const nodeIds = await until(port.brokerIds())
    if (nodeIds.length === 0) throw new Error('No broker to read ACLs from')
    for (const nodeId of nodeIds) brokers.set(nodeId, { pending: false })
    for (;;) {
      for (const [nodeId, state] of brokers) {
        state.pending = true
        const rows = await until(port.describeAclsOn(nodeId, filter))
        Object.assign(state, { diff: compare(want, rows), pending: false })
      }
      if ([...brokers.values()].every(({ diff }) => diff === null)) return
      await until(new Promise((resolve) => (pause = setTimeout(resolve, interval))))
    }
  } finally {
    clearTimeout(timer)
    clearTimeout(pause)
    signal?.removeEventListener('abort', onAbort)
  }
}

/**
 * @param {number} timeout deadline in ms
 * @param {Map<number, {diff?: {missing: string[], extra: string[]}|null, pending: boolean}>} brokers
 *   each broker's latest answer and whether a read of it is outstanding
 * @returns {string} deadline failure text; a broker that never answered gets no invented diff
 */
const timeoutMessage = (timeout, brokers) => {
  const lagging = [...brokers]
    .filter(([, { diff, pending }]) => diff !== null || pending)
    .map(([nodeId, { diff, pending }]) => {
      const parts = []
      if (diff) parts.push(`missing [${diff.missing.join(', ')}] extra [${diff.extra.join(', ')}]`)
      if (pending) parts.push('did not answer its read')
      else if (diff === undefined) parts.push('was not read')
      return `broker ${nodeId} ${parts.join(', then ')}`
    })
  const detail = brokers.size ? lagging.join('; ') : 'the broker list was not read'
  return `ACL bindings were not served by every broker within ${timeout} ms: ${detail}`
}

/**
 * An {@link AclPort} over a test-owned Cluster, reading each broker with `broker.describeAcls`.
 * The port owns the cluster from construction, so a caller can register `close` before the first
 * connect. Close fences the port: later calls are refused, and a connect or read still running is
 * cut off, awaited and disconnected again, so nothing it reopens outlives the port.
 */
class ClusterAclPort {
  /** @param {import('../src/cluster')} cluster cluster the port owns, not yet connected */
  constructor(cluster) {
    this.cluster = cluster
    this.closed = false
    /** @type {Set<Promise<any>>} */
    this.pending = new Set()
  }

  /**
   * Builds a port and connects it; a failed connect closes it before rethrowing.
   * @param {import('../src/cluster')} cluster cluster the port will own
   * @returns {Promise<ClusterAclPort>} connected port
   * @throws {Error} the connect failure
   */
  static async open(cluster) {
    const port = new ClusterAclPort(cluster)
    try {
      await port.connect()
    } catch (error) {
      await port.close()
      throw error
    }
    return port
  }

  /**
   * @returns {Promise<void>} once the owned cluster is connected
   * @throws {Error} the connect failure, or the port is closed
   */
  connect() {
    return this.track(() => this.cluster.connect())
  }

  /**
   * @returns {Promise<number[]>} live broker node ids from fresh metadata, ascending
   * @throws {Error} the metadata failure, or the port is closed
   */
  brokerIds() {
    return this.track(async () => {
      await this.cluster.refreshMetadata()
      return this.cluster
        .getNodeIds()
        .map(Number)
        .sort((a, b) => a - b)
    })
  }

  /**
   * @param {number} nodeId broker to ask
   * @param {object} filter complete DescribeAcls filter
   * @returns {Promise<object[]>} that broker's grouped DescribeAcls resources
   * @throws {Error} the request failure, or the port closed before the request was sent
   */
  describeAclsOn(nodeId, filter) {
    return this.track(async () => {
      const broker = await this.cluster.findBroker({ nodeId })
      this.assertOpen()
      const { resources } = await broker.describeAcls(filter)
      return resources
    })
  }

  /**
   * Disconnects at once, cutting off requests in flight, then waits for the calls that were
   * running when close began and disconnects again whatever they reconnected meanwhile, even when
   * they settled during the first disconnect or that disconnect failed.
   * @returns {Promise<void>} once the owned cluster is disconnected and no call is running
   * @throws {Error} a disconnect failure, after the final disconnect was attempted
   */
  async close() {
    this.closed = true
    const running = [...this.pending]
    try {
      await this.cluster.disconnect({ drainTimeout: 0 })
    } finally {
      if (running.length) {
        await Promise.allSettled(running)
        await this.cluster.disconnect({ drainTimeout: 0 })
      }
    }
  }

  /** @throws {Error} the port is closed */
  assertOpen() {
    if (this.closed) throw new Error('ACL port is closed')
  }

  /**
   * Runs one call while the port is open and keeps it in `pending` until it settles.
   * @template T
   * @param {() => Promise<T>} work the call
   * @returns {Promise<T>} its result
   * @throws {Error} the call's failure, or the port is closed
   */
  async track(work) {
    this.assertOpen()
    const call = work()
    this.pending.add(call)
    try {
      return await call
    } finally {
      this.pending.delete(call)
    }
  }
}

/**
 * Builds a connected {@link ClusterAclPort} on its own cluster with the caller's connection
 * settings, so it reaches the same listener the caller's admin uses. Callers close it.
 * @param {object} options
 * @param {string[]} options.brokers bootstrap brokers of the caller's admin
 * @param {object} [options.ssl] the caller's TLS settings
 * @param {object} [options.sasl] the caller's SASL settings
 * @returns {Promise<ClusterAclPort>} connected port
 */
const clusterAclPort = ({ brokers, ssl, sasl }) =>
  ClusterAclPort.open(createCluster({ ssl, sasl }, brokers))

/**
 * Runs each closer in order, so a test hook that fails to disconnect one client still closes the
 * rest, then rethrows the first failure.
 * @param {...(() => Promise<any>)|null|undefined} closers closers in order; absent ones are skipped
 * @returns {Promise<void>} once every closer has run
 * @throws {Error} the first closer failure
 */
const closeEach = async (...closers) => {
  const failures = []
  for (const close of closers) {
    if (!close) continue
    try {
      await close()
    } catch (error) {
      failures.push(error)
    }
  }
  if (failures.length) throw failures[0]
}

module.exports = { waitForAclBindings, clusterAclPort, ClusterAclPort, closeEach }
