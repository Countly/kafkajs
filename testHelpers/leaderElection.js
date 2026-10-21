const { execFileSync } = require('child_process')
const path = require('path')

const BOOTSTRAP_SERVER = 'kafka1:29092'
// Suffixed with this process's pid so two overlapping runs (there should never be one, but the
// file is otherwise never cleaned up) cannot clobber each other's reassignment plan.
const REASSIGNMENT_FILE = `/tmp/leader-move-reassignment-${process.pid}.json`
const KAFKA_BIN = '/opt/kafka/bin'
// Resolved against the repository root, not the working directory, so a script run from another
// directory (the OpenTelemetry package's tests run from `otel/`) reaches the same cluster.
const COMPOSE_FILE = path.resolve(__dirname, '..', process.env.COMPOSE_FILE || 'docker-compose.yml')

/**
 * Runs `docker compose` against the cluster the tests are using. Defaults to the root compose
 * file; `COMPOSE_FILE` (relative to the repository root) overrides it, which is how a scenario
 * targets its own.
 *
 * Synchronous on purpose, so a test never races the container it just told to stop or start.
 *
 * @param {...string} args `docker compose -f <compose file> <args>`
 * @returns {Buffer} the command's stdout
 * @throws {Error} if the `docker` CLI exits non-zero
 */
const compose = (...args) =>
  execFileSync('docker', ['compose', '-f', COMPOSE_FILE, ...args], { stdio: 'pipe' })

/**
 * Runs a command inside the kafka1 container, piping `input` to its stdin when given.
 *
 * kafka1 is the one broker every live scenario keeps alive, so it is always a safe place to run
 * the admin CLI tools from, even while another node is stopped. It resolves the compose file the
 * same way `compose` does - one rule for the whole module - and differs only in needing stdin,
 * which a variadic wrapper cannot carry.
 *
 * @param {string[]} args argv of the command to run inside the container
 * @param {string|Buffer} [input] written to the command's stdin
 * @returns {Buffer} the command's stdout
 * @throws {Error} if `docker` or the command inside it exits non-zero
 */
const execInKafka1 = (args, input) =>
  execFileSync('docker', ['compose', '-f', COMPOSE_FILE, 'exec', '-T', 'kafka1', ...args], {
    stdio: 'pipe',
    input,
  })

/**
 * Stops one broker container without removing it, so it can be started again with its log intact.
 *
 * Keeping the log is what makes a truncation scenario possible: the restarted broker comes back
 * with exactly the records it had replicated before it went away, not an empty one.
 *
 * @param {string} service e.g. 'kafka3'
 * @returns {Buffer} the command's stdout
 * @throws {Error} if the `docker` CLI exits non-zero
 */
const stopBroker = (service) => compose('stop', service)

/**
 * Starts a previously stopped broker container.
 *
 * Returning is not a readiness signal - the broker still has to register with the controller
 * quorum, so a caller must poll for reachability rather than assume it can be talked to.
 *
 * @param {string} service e.g. 'kafka3'
 * @returns {Buffer} the command's stdout
 * @throws {Error} if the `docker` CLI exits non-zero
 */
const startBroker = (service) => compose('start', service)

/**
 * Kills one broker container (`docker compose kill`): the process dies at once, with no clean
 * shutdown, and the container keeps its log for a later `startBroker`.
 *
 * @param {string} service e.g. 'kafka3'
 * @returns {Buffer} the command's stdout
 * @throws {Error} if the `docker` CLI exits non-zero
 */
const killBroker = (service) => compose('kill', service)

/**
 * Freezes one broker container (`docker compose pause`): its process stops running but keeps its
 * sockets open, so peers see silence rather than a closed connection.
 *
 * @param {string} service e.g. 'kafka3'
 * @returns {Buffer} the command's stdout
 * @throws {Error} if the `docker` CLI exits non-zero
 */
const pauseBroker = (service) => compose('pause', service)

/**
 * Lets a paused broker container run again.
 *
 * @param {string} service e.g. 'kafka3'
 * @returns {Buffer} the command's stdout
 * @throws {Error} if the `docker` CLI exits non-zero
 */
const unpauseBroker = (service) => compose('unpause', service)

/**
 * The compose services of `docker-compose.yml`, each with its KRaft node id and the plaintext
 * address it advertises to clients.
 * @type {Readonly<Record<string, { nodeId: number, host: string, port: number }>>}
 */
const NODES = Object.freeze({
  kafka1: Object.freeze({ nodeId: 1, host: 'localhost', port: 9092 }),
  kafka2: Object.freeze({ nodeId: 2, host: 'localhost', port: 9095 }),
  kafka3: Object.freeze({ nodeId: 3, host: 'localhost', port: 9098 }),
})

/** The cluster's own topics whose replication a bounce also waits for, when they exist. */
const INTERNAL_TOPICS = Object.freeze(['__consumer_offsets', '__transaction_state'])

/** @param {number} ms @returns {Promise<void>} */
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

/**
 * Whether one node answers on its advertised address: a fresh connection negotiates ApiVersions
 * and a Metadata request succeeds. Nothing is retried and the connection is closed either way.
 *
 * @param {{ host: string, port: number }} node
 * @param {object} [options]
 * @param {number} [options.timeout=2000] the connection and request timeout, ms
 * @param {Function} [options.socketFactory] the plain `net` factory by default
 * @returns {Promise<boolean>}
 */
const answers = async ({ host, port }, { timeout = 2000, socketFactory } = {}) => {
  const Broker = require('../src/broker')
  const ConnectionPool = require('../src/network/connectionPool')
  const { createLogger, LEVELS } = require('../src/loggers')
  const logCreator = require('../src/loggers/console')
  const logger = createLogger({ level: LEVELS.NOTHING, logCreator })
  const broker = new Broker({
    connectionPool: new ConnectionPool({
      host,
      port,
      logger,
      socketFactory: socketFactory || require('../src/network/socketFactory')(),
      connectionTimeout: timeout,
      requestTimeout: timeout,
    }),
    logger,
  })
  try {
    await broker.connect()
    await broker.metadata([])
    return true
  } catch {
    return false
  } finally {
    await broker.disconnect().catch(() => {})
  }
}

/**
 * Waits until every partition of `topics`, and of the internal topics the cluster has, has its
 * whole replica set in sync (`isr.length === replicas.length`), as the brokers' metadata reports
 * it.
 *
 * @param {import('../types').Admin} admin a connected admin client
 * @param {string[]} topics the workload topics
 * @param {{ budgetMs?: number, delayMs?: number }} [options]
 * @returns {Promise<void>}
 * @throws {Error} naming the partitions still short when the budget runs out
 */
const waitForFullIsr = async (admin, topics, { budgetMs = 60000, delayMs = 250 } = {}) => {
  const deadline = Date.now() + budgetMs
  for (;;) {
    const internal = (await admin.listTopics()).filter((name) => INTERNAL_TOPICS.includes(name))
    const metadata = await admin.fetchTopicMetadata({ topics: [...topics, ...internal] })
    const short = metadata.topics.flatMap(({ name, partitions }) =>
      partitions
        .filter(({ isr, replicas }) => isr.length !== replicas.length)
        .map(({ partitionId }) => `${name}-${partitionId}`)
    )
    if (short.length === 0) return
    if (Date.now() >= deadline) {
      throw new Error(`ISR not full after ${budgetMs} ms: ${short.join(', ')}`)
    }
    await sleep(delayMs)
  }
}

/**
 * Takes compose nodes down and brings them back one at a time. Before every step it checks that no
 * node is down and that every node answers, and refuses the step otherwise, so the three KRaft
 * controllers never lose two voters at once. Every step restores its node in a `finally` (start or
 * unpause) and waits until it answers again, and after a restart until the ISR is full, even when
 * the step failed. An outage is measured through restored communication: from the moment the
 * down command returned to the first answer after the restore (and, for a restart, the full ISR).
 */
class BrokerBouncer {
  /**
   * @param {object} [boundary] the outside world; a unit spec replaces it
   * @param {(...args: string[]) => *} [boundary.run] runs `docker compose <args>`
   * @param {(node: { host: string, port: number }) => Promise<boolean>} [boundary.answers]
   * @param {(admin: object, topics: string[]) => Promise<void>} [boundary.waitForFullIsr]
   * @param {() => number} [boundary.now]
   * @param {(ms: number) => Promise<void>} [boundary.sleep]
   * @param {Record<string, { host: string, port: number }>} [boundary.nodes]
   */
  constructor({
    run = compose,
    answers: probe = answers,
    waitForFullIsr: isr = waitForFullIsr,
    now = Date.now,
    sleep: wait = sleep,
    nodes = NODES,
  } = {}) {
    Object.assign(this, { run, probe, isr, now, wait, nodes })
    this.down = new Set()
  }

  /**
   * @returns {Promise<void>}
   * @throws {Error} refusing the next step: a node this process took down is not back, or a node
   *   does not answer
   */
  async assertAllUp() {
    if (this.down.size > 0) {
      throw new Error(`Refusing a bounce step while ${[...this.down].join(', ')} is down`)
    }
    for (const [service, node] of Object.entries(this.nodes)) {
      if (!(await this.probe(node))) {
        throw new Error(`Refusing a bounce step: ${service} does not answer`)
      }
    }
  }

  /**
   * Polls until the node answers on its advertised address.
   * @param {string} service
   * @param {{ budgetMs?: number, delayMs?: number }} [options]
   * @returns {Promise<number>} when it first answered, by `now`
   * @throws {Error} when it has not answered within the budget
   */
  async waitForReachable(service, { budgetMs = 120000, delayMs = 200 } = {}) {
    const deadline = this.now() + budgetMs
    for (;;) {
      if (await this.probe(this.nodes[service])) return this.now()
      if (this.now() >= deadline) {
        throw new Error(`${service} did not answer within ${budgetMs} ms`)
      }
      await this.wait(delayMs)
    }
  }

  /**
   * One outage of one node.
   * @param {{ service: string, kind: 'pause'|'stop'|'kill', holdMs?: number }} step `pause` is
   *   restored by `unpause`, `stop` and `kill` by `start`; the node stays down at least `holdMs`
   * @param {{ admin: object, topics: string[] }} cluster what a restart waits on
   * @returns {Promise<{ service: string, kind: string, tDown: number, tRestored: number,
   *   outageMs: number }>}
   * @throws {Error} refusing the step (nothing is run), or the step's first failure once the node
   *   is restored; a node whose restore failed stays marked down, so the next step is refused
   */
  async step({ service, kind, holdMs = 0 }, { admin, topics }) {
    await this.assertAllUp()
    // A concurrent step may have claimed its node while the probes were awaited.
    if (this.down.size > 0) {
      throw new Error(`Refusing a bounce step while ${[...this.down].join(', ')} is down`)
    }
    const failures = []
    let tDown = null
    let tRestored = null
    this.down.add(service)
    try {
      this.run(kind, service)
      tDown = this.now()
      await this.wait(holdMs)
    } catch (error) {
      failures.push(error)
    } finally {
      try {
        this.run(kind === 'pause' ? 'unpause' : 'start', service)
        tRestored = await this.waitForReachable(service)
        if (kind !== 'pause') {
          await this.isr(admin, topics)
          tRestored = this.now()
        }
        this.down.delete(service)
      } catch (error) {
        failures.push(error)
      }
    }
    if (failures.length) throw failures[0]
    return { service, kind, tDown, tRestored, outageMs: tRestored - tDown }
  }

  /**
   * Stops (or, `hard`, kills) and restarts every node in turn, `between` ms apart.
   * @param {{ hard?: boolean, between?: number, admin: object, topics: string[],
   *   services?: string[] }} options
   * @returns {Promise<object[]>} the outage of each step, in order
   * @throws {Error} the first failed step's error, after it restored its node; later nodes are
   *   left alone
   */
  async rollingBounce({ hard = false, between = 0, admin, topics, services }) {
    const outages = []
    for (const service of services || Object.keys(this.nodes)) {
      if (outages.length && between) await this.wait(between)
      outages.push(await this.step({ service, kind: hard ? 'kill' : 'stop' }, { admin, topics }))
    }
    return outages
  }
}

const bouncer = new BrokerBouncer()

/**
 * @param {string} service
 * @param {{ budgetMs?: number, delayMs?: number }} [options]
 * @returns {Promise<number>} see `BrokerBouncer#waitForReachable`
 */
const waitForReachable = (service, options) => bouncer.waitForReachable(service, options)

/**
 * @param {object} options see `BrokerBouncer#rollingBounce`
 * @returns {Promise<object[]>}
 */
const rollingBounce = (options) => bouncer.rollingBounce(options)

/**
 * Reassigns a partition's replica set so `newLeaderId` is first (the preferred leader), without
 * moving any replica off or onto a broker that doesn't already host it - this is a *reordering*,
 * not a real data move, so `--verify` completes almost immediately.
 *
 * Only the reassignment is written here; leadership does not move until
 * `triggerPreferredElection` runs.
 *
 * @param {string} topic
 * @param {number[]} replicas the partition's current replica ids
 * @param {number} newLeaderId the replica id to make preferred
 * @returns {void}
 * @throws {Error} if either CLI invocation exits non-zero
 */
const reassignPreferredReplica = (topic, replicas, newLeaderId) => {
  const reorderedReplicas = [
    newLeaderId,
    ...replicas.filter((replicaId) => replicaId !== newLeaderId),
  ]
  const reassignmentJson = {
    version: 1,
    partitions: [{ topic, partition: 0, replicas: reorderedReplicas }],
  }

  execInKafka1(['sh', '-c', `cat > ${REASSIGNMENT_FILE}`], JSON.stringify(reassignmentJson))
  execInKafka1([
    `${KAFKA_BIN}/kafka-reassign-partitions.sh`,
    '--bootstrap-server',
    BOOTSTRAP_SERVER,
    '--reassignment-json-file',
    REASSIGNMENT_FILE,
    '--execute',
  ])
}

/**
 * True once `kafka-reassign-partitions.sh --verify` reports the reassignment finished.
 *
 * Reads the plan `reassignPreferredReplica` wrote, so it only answers for the most recent
 * reassignment this process asked for.
 *
 * @returns {boolean}
 * @throws {Error} if the CLI invocation exits non-zero
 */
const reassignmentIsComplete = () => {
  const output = execInKafka1([
    `${KAFKA_BIN}/kafka-reassign-partitions.sh`,
    '--bootstrap-server',
    BOOTSTRAP_SERVER,
    '--reassignment-json-file',
    REASSIGNMENT_FILE,
    '--verify',
  ]).toString()

  // Kafka 4.3.1's actual wording, confirmed against a live broker: "Reassignment of partition
  // <topic>-<partition> is completed." - note the trailing "d", which a bare `is complete\b`
  // word-boundary check misses entirely.
  return /reassignment of partition .* is completed/i.test(output)
}

/**
 * Polls `reassignmentIsComplete` until it answers true.
 *
 * @param {number} [deadlineMs=60000] how long to keep polling before giving up
 * @returns {Promise<void>}
 * @throws {Error} if the reassignment has not completed within `deadlineMs`
 */
const waitForReassignmentToComplete = async (deadlineMs = 60000) => {
  const deadline = Date.now() + deadlineMs
  while (Date.now() < deadline) {
    if (reassignmentIsComplete()) return
    await new Promise((resolve) => setTimeout(resolve, 500))
  }
  throw new Error('Partition reassignment did not complete in time')
}

/**
 * Hands leadership of one partition to its preferred (first-listed) replica immediately, instead
 * of waiting for the broker's periodic preferred-election pass.
 *
 * The old leader stays alive, which is the whole point: it keeps answering requests and reports
 * the new leader, so a client can observe a real hinted `NOT_LEADER_OR_FOLLOWER` response.
 *
 * @param {string} topic
 * @param {number} [partition=0]
 * @returns {Buffer} the command's stdout
 * @throws {Error} if the CLI invocation exits non-zero
 */
const triggerPreferredElection = (topic, partition = 0) =>
  execInKafka1([
    `${KAFKA_BIN}/kafka-leader-election.sh`,
    '--bootstrap-server',
    BOOTSTRAP_SERVER,
    '--election-type',
    'PREFERRED',
    '--topic',
    topic,
    '--partition',
    String(partition),
  ])

/**
 * Triggers an unclean leader election for one partition.
 *
 * Setting `unclean.leader.election.enable` dynamically is not enough on its own: the broker waits
 * for its periodic unclean-election thread (five minutes by default), which no test can wait out.
 * The topic is created with the config, and this triggers the election immediately.
 *
 * @see clients/src/main/java/org/apache/kafka/common/config/TopicConfig.java (Apache Kafka 4.3.1)
 * @param {string} topic
 * @param {number} [partition=0]
 * @returns {Buffer} the command's stdout
 * @throws {Error} if the CLI invocation exits non-zero
 */
const triggerUncleanElection = (topic, partition = 0) =>
  execInKafka1([
    `${KAFKA_BIN}/kafka-leader-election.sh`,
    '--bootstrap-server',
    BOOTSTRAP_SERVER,
    '--election-type',
    // Upper case, matching `triggerPreferredElection`'s 'PREFERRED'. `kafka-leader-election.sh`
    // accepts either, and consistency is what keeps the two wrappers reviewable side by side.
    'UNCLEAN',
    '--topic',
    topic,
    '--partition',
    String(partition),
  ])

module.exports = {
  execInKafka1,
  compose,
  stopBroker,
  startBroker,
  killBroker,
  pauseBroker,
  unpauseBroker,
  NODES,
  answers,
  waitForFullIsr,
  waitForReachable,
  rollingBounce,
  BrokerBouncer,
  reassignPreferredReplica,
  reassignmentIsComplete,
  waitForReassignmentToComplete,
  triggerPreferredElection,
  triggerUncleanElection,
}
