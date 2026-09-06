const { execFileSync } = require('child_process')

const BOOTSTRAP_SERVER = 'kafka1:29092'
// Suffixed with this process's pid so two overlapping runs (there should never be one, but the
// file is otherwise never cleaned up) cannot clobber each other's reassignment plan.
const REASSIGNMENT_FILE = `/tmp/leader-move-reassignment-${process.pid}.json`
const KAFKA_BIN = '/opt/kafka/bin'
const COMPOSE_FILE = process.env.COMPOSE_FILE || 'docker-compose.yml'

/**
 * Runs `docker compose` against the cluster the tests are using. Defaults to the root compose
 * file; `COMPOSE_FILE` overrides it, which is how a scenario targets its own.
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
  reassignPreferredReplica,
  reassignmentIsComplete,
  waitForReassignmentToComplete,
  triggerPreferredElection,
  triggerUncleanElection,
}
