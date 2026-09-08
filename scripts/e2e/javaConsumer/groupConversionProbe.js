/**
 * Online classic -> consumer group-conversion probe.
 *
 * Usage: node scripts/e2e/javaConsumer/groupConversionProbe.js [--partitions 3]
 *          [--java-run-ms 40000] [--timeout 120000]
 * Prints one JSON line:
 *   { kafkaVersion, typeBefore, typeWithConsumerMember, typeAfterConsumerMemberLeft,
 *     classicMembersKeptConsuming, converts, convertsBack, classicAssignmentsSeen }
 *
 * Development-only. It answers one question the migration scenario cannot assume: does a classic
 * group with members still in it convert to a consumer group when a consumer-protocol member
 * joins, and convert back when that member leaves? The documentation says it does, with a
 * precondition about assignor metadata; this measures it on the cluster this repo tests against.
 * @see https://kafka.apache.org/43/operations/consumer-rebalance-protocol/
 *
 * The consumer-protocol member is the Java interop member, because this client cannot speak the
 * protocol yet, and the group's type is read through the Java admin client for the same reason.
 * The classic members are this client's own, so "the classic members kept consuming" means what
 * the migration scenario needs it to mean.
 *
 * A verdict is printed only when all three type readings succeeded: a measurement that broke
 * throws and exits non-zero rather than printing `converts: false`, which would be read as the
 * broker's answer. Negative control, no cluster needed - this must exit non-zero, print no
 * `group-type` line and name the connection failure on stderr:
 *
 *   scripts/e2e/javaConsumer/run.sh --describe-group localhost:1 any-group
 */
const path = require('path')
const { spawn, spawnSync } = require('child_process')
const { Kafka, logLevel } = require('../../../index')

const RUN_SH = path.join(__dirname, 'run.sh')
const BROKERS = ['localhost:9092', 'localhost:9095', 'localhost:9098']

const arg = (name, fallback) => {
  const i = process.argv.indexOf(`--${name}`)
  return i === -1 ? fallback : Number(process.argv[i + 1])
}

const PARTITIONS = arg('partitions', 3)
const JAVA_RUN_MS = arg('java-run-ms', 40000)
const TIMEOUT_MS = arg('timeout', 120000)

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

/**
 * Reads one group's type and state through the Java admin client.
 * @param {string} groupId
 * @returns {{ groupType: string, groupState: string }}
 */
const describeGroupType = (groupId) => {
  const result = spawnSync(RUN_SH, ['--describe-group', BROKERS[0], groupId], {
    env: { ...process.env, JAVA_E2E_WORK_DIR: process.env.JAVA_E2E_WORK_DIR },
    encoding: 'utf8',
  })

  const line = (result.stdout || '')
    .split('\n')
    .find((candidate) => candidate.startsWith('{"event":"group-type"'))

  if (result.status !== 0 || !line) {
    throw new Error(
      `could not read the type of ${groupId} (exit ${result.status}): ${result.stdout}${result.stderr}`
    )
  }

  return JSON.parse(line)
}

/**
 * Guards one type reading. An empty type means the coordinator did not know the group, which is a
 * broken measurement rather than the broker answering "no conversion" - reporting it as
 * `converts: false` would silently flip the migration scenario to its weaker form.
 *
 * @param {string} label which of the three readings this is, for the message
 * @param {string} groupType the type read back
 * @returns {string} the same type
 * @throws {Error} when the type is empty
 */
const requireGroupType = (label, groupType) => {
  if (!groupType) {
    throw new Error(`no group type ${label}: the measurement is not valid`)
  }

  return groupType
}

;(async () => {
  const kafka = new Kafka({
    clientId: 'group-conversion-probe',
    brokers: BROKERS,
    logLevel: logLevel.NOTHING,
  })

  const suffix = Date.now()
  const topic = `group-conversion-probe-${suffix}`
  const groupId = `group-conversion-probe-${suffix}`

  const admin = kafka.admin()
  await admin.connect()
  await admin.createTopics({
    topics: [{ topic, numPartitions: PARTITIONS, replicationFactor: 1 }],
    waitForLeaders: true,
  })

  // Two classic members of this client, running and holding partitions before anything converts.
  const classicMembers = [kafka.consumer({ groupId }), kafka.consumer({ groupId })]
  const assignments = [[], []]
  classicMembers.forEach((consumer, index) => {
    consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => {
      assignments[index].push(payload.memberAssignment[topic] || [])
    })
  })

  let java
  try {
    for (const consumer of classicMembers) {
      await consumer.connect()
      await consumer.subscribe({ topic, fromBeginning: false })
      await consumer.run({ eachBatch: async () => {} })
    }

    const deadline = Date.now() + TIMEOUT_MS
    while (assignments.some((seen) => seen.length === 0)) {
      if (Date.now() > deadline) {
        throw new Error('the two classic members never joined')
      }
      await sleep(100)
    }

    const typeBefore = requireGroupType(
      'before the consumer-protocol member joined',
      describeGroupType(groupId).groupType
    )

    if (typeBefore !== 'Classic') {
      throw new Error(`the group was ${typeBefore}, not Classic, before the probe started`)
    }

    const joinsBeforeConversion = assignments.map((seen) => seen.length)

    // The consumer-protocol member joins the *running* classic group. Its own group-type line is
    // read after it holds partitions, which is the only moment at which a conversion is
    // observable rather than merely expected.
    const javaLines = []
    java = spawn(
      RUN_SH,
      [
        '--group-protocol',
        'consumer',
        '--report-group-type',
        BROKERS[0],
        groupId,
        topic,
        'uniform',
        String(JAVA_RUN_MS),
      ],
      { env: process.env, stdio: ['ignore', 'pipe', 'pipe'] }
    )
    java.stdout.setEncoding('utf8')
    java.stdout.on('data', (chunk) => javaLines.push(...chunk.split('\n').filter(Boolean)))
    java.stderr.setEncoding('utf8')
    java.stderr.on('data', (chunk) => javaLines.push(`stderr: ${chunk}`))

    const typeLineDeadline = Date.now() + TIMEOUT_MS
    while (!javaLines.some((line) => line.startsWith('{"event":"group-type"'))) {
      if (Date.now() > typeLineDeadline || java.exitCode !== null) {
        throw new Error(`the consumer-protocol member never reported: ${javaLines.join(' | ')}`)
      }
      await sleep(200)
    }

    const typeWithConsumerMember = requireGroupType(
      'while the consumer-protocol member was in the group',
      JSON.parse(javaLines.find((line) => line.startsWith('{"event":"group-type"'))).groupType
    )

    // Did the classic members survive the conversion, or were they evicted? Either they kept the
    // assignment they had, or they rejoined and got one - both count as "kept consuming"; being
    // dropped out of the group does not.
    await sleep(5000)
    const classicMembersKeptConsuming = assignments.every((seen, index) => {
      const rejoinedOrKept = seen.length >= joinsBeforeConversion[index]
      const holdsPartitions = seen[seen.length - 1].length > 0
      return rejoinedOrKept && holdsPartitions
    })

    await new Promise((resolve) => java.on('exit', resolve))
    java = null
    await sleep(5000)

    const typeAfterConsumerMemberLeft = requireGroupType(
      'after the consumer-protocol member left',
      describeGroupType(groupId).groupType
    )

    console.log(
      JSON.stringify({
        kafkaVersion: process.env.KAFKA_VERSION || '4.3.1',
        typeBefore,
        typeWithConsumerMember,
        typeAfterConsumerMemberLeft,
        classicMembersKeptConsuming,
        converts: typeBefore === 'Classic' && typeWithConsumerMember === 'Consumer',
        convertsBack:
          typeWithConsumerMember === 'Consumer' && typeAfterConsumerMemberLeft === 'Classic',
        classicAssignmentsSeen: assignments.map((seen) => seen.length),
      })
    )
  } finally {
    if (java) {
      java.kill('SIGKILL')
    }
    for (const consumer of classicMembers) {
      await consumer.stop().catch(() => {})
      await consumer.disconnect().catch(() => {})
    }
    await admin.deleteTopics({ topics: [topic] }).catch(() => {})
    await admin.disconnect().catch(() => {})
  }
})().catch((e) => {
  console.error(e)
  process.exit(1)
})
