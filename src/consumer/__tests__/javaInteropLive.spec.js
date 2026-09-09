const fs = require('fs')
const os = require('os')
const path = require('path')
const { spawn, spawnSync } = require('child_process')
const { Kafka, logLevel } = require('../../../index')
const { range, sticky, cooperativeSticky } = require('../assigners')
const { secureRandom, plainTextBrokers, createTopic } = require('testHelpers')

const RUN_SH = path.join(__dirname, '..', '..', '..', 'scripts', 'e2e', 'javaConsumer', 'run.sh')

/**
 * Compiles the Java component once, at collection time, so the three conditions that make this
 * scenario unrunnable - no JDK, no Maven Central, a missing class - are decided before jest builds
 * the suite. Inside a test they could only be a failure or a silent pass, and a silent pass is not
 * an acceptable outcome for a scenario whose whole point is interoperability.
 * @returns {{ ok: boolean, reason: string, workDir: string }}
 */
const preflight = () => {
  const workDir = fs.mkdtempSync(path.join(os.tmpdir(), 'kafkajs-java-interop-'))
  const probe = spawnSync(RUN_SH, ['--probe'], {
    env: { ...process.env, JAVA_E2E_WORK_DIR: workDir },
    encoding: 'utf8',
    timeout: 120000,
  })

  /** The work dir only outlives a successful probe: a skipped `describe` runs no `afterAll`. */
  const failed = (reason) => {
    fs.rmSync(workDir, { recursive: true, force: true })
    return { ok: false, reason, workDir: null }
  }

  if (probe.error && probe.error.code === 'ENOENT') {
    return failed('run.sh not executable')
  }
  if (probe.status !== 0) {
    const stderr = probe.stderr || ''
    return failed(
      /NoClassDefFoundError/.test(stderr)
        ? 'a class was missing from the classpath'
        : /command not found|javac/.test(stderr)
          ? 'no JDK on PATH'
          : 'the jars could not be downloaded'
    )
  }

  return { ok: true, reason: '', workDir }
}

// Gated: the scenario spawns a JVM against the shared cluster. Run it alone:
// `npm run test:e2e:java-interop`.
const JAVA = process.env.JAVA_INTEROP_E2E === '1' ? preflight() : { ok: false, reason: 'gated' }
if (process.env.JAVA_INTEROP_E2E === '1' && !JAVA.ok) {
  console.warn(`Skipping the Java interop scenario: ${JAVA.reason}`)
}

const describeJavaInterop = JAVA.ok ? describe : describe.skip

// Named so each wait says how long it is prepared to be wrong for.
const ROW_BUDGET_MS = 300000
const CHILD_DEADLINE_MS = 60000
const PARTITIONS = 6
const MAX_WAIT_TIME_MS = 1000
const JOIN_BUDGET = { attempts: 120, delay: 500 } // 60 s
const AGREEMENT_BUDGET = { attempts: 90, delay: 500 } // 45 s, inside the child's own deadline
const GROUP_TYPE_BUDGET = { attempts: 60, delay: 500 } // 30 s, the child's own admin round trip
const DISCONNECT_BUDGET_MS = 15000

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

/**
 * Polls `predicate` on a plain interval until it is true, bounded rather than indefinite.
 * @param {() => boolean|Promise<boolean>} predicate
 * @param {{attempts: number, delay: number}} budget
 * @returns {Promise<boolean>} whether `predicate` became true within the attempt budget
 */
const waitForCondition = async (predicate, budget) => {
  for (let attempt = 0; attempt < budget.attempts; attempt++) {
    if (await predicate()) {
      return true
    }
    await sleep(budget.delay)
  }
  return false
}

/**
 * Awaits `promise` but gives up after `ms`, so a teardown can never outlive its hook budget.
 * @param {Promise<unknown>} promise
 * @param {number} ms
 * @returns {Promise<void>}
 */
const withTimeout = async (promise, ms) => {
  await Promise.race([promise, sleep(ms)])
}

/** The three assignors both clients implement, by the protocol name the coordinator sees. */
const ASSIGNORS = [
  {
    protocolName: 'range',
    javaClass: 'org.apache.kafka.clients.consumer.RangeAssignor',
    createAssigner: range,
  },
  {
    protocolName: 'sticky',
    javaClass: 'org.apache.kafka.clients.consumer.StickyAssignor',
    createAssigner: sticky,
  },
  {
    protocolName: 'cooperative-sticky',
    javaClass: 'org.apache.kafka.clients.consumer.CooperativeStickyAssignor',
    createAssigner: cooperativeSticky,
  },
]

/**
 * The two assignors the coordinator itself runs under the consumer protocol. They are named, not
 * classes: no client-side assignor exists under KIP-848, so both members only ask for one.
 * @see https://kafka.apache.org/43/documentation.html#consumerconfigs_group.remote.assignor
 */
const SERVER_ASSIGNORS = ['uniform', 'range']

/**
 * Live proof that a member of this client and a member of the Apache Kafka Java client share one
 * group under each of the three Java assignors, and agree on who holds what.
 *
 * The comparison is between FULL assignments on both sides: the Java component prints its whole
 * owned set after every rebalance callback rather than the callback's argument, because a
 * cooperative round's callbacks carry only the delta and a delta compared against a full set
 * would read as a disagreement.
 *
 * **Both leader directions are exercised** on the classic arm. Each assignor runs twice, once with
 * this client forming the group first and once with the Java member forming it first. The
 * coordinator keeps an existing leader across a rebalance as long as it rejoins - `GroupMetadata`
 * only elects a new one when the current leader has not - so whichever member forms the empty
 * group computes every assignment afterwards. The Java-leader direction is the half that proves
 * this client *decodes* an assignment produced by Java's own assignor; the JavaScript-leader
 * direction proves the assignments this client produces are ones Java accepts.
 *
 * The consumer arm has no such directions: under KIP-848 the coordinator computes the assignment
 * and neither member is a leader, so the two rows there vary the **server** assignor instead and
 * assert that the same membership gets the same assignment twice.
 *
 * @see https://github.com/apache/kafka/blob/4.3/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/classic/ClassicGroup.java
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/consumer/CooperativeStickyAssignor.java
 */
describeJavaInterop('Consumer > mixed Java/JavaScript group (live)', () => {
  // Outside the arms on purpose: the work dir holds the jars and the compiled class both arms run,
  // so it may only be removed once every row has finished.
  afterAll(() => {
    JAVA.workDir && fs.rmSync(JAVA.workDir, { recursive: true, force: true })
  })

  describe.each(['classic', 'consumer'])('%s protocol', (groupProtocol) => {
    let kafka
    let consumer
    let child

    afterEach(async () => {
      if (child && child.exitCode == null) {
        child.kill('SIGKILL')
      }
      child = null

      consumer &&
        (await withTimeout(
          consumer.disconnect().catch(() => {}),
          DISCONNECT_BUDGET_MS
        ))
      consumer = kafka = null
    }, 60000)

    /**
     * Spawns the Java member and collects its stdout JSON and its stderr. Its own kill deadline is
     * well inside the row's budget, so a child that hangs fails one row rather than the suite.
     * @param {object} options
     * @param {string} options.groupId
     * @param {string} options.topic
     * @param {string} options.assignor a Java assignor class, or a server assignor name
     * @param {boolean} [options.reportGroupType] ask the child to print the group's type once
     * @returns {{ events: object[], stderr: string[], owned: () => number[],
     *             groupType: () => object|undefined }}
     */
    const spawnJavaMember = ({ groupId, topic, assignor, reportGroupType = false }) => {
      const events = []
      const stderr = []

      child = spawn(
        RUN_SH,
        [
          ...(groupProtocol === 'consumer' ? ['--group-protocol', 'consumer'] : []),
          ...(reportGroupType ? ['--report-group-type'] : []),
          plainTextBrokers().join(','),
          groupId,
          topic,
          assignor,
          String(CHILD_DEADLINE_MS),
        ],
        {
          env: { ...process.env, JAVA_E2E_WORK_DIR: JAVA.workDir },
          stdio: ['ignore', 'pipe', 'pipe'],
        }
      )

      let stdoutBuffer = ''
      child.stdout.setEncoding('utf8')
      child.stdout.on('data', (chunk) => {
        stdoutBuffer += chunk
        const lines = stdoutBuffer.split('\n')
        stdoutBuffer = lines.pop()
        for (const line of lines) {
          if (!line.startsWith('{')) {
            continue
          }

          try {
            events.push(JSON.parse(line))
          } catch (e) {
            // A malformed line is diagnostic material, not a reason to throw inside a stream
            // handler where it would surface as an uncaught exception.
            stderr.push(`unparsable stdout line: ${line}\n`)
          }
        }
      })
      child.stderr.setEncoding('utf8')
      child.stderr.on('data', (chunk) => stderr.push(chunk))

      return {
        events,
        stderr,
        /** @returns {number[]} the Java member's full owned set for the test topic */
        owned: () => {
          const last = events
            .filter(({ event }) => event !== 'done' && event !== 'group-type')
            .at(-1)
          return last ? [...(last.partitions[topic] || [])].sort((a, b) => a - b) : []
        },
        /** @returns {object|undefined} the line the child's own admin client printed */
        groupType: () => events.find(({ event }) => event === 'group-type'),
      }
    }

    /**
     * Builds this client's member of the group and starts it consuming.
     * @param {object} options
     * @param {string} options.groupId
     * @param {string} options.topic
     * @param {string} options.clientId
     * @param {object} options.memberOptions the arm's membership options, passed as given
     * @returns {Promise<{ groupJoins: object[], crashes: object[], owned: () => number[] }>}
     */
    const startJavaScriptMember = async ({ groupId, topic, clientId, memberOptions }) => {
      kafka = new Kafka({ clientId, brokers: plainTextBrokers(), logLevel: logLevel.NOTHING })

      consumer = kafka.consumer({
        groupId,
        ...memberOptions,
        maxWaitTimeInMs: MAX_WAIT_TIME_MS,
      })

      const groupJoins = []
      const crashes = []
      consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => groupJoins.push(payload))
      consumer.on(consumer.events.CRASH, ({ payload }) => crashes.push(payload))

      await consumer.connect()
      await consumer.subscribe({ topic, fromBeginning: false })
      consumer.run({ eachMessage: async () => {} }).catch((e) => crashes.push({ error: e }))

      return {
        groupJoins,
        crashes,
        /** @returns {number[]} this client's own assignment for the test topic */
        owned: () =>
          groupJoins.length === 0
            ? []
            : [...((groupJoins.at(-1).memberAssignment || {})[topic] || [])].sort((a, b) => a - b),
      }
    }

    /**
     * Whether the two members hold every partition exactly once between them.
     * @param {number[]} javaOwned
     * @param {number[]} javaScriptOwned
     * @returns {boolean}
     */
    const coversEveryPartitionOnce = (javaOwned, javaScriptOwned) => {
      const together = [...javaOwned, ...javaScriptOwned]
      return (
        javaOwned.length > 0 &&
        javaScriptOwned.length > 0 &&
        together.length === PARTITIONS &&
        new Set(together).size === PARTITIONS
      )
    }

    if (groupProtocol === 'classic') {
      for (const { protocolName, javaClass, createAssigner } of ASSIGNORS) {
        for (const javaLeads of [false, true]) {
          const leader = javaLeads ? 'the Java member' : 'this client'

          it(
            `forms one group with the Java client under ${protocolName}, ${leader} leading`,
            async () => {
              const topic = `test-topic-${secureRandom()}`
              const groupId = `test-group-${secureRandom()}`
              await createTopic({ topic, partitions: PARTITIONS })

              const clientId = `java-interop-${protocolName}`
              const memberOptions = { groupProtocol, partitionAssigners: [createAssigner] }
              let java
              let js

              // Whoever forms the empty group is its leader, and stays leader while it keeps
              // rejoining - so the join order below is what decides which side's assignor runs.
              if (javaLeads) {
                java = spawnJavaMember({ groupId, topic, assignor: javaClass })
                const javaFormedTheGroup = await waitForCondition(
                  () => java.owned().length === PARTITIONS,
                  JOIN_BUDGET
                )
                expect(javaFormedTheGroup).toBe(true)

                js = await startJavaScriptMember({ groupId, topic, clientId, memberOptions })
                expect(await waitForCondition(() => js.groupJoins.length > 0, JOIN_BUDGET)).toBe(
                  true
                )
              } else {
                js = await startJavaScriptMember({ groupId, topic, clientId, memberOptions })
                expect(await waitForCondition(() => js.groupJoins.length > 0, JOIN_BUDGET)).toBe(
                  true
                )
                expect(js.owned().length).toEqual(PARTITIONS)

                java = spawnJavaMember({ groupId, topic, assignor: javaClass })
              }

              expect(js.groupJoins.at(-1).groupProtocol).toEqual(protocolName)

              const agreed = await waitForCondition(
                () => coversEveryPartitionOnce(java.owned(), js.owned()),
                AGREEMENT_BUDGET
              )

              // Checked before the agreement assertion: a classpath hole, a bad assignor class name
              // or a ConfigException all produce the same empty assignment otherwise, with nothing
              // in the failure output to say which.
              const childStderr = java.stderr.join('')
              expect(childStderr).not.toMatch(
                /NoClassDefFoundError|ClassNotFoundException|ConfigException/
              )
              expect({
                agreed,
                javaOwned: java.owned(),
                javaScriptOwned: js.owned(),
                lastJavaEvent: java.events.at(-1) || null,
                childExitCode: child && child.exitCode,
                childStderr,
              }).toMatchObject({ agreed: true })

              // The leader is the member that formed the group, so the assignments above were
              // computed by that side's assignor.
              expect(js.groupJoins.at(-1).isLeader).toBe(!javaLeads)

              // No INCONSISTENT_GROUP_PROTOCOL (23): a mismatched protocol name on either side
              // would show up here rather than as a disagreement about partitions.
              expect(
                js.crashes.map(({ error }) => error && error.type).filter(Boolean)
              ).not.toContain('INCONSISTENT_GROUP_PROTOCOL')
              expect(js.crashes).toEqual([])

              console.log('mixed Java/JavaScript group observed', {
                groupProtocol,
                topic,
                groupId,
                protocolName,
                javaClass,
                leader,
                javaAssignment: java.owned(),
                javaScriptAssignment: js.owned(),
                javaScriptIsLeader: js.groupJoins.at(-1).isLeader,
                javaRebalanceEvents: java.events.length,
              })
            },
            ROW_BUDGET_MS
          )
        }
      }
    } else {
      for (const serverAssignor of SERVER_ASSIGNORS) {
        it(
          `forms one leaderless group with the Java client under the ${serverAssignor} server assignor`,
          async () => {
            const topic = `test-topic-${secureRandom()}`
            await createTopic({ topic, partitions: PARTITIONS })

            /**
             * One full run of the pair: both members join a fresh group, converge, and are then
             * stopped. Returns what each side ended up holding and this client's last join.
             * @param {string} label
             * @returns {Promise<{groupId: string, javaOwned: number[], javaScriptOwned: number[],
             *                    groupJoin: object, groupType: object|undefined}>}
             */
            const run = async (label) => {
              const groupId = `test-group-${secureRandom()}`
              const java = spawnJavaMember({
                groupId,
                topic,
                assignor: serverAssignor,
                reportGroupType: true,
              })
              const js = await startJavaScriptMember({
                groupId,
                topic,
                clientId: `java-interop-${serverAssignor}-${label}`,
                memberOptions: { groupProtocol, groupRemoteAssignor: serverAssignor },
              })

              const agreed = await waitForCondition(
                () => coversEveryPartitionOnce(java.owned(), js.owned()),
                AGREEMENT_BUDGET
              )

              const childStderr = java.stderr.join('')
              expect(childStderr).not.toMatch(
                /NoClassDefFoundError|ClassNotFoundException|ConfigException/
              )
              expect({
                label,
                agreed,
                javaOwned: java.owned(),
                javaScriptOwned: js.owned(),
                lastJavaEvent: java.events.at(-1) || null,
                childExitCode: child && child.exitCode,
                childStderr,
              }).toMatchObject({ agreed: true })
              expect(js.crashes).toEqual([])

              // Read through the Java client's own admin API, so the group's type is the
              // coordinator's word rather than this client's.
              await waitForCondition(() => Boolean(java.groupType()), GROUP_TYPE_BUDGET)

              const result = {
                groupId,
                javaOwned: java.owned(),
                javaScriptOwned: js.owned(),
                groupJoin: js.groupJoins.at(-1),
                groupType: java.groupType(),
              }

              child.kill('SIGKILL')
              child = null
              await withTimeout(
                consumer.disconnect().catch(() => {}),
                DISCONNECT_BUDGET_MS
              )
              consumer = kafka = null

              return result
            }

            const first = await run('first')
            const second = await run('second')

            // Neither member is a leader under this protocol: the coordinator computes the
            // assignment and sends each member only its own share.
            for (const { groupJoin } of [first, second]) {
              expect(groupJoin.leaderId).toBeNull()
              expect(groupJoin.isLeader).toBe(false)
              // `groupProtocol` echoes the assignor this member asked for, not the one the
              // coordinator settled on; that is only visible through admin.describeGroups.
              expect(groupJoin.groupProtocol).toEqual(serverAssignor)
            }

            expect(first.groupType).toMatchObject({ groupType: 'Consumer' })
            expect(second.groupType).toMatchObject({ groupType: 'Consumer' })

            // The server assignor is deterministic for a fixed membership, so the same two
            // members get the same two shares twice. Which of them gets which share is NOT
            // asserted: both member ids are freshly generated per run, and the assignor orders
            // members by id.
            const shapeOf = ({ javaOwned, javaScriptOwned }) =>
              [javaOwned, javaScriptOwned]
                .map((partitions) => partitions.join(','))
                .sort()
                .join(' | ')
            expect(shapeOf(second)).toEqual(shapeOf(first))

            console.log('mixed Java/JavaScript consumer-protocol group observed', {
              groupProtocol,
              topic,
              serverAssignor,
              first: {
                groupId: first.groupId,
                javaAssignment: first.javaOwned,
                javaScriptAssignment: first.javaScriptOwned,
                groupType: first.groupType,
              },
              second: {
                groupId: second.groupId,
                javaAssignment: second.javaOwned,
                javaScriptAssignment: second.javaScriptOwned,
                groupType: second.groupType,
              },
            })
          },
          ROW_BUDGET_MS
        )
      }
    }
  })
})
