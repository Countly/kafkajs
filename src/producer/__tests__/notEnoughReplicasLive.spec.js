const { execFile } = require('child_process')
const { promisify } = require('util')
const { Kafka, logLevel } = require('../../../index')
const {
  plainTextBrokers,
  secureRandom,
  waitForTopicPropagation,
  writeEvidence,
  wireVersionsOf,
} = require('testHelpers')
const { BrokerBouncer, NODES } = require('../../../testHelpers/leaderElection')
const { bounded, caseTimeout, liveCase, pollUntil } = require('./fixtures/bounded')
const { createLiveSockets, repliesOf } = require('./fixtures/liveWire')
const {
  CleanupStack,
  ProducerProbe,
  describeError,
  readData,
  settle,
  topicConfig,
} = require('./fixtures/liveProbe')
const {
  CASE_BUDGET_MS,
  CaseTimeline,
  FollowerOutage,
  JOIN_RESERVE_MS,
  PHASE_BUDGET_MS,
  RESTORE_STEP_MS,
  TopologyPin,
  isrIsFull,
} = require('./fixtures/replicaOutage')

// Gated: it stops one broker of the shared cluster, through the guarded entry only. Run alone:
// `npm run test:e2e:not-enough-replicas`.
const describeReplicas = process.env.NOT_ENOUGH_REPLICAS_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'not-enough-replicas'
const REQUEST_TIMEOUT_MS = 1000
const DELIVERY_TIMEOUT_MS = 5000
const DELIVERY_SLACK_MS = 1000
const BUFFER_MEMORY = 1048576
const MAX_REQUEST_SIZE = 1048576
const DISCONNECT_BUDGET_MS = 8000
const QUIET_BUDGET = { attempts: 40, delay: 100 } // 4 s
const FULL_ISR_MS = 8000
const GUARD_MS = Object.freeze({ stop: 20000, start: RESTORE_STEP_MS.start })
const HOOK_BUDGET_MS = PHASE_BUDGET_MS.restore + JOIN_RESERVE_MS
const RELAXED_SENDS = 3
const RECOVERY_MARKERS = 10
const PRE_APPEND = 'NOT_ENOUGH_REPLICAS'
const AFTER_APPEND = 'NOT_ENOUGH_REPLICAS_AFTER_APPEND'

const REPLAY = Object.freeze({
  log: 'acknowledged records once; a pre-append rejection (19) is absent, an after-append one (20) may be present once; an explicit application resend can duplicate',
  offsets: 'n/a',
  handlers: 'n/a',
})

const execFileAsync = promisify(execFile)

/**
 * Runs one broker action through the guarded live entry, which owns the cluster this run started
 * and refuses any broker it did not create. The guard process is killed by the operating system at
 * the action's own budget and when `signal` aborts, so a stop or a start that hangs ends inside its
 * phase and is settled when this promise is.
 * @param {string} operation `stop` or `start`
 * @param {string} service `kafka1`, `kafka2` or `kafka3`
 * @param {object} [options]
 * @param {AbortSignal} [options.signal] the phase's, which kills the guard when it aborts
 * @returns {Promise<{ stdout: string, stderr: string }>}
 * @throws {Error} when the run was not started by the guarded entry, the operation has no budget,
 *   or the guard refuses
 */
const guardedRun = (operation, service, { signal } = {}) => {
  const { P9_CLUSTER_GUARD: guard, P9_CLUSTER_MANIFEST: manifest } = process.env
  if (!guard || !manifest) {
    throw new Error('This scenario stops a broker only through the guarded live entry')
  }
  if (GUARD_MS[operation] === undefined) throw new Error(`No guard budget for ${operation}`)
  return execFileAsync(
    'timeout',
    [`${GUARD_MS[operation] / 1000}s`, 'node', guard, 'control', manifest, operation, service],
    { signal }
  )
}

/**
 * Live proof of what an idempotent producer does while one follower of its partitions is down. A
 * topic that needs all three replicas in sync (`min.insync.replicas` 3) cannot be written: the
 * send retries until its delivery deadline and ends with `KafkaJSDeliveryTimeout` whose cause is
 * NOT_ENOUGH_REPLICAS. A topic that needs two keeps accepting writes while the same follower stays
 * down, each send within the deadline. The follower is the one node that leads neither partition,
 * the other two voters stay up, and it is restored, and both partitions are back in sync, before
 * the recovery markers: the stop and start go through the guarded entry only.
 *
 * The topology is pinned before the fault: both partitions' replica sets and leaders. While the
 * fault is reached only the chosen follower may leave the ISR, and around every measured send,
 * with the follower still stopped, the ISR must be exactly the two survivors and the leaders and
 * replica sets the pinned ones; any other movement fails the case and its snapshots are stamped
 * in the evidence.
 *
 * Budget, serial and inside the 180 s of the case (`PHASE_BUDGET_MS`): 15 s setup, 25 s to refuse
 * a bad cluster and stop the follower, 35 s for the leaders to drop it from the ISR, 30 s for the
 * four measured sends with their topology reads, 35 s to restore it and wait for the full ISR
 * (15 s start, 8 s reachable, 12 s ISR), 20 s for the markers and the probe and 15 s for the
 * cleanup, and a reserve of 4 s for the operations a phase told to stop. The case has one owner,
 * the `AbortController` of its `CaseTimeline`, and every phase takes its signal: each awaited
 * step is a `bounded` call, so a phase that runs out of time aborts its scope, the step that was
 * waiting rejects at once, and the case itself issues no later step, send or broker action after
 * it. An operation that ignores its abort may still finish later; that, a failed cleanup and a
 * failed restore taint the run, and every later case of the file then refuses to start, while the
 * guard contains the run. The actual timings are stamped in the evidence. The restore is one attempt, awaited by the
 * case and by a separate hook alike, under a signal of its own so that it survives the abort of
 * the case, and it always settles inside its phase; a follower it could not restore stays owned,
 * so a case that timed out cannot leave a broker down for the next one. The cleanup is one
 * bounded sequence: each cleanup that outlasts its share is aborted and given the grace to stop.
 *
 * Replay sets: the physical log holds each acknowledged record once; a rejection before the append
 * (code 19) is absent, one after it (code 20) may be present once; an explicit application resend
 * may duplicate; committed offsets and handler deliveries are not part of this scenario.
 * @see https://kafka.apache.org/43/configuration/topic-configs/#topicconfigs_min.insync.replicas
 */
describeReplicas('Producer > one follower down under min.insync.replicas (live)', () => {
  /** @type {FollowerOutage|null} the outage of the running case, until it ended restored */
  let owned = null

  afterEach(async () => {
    const outage = owned
    owned = null
    const failure = await outage?.restore()
    if (failure) throw failure
  }, HOOK_BUDGET_MS)

  test(
    'a send needing every replica ends at the delivery deadline while a send needing two succeeds, and the markers follow once the follower is back',
    liveCase(CASE_BUDGET_MS, async () => {
      const id = secureRandom()
      const strict = `e2e-p9b-not-enough-replicas-all-${id}`
      const relaxed = `e2e-p9b-not-enough-replicas-two-${id}`
      const topics = [strict, relaxed]
      const clientId = `e2e-p9b-not-enough-replicas-${id}`
      const brokers = plainTextBrokers()
      const sockets = createLiveSockets()
      const requests = []
      const cleanup = new CleanupStack()

      const timeline = new CaseTimeline()

      const runCase = async () => {
        const kafka = new Kafka({
          clientId,
          brokers,
          logLevel: logLevel.NOTHING,
          socketFactory: sockets.socketFactory,
          requestTimeout: REQUEST_TIMEOUT_MS,
          enforceRequestTimeout: true,
        })
        const producer = kafka.producer({
          idempotent: true,
          linger: 0,
          deliveryTimeout: DELIVERY_TIMEOUT_MS,
          bufferMemory: BUFFER_MEMORY,
          maxRequestSize: MAX_REQUEST_SIZE,
        })
        producer.on(producer.events.REQUEST, ({ payload }) => requests.push(payload))
        const probe = new ProducerProbe({
          producer,
          frames: sockets.frames,
          clientId,
          maxRequestSize: MAX_REQUEST_SIZE,
          quietBudget: QUIET_BUDGET,
        })
        const admin = new Kafka({ brokers, logLevel: logLevel.NOTHING }).admin()
        const bouncer = new BrokerBouncer({ run: guardedRun })
        probe.install()
        cleanup.add('admission probe', () => probe.restore())
        cleanup.add('resources after disconnect', (signal) => probe.settled({ signal }))
        cleanup.add('admin', () => admin.disconnect())
        cleanup.add('producer', () => producer.disconnect({ timeout: DISCONNECT_BUDGET_MS }))

        /** One topology read inside a phase. */
        const readTopology = async (pinned, signal) =>
          pinned.snapshot(
            (await bounded(signal, Infinity, () => admin.fetchTopicMetadata({ topics }))).topics
          )
        const timed = async (signal, topic, value) => {
          const startedAt = performance.now()
          const { error } = await bounded(signal, Infinity, () =>
            settle(producer.send({ topic, messages: [{ value }], acks: -1 }))
          )
          return { value, ms: performance.now() - startedAt, error }
        }

        const { configs, pin } = await timeline.run('setup', async (signal) => {
          const go = (op) => bounded(signal, Infinity, op)
          await go(() => admin.connect())
          await go(() =>
            admin.createTopics({
              waitForLeaders: true,
              topics: [
                [strict, '3'],
                [relaxed, '2'],
              ].map(([topic, minimum]) => ({
                topic,
                replicaAssignment: [{ partition: 0, replicas: [1, 2, 3] }],
                configEntries: [{ name: 'min.insync.replicas', value: minimum }],
              })),
            })
          )
          await go(() => waitForTopicPropagation(strict, 1))
          await go(() => waitForTopicPropagation(relaxed, 1))
          const read = {
            strict: await go((scope) => topicConfig(brokers, strict, 'min.insync.replicas', scope)),
            relaxed: await go((scope) =>
              topicConfig(brokers, relaxed, 'min.insync.replicas', scope)
            ),
          }
          await pollUntil(signal, FULL_ISR_MS, (scope) => isrIsFull(scope, admin, topics), {
            label: 'The full ISR wait',
          })

          await go(() => producer.connect())
          await go((scope) =>
            probe.capture(
              () => producer.send({ topic: strict, messages: [{ value: 'warm-all' }], acks: -1 }),
              { signal: scope }
            )
          )
          await go(() =>
            producer.send({ topic: relaxed, messages: [{ value: 'warm-two' }], acks: -1 })
          )

          const placed = (await go(() => admin.fetchTopicMetadata({ topics }))).topics.map(
            ({ name, partitions: [{ partitionId, leader, replicas }] }) => ({
              topic: name,
              partition: partitionId,
              leader,
              replicas,
            })
          )
          const leading = new Set(placed.map(({ leader }) => leader))
          const follower = Object.values(NODES)
            .map(({ nodeId }) => nodeId)
            .filter((nodeId) => !leading.has(nodeId))
            .at(-1)
          if (follower === undefined) throw new Error('No node is a follower of both partitions')
          const ordered = topics.map((topic) => placed.find((entry) => entry.topic === topic))
          const pinned = new TopologyPin({ partitions: ordered, follower })
          pinned.whole(await readTopology(pinned, signal))
          return { configs: read, pin: pinned }
        })
        const { follower } = pin
        const service = `kafka${follower}`
        const upBrokers = brokers.filter(
          (address) => Number(address.split(':')[1]) !== NODES[service].port
        )

        const outage = new FollowerOutage({ bouncer, admin, topics, timeline })
        owned = outage
        const during = await outage.run(service, async () => {
          const { established, shrunkAt } = await timeline.run('shrink', async (signal) => {
            let snapshot = null
            await pollUntil(
              signal,
              Infinity,
              async (scope) => {
                let read
                try {
                  read = await readTopology(pin, scope)
                } catch {
                  return false
                }
                if (!pin.establishing(read)) return false
                snapshot = read
                return true
              },
              { label: `The wait for ${service} to leave the ISR` }
            )
            return { established: snapshot, shrunkAt: performance.now() }
          })

          return timeline.run('measure', async (signal) => {
            const arms = []
            const arm = async (topic, value) => {
              const before = await readTopology(pin, signal)
              pin.holding(before)
              const result = await timed(signal, topic, value)
              const after = await readTopology(pin, signal)
              pin.holding(after)
              arms.push({ arm: value, before, after })
              return result
            }
            const rejected = await arm(strict, 'strict-0')
            const accepted = []
            for (let index = 0; index < RELAXED_SENDS; index++) {
              accepted.push(await arm(relaxed, `relaxed-${index}`))
            }
            const strictRead = (
              await bounded(signal, Infinity, () => readData(upBrokers, strict))
            ).map(({ value }) => String(value))
            const relaxedRead = (
              await bounded(signal, Infinity, () => readData(upBrokers, relaxed))
            ).map(({ value }) => String(value))
            const readable = { strict: strictRead, relaxed: relaxedRead }
            return { established, shrunkAt, rejected, accepted, readable, arms }
          })
        })

        const { markers, strictValues, relaxedValues, closing } = await timeline.run(
          'tail',
          async (signal) => {
            const sent = []
            for (let index = 0; index < RECOVERY_MARKERS; index++) {
              sent.push(await timed(signal, strict, `marker-${index}`))
            }
            const strictRead = (
              await bounded(signal, Infinity, () => readData(brokers, strict))
            ).map(({ value }) => String(value))
            const relaxedRead = (
              await bounded(signal, Infinity, () => readData(brokers, relaxed))
            ).map(({ value }) => String(value))
            return {
              markers: sent,
              strictValues: strictRead,
              relaxedValues: relaxedRead,
              closing: await probe.closing({
                topic: `e2e-p9b-not-enough-replicas-probe-${id}`,
                budgetMs: DELIVERY_TIMEOUT_MS,
                signal,
              }),
            }
          }
        )

        const { rejected, accepted } = during
        const rejectedType = rejected.error?.cause?.type ?? rejected.error?.type
        writeEvidence(EVIDENCE, {
          scenario: 'not-enough-replicas',
          case: 'follower-down',
          config: {
            requestTimeout: REQUEST_TIMEOUT_MS,
            enforceRequestTimeout: true,
            linger: 0,
            deliveryTimeout: DELIVERY_TIMEOUT_MS,
            idempotent: true,
            acks: -1,
            bufferMemory: BUFFER_MEMORY,
            minInsyncReplicas: configs,
          },
          budgets: {
            case: CASE_BUDGET_MS,
            joinReserve: JOIN_RESERVE_MS,
            phases: PHASE_BUDGET_MS,
            restore: RESTORE_STEP_MS,
            actual: timeline.timings(),
          },
          replay: REPLAY,
          wireVersions: wireVersionsOf(requests),
          wire: { frames: sockets.frames.trace(), replies: repliesOf(requests) },
          follower: { nodeId: follower, service },
          topology: {
            pinned: pin.partitions,
            established: during.established,
            arms: during.arms,
          },
          chronology: { ...outage.chronology, shrunkAt: during.shrunkAt },
          rejected: {
            ms: rejected.ms,
            code: rejectedType,
            afterAppend: rejectedType === AFTER_APPEND,
            error: describeError(rejected.error),
          },
          accepted: accepted.map(({ value, ms, error }) => ({
            value,
            ms,
            error: describeError(error),
          })),
          markers: markers.map(({ value, ms, error }) => ({
            value,
            ms,
            error: describeError(error),
          })),
          log: {
            duringStrict: during.readable.strict,
            duringRelaxed: during.readable.relaxed,
            strictValues,
            relaxedValues,
          },
          closing,
        })

        expect(configs).toEqual({ strict: '3', relaxed: '2' })
        expect(during.arms).toHaveLength(1 + RELAXED_SENDS)
        expect(rejected.error).toMatchObject({ name: 'KafkaJSDeliveryTimeout' })
        expect([PRE_APPEND, AFTER_APPEND]).toContain(rejectedType)
        expect(rejected.ms).toBeGreaterThanOrEqual(DELIVERY_TIMEOUT_MS)
        expect(rejected.ms).toBeLessThanOrEqual(DELIVERY_TIMEOUT_MS + DELIVERY_SLACK_MS)
        for (const { error, ms } of accepted) {
          expect(error).toBeNull()
          expect(ms).toBeLessThanOrEqual(DELIVERY_TIMEOUT_MS)
        }
        expect(during.readable.relaxed.filter((value) => value.startsWith('relaxed-'))).toEqual(
          Array.from({ length: RELAXED_SENDS }, (_, index) => `relaxed-${index}`)
        )
        expect(relaxedValues.filter((value) => value.startsWith('relaxed-'))).toEqual(
          Array.from({ length: RELAXED_SENDS }, (_, index) => `relaxed-${index}`)
        )
        expect(markers.map(({ error }) => error)).toEqual(Array(RECOVERY_MARKERS).fill(null))
        expect(strictValues.filter((value) => value.startsWith('marker-'))).toEqual(
          Array.from({ length: RECOVERY_MARKERS }, (_, index) => `marker-${index}`)
        )
        const strictOnes = strictValues.filter((value) => value === 'strict-0').length
        expect(strictOnes).toBeLessThanOrEqual(rejectedType === AFTER_APPEND ? 1 : 0)
      }

      await cleanup.run(
        async () => {
          try {
            await runCase()
          } finally {
            timeline.abort()
          }
        },
        { budgetMs: PHASE_BUDGET_MS.cleanup }
      )
    }),
    caseTimeout(CASE_BUDGET_MS)
  )
})
