const { Kafka, logLevel } = require('../../../index')
const Broker = require('../../broker')
const { plainTextBrokers, writeEvidence, evidenceLines, wireVersionsOf } = require('testHelpers')
const { problemsOf } = require('../../../scripts/evidence/verify')

// Gated: it needs a cluster that has only just started, so it is run alone through
// `npm run test:e2e:create-topics-cold`, which starts a fresh cluster for the run.
const describeCreateTopicsCold =
  process.env.CREATE_TOPICS_COLD_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'create-topics-cold'
// The lines earlier runs left behind, so the last case judges only this run's.
const linesBefore = evidenceLines(EVIDENCE).length
const CALLS = 30
// Concurrent callers load the controller the way a busy test run does, which is what makes the
// propagation lag likely; one caller on an idle cluster rarely sees it.
const CREATORS = 4
const PARTITIONS = 12
const CALL_TIMEOUT_MS = 30000
const SCENARIO_BUDGET_MS = 120000
const HOOK_BUDGET_MS = 30000

/**
 * Live proof that `createTopics({ waitForLeaders: true })` does not fail while a new topic is still
 * propagating on a freshly started KRaft cluster, with several callers at once: the broker that
 * took the CreateTopics may answer the leader wait with UNKNOWN_TOPIC_OR_PARTITION for a few
 * seconds.
 *
 * The wait's rejections are counted by wrapping `Broker#metadata`, so the evidence says whether
 * the race was hit at all. A run that saw no such answer proves nothing about the fix and its
 * evidence line says `exercised: false`.
 *
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/admin/Admin.java
 */
describeCreateTopicsCold('Admin > createTopics on a cold cluster (live)', () => {
  const runId = Date.now().toString(36)
  let admin
  let metadata
  /** @type {string[][]} the topic names of every Metadata request answered with code 3 */
  let unknownTopicAnswers
  /** @type {{ apiName: string, apiVersion: number }[]} every request the admin sent */
  let requests

  beforeAll(async () => {
    unknownTopicAnswers = []
    requests = []
    const original = Broker.prototype.metadata
    metadata = jest.spyOn(Broker.prototype, 'metadata').mockImplementation(async function (topics) {
      try {
        return await original.call(this, topics)
      } catch (e) {
        if (e.type === 'UNKNOWN_TOPIC_OR_PARTITION') {
          unknownTopicAnswers.push(topics)
        }
        throw e
      }
    })

    admin = new Kafka({
      clientId: 'create-topics-cold',
      brokers: plainTextBrokers(),
      logLevel: logLevel.NOTHING,
    }).admin()
    admin.on(admin.events.REQUEST, ({ payload }) =>
      requests.push({ apiName: payload.apiName, apiVersion: payload.apiVersion })
    )
    await admin.connect()
  }, HOOK_BUDGET_MS)

  afterAll(async () => {
    metadata.mockRestore()
    await admin.disconnect()
  }, HOOK_BUDGET_MS)

  test(
    `${CALLS} createTopics calls with waitForLeaders, ${CREATORS} at a time, all succeed`,
    async () => {
      const outcomes = []
      let nextCall = 0
      const creator = async () => {
        while (nextCall < CALLS) {
          const call = nextCall
          nextCall += 1
          const topic = `e2e-p8-create-topics-cold-${runId}-${call}`
          const startedAt = Date.now()
          let error = null
          try {
            await admin.createTopics({
              waitForLeaders: true,
              timeout: CALL_TIMEOUT_MS,
              topics: [{ topic, numPartitions: PARTITIONS, replicationFactor: 3 }],
            })
          } catch (e) {
            error = { name: e.name, type: e.type, message: e.message }
          }

          outcomes.push({
            call,
            topic,
            ms: Date.now() - startedAt,
            code3Answers: unknownTopicAnswers.filter(([name]) => name === topic).length,
            error,
          })
        }
      }
      await Promise.all(Array.from({ length: CREATORS }, creator))
      outcomes.sort((a, b) => a.call - b.call)

      const code3Answers = outcomes.reduce((total, outcome) => total + outcome.code3Answers, 0)
      const failures = outcomes.filter(({ error }) => error !== null)
      writeEvidence(EVIDENCE, {
        scenario: 'cold-create',
        calls: CALLS,
        creators: CREATORS,
        failures: failures.length,
        code3Answers,
        exercised: code3Answers > 0,
        outcomes,
        wireVersions: wireVersionsOf(requests),
      })

      expect(failures).toEqual([])
      await admin.deleteTopics({ topics: outcomes.map(({ topic }) => topic) }).catch(() => {})
    },
    SCENARIO_BUDGET_MS
  )

  test('its evidence line carries the stamps a verifier checks', () => {
    const lines = evidenceLines(EVIDENCE).slice(linesBefore)
    const verified = {
      tree: process.env.KAFKAJS_TREE || 'unfrozen',
      node: process.version,
      brokerVersion: process.env.KAFKA_VERSION,
    }

    expect(lines).toHaveLength(1)
    expect(problemsOf(lines[0], verified)).toEqual([])
    expect(lines[0].wireVersions).toMatchObject({ CreateTopics: 7 })
    expect(Object.keys(lines[0].wireVersions)).toEqual(expect.arrayContaining(['Metadata']))
  })
})
