const { secureRandom, writeEvidence } = require('testHelpers')
const { CleanupStack, describeError } = require('../../producer/__tests__/fixtures/liveProbe')
const {
  caseTimeout,
  delay,
  liveCase,
  pollUntil,
} = require('../../producer/__tests__/fixtures/bounded')
const { runWithEvidence } = require('./fixtures/evidenceCase')
const {
  POLL_MS,
  QUIET_MS,
  SessionRig,
  STEP_MS,
  capacityProblems,
  commitProblems,
  corpusProblems,
  sessionFacts,
  summarize,
} = require('./fixtures/sessionRig')

// Gated: it needs the eight-slot compose variant and keeps more than eight consumers alive. Run
// alone through the guarded live entry: `npm run test:e2e:fetch-sessions-capacity` is the
// developer row. Every consumer forces `fetchSessions: true`, whatever the default is.
const describeCapacity = process.env.FETCH_SESSIONS_CAPACITY_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'fetch-sessions-capacity'
const PREFIX = 'e2e-p9c-fetch-sessions-capacity'
const CASE_BUDGET_MS = 180000
const CLEANUP_BUDGET_MS = 40000
const PARTITIONS = 20
const PARTITION_LIST = Array.from({ length: PARTITIONS }, (_, partition) => partition)
const RECORDS = 5
/** Each broker keeps eight sessions: more consumers than that must meet its limit. */
const SLOTS = 8
const CONSUMERS = 12
const PRESSURE_ROUNDS = 6
const COHORT = 3

/**
 * Runs one case body with its own rig and cleanup, and writes the case's evidence row between the
 * two however the body ended, so the row exists before any cleanup runs.
 * @param {string} name the case
 * @param {AbortSignal} scope the case scope
 * @param {(rig: SessionRig) => Promise<void>} body
 * @returns {Promise<void>}
 * @throws {Error} the body's failure, a failed evidence write, or the failed cleanups
 */
const runCase = (name, scope, body) => {
  const cleanup = new CleanupStack()
  const rig = new SessionRig(scope, cleanup, secureRandom(), PREFIX)
  return runWithEvidence(
    cleanup,
    async (failure) => {
      try {
        writeEvidence(EVIDENCE, {
          scenario: EVIDENCE,
          case: name,
          caseError: failure ? describeError(failure) : null,
          ...(await rig.evidence()),
        })
        return null
      } catch (error) {
        return error
      }
    },
    () => body(rig),
    CLEANUP_BUDGET_MS
  )
}

describeCapacity('Consumer > Fetch session capacity (live)', () => {
  test(
    'meets the session cache limit with independent full-corpus consumers',
    liveCase(CASE_BUDGET_MS, (scope) =>
      runCase('independent-consumers', scope, async (rig) => {
        await rig.open()
        const topic = `${PREFIX}-topic-${rig.id}`
        await rig.createTopic({ topic, partitions: PARTITIONS })
        const corpus = await rig.produce('first', topic, PARTITION_LIST, RECORDS)

        const members = []
        for (let index = 0; index < CONSUMERS; index++) {
          const manual = index % 2 === 1
          members.push(
            await rig.startMember({
              name: `consumer-${index}`,
              mode: manual ? 'manual assignment' : 'classic eager dynamic',
              topic,
              partitions: PARTITION_LIST,
              groupId: `${PREFIX}-group-${index}-${rig.id}`,
            })
          )
        }
        rig.facts.cohort = {
          consumers: CONSUMERS,
          slots: SLOTS,
          groups: members.filter(({ group }) => group).length,
          manual: members.filter(({ group }) => !group).length,
        }
        const expected = [...corpus]
        for (const member of members) await rig.delivered(member, corpus)

        for (let round = 0; round < PRESSURE_ROUNDS; round++) {
          const records = await rig.produce(`round-${round}`, topic, PARTITION_LIST, 1)
          expected.push(...records)
          for (const member of members) await rig.delivered(member, records)
          await delay(scope, QUIET_MS)
        }
        await rig.quiet()

        const observed = await Promise.all(members.map((member) => member.wire.observations()))
        const facts = sessionFacts(observed.flat())
        rig.facts.sessions = summarize(facts)
        rig.facts.perConsumer = observed.map((own, index) => ({
          name: members[index].name,
          ...summarize(sessionFacts(own)),
        }))
        const problems = {
          capacity: capacityProblems(observed, SLOTS),
          corpus: members.flatMap((member) =>
            corpusProblems(expected, member.handled).map((problem) => `${member.name}: ${problem}`)
          ),
          commits: (
            await Promise.all(
              members
                .filter(({ group }) => group)
                .map(async (member, index) =>
                  commitProblems(
                    member.handled,
                    await rig.committed(`${PREFIX}-group-${index * 2}-${rig.id}`, topic)
                  )
                )
            )
          ).flat(),
          crashes: members.flatMap((member) => member.crashes.map((error) => error.message)),
        }
        rig.facts.problems = problems
        expect(problems).toEqual({ capacity: [], corpus: [], commits: [], crashes: [] })

        const [{ handled }] = members
        for (const half of [0, 1]) {
          expect(
            corpusProblems(
              expected,
              handled.filter((_, at) => at % 2 === half)
            )
          ).not.toEqual([])
        }
        expect(capacityProblems(observed.slice(0, SLOTS), SLOTS)).not.toEqual([])
      })
    ),
    caseTimeout(CASE_BUDGET_MS)
  )

  test(
    'rejects a same-group cohort as independent full-corpus consumers',
    liveCase(CASE_BUDGET_MS, (scope) =>
      runCase('same-group-cohort', scope, async (rig) => {
        await rig.open()
        const topic = `${PREFIX}-cohort-${rig.id}`
        const groupId = `${PREFIX}-cohort-group-${rig.id}`
        await rig.createTopic({ topic, partitions: PARTITIONS })
        const corpus = await rig.produce('first', topic, PARTITION_LIST, RECORDS)

        const members = []
        for (let index = 0; index < COHORT; index++) {
          members.push(
            await rig.startMember({
              name: `cohort-${index}`,
              mode: 'classic eager dynamic',
              topic,
              partitions: PARTITION_LIST,
              groupId,
              awaitOwnership: false,
            })
          )
        }
        const owned = () => members.flatMap((member) => member.owned(topic)).sort((a, b) => a - b)
        await pollUntil(scope, STEP_MS, () => owned().join() === PARTITION_LIST.join(), {
          delayMs: POLL_MS,
          label: 'The exclusive split of the partitions',
        })
        const everything = () =>
          new Set(members.flatMap(({ handled }) => handled.map((r) => r.value)))
        await pollUntil(scope, STEP_MS, () => corpus.every((r) => everything().has(r.value)), {
          delayMs: POLL_MS,
          label: 'The group deliveries',
        })
        await rig.quiet()

        const shares = members.map((member) => member.owned(topic).length)
        rig.facts.cohort = { groupId, shares }
        const rejected = members.map((member) => corpusProblems(corpus, member.handled).length > 0)
        rig.facts.rejected = rejected
        expect(shares.every((share) => share < PARTITIONS)).toBe(true)
        expect(rejected).toEqual(members.map(() => true))
        const union = members.flatMap(({ handled }) => handled)
        expect(corpusProblems(corpus, union)).toEqual([])
        expect(
          capacityProblems(
            await Promise.all(members.map((member) => member.wire.observations())),
            SLOTS
          )
        ).not.toEqual([])
      })
    ),
    caseTimeout(CASE_BUDGET_MS)
  )
})
