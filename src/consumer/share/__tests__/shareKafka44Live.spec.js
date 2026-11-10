const shareFetch = require('../../../../testHelpers/wire/shareFetchV2')
const shareHeartbeat = require('../../../../testHelpers/wire/shareGroupHeartbeatV1')
const {
  shareRecordIdentity,
  waitForShareGroupsEmpty,
} = require('../../../../testHelpers/shareFixtures')
const { awaitTerminalSharePosition } = require('../../../../testHelpers/shareLiveDiagnostics')
const {
  METADATA_API_KEY,
  ShareKafka44Fixture,
  acknowledgeTypesByOffset,
  assignedPartitions,
  partitionLifecycle,
  readShareRequest,
} = require('../../../../testHelpers/shareKafka44')
const {
  NEVER,
  bounded,
  caseTimeout,
  delay,
  liveCase,
  liveRun,
  pollUntil,
} = require('../../../producer/__tests__/fixtures/bounded')

// Gated: it needs the Kafka 4.4 share cluster of the dedicated share check, whose setup finalized
// share.version at 2 before Jest started. It runs with SHARE_KAFKA44_E2E=1.
const enabled = process.env.SHARE_KAFKA44_E2E === '1' ? describe : describe.skip

const HOOK_MS = 10000
const POLL_MS = 45000
const CASE_MS = 150000
const GROUP_CONFIG = { 'share.auto.offset.reset': 'earliest' }
const ACCEPT = 1
const RELEASE = 2
const UNKNOWN_TOPIC_OR_PARTITION = 3
const UNKNOWN_TOPIC_ID = 100
const GROUP_ID_NOT_FOUND = 69
const MEMBER = 'member'

/**
 * @param {ShareKafka44Fixture} fixture the scenario's fixture
 * @param {string} label the client whose answers to read
 * @returns {object[]} its ShareGroupHeartbeat answers, decoded independently, oldest first
 */
const heartbeatAnswers = (fixture, label) =>
  fixture.tap
    .of(76, label)
    .filter(({ response }) => response)
    .map(({ response }) => shareHeartbeat.readResponse(response))

/**
 * @param {ShareKafka44Fixture} fixture the scenario's fixture
 * @param {string} label the client whose answers to read
 * @returns {Array<{ exchange: object, answer: object }>} its ShareFetch answers, decoded
 *   independently, oldest first
 */
const fetchAnswers = (fixture, label) =>
  fixture.tap
    .of(78, label)
    .filter(({ response }) => response)
    .map((exchange) => ({ exchange, answer: shareFetch.readResponse(exchange.response) }))

/**
 * @param {ShareKafka44Fixture} fixture the scenario's fixture
 * @param {string} label the client whose requests to read
 * @returns {object[]} its ShareFetch requests, decoded independently, oldest first
 */
const fetchRequests = (fixture, label) => fixture.tap.of(78, label).map(readShareRequest)

/**
 * @param {object} admin a connected admin client
 * @param {string} topic an existing topic
 * @returns {Promise<{ topicId: string, leaders: number[] }>} the topic's id and its partitions'
 *   leaders, as the broker reports them
 */
const topicIdentity = async (admin, topic) => {
  const { topics } = await admin.fetchTopicMetadata({ topics: [topic] })
  const [entry] = topics.filter(({ name }) => name === topic)
  return { topicId: entry.topicId, leaders: entry.partitions.map(({ leader }) => leader) }
}

/**
 * @param {number} count how many records
 * @param {string} nonce the scenario's nonce, in every key and value
 * @returns {Array<{ key: Buffer, value: Buffer }>} records with unique, checkable bytes
 */
const records = (count, nonce) =>
  Array.from({ length: count }, (_, index) => ({
    key: Buffer.from(`key-${index}-${nonce}`),
    value: Buffer.from(`value-${index}-${nonce}`),
  }))

enabled('Share Kafka 4.4 (live)', () => {
  beforeAll(
    () =>
      bounded(NEVER, HOOK_MS - 2000, () => {
        liveRun.assertClean()
      }),
    HOOK_MS
  )

  afterEach(() => {
    if (!liveRun.takeAdmission()) return
    liveRun.assertClean()
  })

  it(
    'joins fetches acknowledges and leaves on share.version two',
    liveCase(CASE_MS, async (scope) => {
      const f = new ShareKafka44Fixture('lifecycle')
      await f.run(async () => {
        await f.open(scope)
        const topic = await f.topic(scope)
        const group = await f.group(scope, topic, GROUP_CONFIG)
        const [{ key, value }] = records(1, f.nonce)
        const producer = await f.producer(scope)
        await bounded(scope, 15000, () => producer.send({ topic, messages: [{ key, value }] }))

        const handled = []
        const member = await f.member(scope, MEMBER, { groupId: group }, [topic], {
          eachMessage: async ({ topic: name, partition, message }) => {
            handled.push({
              identity: shareRecordIdentity({ ...message, topic: name, partition }),
              deliveryCount: message.deliveryCount,
            })
          },
        })
        await pollUntil(scope, POLL_MS, () => handled.length > 0, { label: 'The delivery' })
        expect(handled).toEqual([
          {
            identity: shareRecordIdentity({ topic, partition: 0, offset: '0', key, value }),
            deliveryCount: 1,
          },
        ])

        await pollUntil(
          scope,
          POLL_MS,
          () =>
            member.acknowledgements.some(
              ({ type, error, firstOffset, lastOffset }) =>
                type === 'accept' && error === null && firstOffset === '0' && lastOffset === '0'
            ),
          { label: 'The broker accepting the acknowledgement' }
        )
        await awaitTerminalSharePosition(
          f.admin,
          { groupId: group, topic, partitions: [{ partition: 0, floor: '0', end: '1' }] },
          { signal: scope, timeoutMs: 30000 }
        )
        const described = await bounded(scope, 10000, () =>
          f.admin.describeShareGroups([group], { apiTimeout: 6000 })
        )
        expect(described.groups[0]).toMatchObject({ groupId: group, errorCode: 0 })
        expect(described.groups[0].members).toHaveLength(1)
        expect(new Set(f.tap.of(78, MEMBER).map(({ apiVersion }) => apiVersion))).toEqual(
          new Set([2])
        )
        expect(new Set(f.tap.of(76, MEMBER).map(({ apiVersion }) => apiVersion))).toEqual(
          new Set([1])
        )

        await bounded(scope, 15000, () => member.consumer.disconnect({ timeout: 8000 }))
        await waitForShareGroupsEmpty(f.admin, [group], { signal: scope })
        const beats = f.tap.of(76, MEMBER)
        const last = beats[beats.length - 1]
        expect(shareHeartbeat.readRequest(last.request)).toMatchObject({
          groupId: group,
          memberEpoch: -1,
        })
        expect(shareHeartbeat.readResponse(last.response)).toMatchObject({
          errorCode: 0,
          memberEpoch: -1,
        })
        expect(member.crashes).toEqual([])
      })
    }),
    caseTimeout(CASE_MS)
  )

  it(
    'heartbeat code 69 ends the current membership as terminal',
    liveCase(CASE_MS, async (scope) => {
      const f = new ShareKafka44Fixture('code-69')
      await f.run(async () => {
        await f.open(scope)
        const topic = await f.topic(scope)
        const group = await f.group(scope, topic)

        const classic = f.client('classic').consumer({ groupId: group })
        f.stack.add('disconnect the classic member', () => classic.disconnect())
        await bounded(scope, 15000, () => classic.connect())
        await bounded(scope, 15000, () => classic.subscribe({ topic }))
        await bounded(scope, 15000, () => classic.run({ eachMessage: async () => {} }))
        await pollUntil(
          scope,
          POLL_MS,
          async () => {
            const { groups } = await f.admin.describeGroups([group])
            return groups[0].errorCode === 0 && groups[0].members.length === 1
          },
          { label: 'The classic group holding the id' }
        )

        const share = f.client(MEMBER).shareConsumer({ groupId: group, maxWaitTimeInMs: 250 })
        const crashes = []
        f.stack.add(
          'remove the share crash listener',
          share.on(share.events.CRASH, ({ payload }) => crashes.push(payload.error))
        )
        f.stack.add('disconnect the share member', () => share.disconnect({ timeout: 8000 }))
        await bounded(scope, 15000, () => share.connect())
        await bounded(scope, 15000, () => share.subscribe({ topics: [topic] }))
        let failure
        try {
          await bounded(scope, 30000, () => share.run({ eachMessage: async () => {} }))
        } catch (error) {
          failure = error
        }

        expect(failure).toBeDefined()
        expect(failure.retriable).toBe(false)
        expect(failure.code ?? failure.cause?.code).toBe(GROUP_ID_NOT_FOUND)
        const [join] = f.tap.of(76, MEMBER)
        expect(shareHeartbeat.readRequest(join.request)).toMatchObject({
          groupId: group,
          memberEpoch: 0,
        })
        expect(shareHeartbeat.readResponse(join.response)).toMatchObject({
          errorCode: GROUP_ID_NOT_FOUND,
          errorMessage: `Group ${group} is not a share group.`,
        })

        await delay(scope, 2000)
        const epochs = f.tap
          .of(76, MEMBER)
          .map(({ request }) => shareHeartbeat.readRequest(request).memberEpoch)
        expect(epochs.filter((epoch) => epoch === 0)).toHaveLength(1)
        expect(epochs.filter((epoch) => epoch > 0)).toEqual([])
        const { groups } = await bounded(scope, 10000, () => f.admin.describeGroups([group]))
        expect(groups[0].members).toHaveLength(1)
        expect(f.tap.of(78, MEMBER)).toEqual([])
      })
    }),
    caseTimeout(CASE_MS)
  )

  it(
    'deleted topic partition errors refresh metadata without crashing',
    liveCase(CASE_MS, async (scope) => {
      const f = new ShareKafka44Fixture('deleted-topic')
      await f.run(async () => {
        await f.open(scope)
        const doomed = await f.topic(scope)
        const kept = await f.topic(scope)
        const group = await f.group(scope, kept, GROUP_CONFIG)
        const producer = await f.producer(scope)
        const [first, second] = records(2, f.nonce)
        for (const [topic, { key, value }] of [
          [doomed, first],
          [kept, first],
        ]) {
          await bounded(scope, 15000, () => producer.send({ topic, messages: [{ key, value }] }))
        }

        const handled = []
        const member = await f.member(
          scope,
          MEMBER,
          { groupId: group, allowAutoTopicCreation: false },
          [doomed, kept],
          {
            eachMessage: async ({ topic, message }) => {
              handled.push({ topic, value: message.value })
            },
          }
        )
        await pollUntil(scope, POLL_MS, () => handled.length >= 2, { label: 'Both deliveries' })
        await pollUntil(
          scope,
          POLL_MS,
          () =>
            [doomed, kept].every((topic) =>
              member.acknowledgements.some(
                (entry) => entry.topic === topic && entry.type === 'accept' && entry.error === null
              )
            ),
          { label: 'Both acknowledgements being accepted' }
        )
        const { topicId: doomedId } = await topicIdentity(f.admin, doomed)

        const deletedAt = performance.now()
        await bounded(scope, 15000, () => f.admin.deleteTopics({ topics: [doomed], timeout: 8000 }))
        const failedFetch = () =>
          fetchAnswers(f, MEMBER).find(
            ({ exchange, answer }) =>
              exchange.answeredAt >= deletedAt &&
              answer.responses.some(
                ({ topicId, partitions }) =>
                  topicId === doomedId &&
                  partitions.some(({ errorCode }) =>
                    [UNKNOWN_TOPIC_OR_PARTITION, UNKNOWN_TOPIC_ID].includes(errorCode)
                  )
              )
          )
        await pollUntil(scope, POLL_MS, () => failedFetch() !== undefined, {
          label: 'A partition error for the deleted topic',
        })
        const { exchange: failed } = failedFetch()
        await pollUntil(
          scope,
          POLL_MS,
          () =>
            f.tap
              .of(METADATA_API_KEY, MEMBER)
              .some(({ requestedAt }) => requestedAt >= failed.answeredAt),
          { label: 'A metadata refresh after the error' }
        )

        const { key, value } = second
        await bounded(scope, 15000, () =>
          producer.send({ topic: kept, messages: [{ key, value }] })
        )
        await pollUntil(
          scope,
          POLL_MS,
          () => handled.some((entry) => entry.topic === kept && entry.value.equals(value)),
          { label: 'The other topic still delivering' }
        )
        await pollUntil(
          scope,
          POLL_MS,
          () =>
            member.acknowledgements.some(
              (entry) =>
                entry.topic === kept &&
                entry.firstOffset === '1' &&
                entry.type === 'accept' &&
                entry.error === null
            ),
          { label: 'The other topic acknowledging' }
        )
        await awaitTerminalSharePosition(
          f.admin,
          { groupId: group, topic: kept, partitions: [{ partition: 0, floor: '0', end: '2' }] },
          { signal: scope, timeoutMs: 30000 }
        )
        expect(member.crashes).toEqual([])
        expect(
          member.acknowledgements.filter(({ topic, error }) => topic === kept && error)
        ).toEqual([])
        await bounded(scope, 15000, () => member.consumer.disconnect({ timeout: 8000 }))
      })
    }),
    caseTimeout(CASE_MS)
  )

  it(
    'forgets acknowledgement-only partitions in the same request',
    liveCase(CASE_MS, async (scope) => {
      const f = new ShareKafka44Fixture('forget')
      await f.run(async () => {
        await f.open(scope)
        const partitions = [0, 1, 2, 3]
        const topic = await f.topic(scope, { assignment: partitions.map(() => [1, 2]) })
        const group = await f.group(scope, topic, GROUP_CONFIG)
        const { topicId, leaders } = await topicIdentity(f.admin, topic)
        expect(leaders.every(Number.isInteger)).toBe(true)
        expect(new Set(leaders).size).toBe(1)

        const producer = await f.producer(scope)
        const bodies = records(4, f.nonce)
        for (const partition of partitions) {
          await bounded(scope, 15000, () =>
            producer.send({
              topic,
              messages: bodies.map(({ key, value }) => ({ partition, key, value })),
            })
          )
        }

        let release
        const released = new Promise((resolve) => {
          release = resolve
        })
        const held = []
        const memberA = await f.member(
          scope,
          'A',
          { groupId: group, acknowledgementMode: 'explicit' },
          [topic],
          {
            eachMessage: async ({ partition, message, acknowledge }) => {
              held.push({ partition, offset: message.offset })
              await released
              acknowledge('accept')
            },
          }
        )
        await pollUntil(scope, POLL_MS, () => held.length > 0, { label: 'A holding a record' })
        await pollUntil(
          scope,
          POLL_MS,
          () => assignedPartitions(heartbeatAnswers(f, 'A'), topicId).length > 0,
          { label: 'The first assignment of A' }
        )
        expect(assignedPartitions(heartbeatAnswers(f, 'A'), topicId)[0]).toEqual(partitions)

        const memberB = await f.member(scope, 'B', { groupId: group }, [topic], {
          eachMessage: async () => {},
        })
        await pollUntil(
          scope,
          POLL_MS,
          () => assignedPartitions(heartbeatAnswers(f, 'A'), topicId).length > 1,
          { label: 'A losing a partition to B' }
        )
        const [initial, reduced] = assignedPartitions(heartbeatAnswers(f, 'A'), topicId)
        const lost = initial.filter((partition) => !reduced.includes(partition))
        expect(lost.length).toBeGreaterThan(0)
        expect(reduced.length).toBeGreaterThan(0)

        release()
        await pollUntil(
          scope,
          POLL_MS,
          () =>
            lost.every(
              (partition) =>
                partitionLifecycle(fetchRequests(f, 'A'), topicId, partition).firstAcknowledged >= 0
            ),
          { label: 'A acknowledging every lost partition' }
        )

        const requests = fetchRequests(f, 'A')
        for (const partition of lost) {
          const lifecycle = partitionLifecycle(requests, topicId, partition)
          expect(lifecycle.forgotten).toEqual([lifecycle.firstAcknowledged])
          expect(lifecycle.lastListed).toBe(lifecycle.firstAcknowledged)
          expect(requests[lifecycle.firstAcknowledged].shareSessionEpoch).toBeGreaterThan(0)
        }
        for (const partition of reduced) {
          expect(partitionLifecycle(requests, topicId, partition).forgotten).toEqual([])
        }
        expect(memberA.crashes).toEqual([])
        expect(memberB.crashes).toEqual([])
      })
    }),
    caseTimeout(CASE_MS)
  )

  it(
    'keeps both acquisitions of one partition in one fetch',
    liveCase(CASE_MS, async (scope) => {
      const f = new ShareKafka44Fixture('two-ranges')
      await f.run(async () => {
        await f.open(scope)
        const topic = await f.topic(scope)
        const group = await f.group(scope, topic, GROUP_CONFIG)
        const { topicId } = await topicIdentity(f.admin, topic)
        const sent = records(30, f.nonce)
        const producer = await f.producer(scope)
        await bounded(scope, 15000, () => producer.send({ topic, messages: sent }))
        const identityOf = (offset) =>
          shareRecordIdentity({ topic, partition: 0, offset: String(offset), ...sent[offset] })

        const deliveries = []
        const member = await f.member(
          scope,
          MEMBER,
          { groupId: group, acknowledgementMode: 'explicit' },
          [topic],
          {
            eachMessage: async ({ topic: name, partition, message, acknowledge }) => {
              const offset = Number(message.offset)
              deliveries.push({
                offset,
                deliveryCount: message.deliveryCount,
                identity: shareRecordIdentity({ ...message, topic: name, partition }),
              })
              if (message.deliveryCount > 1 || (offset >= 10 && offset < 20)) {
                acknowledge('accept')
              } else {
                acknowledge('release')
              }
            },
          }
        )
        await pollUntil(scope, POLL_MS, () => deliveries.length >= 50, {
          label: 'Both deliveries of the records',
        })
        await awaitTerminalSharePosition(
          f.admin,
          { groupId: group, topic, partitions: [{ partition: 0, floor: '0', end: '30' }] },
          { signal: scope, timeoutMs: 30000 }
        )

        const acquiredRanges = fetchAnswers(f, MEMBER).flatMap(({ answer }) =>
          answer.responses
            .filter((entry) => entry.topicId === topicId)
            .flatMap(({ partitions }) => partitions)
            .filter(({ partitionIndex }) => partitionIndex === 0)
            .map(({ acquiredRecords }) => acquiredRecords)
        )
        expect(acquiredRanges).toContainEqual([
          { firstOffset: '0', lastOffset: '9', deliveryCount: 2 },
          { firstOffset: '20', lastOffset: '29', deliveryCount: 2 },
        ])

        const numeric = (a, b) => a - b
        const firstPass = deliveries.filter(({ deliveryCount }) => deliveryCount === 1)
        const secondPass = deliveries.filter(({ deliveryCount }) => deliveryCount === 2)
        expect(deliveries).toHaveLength(50)
        expect(firstPass.map(({ offset }) => offset).sort(numeric)).toEqual(
          Array.from({ length: 30 }, (_, offset) => offset)
        )
        expect(secondPass.map(({ offset }) => offset).sort(numeric)).toEqual([
          ...Array.from({ length: 10 }, (_, offset) => offset),
          ...Array.from({ length: 10 }, (_, offset) => offset + 20),
        ])
        for (const { offset, identity } of deliveries) expect(identity).toBe(identityOf(offset))

        const requests = f.tap.exchanges
          .filter(({ label, apiKey }) => label === MEMBER && (apiKey === 78 || apiKey === 79))
          .map(readShareRequest)
        const types = acknowledgeTypesByOffset(requests, topicId, 0)
        for (let offset = 0; offset < 30; offset++) {
          expect(types.get(String(offset))).toEqual(
            offset >= 10 && offset < 20 ? [ACCEPT] : [RELEASE, ACCEPT]
          )
        }
        expect(member.acknowledgements.filter(({ error }) => error)).toEqual([])
        expect(member.crashes).toEqual([])
        await bounded(scope, 15000, () => member.consumer.disconnect({ timeout: 8000 }))
      })
    }),
    caseTimeout(CASE_MS)
  )
})
