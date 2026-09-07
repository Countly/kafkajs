const fs = require('fs')
const path = require('path')
const { newLogger } = require('testHelpers')
const { range, sticky, cooperativeSticky } = require('..')
const { MemberAssignment } = require('../../assignerProtocol')
const { stickyUserData } = require('../stickyAssigner/stickyUserData')
const Decoder = require('../../../protocol/decoder')

const FIXTURES = path.join(__dirname, '..', 'fixtures')
const ASSIGNERS = { range, sticky, 'cooperative-sticky': cooperativeSticky }

/**
 * Reads every committed golden vector.
 *
 * A missing directory or a stray file must not throw here: an exception thrown while the module
 * is being loaded reports as a broken suite rather than as the missing fixtures it really is, and
 * `test.each([])` reports as a silent pass. Both are covered by the count assertion below.
 *
 * @returns {Array<[string, object]>} `[name, vector]` pairs, in directory order
 */
const loadCases = () => {
  const cases = []

  if (!fs.existsSync(FIXTURES)) {
    return cases
  }

  for (const assignor of fs.readdirSync(FIXTURES)) {
    const dir = path.join(FIXTURES, assignor)

    if (!fs.statSync(dir).isDirectory()) {
      continue
    }

    for (const file of fs.readdirSync(dir)) {
      if (!file.endsWith('_java.json')) {
        continue
      }

      cases.push([`${assignor}/${file}`, JSON.parse(fs.readFileSync(path.join(dir, file), 'utf8'))])
    }
  }

  return cases
}

const CASES = loadCases()

/**
 * @param {Array<{topic: string, partition: number}>} owned the fixture's flat list
 * @returns {Array<{topic: string, partitions: number[]}>} the grouped shape the codec writes,
 *   sorted by topic and then by partition - the order Java's `serializeSubscription` imposes
 */
const toOwnedPartitions = (owned) => {
  const byTopic = new Map()

  for (const { topic, partition } of owned) {
    if (!byTopic.has(topic)) {
      byTopic.set(topic, [])
    }

    byTopic.get(topic).push(partition)
  }

  return [...byTopic.keys()]
    .sort()
    .map((topic) => ({ topic, partitions: byTopic.get(topic).sort((a, b) => a - b) }))
}

/**
 * Reads a recorded `userData` payload back into the state its assigner retains.
 *
 * @param {string} assignor the wire name of the assignor that wrote the payload
 * @param {Buffer} buffer the recorded payload
 * @returns {{ assignment: Array<{topic: string, partitions: number[]}>, generationId: number }}
 */
const decodeRetained = (assignor, buffer) => {
  if (assignor === 'cooperative-sticky') {
    // This payload carries a generation and nothing else.
    return { assignment: [], generationId: new Decoder(buffer).readInt32() }
  }

  return stickyUserData.decode(buffer)
}

/**
 * @param {Buffer} buffer an encoded `ConsumerProtocolAssignment`
 * @returns {{ version: number, assignment: object }|null} everything but the `userData`, with each
 *   topic's partitions in ascending order
 */
const partitionsOnly = (buffer) => {
  const decoded = MemberAssignment.decode(buffer)

  if (decoded === null) {
    return null
  }

  for (const partitions of Object.values(decoded.assignment)) {
    partitions.sort((a, b) => a - b)
  }

  return { version: decoded.version, assignment: decoded.assignment }
}

/**
 * @param {{[memberId: string]: {[topic: string]: number[]}}} byMember a whole group assignment
 * @returns {string[]} every assigned partition as `topic:partition`, sorted - the set the
 *   assignment covers, independent of who holds what
 */
const flatPartitions = (byMember) =>
  Object.values(byMember)
    .flatMap((byTopic) =>
      Object.entries(byTopic).flatMap(([topic, partitions]) =>
        partitions.map((partition) => `${topic}:${partition}`)
      )
    )
    .sort()

/**
 * @param {{[memberId: string]: {[topic: string]: number[]}}} byMember a whole group assignment
 * @returns {number[]} how many partitions each member holds, sorted - the shape of the balance,
 *   independent of which member has which count
 */
const assignmentSizes = (byMember) =>
  Object.values(byMember)
    .map((byTopic) => Object.values(byTopic).reduce((sum, ps) => sum + ps.length, 0))
    .sort((a, b) => a - b)

describe('Consumer > assigners > Java golden vectors', () => {
  it('found the committed fixtures', () => {
    // A directory rename, or a `test.each([])`, would otherwise turn the whole suite into zero
    // silent passes.
    expect(CASES.length).toBeGreaterThanOrEqual(32)
  })

  describe('each vector reproduces Apache Kafka exactly', () => {
    test.each(CASES)('%s', async (_name, vector) => {
      const cluster = {
        findTopicPartitionMetadata: (topic) =>
          vector.topics[topic].map(({ partition, leader }) => ({ partitionId: partition, leader })),
      }

      const create = () =>
        ASSIGNERS[vector.assignor]({ cluster, groupId: 'golden', logger: newLogger() })

      // One encode path, and it is production's: `assigner.protocol()` is the sole source of
      // subscription bytes. A member whose vector carries userData gets its OWN assigner instance,
      // seeded through `onAssignment` exactly as a real member is, so the payload comes out of
      // `subscriptionUserData()` rather than being injected.
      //
      // Both sortings below are Java's, not ours: `ConsumerProtocol.serializeSubscription` sorts
      // `Topics` lexicographically and `OwnedPartitions` by (topic, partition), so an unsorted
      // input would fail on ordering alone for every non-alphabetical vector.
      const subscriptionFor = (member) => {
        const own = create()

        if (member.userDataHex !== null && own.onAssignment) {
          const { assignment, generationId } = decodeRetained(
            vector.assignor,
            Buffer.from(member.userDataHex, 'hex')
          )
          own.onAssignment({ partitions: assignment, userData: null }, { generationId })
        }

        // The declared version decides which fields the positional format carries, and one vector
        // is recorded at a version that omits `GenerationId` so the fallback to `userData` is
        // exercised on both sides.
        own.version = vector.subscriptionVersion

        return own.protocol({
          topics: [...member.topics].sort(),
          ownedPartitions: toOwnedPartitions(member.ownedPartitions),
          generationId: member.generationId,
          rackId: member.rackId,
        }).metadata
      }

      const members = vector.members.map((member) => ({
        memberId: member.memberId,
        groupInstanceId: member.groupInstanceId,
        memberMetadata: subscriptionFor(member),
      }))

      // 1. The subscription bytes this client would have sent are the bytes Apache Kafka read.
      vector.members.forEach((member, index) => {
        expect(members[index].memberMetadata.toString('hex')).toEqual(member.subscriptionBytesHex)
      })

      // The topic set comes from the members' own subscriptions, not from the cluster: the Java
      // entry point never sees a topic nobody subscribed to. Passing every cluster topic would
      // make `unassigned_topic` pass by construction and ask `heterogeneous_general` and
      // `empty_subscription_member` a different question than Java was asked.
      const assignment = await create().assign({
        members,
        topics: [...new Set(vector.members.flatMap(({ topics }) => topics))],
      })

      // 2. The assignment itself, normalised on both sides. Java emits an Assignment for EVERY
      // member, empty ones included; `range` here omits a member that got nothing, so the empty
      // entries are filled in rather than letting that difference read as a mismatch.
      const decoded = {}
      for (const { memberId } of vector.members) {
        decoded[memberId] = {}
      }
      for (const { memberId, memberAssignment } of assignment) {
        decoded[memberId] = MemberAssignment.decode(memberAssignment).assignment
      }
      for (const byTopic of Object.values(decoded)) {
        for (const partitions of Object.values(byTopic)) {
          partitions.sort((a, b) => a - b)
        }
      }
      if (vector.expectedForkAssignment) {
        // A shape with more than one correct answer: both assignors produce a balanced assignment
        // over the same partitions, and they disagree only about which over-quota incumbent keeps
        // its extra partition, because they visit the members in a different order. That
        // divergence is accepted rather than emulated, so BOTH sides are pinned here - this
        // client's output against the recorded one, and the fact that it still differs from
        // Kafka's. A change on either side turns into a failing test instead of silent drift.
        expect(decoded).toEqual(vector.expectedForkAssignment)
        expect(decoded).not.toEqual(vector.expected)
        expect(flatPartitions(decoded)).toEqual(flatPartitions(vector.expected))
        expect(assignmentSizes(decoded)).toEqual(assignmentSizes(vector.expected))
        return
      }

      expect(decoded).toEqual(vector.expected)

      // 3. The encoded assignment bytes, compared as DECODED AND SORTED STRUCTURES rather than as
      // hex: Java's `Assignment.partitions()` is a List whose order is an assignor internal - the
      // order partitions happened to be placed in - so a hex comparison would pin an
      // implementation detail rather than the wire contract. The per-topic partition ORDER
      // therefore differs on the general path and on re-assignment; the contents never do.
      //
      // Only `version` and `assignment` are compared. `userData` differs by construction: Java's
      // `Assignment` carries `null`, which `serializeAssignment` writes as INT32 -1, while
      // `MemberAssignment.encode`'s destructuring default writes `Buffer.alloc(0)` - INT32 0.
      for (const { memberId, memberAssignment } of assignment) {
        const mine = partitionsOnly(memberAssignment)

        expect(mine.version).toEqual(vector.assignmentVersion)
        expect(mine).toEqual(
          partitionsOnly(Buffer.from(vector.expectedAssignmentBytesHex[memberId], 'hex'))
        )
      }
    })
  })

  it('orders static members by instance id rather than by member id', () => {
    // The guard for the one detail a generator can silently drop: without
    // `Subscription#setGroupInstanceId`, this vector would record the assignment member-id
    // ordering alone produces, and every assigner would reproduce it for the wrong reason.
    const vector = JSON.parse(
      fs.readFileSync(path.join(FIXTURES, 'range', 'group_instance_ids_mixed_java.json'), 'utf8')
    )
    const byMemberId = { 'm-1': [0, 1], 'm-2': [2], 'm-3': [3], 'm-4': [4], 'm-5': [5] }

    expect(vector.expected).not.toEqual(
      Object.fromEntries(Object.entries(byMemberId).map(([id, ps]) => [id, { 't-a': ps }]))
    )
    expect(vector.expected['m-4']).toEqual({ 't-a': [0, 1] })
  })
})
