const fs = require('fs')
const path = require('path')
const { newLogger } = require('testHelpers')
const { range, roundRobin, sticky, cooperativeSticky } = require('..')
const { MemberAssignment } = require('../../assignerProtocol')
const { stickyUserData } = require('../stickyAssigner/stickyUserData')
const Decoder = require('../../../protocol/decoder')

const FIXTURES = path.join(__dirname, '..', 'fixtures')
const ASSIGNERS = { range, roundrobin: roundRobin, sticky, 'cooperative-sticky': cooperativeSticky }

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
    expect(CASES.length).toBeGreaterThanOrEqual(33)
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

const { MemberMetadata } = require('../../assignerProtocol')
const Encoder = require('../../../protocol/encoder')
const fetchPair = require('../../../../testHelpers/wire/fetchV18')
const { decodeFetchRequest, fetchResponse } = require('../../__tests__/fixtures/fetchHarness')
const { applySyncAnswer } = require('../../__tests__/fixtures/emptySyncApply')
const REQUEST_CASES = ['sessionFull', 'sessionIncremental', 'sessionForgotten', 'sessionClose']
const RESPONSE_CASES = [
  'sessionFull',
  'sessionSparse',
  'sessionError70',
  'sessionError71',
  'sessionError106',
]
const SESSION_CASES = Array.from({ length: 7 }, (_, index) => index + 12).flatMap((version) =>
  [
    ['Request', REQUEST_CASES],
    ['Response', RESPONSE_CASES],
  ].flatMap(([direction, names]) => names.map((name) => [version, direction, name]))
)
const SESSION_DIR = path.join(__dirname, '../../../protocol/requests/fetch/fixtures/java')
const RACK_CASES = [
  'discriminating',
  'copartitioned',
  'missing',
  'nonintersecting',
  'all_replicas',
  'balance',
]

/** @param {number} version wire version @param {string} direction message kind @param {string} name fixed case @returns {object} Java evidence */
const sessionVector = (version, direction, name) =>
  JSON.parse(
    fs.readFileSync(path.join(SESSION_DIR, `fetch${direction}-v${version}-${name}.json`), 'utf8')
  )

/** @param {number} version wire version @param {Buffer} body Java body @returns {Buffer} a complete request frame for the inherited reader */
const fetchFrame = (version, body) =>
  Buffer.concat([
    new Encoder()
      .writeInt32(0)
      .writeInt16(1)
      .writeInt16(version)
      .writeInt32(0)
      .writeString('')
      .writeTaggedFields().buffer,
    body,
  ])

describe('Consumer > independent session and assignment evidence', () => {
  test('pins every new vector to generator inputs and the client jar', () => {
    const crypto = require('crypto')
    const root = path.join(__dirname, '../../../..')
    const manifest = require('../fixtures/consumerEvidence.json')
    const digest = (file) =>
      crypto
        .createHash('sha256')
        .update(fs.readFileSync(path.join(root, file)))
        .digest('hex')
    expect(manifest.kafkaTag).toBe('4.3.1')
    expect(manifest.javaClientsJar).toBe('4.3.1')
    expect(manifest.javaClientsJarSha256).toBe(
      '52501b7b47510c66f898871adaf6d2968ab7246561d44ced43643a8a587f0b36'
    )
    const expected = SESSION_CASES.map(
      ([version, direction, name]) =>
        `src/protocol/requests/fetch/fixtures/java/fetch${direction}-v${version}-${name}.json`
    ).concat(
      RACK_CASES.map((name) => `src/consumer/assigners/fixtures/range/rack_${name}_java_rack.json`),
      ['subscription_v0', 'subscription_v2', 'empty_range'].map(
        (name) => `src/consumer/assigners/fixtures/range/${name}_java.json`
      ),
      Object.keys(SCARCE).map(
        (assignor) =>
          `src/consumer/assigners/fixtures/${assignor}/more_consumers_than_partitions_java.json`
      )
    )
    expect(Object.keys(manifest.vectors).sort()).toEqual(expected.sort())
    expect(manifest.titles).toEqual(EVIDENCE_TITLES)
    for (const [file, sha] of Object.entries(manifest.vectors)) expect(digest(file)).toBe(sha)
    for (const [file, sha] of Object.entries(manifest.generators)) expect(digest(file)).toBe(sha)
    for (const [name, sha] of Object.entries(manifest.messageSha256))
      expect(digest(`scripts/compat/messages/4.3.1/${name}.json`)).toBe(sha)
    expect(Object.keys(manifest.nestedMessageSha256).sort()).toEqual([
      'ConsumerProtocolAssignment',
      'ConsumerProtocolSubscription',
    ])
  })

  test('contains every session direction and version', () => {
    const files = fs.existsSync(SESSION_DIR)
      ? fs.readdirSync(SESSION_DIR).filter((file) => /-session/.test(file))
      : []
    expect(files.sort()).toEqual(
      SESSION_CASES.map(
        ([version, direction, name]) => `fetch${direction}-v${version}-${name}.json`
      ).sort()
    )
  })

  test.each(SESSION_CASES)(
    'Fetch v%i %s %s matches independent fields and bytes',
    async (version, direction, name) => {
      const vector = sessionVector(version, direction, name)
      expect(vector.message).toBe(`Fetch${direction}`)
      expect(vector.version).toBe(version)
      expect(vector.case).toBe(name)
      const bytes = Buffer.from(vector.hex, 'hex')
      if (direction === 'Request') {
        const full = name === 'sessionFull'
        const present = full || name === 'sessionIncremental'
        const topic = version === 12 ? 'test-topic' : null
        const topicId = version >= 13 ? '01020304-0506-0708-090a-0b0c0d0e0f10' : null
        const fields = {
          replicaId: version <= 14 ? -1 : null,
          maxWaitMs: 5000,
          minBytes: 1,
          maxBytes: 10485760,
          isolationLevel: 1,
          sessionId: full ? 0 : 37,
          sessionEpoch: full
            ? 0
            : name === 'sessionForgotten'
              ? 2
              : name === 'sessionClose'
                ? -1
                : 1,
          rackId: 'rack-a',
          topics: present
            ? [
                {
                  ...(version === 12 ? { topic } : { topicId }),
                  partitions: [
                    {
                      partition: 0,
                      currentLeaderEpoch: 5,
                      fetchOffset: full ? '42' : '43',
                      lastFetchedEpoch: -1,
                      logStartOffset: '-1',
                      maxBytes: 32768,
                    },
                  ],
                },
              ]
            : [],
          forgottenTopics:
            name === 'sessionForgotten'
              ? [{ ...(version === 12 ? { topic } : { topicId }), partitions: [0] }]
              : [],
        }
        expect(vector.fields).toEqual(fields)
        const read = {
          apiVersion: version,
          ...fields,
          topics: fields.topics.map((item) => ({ topic, topicId, partitions: item.partitions })),
          forgottenTopics: fields.forgottenTopics.map((item) => ({
            topic,
            topicId,
            partitions: item.partitions,
          })),
        }
        expect(decodeFetchRequest([fetchFrame(version, bytes)])).toEqual(read)
        if (version === 18) expect(fetchPair.readRequest(bytes)).toEqual(read)
        const encode = require(`../../../protocol/requests/fetch/v${version}/request`)
        const request = encode({
          replicaId: -1,
          maxWaitTime: 5000,
          minBytes: 1,
          maxBytes: 10485760,
          isolationLevel: 1,
          sessionId: fields.sessionId,
          sessionEpoch: fields.sessionEpoch,
          rackId: 'rack-a',
          topicIds: new Map([['test-topic', '01020304-0506-0708-090a-0b0c0d0e0f10']]),
          topics: present
            ? [
                {
                  topic: 'test-topic',
                  partitions: [
                    {
                      partition: 0,
                      currentLeaderEpoch: 5,
                      fetchOffset: full ? '42' : '43',
                      logStartOffset: '-1',
                      maxBytes: 32768,
                    },
                  ],
                },
              ]
            : [],
          forgottenTopics:
            name === 'sessionForgotten' ? [{ topic: 'test-topic', partitions: [0] }] : [],
        })
        expect((await request.encode()).buffer).toEqual(bytes)
      } else {
        const full = name === 'sessionFull'
        const errorCode = name.startsWith('sessionError') ? Number(name.slice(12)) : 0
        const fields = {
          errorCode,
          sessionId: errorCode === 0 ? 37 : 0,
          includeTopic: full,
          partition: 0,
          highWatermark: '100',
          lastStableOffset: '100',
          logStartOffset: '0',
          preferredReadReplica: -1,
        }
        expect(vector.fields).toEqual(fields)
        const partition = {
          partition: 0,
          errorCode: 0,
          highWatermark: '100',
          lastStableOffset: '100',
          lastStartOffset: '0',
          preferredReadReplica: -1,
          abortedTransactions: [],
          messages: [],
          divergingEpoch: null,
          currentLeader: null,
          snapshotId: null,
        }
        const expected = {
          throttleTime: 0,
          clientSideThrottleTime: 0,
          errorCode,
          sessionId: fields.sessionId,
          responses: full
            ? [
                {
                  ...(version === 12
                    ? { topicName: 'test-topic' }
                    : { topicId: '01020304-0506-0708-090a-0b0c0d0e0f10' }),
                  partitions: [partition],
                },
              ]
            : [],
          ...(version >= 16 && { nodeEndpoints: [] }),
        }
        const response = require(`../../../protocol/requests/fetch/v${version}/response`)
        expect(await response.decode(bytes)).toMatchObject(expected)
        expect(
          fetchResponse([{ partition: 0, errorCode: 0 }], {
            apiVersion: version,
            topLevel: errorCode,
            sessionId: fields.sessionId,
            includeTopic: full,
          })
        ).toEqual(bytes)
        if (version === 18)
          expect(
            fetchPair.buildResponse({
              partitions: [{ partition: 0, errorCode: 0 }],
              nodeEndpoints: undefined,
              topLevel: errorCode,
              sessionId: fields.sessionId,
              includeTopic: full,
            })
          ).toEqual(bytes)
      }
    }
  )

  test.each(['subscription_v0', 'subscription_v2', 'empty_range'])(
    '%s has independently encoded and decoded assignments',
    (name) => {
      const vector = JSON.parse(
        fs.readFileSync(path.join(FIXTURES, 'range', `${name}_java.json`), 'utf8')
      )
      expect(vector.subscriptionVersion).toBe(
        name === 'subscription_v0' ? 0 : name === 'subscription_v2' ? 2 : 3
      )
      expect(vector.members.map(({ memberId }) => memberId)).toEqual(['m-1', 'm-2'])
      for (const member of vector.members) {
        const bytes = Buffer.from(member.subscriptionBytesHex, 'hex')
        const expected = {
          version: vector.subscriptionVersion,
          topics: ['t-a'],
          userData: null,
          ...(vector.subscriptionVersion >= 1 && { ownedPartitions: [] }),
          ...(vector.subscriptionVersion >= 2 && { generationId: -1 }),
          ...(vector.subscriptionVersion >= 3 && { rackId: null }),
        }
        expect(MemberMetadata.decode(bytes)).toEqual(expected)
        expect(MemberMetadata.encode(expected)).toEqual(bytes)
        const assignment =
          name === 'empty_range' ? {} : { 't-a': member.memberId === 'm-1' ? [0, 1] : [2, 3] }
        expect(vector.expected[member.memberId]).toEqual(assignment)
        const encoded = Buffer.from(vector.expectedAssignmentBytesHex[member.memberId], 'hex')
        expect(MemberAssignment.decode(encoded)).toEqual({ version: 3, assignment, userData: null })
        expect(MemberAssignment.encode({ version: 3, assignment, userData: null })).toEqual(encoded)
      }
    }
  )

  test.each(RACK_CASES)(
    'rack %s records the Java quota and locality result without claiming runtime support',
    (shape) => {
      const vector = JSON.parse(
        fs.readFileSync(path.join(FIXTURES, 'range', `rack_${shape}_java_rack.json`), 'utf8')
      )
      const rackMatch = ['discriminating', 'copartitioned'].includes(shape)
      const first = rackMatch ? [2, 3] : shape === 'balance' ? [2, 3] : [0, 1]
      const second = rackMatch ? [0, 1] : shape === 'balance' ? [0, 1] : [2, 3]
      const names = shape === 'copartitioned' ? ['t-a', 't-b'] : ['t-a']
      const expected = {
        'm-1': Object.fromEntries(names.map((name) => [name, first])),
        'm-2': Object.fromEntries(names.map((name) => [name, second])),
      }
      expect(vector.expected).toEqual(expected)
      expect(vector.nodes.map(({ rack }) => rack)).toEqual(['rack-a', 'rack-b', 'rack-c'])
      expect(vector.topics['t-a'].map(({ replicas }) => replicas)).toEqual(
        shape === 'all_replicas'
          ? [
              [0, 1, 2],
              [0, 1, 2],
              [0, 1, 2],
              [0, 1, 2],
            ]
          : shape === 'balance'
            ? [
                [1, 2],
                [1, 2],
                [1, 2],
                [0, 2],
              ]
            : [
                [1, 2],
                [1, 2],
                [0, 2],
                [0, 2],
              ]
      )
      for (const member of vector.members) {
        const metadata = {
          version: 3,
          topics: names,
          userData: null,
          ownedPartitions: [],
          generationId: -1,
          rackId:
            shape === 'missing'
              ? null
              : shape === 'nonintersecting'
                ? member.memberId === 'm-1'
                  ? 'rack-x'
                  : 'rack-y'
                : member.memberId === 'm-1'
                  ? 'rack-a'
                  : 'rack-b',
        }
        const bytes = Buffer.from(member.subscriptionBytesHex, 'hex')
        expect(MemberMetadata.decode(bytes)).toEqual(metadata)
        expect(MemberMetadata.encode(metadata)).toEqual(bytes)
        const assignment = { version: 3, assignment: expected[member.memberId], userData: null }
        const encoded = Buffer.from(vector.expectedAssignmentBytesHex[member.memberId], 'hex')
        expect(MemberAssignment.decode(encoded)).toEqual(assignment)
        expect(MemberAssignment.encode(assignment)).toEqual(encoded)
      }
    }
  )
})

/**
 * What each scarce-partition assignor of the independent corpus must answer, written from the
 * Java generator's inputs and the version each case pins.
 * @type {Record<string, {name: string, version: number, userData: Buffer, factory: Function}>}
 */
const SCARCE = {
  roundrobin: {
    name: 'RoundRobinAssigner',
    version: 0,
    userData: Buffer.alloc(0),
    factory: roundRobin,
  },
  'cooperative-sticky': {
    name: 'cooperative-sticky',
    version: 3,
    userData: Buffer.from('ffffffff', 'hex'),
    factory: cooperativeSticky,
  },
}

/** @param {string} assignor wire key of the scarce case @returns {string} its test title */
const scarceTitle = (assignor) =>
  `${assignor} scarce partitions reproduce independent subscription and assignment bytes`
const VERSIONS_TITLE =
  'subscription versions zero and two decode the independent fields and re-encode the original bytes'
const EMPTY_TITLE = 'a valid empty assignment applies without a corruption error'
/** The titles the evidence manifest records, in its order. */
const EVIDENCE_TITLES = [
  scarceTitle('roundrobin'),
  scarceTitle('cooperative-sticky'),
  EMPTY_TITLE,
  VERSIONS_TITLE,
]

/** Which of the four members holds what, from the Java run: two partitions, four members. */
const SCARCE_ASSIGNMENT = { 'm-1': { 't-a': [0] }, 'm-2': { 't-a': [1] }, 'm-3': {}, 'm-4': {} }

/**
 * @param {string} assignor wire key of the scarce case
 * @returns {object} the committed Java vector of the case
 */
const scarceVector = (assignor) =>
  JSON.parse(
    fs.readFileSync(
      path.join(FIXTURES, assignor, 'more_consumers_than_partitions_java.json'),
      'utf8'
    )
  )

/**
 * Runs this client's assignor over a scarce vector's own subscriptions.
 * @param {string} assignor wire key of the scarce case
 * @returns {Promise<{[memberId: string]: Buffer}>} the assignment bytes by member, only for the
 *   members the assignor returned
 */
const nativeScarce = async (assignor) => {
  const vector = scarceVector(assignor)
  const cluster = {
    findTopicPartitionMetadata: (topic) =>
      vector.topics[topic].map(({ partition, leader }) => ({ partitionId: partition, leader })),
  }
  const result = await SCARCE[assignor]
    .factory({ cluster, groupId: 'golden', logger: newLogger() })
    .assign({
      members: vector.members.map(({ memberId, subscriptionBytesHex }) => ({
        memberId,
        groupInstanceId: null,
        memberMetadata: Buffer.from(subscriptionBytesHex, 'hex'),
      })),
      topics: ['t-a'],
    })
  return Object.fromEntries(
    result.map(({ memberId, memberAssignment }) => [memberId, memberAssignment])
  )
}

describe.each(Object.keys(SCARCE))('Consumer > scarce partitions, %s', (assignor) => {
  const { name, version, userData } = SCARCE[assignor]

  test(scarceTitle(assignor), async () => {
    const vector = scarceVector(assignor)
    expect(vector.assignor).toBe(assignor)
    expect(vector.members.map(({ memberId }) => memberId)).toEqual(['m-1', 'm-2', 'm-3', 'm-4'])
    expect(vector.topics['t-a']).toHaveLength(2)

    const subscription = {
      version,
      topics: ['t-a'],
      userData,
      ...(version >= 1 && { ownedPartitions: [] }),
      ...(version >= 2 && { generationId: -1 }),
      ...(version >= 3 && { rackId: null }),
    }
    for (const member of vector.members) {
      const bytes = Buffer.from(member.subscriptionBytesHex, 'hex')
      expect(MemberMetadata.decode(bytes)).toEqual(subscription)
      expect(MemberMetadata.encode(subscription)).toEqual(bytes)
      const own = SCARCE[assignor].factory({ cluster: {}, groupId: 'golden', logger: newLogger() })
      expect(own.name).toBe(name)
      expect(
        own
          .protocol({ topics: ['t-a'], ownedPartitions: [], generationId: -1, rackId: null })
          .metadata.toString('hex')
      ).toBe(member.subscriptionBytesHex)

      const assignment = SCARCE_ASSIGNMENT[member.memberId]
      expect(vector.expected[member.memberId]).toEqual(assignment)
      const encoded = Buffer.from(vector.expectedAssignmentBytesHex[member.memberId], 'hex')
      expect(vector.assignmentVersion).toBe(version)
      expect(MemberAssignment.decode(encoded)).toEqual({ version, assignment, userData: null })
      expect(MemberAssignment.encode({ version, assignment, userData: null })).toEqual(encoded)
    }

    const native = await nativeScarce(assignor)
    const held = Object.entries(native).flatMap(([memberId, bytes]) =>
      Object.entries(MemberAssignment.decode(bytes).assignment).flatMap(([topic, partitions]) =>
        partitions.map((partition) => [memberId, `${topic}:${partition}`])
      )
    )
    expect(held.map(([, partition]) => partition).sort()).toEqual(['t-a:0', 't-a:1'])
    for (const [memberId, partition] of held) {
      const wanted = SCARCE_ASSIGNMENT[memberId]['t-a'][0]
      expect(partition).toBe(`t-a:${wanted}`)
    }
  })

  test(`${assignor} keeps its assignment envelope observation separate from the Java bytes`, async () => {
    const native = await nativeScarce(assignor)
    const decoded = Object.fromEntries(
      Object.entries(native).map(([memberId, bytes]) => [memberId, MemberAssignment.decode(bytes)])
    )
    const members = Object.keys(decoded)
    expect(members).toEqual(
      assignor === 'roundrobin' ? ['m-1', 'm-2'] : ['m-1', 'm-2', 'm-3', 'm-4']
    )
    for (const entry of Object.values(decoded)) {
      expect(entry.version).toBe(version)
      expect(entry.userData).toEqual(Buffer.alloc(0))
    }
  })

  // The round-robin assignor leaves a member dealt nothing out of its answer; the Java one lists it.
  const omitsEmptyMembers = assignor === 'roundrobin' ? it.failing : it
  omitsEmptyMembers(`${assignor} native assignment answers an entry for every member`, async () => {
    expect(Object.keys(await nativeScarce(assignor))).toEqual(['m-1', 'm-2', 'm-3', 'm-4'])
  })

  it.failing(`${assignor} native assignment bytes equal the Java bytes`, async () => {
    const vector = scarceVector(assignor)
    const native = await nativeScarce(assignor)
    for (const [memberId, bytes] of Object.entries(native)) {
      expect(bytes.toString('hex')).toBe(vector.expectedAssignmentBytesHex[memberId])
    }
  })
})

describe('Consumer > classic subscription versions and the valid empty assignment', () => {
  const LITERAL = {
    subscription_v0: { version: 0, topics: ['t-a'], userData: null },
    subscription_v2: {
      version: 2,
      topics: ['t-a'],
      userData: null,
      ownedPartitions: [],
      generationId: -1,
    },
  }
  const ASSIGNMENT = { 'm-1': { 't-a': [0, 1] }, 'm-2': { 't-a': [2, 3] } }

  test(VERSIONS_TITLE, () => {
    for (const [name, fields] of Object.entries(LITERAL)) {
      const vector = JSON.parse(
        fs.readFileSync(path.join(FIXTURES, 'range', `${name}_java.json`), 'utf8')
      )
      expect(vector.subscriptionVersion).toBe(fields.version)
      expect(vector.members.map(({ memberId }) => memberId)).toEqual(['m-1', 'm-2'])
      for (const member of vector.members) {
        const bytes = Buffer.from(member.subscriptionBytesHex, 'hex')
        expect(MemberMetadata.decode(bytes)).toEqual(fields)
        expect(MemberMetadata.encode(fields)).toEqual(bytes)
        expect(vector.expected[member.memberId]).toEqual(ASSIGNMENT[member.memberId])
        const assignment = Buffer.from(vector.expectedAssignmentBytesHex[member.memberId], 'hex')
        const decoded = { version: 3, assignment: ASSIGNMENT[member.memberId], userData: null }
        expect(MemberAssignment.decode(assignment)).toEqual(decoded)
        expect(MemberAssignment.encode(decoded)).toEqual(assignment)
      }
    }
  })

  test(EMPTY_TITLE, async () => {
    const vector = JSON.parse(
      fs.readFileSync(path.join(FIXTURES, 'range', 'empty_range_java.json'), 'utf8')
    )
    for (const memberId of ['m-1', 'm-2']) {
      const wire = vector.expectedAssignmentBytesHex[memberId]
      expect(wire).toBe('000300000000ffffffff')
      expect(MemberAssignment.decode(Buffer.from(wire, 'hex'))).toEqual({
        version: 3,
        assignment: {},
        userData: null,
      })
      expect(MemberAssignment.decode(Buffer.alloc(0))).toBeNull()

      const seen = await applySyncAnswer(wire)
      expect(seen.crashes).toEqual([])
      expect(seen.assigned).toEqual([[]])
      expect(seen.joins[0].memberAssignment).toEqual({})
      expect(seen.fetchesWhileHeld).toBe(0)
      expect(seen.fetchesAfter).toBe(0)
      expect(seen.heartbeats).toBeGreaterThan(0)
    }
  })
})
