const {
  SentFields,
  buildRequest,
  equalsAsSortedSet,
  localAssignmentEquals,
  LOCAL_ASSIGNMENT,
} = require('../groupProtocol/heartbeatState')

const TOPIC_A = 'AAAAAAAAAAAAAAAAAAAAAg'
const TOPIC_B = 'AAAAAAAAAAAAAAAAAAAAAw'

const localAssignment = (localEpoch, entries = [[TOPIC_A, [0, 1]]]) => ({
  localEpoch,
  byTopicId: new Map(entries),
})

/**
 * A protocol double holding only the fields `buildRequest` reads.
 * @param {Object} [overrides]
 * @returns {Object}
 */
const protocolDouble = (overrides = {}) => ({
  groupId: 'g',
  memberId: 'm',
  memberEpoch: 4,
  groupInstanceId: null,
  rackId: null,
  rebalanceTimeoutMs: 300000,
  subscribedTopicNames: ['b-topic', 'a-topic'],
  subscribedTopicRegex: null,
  regexUpdated: false,
  serverAssignor: null,
  localAssignment: localAssignment(2),
  sentFields: new SentFields(),
  ...overrides,
})

/** Sends one heartbeat and records what went out, so the next call is a real "second heartbeat". */
const send = (protocol, options = {}) => {
  const request = buildRequest(protocol, options)
  protocol.sentFields.remember(request)
  return request
}

describe('Consumer > groupProtocol > heartbeatState', () => {
  describe('identity fields', () => {
    it('always sends GroupId, MemberId and MemberEpoch', () => {
      const protocol = protocolDouble()

      for (const options of [{ sendAllFields: true }, { sendAllFields: false }]) {
        const request = buildRequest(protocol, options)
        expect(request.GroupId).toEqual('g')
        expect(request.MemberId).toEqual('m')
        expect(request.MemberEpoch).toEqual(4)
      }
    })

    it('sends InstanceId on every heartbeat when static, and omits the key when not', () => {
      const stat = protocolDouble({ groupInstanceId: 'instance-1' })
      expect(buildRequest(stat, { sendAllFields: true }).InstanceId).toEqual('instance-1')
      send(stat, { sendAllFields: true })
      expect(buildRequest(stat).InstanceId).toEqual('instance-1')

      expect(buildRequest(protocolDouble())).not.toHaveProperty('InstanceId')
    })

    it('overrides MemberEpoch with leaveEpoch and changes nothing else', () => {
      const protocol = protocolDouble()
      send(protocol, { sendAllFields: true })

      const leave = buildRequest(protocol, { leaveEpoch: -2 })

      expect(leave.MemberEpoch).toEqual(-2)
      expect(leave.GroupId).toEqual('g')
      expect(leave.MemberId).toEqual('m')
    })
  })

  describe('the joining heartbeat sends everything', () => {
    it('carries every configured field', () => {
      const protocol = protocolDouble({
        groupInstanceId: 'instance-1',
        rackId: 'rack-1',
        serverAssignor: 'uniform',
        subscribedTopicRegex: 'topic-.*',
      })

      const request = buildRequest(protocol, { sendAllFields: true })

      expect(request).toMatchObject({
        GroupId: 'g',
        MemberId: 'm',
        MemberEpoch: 4,
        InstanceId: 'instance-1',
        RackId: 'rack-1',
        RebalanceTimeoutMs: 300000,
        SubscribedTopicRegex: 'topic-.*',
        ServerAssignor: 'uniform',
      })
      expect(request.SubscribedTopicNames).toEqual(['b-topic', 'a-topic'])
      expect(request.TopicPartitions).toEqual([{ TopicId: TOPIC_A, Partitions: [0, 1] }])
    })

    it('omits RackId when there is no rack, and never sends it on a non-joining heartbeat', () => {
      const noRack = protocolDouble()
      expect(buildRequest(noRack, { sendAllFields: true })).not.toHaveProperty('RackId')

      const racked = protocolDouble({ rackId: 'rack-1' })
      send(racked, { sendAllFields: true })
      expect(buildRequest(racked)).not.toHaveProperty('RackId')
    })
  })

  describe('send-if-changed, row by row', () => {
    it('sends only the identity fields when nothing changed', () => {
      const protocol = protocolDouble()
      send(protocol, { sendAllFields: true })

      expect(Object.keys(buildRequest(protocol)).sort()).toEqual([
        'GroupId',
        'MemberEpoch',
        'MemberId',
      ])
    })

    it('re-sends RebalanceTimeoutMs only when the value changed', () => {
      const protocol = protocolDouble()
      send(protocol, { sendAllFields: true })
      expect(buildRequest(protocol)).not.toHaveProperty('RebalanceTimeoutMs')

      protocol.rebalanceTimeoutMs = 60000
      expect(buildRequest(protocol).RebalanceTimeoutMs).toEqual(60000)
    })

    it('compares SubscribedTopicNames as a sorted set, so a reorder sends nothing', () => {
      const protocol = protocolDouble()
      send(protocol, { sendAllFields: true })

      protocol.subscribedTopicNames = ['a-topic', 'b-topic']
      expect(buildRequest(protocol)).not.toHaveProperty('SubscribedTopicNames')

      protocol.subscribedTopicNames = ['a-topic', 'b-topic', 'c-topic']
      expect(buildRequest(protocol).SubscribedTopicNames).toEqual(['a-topic', 'b-topic', 'c-topic'])

      expect(equalsAsSortedSet(['a', 'b'], ['b', 'a'])).toBe(true)
      expect(equalsAsSortedSet(['a'], ['a', 'b'])).toBe(false)
      expect(equalsAsSortedSet(null, [])).toBe(false)
      expect(equalsAsSortedSet(null, null)).toBe(true)
    })

    it('sends ServerAssignor only when one is configured, and then only on change', () => {
      const none = protocolDouble()
      expect(buildRequest(none, { sendAllFields: true })).not.toHaveProperty('ServerAssignor')

      const configured = protocolDouble({ serverAssignor: 'uniform' })
      send(configured, { sendAllFields: true })
      expect(buildRequest(configured)).not.toHaveProperty('ServerAssignor')

      configured.serverAssignor = 'range'
      expect(buildRequest(configured).ServerAssignor).toEqual('range')
    })

    it('re-sends TopicPartitions when the local epoch moved, although the partitions did not', () => {
      const protocol = protocolDouble()
      send(protocol, { sendAllFields: true })
      expect(buildRequest(protocol)).not.toHaveProperty('TopicPartitions')

      protocol.localAssignment = localAssignment(3)

      expect(buildRequest(protocol).TopicPartitions).toEqual([
        { TopicId: TOPIC_A, Partitions: [0, 1] },
      ])
    })

    it('re-sends TopicPartitions when the partitions changed under the same epoch', () => {
      const protocol = protocolDouble()
      send(protocol, { sendAllFields: true })

      protocol.localAssignment = localAssignment(2, [
        [TOPIC_A, [0, 1]],
        [TOPIC_B, [7]],
      ])

      expect(buildRequest(protocol).TopicPartitions).toEqual([
        { TopicId: TOPIC_A, Partitions: [0, 1] },
        { TopicId: TOPIC_B, Partitions: [7] },
      ])
    })

    it('compares local assignments by epoch and by partitions', () => {
      expect(localAssignmentEquals(localAssignment(1), localAssignment(1))).toBe(true)
      expect(localAssignmentEquals(localAssignment(1), localAssignment(2))).toBe(false)
      expect(localAssignmentEquals(localAssignment(1), localAssignment(1, [[TOPIC_A, [0]]]))).toBe(
        false
      )
      expect(localAssignmentEquals(null, localAssignment(1))).toBe(false)
      expect(localAssignmentEquals(null, null)).toBe(true)
    })

    it('sends an empty TopicPartitions list for a member that owns nothing', () => {
      const protocol = protocolDouble({ localAssignment: localAssignment(1, []) })

      expect(buildRequest(protocol, { sendAllFields: true }).TopicPartitions).toEqual([])
    })

    it('builds the joining heartbeat of a freshly reset member, whose assignment is empty', () => {
      // The invariant the membership owes this module: the field holds an empty assignment, never
      // null, so the first heartbeat after an epoch reset can be built at all.
      const protocol = protocolDouble({
        localAssignment: { localEpoch: 0, byTopicId: new Map() },
        sentFields: new SentFields(),
      })

      const request = buildRequest(protocol, { sendAllFields: true })

      expect(request.TopicPartitions).toEqual([])
      expect(request.MemberEpoch).toEqual(4)
    })

    it('throws rather than sending a heartbeat with no assignment at all', () => {
      // A null here is a bug in the membership, and a request silently missing TopicPartitions
      // would be read by the coordinator as "unchanged" - the one answer that is never right.
      const protocol = protocolDouble({ localAssignment: null })

      expect(() => buildRequest(protocol, { sendAllFields: true })).toThrow(TypeError)
    })
  })

  describe('regex subscriptions', () => {
    it('sends SubscribedTopicNames as [] and never as null on the joining heartbeat', () => {
      const protocol = protocolDouble({
        subscribedTopicNames: [],
        subscribedTopicRegex: 'orders-.*',
        regexUpdated: true,
      })

      const request = buildRequest(protocol, { sendAllFields: true })

      expect(request.SubscribedTopicNames).toEqual([])
      expect(request.SubscribedTopicNames).not.toBeNull()
      expect(request.SubscribedTopicRegex).toEqual('orders-.*')
    })

    it('sends the regex again only when it was updated', () => {
      const protocol = protocolDouble({ subscribedTopicRegex: 'orders-.*', regexUpdated: true })
      send(protocol, { sendAllFields: true })

      // The caller clears the latch once the request carrying the pattern went out; this is that
      // caller, standing in for the heartbeat path.
      protocol.regexUpdated = false
      expect(buildRequest(protocol)).not.toHaveProperty('SubscribedTopicRegex')

      protocol.subscribedTopicRegex = 'orders-v2-.*'
      protocol.regexUpdated = true
      expect(buildRequest(protocol).SubscribedTopicRegex).toEqual('orders-v2-.*')
    })

    it('stops re-sending the pattern once the caller clears the latch, however many heartbeats', () => {
      const protocol = protocolDouble({ subscribedTopicRegex: 'orders-.*', regexUpdated: true })
      const first = send(protocol, { sendAllFields: true })
      expect(first.SubscribedTopicRegex).toEqual('orders-.*')
      protocol.regexUpdated = false

      for (let i = 0; i < 5; i++) {
        expect(send(protocol)).not.toHaveProperty('SubscribedTopicRegex')
      }
      expect(protocol.sentFields.subscribedTopicRegex).toEqual('orders-.*')
    })

    it('clears a regex with the empty string, not with null, and only once', () => {
      const protocol = protocolDouble({ subscribedTopicRegex: 'orders-.*', regexUpdated: true })
      send(protocol, { sendAllFields: true })
      protocol.regexUpdated = false

      protocol.subscribedTopicRegex = null
      protocol.regexUpdated = true

      expect(send(protocol).SubscribedTopicRegex).toEqual('')
      expect(protocol.sentFields.subscribedTopicRegex).toEqual('')

      protocol.regexUpdated = false
      expect(buildRequest(protocol)).not.toHaveProperty('SubscribedTopicRegex')
    })

    it('re-sends the pattern after a reset, because the remembered value is the comparison', () => {
      const protocol = protocolDouble({ subscribedTopicRegex: 'orders-.*', regexUpdated: true })
      send(protocol, { sendAllFields: true })
      protocol.regexUpdated = false
      expect(buildRequest(protocol)).not.toHaveProperty('SubscribedTopicRegex')

      protocol.sentFields.reset()

      expect(buildRequest(protocol).SubscribedTopicRegex).toEqual('orders-.*')
    })

    it('omits the regex entirely for a member that never had one, even when joining', () => {
      expect(buildRequest(protocolDouble(), { sendAllFields: true })).not.toHaveProperty(
        'SubscribedTopicRegex'
      )
    })

    it('still omits it for a name-only member after a reset', () => {
      const protocol = protocolDouble()
      send(protocol, { sendAllFields: true })

      protocol.sentFields.reset()

      expect(buildRequest(protocol)).not.toHaveProperty('SubscribedTopicRegex')
    })
  })

  describe('SentFields', () => {
    it('remembers only the fields the request actually carried', () => {
      const protocol = protocolDouble({ serverAssignor: 'uniform' })
      send(protocol, { sendAllFields: true })

      const before = protocol.sentFields.serverAssignor
      protocol.subscribedTopicNames = ['z-topic']
      send(protocol)

      expect(protocol.sentFields.serverAssignor).toEqual(before)
      expect(protocol.sentFields.subscribedTopicNames).toEqual(['z-topic'])
    })

    it('carries the local assignment across on the symbol, not on the wire', () => {
      const protocol = protocolDouble()
      const request = buildRequest(protocol, { sendAllFields: true })

      expect(request[LOCAL_ASSIGNMENT]).toEqual(protocol.localAssignment)
      expect(Object.keys(request)).not.toContain('localAssignment')
      expect(JSON.parse(JSON.stringify(request))).not.toHaveProperty('localAssignment')

      protocol.sentFields.remember(request)
      expect(protocol.sentFields.localAssignment).toEqual(protocol.localAssignment)
    })

    it('remembers a snapshot, so mutating the live assignment still re-sends TopicPartitions', () => {
      const protocol = protocolDouble()
      send(protocol, { sendAllFields: true })

      expect(protocol.sentFields.localAssignment).not.toBe(protocol.localAssignment)
      expect(protocol.sentFields.localAssignment.byTopicId).not.toBe(
        protocol.localAssignment.byTopicId
      )

      // An in-place edit is not how the protocol is meant to publish a new assignment, but if it
      // ever happens the diff must still notice. An alias would compare the object with itself.
      protocol.localAssignment.byTopicId.set(TOPIC_B, [7])

      expect(buildRequest(protocol).TopicPartitions).toEqual([
        { TopicId: TOPIC_A, Partitions: [0, 1] },
        { TopicId: TOPIC_B, Partitions: [7] },
      ])
    })

    it('reset() makes the next heartbeat a full one, without touching the identity fields', () => {
      const protocol = protocolDouble({
        groupInstanceId: 'instance-1',
        serverAssignor: 'uniform',
        subscribedTopicRegex: 'orders-.*',
        regexUpdated: true,
      })
      send(protocol, { sendAllFields: true })
      protocol.regexUpdated = false
      expect(Object.keys(buildRequest(protocol)).sort()).toEqual([
        'GroupId',
        'InstanceId',
        'MemberEpoch',
        'MemberId',
      ])

      protocol.sentFields.reset()

      const request = buildRequest(protocol)
      expect(request).toMatchObject({
        RebalanceTimeoutMs: 300000,
        ServerAssignor: 'uniform',
      })
      expect(request.SubscribedTopicNames).toEqual(['b-topic', 'a-topic'])
      expect(request.TopicPartitions).toEqual([{ TopicId: TOPIC_A, Partitions: [0, 1] }])
      // Every diffed row is re-stated, the pattern included: the reset is there precisely because
      // the coordinator may have lost what this member last said.
      expect(request.SubscribedTopicRegex).toEqual('orders-.*')
      // The rack is the one exception: it is a joining-heartbeat field, not a diffed one.
      expect(request).not.toHaveProperty('RackId')
    })

    it('starts reset, so the first heartbeat of a fresh member is full', () => {
      const fields = new SentFields()

      expect(fields.subscribedTopicNames).toBeNull()
      expect(fields.rebalanceTimeoutMs).toEqual(-1)
      expect(fields.serverAssignor).toBeNull()
      expect(fields.subscribedTopicRegex).toBeNull()
      expect(fields.localAssignment).toBeNull()
    })
  })

  it('is pure: buildRequest never mutates the protocol or the sent fields', () => {
    const protocol = protocolDouble({ subscribedTopicRegex: 'orders-.*', regexUpdated: true })
    // JSON.stringify renders a Map as {}, so the assignment is compared through its entries or the
    // aliasing bug this file exists to prevent would be invisible here.
    const readable = () => ({
      ...protocol,
      localAssignment: {
        localEpoch: protocol.localAssignment.localEpoch,
        entries: [...protocol.localAssignment.byTopicId],
      },
      sentFields: {
        ...protocol.sentFields,
        localAssignment: protocol.sentFields.localAssignment && [
          protocol.sentFields.localAssignment.localEpoch,
          [...protocol.sentFields.localAssignment.byTopicId],
        ],
      },
    })
    const snapshot = JSON.stringify(readable())

    buildRequest(protocol, { sendAllFields: true })
    buildRequest(protocol)
    buildRequest(protocol, { leaveEpoch: -1 })

    expect(JSON.stringify(readable())).toEqual(snapshot)
    // The latch in particular: clearing it is the caller's job, not this function's.
    expect(protocol.regexUpdated).toBe(true)
  })
})
