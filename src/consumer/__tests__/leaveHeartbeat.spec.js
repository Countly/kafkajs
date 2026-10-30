const { SentFields, buildRequest, LOCAL_ASSIGNMENT } = require('../groupProtocol/heartbeatState')
const requestV1 = require('../../protocol/requests/consumerGroupHeartbeat/v1/request')
const Decoder = require('../../protocol/decoder')
const Protocol = require('../groupProtocol/consumer')
const Emitter = require('../../instrumentation/emitter')

/** @returns {object} nonempty membership with pending subscription and assignor changes */
const membership = () => ({
  groupId: 'g',
  memberId: 'm',
  memberEpoch: 3,
  groupInstanceId: 'i',
  rackId: 'rack-a',
  rebalanceTimeoutMs: 30000,
  subscribedTopicNames: ['pending'],
  subscribedTopicRegex: 'pending.*',
  regexUpdated: true,
  serverAssignor: 'range',
  localAssignment: {
    localEpoch: 9,
    byTopicId: new Map([['01020304-0506-0708-090a-0b0c0d0e0f10', [0, 1]]]),
  },
  sentFields: new SentFields(),
})

describe('Consumer > leave heartbeat', () => {
  test.each([-1, -2])(
    'builds identity only for epoch %i before optional getters run',
    (leaveEpoch) => {
      const p = membership()
      for (const field of [
        'rackId',
        'rebalanceTimeoutMs',
        'subscribedTopicNames',
        'subscribedTopicRegex',
        'regexUpdated',
        'serverAssignor',
        'localAssignment',
        'sentFields',
      ]) {
        Object.defineProperty(p, field, {
          get() {
            throw new Error(`leave read ${field}`)
          },
        })
      }
      const result = buildRequest(p, { leaveEpoch, sendAllFields: true })
      expect(result).toEqual({
        GroupId: 'g',
        MemberId: 'm',
        MemberEpoch: leaveEpoch,
        InstanceId: 'i',
      })
      expect(result[LOCAL_ASSIGNMENT]).toBeUndefined()
    }
  )
  test.each([-1, -2])('encodes literal no-update defaults at epoch %i', async (memberEpoch) => {
    const p = membership()
    p.sentFields.reset()
    const request = buildRequest(p, { leaveEpoch: memberEpoch })
    const body = (
      await requestV1({
        groupId: request.GroupId,
        memberId: request.MemberId,
        memberEpoch: request.MemberEpoch,
        instanceId: request.InstanceId,
      }).encode()
    ).buffer
    const d = new Decoder(body)
    expect(d.readUVarIntString()).toBe('g')
    expect(d.readUVarIntString()).toBe('m')
    expect(d.readInt32()).toBe(memberEpoch)
    expect(d.readUVarIntString()).toBe('i')
    expect(d.readUVarIntString()).toBeNull()
    expect(d.readInt32()).toBe(-1)
    expect(
      d.readUVarIntArray(() => {
        throw new Error('subscription must be null')
      })
    ).toBeNull()
    expect(d.readUVarIntString()).toBeNull()
    expect(d.readUVarIntString()).toBeNull()
    expect(
      d.readUVarIntArray(() => {
        throw new Error('partitions must be null')
      })
    ).toBeNull()
    expect(d.readUVarInt()).toBe(0)
    expect(d.offset).toBe(body.length)
    expect(body.toString('hex')).toBe(
      memberEpoch === -1
        ? '0267026dffffffff026900ffffffff0000000000'
        : '0267026dfffffffe026900ffffffff0000000000'
    )
  })
  test.each([
    [null, undefined, -1],
    [null, true, -1],
    [null, false, null],
    ['i', undefined, -2],
    ['i', false, -2],
    ['i', true, -1],
  ])(
    'preserves the leave choice for instance %s and leaveGroup %s',
    async (groupInstanceId, leaveGroup, expected) => {
      const logger = {
        namespace() {
          return this
        },
        debug() {},
        error() {},
        warn() {},
        info() {},
      }
      const broker = { consumerGroupHeartbeat: jest.fn(async () => ({})) }
      const p = new Protocol({
        cluster: {},
        groupId: 'g',
        groupInstanceId,
        rebalanceTimeout: 30000,
        logger,
        instrumentationEmitter: new Emitter(),
      })
      p.state = 'STABLE'
      p.memberEpoch = 3
      p.coordinator = broker
      p.subscribedTopicNames = ['pending']
      p.subscribedTopicRegex = 'pending.*'
      p.regexUpdated = true
      p.serverAssignor = 'range'
      await p.leave({ leaveGroup })
      if (expected == null) expect(broker.consumerGroupHeartbeat).not.toHaveBeenCalled()
      else
        expect(broker.consumerGroupHeartbeat).toHaveBeenCalledWith({
          groupId: 'g',
          memberId: p.memberId,
          memberEpoch: expected,
          ...(groupInstanceId ? { instanceId: groupInstanceId } : {}),
        })
      expect(p.state).toBe('UNSUBSCRIBED')
      p.memberEpoch = 0
      const join = buildRequest(p, { sendAllFields: true })
      expect(join.SubscribedTopicNames).toEqual(['pending'])
      expect(join.SubscribedTopicRegex).toBe('pending.*')
      expect(join.ServerAssignor).toBe('range')
      expect(join.TopicPartitions).toEqual([])
    }
  )
})
