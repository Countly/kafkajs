const { ShareRecordWalk } = require('../shareRecordWalk')
const { ShareAckLedger } = require('../shareAckLedger')
const { KafkaJSNonRetriableError, KafkaJSShareAcknowledgementError } = require('../../../errors')
const { createErrorFromCode } = require('../../../protocol/error')
const make = (overrides = {}) => {
  const token = Symbol('acquisition')
  const walk = ShareRecordWalk.walk({
    acquiredRecords: [{ firstOffset: '1', lastOffset: '3', deliveryCount: 2 }],
    messages: [1, 2, 3].map((n) => ({ offset: String(n) })),
  })
  return {
    token,
    ledger: new ShareAckLedger({
      topic: '__proto__',
      topicId: '11111111-1111-4111-8111-111111111111',
      partition: 0,
      token,
      sentAt: 100,
      lockDuration: 1000,
      walk,
      ...overrides,
    }),
  }
}

describe('deterministic compact acknowledgement ledger', () => {
  test('local last decision wins but cannot send before its handler completes', () => {
    const { ledger, token } = make()
    ledger.decide('1', 'release')
    ledger.decide('1', 'reject')
    expect(ledger.prepare({ token }).ticket).toBeNull()
    ledger.complete(['1'], 'accept')
    const { ticket } = ledger.prepare({ token })
    expect(ticket.acknowledgementBatches).toEqual([
      { firstOffset: '1', lastOffset: '1', acknowledgeTypes: [3] },
    ])
    expect(() => ledger.decide('1', 'accept')).toThrow(KafkaJSNonRetriableError)
    expect(ledger.prepare({ token }).ticket).toBeNull()
    expect(ledger.settle(ticket, { token })).toEqual([
      { firstOffset: '1', lastOffset: '1', type: 'reject', error: null },
    ])
    expect(ledger.settle(ticket, { token })).toEqual([])
    expect(ledger.size).toBe(2)
  })
  test('completion validates all offsets atomically and rejects late or foreign calls', () => {
    const { ledger, token } = make()
    expect(() => ledger.complete(['1', '99'], 'accept')).toThrow(KafkaJSNonRetriableError)
    expect(ledger.prepare({ token }).ticket).toBeNull()
    ledger.decide('1')
    expect(() => ledger.decide('1', 'renew')).toThrow(KafkaJSNonRetriableError)
    ledger.complete(['1', '1', '2', '3'], 'release')
    const { ticket } = ledger.prepare({ token })
    expect(ledger.settle(ticket, { token }).map((r) => r.type)).toEqual(['accept', 'release'])
    expect(ledger.size).toBe(0)
  })
  test.each(['session-lost', 'membership-lost', 'not-sent', 'rejected-by-broker'])(
    'settles %s once and never resends',
    (reason) => {
      const { ledger, token } = make()
      ledger.complete(['1', '2', '3'], 'accept')
      const { ticket } = ledger.prepare({ token })
      const cause = createErrorFromCode(121)
      const results = ledger.settle(ticket, { token, result: reason, cause })
      expect(results).toHaveLength(1)
      expect(results[0].error).toBeInstanceOf(KafkaJSShareAcknowledgementError)
      expect(results[0].error).toMatchObject({
        reason,
        retriable: false,
        topic: '__proto__',
        partition: 0,
        ranges: [{ firstOffset: '1', lastOffset: '3' }],
      })
      expect(results[0].error.cause).toBe(cause)
      expect(ledger.settle(ticket, { token })).toEqual([])
      expect(ledger.prepare({ token }).ticket).toBeNull()
      expect(ledger.size).toBe(0)
    }
  )
  test('omitted acknowledged partition is rejected rather than left sent', () => {
    const { ledger, token } = make()
    ledger.complete(['1', '2', '3'], 'accept')
    const { ticket } = ledger.prepare({ token })
    const results = ledger.settle(ticket, { token, present: false })
    expect(results[0].error.reason).toBe('rejected-by-broker')
    expect(results[0].error.cause.message).toContain('omitted a partition')
    expect(ledger.size).toBe(0)
  })
  test('numeric equality cannot replace opaque issuance identity', () => {
    const original = { epoch: 1, sentAt: 100 }
    const { ledger } = make({ token: original })
    ledger.complete(['1', '2', '3'], 'accept')
    const result = ledger.prepare({ token: { epoch: 1, sentAt: 100 } })
    expect(result.ticket).toBeNull()
    expect(result.results[0].error.reason).toBe('session-lost')
    expect(ledger.prepare({ token: original }).ticket).toBeNull()
    expect(ledger.size).toBe(0)
  })
  test('one partition cannot combine two acquisitions, while distinct partitions group', () => {
    const a = make()
    const b = make()
    const c = make({ partition: 1 })
    for (const x of [a, b, c]) x.ledger.complete(['1', '2', '3'], 'accept')
    const prepared = [a, b, c].map((x) => ({
      ledger: x.ledger,
      ticket: x.ledger.prepare({ token: x.token }).ticket,
    }))
    expect(() => ShareAckLedger.requestTopics(prepared.slice(0, 2))).toThrow('One acquisition')
    expect(
      ShareAckLedger.requestTopics([prepared[0], prepared[2]])[0].partitions.map(
        (p) => p.partitionIndex
      )
    ).toEqual([0, 1])
  })
  test('captured final ticket survives ordinary retirement and can settle only once', () => {
    const { ledger, token } = make()
    ledger.complete(['1'], 'accept')
    const { ticket } = ledger.prepare({ token })
    const lost = ledger.invalidate('membership-lost', ticket)
    expect(lost).toHaveLength(1)
    expect(lost[0].firstOffset).toBe('2')
    expect(lost[0].lastOffset).toBe('3')
    expect(() => ledger.decide('2')).toThrow(KafkaJSNonRetriableError)
    expect(ledger.prepare({ token }).ticket).toBeNull()
    expect(ledger.settle(ticket, { token })[0].error).toBeNull()
    expect(ledger.settle(ticket, { token })).toEqual([])
    expect(ledger.size).toBe(0)
  })
  test('a million-offset absent remainder stays compact through failure and error serialization', () => {
    const walk = {
      deliver: [],
      gaps: [],
      reject: [],
      release: [{ firstOffset: '9007199254740993', lastOffset: '9007199255740992' }],
    }
    const { ledger, token } = make({ walk })
    expect(ledger.size).toBe(1)
    const { ticket } = ledger.prepare({ token })
    expect(ticket.acknowledgementBatches).toEqual([
      { firstOffset: '9007199254740993', lastOffset: '9007199255740992', acknowledgeTypes: [2] },
    ])
    expect(Object.isFrozen(ticket.acknowledgementBatches[0].acknowledgeTypes)).toBe(true)
    const result = ledger.settle(ticket, { token, result: 'session-lost' })
    expect(result).toHaveLength(1)
    expect(result[0].error.ranges).toEqual(walk.release)
    expect(JSON.stringify(result).length).toBeLessThan(1000)
    expect(ledger.size).toBe(0)
  })
})

describe('fixed-seed ledger settlement properties', () => {
  const { mulberry32 } = require('../../../../testHelpers/schedule')
  test.each(Array.from({ length: 200 }, (_, i) => 2026102700 + i))('seed %i', (seed) => {
    const random = mulberry32(seed)
    const { ledger, token } = make()
    const expected = new Map()
    const offsets = ['1', '2', '3']
    for (let i = offsets.length - 1; i > 0; i--) {
      const j = Math.floor(random() * (i + 1))
      ;[offsets[i], offsets[j]] = [offsets[j], offsets[i]]
    }
    for (const offset of offsets) {
      ledger.decide(offset, 'release')
      const type = ['accept', 'release', 'reject'][Math.floor(random() * 3)]
      ledger.decide(offset, type)
      expected.set(offset, type)
      expect(ledger.prepare({ token }).ticket).toBeNull()
    }
    ledger.complete(offsets, 'accept')
    const { ticket } = ledger.prepare({ token })
    const results = ledger.settle(ticket, { token })
    const actual = new Map()
    for (const r of results)
      for (let n = BigInt(r.firstOffset); n <= BigInt(r.lastOffset); n++) {
        expect(actual.has(String(n))).toBe(false)
        actual.set(String(n), r.type)
      }
    expect(actual).toEqual(expected)
    expect(ledger.size).toBe(0)
    expect(ledger.settle(ticket, { token })).toEqual([])
    expect(ledger.prepare({ token }).ticket).toBeNull()
  })
})
