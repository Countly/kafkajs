const { ShareRecordWalk } = require('../shareRecordWalk')
const { ShareAckLedger } = require('../shareAckLedger')
const { KafkaJSNonRetriableError, KafkaJSShareAcknowledgementError } = require('../../../errors')
const { createErrorFromCode } = require('../../../protocol/error')
const failure = (Class, message) => expect.objectContaining({ name: Class.name, message })
const OUTSIDE = 'Acknowledgement is outside its active handler acquisition'
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
    expect(() => ledger.decide('1', 'accept')).toThrow(failure(KafkaJSNonRetriableError, OUTSIDE))
    expect(ledger.prepare({ token }).ticket).toBeNull()
    expect(ledger.settle(ticket, { token })).toEqual([
      { firstOffset: '1', lastOffset: '1', type: 'reject', error: null },
    ])
    expect(ledger.settle(ticket, { token })).toEqual([])
    expect(ledger.size).toBe(2)
  })
  test('completion validates all offsets atomically and rejects late or foreign calls', () => {
    const { ledger, token } = make()
    expect(() => ledger.complete(['1', '99'], 'accept')).toThrow(
      failure(KafkaJSNonRetriableError, OUTSIDE)
    )
    expect(ledger.prepare({ token }).ticket).toBeNull()
    ledger.decide('1')
    expect(() => ledger.decide('1', 'renew')).toThrow(
      failure(KafkaJSNonRetriableError, 'Invalid terminal acknowledgement')
    )
    ledger.complete(['1', '1', '2', '3'], 'release')
    const { ticket } = ledger.prepare({ token })
    expect(ledger.settle(ticket, { token }).map((r) => r.type)).toEqual(['accept', 'release'])
    expect(ledger.size).toBe(0)
  })
  test('range completion promotes only the selected records and validates before mutating', () => {
    const { ledger, token } = make()
    const records = [{ offset: '1' }, { offset: '2' }, { offset: '3' }]
    ledger.decide('2', 'reject')
    expect(() => ledger.completeRange([records[0], { offset: '99' }], 0, 2, 'accept')).toThrow(
      failure(KafkaJSNonRetriableError, OUTSIDE)
    )
    expect(() => ledger.completeRange(records, 0, 3, 'done')).toThrow(
      failure(KafkaJSNonRetriableError, 'Invalid default acknowledgement')
    )
    expect(ledger.prepare({ token }).ticket).toBeNull()
    ledger.completeRange(records, 1, 3, 'release')
    ledger.completeRange(records, 3, 3, 'accept')
    expect(ledger.prepare({ token }).ticket.acknowledgementBatches).toEqual([
      { firstOffset: '2', lastOffset: '3', acknowledgeTypes: [3, 2] },
    ])
    expect(() => ledger.completeRange(records, 1, 2, 'accept')).toThrow(
      failure(KafkaJSNonRetriableError, OUTSIDE)
    )
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
    expect(results[0].error.cause).toBeInstanceOf(KafkaJSNonRetriableError)
    expect(results[0].error.cause.message).toBe('Acknowledgement response omitted a partition')
    expect(ledger.size).toBe(0)
  })
  test('numeric equality cannot replace opaque issuance identity', () => {
    const original = { epoch: 1, sentAt: 100 }
    const { ledger } = make({ token: original })
    ledger.complete(['1', '2', '3'], 'accept')
    const result = ledger.prepare({ token: { epoch: 1, sentAt: 100 } })
    expect(result.ticket).toBeNull()
    expect(result.results).toEqual([
      {
        firstOffset: '1',
        lastOffset: '3',
        type: 'accept',
        error: expect.any(KafkaJSShareAcknowledgementError),
      },
    ])
    expect(result.results[0].error.reason).toBe('session-lost')
    expect(ledger.prepare({ token: original }).ticket).toBeNull()
    expect(ledger.size).toBe(0)
  })
  test('a stale-token settlement fails sent and held entries as session-lost exactly once', () => {
    const { ledger, token } = make()
    ledger.complete(['1', '2'], 'accept')
    const { ticket } = ledger.prepare({ token })
    const results = ledger.settle(ticket, { token: Symbol('stale') })
    expect(results).toEqual([
      {
        firstOffset: '1',
        lastOffset: '2',
        type: 'accept',
        error: expect.any(KafkaJSShareAcknowledgementError),
      },
      {
        firstOffset: '3',
        lastOffset: '3',
        type: 'release',
        error: expect.any(KafkaJSShareAcknowledgementError),
      },
    ])
    expect(results[0].error).toMatchObject({
      reason: 'session-lost',
      topic: '__proto__',
      partition: 0,
      ranges: [{ firstOffset: '1', lastOffset: '2' }],
    })
    expect(results[1].error).toMatchObject({
      reason: 'session-lost',
      ranges: [{ firstOffset: '3', lastOffset: '3' }],
    })
    expect(ledger.size).toBe(0)
    expect(ledger.settle(ticket, { token })).toEqual([])
    expect(ledger.settle(ticket, { token: Symbol('stale') })).toEqual([])
    expect(ledger.prepare({ token })).toEqual({ ticket: null, results: [] })
    expect(() => ledger.complete(['3'], 'accept')).toThrow(
      failure(KafkaJSNonRetriableError, OUTSIDE)
    )
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
    expect(() => ShareAckLedger.requestTopics(prepared.slice(0, 2))).toThrow(
      failure(KafkaJSNonRetriableError, 'One acquisition per partition per request is required')
    )
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
    expect(() => ledger.decide('2')).toThrow(failure(KafkaJSNonRetriableError, OUTSIDE))
    expect(ledger.prepare({ token }).ticket).toBeNull()
    expect(ledger.settle(ticket, { token })[0].error).toBeNull()
    expect(ledger.settle(ticket, { token })).toEqual([])
    expect(ledger.size).toBe(0)
  })
  test('a million-offset absent remainder stays compact through failure and error serialization', () => {
    const walk = {
      deliver: { records: [], deliveryCounts: new Uint16Array(0) },
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
  test('holds every delivery from the compact arrays, including the last, without consuming them', () => {
    const records = ['10', '11', '20', '21'].map((offset) => ({ offset }))
    const deliveryCounts = new Uint16Array([1, 2, 3, 32767])
    const walk = {
      deliver: { records, deliveryCounts },
      gaps: [],
      release: [],
      reject: [],
    }
    const { ledger, token } = make({ walk })
    expect(ledger.size).toBe(4)
    ledger.decide('21', 'reject')
    ledger.complete(['10', '11', '20', '21'], 'accept')
    const { ticket } = ledger.prepare({ token })
    expect(ticket.acknowledgementBatches).toEqual([
      { firstOffset: '10', lastOffset: '11', acknowledgeTypes: [1] },
      { firstOffset: '20', lastOffset: '21', acknowledgeTypes: [1, 3] },
    ])
    expect(walk.deliver.records).toBe(records)
    expect(records.map((item) => item.offset)).toEqual(['10', '11', '20', '21'])
    expect(deliveryCounts).toEqual(new Uint16Array([1, 2, 3, 32767]))
  })
})

describe('mixed delivery, gap, hole and release acquisition above 2^53', () => {
  const BASE = 9007199254740993n
  const at = (n) => String(BASE + BigInt(n))
  const span = (first, last) => ({ firstOffset: at(first), lastOffset: at(last) })
  const batch = (first, last, acknowledgeTypes) => ({ ...span(first, last), acknowledgeTypes })
  const outcome = (first, last, type) => ({ ...span(first, last), type })
  const withoutError = ({ error, ...range }) => range
  const record = (n, isControlRecord = false) => ({ offset: at(n), isControlRecord })
  const mixed = () => {
    const walk = ShareRecordWalk.walk({
      acquiredRecords: [
        { ...span(0, 3), deliveryCount: 2 },
        { ...span(6, 9), deliveryCount: 3 },
      ],
      messages: [record(0), record(1), record(3, true), record(6), record(7)],
      corruption: { reason: 'checksum', offset: at(8), error: new Error('decode halted') },
    })
    expect(walk.deliver.records.map((item) => item.offset)).toEqual([at(0), at(1), at(6), at(7)])
    expect(walk.deliver.deliveryCounts).toEqual(new Uint16Array([2, 2, 3, 3]))
    expect(walk.gaps).toEqual([span(2, 3)])
    expect(walk.release).toEqual([span(8, 9)])
    return make({ walk })
  }

  test('one ticket carries exact batches with the unacquired hole omitted and gaps as type zero', () => {
    const { ledger, token } = mixed()
    ledger.decide(at(1), 'reject')
    ledger.decide(at(6), 'release')
    ledger.complete([at(0), at(1), at(6), at(7)], 'accept')
    const { ticket } = ledger.prepare({ token })
    expect(ticket.acknowledgementBatches).toEqual([
      batch(0, 1, [1, 3]),
      batch(2, 3, [0]),
      batch(6, 7, [2, 1]),
      batch(8, 9, [2]),
    ])
    const results = ledger.settle(ticket, { token })
    expect(results.map(withoutError)).toEqual([
      outcome(0, 0, 'accept'),
      outcome(1, 1, 'reject'),
      outcome(2, 3, 'gap'),
      outcome(6, 6, 'release'),
      outcome(7, 7, 'accept'),
      outcome(8, 9, 'release'),
    ])
    expect(results.map((result) => result.error)).toEqual(Array(6).fill(null))
    expect(ledger.size).toBe(0)
  })

  test('a failed settlement reports gaps as gaps and never as accepted ranges', () => {
    const { ledger, token } = mixed()
    ledger.complete([at(0), at(1), at(6), at(7)], 'accept')
    const { ticket } = ledger.prepare({ token })
    const results = ledger.settle(ticket, { token, result: 'rejected-by-broker' })
    expect(results.map(withoutError)).toEqual([
      outcome(0, 1, 'accept'),
      outcome(2, 3, 'gap'),
      outcome(6, 7, 'accept'),
      outcome(8, 9, 'release'),
    ])
    for (const result of results) {
      expect(result.error).toBeInstanceOf(KafkaJSShareAcknowledgementError)
      expect(result.error.reason).toBe('rejected-by-broker')
    }
    expect(results[0].error).toBe(results[2].error)
    expect(results[0].error.ranges).toEqual([span(0, 1), span(6, 7)])
    expect(results[1].error.ranges).toEqual([span(2, 3)])
    expect(results[3].error.ranges).toEqual([span(8, 9)])
    expect(ledger.size).toBe(0)
  })

  test('automatic gap and release ranges go out alone before any handler completes', () => {
    const { ledger, token } = mixed()
    expect(ledger.size).toBe(6)
    const first = ledger.prepare({ token }).ticket
    expect(first.acknowledgementBatches).toEqual([batch(2, 3, [0]), batch(8, 9, [2])])
    expect(ledger.settle(first, { token }).map(withoutError)).toEqual([
      outcome(2, 3, 'gap'),
      outcome(8, 9, 'release'),
    ])
    expect(ledger.size).toBe(4)
    ledger.decide(at(1), 'reject')
    ledger.complete([at(0), at(1), at(6), at(7)], 'accept')
    const second = ledger.prepare({ token }).ticket
    expect(second.acknowledgementBatches).toEqual([batch(0, 1, [1, 3]), batch(6, 7, [1])])
    expect(ledger.settle(second, { token }).map(withoutError)).toEqual([
      outcome(0, 0, 'accept'),
      outcome(1, 1, 'reject'),
      outcome(6, 7, 'accept'),
    ])
    expect(ledger.size).toBe(0)
  })
})

describe('fixed-seed ledger settlement properties', () => {
  const { mulberry32 } = require('../../../../testHelpers/schedule')
  const WIRE = { accept: 1, release: 2, reject: 3 }
  const expand = (batches) => {
    const types = new Map()
    for (const { firstOffset, lastOffset, acknowledgeTypes } of batches)
      for (let n = BigInt(firstOffset); n <= BigInt(lastOffset); n++) {
        expect(types.has(String(n))).toBe(false)
        const position = Number(n - BigInt(firstOffset))
        types.set(String(n), acknowledgeTypes[acknowledgeTypes.length === 1 ? 0 : position])
      }
    return types
  }
  test.each(Array.from({ length: 200 }, (_, i) => [i]))('case %i', (index) => {
    const random = mulberry32(2026102700 + index)
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
    expect(expand(ticket.acknowledgementBatches)).toEqual(
      new Map([...expected].map(([offset, type]) => [offset, WIRE[type]]))
    )
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
