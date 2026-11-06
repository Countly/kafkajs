const { encodeAcknowledgements, validateAcknowledgements } = require('../shareAckLedger')
const { mulberry32 } = require('../../../../testHelpers/schedule')
const Decoder = require('../../../protocol/decoder')
const { KafkaJSNonRetriableError } = require('../../../errors')
const request = require('../../../protocol/requests/shareAcknowledge/v2/request')
const row = (offset, type, automatic = false, last = offset) => ({
  first: BigInt(offset),
  last: BigInt(last),
  type,
  automatic,
})
const failure = (Class, message) => expect.objectContaining({ name: Class.name, message })
const OVERLAP = 'Overlapping or unbounded mixed acknowledgement entries'
const BOUNDS = 'Invalid acknowledgement batch bounds or type count'
const batch = (first, last, types) => ({
  firstOffset: String(first),
  lastOffset: String(last),
  acknowledgeTypes: types,
})

// Independent bounded oracle: broker rules are data, not calls into the encoder's validator.
const check = (entries, isRenewAck = false) => {
  const batches = encodeAcknowledgements(entries, isRenewAck)
  const actual = []
  let previous = -1n
  for (const b of batches) {
    const first = BigInt(b.firstOffset)
    const last = BigInt(b.lastOffset)
    const types = b.acknowledgeTypes
    expect(first > previous).toBe(true)
    expect(first <= last).toBe(true)
    expect(types.length === 1 || BigInt(types.length) === last - first + 1n).toBe(true)
    expect(types.every((t) => Number.isInteger(t) && t >= 0 && t <= (isRenewAck ? 4 : 3))).toBe(
      true
    )
    for (let n = first; n <= last; n++)
      actual.push([String(n), types.length === 1 ? types[0] : types[Number(n - first)]])
    previous = last
  }
  const expected = []
  for (const e of entries)
    for (let n = e.first; n <= e.last; n++) expected.push([String(n), isRenewAck ? 4 : e.type])
  expect(actual).toEqual(expected)
}

describe('deterministic acknowledgement encoding', () => {
  test('uniform runs collapse, mixed runs remain bounded, holes split and automatic ranges stay compact', () => {
    expect(encodeAcknowledgements([row(1, 1), row(2, 1)])).toEqual([batch(1, 2, [1])])
    expect(encodeAcknowledgements([row(1, 1), row(2, 2), row(3, 3), row(5, 1)])).toEqual([
      batch(1, 3, [1, 2, 3]),
      batch(5, 5, [1]),
    ])
    expect(encodeAcknowledgements([row(0, 0, true, 999999), row(1000000, 1)])).toEqual([
      batch(0, 999999, [0]),
      batch(1000000, 1000000, [1]),
    ])
  })
  test.each([10, 11])('mixed equal run boundary %i matches released Java packing', (count) => {
    const input = [
      row(0, 2),
      ...Array.from({ length: count }, (_, i) => row(i + 1, 1)),
      row(count + 1, 3),
    ]
    expect(encodeAcknowledgements(input)).toEqual(
      count === 10
        ? [batch(0, 11, [2, ...Array(10).fill(1), 3])]
        : [batch(0, 0, [2]), batch(1, 11, [1]), batch(12, 12, [3])]
    )
  })
  test('broker validator permits touching boundaries while encoder refuses overlap', () => {
    expect(() => validateAcknowledgements([batch(1, 2, [1]), batch(2, 3, [2])])).not.toThrow()
    expect(() => encodeAcknowledgements([row(1, 1, true, 2), row(2, 2, true, 3)])).toThrow(
      failure(KafkaJSNonRetriableError, OVERLAP)
    )
  })
  test.each([
    [[batch(2, 1, [1])], BOUNDS],
    [[batch(1, 3, [])], BOUNDS],
    [[batch(1, 3, [1, 2])], BOUNDS],
    [[batch(1, 1, [4])], 'Invalid acknowledgement type'],
    [[batch(1, 1, [-1])], 'Invalid acknowledgement type'],
    [[batch(1, 1, [5])], 'Invalid acknowledgement type'],
    [[batch(1, 1, [1.5])], 'Invalid acknowledgement type'],
    [[batch('-1', 1, [1])], 'Offset must be an exact nonnegative integer'],
    [[batch(0, '9223372036854775808', [1])], 'Offset exceeds signed int64'],
    [[batch(1, 3, [1]), batch(2, 4, [1])], BOUNDS],
  ])('locally refuses broker-invalid batches %p', (batches, message) =>
    expect(() => validateAcknowledgements(batches)).toThrow(
      failure(KafkaJSNonRetriableError, message)
    )
  )
  test('type four requires a renew request and leaves other valid types permitted', () => {
    expect(() => validateAcknowledgements([batch(1, 1, [4])], { isRenewAck: true })).not.toThrow()
    expect(() =>
      validateAcknowledgements([batch(1, 3, [0, 2, 4])], { isRenewAck: true })
    ).not.toThrow()
    expect(encodeAcknowledgements([row(1, 1), row(2, 3)], true)).toEqual([batch(1, 2, [4])])
  })
  test.each(['0', '9007199254740993', '9223372036854775802'])(
    'exact disjoint, adjacent and gapped packing properties at %s',
    (baseText) => {
      const base = BigInt(baseText)
      for (const positions of [
        [0, 1, 2, 3],
        [0, 2, 3, 5],
      ])
        for (const types of [
          [1, 1, 1, 1],
          [0, 1, 2, 3],
          [3, 2, 1, 0],
        ]) {
          const entries = positions.map((n, i) => row(base + BigInt(n), types[i]))
          check(entries)
          check(entries, true)
        }
    }
  )
  test('retains large offsets through real request bytes and an independent primitive reader', async () => {
    const batches = encodeAcknowledgements([
      row('9007199254740993', 1),
      row('9223372036854775807', 3),
    ])
    const encoded = await request({
      groupId: 'g',
      memberId: 'm',
      shareSessionEpoch: 1,
      isRenewAck: false,
      topics: [
        {
          topicId: '11111111-1111-4111-8111-111111111111',
          partitions: [{ partitionIndex: 0, acknowledgementBatches: batches }],
        },
      ],
    }).encode()
    const bytes = encoded.buffer
    const d = new Decoder(bytes)
    expect(d.readUVarIntString()).toBe('g')
    expect(d.readUVarIntString()).toBe('m')
    expect(d.readInt32()).toBe(1)
    expect(d.readBoolean()).toBe(false)
    expect(d.readUVarInt()).toBe(2)
    expect(d.readUuid()).toBe('11111111-1111-4111-8111-111111111111')
    expect(d.readUVarInt()).toBe(2)
    expect(d.readInt32()).toBe(0)
    expect(d.readUVarInt()).toBe(3)
    for (const [offset, type] of [
      ['9007199254740993', 1],
      ['9223372036854775807', 3],
    ]) {
      expect(d.readInt64().toString()).toBe(offset)
      expect(d.readInt64().toString()).toBe(offset)
      expect(d.readUVarInt()).toBe(2)
      expect(d.readInt8()).toBe(type)
      d.readTaggedFields()
    }
    d.readTaggedFields()
    d.readTaggedFields()
    d.readTaggedFields()
    expect(d.offset).toBe(bytes.length)
    expect(bytes.toString('hex')).toContain('0020000000000001')
    expect(bytes.toString('hex')).toContain('7fffffffffffffff')
  })
})

describe('fixed-seed ledger encoding properties', () => {
  test.each(Array.from({ length: 200 }, (_, i) => [i]))('case %i', (index) => {
    const seed = 2026102500 + index
    const random = mulberry32(seed)
    const base = seed % 2 ? 9007199254740993n : 0n
    let next = base
    const entries = Array.from({ length: 30 }, () => {
      next += BigInt(1 + Math.floor(random() * 3))
      return row(next, Math.floor(random() * 4))
    })
    check(entries)
    check(entries, true)
    expect(() => encodeAcknowledgements([entries[0], entries[0]])).toThrow(
      failure(KafkaJSNonRetriableError, OVERLAP)
    )
  })
})
