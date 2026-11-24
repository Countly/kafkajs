const { Kafka } = require('../../../../index')
const Record = require('../../../protocol/recordBatch/record/v0')
const buildRecordBatch = require('../../../protocol/requests/produce/buildRecordBatch')
const Long = require('../../../utils/long')
const { createErrorFromCode } = require('../../../protocol/error')
const { mulberry32 } = require('../../../../testHelpers/schedule')
const { wire, held, share, chargeOf, varintSize } = require('../recordSize')
const Accumulator = require('../accumulator')
const {
  KafkaJSInvalidConfigurationError,
  KafkaJSNonRetriableError,
  KafkaJSRecordTooLargeError,
} = require('../../../errors')
const {
  bufferedProducer,
  countersOf,
  deferred,
  flushAll,
  quiescent,
  watch,
} = require('../../__tests__/fixtures/bufferHarness')

const TOPIC = 'topic-a'

/** The bytes of one field, as `recordSize` documents them: varint and bytes, or 1 for null. */
const field = (bytes) => (bytes === null ? 1 : varintSize(bytes) + bytes)

/** Reads a zigzag varint at `offset` by hand: `{ value, next }`. */
const readVarint = (buffer, offset) => {
  let raw = 0
  let shift = 0
  let next = offset
  for (;;) {
    const byte = buffer[next++]
    raw += (byte & 0x7f) * 2 ** shift
    shift += 7
    if (byte < 0x80) break
  }
  return { value: raw % 2 === 0 ? raw / 2 : -(raw + 1) / 2, next }
}

/** Makes Node start a new small-Buffer pool, of `Buffer.poolSize` bytes. */
const rebuildPool = () => {
  const { buffer } = Buffer.allocUnsafe(1)
  let probe
  do {
    probe = Buffer.allocUnsafe(1024)
  } while (probe.buffer === buffer)
}

/** Runs `make` with Node's small-Buffer pool rebuilt at `size`, then rebuilds it at the old size. */
const withPool = (size, make) => {
  const before = Buffer.poolSize
  try {
    Buffer.poolSize = size
    rebuildPool()
    return make()
  } finally {
    Buffer.poolSize = before
    rebuildPool()
  }
}

describe('Producer > the buffer > record sizes', () => {
  test('varintSize is the zigzag varint length at every boundary', () => {
    const sizes = [0, 63, 64, 8191, 8192, 1048575, 1048576, 134217727, 134217728, 2147483647]
    expect(sizes.map(varintSize)).toEqual([1, 1, 2, 2, 3, 3, 4, 4, 5, 5])
  })

  test("a record's wire size is at least the bytes the record encoder writes and at most 21 above them, for 200 seeded records", () => {
    const random = mulberry32(9)
    const pick = (list) => list[Math.floor(random() * list.length)]
    const lengths = [0, 1, 7, 63, 64, 300, 8191, 8192, 20000]
    const bytes = () => {
      const kind = pick(['null', 'absent', 'ascii', 'utf8', 'buffer'])
      const length = pick(lengths)
      if (kind === 'null') return null
      if (kind === 'absent') return undefined
      if (kind === 'ascii') return 'a'.repeat(length)
      if (kind === 'utf8') return 'é€😀'.repeat(Math.ceil(length / 4)).slice(0, length)
      return Buffer.alloc(length, length % 251)
    }
    const headersOf = () => {
      const kind = pick(['none', 'empty', 'strings', 'buffers', 'arrays'])
      if (kind === 'none') return undefined
      if (kind === 'empty') return {}
      const headers = {}
      for (let i = 0; i < 1 + Math.floor(random() * 4); i += 1) {
        const value = kind === 'strings' ? 'v'.repeat(pick(lengths)) : Buffer.alloc(pick(lengths))
        headers[`h-${i}-é`] = kind === 'arrays' ? Object.assign([value], { 2: 'second' }) : value
      }
      return headers
    }

    for (let i = 0; i < 200; i += 1) {
      const record = { key: bytes(), value: bytes(), headers: headersOf() }
      const encoded = Record({
        ...record,
        offsetDelta: Math.floor(random() * 2147483647),
        timestampDelta: Math.floor(random() * 2 ** 40),
      }).buffer.length

      expect(wire(record)).toBeGreaterThanOrEqual(encoded)
      expect(wire(record)).toBeLessThanOrEqual(encoded + 21)
    }
  })

  test.each([11, 12])(
    "the 21 bytes beyond a record's fields are the Java client's, on its v%s records",
    (version) => {
      const { source, batchHex } = require(
        `../../../protocol/requests/produce/fixtures/java/produceRequest-v${version}-records.json`
      )
      const batch = Buffer.from(batchHex, 'hex')
      let offset = 61
      source.keys.forEach((key, index) => {
        const { value: length, next } = readVarint(batch, offset)
        const body = next + 1
        const afterTimestamp = readVarint(batch, body).next
        const afterOffset = readVarint(batch, afterTimestamp).next
        const fieldBytes = next + length - afterOffset
        const record = { key, value: source.values[index], headers: source.headers[index] }

        expect(wire(record)).toBe(21 + fieldBytes)
        offset = next + length
      })
      expect(offset).toBe(batch.length)
    }
  )

  test('a held record is its wire size, 64, and 16 for each header: an array value is one header per element and a hole none', () => {
    const record = {
      key: 'k',
      value: Buffer.alloc(100),
      headers: { a: 'x', list: Object.assign(['yy'], { 2: 'zzz' }) },
    }
    const expected =
      21 + field(1) + field(100) + 1 + (field(1) + field(1)) + 2 * field(4) + field(2) + field(3)

    expect(wire(record)).toBe(expected)
    expect(held(record)).toBe(expected + 64 + 3 * 16)
    expect(held({ value: null })).toBe(88)
  })

  test("a record's share is its held size without the bytes of its Buffers", () => {
    const record = {
      key: Buffer.alloc(10),
      value: Buffer.alloc(100),
      headers: { a: Buffer.alloc(7), b: [Buffer.alloc(3), 'str'] },
    }

    expect(share(record)).toBe(held(record) - 120)
    expect(share({ key: 'k', value: 'v' })).toBe(held({ key: 'k', value: 'v' }))
  })

  test('a send is charged its held records and 1536, and its shell is the 1536 and the bytes of its Buffers', () => {
    const records = [{ key: 'k', value: 'v' }, { value: Buffer.alloc(10) }]

    // by hand: 21 + 64 + 2 + 2 + 1 = 90 and 21 + 64 + 1 + 11 + 1 = 98, then 1536
    expect(chargeOf([{ topic: TOPIC, messages: records }], 1048576)).toEqual({
      total: 1724,
      shell: 1546,
    })
    expect(held(records[0]) + held(records[1]) + 1536).toBe(1724)
  })

  test('the stores of Buffers are charged by identity: a store once per send, its uncovered bytes on top', () => {
    const slab = Buffer.alloc(16384)
    const charge = (...values) =>
      chargeOf([{ topic: TOPIC, messages: values.map((value) => ({ value })) }], 1048576)
    const heldOf = (...lengths) =>
      lengths.reduce((sum, length) => sum + held({ value: Buffer.alloc(length) }), 0)

    // two disjoint 8 KiB views cover the store: nothing more is charged
    expect(charge(slab.subarray(0, 8192), slab.subarray(8192))).toEqual({
      total: heldOf(8192, 8192) + 1536,
      shell: 1536 + 16384,
    })
    // a 1 KiB view keeps the other 15 KiB alive: by hand, 1113 + 1536 + 15360 and 1536 + 15360 + 1024
    expect(charge(slab.subarray(0, 1024))).toEqual({ total: 18009, shell: 17920 })
    expect(heldOf(1024)).toBe(1113)
    // overlapping views cover their union once
    expect(charge(slab.subarray(0, 6000), slab.subarray(4000, 10000))).toEqual({
      total: heldOf(6000, 6000) + 1536 + 16384 - 10000,
      shell: 1536 + 16384 - 10000 + 12000,
    })
    // independent slabs, each fully used
    expect(charge(Buffer.alloc(16384), Buffer.alloc(16384))).toEqual({
      total: heldOf(16384, 16384) + 1536,
      shell: 1536 + 32768,
    })
    // a Buffer used twice is counted in each record, its store once
    const twice = Buffer.alloc(100)
    expect(charge(twice, twice)).toEqual({ total: heldOf(100, 100) + 1536, shell: 1536 + 200 })
  })

  test('Buffer-valued headers and keys are grouped with the values that share their store', () => {
    const slab = Buffer.alloc(4096)
    const record = {
      key: slab.subarray(0, 100),
      value: slab.subarray(100, 200),
      headers: { a: slab.subarray(200, 300), b: [slab.subarray(300, 400), 'text'] },
    }

    expect(chargeOf([{ topic: TOPIC, messages: [record] }], 1048576)).toEqual({
      total: held(record) + 1536 + 4096 - 400,
      shell: 1536 + 4096 - 400 + 400,
    })
  })

  test('a store shared by two sends is charged to each of them', () => {
    const slab = Buffer.alloc(8192)
    const send = (value) => chargeOf([{ topic: TOPIC, messages: [{ value }] }], 1048576)

    expect(send(slab.subarray(0, 10)).total).toBe(held({ value: Buffer.alloc(10) }) + 1536 + 8182)
    expect(send(slab.subarray(10, 20)).total).toBe(held({ value: Buffer.alloc(10) }) + 1536 + 8182)
  })

  test('the charge stops at the first prefix above the limit, before reading the records after it', () => {
    let readsAfter = 0
    const after = {
      get value() {
        readsAfter += 1
        return 'late'
      },
    }
    const messages = [{ value: 'a'.repeat(400) }, { value: 'b'.repeat(400) }, after]

    const { total } = chargeOf([{ topic: TOPIC, messages }], 2200)

    expect(total).toBe(1536 + held(messages[0]) + held(messages[1]))
    expect(total).toBeGreaterThan(2200)
    expect(readsAfter).toBe(0)
  })

  test('the backing stores count in every prefix: distinct stores, a repeated view and overlapping views', () => {
    const later = () => {
      const record = {
        reads: 0,
        get value() {
          record.reads += 1
          return 'late'
        },
      }
      return record
    }
    const run = (views, limit) => {
      const after = later()
      const messages = [...views.map((value) => ({ value })), after]
      return { ...chargeOf([{ topic: TOPIC, messages }], limit), reads: after.reads }
    }
    const first = Buffer.alloc(8192)
    const second = Buffer.alloc(8192)

    // a 1 KiB view holds 1113 (21 + 64 + 1 + 1026 + 1) and keeps the other 7168 bytes of its store
    // alive: 1536 + 1113 + 7168 = 9817 for the first record
    expect(run([first.subarray(0, 1024), second.subarray(0, 1024)], 9816)).toEqual({
      total: 9817,
      shell: 0,
      reads: 0,
    })
    // a second store adds its own 7168: 9817 + 1113 + 7168 = 18098
    expect(run([first.subarray(0, 1024), second.subarray(0, 1024)], 18000)).toEqual({
      total: 18098,
      shell: 0,
      reads: 0,
    })
    // the same view again adds its held size and nothing of the store: 9817 + 1113 = 10930
    const view = first.subarray(0, 1024)
    expect(run([view, view], 10929)).toEqual({ total: 10930, shell: 0, reads: 0 })
    // an overlapping view takes back the 512 bytes it shows that the first did not:
    // 9817 + 1113 - 512 = 10418
    expect(run([first.subarray(0, 1024), first.subarray(512, 1536)], 10417)).toEqual({
      total: 10418,
      shell: 0,
      reads: 0,
    })
    expect(
      chargeOf(
        [
          {
            topic: TOPIC,
            messages: [{ value: first.subarray(0, 1024) }, { value: first.subarray(512, 1536) }],
          },
        ],
        1048576
      )
    ).toEqual({ total: 10418, shell: 1536 + 6656 + 2048 })
  })

  test('the stores are charged what a byte-by-byte count of the bytes no view shows gives, for 300 seeded sends', () => {
    const random = mulberry32(17)
    for (let i = 0; i < 300; i += 1) {
      const stores = Array.from({ length: 1 + Math.floor(random() * 3) }, () =>
        Buffer.alloc(1 + Math.floor(random() * 4096))
      )
      const messages = Array.from({ length: Math.floor(random() * 10) }, () => {
        const store = stores[Math.floor(random() * stores.length)]
        const start = Math.floor(random() * store.length)
        return {
          value: store.subarray(start, start + Math.floor(random() * (store.length - start + 1))),
        }
      })
      /** @type {Map<ArrayBuffer, Set<number>>} by store its views reach, the bytes they show */
      const shown = new Map()
      for (const { value } of messages) {
        const bytes = shown.get(value.buffer) ?? new Set()
        for (let byte = value.byteOffset; byte < value.byteOffset + value.length; byte += 1) {
          bytes.add(byte)
        }
        shown.set(value.buffer, bytes)
      }
      const kept = [...shown].reduce(
        (sum, [store, bytes]) => sum + store.byteLength - bytes.size,
        0
      )
      const records = messages.reduce((sum, message) => sum + held(message), 0)
      const bytes = messages.reduce((sum, { value }) => sum + value.length, 0)

      expect(chargeOf([{ topic: TOPIC, messages }], Number.MAX_SAFE_INTEGER)).toEqual({
        total: records + 1536 + kept,
        shell: 1536 + kept + bytes,
      })
    }
  })

  test("a store's remainder ends the pass at the first prefix it takes past the limit, before a later record is read", () => {
    let reads = 0
    const later = {
      get value() {
        reads += 1
        return 'late'
      },
    }
    const view = Buffer.alloc(16384).subarray(0, 1)

    // 1536 + 89 (21 + 64 + 1 + 2 + 1) + 16383 = 18008
    expect(chargeOf([{ topic: TOPIC, messages: [{ value: view }, later] }], 2000)).toEqual({
      total: 18008,
      shell: 0,
    })
    expect(reads).toBe(0)
  })

  test('a record whose held size alone is above the limit is refused, naming its topic', () => {
    const run = () => chargeOf([{ topic: TOPIC, messages: [{ value: 'x'.repeat(100) }] }], 150)

    expect(run).toThrow(KafkaJSRecordTooLargeError)
    expect(run).toThrow(
      expect.objectContaining({
        reason: 'bufferMemory',
        scope: 'record',
        size: held({ value: 'x'.repeat(100) }),
        limit: 150,
        topic: TOPIC,
      })
    )
  })
})

describe('Producer > the buffer > small views of large stores', () => {
  /** The records of a one-topic send as the producer keeps them. */
  const snapshotOf = (messages) => Accumulator.snapshot([{ topic: TOPIC, messages }])[0].messages

  test('the small views of large stores in one send are copied into one store of the sum of their lengths', () => {
    const slab = Buffer.alloc(1048576, 'a')
    slab.write('bcdefg', 10)
    const views = [slab.subarray(10, 11), slab.subarray(11, 13), slab.subarray(13, 16)]

    const copies = snapshotOf(views.map((value) => ({ value }))).map(({ value }) => value)

    expect(copies.map(String)).toEqual(['b', 'cd', 'efg'])
    copies.forEach((copy, i) => expect(copy).not.toBe(views[i]))
    expect(new Set(copies.map((copy) => copy.buffer)).size).toBe(1)
    expect(copies[0].buffer.byteLength).toBe(6)
    slab.fill('z')
    expect(copies.map(String)).toEqual(['b', 'cd', 'efg'])
  })

  test('the charge of a send whose views share one copy is its held bytes and its shell, with no store excess', () => {
    const slab = Buffer.alloc(1048576, 'a')
    const messages = snapshotOf([
      { key: slab.subarray(0, 3), value: slab.subarray(3, 10) },
      { value: slab.subarray(10, 30), headers: { h: slab.subarray(30, 31) } },
    ])
    const heldBytes = held(messages[0]) + held(messages[1])
    const { key, value } = messages[0]

    expect(
      new Set([key, value, messages[1].value, messages[1].headers.h].map((field) => field.buffer))
        .size
    ).toBe(1)
    expect(key.buffer.byteLength).toBe(31)
    expect(chargeOf([{ topic: TOPIC, messages }], Infinity)).toEqual({
      total: 1536 + heldBytes,
      shell: 1536 + 31,
    })
  })

  test('a hole in a messages list stays a hole in the plain array the producer keeps', () => {
    const slab = Buffer.alloc(1048576, 'a')
    const messages = [{ value: slab.subarray(0, 1) }]
    messages[2] = { value: slab.subarray(1, 2) }

    const own = snapshotOf(messages)

    expect(Object.getPrototypeOf(own)).toBe(Array.prototype)
    expect(own).toHaveLength(3)
    expect(1 in own).toBe(false)
    expect(String(own[2].value)).toBe('a')
    expect(own[2].value.buffer).toBe(own[0].value.buffer)
    expect(own[0].value.buffer.byteLength).toBe(2)
  })

  test('independent sends of the same source view get independent copies', () => {
    const view = Buffer.alloc(1048576, 'a').subarray(0, 1)

    const [first] = snapshotOf([{ value: view }])
    const [second] = snapshotOf([{ value: view }])

    expect(first.value.buffer).not.toBe(second.value.buffer)
    expect(first.value.buffer.byteLength).toBe(1)
    expect(second.value.buffer.byteLength).toBe(1)
    first.value[0] = 'z'.charCodeAt(0)
    expect(String(second.value)).toBe('a')
  })

  test('a view at or under the thresholds, a string and a null are kept as the same objects; a view just above them is copied', () => {
    const atSixteenKiB = Buffer.alloc(16384).subarray(0, 1)
    const atSixteenTimes = Buffer.alloc(32768).subarray(0, 2048)
    const headers = { text: 'text', none: null, list: [atSixteenKiB, 'item'] }

    const [record] = snapshotOf([{ key: atSixteenKiB, value: atSixteenTimes, headers }])

    expect(record.key).toBe(atSixteenKiB)
    expect(record.value).toBe(atSixteenTimes)
    expect(record.headers.text).toBe('text')
    expect(record.headers.none).toBeNull()
    expect(record.headers.list[0]).toBe(atSixteenKiB)
    expect(record.headers.list[1]).toBe('item')

    const aboveSixteenKiB = Buffer.alloc(16385).subarray(0, 1)
    const underSixteenTimes = Buffer.alloc(32768).subarray(0, 2047)
    const [copied] = snapshotOf([{ key: aboveSixteenKiB, value: underSixteenTimes }])
    expect(copied.key).not.toBe(aboveSixteenKiB)
    expect(copied.value).not.toBe(underSixteenTimes)
    expect(copied.key.buffer).toBe(copied.value.buffer)
    expect(copied.key.buffer.byteLength).toBe(1 + 2047)
  })

  test("the keys, values and header values of one send, array elements included, land in one copy store; the caller's array keeps its views", () => {
    const slab = Buffer.alloc(65536)
    slab.write('kvhab', 0)
    const [key, value, header, first, second] = [0, 1, 2, 3, 4].map((i) => slab.subarray(i, i + 1))
    const list = [first, 'text', second]

    const [record] = snapshotOf([{ key, value, headers: { h: header, list } }])
    const fields = [
      record.key,
      record.value,
      record.headers.h,
      record.headers.list[0],
      record.headers.list[2],
    ]

    expect(fields.map(String).join('')).toBe('kvhab')
    expect(new Set(fields.map((field) => field.buffer)).size).toBe(1)
    expect(fields[0].buffer.byteLength).toBe(5)
    expect(record.headers.list[1]).toBe('text')
    expect(list).toEqual([first, 'text', second])
    expect(list[0]).toBe(first)
  })

  test("pooled Buffer values are charged by their own bytes under a 64 KiB pool, and kept as the caller's under an 8 KiB pool", () => {
    const texts = ['{"a":1}', '{"b":22}', 'x'.repeat(300)]

    withPool(65536, () => {
      const values = texts.map((text) => Buffer.from(text))
      expect(values.every((value) => value.buffer.byteLength >= 65536)).toBe(true)
      const messages = snapshotOf(values.map((value) => ({ value })))
      const heldBytes = messages.reduce((sum, message) => sum + held(message), 0)

      expect(messages.map(({ value }) => String(value))).toEqual(texts)
      expect(new Set(messages.map(({ value }) => value.buffer)).size).toBe(1)
      expect(chargeOf([{ topic: TOPIC, messages }], Infinity)).toEqual({
        total: 1536 + heldBytes,
        shell: 1536 + 7 + 8 + 300,
      })
    })
    withPool(8192, () => {
      const values = texts.map((text) => Buffer.from(text))
      expect(values.every((value) => value.buffer.byteLength <= 16384)).toBe(true)
      const messages = snapshotOf(values.map((value) => ({ value })))
      const heldBytes = messages.reduce((sum, message) => sum + held(message), 0)
      const pool = values[0].buffer

      messages.forEach((message, i) => expect(message.value).toBe(values[i]))
      expect(values.every((value) => value.buffer === pool)).toBe(true)
      expect(chargeOf([{ topic: TOPIC, messages }], Infinity)).toEqual({
        total: 1536 + heldBytes + pool.byteLength - (7 + 8 + 300),
        shell: 1536 + pool.byteLength,
      })
    })
  })

  test('a Buffer whose own copy method is replaced is copied without calling it, at its own bytes', () => {
    const slab = Buffer.alloc(1048576, 'a')
    slab.write('xyz', 5)
    const key = slab.subarray(5, 8)
    const calls = []
    key.copy = (...args) => {
      calls.push(args)
      return 1000
    }

    const [record] = snapshotOf([{ key, value: slab.subarray(8, 10) }])

    expect(calls).toEqual([])
    expect(String(record.key)).toBe('xyz')
    expect(String(record.value)).toBe('aa')
    expect(record.value.buffer).toBe(record.key.buffer)
    expect(record.key.buffer.byteLength).toBe(5)
  })

  test("a header-value array whose own slice returns itself is copied without it: the caller's array keeps its views and its holes", () => {
    const slab = Buffer.alloc(65536)
    slab.write('ab', 0)
    const first = slab.subarray(0, 1)
    const second = slab.subarray(1, 2)
    const list = [first]
    list[2] = second
    list.slice = jest.fn(() => list)

    const [record] = snapshotOf([{ headers: { list } }])
    const copy = record.headers.list

    expect(list.slice).not.toHaveBeenCalled()
    expect(list[0]).toBe(first)
    expect(list[2]).toBe(second)
    expect(1 in list).toBe(false)
    expect(copy).not.toBe(list)
    expect(copy).toHaveLength(3)
    expect(1 in copy).toBe(false)
    expect(String(copy[0]) + String(copy[2])).toBe('ab')
    expect(copy[2].buffer).toBe(copy[0].buffer)
    expect(copy[0].buffer.byteLength).toBe(2)
  })

  test('a header-value array is copied up to the length it had when the copy began: an element getter that appends adds nothing', () => {
    const slab = Buffer.alloc(65536)
    slab.write('abc', 0)
    const list = [slab.subarray(0, 1), slab.subarray(1, 2)]
    const first = list[0]
    Object.defineProperty(list, 0, {
      get() {
        list.push(slab.subarray(2, 3))
        return first
      },
      enumerable: true,
      configurable: true,
    })

    const [record] = snapshotOf([{ headers: { list } }])
    const copy = record.headers.list

    expect(list).toHaveLength(3)
    expect(copy).toHaveLength(2)
    expect(copy.map(String)).toEqual(['a', 'b'])
    expect(copy[0].buffer.byteLength).toBe(2)
  })

  test("a slice of another send's copy store that is kept as it is is charged the bytes of that store it does not show", () => {
    const slab = Buffer.alloc(1048576)
    const [kept] = snapshotOf([
      { value: slab.subarray(0, 100) },
      { value: slab.subarray(100, 200) },
    ])

    const messages = snapshotOf([{ value: kept.value }])

    expect(messages[0].value).toBe(kept.value)
    expect(chargeOf([{ topic: TOPIC, messages }], Infinity)).toEqual({
      total: 1536 + held(messages[0]) + 100,
      shell: 1536 + 200,
    })
  })

  test('5,000 references to one small view of a 1 MiB store are charged by their copied bytes, not the store', () => {
    const view = Buffer.alloc(1048576).subarray(0, 10)

    const messages = snapshotOf(Array.from({ length: 5000 }, () => ({ value: view })))

    expect(messages[0].value.buffer.byteLength).toBe(50000)
    expect(chargeOf([{ topic: TOPIC, messages }], Infinity)).toEqual({
      total: 1536 + 5000 * held({ value: view }),
      shell: 1536 + 50000,
    })
  })
})

describe("Producer > the buffer > the producer's own copy of a send", () => {
  beforeEach(() => {
    jest.useFakeTimers({ now: 1700000000000, doNotFake: ['nextTick', 'setImmediate'] })
  })

  afterEach(() => {
    jest.useRealTimers()
  })

  /**
   * A producer whose metadata wait is held until `release()`, and whose fake broker encodes each
   * Produce batch with the record encoder, as the real broker would, and keeps the bytes.
   */
  const heldProducer = async (options = {}) => {
    const fixture = bufferedProducer({ capacity: 4 * 1048576, ...options })
    const gate = deferred()
    const written = []
    fixture.cluster.refreshMetadataIfNecessary.mockImplementation(() => gate.promise)
    fixture.cluster.broker.produce.mockImplementation(async ({ topicData }) => {
      for (const { partitions } of topicData) {
        for (const { messages } of partitions) {
          const batch = await buildRecordBatch({
            compression: 0,
            messages,
            producerId: Long.fromValue(1000),
            producerEpoch: 0,
            firstSequence: 0,
          })
          written.push({ messages, bytes: batch.buffer })
        }
      }
      return topicData.flatMap(({ topic, partitions }) =>
        partitions.map(({ partition }) => ({
          topicName: topic,
          partition,
          errorCode: 0,
          error: null,
          baseOffset: '0',
          logAppendTime: '-1',
          logStartOffset: '0',
        }))
      )
    })
    await fixture.producer.connect()
    const release = async () => {
      gate.resolve()
      await flushAll()
    }
    return { ...fixture, written, release }
  }

  /** The first timestamp of a record batch, read from its v2 header by hand. */
  const firstTimestampOf = (bytes) => bytes.readBigInt64BE(27)

  test('changing a record, its headers, a header-value array or its timestamps after the call changes neither the bytes nor the charge', async () => {
    const { producer, admission, written, release } = await heldProducer()
    const date = new Date(1700000000123)
    const long = Long.fromValue(1700000000456n)
    const first = {
      key: 'k',
      value: 'before',
      headers: { h: 'x', list: ['a', 'b'] },
      timestamp: date,
    }
    const second = { value: 'other', timestamp: long }
    const expectedCharge = chargeOf(
      [
        {
          topic: TOPIC,
          messages: [
            { key: 'k', value: 'before', headers: { h: 'x', list: ['a', 'b'] } },
            { value: 'other' },
          ],
        },
      ],
      admission.capacity
    ).total

    const sent = watch(producer.send({ topic: TOPIC, messages: [first, second] }))
    await flushAll()
    expect(admission.reserved).toBe(expectedCharge)
    first.key = 'changed'
    first.value = 'after'
    first.headers.h = 'y'
    first.headers.added = 'z'
    first.headers.list.push('c')
    date.setTime(5)
    long.value = 6n
    await release()

    expect(sent.state).toBe('fulfilled')
    const [{ messages, bytes }] = written
    expect(messages).toEqual([
      {
        key: 'k',
        value: 'before',
        headers: { h: 'x', list: ['a', 'b'] },
        timestamp: '1700000000123',
      },
      { value: 'other', timestamp: '1700000000456' },
    ])
    expect(firstTimestampOf(bytes)).toBe(1700000000123n)
    expect(bytes.includes(Buffer.from('before'))).toBe(true)
    expect(bytes.includes(Buffer.from('after'))).toBe(false)
    await producer.disconnect()
  })

  test.each([
    ['Date(0)', () => new Date(0)],
    ['a Long of 0', () => Long.fromValue(0)],
    ['the string 0', () => '0'],
  ])('%s stays the literal epoch', async (_, timestamp) => {
    const { producer, written, release } = await heldProducer()

    const sent = producer.send({ topic: TOPIC, messages: [{ value: 'v', timestamp: timestamp() }] })
    await release()
    await sent

    expect(firstTimestampOf(written[0].bytes)).toBe(0n)
    await producer.disconnect()
  })

  test("the bytes of a Buffer the producer did not copy are the caller's: changing them after the call reaches the wire, as in 2.2.4", async () => {
    const { producer, written, release } = await heldProducer()
    const value = Buffer.alloc(8)
    value.write('original')

    const sent = producer.send({ topic: TOPIC, messages: [{ value }] })
    await flushAll()
    value.write('mutated!')
    await release()
    await sent

    expect(written[0].messages[0].value).toBe(value)
    expect(written[0].bytes.includes(Buffer.from('mutated!'))).toBe(true)
  })

  test('a small view of a large store is copied at the call: the store changed afterwards changes nothing, and only the copy is charged', async () => {
    const { producer, admission, written, release } = await heldProducer()
    const slab = Buffer.alloc(1048576, 'a')
    const view = slab.subarray(100, 116)

    const sent = producer.send({ topic: TOPIC, messages: [{ value: view }] })
    await flushAll()
    expect(admission.reserved).toBe(held({ value: Buffer.alloc(16) }) + 1536)
    slab.fill('z')
    await release()
    await sent

    expect(written[0].messages[0].value).not.toBe(view)
    expect(written[0].bytes.includes(Buffer.alloc(16, 'a'))).toBe(true)
    expect(written[0].bytes.includes(Buffer.alloc(16, 'z'))).toBe(false)
    await producer.disconnect()
  })

  test('small views of one large store in a send are admitted at their held bytes and the send shell exactly', async () => {
    const { producer, admission, written, release } = await heldProducer()
    const slab = Buffer.alloc(1048576, 'a')
    const messages = [
      { key: slab.subarray(0, 4), value: slab.subarray(4, 20) },
      { value: slab.subarray(20, 52), headers: { h: slab.subarray(52, 53) } },
    ]

    const sent = producer.send({ topic: TOPIC, messages })
    await flushAll()
    expect(admission.reserved).toBe(held(messages[0]) + held(messages[1]) + 1536)
    await release()
    await sent

    const [first, second] = written[0].messages
    expect(first.value.buffer).toBe(first.key.buffer)
    expect(second.value.buffer).toBe(first.key.buffer)
    expect(second.headers.h.buffer).toBe(first.key.buffer)
    expect(first.key.buffer.byteLength).toBe(53)
    await producer.disconnect()
  })

  test('a send whose copied views cannot fit in bufferMemory is refused, and nothing is admitted or charged', async () => {
    const { producer, admission, cluster, release } = await heldProducer({ capacity: 2000 })
    const slab = Buffer.alloc(1048576)

    const sent = watch(
      producer.send({
        topic: TOPIC,
        messages: [{ value: slab.subarray(0, 1) }, { value: slab.subarray(1, 2001) }],
      })
    )
    await flushAll()

    expect(sent.state).toBe('rejected')
    expect(sent.value).toBeInstanceOf(KafkaJSRecordTooLargeError)
    expect(sent.value).toMatchObject({ reason: 'bufferMemory', limit: 2000 })
    expect(countersOf(admission)).toEqual(quiescent(admission))
    await release()
    expect(cluster.broker.produce).not.toHaveBeenCalled()
    expect(countersOf(admission)).toEqual(quiescent(admission))
    await producer.disconnect()
  })

  test('a messages array whose iterator yields fresh copies still has its view copied: a one-byte view of a 1 MiB store fits a bufferMemory of 4096', async () => {
    const { producer, admission, written, release } = await heldProducer({ capacity: 4096 })
    const slab = Buffer.alloc(1048576, 'a')
    slab.write('q', 7)
    class Messages extends Array {
      *[Symbol.iterator]() {
        for (let i = 0; i < this.length; i++) {
          yield { ...this[i] }
        }
      }
    }
    const messages = new Messages()
    messages.push({ value: slab.subarray(7, 8) })

    const sent = watch(producer.send({ topic: TOPIC, messages }))
    await flushAll()
    expect(admission.reserved).toBe(held({ value: Buffer.alloc(1) }) + 1536)
    await release()

    expect(sent.state).toBe('fulfilled')
    const [{ messages: sentMessages }] = written
    expect(String(sentMessages[0].value)).toBe('q')
    expect(sentMessages[0].value.buffer.byteLength).toBe(1)
    await producer.disconnect()
  })

  test('a view that shrinks while its send is read refuses the send instead of sending unwritten bytes', async () => {
    const { producer, admission, cluster, release } = await heldProducer()
    const backing = new ArrayBuffer(1048576, { maxByteLength: 1048576 })
    const view = new Uint8Array(backing, 1048576 - 10)
    Object.setPrototypeOf(view, Buffer.prototype)
    let reads = 0
    const later = {
      get value() {
        if (++reads === 1) {
          backing.resize(1048576 - 5)
        }
        return 'x'
      },
    }

    const sent = watch(producer.send({ topic: TOPIC, messages: [{ value: view }, later] }))
    await flushAll()

    expect(reads).toBe(1)
    expect(sent.state).toBe('rejected')
    expect(sent.value).toBeInstanceOf(KafkaJSNonRetriableError)
    expect(sent.value.message).toBe('A Buffer changed size while its send was being read')
    expect(countersOf(admission)).toEqual(quiescent(admission))
    await release()
    expect(cluster.broker.produce).not.toHaveBeenCalled()

    const next = watch(producer.send({ topic: TOPIC, messages: [{ value: 'ok' }] }))
    await flushAll()
    expect(next.state).toBe('fulfilled')
    expect(cluster.broker.produce).toHaveBeenCalledTimes(1)
    expect(countersOf(admission)).toEqual(quiescent(admission))
    await producer.disconnect()
  })

  test('a one-byte Buffer from a 64 KiB pool fits a bufferMemory of 4096', async () => {
    const { producer, admission, release } = await heldProducer({ capacity: 4096 })
    const value = withPool(65536, () => Buffer.from('v'))
    expect(value.buffer.byteLength).toBeGreaterThanOrEqual(65536)

    const sent = watch(producer.send({ topic: TOPIC, messages: [{ value }] }))
    await flushAll()
    expect(admission.reserved).toBe(held({ value }) + 1536)
    await release()

    expect(sent.state).toBe('fulfilled')
    await producer.disconnect()
  })

  test('each field the producer reads is read once, through own and inherited accessors alike, and that value is checked, charged and sent', async () => {
    const { producer, admission, written, release } = await heldProducer()
    const reads = { key: 0, value: 0, headers: 0, timestamp: 0, header: 0 }
    const values = ['first', undefined, 'third']
    class Message {
      get value() {
        return values[reads.value++]
      }
    }
    const headers = {
      get trace() {
        reads.header += 1
        return reads.header === 1 ? 'one' : 'two'
      },
    }
    const message = Object.defineProperties(new Message(), {
      key: {
        enumerable: true,
        get: () => {
          reads.key += 1
          return reads.key === 1 ? 'key-1' : 'key-2'
        },
      },
      headers: {
        enumerable: false,
        get: () => {
          reads.headers += 1
          return headers
        },
      },
      timestamp: {
        enumerable: true,
        get: () => {
          reads.timestamp += 1
          return reads.timestamp === 1 ? 1700000000777 : -1
        },
      },
    })

    const sent = watch(producer.send({ topic: TOPIC, messages: [message] }))
    await flushAll()
    expect(admission.reserved).toBe(
      chargeOf(
        [{ topic: TOPIC, messages: [{ key: 'key-1', value: 'first', headers: { trace: 'one' } }] }],
        admission.capacity
      ).total
    )
    await release()

    expect(sent.state).toBe('fulfilled')
    expect(reads).toEqual({ key: 1, value: 1, headers: 1, timestamp: 1, header: 1 })
    expect(written[0].messages[0]).toEqual({
      key: 'key-1',
      value: 'first',
      headers: { trace: 'one' },
      timestamp: 1700000000777,
    })
    expect(firstTimestampOf(written[0].bytes)).toBe(1700000000777n)
    await producer.disconnect()
  })

  test.each([
    [
      'a Long whose value cannot be read',
      () => {
        const timestamp = Long.fromValue(1700000000000n)
        const reads = { count: 0 }
        Object.defineProperty(timestamp, 'value', {
          get: () => {
            reads.count += 1
            throw new Error('the value cannot be read')
          },
        })
        return { timestamp, reads }
      },
    ],
    [
      'a Date whose time cannot be read',
      () => ({ timestamp: Object.create(Date.prototype), reads: { count: 1 } }),
    ],
  ])(
    '%s is refused with the invalid-input error it always had, read once and charged nothing',
    async (_, make) => {
      const { producer, admission } = await heldProducer()
      const { timestamp, reads } = make()

      const error = await producer
        .send({ topic: TOPIC, messages: [{ value: 'v', timestamp }] })
        .catch((e) => e)

      expect(error).toBeInstanceOf(KafkaJSInvalidConfigurationError)
      expect(error.message).toBe(
        `Invalid message for topic "${TOPIC}": its timestamp must be an integer from 0 to 2^63 - 1: ` +
          'a number, a numeric string, a Date, a BigInt or a Long'
      )
      expect(reads.count).toBe(1)
      expect(countersOf(admission)).toEqual(quiescent(admission))
      await producer.disconnect()
    }
  )

  test('missing-value validation uses the value read once into the copy', async () => {
    const { producer, admission } = await heldProducer()
    const reads = { count: 0 }
    const record = { key: 'k' }
    Object.defineProperty(record, 'value', {
      enumerable: true,
      get: () => {
        reads.count += 1
        return reads.count === 1 ? undefined : 'a later read'
      },
    })

    const error = await producer.send({ topic: TOPIC, messages: [record] }).catch((e) => e)

    expect(reads.count).toBe(1)
    expect(error).toBeInstanceOf(KafkaJSNonRetriableError)
    expect(error.message).toBe(`Invalid message without value for topic "${TOPIC}" at index 0`)
    expect(admission.reserved).toBe(0)
    await producer.disconnect()
  })

  test('a record without a value is named by topic and index, never by its contents', async () => {
    const { producer, admission } = await heldProducer()
    const marker = 'secret-marker-4d1f'

    const error = await producer
      .send({
        topic: TOPIC,
        messages: [
          { value: 'ok' },
          { key: `key-${marker}`, headers: { h: `header-${marker}` }, extra: `extra-${marker}` },
        ],
      })
      .catch((e) => e)

    expect(error).toBeInstanceOf(KafkaJSNonRetriableError)
    expect(error.message).toBe(`Invalid message without value for topic "${TOPIC}" at index 1`)
    for (const text of [error.message, error.stack, JSON.stringify(error)]) {
      expect(text).not.toContain(marker)
    }
    expect(admission.reserved).toBe(0)
    await producer.disconnect()
  })

  test('a string record without a value is not echoed', async () => {
    const { producer, admission } = await heldProducer()
    const marker = 'secret-marker-9b2e'

    const error = await producer.send({ topic: TOPIC, messages: [marker] }).catch((e) => e)

    expect(error).toBeInstanceOf(KafkaJSNonRetriableError)
    expect(error.message).toBe(`Invalid message without value for topic "${TOPIC}" at index 0`)
    for (const text of [error.message, error.stack, JSON.stringify(error)]) {
      expect(text).not.toContain(marker)
    }
    expect(admission.reserved).toBe(0)
    await producer.disconnect()
  })

  test('an entry, a list or a record that is not an object is kept as it is, so validation refuses it as before', async () => {
    const { producer, admission } = await heldProducer()

    await expect(producer.send({ topic: TOPIC, messages: ['text'] })).rejects.toThrow(
      `Invalid message without value for topic "${TOPIC}" at index 0`
    )
    await expect(producer.send({ topic: TOPIC, messages: {} })).rejects.toThrow(TypeError)
    await expect(producer.sendBatch({ topicMessages: [null] })).rejects.toThrow(TypeError)
    expect(admission.reserved).toBe(0)
    await producer.disconnect()
  })

  test('a Buffer key is kept as a Buffer value is, and a timestamp object other than a Date or a Long is left to the encoder', async () => {
    const { producer, written, release } = await heldProducer()
    const key = Buffer.alloc(4, 'k')
    const timestamp = { valueOf: () => 1700000000999 }

    const sent = producer.send({ topic: TOPIC, messages: [{ key, value: 'v', timestamp }] })
    await release()
    await sent

    expect(written[0].messages[0].key).toBe(key)
    expect(written[0].messages[0].timestamp).toBe(timestamp)
    expect(firstTimestampOf(written[0].bytes)).toBe(1700000000999n)
    await producer.disconnect()
  })

  test('an error that names a failing record names the copy: equal in its fields, not the same object', async () => {
    const { producer, cluster } = bufferedProducer()
    cluster.broker.produce.mockImplementation(async () => {
      const error = createErrorFromCode(87)
      Object.assign(error, { topic: TOPIC, partition: 0, recordErrors: [{ batchIndex: 0 }] })
      return [{ topicName: TOPIC, partition: 0, errorCode: 87, error, baseOffset: '-1' }]
    })
    await producer.connect()
    const record = { key: 'k', value: 'refused' }

    const sent = watch(producer.send({ topic: TOPIC, messages: [record] }))
    await flushAll()
    const error = sent.value

    expect(sent.state).toBe('rejected')
    expect(error.recordErrors[0].record).toEqual(record)
    expect(error.recordErrors[0].record).not.toBe(record)
    await producer.disconnect()
  })

  test("a custom partitioner sees the record's own fields", async () => {
    const seen = []
    const { producer, release } = await heldProducer({
      createPartitioner:
        () =>
        ({ message }) => {
          seen.push(message)
          return 0
        },
    })

    const sent = producer.send({ topic: TOPIC, messages: [{ value: 'v', routing: 'eu-1' }] })
    await release()
    await sent

    expect(seen).toEqual([{ value: 'v', routing: 'eu-1' }])
    await producer.disconnect()
  })
})

describe('Producer > the buffer options', () => {
  const kafka = new Kafka({ brokers: ['localhost:9092'], logLevel: 0 })

  test.each([
    ['bufferMemory', 1],
    ['bufferMemory', Number.MAX_SAFE_INTEGER],
    ['maxBlockMs', 0],
    ['maxBlockMs', 2147483647],
  ])('%s %p is accepted', (option, value) => {
    expect(() => kafka.producer({ [option]: value })).not.toThrow()
  })

  test.each([
    ['bufferMemory', 0],
    ['bufferMemory', 1.5],
    ['bufferMemory', 2 ** 53],
    ['bufferMemory', '1024'],
    ['bufferMemory', null],
    ['maxBlockMs', -1],
    ['maxBlockMs', 2147483648],
    ['maxBlockMs', 0.5],
    ['maxBlockMs', NaN],
  ])('%s %p is refused, naming the option', (option, value) => {
    expect(() => kafka.producer({ [option]: value })).toThrow(KafkaJSInvalidConfigurationError)
    expect(() => kafka.producer({ [option]: value })).toThrow(new RegExp(`^${option} must be`))
  })
})
