const Decoder = require('../decoder')
const { ShareDecoder } = require('./shareFetch/codec')
const { KafkaJSPartialMessageError } = require('../../errors')

const MAX_COUNT = 1000000
const MAX_PREALLOCATION = 1000
const BIG_TIMEOUT = 60000
const INVALID_LENGTH = -2

/** @param {number} value @returns {Buffer} the value as a big-endian int32 */
const int32 = (value) => {
  const buffer = Buffer.alloc(4)
  buffer.writeInt32BE(value)
  return buffer
}

/** @param {number} value @returns {Buffer} the value as an unsigned varint */
const uvarint = (value) => {
  const bytes = []
  let rest = value
  while (rest > 127) {
    bytes.push((rest % 128) + 128)
    rest = Math.floor(rest / 128)
  }
  bytes.push(rest)
  return Buffer.from(bytes)
}

const ZERO = (length) => Buffer.alloc(length)

/**
 * One row per codec. Every smallest valid element of these schemas is all zero bytes (empty
 * strings, empty arrays, zero numbers), so a body is a head, a count, zero filler and a tail.
 *
 * - `head`/`tail`: bytes before the top-level array count and after its elements
 * - `headStrings`: `readString` calls made by the head
 * - `element`: bytes of the smallest valid top-level element
 * - `malformAt`: offset in the first element of an array count that is overwritten with an invalid length
 * - `nullable`: whether the top-level array may be null on the wire
 * - `nested`: each nested array, by schema field `name`, as the bytes of one top-level element
 *   before its count (`before`), the bytes of the nested array's own smallest element (`element`),
 *   the bytes after the nested array in the element (`after`), and the `readString` calls the
 *   element makes before the count (`strings`)
 */
const CODECS = [
  {
    api: 'describeDelegationToken',
    version: 1,
    head: ZERO(2),
    headStrings: 0,
    tail: ZERO(4),
    element: 38,
    malformAt: 34,
    nullable: false,
    nested: [{ name: 'renewers', before: 34, element: 4, after: 0, strings: 3 }],
  },
  {
    api: 'describeClientQuotas',
    version: 0,
    head: ZERO(8),
    headStrings: 1,
    tail: ZERO(0),
    element: 8,
    malformAt: 0,
    nullable: true,
    nested: [
      { name: 'entity', before: 0, element: 4, after: 4, strings: 0 },
      { name: 'values', before: 4, element: 10, after: 0, strings: 0 },
    ],
  },
  {
    api: 'alterClientQuotas',
    version: 0,
    head: ZERO(4),
    headStrings: 0,
    tail: ZERO(0),
    element: 8,
    malformAt: 4,
    nullable: false,
    nested: [{ name: 'entity', before: 4, element: 4, after: 0, strings: 1 }],
  },
  {
    api: 'electLeaders',
    version: 0,
    head: ZERO(4),
    headStrings: 0,
    tail: ZERO(0),
    element: 6,
    malformAt: 2,
    nullable: false,
    nested: [{ name: 'partitionResult', before: 2, element: 8, after: 0, strings: 1 }],
  },
  {
    api: 'electLeaders',
    version: 1,
    head: ZERO(6),
    headStrings: 0,
    tail: ZERO(0),
    element: 6,
    malformAt: 2,
    nullable: false,
    nested: [{ name: 'partitionResult', before: 2, element: 8, after: 0, strings: 1 }],
  },
]

/**
 * @param {object} codec a CODECS row
 * @param {number} count the top-level array count to write
 * @param {Buffer} elements the bytes that follow the count
 * @returns {Buffer} a complete response body
 */
const bodyOf = (codec, count, elements) =>
  Buffer.concat([codec.head, int32(count), elements, codec.tail])

/**
 * A body whose top-level array holds one valid element whose nested array claims `count` entries.
 * @param {object} codec a CODECS row
 * @param {object} nested one of the row's nested arrays
 * @param {number} count the nested array count to write
 * @returns {Buffer}
 */
const nestedBodyOf = (codec, nested, count) => {
  const first = Buffer.concat([
    ZERO(nested.before),
    int32(count),
    ZERO(count * nested.element + nested.after),
  ])
  return bodyOf(codec, 1, first)
}

/**
 * Counts `readString` calls while `run` executes; each codec's elements read strings, so the
 * count says how many elements and prefixes were read before the decode ended.
 * @param {() => *} run
 * @returns {{ outcome: *, strings: number }}
 */
const countStrings = (run) => {
  const original = Decoder.prototype.readString
  let strings = 0
  Decoder.prototype.readString = function readString() {
    strings++
    return original.apply(this, arguments)
  }
  try {
    return { outcome: run(), strings }
  } finally {
    Decoder.prototype.readString = original
  }
}

/** @param {() => Promise<*>} call @returns {Promise<Error>} the error the decode rejected with */
const failureOf = async (call) => {
  try {
    await call()
  } catch (error) {
    return error
  }
  throw new Error('expected the decode to fail, but it returned')
}

/** @param {() => *} call @returns {Promise<*>} the promise, with a synchronous throw turned into a rejection */
const settle = (call) => {
  try {
    return Promise.resolve(call())
  } catch (error) {
    return Promise.reject(error)
  }
}

/**
 * Replaces the global Array with a forwarding Proxy while `run` executes synchronously.
 * @param {() => *} run
 * @returns {{ outcome: *, largest: number }} what `run` returned and the largest capacity a construct call advertised
 */
const observeArrayCapacity = (run) => {
  const original = global.Array
  let largest = 0
  global.Array = new Proxy(original, {
    construct(target, args, newTarget) {
      if (typeof args[0] === 'number') largest = Math.max(largest, args[0])
      return Reflect.construct(target, args, newTarget)
    },
  })
  try {
    return { outcome: run(), largest }
  } finally {
    global.Array = original
  }
}

const expectNonRetriableLimit = (error, count) => {
  expect(error).toBeInstanceOf(KafkaJSPartialMessageError)
  expect(error.retriable).toBe(false)
  expect(error.message).toContain(String(count))
}

describe('Codec-local count bounds', () => {
  for (const codec of CODECS) {
    const { decode } = require(`./${codec.api}/v${codec.version}/response`)
    const label = `${codec.api} v${codec.version}`

    it(
      `${label} response rejects ${MAX_COUNT + 1} entries before reading an element`,
      async () => {
        const body = bodyOf(codec, MAX_COUNT + 1, ZERO((MAX_COUNT + 1) * codec.element))

        const { outcome, strings } = countStrings(() => settle(() => decode(body)))
        const error = await failureOf(() => outcome)

        expectNonRetriableLimit(error, MAX_COUNT + 1)
        expect(strings).toBe(codec.headStrings)
      },
      BIG_TIMEOUT
    )

    for (const nested of codec.nested) {
      it(
        `${label} response rejects ${MAX_COUNT + 1} nested ${nested.name} entries before reading an element`,
        async () => {
          const body = nestedBodyOf(codec, nested, MAX_COUNT + 1)

          const { outcome, strings } = countStrings(() => settle(() => decode(body)))
          const error = await failureOf(() => outcome)

          expectNonRetriableLimit(error, MAX_COUNT + 1)
          expect(strings).toBe(codec.headStrings + nested.strings)
        },
        BIG_TIMEOUT
      )

      it(`${label} response preserves truncated nested ${nested.name} array behaviour`, async () => {
        const body = Buffer.concat([codec.head, int32(1), ZERO(nested.before), int32(3), ZERO(2)])

        const error = await failureOf(() => decode(body))

        expect(error).toBeInstanceOf(KafkaJSPartialMessageError)
        expect(error.message).toContain('remain')
      })
    }

    it(
      `${label} response accepts ${MAX_COUNT} entries`,
      async () => {
        const body = bodyOf(codec, MAX_COUNT, ZERO(MAX_COUNT * codec.element))

        const decoded = await decode(body)
        const entries = decoded.tokens ?? decoded.entries ?? decoded.replicaElectionResults

        expect(entries).toHaveLength(MAX_COUNT)
      },
      BIG_TIMEOUT
    )

    it(`${label} response allocates at most ${MAX_PREALLOCATION} slots before the first element fails`, async () => {
      const first = ZERO(codec.element)
      first.writeInt32BE(INVALID_LENGTH, codec.malformAt)
      const rest = ZERO(900000 * codec.element - codec.element)
      const body = bodyOf(codec, 900000, Buffer.concat([first, rest]))

      const { outcome, largest } = observeArrayCapacity(() => settle(() => decode(body)))
      const error = await failureOf(() => outcome)

      expect(error).toBeInstanceOf(KafkaJSPartialMessageError)
      expect(largest).toBeLessThanOrEqual(MAX_PREALLOCATION)
    }, 30000)

    it(`${label} response preserves null and truncated array behaviour`, async () => {
      const nullBody = bodyOf(codec, -1, ZERO(0))
      if (codec.nullable) {
        expect((await decode(nullBody)).entries).toBeNull()
      } else {
        const error = await failureOf(() => decode(nullBody))
        expect(error).toBeInstanceOf(KafkaJSPartialMessageError)
      }

      const negative = await failureOf(() => decode(bodyOf(codec, INVALID_LENGTH, ZERO(0))))
      expect(negative).toBeInstanceOf(KafkaJSPartialMessageError)

      const truncated = await failureOf(() =>
        decode(Buffer.concat([codec.head, int32(3), ZERO(2)]))
      )
      expect(truncated).toBeInstanceOf(KafkaJSPartialMessageError)
      expect(truncated.message).toContain('remain')
    })
  }

  it('ShareDecoder rejects 1000001 compact entries before reading an element', () => {
    let calls = 0
    const decoder = new ShareDecoder(Buffer.concat([uvarint(MAX_COUNT + 2), ZERO(MAX_COUNT + 1)]))

    let error
    try {
      decoder.readCompactArray((d) => {
        calls++
        return d.readInt8()
      })
    } catch (e) {
      error = e
    }

    expectNonRetriableLimit(error, MAX_COUNT + 1)
    expect(calls).toBe(0)
  })
})
