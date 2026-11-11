const Decoder = require('./decoder')
const { KafkaJSPartialMessageError } = require('../errors')

const MAX_COUNT = 1000000
const MAX_TAGS = 10000
const MAX_PREALLOCATION = 1000
const BIG_TIMEOUT = 60000

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

const compactPrefix = (count) => uvarint(count + 1)

const READERS = [
  { name: 'readArray', prefix: int32, nullBytes: int32(-1), nullResult: [] },
  { name: 'readUVarIntArray', prefix: compactPrefix, nullBytes: uvarint(0), nullResult: null },
  { name: 'readArrayAsync', prefix: int32, nullBytes: int32(-1), nullResult: [] },
  {
    name: 'readUVarIntArrayAsync',
    prefix: compactPrefix,
    nullBytes: uvarint(0),
    nullResult: null,
  },
]

/** @returns {{ calls: () => number, read: (decoder: Decoder) => number }} a one-byte element reader */
const countingReader = () => {
  let calls = 0
  return {
    calls: () => calls,
    read: (decoder) => {
      calls++
      return decoder.readInt8()
    },
  }
}

/** @returns {Buffer} the count prefix followed by one backing byte per entry, so ensureReadable passes */
const bodyOf = (reader, count) => Buffer.concat([reader.prefix(count), Buffer.alloc(count)])

/**
 * Replaces a global constructor with a forwarding Proxy while `run` executes synchronously.
 * @param {'Array'|'Map'} name the global to observe
 * @param {() => *} run
 * @returns {{ outcome: *, constructions: number[][] }} what `run` returned and each construct call's arguments
 */
const observeConstruction = (name, run) => {
  const original = global[name]
  const constructions = []
  global[name] = new Proxy(original, {
    construct(target, args, newTarget) {
      constructions.push(args)
      return Reflect.construct(target, args, newTarget)
    },
  })
  try {
    return { outcome: run(), constructions }
  } finally {
    global[name] = original
  }
}

/** @param {number[][]} constructions @returns {number} the largest numeric capacity a construct call advertised */
const largestCapacity = (constructions) =>
  constructions.reduce(
    (largest, args) => (typeof args[0] === 'number' ? Math.max(largest, args[0]) : largest),
    0
  )

/** @param {() => *} call @returns {Promise<*>} what `call` returned or threw, as a promise */
const settle = (call) => {
  try {
    return Promise.resolve(call())
  } catch (error) {
    return Promise.reject(error)
  }
}

/** @param {() => *} run @returns {Promise<*>} the error `run` threw or rejected with */
const failureOf = async (run) => {
  try {
    await run()
  } catch (error) {
    return error
  }
  throw new Error('expected the decode to fail, but it returned')
}

const expectNonRetriableLimit = (error, count) => {
  expect(error).toBeInstanceOf(KafkaJSPartialMessageError)
  expect(error.retriable).toBe(false)
  expect(error.message).toContain(String(count))
}

/** @returns {Buffer} a tag section of `count` triples: tag = index * spacing + first, one value byte each */
const tagSection = (count, { spacing = 1, first = 0, valueFor = (i) => [i & 0xff] } = {}) =>
  Buffer.concat([
    uvarint(count),
    ...Array.from({ length: count }, (_, i) => {
      const value = Buffer.from(valueFor(i))
      return Buffer.concat([uvarint(i * spacing + first), uvarint(value.length), value])
    }),
  ])

describe('Decoder count bounds', () => {
  for (const reader of READERS.filter(({ name }) => name.endsWith('Async'))) {
    it(`${reader.name} awaits each element in wire order across suspension`, async () => {
      const decoder = new Decoder(Buffer.concat([reader.prefix(3), Buffer.from([11, 22, 33])]))
      const events = []
      const element = async (input) => {
        events.push(`start:${input.offset}`)
        await Promise.resolve()
        const value = input.readInt8()
        events.push(`end:${value}`)
        return value * 2
      }
      const start = reader.prefix(3).length

      const result = await decoder[reader.name](element)

      expect(result).toEqual([22, 44, 66])
      expect(events).toEqual([
        `start:${start}`,
        'end:11',
        `start:${start + 1}`,
        'end:22',
        `start:${start + 2}`,
        'end:33',
      ])
      expect(decoder.offset).toBe(decoder.buffer.length)
    })

    it(`${reader.name} propagates an element failure without reading later elements`, async () => {
      for (const asynchronous of [false, true]) {
        const decoder = new Decoder(Buffer.concat([reader.prefix(3), Buffer.from([11, 22, 33])]))
        const sentinel = new Error('second element failed')
        const values = []
        const element = (input) => {
          const value = input.readInt8()
          values.push(value)
          if (value === 22) {
            if (asynchronous) return Promise.reject(sentinel)
            throw sentinel
          }
          return Promise.resolve(value)
        }

        await expect(decoder[reader.name](element)).rejects.toBe(sentinel)

        expect(values).toEqual([11, 22])
        expect(decoder.offset).toBe(reader.prefix(3).length + 2)
      }
    })
  }

  it('readArrayAsync rejects negative counts other than -1 before reading an element', async () => {
    const element = jest.fn()

    for (const count of [-2, -2147483648]) {
      await expect(new Decoder(int32(count)).readArrayAsync(element)).rejects.toBeInstanceOf(
        KafkaJSPartialMessageError
      )
    }

    expect(element).not.toHaveBeenCalled()
  })

  for (const reader of READERS) {
    it(
      `${reader.name} rejects ${MAX_COUNT + 1} entries before reading an element`,
      async () => {
        const element = countingReader()
        const decoder = new Decoder(bodyOf(reader, MAX_COUNT + 1))

        const error = await failureOf(() => decoder[reader.name](element.read))

        expectNonRetriableLimit(error, MAX_COUNT + 1)
        expect(element.calls()).toBe(0)
      },
      BIG_TIMEOUT
    )

    it(
      `${reader.name} accepts ${MAX_COUNT} entries`,
      async () => {
        const element = countingReader()
        const decoder = new Decoder(bodyOf(reader, MAX_COUNT))

        const result = await decoder[reader.name](element.read)

        expect(result).toHaveLength(MAX_COUNT)
        expect(element.calls()).toBe(MAX_COUNT)
        expect(decoder.offset).toBe(decoder.buffer.length)
      },
      BIG_TIMEOUT
    )

    it(`${reader.name} allocates at most ${MAX_PREALLOCATION} slots before the first element fails`, async () => {
      const sentinel = new Error('first element failed')
      const decoder = new Decoder(bodyOf(reader, 900000))
      const failing = () => {
        throw sentinel
      }

      const { outcome, constructions } = observeConstruction('Array', () =>
        settle(() => decoder[reader.name](failing))
      )

      await expect(Promise.resolve(outcome)).rejects.toBe(sentinel)
      expect(largestCapacity(constructions)).toBeLessThanOrEqual(MAX_PREALLOCATION)
    }, 20000)
  }

  it('accepts 10000 ordered tags', () => {
    const body = tagSection(MAX_TAGS)
    const decoder = new Decoder(body)

    const tags = decoder.readTaggedFields()

    expect(tags.size).toBe(MAX_TAGS)
    expect([...tags.keys()]).toEqual(Array.from({ length: MAX_TAGS }, (_, i) => i))
    expect(tags.get(9999)).toEqual(Buffer.from([9999 & 0xff]))
    expect(decoder.offset).toBe(body.length)
  })

  it('rejects 10001 tags before allocating the tag map', () => {
    const decoder = new Decoder(tagSection(MAX_TAGS + 1))

    const { outcome, constructions } = observeConstruction('Map', () => {
      try {
        return decoder.readTaggedFields()
      } catch (error) {
        return error
      }
    })

    expectNonRetriableLimit(outcome, MAX_TAGS + 1)
    expect(constructions).toEqual([])
  })

  it('preserves null empty and truncated array behaviour', async () => {
    for (const reader of READERS) {
      const element = countingReader()

      const nullArray = await new Decoder(reader.nullBytes)[reader.name](element.read)
      expect(nullArray).toEqual(reader.nullResult)

      const emptyArray = await new Decoder(reader.prefix(0))[reader.name](element.read)
      expect(emptyArray).toEqual([])

      const truncated = new Decoder(Buffer.concat([reader.prefix(3), Buffer.alloc(2)]))
      const error = await failureOf(() => truncated[reader.name](element.read))
      expect(error).toBeInstanceOf(KafkaJSPartialMessageError)
      expect(error.message).toContain('remain')
      expect(element.calls()).toBe(0)
    }
  })

  it('preserves ordered unknown tags at the limit', () => {
    const body = tagSection(MAX_TAGS, {
      spacing: 3,
      first: 2,
      valueFor: (i) => Array.from({ length: i % 5 }, (_, at) => (i + at) & 0xff),
    })
    const decoder = new Decoder(body)

    const tags = decoder.readTaggedFields()

    expect([...tags.keys()]).toEqual(Array.from({ length: MAX_TAGS }, (_, i) => i * 3 + 2))
    for (const i of [0, 1, 4, 5, 127, 128, 9999]) {
      const expected = Buffer.from(Array.from({ length: i % 5 }, (_, at) => (i + at) & 0xff))
      expect(tags.get(i * 3 + 2)).toEqual(expected)
    }
    expect(decoder.offset).toBe(body.length)
  })
})
