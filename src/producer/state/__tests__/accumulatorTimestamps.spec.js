const Accumulator = require('../accumulator')
const Long = require('../../../utils/long')
const { KafkaJSInvalidConfigurationError } = require('../../../errors')

const TOPIC = 'topic-a'
const INT64_MAX = 2n ** 63n - 1n
/** The largest number below 2^63: a double cannot hold 2^63 - 1. */
const LARGEST_DOUBLE_BELOW_INT64 = 2 ** 63 - 1024
/** The largest instant a Date can hold, in milliseconds. */
const LATEST_DATE_MS = 8.64e15

/**
 * An accumulator that sends at once over a send path that settles every batch and records the
 * records it was handed.
 */
const createAccumulator = () => {
  const partition = jest.fn(async (topicMessages) =>
    topicMessages.map(({ topic, messages }) => ({ topic, partition: 0, messages }))
  )
  const sendPartitioned = jest.fn(async ({ batches }) =>
    batches.map(({ topic, partition: id, settlers }) => {
      const metadata = { topicName: topic, partition: id, errorCode: 0, baseOffset: '0' }
      settlers.forEach((settler) => settler.resolve(metadata))
      return metadata
    })
  )
  const accumulator = new Accumulator({ linger: 0, batchSize: 16384, partition, sendPartitioned })
  /** The values of the records of every batch handed on, in order. */
  const handedOn = () =>
    sendPartitioned.mock.calls.flatMap(([{ batches }]) =>
      batches.flatMap(({ messages }) => messages.map(({ value }) => value))
    )
  return { accumulator, partition, sendPartitioned, handedOn }
}

const append = (accumulator, messages) =>
  accumulator.append({
    acks: -1,
    timeout: 30000,
    compression: 0,
    topicMessages: [{ topic: TOPIC, messages }],
  })

const ACCEPTED = [
  ['the number 1', 1],
  ['the send time as a number', 1700000000000],
  ['the number 2^53', 2 ** 53],
  ['the largest number below 2^63', LARGEST_DOUBLE_BELOW_INT64],
  ['the string 0, the literal epoch', '0'],
  ['a numeric string of the int64 maximum', INT64_MAX.toString()],
  ['the BigInt 0', 0n],
  ['the BigInt int64 maximum', INT64_MAX],
  ['a Long of 0', Long.fromNumber(0)],
  ['a Long of the int64 maximum', Long.fromValue(INT64_MAX)],
  ['the Date of the epoch', new Date(0)],
  ['the latest Date', new Date(LATEST_DATE_MS)],
  ['true, as before', true],
]

const REJECTED = [
  ['the number -1', -1],
  ['the number 2^63', 2 ** 63],
  ['the number -2^63', -(2 ** 63)],
  ['the number 1e20, which wraps to 7766279631452241920', 1e20],
  ['the largest number', Number.MAX_VALUE],
  ['the string -1', '-1'],
  ['the string 2^63', (2n ** 63n).toString()],
  ['the string -2^63', (-(2n ** 63n)).toString()],
  ['the string 2^64, which wraps to 0', (2n ** 64n).toString()],
  ['a numeric string of 21 digits', '1'.repeat(21)],
  ['the BigInt -1', -1n],
  ['the BigInt 2^63', 2n ** 63n],
  ['the BigInt -2^63', -(2n ** 63n)],
  ['the BigInt 2^64 + 5, which wraps to 5', 2n ** 64n + 5n],
  ['the BigInt 1e20', 10n ** 20n],
  ['a Long of -1', Long.fromValue(-1n)],
  ['a Long of 2^63', Long.fromValue(2n ** 63n)],
  ['a Long of 2^64 + 5', Long.fromValue(2n ** 64n + 5n)],
  ['the Date one millisecond before the epoch', new Date(-1)],
]

describe('Producer > the accumulator > timestamp range', () => {
  describe.each(ACCEPTED)('%s', (_, timestamp) => {
    test('joins a batch and the send resolves', async () => {
      const { accumulator, handedOn } = createAccumulator()

      await expect(append(accumulator, [{ value: 'v', timestamp }])).resolves.toHaveLength(1)

      expect(handedOn()).toEqual(['v'])
    })
  })

  describe.each(REJECTED)('%s', (_, timestamp) => {
    test('fails the send before the record joins a batch, naming the field', async () => {
      const { accumulator, partition, sendPartitioned } = createAccumulator()

      const error = await append(accumulator, [{ value: 'v', timestamp }]).catch((e) => e)

      expect(error).toBeInstanceOf(KafkaJSInvalidConfigurationError)
      expect(error.message).toBe(
        `Invalid message for topic "${TOPIC}": its timestamp must be an integer from 0 to 2^63 - 1: a number, a numeric string, a Date, a BigInt or a Long`
      )
      expect(partition).not.toHaveBeenCalled()
      expect(sendPartitioned).not.toHaveBeenCalled()
    })
  })

  test('the batch of the int64 extremes, which used to be written as a corrupt record, cannot be built', async () => {
    const { accumulator, partition } = createAccumulator()

    await expect(
      append(accumulator, [
        { value: 'low', timestamp: -(2n ** 63n) },
        { value: 'high', timestamp: INT64_MAX },
      ])
    ).rejects.toBeInstanceOf(KafkaJSInvalidConfigurationError)

    expect(partition).not.toHaveBeenCalled()
  })

  test('a rejected record takes its whole send with it, and the next send is unharmed', async () => {
    const { accumulator, handedOn } = createAccumulator()

    await expect(
      append(accumulator, [
        { value: 'good', timestamp: 1 },
        { value: 'bad', timestamp: -1 },
      ])
    ).rejects.toBeInstanceOf(KafkaJSInvalidConfigurationError)
    await expect(append(accumulator, [{ value: 'later', timestamp: 2 }])).resolves.toHaveLength(1)

    expect(handedOn()).toEqual(['later'])
  })

  test.each([[0], [0n], [null], [undefined]])(
    'a timestamp of %p still means the send time and is not range-checked away',
    async (timestamp) => {
      const { accumulator, handedOn } = createAccumulator()

      await expect(append(accumulator, [{ value: 'v', timestamp }])).resolves.toHaveLength(1)

      expect(handedOn()).toEqual(['v'])
    }
  )
})
