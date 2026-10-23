const UniformStickyPartitioner = require('./index')
const Partitioners = require('../index')
const { KafkaJSNonRetriableError, KafkaJSInvalidConfigurationError } = require('../../../errors')

/**
 * The keys `default/murmur2.spec.js` pins against the Java client, plus binary, non-ASCII and
 * empty or falsy ones, which are still keys.
 */
const KEYS = [
  '',
  0,
  Buffer.alloc(0),
  '0',
  '1',
  '128',
  '2187',
  '16384',
  '78125',
  '279936',
  '823543',
  '2097152',
  '4782969',
  '10000000',
  '19487171',
  '35831808',
  '62748517',
  '105413504',
  '170859375',
  '100:48069',
  Buffer.from([0x80, 0xff, 0x00, 0x7f, 0x01]),
  'clé',
]

/**
 * @param {...number} leaders one entry per partition, in partition order
 * @returns {{ partitionId: number, leader: number }[]}
 */
const metadata = (...leaders) => leaders.map((leader, partitionId) => ({ partitionId, leader }))

describe('Producer > Partitioner > UniformSticky', () => {
  afterEach(() => jest.restoreAllMocks())

  test('is exported from Partitioners, next to the default', () => {
    expect(Partitioners.UniformStickyPartitioner).toBe(UniformStickyPartitioner)
    expect(Partitioners.DefaultPartitioner).toBeDefined()
  })

  test('places a keyed record exactly where DefaultPartitioner does', () => {
    for (const count of [1, 2, 3, 7, 12, 64, 100]) {
      const partitionMetadata = metadata(...Array(count).fill(0))
      const sticky = UniformStickyPartitioner()()
      const byDefault = Partitioners.DefaultPartitioner()

      for (const key of KEYS) {
        const args = { topic: 'topic', partitionMetadata, message: { key, value: 'value' } }
        expect(sticky(args)).toEqual(byDefault(args))
      }
    }
  })

  test('does not draw a random pick for a keyed record', () => {
    const random = jest.spyOn(Math, 'random')
    const partitioner = UniformStickyPartitioner()()

    partitioner({
      topic: 'topic',
      partitionMetadata: metadata(0, 1, 2),
      message: { key: 'k', value: 'v' },
    })

    expect(random).not.toHaveBeenCalled()
  })

  test('sticks to one partition until stickyBytes are reached, then picks again', () => {
    const random = jest.spyOn(Math, 'random').mockReturnValueOnce(0).mockReturnValueOnce(0.5)
    const partitioner = UniformStickyPartitioner({ stickyBytes: 162 })()
    const send = () =>
      partitioner({
        topic: 'topic',
        partitionMetadata: metadata(0, 1, 2),
        message: { key: null, value: 'x'.repeat(30) },
      })

    // 54 wire bytes a record (21, 1 for the null key, 31 for the value, 1 for no header): 54, 108,
    // 162 - the record that reaches stickyBytes still goes to the sticky partition.
    expect([send(), send(), send()]).toEqual([0, 0, 0])
    expect(random).toHaveBeenCalledTimes(1)
    expect(send()).toEqual(1)
    expect(random).toHaveBeenCalledTimes(2)
  })

  test('one byte short of stickyBytes does not switch', () => {
    jest.spyOn(Math, 'random').mockReturnValueOnce(0).mockReturnValueOnce(0.99)
    const partitioner = UniformStickyPartitioner({ stickyBytes: 163 })()
    const send = () =>
      partitioner({
        topic: 'topic',
        partitionMetadata: metadata(0, 1, 2),
        message: { value: 'x'.repeat(30) },
      })

    expect([send(), send(), send(), send(), send()]).toEqual([0, 0, 0, 0, 2])
  })

  test('counts each record in its wire size: key, value and their lengths, a null or absent one as 1 byte', () => {
    jest.spyOn(Math, 'random').mockReturnValueOnce(0).mockReturnValueOnce(0.99)
    const partitioner = UniformStickyPartitioner({ stickyBytes: 83, ignoreKeys: true })()
    const send = (message) =>
      partitioner({ topic: 'topic', partitionMetadata: metadata(0, 1, 2), message })

    // 24, then 33 (57), then 25 (82): one byte short; the next 25 reach 83 and stay.
    expect(send({ key: null, value: null })).toEqual(0)
    expect(send({ key: Buffer.alloc(4), value: Buffer.alloc(5) })).toEqual(0)
    expect(send({ value: Buffer.alloc(1) })).toEqual(0)
    expect(send({ value: Buffer.alloc(1) })).toEqual(0)
    expect(send({ value: Buffer.alloc(1) })).toEqual(2)
  })

  test('defaults stickyBytes to 16384', () => {
    jest.spyOn(Math, 'random').mockReturnValueOnce(0).mockReturnValueOnce(0.99)
    const partitioner = UniformStickyPartitioner()()
    const send = (value) =>
      partitioner({ topic: 'topic', partitionMetadata: metadata(0, 1, 2), message: { value } })

    // 21 + 1 + 3 + 16357 + 1 = 16383 wire bytes, one short; the next record's 25 reach it.
    expect(send('x'.repeat(16357))).toEqual(0)
    expect(send('x')).toEqual(0)
    expect(send('x')).toEqual(2)
  })

  test('keeps one sticky partition per topic, across calls', () => {
    jest.spyOn(Math, 'random').mockReturnValueOnce(0).mockReturnValueOnce(0.99)
    const partitioner = UniformStickyPartitioner()()
    const partitionMetadata = metadata(0, 1, 2)
    const send = (topic) => partitioner({ topic, partitionMetadata, message: { value: 'x' } })

    expect([send('a'), send('b'), send('a'), send('b')]).toEqual([0, 2, 0, 2])
  })

  test('picks only partitions that have a leader', () => {
    const partitioner = UniformStickyPartitioner({ stickyBytes: 1 })()
    const partitionMetadata = metadata(-1, 1, -1, 3)
    const chosen = new Set()

    for (let i = 0; i < 200; i++) {
      chosen.add(partitioner({ topic: 'topic', partitionMetadata, message: { value: 'x' } }))
    }

    expect([...chosen].sort()).toEqual([1, 3])
  })

  test('falls back to every partition when none has a leader', () => {
    jest.spyOn(Math, 'random').mockReturnValueOnce(0).mockReturnValueOnce(0.99)
    const partitioner = UniformStickyPartitioner({ stickyBytes: 1 })()
    const send = () =>
      partitioner({
        topic: 'topic',
        partitionMetadata: metadata(-1, -1, -1, -1),
        message: { value: 'x' },
      })

    expect([send(), send()]).toEqual([0, 3])
  })

  test('with ignoreKeys, a keyed record follows the sticky rule and its key counts', () => {
    jest.spyOn(Math, 'random').mockReturnValueOnce(0.99).mockReturnValueOnce(0)
    const partitioner = UniformStickyPartitioner({ stickyBytes: 58, ignoreKeys: true })()
    const send = () =>
      partitioner({
        topic: 'topic',
        partitionMetadata: metadata(0, 1, 2),
        message: { key: 'abcd', value: 'x' },
      })

    // 29 wire bytes per record, the key's 5 included: the second reaches 58, the third picks again.
    expect([send(), send(), send()]).toEqual([2, 2, 0])
  })

  test('honours an explicit partition, partition 0 included, and leaves the sticky count alone', () => {
    jest.spyOn(Math, 'random').mockReturnValueOnce(0.5).mockReturnValueOnce(0.99)
    const partitioner = UniformStickyPartitioner({ stickyBytes: 75 })()
    const send = (message) =>
      partitioner({ topic: 'topic', partitionMetadata: metadata(0, 1, 2), message })

    // 25 wire bytes stick to partition 1; the explicit records go where they say, keyed or not, and
    // are not counted: the sticky partition is left only after the third sticky record.
    expect(send({ value: 'x' })).toEqual(1)
    expect(send({ partition: 0, value: 'x'.repeat(50) })).toEqual(0)
    expect(send({ partition: 2, key: 'k', value: 'x'.repeat(50) })).toEqual(2)
    expect(send({ value: 'x' })).toEqual(1)
    expect(send({ value: 'x' })).toEqual(1)
    expect(send({ value: 'x' })).toEqual(2)
  })

  test('picks again when the topic no longer has the sticky partition', () => {
    jest.spyOn(Math, 'random').mockReturnValueOnce(0.99).mockReturnValueOnce(0)
    const partitioner = UniformStickyPartitioner()()
    const send = (partitionMetadata) =>
      partitioner({ topic: 'topic', partitionMetadata, message: { value: 'x' } })

    expect(send(metadata(0, 0, 0, 0))).toEqual(3)
    expect(send(metadata(0, 0, 0))).toEqual(0)
  })

  test.each([
    ['a string', (text) => text],
    ['the same text in a Buffer', (text) => Buffer.from(text)],
  ])('counts %s in the bytes it encodes to, not in characters', (_, toValue) => {
    jest.spyOn(Math, 'random').mockReturnValueOnce(0).mockReturnValueOnce(0.99)
    const partitioner = UniformStickyPartitioner({ stickyBytes: 30 })()
    const send = (value) =>
      partitioner({ topic: 'topic', partitionMetadata: metadata(0, 1, 2), message: { value } })

    // '€€' is 6 bytes but 2 characters: 30 wire bytes, so it reaches stickyBytes on its own.
    expect([send(toValue('€€')), send(toValue('x'))]).toEqual([0, 2])
  })

  test('counts an astral character in its four bytes, in a string and in a Buffer alike', () => {
    jest.spyOn(Math, 'random').mockReturnValueOnce(0).mockReturnValueOnce(0.99)
    const partitioner = UniformStickyPartitioner({ stickyBytes: 56 })()
    const send = (value) =>
      partitioner({ topic: 'topic', partitionMetadata: metadata(0, 1, 2), message: { value } })

    // 28 wire bytes each: the second reaches 56.
    expect([send('\u{1F600}'), send(Buffer.from('\u{1F600}')), send('x')]).toEqual([0, 0, 2])
  })

  test('counts a record’s headers in its wire size', () => {
    jest.spyOn(Math, 'random').mockReturnValueOnce(0).mockReturnValueOnce(0.99)
    const partitioner = UniformStickyPartitioner({ stickyBytes: 29 })()
    const send = (message) =>
      partitioner({ topic: 'topic', partitionMetadata: metadata(0, 1, 2), message })

    // 21 + 1 + 2 + the header's 2 + 2 + its count's 1 = 29
    expect([send({ value: 'x', headers: { h: 'v' } }), send({ value: 'x' })]).toEqual([0, 2])
  })

  test('keeps separate state for each partitioner a factory builds', () => {
    jest.spyOn(Math, 'random').mockReturnValueOnce(0).mockReturnValueOnce(0.99)
    const createPartitioner = UniformStickyPartitioner({ stickyBytes: 75 })
    const [first, second] = [createPartitioner(), createPartitioner()]
    const send = (partitioner) =>
      partitioner({ topic: 'topic', partitionMetadata: metadata(0, 1, 2), message: { value: 'x' } })

    expect([send(first), send(second), send(first), send(second)]).toEqual([0, 2, 0, 2])
  })

  test('works when passed uncalled, as DefaultPartitioner is', () => {
    jest.spyOn(Math, 'random').mockReturnValueOnce(0).mockReturnValueOnce(0.99)
    const createPartitioner = UniformStickyPartitioner
    const [first, second] = [createPartitioner(), createPartitioner()]
    const send = (partitioner, message) =>
      partitioner({ topic: 'topic', partitionMetadata: metadata(0, 1, 2), message })

    expect([send(first, { value: 'x' }), send(first, { value: 'x' })]).toEqual([0, 0])
    expect(send(second, { value: 'x' })).toEqual(2)
    expect(send(first, { key: '1', value: 'x' })).toEqual(
      send(Partitioners.DefaultPartitioner(), { key: '1' })
    )
  })

  test.each([
    ['a Symbol', Symbol('sticky'), 'Symbol(sticky)'],
    ['a string', '16384', "'16384'"],
  ])('refuses %s as KafkaJSNonRetriableError, showing the value', (_name, stickyBytes, shown) => {
    expect(() => UniformStickyPartitioner({ stickyBytes })).toThrow(KafkaJSNonRetriableError)
    expect(() => UniformStickyPartitioner({ stickyBytes })).toThrow(`got ${shown}`)
  })

  test.each([0, -1, 1.5, NaN, '16384'])('rejects stickyBytes %p', (stickyBytes) => {
    expect(() => UniformStickyPartitioner({ stickyBytes })).toThrow(KafkaJSNonRetriableError)
    expect(() => UniformStickyPartitioner({ stickyBytes })).toThrow(
      'stickyBytes must be an integer of at least 1'
    )
  })

  test('records with no bytes move on too: one record of 24 wire bytes at a time, stickyBytes / 24 of them', () => {
    let draws = 0
    jest.spyOn(Math, 'random').mockImplementation(() => (draws++ % 3) / 3)
    const partitioner = UniformStickyPartitioner({ stickyBytes: 240 })()
    const partitionMetadata = metadata(0, 1, 2)

    const chosen = Array.from({ length: 1000 }, () =>
      partitioner({ topic: 'topic', partitionMetadata, message: { value: null } })
    )

    const runs = []
    for (const partition of chosen) {
      if (runs.length > 0 && runs.at(-1).partition === partition) runs.at(-1).length += 1
      else runs.push({ partition, length: 1 })
    }
    expect(runs).toHaveLength(100)
    expect(runs.every(({ length }) => length === 10)).toBe(true)
  })

  test('a sticky partition that lost its leader is left at the next record while another has one, checked once for each partition list', () => {
    const random = jest.spyOn(Math, 'random').mockReturnValueOnce(0).mockReturnValueOnce(0.99)
    const partitioner = UniformStickyPartitioner()()
    const send = (partitionMetadata) =>
      partitioner({ topic: 'topic', partitionMetadata, message: { value: 'x' } })
    const lost = metadata(-1, 1, 2)

    expect(send(metadata(0, 1, 2))).toBe(0)
    expect(send(metadata(0, 1, 2))).toBe(0)
    expect(send(lost)).toBe(2)
    expect(send(lost)).toBe(2)
    expect(random).toHaveBeenCalledTimes(2)
  })

  test('a topic none of whose partitions has a leader gets one partition until stickyBytes, with no throw and no spin', () => {
    const random = jest.spyOn(Math, 'random').mockReturnValue(0.5)
    for (const adaptive of [false, true]) {
      random.mockClear()
      const partitioner = UniformStickyPartitioner({ stickyBytes: 100, adaptive })()
      partitioner.attach({ loadOf: () => 0, rackOf: () => undefined })
      const partitionMetadata = metadata(-1, -1, -1, -1)
      const send = () => partitioner({ topic: 'topic', partitionMetadata, message: { value: 'x' } })

      expect([send(), send(), send(), send(), send()]).toEqual([2, 2, 2, 2, 2])
      expect(random).toHaveBeenCalledTimes(2)
    }
  })

  test('the state of topics idle for longer than 300000 ms is dropped when another topic’s is made while 64 are held', () => {
    let now = 0
    jest.spyOn(Date, 'now').mockImplementation(() => now)
    const random = jest.spyOn(Math, 'random').mockReturnValue(0)
    const partitioner = UniformStickyPartitioner()()
    const partitionMetadata = metadata(0, 1, 2)
    const send = (topic) => partitioner({ topic, partitionMetadata, message: { value: 'x' } })

    for (let i = 0; i < 1000; i += 1) {
      now = i * 5000
      expect(send(`topic-${i}`)).toBe(0)
    }
    random.mockReturnValue(0.99)
    const kept = Array.from({ length: 1000 }, (_, i) => send(`topic-${i}`) === 0)

    expect(kept.filter(Boolean).length).toBeLessThanOrEqual(64)
    expect(kept.filter(Boolean).length).toBeGreaterThan(0)
    expect([kept[0], kept[998], kept[999]]).toEqual([false, true, true])
  })

  test.each([['yes'], [1], [null]])(
    'refuses adaptive %p as KafkaJSInvalidConfigurationError',
    (adaptive) => {
      expect(() => UniformStickyPartitioner({ adaptive })).toThrow(KafkaJSInvalidConfigurationError)
      expect(() => UniformStickyPartitioner({ adaptive })).toThrow('adaptive must be a boolean')
    }
  )

  test('each built instance and the supported uncalled wrapper can be attached', () => {
    const createPartitioner = UniformStickyPartitioner()

    expect(typeof createPartitioner().attach).toBe('function')
    expect(typeof createPartitioner.attach).toBe('function')
  })
})
