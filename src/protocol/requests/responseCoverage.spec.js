const { requireCoverage } = require('./responseCoverage')
const { KafkaJSError } = require('../../errors')

/**
 * @param {() => void} call
 * @returns {Error} what the call threw
 */
const thrownBy = (call) => {
  try {
    call()
  } catch (error) {
    return error
  }
  throw new Error('the call did not throw')
}

/**
 * @param {string|null} topic
 * @param {...number|string} ids
 * @returns {{ topic: string|null, partitions: { partition: number|string }[] }}
 */
const entry = (topic, ...ids) => ({ topic, partitions: ids.map((partition) => ({ partition })) })

const SENT = [entry('t', 0, 1)]

describe('Protocol > response coverage', () => {
  test.each([
    ['an unrequested topic with partitions', [entry('t', 0, 1), entry('other', 0)]],
    ['an unrequested topic with no partitions', [entry('t', 0, 1), entry('other')]],
    ['an unrequested partition', [entry('t', 0, 1, 5)]],
    ['a null topic', [entry(null, 0)]],
    ['a pair beyond its request count', [entry('t', 0, 1, 0)]],
    ['a pair the answer leaves out', [entry('t', 0)]],
    ['an empty answer', []],
  ])('refuses %s with a retriable error naming the API', (_, rows) => {
    const error = thrownBy(() => requireCoverage('Sample', SENT, rows))

    expect(error).toBeInstanceOf(KafkaJSError)
    expect(error.retriable).toBe(true)
    expect(error.message).toMatch(/^Sample response /)
  })

  test.each([
    [
      'an unrequested topic names its first partition',
      [entry('t', 0, 1), entry('other', 4)],
      'Sample response names partition 4 of topic other that the request did not carry',
    ],
    [
      'an unrequested topic with no partitions names the topic',
      [entry('t', 0, 1), entry('other')],
      'Sample response names topic other that the request did not carry',
    ],
    [
      'an unrequested partition',
      [entry('t', 0, 1, 5)],
      'Sample response names partition 5 of topic t that the request did not carry',
    ],
    [
      'a null topic',
      [entry(null, 0)],
      'Sample response names partition 0 of topic null that the request did not carry',
    ],
    [
      'a pair beyond its request count',
      [entry('t', 0, 1, 0)],
      'Sample response names partition 0 of topic t twice',
    ],
    ['a missing pair', [entry('t', 0)], 'Sample response does not name partition 1 of topic t'],
  ])('words the refusal of %s', (_, rows, message) => {
    expect(thrownBy(() => requireCoverage('Sample', SENT, rows)).message).toBe(message)
  })

  test('accepts rows in another order, a pair sent twice and answered twice, string partition ids in the request, and a request topic split into two entries answered as one', () => {
    expect(requireCoverage('Sample', SENT, [entry('t', 1, 0)])).toBeUndefined()
    expect(requireCoverage('Sample', [entry('t', 0, 0)], [entry('t', 0, 0)])).toBeUndefined()
    expect(requireCoverage('Sample', [entry('t', 0, 1, 0)], [entry('t', 0, 0, 1)])).toBeUndefined()
    expect(requireCoverage('Sample', [entry('t', '0', '1')], [entry('t', 0, 1)])).toBeUndefined()
    expect(
      requireCoverage('Sample', [entry('t', 0), entry('t', 1)], [entry('t', 0, 1)])
    ).toBeUndefined()
    expect(
      requireCoverage('Sample', [entry('t', 0), entry('t', 1)], [entry('t', 1, 0)])
    ).toBeUndefined()
  })

  test('accepts an answer that splits one request topic over two entries, and one request topic with no partitions', () => {
    expect(requireCoverage('Sample', SENT, [entry('t', 0), entry('t', 1)])).toBeUndefined()
    expect(
      requireCoverage('Sample', [entry('empty'), entry('t', 0, 1)], [entry('t', 0, 1)])
    ).toBeUndefined()
  })

  test('a missing pair is allowed when the rule says so, an unrequested one still is not', () => {
    const rule = { missing: 'allow' }

    expect(requireCoverage('Sample', SENT, [entry('t', 1)], rule)).toBeUndefined()
    expect(requireCoverage('Sample', SENT, [], rule)).toBeUndefined()
    expect(() => requireCoverage('Sample', SENT, [entry('t', 1, 7)], rule)).toThrow(
      /partition 7 of topic t that the request did not carry/
    )
  })

  test('skips an unnamed topic only when the rule says so', () => {
    const rows = [entry('t', 0, 1), { topic: null, partitions: [{ partition: 9 }] }]

    expect(requireCoverage('Sample', SENT, rows, { skipUnnamed: true })).toBeUndefined()
    expect(() => requireCoverage('Sample', SENT, rows)).toThrow(
      /partition 9 of topic null that the request did not carry/
    )
  })

  test('reads the topic and partition from the fields the rule names', () => {
    const sent = [{ topic: 'id-1', partitions: [{ partition: 0 }] }]
    const rule = { rowTopic: 'topicId', rowPartition: 'partitionIndex' }

    expect(
      requireCoverage(
        'Sample',
        sent,
        [{ topicId: 'id-1', partitions: [{ partitionIndex: 0 }] }],
        rule
      )
    ).toBeUndefined()
    expect(() =>
      requireCoverage(
        'Sample',
        sent,
        [{ topicId: 'id-1', partitions: [{ partitionIndex: 3 }] }],
        rule
      )
    ).toThrow(/partition 3 of topic id-1 that the request did not carry/)
  })

  describe('Produce rule', () => {
    const rule = { rowTopic: 'topicName', repeats: 'refuse', missing: 'allow' }
    const asked = [{ topic: 't', partitions: [{ partition: 0 }, { partition: 1 }] }]
    const row = (topicName, ...ids) => ({
      topicName,
      partitions: ids.map((partition) => ({ partition })),
    })

    test('refuses a duplicate row in either order', () => {
      for (const rows of [
        [row('t', 0, 1, 0)],
        [row('t', 0, 0, 1)],
        [row('t', 0, 1), row('t', 0)],
      ]) {
        const error = thrownBy(() => requireCoverage('Produce', asked, rows, rule))

        expect(error).toBeInstanceOf(KafkaJSError)
        expect(error.retriable).toBe(true)
        expect(error.message).toMatch(/partition 0 of topic t twice/)
      }
    })

    test('refuses an unrequested partition, an unrequested topic and a null topic name', () => {
      expect(() => requireCoverage('Produce', asked, [row('t', 0, 5)], rule)).toThrow(
        /partition 5 of topic t that the request did not carry/
      )
      expect(() => requireCoverage('Produce', asked, [row('other', 0)], rule)).toThrow(
        /partition 0 of topic other that the request did not carry/
      )
      expect(() => requireCoverage('Produce', asked, [row(null, 0)], rule)).toThrow(
        /that the request did not carry/
      )
    })

    test('accepts rows in another order and missing rows', () => {
      expect(requireCoverage('Produce', asked, [row('t', 1, 0)], rule)).toBeUndefined()
      expect(requireCoverage('Produce', asked, [row('t', 1)], rule)).toBeUndefined()
      expect(requireCoverage('Produce', asked, [], rule)).toBeUndefined()
    })

    test('merges two request entries for one topic and accepts string partition ids in the request', () => {
      const request = [
        { topic: 't', partitions: [{ partition: 0 }] },
        { topic: 't', partitions: [{ partition: '1' }] },
      ]

      expect(requireCoverage('Produce', request, [row('t', 0, 1)], rule)).toBeUndefined()
    })

    test('refuses two identical rows when the request repeats the pair', () => {
      const request = [
        { topic: 't', partitions: [{ partition: 0 }] },
        { topic: 't', partitions: [{ partition: '0' }] },
      ]

      expect(() => requireCoverage('Produce', request, [row('t', 0, 0)], rule)).toThrow(
        /partition 0 of topic t twice/
      )
    })
  })
})
