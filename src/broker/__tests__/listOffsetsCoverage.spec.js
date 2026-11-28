const { createGroupApiBroker } = require('./fixtures/groupApiHarness')
const apiKeys = require('../../protocol/requests/apiKeys')
const listOffsetsV9 = require('../../../testHelpers/wire/listOffsetsV9')

const TOPIC = 'test-topic'
const OTHER = 'other-topic'
const VERSIONS = [6, 9, 11]

/**
 * @param {number} partitionIndex
 * @param {string} [offset='10']
 * @param {number} [errorCode=0]
 * @returns {object} one partition row of an answer
 */
const row = (partitionIndex, offset = '10', errorCode = 0) => ({
  partitionIndex,
  errorCode,
  timestamp: '-1',
  offset,
  leaderEpoch: 5,
})

/**
 * @param {string} name
 * @param {...object} partitions rows from `row`
 * @returns {{ name: string, partitions: object[] }} one topic entry of an answer
 */
const entry = (name, ...partitions) => ({ name, partitions })

/**
 * @param {number[]} partitions the partitions of `TOPIC` to look up
 * @returns {object[]} the request's topics
 */
const askedFor = (partitions) => [
  { topic: TOPIC, partitions: partitions.map((partition) => ({ partition })) },
]

describe('Broker > listOffsets answer coverage', () => {
  const harnesses = []

  afterEach(async () => {
    await Promise.all(harnesses.splice(0).map(({ pool }) => pool.destroy()))
  })

  /**
   * One ListOffsets answered with `entries`, through a real broker and connection. The answer's
   * layout is the same flexible one at v6, v9 and v11.
   * @param {number} version
   * @param {ReturnType<typeof entry>[]} entries
   * @param {number[]} [partitions=[0, 1]] the partitions the request names
   * @returns {Promise<object>}
   */
  const list = (version, entries, partitions = [0, 1]) => {
    const harness = createGroupApiBroker({
      versions: { [apiKeys.ListOffsets]: { minVersion: 0, maxVersion: version } },
      respond: () => listOffsetsV9.buildResponse({ throttleTime: 0, topics: entries }),
    })
    harnesses.push(harness)
    return harness.broker.listOffsets({ topics: askedFor(partitions), maxVersion: version })
  }

  /** @returns {number[]} the ListOffsets versions the latest broker was asked in */
  const versionsSent = () => harnesses[harnesses.length - 1].requests.map((r) => r.apiVersion)

  test.each([
    ['no rows at all', [0, 1], []],
    ['a missing partition', [0, 1], [entry(TOPIC, row(0))]],
    [
      'an unrequested topic beside the requested rows',
      [0, 1],
      [entry(TOPIC, row(0), row(1)), entry(OTHER, row(0))],
    ],
    [
      'an unrequested topic with no partitions',
      [0, 1],
      [entry(TOPIC, row(0), row(1)), entry(OTHER)],
    ],
    ['only another topic', [0, 1], [entry(OTHER, row(0), row(1))]],
    ['an unrequested partition', [0, 1], [entry(TOPIC, row(0), row(1), row(7))]],
    ['a partition answered twice', [0], [entry(TOPIC, row(0, '10'), row(0, '9999'))]],
  ])('%s is refused retriably', async (_, asked, entries) => {
    for (const version of VERSIONS) {
      await expect(list(version, entries, asked)).rejects.toMatchObject({
        name: 'KafkaJSError',
        retriable: true,
        message: expect.stringMatching(/^ListOffsets response /),
      })
      expect(versionsSent()).toEqual([version])
    }
  })

  test('an offset below -1 on a row without an error is refused retriably, naming the row', async () => {
    for (const version of VERSIONS) {
      await expect(list(version, [entry(TOPIC, row(0, '-5'))], [0])).rejects.toMatchObject({
        name: 'KafkaJSError',
        retriable: true,
        message: expect.stringMatching(
          /^ListOffsets response gives partition 0 of topic test-topic the offset -5/
        ),
      })
    }
  })

  test('an answer that covers the request resolves, rows in any order', async () => {
    const entries = [entry(TOPIC, row(1, '11'), row(0, '10'))]
    for (const version of VERSIONS) {
      const result = await list(version, entries)

      expect(result.responses[0].partitions.map(({ partition }) => partition)).toEqual([1, 0])
    }
  })

  test('an offset of -1 on a row without an error resolves: no record at or after the timestamp', async () => {
    for (const version of VERSIONS) {
      const result = await list(version, [entry(TOPIC, row(0, '-1'))], [0])

      expect(result.responses[0].partitions[0]).toMatchObject({ partition: 0, offset: '-1' })
    }
  })

  test('an answer that refuses every partition is reported as that error, not as a coverage error', async () => {
    const entries = [entry(TOPIC, row(0, '-1', 6), row(1, '-1', 6))]
    for (const version of VERSIONS) {
      await expect(list(version, entries)).rejects.toMatchObject({
        type: 'NOT_LEADER_FOR_PARTITION',
      })
    }
  })
})
