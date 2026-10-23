const UniformStickyPartitioner = require('./index')
const { mulberry32 } = require('../../../../testHelpers/schedule')

/**
 * @param {...number} leaders one entry per partition, in partition order
 * @returns {{ partitionId: number, leader: number }[]}
 */
const metadata = (...leaders) => leaders.map((leader, partitionId) => ({ partitionId, leader }))

/** An adaptive partitioner that switches at every keyless record, attached to fixed queues. */
const adaptiveOver = (queues, options = {}) => {
  const reads = []
  const partitioner = UniformStickyPartitioner({ stickyBytes: 1, adaptive: true, ...options })()
  partitioner.attach({
    loadOf: (topic, partition) => {
      reads.push([topic, partition])
      return queues[partition]
    },
    rackOf: () => undefined,
  })
  return { partitioner, reads }
}

describe('Producer > Partitioner > UniformSticky > adaptive weighting', () => {
  afterEach(() => jest.restoreAllMocks())

  test('weighs each partition by the deepest queue plus one less its own, as a cumulative table', () => {
    // queues 0, 3, 1: weights 4, 1, 3, the table 4, 5, 8 - draws 0 to 3 pick partition 0, 4 picks
    // 1, 5 to 7 pick 2 (the example of BuiltInPartitioner.updatePartitionLoadStats)
    const draws = [0, 3.99, 4, 4.99, 5, 7.99].map((draw) => draw / 8)
    jest.spyOn(Math, 'random').mockImplementation(() => draws.shift())
    const { partitioner } = adaptiveOver([0, 3, 1])
    const send = () =>
      partitioner({ topic: 'topic', partitionMetadata: metadata(0, 1, 2), message: { value: 'x' } })

    expect(Array.from({ length: 6 }, send)).toEqual([0, 0, 1, 1, 2, 2])
  })

  test('equal queues pick uniformly, as a partitioner without weighting does', () => {
    jest.spyOn(Math, 'random').mockReturnValue(0.5)
    const { partitioner } = adaptiveOver([2, 2, 2, 2])

    expect(
      partitioner({
        topic: 'topic',
        partitionMetadata: metadata(0, 0, 0, 0),
        message: { value: 'x' },
      })
    ).toBe(2)
  })

  test('only partitions with a leader are weighed and picked; fewer than two pick uniformly', () => {
    jest.spyOn(Math, 'random').mockReturnValue(0)
    const { partitioner, reads } = adaptiveOver([0, 9, 5, 0])
    const send = (partitionMetadata) =>
      partitioner({ topic: 'topic', partitionMetadata, message: { value: 'x' } })

    // the leaders are partitions 1 and 2: weights 1 and 5, a draw of 0 picks partition 1
    expect(send(metadata(-1, 1, 1, -1))).toBe(1)
    expect(reads).toEqual([
      ['topic', 1],
      ['topic', 2],
    ])
    expect(send(metadata(-1, -1, 1, -1))).toBe(2)
  })

  test('over 30 000 seeded picks the frequencies match the weights within a chi-square bound', () => {
    const random = mulberry32(42)
    jest.spyOn(Math, 'random').mockImplementation(random)
    const queues = [0, 3, 1, 7]
    const { partitioner } = adaptiveOver(queues)
    const partitionMetadata = metadata(0, 1, 2, 3)
    const counts = [0, 0, 0, 0]

    for (let i = 0; i < 30000; i += 1) {
      counts[partitioner({ topic: 'topic', partitionMetadata, message: { value: 'x' } })] += 1
    }

    const weights = [8, 5, 7, 1]
    const chiSquare = counts.reduce((sum, count, i) => {
      const expected = (30000 * weights[i]) / 21
      return sum + (count - expected) ** 2 / expected
    }, 0)
    // three degrees of freedom: 16.27 is the 0.001 critical value
    expect(chiSquare).toBeLessThan(16.27)
  })

  test('reads the queues only when it picks, never for each record', () => {
    jest.spyOn(Math, 'random').mockReturnValue(0)
    const { partitioner, reads } = adaptiveOver([0, 1, 0, 1], { stickyBytes: 250 })
    const partitionMetadata = metadata(0, 1, 2, 3)

    for (let i = 0; i < 1000; i += 1) {
      partitioner({ topic: 'topic', partitionMetadata, message: { value: 'x' } })
    }

    // 25 wire bytes a record: a pick every 10 records, 4 reads a pick
    expect(reads).toHaveLength(100 * 4)
  })

  test('without being attached, or when not adaptive, it reads no queue and picks uniformly', () => {
    jest.spyOn(Math, 'random').mockReturnValue(0.99)
    const unattached = UniformStickyPartitioner({ adaptive: true })()
    const { partitioner, reads } = adaptiveOver([0, 9, 9], { adaptive: false })
    const send = (one) =>
      one({ topic: 'topic', partitionMetadata: metadata(0, 0, 0), message: { value: 'x' } })

    expect([send(unattached), send(partitioner)]).toEqual([2, 2])
    expect(reads).toEqual([])
  })
})
