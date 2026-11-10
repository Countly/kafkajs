const {
  A,
  acquire,
  acquired,
  fetchAnswer,
  flush,
  frames,
  harness,
  part,
  recordsHex,
} = require('../../../../testHelpers/shareHarness')

/** @param {object} h The harness. @param {number} [node] Restricts the frames to one node. @returns {object[]} The ShareFetch frames written, oldest first. */
const fetchFrames = (h, node) =>
  frames(h.cluster, 78).filter(({ node: id }) => node === undefined || id === node)

/** @param {number|string} first First offset. @param {number|string} last Last offset. @param {number} type Wire acknowledge type. @returns {object} One acknowledgement batch. */
const batchOf = (first, last, type) => ({
  firstOffset: String(first),
  lastOffset: String(last),
  acknowledgeTypes: [type],
})

/** @param {object} frame One ShareFetch frame. @returns {object[]} Its acknowledgement batches, flattened over partitions. */
const acknowledgementsOf = (frame) =>
  frame.fields.topics.flatMap(({ partitions }) =>
    partitions.flatMap(({ acknowledgementBatches }) => acknowledgementBatches)
  )

/**
 * @param {Array<{row: object, results: object[]}>} outcomes What `onResults` saw.
 * @returns {Map<string, Array<{row: object, type: string, error: unknown}>>} Every offset's
 *   outcomes, with the acquisition each belongs to.
 */
const byOffset = (outcomes) => {
  const seen = new Map()
  for (const { row, results } of outcomes) {
    for (const { firstOffset, lastOffset, type, error } of results) {
      for (let offset = BigInt(firstOffset); offset <= BigInt(lastOffset); offset++) {
        const key = String(offset)
        seen.set(key, [...(seen.get(key) ?? []), { row, type, error }])
      }
    }
  }
  return seen
}

/**
 * @param {object[]} results A node's compact outcomes.
 * @returns {Map<string, Array<{type: string, reason: string|undefined}>>} Every offset's outcomes
 *   with the failure reason, if any.
 */
const tally = (results) => {
  const seen = new Map()
  for (const { firstOffset, lastOffset, type, error } of results) {
    for (let offset = BigInt(firstOffset); offset <= BigInt(lastOffset); offset++) {
      const key = String(offset)
      seen.set(key, [...(seen.get(key) ?? []), { type, reason: error?.reason }])
    }
  }
  return seen
}

/**
 * Builds a one-node harness whose loop reports every compact outcome with its acquisition and
 * whose handler is the test's own `dispatch`, so an offset stays undecided until the test decides.
 * @param {object} [options] Harness options.
 * @returns {Promise<{h: object, m: object, outcomes: object[], rows: object[]}>} The harness, its
 *   node, the outcomes seen and the acquisitions delivered, in order.
 */
const oneNode = async (options = {}) => {
  const outcomes = []
  const rows = []
  const h = await harness({
    ...options,
    loop: { onResults: (results, row) => outcomes.push({ row, results }) },
  })
  return { h, m: h.members[0], outcomes, rows }
}

describe('Kafka 4.4 client fix audit', () => {
  let h
  afterEach(async () => {
    if (h) await h.close()
    h = null
    jest.restoreAllMocks()
  })

  test('KAFKA-21106 two acquisitions of one partition settle separately and leave nothing retained', async () => {
    const node = await oneNode()
    h = node.h
    const { m, outcomes, rows } = node
    const delivered = []
    m.want = [{ topicId: A, partition: 0 }]
    m.dispatch = async (cohorts) => {
      for (const row of cohorts) {
        rows.push(row)
        for (const message of row.walk.deliver.records) delivered.push({ row, message })
        if (rows.length === 1) row.ledger.complete(['10'], 'accept')
      }
    }
    h.script('ShareFetch', [
      fetchAnswer([acquire(0, 10, ['a', 'b'])]),
      fetchAnswer([acquire(0, 20, ['c', 'd'])]),
    ])

    await m.loop.poll()
    await m.loop.poll()
    const [first, second] = rows
    expect(rows).toHaveLength(2)
    expect(first).not.toBe(second)
    expect(first.token).not.toBe(second.token)
    expect(first.ledger).not.toBe(second.ledger)
    expect(acknowledgementsOf(fetchFrames(h)[1])).toEqual([batchOf(10, 10, 1)])
    expect(delivered.map(({ message }) => message.offset)).toEqual(['10', '11', '20', '21'])
    delivered.forEach(({ row, message }) => {
      const own = row.walk.deliver.records.find(({ offset }) => offset === message.offset)
      expect(message).toBe(own)
    })

    first.ledger.complete(['11'], 'accept')
    second.ledger.complete(['20', '21'], 'accept')
    await m.loop.poll()
    expect(acknowledgementsOf(fetchFrames(h)[2])).toEqual([batchOf(11, 11, 1)])
    await m.loop.poll()
    expect(acknowledgementsOf(fetchFrames(h)[3])).toEqual([batchOf(20, 21, 1)])

    const seen = byOffset(outcomes)
    expect([...seen.keys()].sort()).toEqual(['10', '11', '20', '21'])
    for (const [offset, row] of [
      ['10', first],
      ['11', first],
      ['20', second],
      ['21', second],
    ]) {
      expect(seen.get(offset)).toEqual([{ row, type: 'accept', error: null }])
    }
    expect(m.loop.cohorts.size).toBe(0)
    expect(m.loop.exchanges.size).toBe(0)

    m.want = []
    await m.loop.poll()
    const last = fetchFrames(h)[4].fields
    expect(last.forgottenTopicsData).toEqual([{ topicId: A, partitions: [0] }])
    expect(last.topics).toEqual([])
    expect(m.loop.cohorts.size).toBe(0)
    expect(m.loop.exchanges.size).toBe(0)
  })

  test('KAFKA-21106 two acquisition ranges of one partition in one answer are acknowledged per range and released', async () => {
    const node = await oneNode()
    h = node.h
    const { m, outcomes, rows } = node
    const values = Array.from({ length: 16 }, (_, offset) => `v${offset}`)
    m.want = [{ topicId: A, partition: 0 }]
    m.dispatch = async (cohorts) => {
      for (const row of cohorts) {
        rows.push(row)
        row.ledger.complete(['0', '1', '2'], 'accept')
        row.ledger.complete(['13', '14', '15'], 'release')
      }
    }
    h.script('ShareFetch', [
      fetchAnswer([
        part({
          partition: 0,
          acquire: [acquired(0, 2, 1), acquired(13, 15, 2)],
          records: recordsHex(0, values),
        }),
      ]),
    ])

    await m.loop.poll()
    expect(rows).toHaveLength(1)
    const [row] = rows
    expect(row.walk.deliver.records.map(({ offset }) => offset)).toEqual([
      '0',
      '1',
      '2',
      '13',
      '14',
      '15',
    ])
    expect(Array.from(row.walk.deliver.deliveryCounts)).toEqual([1, 1, 1, 2, 2, 2])
    expect(row.walk.gaps).toEqual([])

    await m.loop.poll()
    expect(acknowledgementsOf(fetchFrames(h)[1])).toEqual([batchOf(0, 2, 1), batchOf(13, 15, 2)])
    const seen = byOffset(outcomes)
    expect([...seen.keys()]).toEqual(['0', '1', '2', '13', '14', '15'])
    for (const [offset, entries] of seen) {
      expect(entries).toEqual([
        { row, type: Number(offset) < 3 ? 'accept' : 'release', error: null },
      ])
    }
    expect(m.loop.cohorts.size).toBe(0)
    expect(m.loop.exchanges.size).toBe(0)
  })

  test('gap-only acquisitions send acknowledgements without delivering records', async () => {
    const node = await oneNode()
    h = node.h
    const { m, outcomes, rows } = node
    const delivered = []
    m.want = [{ topicId: A, partition: 0 }]
    m.dispatch = async (cohorts) => {
      for (const row of cohorts) {
        rows.push(row)
        delivered.push(...row.walk.deliver.records)
      }
    }
    h.script('ShareFetch', [
      fetchAnswer([part({ partition: 0, acquire: [acquired(5, 7)], records: '' })]),
    ])

    await m.loop.poll()
    expect(rows).toHaveLength(1)
    expect(rows[0].walk.deliver.records).toEqual([])
    expect(delivered).toEqual([])
    expect(m.loop.cohorts.size).toBe(1)

    await m.loop.poll()
    expect(acknowledgementsOf(fetchFrames(h)[1])).toEqual([batchOf(5, 7, 0)])
    const seen = byOffset(outcomes)
    expect([...seen.keys()]).toEqual(['5', '6', '7'])
    for (const entries of seen.values())
      expect(entries).toEqual([{ row: rows[0], type: 'gap', error: null }])
    expect(delivered).toEqual([])
    expect(m.loop.cohorts.size).toBe(0)
    expect(m.loop.exchanges.size).toBe(0)
  })

  test('KAFKA-21106 a late old-leader response cannot restore retired ownership', async () => {
    h = await harness({ nodes: 3 })
    const [old, , replacement] = h.members
    const key = `${A}:0`
    const delivered = { old: [], replacement: [] }
    old.want = [{ topicId: A, partition: 0 }]
    old.dispatch = async (cohorts) => {
      for (const row of cohorts) {
        delivered.old.push(...row.walk.deliver.records.map(({ offset }) => offset))
        if (delivered.old.length === 2) row.ledger.complete(['10'], 'accept')
      }
    }
    replacement.dispatch = async (cohorts) => {
      for (const row of cohorts) {
        delivered.replacement.push(...row.walk.deliver.records.map(({ offset }) => offset))
        row.ledger.complete(
          row.walk.deliver.records.map(({ offset }) => offset),
          'accept'
        )
      }
    }
    h.scriptNodes('ShareFetch', {
      0: [fetchAnswer([acquire(0, 10, ['a', 'b'])])],
      2: [fetchAnswer([acquire(0, 30, ['new', 'newer'])])],
    })
    await old.loop.poll()
    expect(delivered.old).toEqual(['10', '11'])

    const gate = h.hold({ api: 'ShareFetch', node: 0 })
    const late = old.loop.poll()
    await gate.arrived()
    // What the run does when routing moves the partition: stop wanting it, retire it, end the node.
    old.want = []
    old.loop.retireKey(key, 'session-lost')
    const finishing = old.loop.finishNode()
    replacement.want = [{ topicId: A, partition: 0 }]
    await replacement.loop.poll()
    await replacement.loop.poll()
    expect(delivered.replacement).toEqual(['30', '31'])

    gate.release(fetchAnswer([acquire(0, 12, ['late'])]))
    await late.catch(() => {})
    await finishing
    await flush()

    expect(delivered).toEqual({ old: ['10', '11'], replacement: ['30', '31'] })
    expect(tally(old.results)).toEqual(
      new Map([
        ['10', [{ type: 'accept', reason: 'session-lost' }]],
        ['11', [{ type: 'release', reason: 'session-lost' }]],
        ['12', [{ type: 'release', reason: undefined }]],
      ])
    )
    expect(tally(replacement.results)).toEqual(
      new Map([
        ['30', [{ type: 'accept', reason: undefined }]],
        ['31', [{ type: 'accept', reason: undefined }]],
      ])
    )
    expect(fetchFrames(h, 2).map(acknowledgementsOf)).toEqual([[], [batchOf(30, 31, 1)]])
    const releases = frames(h.cluster, 79)
    expect(releases).toHaveLength(1)
    expect(releases[0]).toMatchObject({ node: 0, fields: { shareSessionEpoch: -1 } })
    expect(acknowledgementsOf(releases[0])).toEqual([batchOf(12, 12, 2)])
    for (const loop of [old.loop, replacement.loop]) {
      expect(loop.cohorts.size).toBe(0)
      expect(loop.exchanges.size).toBe(0)
    }
  })
})
