const {
  DeliveryLedger,
  IntervalSet,
  SendLedger,
  SequenceChecker,
  decodeKey,
  encodeKey,
  parseDumpLine,
} = require('../sequenceOracle')

const TOPICS = ['load-a', 'load-b']
const PARTITIONS = 8
const PRODUCERS = [
  { producer: 'p0', kind: 'idempotent' },
  { producer: 'p1', kind: 'plain' },
  { producer: 'p2', kind: 'transactional' },
]
const GROUP = 'load-group'

/**
 * A generated log: record i goes to cell i mod cells, written by producer (i div cells) mod
 * producers, so every producer writes an increasing sequence in every partition at consecutive
 * offsets. Producers settle every record as acknowledged unless told otherwise; the consumer is
 * delivered every record and commits the end.
 */
class GeneratedLog {
  /** @param {number} records */
  constructor(records) {
    this.records = records
    this.cells = TOPICS.flatMap((topic) =>
      Array.from({ length: PARTITIONS }, (_, partition) => ({ topic, partition }))
    )
  }

  /** @returns {Generator<{ topic: string, partition: number, offset: number, key: string, producer: number }>} */
  *entries() {
    const seq = new Map()
    const offset = new Map()
    for (let i = 0; i < this.records; i++) {
      const c = i % this.cells.length
      const { topic, partition } = this.cells[c]
      const producer = Math.floor(i / this.cells.length) % PRODUCERS.length
      const s = seq.get(`${producer}|${c}`) ?? 0
      seq.set(`${producer}|${c}`, s + 1)
      const o = offset.get(c) ?? 0
      offset.set(c, o + 1)
      yield {
        topic,
        partition,
        offset: o,
        key: encodeKey(PRODUCERS[producer].producer, 0, s),
        producer,
      }
    }
  }

  /**
   * @param {object} [options]
   * @param {(entry: object) => string|undefined} [options.settleAs] the status of each record's send
   * @returns {object[]} `SendLedger#toJSON` of every producer
   */
  producerReports({ settleAs = () => 'acked' } = {}) {
    const ledgers = PRODUCERS.map(
      ({ producer, kind }) => new SendLedger({ producer, incarnation: 0, kind, maxInFlight: 5 })
    )
    for (const entry of this.entries()) {
      const ledger = ledgers[entry.producer]
      const { first } = ledger.issue(entry.topic, entry.partition, 1)
      ledger.settle(entry.topic, entry.partition, first, settleAs(entry), entry.offset)
    }
    return ledgers.map((l) => l.toJSON())
  }

  /**
   * @param {object} [options]
   * @param {(entry: object) => boolean} [options.skip] records the consumer is not delivered
   * @returns {{ consumers: object[], committed: object, logEnds: object }}
   */
  consumerReports({ skip = () => false } = {}) {
    const member = new DeliveryLedger({ group: GROUP, member: 'c0' })
    const ends = {}
    for (const entry of this.entries()) {
      if (!skip(entry)) member.deliver(entry.topic, entry.partition, entry.offset)
      ends[`${entry.topic}/${entry.partition}`] = entry.offset + 1
    }
    return { consumers: [member.toJSON()], committed: { [GROUP]: { ...ends } }, logEnds: ends }
  }

  /**
   * Feeds the log to a checker, through `edit`, which may drop, change or add records.
   * @param {SequenceChecker} checker
   * @param {(entry: object) => object[]} [edit]
   * @returns {SequenceChecker}
   */
  feed(checker, edit = (entry) => [entry]) {
    for (const entry of this.entries())
      for (const { topic, partition, offset, key } of edit(entry))
        checker.observe(topic, partition, offset, key)
    return checker
  }
}

const RECORDS = 120000

/** @param {GeneratedLog} log @param {object} [options] @returns {SequenceChecker} */
const checkerFor = (log, { settleAs, skip } = {}) =>
  new SequenceChecker({
    producers: log.producerReports({ settleAs }),
    ...log.consumerReports({ skip }),
  })

/** @param {object} verdict @returns {Record<string, number>} the finding kinds with a count */
const counted = ({ findings }) =>
  Object.fromEntries(
    Object.entries(findings)
      .filter(([, f]) => f.count > 0)
      .map(([kind, f]) => [kind, f.count])
  )

/** The offset of producer `producer`'s `n`-th record, in every partition. */
const nth = (producer, n) => ({ offset: n * PRODUCERS.length + producer })

/** @returns {(e: object) => boolean} whether a record is at that topic, partition and offset */
const at = (topic, partition, offset) => (e) =>
  e.topic === topic && e.partition === partition && e.offset === offset

/**
 * A repeat of producer `producer`'s `n`-th record in load-b/3, written over the next record (of
 * the next producer), whose own send is then settled as failed so its absence is no loss.
 * @returns {{ settleAs: Function, edit: Function, offset: number }}
 */
const repeatOf = (producer, n) => {
  const { offset } = nth(producer, n)
  const next = at(TOPICS[1], 3, offset + 1)
  const key = encodeKey(PRODUCERS[producer].producer, 0, n)
  return {
    offset: offset + 1,
    settleAs: (e) => (next(e) ? 'failed' : 'acked'),
    edit: (e) => (next(e) ? [{ ...e, key }] : [e]),
  }
}

describe('sequence oracle', () => {
  const log = new GeneratedLog(RECORDS)

  it('passes a clean stream of 120,000 records from all three producer kinds', () => {
    const verdict = log.feed(checkerFor(log)).finish()
    expect(counted(verdict)).toEqual({})
    expect(verdict).toMatchObject({ ok: true, complete: true })
    expect(verdict.counts).toEqual({
      records: RECORDS,
      repeatsAllowed: 0,
      reordersAllowed: 0,
      notYetCommitted: 0,
    })
  })

  it('detects one lost acknowledged record', () => {
    const lost = nth(0, 500)
    const verdict = log
      .feed(checkerFor(log), (e) =>
        e.topic === TOPICS[0] && e.partition === 0 && e.offset === lost.offset ? [] : [e]
      )
      .finish()
    expect(counted(verdict)).toEqual({ loss: 1 })
    expect(verdict.findings.loss.samples).toEqual(['p0.0 load-a/0: [[500,500]]'])
    expect(verdict.ok).toBe(false)
  })

  it('does not count a record whose send was rejected as lost', () => {
    const missing = nth(0, 500)
    const verdict = log
      .feed(
        checkerFor(log, {
          settleAs: (e) =>
            e.topic === TOPICS[0] && e.partition === 0 && e.offset === missing.offset
              ? 'failed'
              : 'acked',
        }),
        (e) =>
          e.topic === TOPICS[0] && e.partition === 0 && e.offset === missing.offset ? [] : [e]
      )
      .finish()
    expect(counted(verdict)).toEqual({})
  })

  it('detects one duplicate from an idempotent producer', () => {
    const twice = repeatOf(0, 700)
    const verdict = log.feed(checkerFor(log, twice), twice.edit).finish()
    expect(counted(verdict)).toEqual({ duplicate: 1 })
    expect(verdict.findings.duplicate.samples).toEqual([`load-b/3@${twice.offset}: p0.0.700`])
  })

  it('allows the same repeat from a non-idempotent producer and counts it', () => {
    const twice = repeatOf(1, 700)
    const verdict = log.feed(checkerFor(log, twice), twice.edit).finish()
    expect(counted(verdict)).toEqual({})
    expect(verdict.counts.repeatsAllowed).toBe(1)
  })

  it('detects one reordering from an idempotent producer', () => {
    const [a, b] = [nth(0, 900), nth(0, 901)]
    const swap = { [a.offset]: encodeKey('p0', 0, 901), [b.offset]: encodeKey('p0', 0, 900) }
    const verdict = log
      .feed(checkerFor(log), (e) =>
        e.topic === TOPICS[0] && e.partition === 0 && swap[e.offset]
          ? [{ ...e, key: swap[e.offset] }]
          : [e]
      )
      .finish()
    expect(counted(verdict)).toEqual({ reorder: 1 })
    expect(verdict.findings.reorder.samples).toEqual([`load-a/0@${b.offset}: p0.0.900 after 901`])
  })

  it('detects one skipped commit: a committed record no member was delivered', () => {
    const skipped = nth(2, 1000)
    const verdict = log
      .feed(
        checkerFor(log, {
          skip: (e) => e.topic === TOPICS[0] && e.partition === 5 && e.offset === skipped.offset,
        })
      )
      .finish()
    expect(counted(verdict)).toEqual({ skippedCommit: 1 })
    expect(verdict.findings.skippedCommit.samples).toEqual([
      `${GROUP} load-a/5: offset ${skipped.offset} committed, never delivered`,
    ])
  })

  it('detects a record of an aborted transaction in the read_committed dump', () => {
    const shown = nth(2, 10)
    const verdict = log
      .feed(
        checkerFor(log, {
          settleAs: (e) =>
            e.topic === TOPICS[0] && e.partition === 0 && e.offset === shown.offset
              ? 'aborted'
              : 'acked',
        })
      )
      .finish()
    expect(counted(verdict)).toEqual({ abortedVisible: 1 })
  })

  it('detects a key no producer issued, a commit beyond the log end and a short read', () => {
    const small = new GeneratedLog(48)
    const reports = small.consumerReports()
    reports.committed[GROUP]['load-a/0'] = 99
    const checker = new SequenceChecker({ producers: small.producerReports(), ...reports })
    small.feed(checker, (e) =>
      e.topic === TOPICS[1] && e.partition === 7 && e.offset === 2 ? [] : [e]
    )
    checker.observe('load-a', 1, 1000, encodeKey('p0', 0, 999))
    checker.unparseable('load-a', '[warn] something')
    const verdict = checker.finish()
    expect(counted(verdict)).toEqual({
      foreign: 1,
      commitBeyondLog: 1,
      loss: 1,
      incompleteRead: 1,
      unparseable: 1,
    })
    expect(verdict.complete).toBe(false)
  })

  it('keeps checker structures the same size for 120,000 and 600,000 records', () => {
    const big = new GeneratedLog(RECORDS * 5)
    const smallChecker = log.feed(checkerFor(log))
    const bigChecker = big.feed(checkerFor(big))
    expect(bigChecker.finish().counts.records).toBe(RECORDS * 5)
    expect(bigChecker.size()).toEqual(smallChecker.size())
    expect(bigChecker.size()).toEqual({
      cells: PRODUCERS.length * TOPICS.length * PARTITIONS,
      gapRanges: 0,
      groupCells: TOPICS.length * PARTITIONS,
      deliveredRanges: TOPICS.length * PARTITIONS,
      partitions: TOPICS.length * PARTITIONS,
      samples: 0,
    })
  })
})

describe('sequence oracle ledgers', () => {
  it('reports a send still unsettled as not acknowledged, and the last acknowledged offset', () => {
    const ledger = new SendLedger({
      producer: 'p3',
      incarnation: 2,
      kind: 'idempotent',
      maxInFlight: 5,
    })
    const a = ledger.issue('t', 0, 10)
    const b = ledger.issue('t', 0, 5)
    const c = ledger.issue('t', 0, 3)
    ledger.settle('t', 0, b.first, 'failed')
    ledger.settle('t', 0, a.first, 'acked', 40)
    expect(a.key(9)).toBe('p3.2.9')
    expect(ledger.unsettled).toBe(1)
    expect(ledger.toJSON().cells['t/0']).toEqual({
      issued: 18,
      notAcked: [[10, 17]],
      aborted: [],
      maxOffset: 49,
    })
    expect(() => ledger.settle('t', 0, a.first, 'acked')).toThrow('No pending send t/0@0')
    expect(c.first).toBe(15)
  })

  it('round-trips keys and dump lines', () => {
    expect(decodeKey('p12.3.456')).toEqual({ producer: 'p12', incarnation: 3, sequence: 456 })
    expect(decodeKey('x.1.2')).toBeNull()
    expect(parseDumpLine('Partition:7\tOffset:123\tp0.0.5')).toEqual({
      partition: 7,
      offset: 123,
      key: 'p0.0.5',
    })
    expect(parseDumpLine('Processed a total of 3 messages')).toBeNull()
  })
})

describe('IntervalSet', () => {
  it('merges, splits and subtracts ranges', () => {
    const set = new IntervalSet([
      [10, 12],
      [1, 3],
      [5, 5],
    ])
    set.add(4)
    expect(set.toJSON()).toEqual([
      [1, 5],
      [10, 12],
    ])
    set.add(6, 11)
    expect(set.toJSON()).toEqual([[1, 12]])
    expect(set.delete(7)).toBe(true)
    expect(set.delete(7)).toBe(false)
    expect(set.toJSON()).toEqual([
      [1, 6],
      [8, 12],
    ])
    expect(set.has(6)).toBe(true)
    expect(set.has(7)).toBe(false)
    expect(set.count).toBe(11)
    expect(set.minus(new IntervalSet([[3, 9]])).toJSON()).toEqual([
      [1, 2],
      [10, 12],
    ])
  })
})
