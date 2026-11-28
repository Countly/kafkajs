const CommittedOffsetRevisions = require('../committedOffsetRevisions')

describe('Cluster > CommittedOffsetRevisions', () => {
  test('capture then advance makes the position stale', () => {
    const revisions = CommittedOffsetRevisions.forCache({})
    const captured = revisions.capture('topic', 0)

    expect(captured).toBe(1)
    expect(revisions.isCurrent('topic', 0, captured)).toBe(true)
    revisions.advance('topic', 0)
    expect(revisions.isCurrent('topic', '0', captured)).toBe(false)
    const recaptured = revisions.capture('topic', 0)
    expect(recaptured).not.toBe(captured)
    expect(revisions.isCurrent('topic', 0, recaptured)).toBe(true)
  })

  test('a same-value publication makes the position stale', () => {
    const cache = { topic: { 0: '99' } }
    const revisions = CommittedOffsetRevisions.forCache(cache)
    revisions.advance('topic', 0)
    const captured = revisions.capture('topic', 0)

    cache.topic[0] = '99'
    revisions.advance('topic', 0)

    expect(cache.topic[0]).toBe('99')
    expect(revisions.isCurrent('topic', 0, captured)).toBe(false)
  })

  test('clearTopic makes the position stale and drops the topic cell entries', () => {
    const revisions = CommittedOffsetRevisions.forCache({})
    const initial = revisions.capture('topic', 0)
    revisions.advance('topic', 0)
    const captured = [0, 1, 2].map((partition) => revisions.capture('topic', partition))
    const other = revisions.capture('other', 0)

    revisions.clearTopic('topic')

    expect(revisions.isCurrent('topic', 0, initial)).toBe(false)
    for (const [partition, stamp] of captured.entries()) {
      expect(revisions.isCurrent('topic', partition, stamp)).toBe(false)
    }
    expect(revisions.cells.has('topic')).toBe(false)
    expect(revisions.cells.size).toBe(1)
    expect(revisions.isCurrent('other', 0, other)).toBe(true)
    const fresh = revisions.capture('topic', 0)
    expect(captured).not.toContain(fresh)
    expect(revisions.isCurrent('topic', 0, fresh)).toBe(true)
  })

  test('an advance on another cell or topic keeps the position current', () => {
    const revisions = CommittedOffsetRevisions.forCache({})
    revisions.advance('topic', 0)
    const captured = revisions.capture('topic', 0)

    revisions.capture('topic', 1)
    revisions.advance('topic', 1)
    revisions.capture('other', 0)
    revisions.advance('other', 0)
    revisions.clearTopic('other')

    expect(revisions.isCurrent('topic', 0, captured)).toBe(true)
  })

  test('repeated captures of one cell share one stamp and add no entries', () => {
    const revisions = CommittedOffsetRevisions.forCache({})
    const stamp = revisions.capture('topic', 0)
    const seq = revisions.seq
    const topics = [...revisions.cells.keys()]
    const partitions = [...revisions.cells.get('topic')]

    for (let index = 0; index < 100; index++) {
      expect(revisions.capture('topic', 0)).toBe(stamp)
      expect(revisions.capture('topic', '0')).toBe(stamp)
    }

    expect(revisions.seq).toBe(seq)
    expect([...revisions.cells.keys()]).toEqual(topics)
    expect([...revisions.cells.get('topic')]).toEqual(partitions)
  })

  test('a captured cell keeps one stamp until it is published, cleared or retired', () => {
    const cache = {}
    const revisions = CommittedOffsetRevisions.forCache(cache)

    for (let partition = 0; partition < 100; partition++) {
      const captured = revisions.capture('topic', partition)
      expect(revisions.isCurrent('topic', partition, captured)).toBe(true)
    }
    expect(CommittedOffsetRevisions.sizeOf(cache)).toEqual({ topics: 1, cells: 100 })

    for (let partition = 0; partition < 50; partition++) revisions.advance('topic', partition)
    expect(CommittedOffsetRevisions.sizeOf(cache)).toEqual({ topics: 1, cells: 50 })
    revisions.clearTopic('topic')
    expect(CommittedOffsetRevisions.sizeOf(cache)).toEqual({ topics: 0, cells: 0 })
  })
})
