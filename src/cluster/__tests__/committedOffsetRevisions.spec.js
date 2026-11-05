const CommittedOffsetRevisions = require('../committedOffsetRevisions')

describe('Cluster > CommittedOffsetRevisions', () => {
  test('capture then advance makes the position stale', () => {
    const revisions = CommittedOffsetRevisions.forCache({})
    const captured = revisions.capture()

    expect(captured).toBe(0)
    expect(revisions.isCurrent('topic', 0, captured)).toBe(true)
    revisions.advance('topic', 0)
    expect(revisions.isCurrent('topic', '0', captured)).toBe(false)
    expect(revisions.isCurrent('topic', 0, revisions.capture())).toBe(true)
  })

  test('a same-value publication makes the position stale', () => {
    const cache = { topic: { 0: '99' } }
    const revisions = CommittedOffsetRevisions.forCache(cache)
    revisions.advance('topic', 0)
    const captured = revisions.capture()

    cache.topic[0] = '99'
    revisions.advance('topic', 0)

    expect(cache.topic[0]).toBe('99')
    expect(revisions.isCurrent('topic', 0, captured)).toBe(false)
  })

  test('clearTopic makes the position stale and drops the topic cell entries', () => {
    const revisions = CommittedOffsetRevisions.forCache({})
    const initial = revisions.capture()
    revisions.advance('topic', 0)
    revisions.advance('topic', 1)
    revisions.advance('other', 0)
    const captured = revisions.capture()

    revisions.clearTopic('topic')

    expect(revisions.isCurrent('topic', 0, initial)).toBe(false)
    expect(revisions.isCurrent('topic', 0, captured)).toBe(false)
    expect(revisions.isCurrent('topic', 1, captured)).toBe(false)
    expect(revisions.isCurrent('topic', 2, captured)).toBe(false)
    expect(revisions.cells.has('topic')).toBe(false)
    expect(revisions.cells.size).toBe(1)
    expect(revisions.topics.get('topic')).toBe(revisions.seq)
    expect(revisions.isCurrent('topic', 0, revisions.capture())).toBe(true)
  })

  test('an advance on another cell or topic keeps the position current', () => {
    const revisions = CommittedOffsetRevisions.forCache({})
    revisions.advance('topic', 0)
    const captured = revisions.capture()

    revisions.advance('topic', 1)
    revisions.advance('other', 0)
    revisions.clearTopic('other')

    expect(revisions.isCurrent('topic', 0, captured)).toBe(true)
  })

  test('capture allocates nothing', () => {
    const revisions = CommittedOffsetRevisions.forCache({})
    revisions.advance('topic', 0)
    revisions.clearTopic('other')
    const seq = revisions.seq
    const cells = [...revisions.cells]
    const partitions = [...revisions.cells.get('topic')]
    const topics = [...revisions.topics]

    for (let index = 0; index < 100; index++) {
      expect(revisions.capture()).toBe(seq)
    }

    expect([...revisions.cells]).toEqual(cells)
    expect([...revisions.cells.get('topic')]).toEqual(partitions)
    expect([...revisions.topics]).toEqual(topics)
  })

  test('private positions that are never published leave no state', () => {
    const revisions = CommittedOffsetRevisions.forCache({})

    for (let partition = 0; partition < 100; partition++) {
      const captured = revisions.capture()
      expect(revisions.isCurrent('topic', partition, captured)).toBe(true)
    }

    expect(revisions.seq).toBe(0)
    expect(revisions.cells.size).toBe(0)
    expect(revisions.topics.size).toBe(0)
  })
})
