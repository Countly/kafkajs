const ResetObligations = require('../resetObligations')
const OffsetOperations = require('../offsetOperations')

const T = 'test-topic'
const OLD = 'id-old'
const NEW = 'id-new'

describe('Consumer > ResetObligations', () => {
  let obligations

  beforeEach(() => {
    obligations = new ResetObligations()
  })

  test('a recreated topic owes a reset on every partition, whichever the table is asked about', () => {
    obligations.owe(T, NEW)

    expect(obligations.isOwed(T, 0)).toBe(true)
    expect(obligations.isOwed(T, 7)).toBe(true)
    expect(obligations.isOwed('other', 0)).toBe(false)
    expect(obligations.size()).toBe(1)
  })

  test('a newer recreation replaces the older entry and its discharged partitions', () => {
    obligations.owe(T, OLD)
    obligations.discharge(T, 0)

    obligations.owe(T, NEW)

    expect(obligations.current(T)).toEqual(NEW)
    expect(obligations.isOwed(T, 0)).toBe(true)
  })

  describe('current', () => {
    test('returns the stamp, and nothing for a topic that owes no reset', () => {
      obligations.owe(T, NEW)

      expect(obligations.current(T, undefined)).toEqual(NEW)
      expect(obligations.current(T, NEW)).toEqual(NEW)
      expect(obligations.current('other', NEW)).toBeUndefined()
    })

    test('an unknown cached id keeps the entry, a cleared cache is not a recreation', () => {
      obligations.owe(T, NEW)

      expect(obligations.current(T, undefined)).toEqual(NEW)
      expect(obligations.size()).toBe(1)
    })

    test('a known cached id that differs from the stamp disposes of the entry', () => {
      obligations.owe(T, OLD)

      expect(obligations.current(T, NEW)).toBeUndefined()
      expect(obligations.size()).toBe(0)
      expect(obligations.isOwed(T, 0)).toBe(false)
    })
  })

  describe('discharge', () => {
    test('settles one partition and leaves the others owed', () => {
      obligations.owe(T, NEW)

      obligations.discharge(T, 0)

      expect(obligations.isOwed(T, 0)).toBe(false)
      expect(obligations.isOwed(T, 1)).toBe(true)
    })

    test('an unknown mutation id settles', () => {
      obligations.owe(T, NEW)

      obligations.discharge(T, 0, undefined)

      expect(obligations.isOwed(T, 0)).toBe(false)
    })

    test('a known mutation id equal to the stamp settles', () => {
      obligations.owe(T, NEW)

      obligations.discharge(T, 0, NEW)

      expect(obligations.isOwed(T, 0)).toBe(false)
    })

    test('a known mutation id that differs from the known stamp settles nothing', () => {
      obligations.owe(T, NEW)

      obligations.discharge(T, 0, OLD)

      expect(obligations.isOwed(T, 0)).toBe(true)
    })

    test('a topic that owes nothing ignores it', () => {
      obligations.discharge('other', 0)

      expect(obligations.size()).toBe(0)
    })
  })

  describe('settle', () => {
    test('deletes the entry once every assigned partition has discharged it', () => {
      obligations.owe(T, NEW)
      obligations.discharge(T, 0)
      obligations.settle(T, new Set([0, 1]))
      expect(obligations.size()).toBe(1)

      obligations.discharge(T, 1)
      obligations.settle(T, new Set([0, 1]))

      expect(obligations.size()).toBe(0)
    })

    test('a partial or empty assignment does not delete a debt another known partition owes', () => {
      obligations.owe(T, NEW)
      obligations.discharge(T, 0)

      obligations.settle(T, [], [0, 1])
      obligations.settle(T, [0], [0, 1])
      expect(obligations.size()).toBe(1)

      obligations.discharge(T, 1)
      obligations.settle(T, [], [0, 1])
      expect(obligations.size()).toBe(0)
    })

    test('metadata that could not be verified keeps the debt', () => {
      obligations.owe(T, NEW)
      obligations.discharge(T, 0)

      obligations.settle(T, [0], [])
      expect(obligations.size()).toBe(1)

      obligations.settle(T, [0])
      expect(obligations.size()).toBe(0)
    })

    test('a partition no longer assigned does not hold the entry', () => {
      obligations.owe(T, NEW)
      obligations.discharge(T, 1)

      obligations.settle(T, [1])

      expect(obligations.size()).toBe(0)
    })

    test('a topic that owes nothing is a no-op', () => {
      obligations.settle('other', [0])

      expect(obligations.size()).toBe(0)
    })
  })

  describe('linking to the ledger', () => {
    test('clear drops every entry and unlinks the table', () => {
      const operations = new OffsetOperations()
      obligations.attachTo(operations)
      obligations.owe(T, NEW)

      obligations.clear()
      obligations.owe(T, NEW)
      const mutation = operations.begin([{ topic: T, partition: 0 }], { mutation: true })
      operations.apply(mutation, T, 0)

      expect(obligations.isOwed(T, 0)).toBe(true)
    })

    test('unlink keeps every entry, stops discharges from the ledger and lets the next link resume them', () => {
      const operations = new OffsetOperations()
      obligations.attachTo(operations)
      obligations.owe(T, NEW)

      obligations.unlink()
      const unlinked = operations.begin([{ topic: T, partition: 0 }], { mutation: true })
      operations.apply(unlinked, T, 0)
      expect(obligations.size()).toBe(1)
      expect(obligations.isOwed(T, 0)).toBe(true)

      obligations.attachTo(operations)
      const linked = operations.begin([{ topic: T, partition: 0 }], { mutation: true })
      operations.apply(linked, T, 0)
      expect(obligations.isOwed(T, 0)).toBe(false)
    })

    test('attaching twice links once, and a second ledger replaces the first', () => {
      const first = new OffsetOperations()
      const second = new OffsetOperations()
      obligations.attachTo(first)
      obligations.attachTo(first)
      obligations.attachTo(second)
      obligations.owe(T, NEW)

      first.apply(first.begin([{ topic: T, partition: 0 }], { mutation: true }), T, 0)
      expect(obligations.isOwed(T, 0)).toBe(true)
      second.apply(second.begin([{ topic: T, partition: 0 }], { mutation: true }), T, 0)

      expect(obligations.isOwed(T, 0)).toBe(false)
    })

    test('clear and unlink on a table that was never linked are harmless', () => {
      expect(() => obligations.clear()).not.toThrow()
      expect(() => obligations.unlink()).not.toThrow()
    })
  })
})
