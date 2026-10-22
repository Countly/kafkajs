const OffsetOperations = require('../offsetOperations')
const ResetObligations = require('../resetObligations')

const T = 'test-topic'
const cell = (partition = 0, topic = T) => ({ topic, partition })

describe('Consumer > OffsetOperations', () => {
  let operations

  beforeEach(() => {
    operations = new OffsetOperations()
  })

  describe('reads fill and never supersede', () => {
    test('a read is not superseded while nothing was applied to its cell', () => {
      const read = operations.begin([cell()])

      expect(operations.isSuperseded(read, T, 0)).toBe(false)
    })

    test('a read is superseded by a mutation applied after it began', () => {
      const read = operations.begin([cell()])
      const mutation = operations.begin([cell()], { mutation: true })

      operations.apply(mutation, T, 0)

      expect(operations.isSuperseded(read, T, 0)).toBe(true)
    })

    test('a read that began after a mutation was applied is not superseded by it', () => {
      const mutation = operations.begin([cell()], { mutation: true })
      operations.apply(mutation, T, 0)

      const read = operations.begin([cell()])

      expect(operations.isSuperseded(read, T, 0)).toBe(false)
    })

    test('a read completing does not supersede another read or a mutation', () => {
      const first = operations.begin([cell()])
      const second = operations.begin([cell()])
      const mutation = operations.begin([cell()], { mutation: true })

      operations.end(first)

      expect(operations.isSuperseded(second, T, 0)).toBe(false)
      expect(operations.isSuperseded(mutation, T, 0)).toBe(false)
    })

    test('a read is fenced by a mutation applied to its cell even when it began after that mutation began', () => {
      const mutation = operations.begin([cell()], { mutation: true })
      const read = operations.begin([cell()])

      operations.apply(mutation, T, 0)

      expect(operations.isSuperseded(read, T, 0)).toBe(true)
    })

    test('a read is superseded only on the cells a mutation was applied to', () => {
      const read = operations.begin([cell(0), cell(1)])
      const mutation = operations.begin([cell(1)], { mutation: true })

      operations.apply(mutation, T, 1)

      expect(operations.isSuperseded(read, T, 0)).toBe(false)
      expect(operations.isSuperseded(read, T, 1)).toBe(true)
    })
  })

  describe('mutations are ordered by when they began', () => {
    test('an older mutation applied first does not supersede a newer one that already began', () => {
      const older = operations.begin([cell()], { mutation: true })
      const newer = operations.begin([cell()], { mutation: true })

      operations.apply(older, T, 0)

      expect(operations.isSuperseded(newer, T, 0)).toBe(false)
    })

    test('a newer mutation applied first supersedes the older one, whichever answer arrived first', () => {
      const older = operations.begin([cell()], { mutation: true })
      const newer = operations.begin([cell()], { mutation: true })

      operations.apply(newer, T, 0)

      expect(operations.isSuperseded(older, T, 0)).toBe(true)
      expect(operations.isSuperseded(newer, T, 0)).toBe(false)
    })

    test('a mutation that began after another was applied is not superseded by it', () => {
      const older = operations.begin([cell()], { mutation: true })
      operations.apply(older, T, 0)

      const newer = operations.begin([cell()], { mutation: true })

      expect(operations.isSuperseded(newer, T, 0)).toBe(false)
    })

    test('a mutation does not supersede itself once applied', () => {
      const mutation = operations.begin([cell()], { mutation: true })

      operations.apply(mutation, T, 0)

      expect(operations.isSuperseded(mutation, T, 0)).toBe(false)
    })

    test('a mutation is not superseded by a read, which never applies', () => {
      const mutation = operations.begin([cell()], { mutation: true })
      const read = operations.begin([cell()])

      operations.end(read)

      expect(operations.isSuperseded(mutation, T, 0)).toBe(false)
    })

    test('a newer mutation applied to one cell leaves the older ticket current on its other cells', () => {
      const older = operations.begin([cell(0), cell(1)], { mutation: true })
      const newer = operations.begin([cell(0)], { mutation: true })

      operations.apply(newer, T, 0)

      expect(operations.isSuperseded(older, T, 0)).toBe(true)
      expect(operations.isSuperseded(older, T, 1)).toBe(false)
    })
  })

  describe('barriers fence by application time', () => {
    test.each([
      ['a read', {}],
      ['a mutation', { mutation: true }],
    ])('%s begun before a touch of its cell is superseded', (_, options) => {
      const ticket = operations.begin([cell()], options)

      operations.touch(T, 0)

      expect(operations.isSuperseded(ticket, T, 0)).toBe(true)
    })

    test.each([
      ['a read', {}],
      ['a mutation', { mutation: true }],
    ])('%s begun before a touch of its topic is superseded on every covered cell', (_, options) => {
      const ticket = operations.begin([cell(0), cell(1)], options)

      operations.touchTopic(T)

      expect(operations.isSuperseded(ticket, T, 0)).toBe(true)
      expect(operations.isSuperseded(ticket, T, 1)).toBe(true)
    })

    test('an operation that began after the barrier is not superseded by it', () => {
      const earlier = operations.begin([cell()])
      operations.touchTopic(T)

      const later = operations.begin([cell()], { mutation: true })

      expect(operations.isSuperseded(earlier, T, 0)).toBe(true)
      expect(operations.isSuperseded(later, T, 0)).toBe(false)
    })

    test('a touch reaches only its own cell and a topic touch only its own topic', () => {
      const ticket = operations.begin([cell(0), cell(1), cell(0, 'other')])

      operations.touch(T, 0)
      operations.touchTopic('unrelated')

      expect(operations.isSuperseded(ticket, T, 0)).toBe(true)
      expect(operations.isSuperseded(ticket, T, 1)).toBe(false)
      expect(operations.isSuperseded(ticket, 'other', 0)).toBe(false)
    })
  })

  describe('owner barriers fence only their owner', () => {
    const owner = { name: 'owner' }
    const other = { name: 'other' }

    test.each([
      ['a read', {}],
      ['a mutation', { mutation: true }],
    ])('%s of the owner begun before the touch is superseded', (_, options) => {
      const ticket = operations.begin([cell()], { ...options, owner })

      operations.touchOwned(owner, T, 0)

      expect(operations.isSuperseded(ticket, T, 0)).toBe(true)
    })

    test('the touch leaves another owner, an ownerless ticket and other cells of the owner alone', () => {
      const mine = operations.begin([cell(0), cell(1)], { owner })
      const theirs = operations.begin([cell(0)], { owner: other })
      const commit = operations.begin([cell(0)], { mutation: true })
      const read = operations.begin([cell(0)])

      operations.touchOwned(owner, T, 0)

      expect(operations.isSuperseded(mine, T, 0)).toBe(true)
      expect(operations.isSuperseded(mine, T, 1)).toBe(false)
      expect(operations.isSuperseded(theirs, T, 0)).toBe(false)
      expect(operations.isSuperseded(commit, T, 0)).toBe(false)
      expect(operations.isSuperseded(read, T, 0)).toBe(false)
    })

    test('a ticket of the owner that began after the touch is not superseded by it', () => {
      const earlier = operations.begin([cell()], { owner })
      operations.touchOwned(owner, T, 0)

      const later = operations.begin([cell()], { owner })

      expect(operations.isSuperseded(earlier, T, 0)).toBe(true)
      expect(operations.isSuperseded(later, T, 0)).toBe(false)
    })

    test('a shared touch still fences the tickets of every owner', () => {
      const ticket = operations.begin([cell()], { owner })

      operations.touch(T, 0)

      expect(operations.isSuperseded(ticket, T, 0)).toBe(true)
    })

    test('a null owner never fences the ownerless tickets of the other participants', () => {
      const ticket = operations.begin([cell()])

      operations.touchOwned(null, T, 0)

      expect(operations.isSuperseded(ticket, T, 0)).toBe(false)
    })

    test('the mark retains nothing for a cell nobody covers and leaves with the cell', () => {
      operations.touchOwned(owner, T, 0)
      expect(operations.size()).toBe(0)

      const first = operations.begin([cell()], { owner })
      operations.touchOwned(owner, T, 0)
      operations.end(first)
      expect(operations.size()).toBe(0)

      const second = operations.begin([cell()], { owner })
      expect(operations.isSuperseded(second, T, 0)).toBe(false)
    })
  })

  describe('fence and discharge are the two halves of apply', () => {
    const attachedTable = () => {
      const obligations = new ResetObligations()
      obligations.attachTo(operations)
      obligations.owe(T, 'id-b')
      return obligations
    }

    test('a fence supersedes what an apply would and settles no obligation', () => {
      const obligations = attachedTable()
      const read = operations.begin([cell()])
      const older = operations.begin([cell()], { mutation: true })
      const mutation = operations.begin([cell()], { mutation: true })

      operations.fence(mutation, T, 0)

      expect(operations.isSuperseded(read, T, 0)).toBe(true)
      expect(operations.isSuperseded(older, T, 0)).toBe(true)
      expect(operations.isSuperseded(mutation, T, 0)).toBe(false)
      expect(obligations.isOwed(T, 0)).toBe(true)
    })

    test('a fence retains nothing for a cell nobody covers', () => {
      const mutation = operations.begin([cell()], { mutation: true })
      operations.end(mutation)

      operations.fence(mutation, T, 0)

      expect(operations.size()).toBe(0)
    })

    test('a discharge settles every attached table and supersedes nothing', () => {
      const first = attachedTable()
      const second = attachedTable()
      const read = operations.begin([cell()])
      const mutation = operations.begin([cell()], { mutation: true })

      operations.discharge(T, '0')

      expect(first.isOwed(T, 0)).toBe(false)
      expect(second.isOwed(T, 0)).toBe(false)
      expect(operations.isSuperseded(read, T, 0)).toBe(false)
      expect(operations.isSuperseded(mutation, T, 0)).toBe(false)
    })

    test('a fence then a discharge leave the cell as an apply does', () => {
      const obligations = attachedTable()
      const read = operations.begin([cell()])
      const mutation = operations.begin([cell()], { mutation: true })

      operations.fence(mutation, T, 0)
      operations.discharge(T, 0)

      expect(operations.isSuperseded(read, T, 0)).toBe(true)
      expect(obligations.isOwed(T, 0)).toBe(false)
    })
  })

  describe('partitions given as strings', () => {
    test('a string partition is the same cell as its number', () => {
      const read = operations.begin([cell('0')])
      const mutation = operations.begin([cell(0)], { mutation: true })

      operations.apply(mutation, T, '0')

      expect(operations.isSuperseded(read, T, 0)).toBe(true)
      expect(operations.size()).toBe(1)
    })
  })

  describe('retention follows the open tickets', () => {
    test('a cell is retained from begin to end and released by the last covering ticket', () => {
      const first = operations.begin([cell(0), cell(1)])
      const second = operations.begin([cell(1)])
      expect(operations.size()).toBe(2)

      operations.end(first)
      expect(operations.size()).toBe(1)

      operations.end(second)
      expect(operations.size()).toBe(0)
    })

    test('end is idempotent', () => {
      const first = operations.begin([cell()])
      const second = operations.begin([cell()])

      operations.end(first)
      operations.end(first)

      expect(operations.size()).toBe(1)
      operations.end(second)
      expect(operations.size()).toBe(0)
    })

    test('stamps for cells and topics nobody covers retain nothing', () => {
      const mutation = operations.begin([cell()], { mutation: true })
      operations.end(mutation)

      operations.apply(mutation, T, 0)
      operations.touch(T, 0)
      operations.touchTopic(T)
      operations.touchTopic('never-covered')

      expect(operations.size()).toBe(0)
    })

    test('one stuck ticket holds only its own cells while thousands of unrelated topics come and go', () => {
      const stuck = operations.begin([cell()])

      for (let index = 0; index < 5000; index++) {
        const name = `topic-${index}`
        const ticket = operations.begin([cell(0, name)], { mutation: true })
        operations.touchTopic(name)
        operations.apply(ticket, name, 0)
        operations.end(ticket)
      }

      expect(operations.size()).toBe(1)
      operations.touchTopic(T)
      expect(operations.isSuperseded(stuck, T, 0)).toBe(true)
      operations.end(stuck)
      expect(operations.size()).toBe(0)
    })

    test('a state that was released starts afresh for the next ticket', () => {
      const first = operations.begin([cell()], { mutation: true })
      operations.apply(first, T, 0)
      operations.touch(T, 0)
      operations.end(first)

      const second = operations.begin([cell()], { mutation: true })

      expect(operations.isSuperseded(second, T, 0)).toBe(false)
    })

    test('a cell no ticket covers is never reported superseded', () => {
      const ticket = operations.begin([cell()])
      operations.end(ticket)

      expect(operations.isSuperseded(ticket, T, 0)).toBe(false)
      expect(operations.isSuperseded(ticket, 'other', 3)).toBe(false)
    })
  })

  describe('attached obligations', () => {
    test('an applied mutation discharges the matching partition of every attached table', () => {
      const first = new ResetObligations()
      const second = new ResetObligations()
      first.attachTo(operations)
      second.attachTo(operations)
      first.owe(T, 'id-b')
      second.owe(T, 'id-b')
      const mutation = operations.begin([cell(0)], { mutation: true })

      operations.apply(mutation, T, 0)

      expect(first.isOwed(T, 0)).toBe(false)
      expect(second.isOwed(T, 0)).toBe(false)
    })

    test('a table that was detached is no longer discharged', () => {
      const obligations = new ResetObligations()
      obligations.attachTo(operations)
      obligations.owe(T, 'id-b')
      obligations.clear()
      obligations.owe(T, 'id-b')
      const mutation = operations.begin([cell(0)], { mutation: true })

      operations.apply(mutation, T, 0)

      expect(obligations.isOwed(T, 0)).toBe(true)
    })

    test('attach returns the function that unlinks the table', () => {
      const obligations = { discharge: jest.fn() }
      const unlink = operations.attach(obligations)
      const mutation = operations.begin([cell(0)], { mutation: true })

      operations.apply(mutation, T, 0)
      unlink()
      operations.apply(mutation, T, 0)

      expect(obligations.discharge).toHaveBeenCalledTimes(1)
      expect(obligations.discharge).toHaveBeenCalledWith(T, 0)
    })

    test('a superseded mutation that its caller does not apply discharges nothing', () => {
      const obligations = new ResetObligations()
      obligations.attachTo(operations)
      obligations.owe(T, 'id-b')
      const older = operations.begin([cell(0)], { mutation: true })
      const newer = operations.begin([cell(0)], { mutation: true })
      operations.apply(newer, T, 0)
      obligations.owe(T, 'id-c')

      if (!operations.isSuperseded(older, T, 0)) {
        operations.apply(older, T, 0)
      }

      expect(obligations.isOwed(T, 0)).toBe(true)
    })
  })
})
