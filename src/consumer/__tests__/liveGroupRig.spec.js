jest.mock('../index')
jest.mock('../../cluster')

const createConsumer = require('../index')
const ISOLATION_LEVEL = require('../../protocol/isolationLevel')
const { JournalMember } = require('./fixtures/liveGroupRig')

const EVENTS = Object.fromEntries(
  [
    'GROUP_JOIN',
    'REBALANCING',
    'PARTITIONS_ASSIGNED',
    'PARTITIONS_REVOKED',
    'PARTITIONS_LOST',
    'COMMIT_OFFSETS',
    'CRASH',
  ].map((name) => [name, name])
)

const memberWith = (options) =>
  new JournalMember({
    scope: new AbortController().signal,
    cleanup: { add: jest.fn() },
    corpus: { prefix: 'rig', id: '1', brokers: ['localhost:9092'], topic: 'rig-topic' },
    name: 'member',
    groupId: 'rig-group',
    ...options,
  })

describe('Consumer > live group rig > fetch isolation', () => {
  beforeEach(() => {
    jest.clearAllMocks()
    createConsumer.mockReturnValue({ events: EVENTS, on: jest.fn() })
  })

  it.each([
    ['read_committed', ISOLATION_LEVEL.READ_COMMITTED],
    ['read_uncommitted', ISOLATION_LEVEL.READ_UNCOMMITTED],
  ])('builds the %s member with the fetch isolation the client takes', (_, isolationLevel) => {
    memberWith({ isolationLevel })

    expect(createConsumer.mock.calls[0][0].isolationLevel).toBe(isolationLevel)
  })

  it('reads committed when no isolation level is given', () => {
    memberWith({})

    expect(createConsumer.mock.calls[0][0].isolationLevel).toBe(ISOLATION_LEVEL.READ_COMMITTED)
  })

  it('refuses the consumer option the client ignores', () => {
    expect(() => memberWith({ consumer: { isolationLevel: 0 } })).toThrow(/isolationLevel/)
    expect(createConsumer).not.toHaveBeenCalled()
  })
})
