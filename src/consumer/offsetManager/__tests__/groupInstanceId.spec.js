const OffsetManager = require('../index')
const InstrumentationEventEmitter = require('../../../instrumentation/emitter')

/**
 * End-to-end proof that `groupInstanceId` (KIP-345 static membership) reaches every
 * `Broker#offsetCommit` call `OffsetManager` makes - `commitOffsets`, `seek` and
 * `setDefaultOffset` - completing the plumbing OffsetCommit v7 added. The wire-level half of
 * this fact is covered by `src/protocol/requests/offsetCommit/v7/request.spec.js` and
 * `v8/request.spec.js`.
 */
describe('Consumer > OffsetManager > groupInstanceId', () => {
  const topic = 'topic-1'
  const groupId = 'groupId'
  const generationId = 'generationId'
  const memberId = 'memberId'

  const buildOffsetManager = (groupInstanceId) => {
    const memberAssignment = { [topic]: [0] }
    const mockCluster = {
      committedOffsets: jest.fn(() => ({})),
      defaultOffset: jest.fn(() => '0'),
    }
    const mockCoordinator = { offsetCommit: jest.fn() }

    const offsetManager = new OffsetManager({
      cluster: mockCluster,
      memberAssignment,
      topicConfigurations: { [topic]: { fromBeginning: false } },
      autoCommit: true,
      groupId,
      generationId,
      memberId,
      groupInstanceId,
      instrumentationEmitter: new InstrumentationEventEmitter(),
    })
    offsetManager.getCoordinator = jest.fn(() => mockCoordinator)

    return { offsetManager, mockCoordinator }
  }

  const payloadGroupInstanceId = (mockCoordinator) =>
    mockCoordinator.offsetCommit.mock.calls.map((call) => call[0].groupInstanceId)

  it('carries the configured groupInstanceId on commitOffsets, seek and setDefaultOffset', async () => {
    const { offsetManager, mockCoordinator } = buildOffsetManager('instance-1')

    offsetManager.resolveOffset({ topic, partition: 0, offset: '9' })
    await offsetManager.commitOffsets()
    await offsetManager.seek({ topic, partition: 0, offset: '5' })
    await offsetManager.setDefaultOffset({ topic, partition: 0 })

    expect(mockCoordinator.offsetCommit).toHaveBeenCalledTimes(3)
    expect(payloadGroupInstanceId(mockCoordinator)).toEqual([
      'instance-1',
      'instance-1',
      'instance-1',
    ])
  })

  it('carries null when static membership is not configured', async () => {
    const { offsetManager, mockCoordinator } = buildOffsetManager(undefined)

    offsetManager.resolveOffset({ topic, partition: 0, offset: '9' })
    await offsetManager.commitOffsets()
    await offsetManager.seek({ topic, partition: 0, offset: '5' })
    await offsetManager.setDefaultOffset({ topic, partition: 0 })

    expect(mockCoordinator.offsetCommit).toHaveBeenCalledTimes(3)
    expect(payloadGroupInstanceId(mockCoordinator)).toEqual([null, null, null])
  })
})
