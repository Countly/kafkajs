// The admin's internal consumer is real; only its group description is stubbed (the scripted
// cluster has no DescribeGroups pair), so the group reads as empty.
jest.mock('../../consumer', () => {
  const createConsumer = jest.requireActual('../../consumer')
  return (params) =>
    Object.assign(createConsumer(params), {
      describeGroup: async () => ({ state: 'Empty', members: [] }),
    })
})

const { Kafka } = require('../../../index')
const { createScriptedCluster, groupScript } = require('../../../testHelpers/scriptedCluster')
const { TOPIC } = require('../../consumer/__tests__/fixtures/fetchHarness')

describe('Admin > setOffsets and resetOffsets > on the scripted cluster', () => {
  let scripted, admin

  beforeEach(async () => {
    scripted = groupScript(
      createScriptedCluster({ nodes: [{ id: 0, host: 'node', port: 9092 }] }),
      {
        committed: { 0: '13' },
        batches: 0,
      }
    )
    admin = new Kafka(scripted.kafkaOptions()).admin()
    await admin.connect()
  })

  afterEach(async () => {
    await admin.disconnect()
    scripted.assertClean()
  })

  /**
   * @returns {Array<{ topic: string, partition: number, offset: string }>} every partition offset
   *   the cluster was sent in an OffsetCommit, in the order sent
   */
  const committed = () =>
    scripted
      .requests()
      .filter(({ api }) => api === 'OffsetCommit')
      .flatMap(({ fields }) =>
        fields.topics.flatMap(({ topic, partitions }) =>
          partitions.map(({ partition, offset }) => ({ topic, partition, offset: String(offset) }))
        )
      )

  test.each([
    [false, '-1'],
    [true, '-2'],
  ])('earliest=%s commits the sentinel %s as the group offset', async (earliest, sentinel) => {
    await admin.resetOffsets({ groupId: 'reset-group', topic: TOPIC, earliest })

    expect(committed()).toEqual([{ topic: TOPIC, partition: 0, offset: sentinel }])
    expect(scripted.requests().find(({ api }) => api === 'OffsetCommit').fields).toMatchObject({
      groupId: 'reset-group',
      groupGenerationId: 1,
      memberId: 'member-1',
      groupInstanceId: null,
      topics: [
        {
          topic: TOPIC,
          partitions: [{ partition: 0, offset: sentinel, leaderEpoch: -1, metadata: null }],
        },
      ],
    })
    const apis = scripted.requests().map(({ api }) => api)
    expect(apis.indexOf('OffsetCommit')).toBeLessThan(apis.indexOf('LeaveGroup'))
  })

  test('setOffsets still commits a concrete offset as given', async () => {
    await admin.setOffsets({
      groupId: 'set-group',
      topic: TOPIC,
      partitions: [{ partition: 0, offset: '7' }],
    })

    expect(committed()).toEqual([{ topic: TOPIC, partition: 0, offset: '7' }])
  })

  test('setOffsets drops a sentinel for a partition the topic does not have', async () => {
    await admin.setOffsets({
      groupId: 'set-group',
      topic: TOPIC,
      partitions: [
        { partition: 0, offset: '-1' },
        { partition: 5, offset: '-2' },
      ],
    })

    expect(committed()).toEqual([{ topic: TOPIC, partition: 0, offset: '-1' }])
  })

  test('setOffsets commits the last entry for a partition, a concrete one after a sentinel', async () => {
    await admin.setOffsets({
      groupId: 'set-group',
      topic: TOPIC,
      partitions: [
        { partition: 0, offset: '-1' },
        { partition: 0, offset: '7' },
      ],
    })

    expect(committed()).toEqual([{ topic: TOPIC, partition: 0, offset: '7' }])
  })

  test('a sentinel commit still in flight when the consumer reaches its first fetch finishes before the member leaves', async () => {
    await admin.disconnect()
    admin = new Kafka(scripted.kafkaOptions({ requestTimeout: 15000 })).admin()
    await admin.connect()
    const gate = scripted.schedule.hold({ api: 'OffsetCommit' })
    const reset = admin.resetOffsets({ groupId: 'reset-group', topic: TOPIC })
    await gate.arrived()
    await new Promise((resolve) => setTimeout(resolve, 5500))
    gate.release()
    await reset

    const requests = scripted.requests()
    const commit = requests.find(({ api }) => api === 'OffsetCommit')
    const leave = requests.find(({ api }) => api === 'LeaveGroup')
    expect(commit.answeredOrder).toBeLessThan(leave.order)
  })

  test('a rejected sentinel commit rejects setOffsets after the internal consumer stopped', async () => {
    scripted.answer('OffsetCommit', ({ topics }) => ({
      topics: topics.map(({ topic, partitions }) => ({
        topic,
        partitions: partitions.map(({ partition }) => ({ partition, errorCode: 30 })),
      })),
    }))

    await expect(
      admin.resetOffsets({ groupId: 'reset-group', topic: TOPIC })
    ).rejects.toMatchObject({ type: 'GROUP_AUTHORIZATION_FAILED' })

    expect(scripted.requests().map(({ api }) => api)).toContain('LeaveGroup')
  })
})
