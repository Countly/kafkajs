const { Kafka } = require('../../../index')
const { createScriptedCluster, groupScript } = require('../../../testHelpers/scriptedCluster')
const { TOPIC } = require('../../consumer/__tests__/fixtures/fetchHarness')
const { describeGroupAs } = require('./fixtures/describeGroupAs')

describe('Admin > setOffsets and resetOffsets > on the scripted cluster', () => {
  let scripted, admin, described

  beforeEach(async () => {
    described = describeGroupAs()
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
    described.restore()
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

  /** @returns {boolean} every OffsetCommit carried no member id and generation -1 */
  const memberless = () =>
    scripted
      .requests()
      .filter(({ api }) => api === 'OffsetCommit')
      .every(({ fields }) => fields.memberId === '' && fields.groupGenerationId === -1)

  test.each([
    [false, '-1'],
    [true, '-2'],
  ])('earliest=%s commits the sentinel %s as the group offset', async (earliest, sentinel) => {
    await admin.resetOffsets({ groupId: 'reset-group', topic: TOPIC, earliest })

    expect(committed()).toEqual([{ topic: TOPIC, partition: 0, offset: sentinel }])
    expect(scripted.requests().find(({ api }) => api === 'OffsetCommit').fields).toMatchObject({
      groupId: 'reset-group',
      groupGenerationId: -1,
      memberId: '',
      groupInstanceId: null,
      topics: [
        {
          topic: TOPIC,
          partitions: [{ partition: 0, offset: sentinel, leaderEpoch: -1, metadata: null }],
        },
      ],
    })
    expect(scripted.requests().map(({ api }) => api)).not.toContain('JoinGroup')
  })

  test('setOffsets still commits a concrete offset as given', async () => {
    await admin.setOffsets({
      groupId: 'set-group',
      topic: TOPIC,
      partitions: [{ partition: 0, offset: '7' }],
    })

    expect(committed()).toEqual([{ topic: TOPIC, partition: 0, offset: '7' }])
    expect(scripted.requests().find(({ api }) => api === 'OffsetCommit').fields).toMatchObject({
      groupGenerationId: -1,
      memberId: '',
      topics: [{ partitions: [{ partition: 0, offset: '7', leaderEpoch: -1, metadata: null }] }],
    })
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
    expect(memberless()).toBe(true)
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
    expect(memberless()).toBe(true)
  })

  test("a rejected sentinel commit rejects setOffsets with the coordinator's answer", async () => {
    scripted.answer('OffsetCommit', ({ topics }) => ({
      topics: topics.map(({ topic, partitions }) => ({
        topic,
        partitions: partitions.map(({ partition }) => ({ partition, errorCode: 30 })),
      })),
    }))

    const error = await admin.resetOffsets({ groupId: 'reset-group', topic: TOPIC }).catch((e) => e)

    expect(error.name).toBe('KafkaJSAggregateError')
    expect(error.errors).toEqual([
      expect.objectContaining({ type: 'GROUP_AUTHORIZATION_FAILED', topic: TOPIC, partition: 0 }),
    ])
    expect(scripted.requests().filter(({ api }) => api === 'OffsetCommit')).toHaveLength(1)
  })
})
