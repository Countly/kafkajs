const Cluster = require('../../cluster')
const { PartitionAssigners } = require('../../../index')
const { lanes } = require('../../../testHelpers/lane')
const { TOPIC_ID } = require('./fixtures/fetchHarness')
const { writeEvidence, waitFor } = require('testHelpers')
const {
  NAMES,
  GROUP,
  GROWTH_TOPIC,
  PrototypeCase,
  prototypeDescriptors,
  expectPrototypeUntouched,
  ordinary,
  until,
} = require('./fixtures/prototypeCase')

/**
 * Replay sets: durable log, none; committed offsets, the values each case commits; handler
 * delivery, the one record each case reads.
 */
describe.each(lanes())('Consumer > topics named like Object.prototype members on %s', (lane) => {
  describe.each(NAMES)('%s', (name) => {
    let run, before

    beforeEach(() => {
      before = prototypeDescriptors()
      run = new PrototypeCase(name, lane)
    })

    afterEach(() => run.close())

    test('subscribe and run deliver its records, and the assignment it reports is ordinary', async () => {
      await run.consume(await run.kafka())

      expect(run.delivered[0]).toEqual({ topic: name, partition: 0, offset: '0' })
      expect(run.joins.at(-1).memberAssignment).toEqual({ [name]: [0] })
      expect(Object.keys(run.joins.at(-1).memberAssignment)).toEqual([name])
      expect(run.assigned.at(-1).partitions).toEqual([{ topic: name, partitions: [0] }])
      expect(ordinary(run.joins.at(-1))).toBe(true)
      expect(ordinary(run.assigned.at(-1))).toBe(true)
      expectPrototypeUntouched(before)
    })

    test.each(Object.keys(PartitionAssigners))(
      'the %s assigner assigns it, and subscribe and run deliver its records',
      async (assigner) => {
        run = new PrototypeCase(name, lane, { assigner: PartitionAssigners[assigner] })
        await run.consume(await run.kafka())

        expect(run.delivered[0]).toEqual({ topic: name, partition: 0, offset: '0' })
        expect(run.assigned.at(-1).partitions).toEqual([{ topic: name, partitions: [0] }])
        expectPrototypeUntouched(before)
      }
    )

    test('pause, paused() and resume', async () => {
      const consumer = await run.consume(await run.kafka())

      consumer.pause([{ topic: name }])
      const paused = consumer.paused()
      consumer.resume([{ topic: name }])

      expect(paused).toEqual([{ topic: name, partitions: [0] }])
      expect(ordinary(paused)).toBe(true)
      expect(consumer.paused()).toEqual([])
      expectPrototypeUntouched(before)
    })

    test('commitOffsets commits it, and the commit it reports is ordinary', async () => {
      const consumer = await run.consume(await run.kafka())

      await consumer.commitOffsets([{ topic: name, partition: 0, offset: '1' }])

      const commit = run.commits.at(-1)
      expect(
        commit.topics.map(({ topic, partitions }) => ({
          topic,
          partitions: partitions.map(({ partition, offset }) => ({
            partition,
            offset: `${offset}`,
          })),
        }))
      ).toEqual([{ topic: name, partitions: [{ partition: 0, offset: '1' }] }])
      expect(ordinary(commit)).toBe(true)
      if (lane === 'fake') {
        const sent = run.scripted.requests().filter(({ api }) => api === 'OffsetCommit')
        expect(sent.at(-1).fields.topics).toEqual([
          expect.objectContaining({
            topic: name,
            partitions: [expect.objectContaining({ partition: 0, offset: '1' })],
          }),
        ])
      } else {
        const [offsets] = await run.admin.fetchOffsets({ groupId: GROUP, topics: [name] })
        expect(offsets).toEqual({
          topic: name,
          partitions: [expect.objectContaining({ offset: '1' })],
        })
      }
      expectPrototypeUntouched(before)
    })

    test('seek moves its position', async () => {
      const consumer = await run.consume(await run.kafka())

      consumer.seek({ topic: name, partition: 0, offset: '0' })

      await until(() => run.delivered.length > 1, 'the record read again')
      expect(run.delivered.map(({ offset }) => offset).slice(0, 2)).toEqual(['0', '0'])
      expectPrototypeUntouched(before)
    })

    test('a producer sends to it, and the metadata it returns is ordinary', async () => {
      const kafka = await run.kafka()
      const producer = await run.connected(kafka.producer())

      const sent = await producer.send({ topic: name, messages: [{ value: 'v' }] })

      expect(sent).toEqual([expect.objectContaining({ topicName: name, partition: 0 })])
      expect(ordinary(sent)).toBe(true)
      expectPrototypeUntouched(before)
    })

    test('transaction.sendOffsets publishes it to the consumers of the same client', async () => {
      const kafka = await run.kafka()
      const consumer = await run.consume(kafka)
      const producer = await run.connected(
        kafka.producer({ transactionalId: `prototype-${lane}`, maxInFlightRequests: 1 })
      )
      const writes = []
      const caches = []
      const mark = Cluster.prototype.markOffsetAsCommitted
      const spy = jest
        .spyOn(Cluster.prototype, 'markOffsetAsCommitted')
        .mockImplementation(function (value) {
          writes.push(value)
          caches.push(this.committedOffsets({ groupId: GROUP }))
          return mark.call(this, value)
        })

      try {
        const transaction = await producer.transaction()
        await transaction.sendOffsets({
          consumerGroupId: GROUP,
          topics: [{ topic: name, partitions: [{ partition: 0, offset: '5' }] }],
        })
        await transaction.commit()
      } finally {
        spy.mockRestore()
      }

      expect(writes).toEqual([
        expect.objectContaining({ groupId: GROUP, topic: name, partition: 0, offset: '5' }),
      ])
      expect(Object.keys(caches[0])).toContain(name)
      expect(caches[0][name]).toEqual({ 0: '5' })
      expect(consumer.paused()).toEqual([])
      if (lane === 'docker') {
        await waitFor(
          async () => {
            const [offsets] = await run.admin.fetchOffsets({ groupId: GROUP, topics: [name] })
            return offsets.partitions[0].offset === '5'
          },
          {
            maxWait: 10000,
            ignoreTimeout: false,
            timeoutMessage: `Transactional offset for ${name} did not reach 5`,
          }
        )
        writeEvidence('prototype-topics', { topic: name, sendOffsets: '5' })
      }
      expectPrototypeUntouched(before)
    })

    test('under the consumer group protocol, subscribe and run deliver its records', async () => {
      run = new PrototypeCase(name, lane, { protocol: 'consumer' })
      await run.consume(await run.kafka())

      expect(run.delivered[0]).toEqual({ topic: name, partition: 0, offset: '0' })
      expect(run.assigned.at(-1).partitions).toEqual([{ topic: name, partitions: [0] }])
      expect(ordinary(run.assigned.at(-1))).toBe(true)
      expectPrototypeUntouched(before)
    })

    const fake = lane === 'fake' ? test : test.skip
    fake(
      'under the consumer group protocol, an assignment that grows to take it on delivers its records',
      async () => {
        run = new PrototypeCase(name, lane, { protocol: 'consumer', grow: true })
        await run.consume(await run.kafka())

        expect(run.delivered).toEqual([{ topic: name, partition: 0, offset: '0' }])
        expectPrototypeUntouched(before)
        expect(run.assigned.map(({ partitions }) => partitions)).toEqual([
          [{ topic: GROWTH_TOPIC, partitions: [0] }],
          [{ topic: name, partitions: [0] }],
        ])
        await until(
          () =>
            run.scripted
              .requests()
              .some(
                ({ api, fields }) =>
                  api === 'Fetch' &&
                  fields.topics.some(
                    ({ topicId, partitions }) =>
                      topicId === TOPIC_ID && partitions[0].fetchOffset === '1'
                  )
              ),
          'its next fetch, from the offset after its record'
        )
      }
    )

    fake('a preferred read replica of it is kept for it alone', async () => {
      run = new PrototypeCase(name, lane, { rackId: 'rack-a' })
      await run.consume(await run.kafka())

      const fetches = run.scripted.requests().filter(({ api }) => api === 'Fetch')
      expect(fetches.length).toBeGreaterThan(1)
      expect(fetches.every(({ fields }) => fields.rackId === 'rack-a')).toBe(true)
      expectPrototypeUntouched(before)
    })
  })
})
