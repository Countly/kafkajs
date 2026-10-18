const { Kafka } = require('../../index')
const { WireOrder, WireOrderViolation } = require('../wireOrder')
const { createScriptedCluster, producerScript } = require('../scriptedCluster')
const { TOPIC, TOPIC_ID } = require('../../src/consumer/__tests__/fixtures/fetchHarness')

const NODES = [{ id: 0, host: 'node0', port: 9092 }]

/** A Produce request record, as the scripted cluster's reader logs it. */
const produce = ({
  order,
  topic = 't',
  partition = 0,
  sequence,
  node = 0,
  generation = 1,
  ...rest
}) => ({
  order,
  node,
  api: 'Produce',
  apiKey: 0,
  version: 11,
  correlationId: order,
  generation,
  fields: {
    transactionalId: rest.transactionalId ?? null,
    topics: [
      {
        ...(rest.topicId ? { topicId: rest.topicId } : { topic }),
        partitions: [
          {
            partition,
            batches: [
              {
                producerId: '7',
                producerEpoch: 1,
                baseSequence: sequence,
                recordCount: rest.recordCount ?? 1,
                transactional: rest.transactionalId != null,
                digest: rest.digest ?? `batch-${sequence}`,
              },
            ],
          },
        ],
      },
    ],
  },
})

const orderOf = (...records) => new WireOrder({ requests: records })

describe('WireOrder rows', () => {
  it('has one row per partition batch of a Produce, with the fields the oracle reads', () => {
    const wire = orderOf(produce({ order: 3, sequence: 4, transactionalId: 'tx', generation: 2 }))

    expect(wire.produced()).toEqual([
      {
        order: 3,
        api: 'Produce',
        apiKey: 0,
        node: 0,
        connectionGeneration: 2,
        correlationId: 3,
        transactionalId: 'tx',
        topic: 't',
        partition: 0,
        producerId: '7',
        producerEpoch: 1,
        sequence: 4,
        recordCount: 1,
        transactional: true,
        digest: 'batch-4',
      },
    ])
  })

  it('names the topic by its id when the request carries no name', () => {
    expect(orderOf(produce({ order: 1, topicId: 'id-1', sequence: 0 })).produced()[0].topic).toBe(
      'id-1'
    )
  })

  it('has one row per transaction request and none for any other API', () => {
    const wire = new WireOrder({
      requests: [
        {
          order: 1,
          node: 0,
          api: 'EndTxn',
          apiKey: 26,
          version: 5,
          correlationId: 9,
          generation: 1,
          fields: {
            transactionalId: 'tx',
            producerId: '7',
            producerEpoch: 1,
            transactionResult: true,
          },
        },
        {
          order: 2,
          node: 0,
          api: 'Metadata',
          apiKey: 3,
          version: 13,
          correlationId: 10,
          generation: 1,
          fields: {},
        },
      ],
    })

    expect(wire.rows).toEqual([
      {
        order: 1,
        api: 'EndTxn',
        apiKey: 26,
        node: 0,
        connectionGeneration: 1,
        correlationId: 9,
        transactionalId: 'tx',
        producerId: '7',
        producerEpoch: 1,
        committed: true,
      },
    ])
  })

  it('reads no row from a Produce whose Records field is null', () => {
    const record = produce({ order: 1, sequence: 0 })
    record.fields.topics[0].partitions[0].batches = null

    expect(orderOf(record).rows).toEqual([])
  })
})

describe('WireOrder#assertSequencesInOrder', () => {
  it('accepts an unbroken run, a resend of the same bytes and a newer connection resending in order', () => {
    const wire = orderOf(
      produce({ order: 1, sequence: 0 }),
      produce({ order: 2, sequence: 1 }),
      produce({ order: 3, sequence: 2, recordCount: 2 }),
      produce({ order: 4, sequence: 1, generation: 2 }),
      produce({ order: 5, sequence: 2, recordCount: 2, generation: 2 }),
      produce({ order: 6, sequence: 4, generation: 2 })
    )

    expect(() => wire.assertSequencesInOrder()).not.toThrow()
  })

  it('rejects a gap, which is a batch that was lost', () => {
    const wire = orderOf(produce({ order: 1, sequence: 0 }), produce({ order: 2, sequence: 2 }))

    expect(() => wire.assertSequencesInOrder()).toThrow(
      't|0: sequences 0,2 are not an unbroken run'
    )
  })

  it('rejects a sequence written after a higher one on the same connection', () => {
    const wire = orderOf(
      produce({ order: 1, sequence: 0 }),
      produce({ order: 2, sequence: 1 }),
      produce({ order: 3, sequence: 0 })
    )

    expect(() => wire.assertSequencesInOrder()).toThrow(
      't|0: sequence 0 was written after 1 on 0#1'
    )
  })

  it('keeps the partitions apart', () => {
    const wire = orderOf(
      produce({ order: 1, partition: 0, sequence: 0 }),
      produce({ order: 2, partition: 1, sequence: 0 }),
      produce({ order: 3, partition: 0, sequence: 1 })
    )

    expect(() => wire.assertSequencesInOrder()).not.toThrow()
  })
})

describe('WireOrder#assertNoRewrite', () => {
  it('accepts a resend of the same bytes', () => {
    const wire = orderOf(produce({ order: 1, sequence: 0 }), produce({ order: 2, sequence: 0 }))

    expect(() => wire.assertNoRewrite()).not.toThrow()
  })

  it('rejects a sequence written twice with different bytes', () => {
    const wire = orderOf(
      produce({ order: 1, sequence: 0 }),
      produce({ order: 2, sequence: 0, digest: 'other' })
    )

    expect(() => wire.assertNoRewrite()).toThrow('t|0|7|1|0 was written twice with different bytes')
  })

  it('rejects the same bytes with a different record count', () => {
    const wire = orderOf(
      produce({ order: 1, sequence: 0 }),
      produce({ order: 2, sequence: 0, recordCount: 2 })
    )

    expect(() => wire.assertNoRewrite()).toThrow('was written twice')
  })
})

describe('WireOrder#assertNoStaleGeneration', () => {
  it('accepts a newer connection after an older one, and another node on an older connection', () => {
    const wire = orderOf(
      produce({ order: 1, sequence: 0, generation: 1 }),
      produce({ order: 2, sequence: 1, generation: 2 }),
      produce({ order: 3, sequence: 2, generation: 1, node: 1 })
    )

    expect(() => wire.assertNoStaleGeneration()).not.toThrow()
  })

  it('rejects an older connection writing after a newer one for the same partition', () => {
    const wire = orderOf(
      produce({ order: 1, sequence: 0, generation: 2 }),
      produce({ order: 2, sequence: 1, generation: 1 })
    )

    expect(() => wire.assertNoStaleGeneration()).toThrow('generation 1 wrote after generation 2')
  })
})

describe('WireOrder#assertEndTxnAfterOffsets', () => {
  const event = (order, kind, api, correlationId) => ({ order, kind, api, correlationId, node: 0 })
  const timelineOf = (...events) => new WireOrder({ requests: [], timeline: events })

  it('accepts an EndTxn written after the TxnOffsetCommit was answered', () => {
    const wire = timelineOf(
      event(1, 'request', 'TxnOffsetCommit', 5),
      event(2, 'response', 'TxnOffsetCommit', 5),
      event(3, 'request', 'EndTxn', 6)
    )

    expect(() => wire.assertEndTxnAfterOffsets()).not.toThrow()
  })

  it('rejects an EndTxn written while a TxnOffsetCommit was unanswered', () => {
    const wire = timelineOf(
      event(1, 'request', 'TxnOffsetCommit', 5),
      event(2, 'request', 'EndTxn', 6),
      event(3, 'response', 'TxnOffsetCommit', 5)
    )

    expect(() => wire.assertEndTxnAfterOffsets()).toThrow(
      'EndTxn (correlation 6) was written before TxnOffsetCommit (correlation 5) was answered'
    )
  })

  it('rejects an EndTxn when the TxnOffsetCommit was never answered', () => {
    const wire = timelineOf(
      event(1, 'request', 'TxnOffsetCommit', 5),
      event(2, 'request', 'EndTxn', 6)
    )

    expect(() => wire.assertEndTxnAfterOffsets()).toThrow('was answered')
  })

  it('does not hold an EndTxn to a TxnOffsetCommit written after it', () => {
    const wire = timelineOf(
      event(1, 'request', 'EndTxn', 6),
      event(2, 'request', 'TxnOffsetCommit', 7)
    )

    expect(() => wire.assertEndTxnAfterOffsets()).not.toThrow()
  })
})

describe('WireOrder#assertTransmitted', () => {
  const sent = orderOf(
    produce({ order: 1, sequence: 0 }),
    produce({ order: 2, sequence: 1 }),
    produce({ order: 3, sequence: 3 })
  )
  const expecting = (...sequences) =>
    sequences.map((sequence) => ({ topic: 't', partition: 0, sequence }))

  it('accepts what the test expected, in any order', () => {
    expect(() => sent.assertTransmitted(expecting(3, 0, 1))).not.toThrow()
  })

  it('rejects an expectation that differs from what was transmitted, naming both sides', () => {
    expect(() => sent.assertTransmitted(expecting(0, 1, 2))).toThrow(
      'lost ["t|0|2"], unexpected ["t|0|3"]'
    )
  })

  it('rejects a log with nothing in it when something was expected', () => {
    expect(() => orderOf().assertTransmitted(expecting(0))).toThrow('lost ["t|0|0"], unexpected []')
  })
})

describe('WireOrder#assertOrder', () => {
  const timelineOf = (...labels) =>
    new WireOrder({
      requests: [],
      timeline: labels.map((label, order) =>
        label.includes(' ')
          ? { order, kind: label.split(' ')[1], api: label.split(' ')[0] }
          : { order, kind: 'mark', label }
      ),
    })

  it('accepts the events in order with others between them', () => {
    const wire = timelineOf('A request', 'noise', 'A response', 'B request', 'cache write')

    expect(() => wire.assertOrder(['A response', 'B request', 'cache write'])).not.toThrow()
  })

  it('rejects an event that comes before the one it should follow, naming what it should follow', () => {
    const wire = timelineOf('A response', 'cache write', 'B request')

    expect(() => wire.assertOrder(['A response', 'B request', 'cache write'])).toThrow(
      '"cache write" did not follow ["A response","B request"]'
    )
  })

  it('rejects an event that never happened', () => {
    expect(() => timelineOf('A request').assertOrder(['A request', 'B request'])).toThrow(
      '"B request" did not follow ["A request"]'
    )
  })
})

describe('WireOrderViolation', () => {
  it('is what every check throws, so a violation is told from any other failure', () => {
    const wire = orderOf(produce({ order: 1, sequence: 0 }), produce({ order: 2, sequence: 2 }))
    const checks = [
      () => wire.assertSequencesInOrder(),
      () =>
        orderOf(
          produce({ order: 1, sequence: 0 }),
          produce({ order: 2, sequence: 0, digest: 'x' })
        ).assertNoRewrite(),
      () =>
        orderOf(
          produce({ order: 1, sequence: 0, generation: 2 }),
          produce({ order: 2, sequence: 1, generation: 1 })
        ).assertNoStaleGeneration(),
      () => wire.assertTransmitted([]),
      () => wire.assertOrder(['nothing']),
    ]

    for (const check of checks) {
      expect(check).toThrow(WireOrderViolation)
    }
    expect(new WireOrderViolation('x').name).toBe('WireOrderViolation')
  })

  it('is not thrown for a log that cannot be read', () => {
    expect(() => new WireOrder({ requests: [{ api: 'Produce', fields: null }] })).toThrow(TypeError)
  })
})

describe('WireOrder over a real producer on the scripted cluster', () => {
  const closers = []
  afterEach(async () => {
    await Promise.all(closers.splice(0).map((close) => close()))
  })

  const runIdempotent = async (batches) => {
    const cluster = producerScript(createScriptedCluster({ nodes: NODES }))
    const producer = new Kafka(cluster.kafkaOptions()).producer()
    closers.push(() => producer.disconnect())
    await producer.connect()
    for (let batch = 0; batch < batches; batch++) {
      await producer.send({
        topic: TOPIC,
        messages: [{ value: `m${batch}` }, { value: `n${batch}` }],
      })
    }

    return cluster
  }

  it('reads the producer id, epoch and sequences an idempotent producer actually wrote', async () => {
    const cluster = await runIdempotent(3)
    const wire = WireOrder.of(cluster)

    expect(wire.produced().map(({ sequence, recordCount }) => [sequence, recordCount])).toEqual([
      [0, 2],
      [2, 2],
      [4, 2],
    ])
    expect(new Set(wire.produced().map(({ producerId }) => producerId))).toEqual(new Set(['1000']))
    expect(wire.produced().every(({ topic }) => topic === TOPIC_ID)).toBe(true)
    expect(() => {
      wire.assertSequencesInOrder()
      wire.assertNoRewrite()
      wire.assertNoStaleGeneration()
    }).not.toThrow()
    cluster.assertClean()
  })

  it('fails when the test expected other batches than the producer wrote', async () => {
    const cluster = await runIdempotent(2)
    const wire = WireOrder.of(cluster)
    const meant = [0, 1, 2].map((sequence) => ({ topic: TOPIC_ID, partition: 0, sequence }))

    expect(() => wire.assertTransmitted(meant)).toThrow(`lost ["${TOPIC_ID}|0|1"], unexpected []`)
    cluster.assertClean()
  })

  it.each(['tv1', 'tv2'])(
    'finds the EndTxn after the TxnOffsetCommit answers under %s',
    async (profile) => {
      const cluster = producerScript(createScriptedCluster({ nodes: NODES, profile }))
      const producer = new Kafka(cluster.kafkaOptions()).producer({
        transactionalId: `order-${profile}`,
        maxInFlightRequests: 1,
      })
      closers.push(() => producer.disconnect())
      await producer.connect()
      const transaction = await producer.transaction()
      await transaction.send({ topic: TOPIC, messages: [{ value: 'a' }] })
      await transaction.sendOffsets({
        consumerGroupId: 'g',
        topics: [{ topic: TOPIC, partitions: [{ partition: 0, offset: '5' }] }],
      })
      await transaction.commit()

      const wire = WireOrder.of(cluster)

      expect(() => wire.assertEndTxnAfterOffsets()).not.toThrow()
      expect(wire.rows.filter(({ api }) => api === 'EndTxn')).toEqual([
        expect.objectContaining({ transactionalId: `order-${profile}`, committed: true }),
      ])
      expect(wire.produced()).toEqual([
        expect.objectContaining({
          transactionalId: `order-${profile}`,
          transactional: true,
          topic: TOPIC,
        }),
      ])
      cluster.assertClean()
    }
  )
})

describe('connection identity and producer sequence domains', () => {
  it('rejects a Metadata answer that shares an unanswered offset commit correlation id', () => {
    const wire = new WireOrder({
      requests: [],
      timeline: [
        { order: 0, kind: 'request', api: 'Metadata', node: 0, correlationId: 5, generation: 1 },
        {
          order: 1,
          kind: 'request',
          api: 'TxnOffsetCommit',
          node: 0,
          correlationId: 5,
          generation: 2,
        },
        { order: 2, kind: 'response', api: 'Metadata', node: 0, correlationId: 5, generation: 1 },
        { order: 3, kind: 'request', api: 'EndTxn', node: 0, correlationId: 6, generation: 2 },
      ],
    })
    expect(() => wire.assertEndTxnAfterOffsets()).toThrow(WireOrderViolation)
  })

  it('rejects an older connection answer when the current offset commit is unanswered', () => {
    const cluster = createScriptedCluster({ nodes: NODES, profile: 'tv1' })
    cluster.log.push(
      {
        order: 0,
        answeredOrder: 1,
        api: 'TxnOffsetCommit',
        apiKey: 28,
        version: 4,
        node: 0,
        correlationId: 5,
        generation: 1,
        fields: {},
      },
      {
        order: 2,
        api: 'TxnOffsetCommit',
        apiKey: 28,
        version: 4,
        node: 0,
        correlationId: 5,
        generation: 2,
        fields: {},
      },
      {
        order: 3,
        api: 'EndTxn',
        apiKey: 26,
        version: 4,
        node: 0,
        correlationId: 6,
        generation: 2,
        fields: {},
      }
    )
    expect(() => WireOrder.of(cluster).assertEndTxnAfterOffsets()).toThrow(WireOrderViolation)
    expect(
      cluster
        .timeline()
        .filter(({ kind }) => kind === 'request')
        .map(({ generation }) => generation)
    ).toEqual([1, 2, 2])
  })

  it('rejects a producer sequence gap even when another producer sent the missing number', () => {
    const first = produce({ order: 1, sequence: 0 })
    const other = produce({ order: 2, sequence: 1 })
    other.fields.topics[0].partitions[0].batches[0].producerId = '8'
    const last = produce({ order: 3, sequence: 2 })
    expect(() => orderOf(first, other, last).assertSequencesInOrder()).toThrow(WireOrderViolation)
  })

  it('accepts a sequence restart when the producer epoch changes', () => {
    const first = produce({ order: 1, sequence: 0 })
    const second = produce({ order: 2, sequence: 1 })
    const next = produce({ order: 3, sequence: 0 })
    next.fields.topics[0].partitions[0].batches[0].producerEpoch = 2
    expect(() => orderOf(first, second, next).assertSequencesInOrder()).not.toThrow()
  })
})
