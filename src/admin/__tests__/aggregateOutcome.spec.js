const createAdmin = require('../index')
const createRetry = require('../../retry')
const { isPerItemFailure, settleAggregate, AggregateOutcome } = require('../aggregateOutcome')
const {
  KafkaJSAggregateError,
  KafkaJSConnectionError,
  KafkaJSDeliveryTimeout,
  KafkaJSNonRetriableError,
  KafkaJSNumberOfRetriesExceeded,
} = require('../../errors')
const { createErrorFromCode } = require('../../protocol/error')
const createTopicsResponse = require('../../protocol/requests/createTopics/v7/response')
const reassignmentsResponse = require('../../protocol/requests/alterPartitionReassignments/v0/response')
const { newLogger } = require('testHelpers')

const OK = 0
const UNKNOWN_TOPIC_OR_PARTITION = 3
const REQUEST_TIMED_OUT = 7
const TOPIC_ALREADY_EXISTS = 36
const INVALID_PARTITIONS = 37
const NOT_CONTROLLER = 41
const NO_REASSIGNMENT_IN_PROGRESS = 85
const THROTTLING_QUOTA_EXCEEDED = 89
// Explicit, so that an attempt count means the same under every run mode: three attempts in all.
const RETRY = { retries: 2, initialRetryTime: 1, maxRetryTime: 1 }

/** An answer to CreateTopics for `[topic, errorCode]` rows, run through the decoder's own `parse`. */
const topicsAnswer =
  (...rows) =>
  () =>
    createTopicsResponse.parse({
      topicErrors: rows.map(([topic, errorCode]) => ({ topic, errorCode })),
    })

/** An answer to AlterPartitionReassignments for `[topic, partition, errorCode]` rows. */
const partitionsAnswer =
  (...rows) =>
  () =>
    reassignmentsResponse.parse({
      errorCode: OK,
      responses: [...new Set(rows.map(([topic]) => topic))].map((topic) => ({
        topic,
        partitions: rows
          .filter(([name]) => name === topic)
          .map(([, partition, errorCode]) => ({ partition, errorCode })),
      })),
    })

/** An answer that fails the whole request, as the broker does for a controller that moved. */
const wholeRequestAnswer = (errorCode) => () =>
  reassignmentsResponse.parse({ errorCode, responses: [] })

/**
 * An admin whose controller answers each request with the next of `answers` (the last one for
 * ever): a function from the helpers above, or an error to throw.
 */
const adminAnswering = (method, answers, retry = RETRY) => {
  const queue = [...answers]
  const broker = {
    [method]: jest.fn(async () => {
      const answer = queue.length > 1 ? queue.shift() : queue[0]
      if (answer instanceof Error) {
        throw answer
      }
      return answer()
    }),
    metadata: jest.fn(async () => ({ topicMetadata: [] })),
  }
  const cluster = {
    refreshMetadata: jest.fn(async () => {}),
    findControllerBroker: jest.fn(async () => broker),
  }
  const admin = createAdmin({ logger: newLogger(), retry, cluster })

  return { admin, broker }
}

const child = (code, extra = {}) => Object.assign(createErrorFromCode(code), extra)
const aggregate = (...children) => new KafkaJSAggregateError('several failed', children)
const names = (children) => children.map(({ topic }) => topic)
const thrownBy = (fn) => {
  try {
    fn()
  } catch (e) {
    return e
  }
  throw new Error('Expected the call to throw')
}
const sentTopics = (broker) =>
  broker.createTopics.mock.calls.map(([{ topics }]) => topics.map(({ topic }) => topic))

describe('settleAggregate', () => {
  const keyOf = ({ topic }) => topic

  test('splits the children by retriable === true and lists the key of every child', () => {
    const error = aggregate(
      child(REQUEST_TIMED_OUT, { topic: 'a' }),
      child(INVALID_PARTITIONS, { topic: 'b' }),
      child(THROTTLING_QUOTA_EXCEEDED, { topic: 'c' })
    )

    const settled = settleAggregate(error, keyOf)

    expect(names(settled.retryable)).toEqual(['a', 'c'])
    expect(names(settled.terminal)).toEqual(['b'])
    expect(settled.failed).toEqual(new Set(['a', 'b', 'c']))
  })

  test('treats a child that does not say it is retriable as terminal', () => {
    const settled = settleAggregate(
      aggregate(Object.assign(new Error('plain'), { topic: 'a' }), {
        topic: 'b',
        retriable: 'yes',
      }),
      keyOf
    )

    expect(settled.retryable).toEqual([])
    expect(names(settled.terminal)).toEqual(['a', 'b'])
  })

  test.each([[null], [undefined], ['not a list']])(
    'has nothing to retry in an aggregate whose list of children is %p',
    (errors) => {
      expect(settleAggregate({ errors }, keyOf)).toEqual({
        retryable: [],
        terminal: [],
        failed: new Set(),
      })
    }
  )

  test('treats a null child as terminal and gives it no key', () => {
    const timedOut = child(REQUEST_TIMED_OUT, { topic: 'a' })

    const settled = settleAggregate(aggregate(null, timedOut), keyOf)

    expect(settled.retryable).toEqual([timedOut])
    expect(settled.terminal).toEqual([null])
    expect(settled.failed).toEqual(new Set(['a']))
  })

  test('treats a hole in the list as terminal and gives it no key', () => {
    const timedOut = child(REQUEST_TIMED_OUT, { topic: 'a' })
    const sparse = new Array(2)
    sparse[1] = timedOut

    const settled = settleAggregate(new KafkaJSAggregateError('several failed', sparse), keyOf)

    expect(settled.retryable).toEqual([timedOut])
    expect(settled.terminal).toHaveLength(1)
    expect(settled.terminal[0]).toBeUndefined()
    expect(settled.failed).toEqual(new Set(['a']))
  })

  test('has nothing to retry in an aggregate without children', () => {
    expect(settleAggregate(aggregate(), keyOf)).toEqual({
      retryable: [],
      terminal: [],
      failed: new Set(),
    })
  })
})

describe('isPerItemFailure', () => {
  test.each([
    ['an aggregate with children', aggregate(child(REQUEST_TIMED_OUT, { topic: 'a' })), true],
    ['an aggregate with no children', aggregate(), false],
    ['an aggregate with no list', Object.assign(aggregate(), { errors: null }), false],
    ['an error that is not an aggregate', child(REQUEST_TIMED_OUT), false],
    ['a value that is not an error', { errors: [child(REQUEST_TIMED_OUT)] }, false],
  ])('is %s: %p', (_name, error, expectedValue) => {
    expect(isPerItemFailure(error)).toBe(expectedValue)
  })
})

describe('AggregateOutcome', () => {
  const original = [{ topic: 'a' }, { topic: 'b' }, { topic: 'c' }, { topic: 'd' }]
  const snapshot = JSON.stringify(original)
  const outcomeOver = (overrides = {}) =>
    new AggregateOutcome({
      keys: original.map(({ topic }) => topic),
      keyOf: ({ topic }) => topic,
      select: (keys) => original.filter(({ topic }) => keys.has(topic)),
      message: 'some failed',
      isBenign: ({ type }) => type === 'TOPIC_ALREADY_EXISTS',
      ...overrides,
    })

  test('sends the whole original request first, and never changes it', () => {
    const outcome = outcomeOver()

    expect(outcome.pending).toEqual(original)
    outcome.settle(aggregate(child(REQUEST_TIMED_OUT, { topic: 'a' })))

    expect(names(outcome.pending)).toEqual(['a'])
    expect(JSON.stringify(original)).toBe(snapshot)
  })

  test('succeeded confirms every item that was sent', () => {
    const outcome = outcomeOver()

    outcome.succeeded()

    expect(outcome.created).toEqual(new Set(['a', 'b', 'c', 'd']))
    expect(outcome.pending).toEqual([])
  })

  test('settle confirms the items the answer leaves out, keeps the terminal children and narrows pending to the retryable ones', () => {
    const outcome = outcomeOver()
    const timedOut = child(REQUEST_TIMED_OUT, { topic: 'b' })
    const refused = child(INVALID_PARTITIONS, { topic: 'c' })

    const again = outcome.settle(aggregate(timedOut, refused))

    expect(again).toBeInstanceOf(KafkaJSAggregateError)
    expect(again.errors).toEqual([timedOut])
    expect(again.retriable).toBe(true)
    expect(outcome.created).toEqual(new Set(['a', 'd']))
    expect(outcome.terminal).toEqual([refused])
    expect(names(outcome.pending)).toEqual(['b'])
  })

  test('settle returns null when no child can be retried, and leaves nothing pending', () => {
    const outcome = outcomeOver()

    const again = outcome.settle(aggregate(child(INVALID_PARTITIONS, { topic: 'a' })))

    expect(again).toBeNull()
    expect(outcome.pending).toEqual([])
  })

  test('never offers a confirmed item again, however many answers narrow the request', () => {
    const outcome = outcomeOver()
    const sent = []

    for (const failed of [['a', 'b', 'c'], ['b', 'c'], ['c']]) {
      sent.push(names(outcome.pending))
      outcome.settle(aggregate(...failed.map((topic) => child(REQUEST_TIMED_OUT, { topic }))))
    }
    sent.push(names(outcome.pending))

    expect(sent).toEqual([['a', 'b', 'c', 'd'], ['a', 'b', 'c'], ['b', 'c'], ['c']])
    expect(outcome.created).toEqual(new Set(['d', 'a', 'b']))
  })

  describe('finish', () => {
    const exists = child(TOPIC_ALREADY_EXISTS, { topic: 'a' })
    const refused = child(INVALID_PARTITIONS, { topic: 'b' })
    const withTerminal = (...terminal) => {
      const outcome = outcomeOver()
      outcome.terminal.push(...terminal)
      return outcome
    }

    test('is true when nothing failed for good', () => {
      expect(outcomeOver().finish()).toBe(true)
    })

    test('is false when the only terminal children are benign', () => {
      expect(withTerminal(exists).finish()).toBe(false)
    })

    test('rejects with an aggregate of every terminal child, benign ones included, when one is not benign', () => {
      const error = thrownBy(() => withTerminal(exists, refused).finish())

      expect(error).toBeInstanceOf(KafkaJSAggregateError)
      expect(error.message).toBe('some failed')
      expect(error.errors).toEqual([exists, refused])
      expect(error.retriable).toBe(false)
    })

    test('rethrows what ended the retrier, the same object, when no child failed for good', () => {
      const outstanding = new KafkaJSConnectionError('reset')

      expect(thrownBy(() => outcomeOver().finish(outstanding))).toBe(outstanding)
    })

    test.each([
      [
        'an exhausted retrier',
        (inner) => new KafkaJSNumberOfRetriesExceeded(inner, { retryCount: 2, retryTime: 1 }),
      ],
      ['a deadline', (inner) => new KafkaJSDeliveryTimeout('too late', { cause: inner })],
    ])(
      'puts the children of the aggregate %s gave up on after the terminal ones',
      (_name, wrap) => {
        const timedOut = child(REQUEST_TIMED_OUT, { topic: 'c' })

        const error = thrownBy(() => withTerminal(refused).finish(wrap(aggregate(timedOut))))

        expect(error).toBeInstanceOf(KafkaJSAggregateError)
        expect(error.errors).toEqual([refused, timedOut])
        expect(error.retriable).toBe(false)
      }
    )

    test('puts an aggregate that lists nothing after the terminal children as the one failure it is', () => {
      const unlisted = Object.assign(aggregate(), { errors: null })
      const outstanding = new KafkaJSNumberOfRetriesExceeded(unlisted, {
        retryCount: 2,
        retryTime: 1,
      })

      const error = thrownBy(() => withTerminal(refused).finish(outstanding))

      expect(error.errors).toEqual([refused, unlisted])
    })

    test('puts a failure that is not an aggregate after the terminal children', () => {
      const outstanding = new KafkaJSConnectionError('reset')

      const error = thrownBy(() => withTerminal(refused).finish(outstanding))

      expect(error.errors).toEqual([refused, outstanding])
    })
  })
})

describe('Admin > createTopics > an answer that names only some topics', () => {
  const topics = ['a', 'b', 'c'].map((topic) => ({ topic }))
  const create = (admin, options = {}) => admin.createTopics({ topics, ...options })

  test('sends again only the retriable topics and keeps the one refused for good', async () => {
    const { admin, broker } = adminAnswering('createTopics', [
      topicsAnswer(['a', REQUEST_TIMED_OUT], ['b', INVALID_PARTITIONS], ['c', OK]),
      topicsAnswer(['a', OK]),
    ])

    const error = await create(admin).catch((e) => e)

    expect(sentTopics(broker)).toEqual([['a', 'b', 'c'], ['a']])
    expect(error).toBeInstanceOf(KafkaJSAggregateError)
    expect(error.errors.map(({ topic, type }) => [topic, type])).toEqual([
      ['b', 'INVALID_PARTITIONS'],
    ])
    expect(error.retriable).toBe(false)
    expect(broker.metadata).not.toHaveBeenCalled()
  })

  test('keeps sending the retriable topics until the budget is spent, then reports both kinds', async () => {
    const { admin, broker } = adminAnswering('createTopics', [
      topicsAnswer(['a', REQUEST_TIMED_OUT], ['b', INVALID_PARTITIONS], ['c', OK]),
      topicsAnswer(['a', REQUEST_TIMED_OUT]),
    ])

    const error = await create(admin).catch((e) => e)

    expect(sentTopics(broker)).toEqual([['a', 'b', 'c'], ['a'], ['a']])
    expect(error).toBeInstanceOf(KafkaJSAggregateError)
    expect(error.errors.map(({ topic, type }) => [topic, type])).toEqual([
      ['b', 'INVALID_PARTITIONS'],
      ['a', 'REQUEST_TIMED_OUT'],
    ])
    expect(error.retriable).toBe(false)
  })

  test('ends with the retrier’s own error when nothing was refused for good', async () => {
    const { admin, broker } = adminAnswering('createTopics', [
      topicsAnswer(['a', REQUEST_TIMED_OUT], ['c', OK]),
    ])

    const error = await create(admin).catch((e) => e)

    expect(error).toBeInstanceOf(KafkaJSNumberOfRetriesExceeded)
    expect(error.cause).toBeInstanceOf(KafkaJSAggregateError)
    expect(names(error.cause.errors)).toEqual(['a'])
    expect(sentTopics(broker)).toEqual([['a', 'b', 'c'], ['a'], ['a']])
  })

  test('does not lose a terminal outcome to a later transport failure', async () => {
    const reset = new KafkaJSConnectionError('Connection error: ECONNRESET')
    const { admin, broker } = adminAnswering('createTopics', [
      topicsAnswer(['a', REQUEST_TIMED_OUT], ['b', INVALID_PARTITIONS]),
      reset,
    ])

    const error = await create(admin).catch((e) => e)

    expect(error).toBeInstanceOf(KafkaJSAggregateError)
    expect(error.errors.map((e) => e.type || e.message)).toEqual([
      'INVALID_PARTITIONS',
      'Connection error: ECONNRESET',
    ])
    expect(error.errors[1]).toBe(reset)
    expect(broker.createTopics).toHaveBeenCalledTimes(2)
  })

  test('repeats the same pending topics after a controller change and then succeeds', async () => {
    const { admin, broker } = adminAnswering('createTopics', [
      topicsAnswer(['a', REQUEST_TIMED_OUT], ['b', OK], ['c', OK]),
      createErrorFromCode(NOT_CONTROLLER),
      topicsAnswer(['a', OK]),
    ])

    await expect(create(admin)).resolves.toBe(true)

    expect(sentTopics(broker)).toEqual([['a', 'b', 'c'], ['a'], ['a']])
  })

  test('sends again only the topic a quota refused, and rejects with the one refused for good', async () => {
    const { admin, broker } = adminAnswering('createTopics', [
      topicsAnswer(['a', THROTTLING_QUOTA_EXCEEDED], ['b', INVALID_PARTITIONS], ['c', OK]),
      topicsAnswer(['a', OK]),
    ])

    const error = await create(admin).catch((e) => e)

    expect(sentTopics(broker)).toEqual([['a', 'b', 'c'], ['a']])
    expect(names(error.errors)).toEqual(['b'])
  })

  test('never sends a confirmed topic again, whatever the answers', async () => {
    const { admin, broker } = adminAnswering(
      'createTopics',
      [
        topicsAnswer(['a', REQUEST_TIMED_OUT], ['b', REQUEST_TIMED_OUT], ['c', REQUEST_TIMED_OUT]),
        topicsAnswer(['b', REQUEST_TIMED_OUT], ['c', REQUEST_TIMED_OUT]),
        topicsAnswer(['c', REQUEST_TIMED_OUT]),
        topicsAnswer(['c', OK]),
      ],
      { ...RETRY, retries: 4 }
    )

    await expect(
      admin.createTopics({ topics: [...topics, { topic: 'd' }], waitForLeaders: false })
    ).resolves.toBe(true)

    expect(sentTopics(broker)).toEqual([['a', 'b', 'c', 'd'], ['a', 'b', 'c'], ['b', 'c'], ['c']])
  })

  test.each([[[]], [null]])(
    'ends at once, unchanged, with an aggregate whose list of failures is %p',
    async (errors) => {
      const unlisted = Object.assign(aggregate(), { errors })
      const { admin, broker } = adminAnswering('createTopics', [unlisted])

      await expect(create(admin)).rejects.toBe(unlisted)

      expect(broker.createTopics).toHaveBeenCalledTimes(1)
      expect(broker.metadata).not.toHaveBeenCalled()
    }
  )

  test.each([
    ['a null child', (timedOut) => [null, timedOut]],
    [
      'a hole',
      (timedOut) => {
        const sparse = new Array(2)
        sparse[1] = timedOut
        return sparse
      },
    ],
  ])(
    'rejects with the aggregate, never a success or a TypeError, when the failures list %s',
    async (_name, listOf) => {
      const unclassified = new KafkaJSAggregateError(
        'several failed',
        listOf(child(REQUEST_TIMED_OUT, { topic: 'a' }))
      )
      const { admin, broker } = adminAnswering('createTopics', [
        unclassified,
        topicsAnswer(['a', OK]),
      ])

      const error = await create(admin).catch((e) => e)

      expect(error).toBeInstanceOf(KafkaJSAggregateError)
      expect(error.errors).toHaveLength(1)
      expect(error.errors[0] == null).toBe(true)
      expect(error.retriable).toBe(false)
      expect(sentTopics(broker)).toEqual([['a', 'b', 'c'], ['a']])
      expect(broker.metadata).not.toHaveBeenCalled()
    }
  )

  describe('the result is one boolean over the whole call', () => {
    test('is false when some topics already exist and the others were created, and waits for the created ones only', async () => {
      const { admin, broker } = adminAnswering('createTopics', [
        topicsAnswer(['a', TOPIC_ALREADY_EXISTS]),
      ])

      await expect(create(admin)).resolves.toBe(false)

      expect(sentTopics(broker)).toEqual([['a', 'b', 'c']])
      expect(broker.metadata).toHaveBeenCalledTimes(1)
      expect(broker.metadata).toHaveBeenCalledWith(['b', 'c'])
    })

    test('is false, and asks for no leaders, when every topic already exists', async () => {
      const { admin, broker } = adminAnswering('createTopics', [
        topicsAnswer(['a', TOPIC_ALREADY_EXISTS], ['b', TOPIC_ALREADY_EXISTS], ['c', 36]),
      ])

      await expect(create(admin)).resolves.toBe(false)

      expect(broker.metadata).not.toHaveBeenCalled()
    })

    test('stays false when the already-existing topic was seen on an earlier attempt than the last', async () => {
      const { admin, broker } = adminAnswering('createTopics', [
        topicsAnswer(
          ['a', TOPIC_ALREADY_EXISTS],
          ['b', REQUEST_TIMED_OUT],
          ['c', REQUEST_TIMED_OUT]
        ),
        topicsAnswer(['b', OK]),
      ])

      await expect(create(admin)).resolves.toBe(false)

      expect(sentTopics(broker)).toEqual([
        ['a', 'b', 'c'],
        ['b', 'c'],
      ])
    })
  })

  describe('waiting for leaders', () => {
    test('happens once, after the last attempt, for every topic that was created', async () => {
      const { admin, broker } = adminAnswering('createTopics', [
        topicsAnswer(['a', REQUEST_TIMED_OUT]),
        topicsAnswer(['a', OK]),
      ])

      await expect(create(admin)).resolves.toBe(true)

      expect(broker.metadata).toHaveBeenCalledTimes(1)
      expect(broker.metadata).toHaveBeenCalledWith(['b', 'c', 'a'])
      const [last] = broker.createTopics.mock.invocationCallOrder.slice(-1)
      expect(broker.metadata.mock.invocationCallOrder[0]).toBeGreaterThan(last)
    })

    test('does not happen when the call rejects', async () => {
      const { admin, broker } = adminAnswering('createTopics', [
        topicsAnswer(['a', INVALID_PARTITIONS]),
      ])

      await expect(create(admin)).rejects.toBeInstanceOf(KafkaJSAggregateError)

      expect(broker.metadata).not.toHaveBeenCalled()
    })

    test('does not happen when waitForLeaders is false', async () => {
      const { admin, broker } = adminAnswering('createTopics', [topicsAnswer(['a', OK])])

      await expect(create(admin, { waitForLeaders: false })).resolves.toBe(true)

      expect(broker.metadata).not.toHaveBeenCalled()
    })

    test('fails at once with the broker’s answer for a request that only validates, since no topic is on its way', async () => {
      const { admin, broker } = adminAnswering('createTopics', [
        topicsAnswer(['a', REQUEST_TIMED_OUT]),
        topicsAnswer(['a', OK]),
      ])
      broker.metadata.mockRejectedValue(createErrorFromCode(UNKNOWN_TOPIC_OR_PARTITION))

      await expect(create(admin, { validateOnly: true })).rejects.toMatchObject({
        type: 'UNKNOWN_TOPIC_OR_PARTITION',
      })

      expect(broker.metadata).toHaveBeenCalledTimes(1)
      expect(broker.createTopics.mock.calls.map(([request]) => request.validateOnly)).toEqual([
        true,
        true,
      ])
    })
  })

  test('is not repeated by a caller’s own retrier once a topic was refused for good', async () => {
    const { admin, broker } = adminAnswering('createTopics', [
      topicsAnswer(['a', INVALID_PARTITIONS], ['b', REQUEST_TIMED_OUT]),
      topicsAnswer(['b', OK]),
    ])
    const outer = createRetry({ retries: 3, initialRetryTime: 1, maxRetryTime: 1 })

    const error = await outer(() => create(admin)).catch((e) => e)

    expect(error).toBeInstanceOf(KafkaJSNonRetriableError)
    expect(error.cause).toBeInstanceOf(KafkaJSAggregateError)
    expect(broker.createTopics).toHaveBeenCalledTimes(2)
  })
})

describe('Admin > alterPartitionReassignments > an answer that names only some partitions', () => {
  const topics = [
    {
      topic: 't1',
      partitionAssignment: [
        { partition: 0, replicas: [1, 2] },
        { partition: 1, replicas: [2, 3] },
      ],
    },
    { topic: 't2', partitionAssignment: [{ partition: 0, replicas: [1] }] },
  ]
  const snapshot = JSON.stringify(topics)
  const alter = (admin) => admin.alterPartitionReassignments({ topics })
  const sentPartitions = (broker) =>
    broker.alterPartitionReassignments.mock.calls.map(([{ topics: sent }]) => sent)

  test('sends again only the partitions that can still go through, with their replicas, and drops a topic with none left', async () => {
    const { admin, broker } = adminAnswering('alterPartitionReassignments', [
      partitionsAnswer(
        ['t1', 0, REQUEST_TIMED_OUT],
        ['t1', 1, NO_REASSIGNMENT_IN_PROGRESS],
        ['t2', 0, OK]
      ),
      partitionsAnswer(['t1', 0, OK]),
    ])

    const error = await alter(admin).catch((e) => e)

    expect(sentPartitions(broker)).toEqual([
      topics,
      [{ topic: 't1', partitionAssignment: [{ partition: 0, replicas: [1, 2] }] }],
    ])
    expect(error).toBeInstanceOf(KafkaJSAggregateError)
    expect(error.errors.map(({ topic, partition, type }) => [topic, partition, type])).toEqual([
      ['t1', 1, 'NO_REASSIGNMENT_IN_PROGRESS'],
    ])
    expect(error.retriable).toBe(false)
    expect(JSON.stringify(topics)).toBe(snapshot)
  })

  test('resolves with nothing once every partition went through, whatever the attempt', async () => {
    const { admin, broker } = adminAnswering('alterPartitionReassignments', [
      partitionsAnswer(['t1', 0, REQUEST_TIMED_OUT], ['t1', 1, OK], ['t2', 0, OK]),
      partitionsAnswer(['t1', 0, OK]),
    ])

    await expect(alter(admin)).resolves.toBeUndefined()

    expect(sentPartitions(broker).map((sent) => sent.length)).toEqual([2, 1])
  })

  test('does not send a topic whose partitionAssignment is empty', async () => {
    const { admin, broker } = adminAnswering('alterPartitionReassignments', [
      partitionsAnswer(['t2', 0, OK]),
    ])

    await expect(
      admin.alterPartitionReassignments({
        topics: [{ topic: 't0', partitionAssignment: [] }, topics[1]],
      })
    ).resolves.toBeUndefined()

    expect(sentPartitions(broker)).toEqual([[topics[1]]])
  })

  test('rejects after one attempt with the aggregate of the partitions refused for good', async () => {
    const { admin, broker } = adminAnswering('alterPartitionReassignments', [
      partitionsAnswer(['t1', 0, NO_REASSIGNMENT_IN_PROGRESS], ['t2', 0, OK]),
    ])

    const error = await alter(admin).catch((e) => e)

    expect(error).toBeInstanceOf(KafkaJSAggregateError)
    expect(error.errors).toHaveLength(1)
    expect(broker.alterPartitionReassignments).toHaveBeenCalledTimes(1)
  })

  test('repeats the same pending partitions after a controller change', async () => {
    const { admin, broker } = adminAnswering('alterPartitionReassignments', [
      partitionsAnswer(['t1', 0, REQUEST_TIMED_OUT], ['t1', 1, OK], ['t2', 0, OK]),
      wholeRequestAnswer(NOT_CONTROLLER),
      partitionsAnswer(['t1', 0, OK]),
    ])

    await expect(alter(admin)).resolves.toBeUndefined()

    const only = [{ topic: 't1', partitionAssignment: [{ partition: 0, replicas: [1, 2] }] }]
    expect(sentPartitions(broker)).toEqual([topics, only, only])
  })

  test('ends with a failure of the whole request unchanged when no partition was refused for good', async () => {
    const { admin, broker } = adminAnswering('alterPartitionReassignments', [
      wholeRequestAnswer(INVALID_PARTITIONS),
    ])

    const error = await alter(admin).catch((e) => e)

    expect(error).toMatchObject({ type: 'INVALID_PARTITIONS' })
    expect(error).not.toBeInstanceOf(KafkaJSAggregateError)
    expect(broker.alterPartitionReassignments).toHaveBeenCalledTimes(1)
  })

  test('ends at once, unchanged, with an aggregate that lists no failure', async () => {
    const unlisted = aggregate()
    const { admin, broker } = adminAnswering('alterPartitionReassignments', [unlisted])

    await expect(alter(admin)).rejects.toBe(unlisted)

    expect(broker.alterPartitionReassignments).toHaveBeenCalledTimes(1)
  })

  test('with retries: 0 makes one attempt and reports every failure', async () => {
    const { admin, broker } = adminAnswering(
      'alterPartitionReassignments',
      [partitionsAnswer(['t1', 0, REQUEST_TIMED_OUT], ['t1', 1, NO_REASSIGNMENT_IN_PROGRESS])],
      { ...RETRY, retries: 0 }
    )

    const error = await alter(admin).catch((e) => e)

    expect(error).toBeInstanceOf(KafkaJSAggregateError)
    expect(error.errors.map(({ partition }) => partition)).toEqual([1, 0])
    expect(error.retriable).toBe(false)
    expect(broker.alterPartitionReassignments).toHaveBeenCalledTimes(1)
  })

  test('lists every partition refused for good, however many the answer carries', async () => {
    const count = 150000
    const partitionAssignment = Array.from({ length: count }, (_, partition) => ({
      partition,
      replicas: [1],
    }))
    const refusals = partitionAssignment.map(({ partition }) => ({
      topic: 't1',
      partition,
      retriable: false,
    }))
    const { admin, broker } = adminAnswering('alterPartitionReassignments', [
      new KafkaJSAggregateError('Errors altering partition reassignments', refusals),
    ])

    const error = await admin
      .alterPartitionReassignments({ topics: [{ topic: 't1', partitionAssignment }] })
      .catch((e) => e)

    expect(error).toBeInstanceOf(KafkaJSAggregateError)
    expect(error.errors).toHaveLength(count)
    expect(error.errors.every((child, index) => child === refusals[index])).toBe(true)
    expect(broker.alterPartitionReassignments).toHaveBeenCalledTimes(1)
  })
})
