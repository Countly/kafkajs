const { classify, transactionStateFor, categoryOf } = require('../errorClassifier')
const { createErrorFromCode, errorCodes } = require('../../../protocol/error')
const { decode, parse } = require('../../../protocol/requests/produce/v10/response')
const retry = require('../../../retry')
const {
  KafkaJSError,
  KafkaJSNonRetriableError,
  KafkaJSProtocolError,
  KafkaJSConnectionError,
  KafkaJSOffsetOutOfRange,
  KafkaJSNumberOfRetriesExceeded,
  KafkaJSRefreshRetriableError,
  KafkaJSTransactionAbortableError,
  KafkaJSApplicationRecoverableError,
  KafkaJSInvalidConfigurationError,
  KafkaJSDeliveryTimeout,
} = require('../../../errors')

const SURFACES = ['produce', 'txn', 'offsets']
const KINDS = [true, false]
const RETRIABLE_CATEGORIES = ['retriable', 'refreshRetriable']

/**
 * The table, restated row by row and independently of the module under test: the code, the type
 * the protocol table gives it, the surfaces the row holds on and the category it answers there,
 * for both producer kinds. The kind-split codes (45 and 59 on Produce, 59 on the transaction
 * requests) and 46 have their own cases below.
 */
const TABLE = [
  { type: 'CORRUPT_MESSAGE', code: 2, surfaces: SURFACES, category: 'retriable' },
  { type: 'REQUEST_TIMED_OUT', code: 7, surfaces: SURFACES, category: 'retriable' },
  { type: 'GROUP_LOAD_IN_PROGRESS', code: 14, surfaces: SURFACES, category: 'retriable' },
  { type: 'NOT_ENOUGH_REPLICAS', code: 19, surfaces: ['produce'], category: 'retriable' },
  {
    type: 'NOT_ENOUGH_REPLICAS_AFTER_APPEND',
    code: 20,
    surfaces: ['produce'],
    category: 'retriable',
  },
  { type: 'CONCURRENT_TRANSACTIONS', code: 51, surfaces: ['txn'], category: 'retriable' },
  { type: 'UNSTABLE_OFFSET_COMMIT', code: 88, surfaces: ['offsets'], category: 'retriable' },
  { type: 'UNKNOWN_TOPIC_OR_PARTITION', code: 3, surfaces: SURFACES, category: 'refreshRetriable' },
  { type: 'LEADER_NOT_AVAILABLE', code: 5, surfaces: SURFACES, category: 'refreshRetriable' },
  { type: 'NOT_LEADER_FOR_PARTITION', code: 6, surfaces: SURFACES, category: 'refreshRetriable' },
  { type: 'REPLICA_NOT_AVAILABLE', code: 9, surfaces: SURFACES, category: 'refreshRetriable' },
  { type: 'NETWORK_EXCEPTION', code: 13, surfaces: SURFACES, category: 'refreshRetriable' },
  {
    type: 'GROUP_COORDINATOR_NOT_AVAILABLE',
    code: 15,
    surfaces: ['txn', 'offsets'],
    category: 'refreshRetriable',
  },
  {
    type: 'NOT_COORDINATOR_FOR_GROUP',
    code: 16,
    surfaces: ['txn', 'offsets'],
    category: 'refreshRetriable',
  },
  { type: 'KAFKA_STORAGE_ERROR', code: 56, surfaces: SURFACES, category: 'refreshRetriable' },
  { type: 'FENCED_LEADER_EPOCH', code: 74, surfaces: SURFACES, category: 'refreshRetriable' },
  { type: 'UNKNOWN_LEADER_EPOCH', code: 75, surfaces: SURFACES, category: 'refreshRetriable' },
  { type: 'UNKNOWN_TOPIC_ID', code: 100, surfaces: SURFACES, category: 'refreshRetriable' },
  { type: 'INCONSISTENT_TOPIC_ID', code: 103, surfaces: SURFACES, category: 'refreshRetriable' },
  { type: 'TRANSACTION_ABORTABLE', code: 120, surfaces: SURFACES, category: 'abortable' },
  { type: 'INVALID_TXN_STATE', code: 48, surfaces: ['produce'], category: 'abortable' },
  { type: 'STALE_MEMBER_EPOCH', code: 113, surfaces: ['offsets'], category: 'abortable' },
  { type: 'ILLEGAL_GENERATION', code: 22, surfaces: ['offsets'], category: 'abortable' },
  { type: 'UNKNOWN_MEMBER_ID', code: 25, surfaces: ['offsets'], category: 'abortable' },
  { type: 'FENCED_INSTANCE_ID', code: 82, surfaces: ['offsets'], category: 'abortable' },
  { type: 'PRODUCER_FENCED', code: 90, surfaces: SURFACES, category: 'applicationRecoverable' },
  {
    type: 'INVALID_PRODUCER_EPOCH',
    code: 47,
    surfaces: SURFACES,
    category: 'applicationRecoverable',
  },
  {
    type: 'INVALID_TXN_STATE',
    code: 48,
    surfaces: ['txn', 'offsets'],
    category: 'applicationRecoverable',
  },
  {
    type: 'INVALID_PRODUCER_ID_MAPPING',
    code: 49,
    surfaces: SURFACES,
    category: 'applicationRecoverable',
  },
  {
    type: 'ILLEGAL_GENERATION',
    code: 22,
    surfaces: ['produce', 'txn'],
    category: 'applicationRecoverable',
  },
  {
    type: 'UNKNOWN_MEMBER_ID',
    code: 25,
    surfaces: ['produce', 'txn'],
    category: 'applicationRecoverable',
  },
  {
    type: 'FENCED_INSTANCE_ID',
    code: 82,
    surfaces: ['produce', 'txn'],
    category: 'applicationRecoverable',
  },
  {
    type: 'CLUSTER_AUTHORIZATION_FAILED',
    code: 31,
    surfaces: SURFACES,
    category: 'invalidConfiguration',
  },
  {
    type: 'TRANSACTIONAL_ID_AUTHORIZATION_FAILED',
    code: 53,
    surfaces: SURFACES,
    category: 'invalidConfiguration',
  },
  {
    type: 'TOPIC_AUTHORIZATION_FAILED',
    code: 29,
    surfaces: SURFACES,
    category: 'invalidConfiguration',
  },
  {
    type: 'GROUP_AUTHORIZATION_FAILED',
    code: 30,
    surfaces: SURFACES,
    category: 'invalidConfiguration',
  },
  {
    type: 'SASL_AUTHENTICATION_FAILED',
    code: 58,
    surfaces: SURFACES,
    category: 'invalidConfiguration',
  },
  { type: 'UNSUPPORTED_VERSION', code: 35, surfaces: SURFACES, category: 'invalidConfiguration' },
  {
    type: 'UNSUPPORTED_FOR_MESSAGE_FORMAT',
    code: 43,
    surfaces: ['produce'],
    category: 'invalidConfiguration',
  },
  { type: 'INVALID_RECORD', code: 87, surfaces: ['produce'], category: 'invalidConfiguration' },
  {
    type: 'INVALID_REQUIRED_ACKS',
    code: 21,
    surfaces: ['produce'],
    category: 'invalidConfiguration',
  },
  {
    type: 'RECORD_LIST_TOO_LARGE',
    code: 18,
    surfaces: ['produce'],
    category: 'invalidConfiguration',
  },
  {
    type: 'MESSAGE_TOO_LARGE',
    code: 10,
    surfaces: ['produce'],
    category: 'invalidConfiguration',
  },
  {
    type: 'INVALID_TIMESTAMP',
    code: 32,
    surfaces: ['produce'],
    category: 'invalidConfiguration',
  },
  {
    type: 'INVALID_TOPIC_EXCEPTION',
    code: 17,
    surfaces: SURFACES,
    category: 'invalidConfiguration',
  },
  {
    type: 'INVALID_TRANSACTION_TIMEOUT',
    code: 50,
    surfaces: ['txn'],
    category: 'invalidConfiguration',
  },
]

/**
 * @param {number} code
 * @param {'produce'|'txn'|'offsets'} surface
 * @returns {Error} a protocol error for `code`, classified for a transactional producer
 */
const classified = (code, surface) => {
  const error = createErrorFromCode(code)
  classify(error, surface, { transactional: true })
  return error
}

describe('classify > one case per row of the category table', () => {
  test.each(TABLE)(
    '$type ($code) on $surfaces is $category',
    ({ type, code, surfaces, category }) => {
      for (const surface of surfaces) {
        for (const transactional of KINDS) {
          const error = createErrorFromCode(code)

          expect(error.type).toEqual(type)
          expect(classify(error, surface, { transactional })).toEqual({ category })
          expect(error.category).toEqual(category)
          expect(error).toBeInstanceOf(KafkaJSProtocolError)
          expect(error.name).toEqual('KafkaJSProtocolError')
        }
      }
    }
  )

  test.each([
    ['OUT_OF_ORDER_SEQUENCE_NUMBER', 45, 'produce', true, 'abortable'],
    ['OUT_OF_ORDER_SEQUENCE_NUMBER', 45, 'produce', false, 'applicationRecoverable'],
    ['UNKNOWN_PRODUCER_ID', 59, 'produce', true, 'abortable'],
    ['UNKNOWN_PRODUCER_ID', 59, 'produce', false, 'applicationRecoverable'],
    ['UNKNOWN_PRODUCER_ID', 59, 'txn', true, 'abortable'],
    ['UNKNOWN_PRODUCER_ID', 59, 'txn', false, 'applicationRecoverable'],
  ])('%s (%i) on %s, transactional %p, is %s', (type, code, surface, transactional, category) => {
    // An idempotent-only producer has no transaction to abort, so the same code that ends a
    // transaction tells it to start over with a new producer.
    const error = createErrorFromCode(code)

    expect(error.type).toEqual(type)
    expect(classify(error, surface, { transactional })).toEqual({ category })
    expect(error.category).toEqual(category)
  })

  test('DUPLICATE_SEQUENCE_NUMBER on produce has no category: the broker already has the batch', () => {
    for (const transactional of KINDS) {
      const error = createErrorFromCode(46)

      expect(classify(error, 'produce', { transactional })).toEqual({
        category: null,
        duplicate: true,
      })
      expect(error.category).toEqual('applicationRecoverable')
    }
  })

  test('a table code off its own surface takes the fall-through', () => {
    expect(classify(createErrorFromCode(87), 'txn')).toEqual({ category: 'applicationRecoverable' })
    expect(classify(createErrorFromCode(15), 'produce')).toEqual({ category: 'retriable' })
    expect(classify(createErrorFromCode(46), 'offsets')).toEqual({
      category: 'applicationRecoverable',
    })
    expect(classify(createErrorFromCode(45), 'txn', { transactional: true })).toEqual({
      category: 'applicationRecoverable',
    })
    expect(classify(createErrorFromCode(59), 'offsets', { transactional: true })).toEqual({
      category: 'applicationRecoverable',
    })
  })

  describe('every row scoped to fewer than all surfaces', () => {
    /**
     * @param {number} code
     * @param {string} surface
     * @param {boolean} retriable the code's flag in the protocol table
     * @returns {string} what the table answers for `code` on `surface`: another row for the same
     *   code, else the category the flag implies
     */
    const expectedOn = (code, surface, retriable) => {
      const row = TABLE.find((r) => r.code === code && r.surfaces.includes(surface))
      return row ? row.category : retriable ? 'retriable' : 'applicationRecoverable'
    }

    const SCOPED = TABLE.filter(({ surfaces }) => surfaces.length < SURFACES.length).flatMap(
      ({ type, code, surfaces }) =>
        SURFACES.filter((surface) => !surfaces.includes(surface)).map((surface) => [
          type,
          code,
          surface,
        ])
    )

    test.each(SCOPED)(
      '%s (%i) off its surface, on %s, takes the fall-through',
      (_t, code, surface) => {
        for (const transactional of KINDS) {
          const error = createErrorFromCode(code)
          const { category } = classify(error, surface, { transactional })

          expect(category).toEqual(expectedOn(code, surface, error.retriable))
          expect(error.category).toEqual(category)
        }
      }
    )
  })

  test('UNSUPPORTED_FOR_MESSAGE_FORMAT from TxnOffsetCommit is not abortable: Java makes it fatal', () => {
    const error = createErrorFromCode(43)

    expect(classify(error, 'offsets', { transactional: true })).toEqual({
      category: 'applicationRecoverable',
    })
    expect(transactionStateFor(error)).toEqual('FATAL_ERROR')
  })
})

describe('classify > a code the table does not name', () => {
  test('an undeclared code is applicationRecoverable and not retriable', () => {
    const fromTable = createErrorFromCode(9999)
    const fromNewerBroker = new KafkaJSProtocolError({
      type: 'FUTURE_ERROR',
      code: 9999,
      retriable: false,
      message: 'An error code this client does not know',
    })

    for (const error of [fromTable, fromNewerBroker]) {
      for (const surface of SURFACES) {
        expect(classify(error, surface, { transactional: true })).toEqual({
          category: 'applicationRecoverable',
        })
        expect(error.retriable).toBe(false)
      }
    }
  })

  test.each([
    [89, 'THROTTLING_QUOTA_EXCEEDED'],
    [41, 'NOT_CONTROLLER'],
  ])('%i (%s), retriable in the protocol table, is retriable on every surface', (code, type) => {
    for (const surface of SURFACES) {
      const error = createErrorFromCode(code)

      expect(error.type).toEqual(type)
      expect(classify(error, surface, { transactional: false })).toEqual({ category: 'retriable' })
    }
  })
})

describe('classify > a stale member on a transactional offset commit', () => {
  test.each([22, 25, 82, 113])(
    'code %i is abortable from TxnOffsetCommit, applicationRecoverable from Produce, and unchanged elsewhere',
    (code) => {
      expect(classify(createErrorFromCode(code), 'offsets', { transactional: true })).toEqual({
        category: 'abortable',
      })
      expect(classify(createErrorFromCode(code), 'produce', { transactional: true })).toEqual({
        category: 'applicationRecoverable',
      })
      // Before a transactional caller classifies it - the consumer's own commit path, say - the
      // code keeps the category its retriable flag gives it.
      expect(createErrorFromCode(code).category).toEqual('applicationRecoverable')
    }
  )
})

describe('the retriable flag and the category agree for every code in the protocol table', () => {
  test.each(errorCodes.map(({ code, type, retriable }) => [code, type, retriable]))(
    '%i %s (retriable: %p)',
    (code, _type, retriable) => {
      expect(RETRIABLE_CATEGORIES.includes(createErrorFromCode(code).category)).toBe(retriable)

      for (const surface of SURFACES) {
        for (const transactional of KINDS) {
          const error = createErrorFromCode(code)
          const { category } = classify(error, surface, { transactional })

          expect(RETRIABLE_CATEGORIES.includes(category)).toBe(retriable)
          expect(RETRIABLE_CATEGORIES.includes(error.category)).toBe(error.retriable === true)
        }
      }
    }
  )
})

describe('the retriable flag and the category agree on every wrapper of every code', () => {
  // The wraps this client builds: the retrier's (src/retry/index.js), the retries-exceeded error, a
  // plain KafkaJSError, and a protocol error rebuilt with the opposite flag.
  const WRAPPERS = [
    (inner) => new KafkaJSNonRetriableError(inner, { cause: inner.cause || inner }),
    (inner) => new KafkaJSNumberOfRetriesExceeded(inner, { retryCount: 5, retryTime: 100 }),
    (inner) => new KafkaJSError(inner, { cause: inner }),
    (inner) => new KafkaJSProtocolError(inner, { retriable: !inner.retriable }),
  ]

  test.each(errorCodes.map(({ code, type }) => [code, type]))('%i %s', (code) => {
    for (const wrap of WRAPPERS) {
      for (const surface of SURFACES) {
        for (const transactional of KINDS) {
          const wrapped = wrap(createErrorFromCode(code))

          expect(RETRIABLE_CATEGORIES.includes(wrapped.category)).toBe(wrapped.retriable === true)

          classify(wrapped, surface, { transactional })

          expect(RETRIABLE_CATEGORIES.includes(wrapped.category)).toBe(wrapped.retriable === true)
        }
      }
    }
  })
})

describe('the category a protocol error carries before its request is known', () => {
  test.each([
    [6, 'refreshRetriable'],
    [31, 'invalidConfiguration'],
    [120, 'abortable'],
    [90, 'applicationRecoverable'],
    [2, 'retriable'],
    [89, 'retriable'],
    // These four depend on the request, so they keep the fall-through until a caller classifies them.
    [87, 'applicationRecoverable'],
    [15, 'retriable'],
    [48, 'applicationRecoverable'],
    [22, 'applicationRecoverable'],
  ])('code %i is %s', (code, category) => {
    const error = createErrorFromCode(code)

    expect(error.category).toEqual(category)
    expect(categoryOf(code, { retriable: error.retriable })).toEqual(category)
  })

  test('an error built from a protocol error keeps its category', () => {
    expect(
      new KafkaJSOffsetOutOfRange(createErrorFromCode(1), { topic: 'topic', partition: 0 }).category
    ).toEqual('applicationRecoverable')
    expect(new KafkaJSNonRetriableError(createErrorFromCode(31)).category).toEqual(
      'invalidConfiguration'
    )
    expect(new KafkaJSNonRetriableError(createErrorFromCode(120)).category).toEqual('abortable')
  })
})

describe('classify stamps the category in place and never re-wraps', () => {
  test('a produce error keeps its class, name and every field the parser attached', async () => {
    const data = await decode(
      Buffer.from(
        require('../../../protocol/requests/produce/fixtures/v10_response_not_leader.json')
      )
    )
    const error = await parse(data).catch((e) => e)

    expect(classify(error, 'produce', { transactional: false })).toEqual({
      category: 'refreshRetriable',
    })
    expect(error).toBeInstanceOf(KafkaJSProtocolError)
    expect(error.name).toEqual('KafkaJSProtocolError')
    expect(error).toMatchObject({
      type: 'NOT_LEADER_FOR_PARTITION',
      code: 6,
      retriable: true,
      category: 'refreshRetriable',
      topic: 'test-topic',
      partition: 0,
      recordErrors: [],
      currentLeader: { leaderId: 2, leaderEpoch: 7 },
      nodeEndpoints: [{ nodeId: 2, host: '127.0.0.1', port: 9095, rack: null }],
    })
  })

  test('a rejected record becomes invalidConfiguration on the same object, record errors intact', () => {
    const error = createErrorFromCode(87)
    const recordErrors = [
      {
        batchIndex: 1,
        errorMessage: 'Compacted topic cannot accept message without key',
        record: { key: null, value: 'v' },
      },
    ]
    Object.assign(error, { topic: 'topic', partition: 3, recordErrors })

    expect(error.category).toEqual('applicationRecoverable')
    expect(classify(error, 'produce', { transactional: true })).toEqual({
      category: 'invalidConfiguration',
    })
    expect(error.category).toEqual('invalidConfiguration')
    expect(error).toBeInstanceOf(KafkaJSProtocolError)
    expect(error).toMatchObject({
      name: 'KafkaJSProtocolError',
      type: 'INVALID_RECORD',
      code: 87,
      retriable: false,
      topic: 'topic',
      partition: 3,
      recordErrors,
    })
  })

  test('an offset-out-of-range error keeps its subclass, topic and partition', () => {
    const error = new KafkaJSOffsetOutOfRange(createErrorFromCode(1), {
      topic: 'topic',
      partition: 2,
    })

    expect(classify(error, 'offsets', { transactional: true })).toEqual({
      category: 'applicationRecoverable',
    })
    expect(error).toBeInstanceOf(KafkaJSOffsetOutOfRange)
    expect(error).toMatchObject({ name: 'KafkaJSOffsetOutOfRange', topic: 'topic', partition: 2 })
  })
})

describe('classify > errors that are not bare protocol errors', () => {
  test('reads the Kafka code of a wrapped error through cause', () => {
    const error = new KafkaJSNonRetriableError('The transaction cannot go on', {
      cause: createErrorFromCode(120),
    })

    expect(classify(error, 'txn', { transactional: true })).toEqual({ category: 'abortable' })
    expect(error.category).toEqual('abortable')
  })

  test('never gives a non-retriable wrapper a retriable category', () => {
    const exceeded = new KafkaJSNumberOfRetriesExceeded(createErrorFromCode(6), {
      retryCount: 5,
      retryTime: 100,
    })

    // A message around a cause carries no code of its own, so the code is read from the cause.
    const gaveUp = new KafkaJSNonRetriableError('Gave up refreshing the metadata', {
      cause: createErrorFromCode(6),
    })

    for (const wrapper of [exceeded, gaveUp]) {
      expect(classify(wrapper, 'produce', { transactional: false })).toEqual({
        category: 'applicationRecoverable',
      })
      expect(wrapper.retriable).toBe(false)
    }
  })

  test('leaves the five category classes as they are, whatever their cause says', () => {
    const errors = [
      new KafkaJSDeliveryTimeout('Delivery timed out', { cause: createErrorFromCode(87) }),
      new KafkaJSTransactionAbortableError('Abort it', { cause: createErrorFromCode(6) }),
      new KafkaJSApplicationRecoverableError('Re-create it', { cause: createErrorFromCode(120) }),
      new KafkaJSInvalidConfigurationError('Fix it', { cause: createErrorFromCode(90) }),
      new KafkaJSRefreshRetriableError('Refresh it', { cause: createErrorFromCode(31) }),
    ]

    for (const error of errors) {
      const category = error.category

      expect(classify(error, 'produce', { transactional: true })).toEqual({ category })
      expect(error.category).toEqual(category)
    }
    expect(errors[0].category).toEqual('abortable')
  })

  test('an error with no Kafka code keeps the category it was built with', () => {
    const refused = new KafkaJSConnectionError('Connection error', { code: 'ECONNREFUSED' })

    expect(classify(new KafkaJSError('socket closed'), 'produce')).toEqual({
      category: 'retriable',
    })
    expect(classify(refused, 'txn')).toEqual({ category: 'retriable' })
    expect(classify(new Error('not ours'), 'produce')).toEqual({
      category: 'applicationRecoverable',
    })
  })

  test('an error from outside this package that says it is retriable is retriable', () => {
    const foreign = Object.assign(new Error('from another library'), { retriable: true })

    expect(classify(foreign, 'produce')).toEqual({ category: 'retriable' })
    expect(foreign.category).toBeUndefined()
  })

  test('a cause chain that loops ends the walk', () => {
    const error = new KafkaJSError('loop')
    error.cause = error

    expect(classify(error, 'txn')).toEqual({ category: 'retriable' })
  })

  test('rejects a surface it does not know', () => {
    expect(() => classify(createErrorFromCode(6), 'transaction')).toThrow(
      'Unknown error surface "transaction"'
    )
  })
})

describe('transactionStateFor', () => {
  test.each([
    ['an abortable code', () => classified(120, 'txn')],
    ['a stale generation on TxnOffsetCommit', () => classified(22, 'offsets')],
    ['a stale member epoch on TxnOffsetCommit', () => classified(113, 'offsets')],
    ['a retriable error that ended a send', () => classified(6, 'produce')],
    [
      'a retriable error the retrier gave up on',
      () =>
        new KafkaJSNumberOfRetriesExceeded(createErrorFromCode(6), {
          retryCount: 5,
          retryTime: 100,
        }),
    ],
    [
      'a delivery timeout',
      () => new KafkaJSDeliveryTimeout('Delivery timed out', { cause: createErrorFromCode(7) }),
    ],
    ['a rejected record', () => classified(87, 'produce')],
    ['a topic ACL', () => classified(29, 'produce')],
    ['a group ACL on TxnOffsetCommit', () => classified(30, 'offsets')],
    ['a group ACL on AddOffsetsToTxn or FindCoordinator', () => classified(30, 'txn')],
    ['an unknown producer id on a transaction request', () => classified(59, 'txn')],
    ['a record list too large', () => classified(18, 'produce')],
    ['an invalid topic', () => classified(17, 'produce')],
    ['an invalid acks value', () => classified(21, 'produce')],
    ['a message format too old', () => classified(43, 'produce')],
    ['a transaction timeout the broker refuses', () => classified(50, 'txn')],
    ['a SASL failure', () => classified(58, 'txn')],
    [
      'a configuration error with no Kafka code',
      () => new KafkaJSInvalidConfigurationError('acks must be -1'),
    ],
  ])('%s arms ABORTABLE_ERROR', (_name, build) => {
    expect(transactionStateFor(build())).toEqual('ABORTABLE_ERROR')
  })

  test.each([
    ['a fenced producer', () => classified(90, 'produce')],
    ['an old epoch', () => classified(47, 'txn')],
    ['a producer id the coordinator lost', () => classified(49, 'produce')],
    ['a stale generation on Produce', () => classified(22, 'produce')],
    ['an INVALID_TXN_STATE from EndTxn', () => classified(48, 'txn')],
    ['a message format too old on TxnOffsetCommit', () => classified(43, 'offsets')],
    ['an unknown code', () => classified(9999, 'produce')],
    ['a cluster ACL', () => classified(31, 'txn')],
    ['a transactional id ACL', () => classified(53, 'txn')],
    ['a broker too old for the request', () => classified(35, 'produce')],
    [
      'the cluster-ACL error raised in place of InitProducerId code 31',
      () =>
        new KafkaJSInvalidConfigurationError('Grant IDEMPOTENT_WRITE', {
          cause: createErrorFromCode(31),
        }),
    ],
    [
      'an application-recoverable error even with a retriable cause',
      () =>
        new KafkaJSApplicationRecoverableError('The bump was not verified', {
          cause: createErrorFromCode(6),
        }),
    ],
    [
      'an application-recoverable error even when its cause is a spent budget',
      () =>
        new KafkaJSApplicationRecoverableError('x', {
          cause: new KafkaJSNumberOfRetriesExceeded(createErrorFromCode(6), {
            retryCount: 5,
            retryTime: 100,
          }),
        }),
    ],
    ['an error that is not ours', () => new Error('not ours')],
  ])('%s arms FATAL_ERROR', (_name, build) => {
    expect(transactionStateFor(build())).toEqual('FATAL_ERROR')
  })
})

describe('transactionStateFor after the retrier has wrapped an error', () => {
  // Runs the real retrier (src/retry/index.js), so a change to its wrap shows up here.
  const retried = (build) =>
    retry({ retries: 0 })(async () => {
      throw build()
    }).catch((error) => error)

  test('a retriable error whose budget ran out arms ABORTABLE_ERROR', async () => {
    const exhausted = await retried(() => createErrorFromCode(6))

    expect(exhausted).toBeInstanceOf(KafkaJSNumberOfRetriesExceeded)
    expect(transactionStateFor(exhausted)).toEqual('ABORTABLE_ERROR')
  })

  test('an application-recoverable error with a retriable cause still arms FATAL_ERROR', async () => {
    const wrapped = await retried(
      () =>
        new KafkaJSApplicationRecoverableError('The bump was not verified', {
          cause: createErrorFromCode(6),
        })
    )

    // The wrap keeps the cause and drops the class: nothing marks this as a spent budget.
    expect(wrapped).toBeInstanceOf(KafkaJSNonRetriableError)
    expect(wrapped).not.toBeInstanceOf(KafkaJSApplicationRecoverableError)
    expect(wrapped.category).toEqual('applicationRecoverable')
    expect(transactionStateFor(wrapped)).toEqual('FATAL_ERROR')
  })

  test('a delivery timeout is abortable as thrown, and answered from its cause once the retrier has wrapped it', async () => {
    const timeout = () =>
      new KafkaJSDeliveryTimeout('Delivery timed out', { cause: createErrorFromCode(31) })
    const asThrown = timeout()

    expect(classify(asThrown, 'produce', { transactional: true })).toEqual({
      category: 'abortable',
    })
    expect(transactionStateFor(asThrown)).toEqual('ABORTABLE_ERROR')

    // The wrap keeps the cause and drops the class, so classify no longer knows what it was given.
    // Classify an error as thrown, before any retrier wrap.
    const wrapped = await retried(timeout)

    expect(wrapped).toBeInstanceOf(KafkaJSNonRetriableError)
    expect(wrapped).not.toBeInstanceOf(KafkaJSDeliveryTimeout)
    expect(wrapped.category).toEqual('abortable')
    expect(classify(wrapped, 'produce', { transactional: true })).toEqual({
      category: 'invalidConfiguration',
    })
    expect(transactionStateFor(wrapped)).toEqual('FATAL_ERROR')
  })

  test('a spent budget wrapped a second time arms FATAL_ERROR, the safe answer', async () => {
    const exhausted = await retried(() => createErrorFromCode(6))
    const wrappedAgain = await retried(() => exhausted)

    // The outer wrap's cause is the protocol error beneath the retries-exceeded error, so the
    // marker is gone; a new producer is the safe answer.
    expect(wrappedAgain).not.toBeInstanceOf(KafkaJSNumberOfRetriesExceeded)
    expect(transactionStateFor(wrappedAgain)).toEqual('FATAL_ERROR')
  })
})
