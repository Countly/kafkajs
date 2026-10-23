const errors = require('./errors')
const {
  isRebalancing,
  isStaleCoordinator,
  isLostMember,
  isRevoked,
  isFencedInstance,
  isUnknownMemberId,
  isFatalMembership,
  CONSUMER_PROTOCOL_UNSUPPORTED,
  KafkaJSError,
  KafkaJSNonRetriableError,
  KafkaJSNumberOfRetriesExceeded,
  KafkaJSProtocolError,
  KafkaJSRefreshRetriableError,
  KafkaJSTransactionAbortableError,
  KafkaJSApplicationRecoverableError,
  KafkaJSInvalidConfigurationError,
  KafkaJSDeliveryTimeout,
  KafkaJSAggregateError,
  KafkaJSFetcherRebalanceError,
  KafkaJSConnectionError,
  KafkaJSInvalidFrameError,
  KafkaJSCorruptRecordError,
  KafkaJSCommitFailedError,
  isPollIntervalExceeded,
} = errors
const createRetry = require('./retry')
const { createErrorFromCode, errorCodes } = require('./protocol/error')
const { classify } = require('./producer/state/errorClassifier')
const packageEntry = require('../index')

describe('isRebalancing', () => {
  it.each(['REBALANCE_IN_PROGRESS', 'NOT_COORDINATOR_FOR_GROUP', 'ILLEGAL_GENERATION'])(
    'returns true for %s',
    (type) => expect(isRebalancing({ type })).toBe(true)
  )

  it('returns true for the coordinator-loading error (code 14)', () => {
    expect(isRebalancing({ type: 'GROUP_LOAD_IN_PROGRESS', code: 14 })).toBe(true)
  })

  it.each(['UNKNOWN_MEMBER_ID', 'NETWORK_EXCEPTION', 'BROKER_NOT_AVAILABLE'])(
    'returns false for %s',
    (type) => expect(isRebalancing({ type })).toBe(false)
  )

  it('returns false when the error has no type', () => {
    expect(isRebalancing({})).toBe(false)
  })
})

describe('isStaleCoordinator', () => {
  it.each([15, 16])('returns true for code %i regardless of the type string', (code) => {
    expect(isStaleCoordinator({ code })).toBe(true)
  })

  it('returns false for other codes', () => {
    expect(isStaleCoordinator({ code: 14 })).toBe(false)
    expect(isStaleCoordinator({})).toBe(false)
  })
})

describe('isLostMember', () => {
  it.each([22, 25, 82])('returns true for code %i (member is LOST)', (code) => {
    expect(isLostMember({ code })).toBe(true)
  })

  it.each([27, 16, 14])('returns false for code %i (member is not lost)', (code) => {
    expect(isLostMember({ code })).toBe(false)
  })

  it('returns false when the error has no code', () => {
    expect(isLostMember({})).toBe(false)
  })
})

describe('isRevoked', () => {
  it.each([27, 16, 14])(
    'returns true for code %i (rebalance-class, member still valid)',
    (code) => {
      expect(isRevoked(createErrorFromCode(code))).toBe(true)
    }
  )

  it.each([22, 25, 82])('returns false for code %i (member is lost, not revoked)', (code) => {
    expect(isRevoked(createErrorFromCode(code))).toBe(false)
  })

  it('returns true for a type-only REBALANCE_IN_PROGRESS error with no code', () => {
    expect(isRevoked({ type: 'REBALANCE_IN_PROGRESS' })).toBe(true)
  })

  it('returns false when the error has no code or type', () => {
    expect(isRevoked({})).toBe(false)
  })
})

describe('isFencedInstance', () => {
  it('returns true for code 82', () => {
    expect(isFencedInstance({ code: 82 })).toBe(true)
  })

  it('returns false for other codes', () => {
    expect(isFencedInstance({ code: 22 })).toBe(false)
    expect(isFencedInstance({})).toBe(false)
  })
})

describe('isUnknownMemberId', () => {
  it('returns true for code 25', () => {
    expect(isUnknownMemberId({ code: 25 })).toBe(true)
  })

  it('returns false for other codes', () => {
    expect(isUnknownMemberId({ code: 22 })).toBe(false)
    expect(isUnknownMemberId({})).toBe(false)
  })
})

describe('error wrapping preserves the protocol type', () => {
  const protocolError = new KafkaJSProtocolError({
    type: 'REBALANCE_IN_PROGRESS',
    code: 27,
    retriable: false,
    message: 'The group is rebalancing, so a rejoin is needed',
  })

  it('KafkaJSError keeps type and code when wrapping a protocol error', () => {
    const wrapped = new KafkaJSError(protocolError)
    expect(wrapped.type).toEqual('REBALANCE_IN_PROGRESS')
    expect(wrapped.code).toEqual(27)
  })

  it('KafkaJSError does not invent a type when wrapping a string or a plain Error', () => {
    expect(new KafkaJSError('boom')).not.toHaveProperty('type')
    expect(new KafkaJSError(new Error('boom'))).not.toHaveProperty('type')
  })

  it('KafkaJSNumberOfRetriesExceeded keeps the original type and code', () => {
    const exceeded = new KafkaJSNumberOfRetriesExceeded(protocolError, {
      retryCount: 5,
      retryTime: 100,
    })
    expect(exceeded.type).toEqual('REBALANCE_IN_PROGRESS')
    expect(exceeded.code).toEqual(27)
    expect(exceeded.cause).toBe(protocolError)
  })

  it('KafkaJSNumberOfRetriesExceeded digs through a chain of wrappers', () => {
    const exceeded = new KafkaJSNumberOfRetriesExceeded(new KafkaJSError(protocolError), {
      retryCount: 5,
      retryTime: 100,
    })
    expect(exceeded.type).toEqual('REBALANCE_IN_PROGRESS')
  })

  it('KafkaJSNonRetriableError keeps type and code for a fenced static member (code 82)', () => {
    const fencedError = new KafkaJSProtocolError({
      type: 'FENCED_INSTANCE_ID',
      code: 82,
      retriable: false,
      message: 'The broker rejected this static consumer',
    })

    const wrapped = new KafkaJSNonRetriableError(fencedError)
    expect(wrapped.type).toEqual('FENCED_INSTANCE_ID')
    expect(wrapped.code).toEqual(82)
  })
})

describe('membership predicates', () => {
  it('treats a fenced member epoch as a lost membership', () => {
    expect(isLostMember(createErrorFromCode(110))).toBe(true)
    // 110 is only ever raised for a group the coordinator runs under the consumer protocol, so
    // widening the predicate cannot change what a classic member does.
    expect(isLostMember(createErrorFromCode(22))).toBe(true)
    expect(isLostMember(createErrorFromCode(25))).toBe(true)
    expect(isLostMember(createErrorFromCode(82))).toBe(true)
    expect(isLostMember(createErrorFromCode(113))).toBe(false)
  })

  it('names the membership errors a member must never rejoin after', () => {
    for (const code of [81, 111, 112, 128]) {
      expect(isFatalMembership(createErrorFromCode(code))).toBe(true)
    }

    for (const code of [14, 15, 16, 22, 25, 27, 30, 35, 42, 82, 110, 113]) {
      expect(isFatalMembership(createErrorFromCode(code))).toBe(false)
    }

    expect(isFatalMembership({ type: CONSUMER_PROTOCOL_UNSUPPORTED })).toBe(true)
    expect(isFatalMembership({})).toBe(false)
  })
})

describe('the shipped error table, for the codes the membership depends on', () => {
  it('keeps a stale member epoch non-retriable', () => {
    // Flipping this would feed every generic retrier a request that re-sends the same stale
    // epoch, and would auto-restart the consumer through the crash handler's retriable check.
    expect(errorCodes.find(({ code }) => code === 113).retriable).toBe(false)
  })

  test.each([[81], [82], [110], [111], [112], [113], [128], [29], [30], [42], [69]])(
    'code %i is present and non-retriable',
    (code) => {
      const entry = errorCodes.find((row) => row.code === code)
      expect(entry).toBeDefined()
      expect(entry.retriable).toBe(false)
    }
  )
})

const CATEGORIES = [
  'retriable',
  'refreshRetriable',
  'abortable',
  'applicationRecoverable',
  'invalidConfiguration',
]
const RETRIABLE_CATEGORIES = ['retriable', 'refreshRetriable']
const CATEGORY_CLASS_NAMES = [
  'KafkaJSRefreshRetriableError',
  'KafkaJSTransactionAbortableError',
  'KafkaJSApplicationRecoverableError',
  'KafkaJSInvalidConfigurationError',
  'KafkaJSDeliveryTimeout',
]

describe('the five category classes', () => {
  test.each([
    [
      'KafkaJSRefreshRetriableError',
      KafkaJSRefreshRetriableError,
      KafkaJSError,
      true,
      'refreshRetriable',
    ],
    [
      'KafkaJSTransactionAbortableError',
      KafkaJSTransactionAbortableError,
      KafkaJSNonRetriableError,
      false,
      'abortable',
    ],
    [
      'KafkaJSApplicationRecoverableError',
      KafkaJSApplicationRecoverableError,
      KafkaJSNonRetriableError,
      false,
      'applicationRecoverable',
    ],
    [
      'KafkaJSInvalidConfigurationError',
      KafkaJSInvalidConfigurationError,
      KafkaJSNonRetriableError,
      false,
      'invalidConfiguration',
    ],
    [
      'KafkaJSDeliveryTimeout',
      KafkaJSDeliveryTimeout,
      KafkaJSNonRetriableError,
      false,
      'abortable',
    ],
  ])('%s', (name, ErrorClass, Parent, retriable, category) => {
    const cause = new KafkaJSError('the error underneath')
    const error = new ErrorClass('the message', { cause })

    expect(error).toBeInstanceOf(Parent)
    expect(error).toBeInstanceOf(KafkaJSError)
    expect(error).toMatchObject({ name, message: 'the message', retriable, category })
    expect(error.cause).toBe(cause)
  })

  test('a delivery timeout is abortable, whatever error it gave up on', () => {
    const error = new KafkaJSDeliveryTimeout('Delivery timed out after 120000 ms', {
      cause: new KafkaJSError('NOT_LEADER_FOR_PARTITION, still retrying'),
    })

    expect(error.category).toEqual('abortable')
    expect(error.retriable).toBe(false)
  })

  test('each is exported from the package entry as the same class', () => {
    for (const name of CATEGORY_CLASS_NAMES) {
      expect(typeof errors[name]).toEqual('function')
      expect(packageEntry[name]).toBe(errors[name])
    }
  })

  test('error.retriable === true holds for the two retriable categories and for none of the others', () => {
    const byCategory = {
      retriable: new KafkaJSError('a'),
      refreshRetriable: new KafkaJSRefreshRetriableError('b'),
      abortable: new KafkaJSTransactionAbortableError('c'),
      applicationRecoverable: new KafkaJSApplicationRecoverableError('d'),
      invalidConfiguration: new KafkaJSInvalidConfigurationError('e'),
    }

    for (const [category, error] of Object.entries(byCategory)) {
      expect(error.category).toEqual(category)
      expect(error.retriable === true).toBe(RETRIABLE_CATEGORIES.includes(category))
    }
  })
})

describe('every error class this package exports carries an own, enumerable category', () => {
  // Constructor arguments for the classes that need more than a message.
  const ARGUMENTS = {
    KafkaJSOffsetOutOfRange: () => [createErrorFromCode(1), { topic: 'topic', partition: 0 }],
    KafkaJSMemberIdRequired: () => [createErrorFromCode(79), { memberId: 'member' }],
    KafkaJSNumberOfRetriesExceeded: () => [
      createErrorFromCode(7),
      { retryCount: 5, retryTime: 100 },
    ],
    KafkaJSDeleteTopicRecordsError: () => [{ partitions: [] }],
    KafkaJSAggregateError: () => ['two topics failed', []],
  }
  const CLASSES = Object.entries(errors).filter(
    ([, value]) => typeof value === 'function' && value.prototype instanceof Error
  )

  test('the list includes the five category classes', () => {
    const names = CLASSES.map(([name]) => name)
    expect(names).toEqual(expect.arrayContaining(CATEGORY_CLASS_NAMES))
  })

  test.each(CLASSES)('%s', (name, ErrorClass) => {
    const error = new ErrorClass(...(ARGUMENTS[name] ? ARGUMENTS[name]() : ['boom']))

    expect(Object.keys(error)).toContain('category')
    expect(CATEGORIES).toContain(error.category)
    expect(RETRIABLE_CATEGORIES.includes(error.category)).toBe(error.retriable === true)
    expect(JSON.parse(JSON.stringify(error)).category).toEqual(error.category)
  })

  test('the two errors outside the KafkaJSError tree carry it too, and the fetcher signal keeps its name', () => {
    const rebalance = new KafkaJSFetcherRebalanceError()

    expect(new KafkaJSAggregateError('two topics failed', []).category).toEqual(
      'applicationRecoverable'
    )
    expect(rebalance.category).toEqual('applicationRecoverable')
    expect(rebalance.name).toEqual('Error')
  })
})

describe('a protocol error rebuilt from another one', () => {
  test('keeps the category a producer request refined, not the one its code has everywhere', () => {
    const rejected = createErrorFromCode(87)

    expect(rejected.category).toEqual('applicationRecoverable')
    classify(rejected, 'produce', { transactional: true })
    expect(rejected.category).toEqual('invalidConfiguration')

    expect(new KafkaJSProtocolError(rejected).category).toEqual('invalidConfiguration')
  })

  test('takes the category the flag it is built with implies, whatever the row said', () => {
    const row = { type: 'CLUSTER_AUTHORIZATION_FAILED', code: 31, retriable: false, message: 'm' }

    expect(new KafkaJSProtocolError(row).category).toEqual('invalidConfiguration')

    const forced = new KafkaJSProtocolError(row, { retriable: true })

    expect(forced.retriable).toBe(true)
    expect(forced.category).toEqual('retriable')
  })
})

describe('a wrap keeps the wrapped error category when it keeps its retriable flag', () => {
  test('a non-retriable wrap of a non-retriable error keeps its category', () => {
    expect(
      new KafkaJSNonRetriableError(new KafkaJSInvalidConfigurationError('fix the ACL')).category
    ).toEqual('invalidConfiguration')
    expect(
      new KafkaJSNonRetriableError(new KafkaJSTransactionAbortableError('abort it')).category
    ).toEqual('abortable')
  })

  test('a wrap that changes the retriable flag takes the category its own flag implies', () => {
    const exceeded = new KafkaJSNumberOfRetriesExceeded(new KafkaJSRefreshRetriableError('moved'), {
      retryCount: 5,
      retryTime: 100,
    })

    expect(exceeded.retriable).toBe(false)
    expect(exceeded.category).toEqual('applicationRecoverable')
    expect(new KafkaJSError(new KafkaJSInvalidConfigurationError('x')).category).toEqual(
      'retriable'
    )
  })

  test('a message wrapped around a cause keeps the cause category on the same terms', () => {
    const fixTheAcl = new KafkaJSInvalidConfigurationError('fix the ACL')

    expect(
      new KafkaJSNonRetriableError('Could not initialize the producer', { cause: fixTheAcl })
        .category
    ).toEqual('invalidConfiguration')
    expect(
      new KafkaJSNonRetriableError('Could not initialize the producer', {
        cause: new KafkaJSRefreshRetriableError('moved'),
      }).category
    ).toEqual('applicationRecoverable')
    expect(
      new KafkaJSNonRetriableError(new KafkaJSTransactionAbortableError('abort it'), {
        cause: fixTheAcl,
      }).category
    ).toEqual('abortable')
  })

  test('without a wrapped category the retriable flag decides', () => {
    expect(new KafkaJSError('boom').category).toEqual('retriable')
    expect(new KafkaJSError('boom', { retriable: false }).category).toEqual(
      'applicationRecoverable'
    )
    expect(new KafkaJSNonRetriableError(new Error('plain')).category).toEqual(
      'applicationRecoverable'
    )
  })

  test('a protocol error keeps every code its table row gave it', () => {
    for (const row of errorCodes) {
      expect(createErrorFromCode(row.code)).toMatchObject({
        type: row.type,
        code: row.code,
        retriable: row.retriable,
      })
      expect(createErrorFromCode(row.code)).toBeInstanceOf(KafkaJSProtocolError)
    }
  })
})

describe('KafkaJSAggregateError derives retriable and category from its children', () => {
  const refresh = () => new KafkaJSRefreshRetriableError('moved')
  const retriable = () => new KafkaJSError('timed out')
  const terminal = () => new KafkaJSNonRetriableError('exists')

  const inner = (children) => new KafkaJSAggregateError('inner', children)

  // A literal hole is rejected by eslint and prettier, so the sparse lists are built by assignment.
  const onlyHoles = new Array(2)
  const holeAmongRetriable = new Array(3)
  holeAmongRetriable[0] = retriable()
  holeAmongRetriable[2] = retriable()

  test.each([
    ['no children', [], false, 'applicationRecoverable'],
    ['no list', undefined, false, 'applicationRecoverable'],
    ['a null list', null, false, 'applicationRecoverable'],
    ['a list that is not an array', 'plain text', false, 'applicationRecoverable'],
    [
      'an array-like list of retriable children',
      { length: 1, 0: retriable() },
      false,
      'applicationRecoverable',
    ],
    ['a list made only of holes', onlyHoles, false, 'applicationRecoverable'],
    ['a hole among retriable children', holeAmongRetriable, false, 'applicationRecoverable'],
    ['a null child', [null], false, 'applicationRecoverable'],
    [
      'a child whose retriable is truthy but not true',
      [{ retriable: 1 }],
      false,
      'applicationRecoverable',
    ],
    ['an undefined child', [undefined], false, 'applicationRecoverable'],
    ['a null child among retriable ones', [retriable(), null], false, 'applicationRecoverable'],
    ['one string child', ['plain text'], false, 'applicationRecoverable'],
    ['a plain Error child', [new Error('plain')], false, 'applicationRecoverable'],
    ['every child refreshRetriable', [refresh(), refresh()], true, 'refreshRetriable'],
    ['every child retriable', [retriable(), retriable()], true, 'retriable'],
    ['retriable and refreshRetriable children', [retriable(), refresh()], true, 'retriable'],
    [
      'one terminal child among retriable ones',
      [retriable(), terminal()],
      false,
      'applicationRecoverable',
    ],
    ['only terminal children', [terminal()], false, 'applicationRecoverable'],
    ['a retriable nested aggregate', [inner([retriable()]), refresh()], true, 'retriable'],
    [
      'a nested aggregate whose children are all refreshRetriable',
      [inner([refresh()]), refresh()],
      true,
      'refreshRetriable',
    ],
    ['an empty nested aggregate', [inner([]), retriable()], false, 'applicationRecoverable'],
    [
      'a nested aggregate with a terminal child',
      [inner([terminal()]), retriable()],
      false,
      'applicationRecoverable',
    ],
  ])('%s', (_name, children, retriableValue, category) => {
    const error = new KafkaJSAggregateError('two topics failed', children)

    expect(error.retriable).toBe(retriableValue)
    expect(error.category).toEqual(category)
    expect(error.errors).toBe(children)
    expect(JSON.parse(JSON.stringify(error))).toMatchObject({
      retriable: retriableValue,
      category,
    })
  })

  test('a huge sparse list is read without being copied, so construction never throws', () => {
    const huge = new Array(2e8)
    let error

    expect(() => {
      error = new KafkaJSAggregateError('x', huge)
    }).not.toThrow()
    expect(error.retriable).toBe(false)
    expect(error.category).toEqual('applicationRecoverable')
  })

  test('a custom retrier sees a retriable flag it can act on', () => {
    expect(new KafkaJSAggregateError('x', [retriable()]).retriable).toBe(true)
    expect(new KafkaJSAggregateError('x', [terminal()]).retriable).toBe(false)
  })
})

describe('a retrier around a KafkaJSAggregateError', () => {
  const retrier = () => createRetry({ retries: 3, initialRetryTime: 1, maxRetryTime: 1 })

  test('stops after one attempt when a child is not retriable, whatever the others are', async () => {
    const attempt = jest.fn(async () => {
      throw new KafkaJSAggregateError('two topics failed', [
        new KafkaJSError('timed out'),
        new KafkaJSNonRetriableError('already exists'),
      ])
    })

    const error = await retrier()(attempt).catch((e) => e)

    expect(error).toBeInstanceOf(KafkaJSNonRetriableError)
    expect(error.cause).toBeInstanceOf(KafkaJSAggregateError)
    expect(attempt).toHaveBeenCalledTimes(1)
  })

  test('retries an aggregate whose children are all retriable, until the budget is spent', async () => {
    const attempt = jest.fn(async () => {
      throw new KafkaJSAggregateError('two topics failed', [
        new KafkaJSError('timed out'),
        new KafkaJSRefreshRetriableError('moved'),
      ])
    })

    const error = await retrier()(attempt).catch((e) => e)

    expect(error).toBeInstanceOf(KafkaJSNumberOfRetriesExceeded)
    expect(attempt).toHaveBeenCalledTimes(4)
  })
})

describe('KafkaJSInvalidFrameError', () => {
  test('is a retriable connection error that names the rule the frame broke', () => {
    const cause = new Error('socket data listener')
    const error = new KafkaJSInvalidFrameError('frame of -5 bytes', {
      broker: 'localhost:9092',
      size: -5,
      reason: 'negative',
      cause,
    })

    expect(error).toBeInstanceOf(KafkaJSConnectionError)
    expect(error).toBeInstanceOf(KafkaJSError)
    expect(error).toMatchObject({
      name: 'KafkaJSInvalidFrameError',
      message: 'frame of -5 bytes',
      broker: 'localhost:9092',
      size: -5,
      reason: 'negative',
      retriable: true,
      category: 'retriable',
    })
    expect(error.cause).toBe(cause)
  })

  test('has a null size and no cause unless given them', () => {
    const error = new KafkaJSInvalidFrameError('cut short', { reason: 'unreadable' })

    expect(error.size).toBeNull()
    expect(error.cause).toBeUndefined()
  })
})

describe('KafkaJSCorruptRecordError', () => {
  test('is non-retriable and carries the rule the batch broke and where it starts', () => {
    const cause = new Error('while decoding')
    const error = new KafkaJSCorruptRecordError('batch length 5 is below the minimum', {
      reason: 'length',
      offset: '1234567890123',
      cause,
    })

    expect(error).toBeInstanceOf(KafkaJSNonRetriableError)
    expect(error).toMatchObject({
      name: 'KafkaJSCorruptRecordError',
      reason: 'length',
      offset: '1234567890123',
      retriable: false,
      category: 'applicationRecoverable',
    })
    expect(error.cause).toBe(cause)
  })

  test('offset is optional', () => {
    expect(new KafkaJSCorruptRecordError('bad magic', { reason: 'magic' }).offset).toBeUndefined()
  })
})

describe('KafkaJSCommitFailedError', () => {
  const brokerLoss = () =>
    Object.assign(createErrorFromCode(25), { helpUrl: 'https://example.test/unknown-member' })

  test('is non-retriable and applicationRecoverable, and keeps the loss as its cause', () => {
    const cause = brokerLoss()
    const error = new KafkaJSCommitFailedError('commit refused', {
      reason: 'membership-lost',
      cause,
    })

    expect(error).toBeInstanceOf(KafkaJSNonRetriableError)
    expect(error).toMatchObject({
      name: 'KafkaJSCommitFailedError',
      reason: 'membership-lost',
      retriable: false,
      category: 'applicationRecoverable',
    })
    expect(error.cause).toBe(cause)
  })

  test('stays applicationRecoverable when the loss it wraps carries another category', () => {
    const cause = Object.assign(new KafkaJSNonRetriableError('fenced'), { category: 'abortable' })

    const error = new KafkaJSCommitFailedError('commit refused', {
      reason: 'membership-lost',
      cause,
    })

    expect(cause.category).toEqual('abortable')
    expect(error.category).toEqual('applicationRecoverable')
  })

  test('copies type, code and helpUrl from a loss the broker reported', () => {
    const error = new KafkaJSCommitFailedError('commit refused', {
      reason: 'membership-lost',
      cause: brokerLoss(),
    })

    expect(error).toMatchObject({
      type: 'UNKNOWN_MEMBER_ID',
      code: 25,
      helpUrl: 'https://example.test/unknown-member',
    })
  })

  test('a synthetic loss carries no code, and a poll-interval loss keeps its existing type', () => {
    const synthetic = new KafkaJSCommitFailedError('commit refused', {
      reason: 'membership-lost',
      cause: new KafkaJSError('left the group'),
    })
    const pollExpiry = new KafkaJSCommitFailedError('commit refused', {
      reason: 'poll-interval-exceeded',
      cause: new KafkaJSError({
        message: 'poll interval exceeded',
        type: 'POLL_INTERVAL_EXCEEDED',
      }),
    })

    expect(synthetic.code).toBeUndefined()
    expect(synthetic.type).toBeUndefined()
    expect(pollExpiry.type).toEqual('POLL_INTERVAL_EXCEEDED')
    expect(pollExpiry.code).toBeUndefined()
    expect(isPollIntervalExceeded(pollExpiry)).toBe(true)
  })

  test('a stale handler has the type STALE_ASSIGNMENT and no code', () => {
    const error = new KafkaJSCommitFailedError('commit refused', {
      reason: 'stale-assignment',
      cause: new KafkaJSError('the assignment was replaced'),
    })

    expect(error.type).toEqual('STALE_ASSIGNMENT')
    expect(error.code).toBeUndefined()
    expect(isLostMember(error)).toBe(false)
  })

  test('a stale handler copies neither the code nor the helpUrl of a coded cause', () => {
    const error = new KafkaJSCommitFailedError('commit refused', {
      reason: 'stale-assignment',
      cause: brokerLoss(),
    })

    expect(error.type).toEqual('STALE_ASSIGNMENT')
    expect(error.code).toBeUndefined()
    expect(error.helpUrl).toBeUndefined()
    expect(isLostMember(error)).toBe(false)
  })

  test('a stale handler has no code or helpUrl even when it is built from a coded error', () => {
    const error = new KafkaJSCommitFailedError(brokerLoss(), {
      reason: 'stale-assignment',
      cause: brokerLoss(),
    })

    expect(error.type).toEqual('STALE_ASSIGNMENT')
    expect(error.code).toBeUndefined()
    expect(error.helpUrl).toBeUndefined()
  })

  test('the error it is built from keeps its own type, code and helpUrl over the cause', () => {
    const source = Object.assign(createErrorFromCode(82), {
      helpUrl: 'https://example.test/fenced-instance',
    })

    const error = new KafkaJSCommitFailedError(source, {
      reason: 'membership-lost',
      cause: brokerLoss(),
    })

    expect(error).toMatchObject({
      type: 'FENCED_INSTANCE_ID',
      code: 82,
      helpUrl: 'https://example.test/fenced-instance',
    })
  })

  test('isLostMember is unchanged: it is false for code 69 and for a synthetic loss', () => {
    const code69 = createErrorFromCode(69)

    expect(code69.code).toEqual(69)
    expect(isLostMember(code69)).toBe(false)
    expect(isLostMember({ code: 110 })).toBe(true)
    expect(
      isLostMember(
        new KafkaJSCommitFailedError('commit refused', {
          reason: 'membership-lost',
          cause: new KafkaJSError('left the group'),
        })
      )
    ).toBe(false)
  })
})

describe('the new error classes are exported from the package entry', () => {
  test.each(['KafkaJSInvalidFrameError', 'KafkaJSCorruptRecordError', 'KafkaJSCommitFailedError'])(
    '%s',
    (name) => {
      expect(typeof errors[name]).toEqual('function')
      expect(packageEntry[name]).toBe(errors[name])
    }
  )
})

describe('delivery transmission evidence', () => {
  test('keeps cause fields and independent outcomes on distinct timeout instances', () => {
    const cause = new KafkaJSProtocolError({
      type: 'NOT_LEADER_OR_FOLLOWER',
      code: 6,
      message: 'leader moved',
      retriable: true,
    })
    const unsent = new KafkaJSDeliveryTimeout('deadline', { cause, outcome: 'unsent' })
    const unknown = new KafkaJSDeliveryTimeout('deadline', { cause, outcome: 'unknown' })
    expect(unsent).toMatchObject({ type: 'NOT_LEADER_OR_FOLLOWER', code: 6, outcome: 'unsent' })
    expect(unknown).toMatchObject({ type: 'NOT_LEADER_OR_FOLLOWER', code: 6, outcome: 'unknown' })
    expect(unsent).not.toBe(unknown)
    expect(cause).not.toHaveProperty('outcome')
    expect(new KafkaJSDeliveryTimeout('retry budget')).not.toHaveProperty('outcome')
    expect(new KafkaJSConnectionError('connection')).not.toHaveProperty('outcome')
    expect(new KafkaJSInvalidConfigurationError('configuration')).not.toHaveProperty('outcome')
    expect(
      new KafkaJSNumberOfRetriesExceeded(cause, { retryCount: 1, retryTime: 1 })
    ).not.toHaveProperty('outcome')
  })

  test('contains a cause whose diagnostic field access throws', () => {
    const cause = new Error('failure')
    Object.defineProperty(cause, 'code', {
      get() {
        throw new Error('diagnostic access')
      },
    })
    expect(
      () => new KafkaJSDeliveryTimeout('deadline', { cause, outcome: 'unknown' })
    ).not.toThrow()
  })
})
