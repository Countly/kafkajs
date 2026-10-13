const TransactionState = require('../transactionState')
const STATES = require('../../eosManager/transactionStates')
const { createErrorFromCode } = require('../../../protocol/error')
const {
  KafkaJSApplicationRecoverableError,
  KafkaJSInvariantViolation,
  KafkaJSNonRetriableError,
  KafkaJSTransactionAbortableError,
} = require('../../../errors')

const { UNINITIALIZED, READY, IN_TRANSACTION, COMMITTING, ABORTING, ABORTABLE_ERROR, FATAL_ERROR } =
  STATES
const ALL_STATES = [
  UNINITIALIZED,
  READY,
  IN_TRANSACTION,
  COMMITTING,
  ABORTING,
  ABORTABLE_ERROR,
  FATAL_ERROR,
]
const METHODS = [
  'initProducerId',
  'beginTransaction',
  'addPartitionsToTransaction',
  'sendOffsets',
  'commit',
  'abort',
]

const TRANSACTION_ABORTABLE = 120
const PRODUCER_FENCED = 90

/** Every edge the machine takes, from the state diagram. */
const LEGAL_EDGES = [
  [UNINITIALIZED, READY],
  [READY, READY],
  [READY, IN_TRANSACTION],
  [IN_TRANSACTION, COMMITTING],
  [IN_TRANSACTION, ABORTING],
  [COMMITTING, READY],
  [ABORTING, READY],
  [ABORTABLE_ERROR, ABORTING],
]
const ILLEGAL_EDGES = ALL_STATES.flatMap((from) => ALL_STATES.map((to) => [from, to])).filter(
  ([from, to]) => !LEGAL_EDGES.some(([a, b]) => a === from && b === to)
)

/** The calls each non-error state admits, from the legal-calls table. */
const LEGAL_CALLS = {
  [UNINITIALIZED]: ['initProducerId'],
  [READY]: ['initProducerId', 'beginTransaction'],
  [IN_TRANSACTION]: ['addPartitionsToTransaction', 'sendOffsets', 'commit', 'abort'],
  [COMMITTING]: [],
  [ABORTING]: [],
}
const CALLS = Object.entries(LEGAL_CALLS).flatMap(([state, legal]) =>
  METHODS.map((method) => [method, state, legal.includes(method)])
)

const PATHS = {
  [UNINITIALIZED]: [],
  [READY]: [READY],
  [IN_TRANSACTION]: [READY, IN_TRANSACTION],
  [COMMITTING]: [READY, IN_TRANSACTION, COMMITTING],
  [ABORTING]: [READY, IN_TRANSACTION, ABORTING],
  [ABORTABLE_ERROR]: [READY, IN_TRANSACTION],
}

/** A fresh machine driven to `state` along legal edges, or armed into an error state. */
const reach = (state) => {
  const machine = new TransactionState()
  if (state === FATAL_ERROR) {
    machine.armFatal(createErrorFromCode(PRODUCER_FENCED))
    return machine
  }

  for (const step of PATHS[state]) {
    machine.transitionTo(step)
  }

  if (state === ABORTABLE_ERROR) {
    machine.armAbortable(createErrorFromCode(TRANSACTION_ABORTABLE))
  }

  return machine
}

describe('TransactionState', () => {
  test('starts UNINITIALIZED with nothing armed', () => {
    const machine = new TransactionState()

    expect(machine.state()).toEqual(UNINITIALIZED)
    expect(machine.armedError()).toBeNull()
  })

  test.each(LEGAL_EDGES)('%s --> %s is an edge', (from, to) => {
    const machine = reach(from)

    machine.transitionTo(to)

    expect(machine.state()).toEqual(to)
    expect(machine.armedError()).toBeNull()
  })

  test.each(ILLEGAL_EDGES)(
    '%s --> %s throws, naming both states, and moves nothing',
    (from, to) => {
      const machine = reach(from)
      const armed = machine.armedError()

      expect(() => machine.transitionTo(to)).toThrow(KafkaJSNonRetriableError)
      expect(() => machine.transitionTo(to)).toThrow(
        `Transaction state exception: Invalid transition ${from} --> ${to}`
      )
      expect(machine.state()).toEqual(from)
      expect(machine.armedError()).toBe(armed)
    }
  )

  test.each(CALLS)('%s in %s is legal: %s', (method, state, legal) => {
    const machine = reach(state)

    if (legal) {
      expect(() => machine.guard(method)).not.toThrow()
    } else {
      expect(() => machine.guard(method)).toThrow(KafkaJSNonRetriableError)
      expect(() => machine.guard(method)).toThrow(
        `Transaction state exception: Cannot call "${method}" in state "${state}"`
      )
    }
  })

  test.each(['flush', 'constructor', 'toString', '__proto__'])(
    'refuses to guard "%s", which is not a guarded method',
    (method) => {
      expect(() => new TransactionState().guard(method)).toThrow(KafkaJSInvariantViolation)
      expect(() => new TransactionState().guard(method)).toThrow(
        `No transaction state guard for "${method}"`
      )
    }
  )

  test.each([ABORTABLE_ERROR, FATAL_ERROR])(
    'a method it does not know is a programming error, not the armed error, in %s',
    (state) => {
      const machine = reach(state)

      expect(() => machine.guard('flush')).toThrow(KafkaJSInvariantViolation)
    }
  )

  describe('a pending commit', () => {
    /** A machine whose commit's outcome is unknown. */
    const pending = () => {
      const machine = reach(COMMITTING)
      machine.holdCommit()
      return machine
    }

    test('stays COMMITTING and admits commit() alone', () => {
      const machine = pending()

      expect(machine.state()).toEqual(COMMITTING)
      expect(machine.commitPending()).toBe(true)
      expect(() => machine.guard('commit')).not.toThrow()
    })

    test.each(METHODS.filter((method) => method !== 'commit'))(
      '%s throws a KafkaJSNonRetriableError naming the pending commit',
      (method) => {
        const machine = pending()

        expect(() => machine.guard(method)).toThrow(TransactionState.pendingCommitError(method))
        expect(() => machine.guard(method)).toThrow(KafkaJSNonRetriableError)
      }
    )

    test('says commit() did not complete and is to be called again', () => {
      expect(TransactionState.pendingCommitError('abort').message).toEqual(
        'Transaction state exception: Cannot call "abort" while a commit is pending: commit() did not complete; call commit() again'
      )
    })

    test('COMMITTING --> COMMITTING sends it again, and keeps it pending', () => {
      const machine = pending()

      machine.transitionTo(COMMITTING)

      expect(machine.state()).toEqual(COMMITTING)
      expect(machine.commitPending()).toBe(true)
    })

    test('COMMITTING --> READY settles it, and the edge back to COMMITTING is gone', () => {
      const machine = pending()

      machine.transitionTo(READY)

      expect(machine.commitPending()).toBe(false)
      machine.transitionTo(IN_TRANSACTION)
      machine.transitionTo(COMMITTING)
      expect(() => machine.transitionTo(COMMITTING)).toThrow(
        `Transaction state exception: Invalid transition ${COMMITTING} --> ${COMMITTING}`
      )
    })

    test.each([
      ['armAbortable', ABORTABLE_ERROR, TRANSACTION_ABORTABLE],
      ['armFatal', FATAL_ERROR, PRODUCER_FENCED],
    ])('%s settles it in %s', (arm, state, code) => {
      const machine = pending()

      machine[arm](createErrorFromCode(code))

      expect(machine.state()).toEqual(state)
      expect(machine.commitPending()).toBe(false)
      expect(() => machine.guard('commit')).toThrow(machine.armedError())
    })

    test.each(ALL_STATES.filter((state) => state !== COMMITTING))(
      'cannot be held in %s: a programming error',
      (state) => {
        const machine = reach(state)

        expect(() => machine.holdCommit()).toThrow(KafkaJSInvariantViolation)
        expect(machine.commitPending()).toBe(false)
      }
    )
  })

  describe('ABORTABLE_ERROR', () => {
    test('admits abort() alone; every other call throws the armed abortable error', () => {
      const machine = reach(ABORTABLE_ERROR)
      const armed = machine.armedError()

      expect(() => machine.guard('abort')).not.toThrow()
      for (const method of METHODS.filter((name) => name !== 'abort')) {
        let thrown
        try {
          machine.guard(method)
        } catch (e) {
          thrown = e
        }
        expect(thrown).toBe(armed)
      }
    })

    test.each([IN_TRANSACTION, COMMITTING])(
      'is armed from %s, wrapping the error in one that says to abort',
      (from) => {
        const machine = reach(from)
        const cause = createErrorFromCode(TRANSACTION_ABORTABLE)

        machine.armAbortable(cause)

        const armed = machine.armedError()
        expect(machine.state()).toEqual(ABORTABLE_ERROR)
        expect(armed).toBeInstanceOf(KafkaJSTransactionAbortableError)
        expect(armed).toMatchObject({ category: 'abortable', retriable: false })
        expect(armed.cause).toBe(cause)
        expect(armed.message).toContain('call abort()')
        expect(armed.message).toContain(cause.message)
      }
    )

    test('arms an abortable error as it is, and keeps the first one', () => {
      const machine = reach(IN_TRANSACTION)
      const first = new KafkaJSTransactionAbortableError('first')

      machine.armAbortable(first)
      machine.armAbortable(new Error('second'))

      expect(machine.armedError()).toBe(first)
    })

    test('abort() leaves it, and READY forgets the armed error', () => {
      const machine = reach(ABORTABLE_ERROR)

      machine.transitionTo(ABORTING)
      machine.transitionTo(READY)

      expect(machine.armedError()).toBeNull()
      expect(() => machine.guard('beginTransaction')).not.toThrow()
    })

    test('arming while the transaction is being aborted changes nothing', () => {
      const machine = reach(ABORTING)

      machine.armAbortable(createErrorFromCode(TRANSACTION_ABORTABLE))

      expect(machine.state()).toEqual(ABORTING)
      expect(machine.armedError()).toBeNull()
    })

    test.each([UNINITIALIZED, READY])('cannot be armed from %s, outside a transaction', (from) => {
      const machine = reach(from)

      expect(() => machine.armAbortable(new Error('no transaction'))).toThrow(
        `Transaction state exception: Invalid transition ${from} --> ${ABORTABLE_ERROR}`
      )
      expect(machine.state()).toEqual(from)
      expect(machine.armedError()).toBeNull()
    })
  })

  describe('FATAL_ERROR', () => {
    test.each(ALL_STATES)('is armed from %s', (from) => {
      const machine = reach(from)
      const cause = createErrorFromCode(PRODUCER_FENCED)

      machine.armFatal(cause)

      expect(machine.state()).toEqual(FATAL_ERROR)
      expect(machine.armedError()).toBeInstanceOf(KafkaJSApplicationRecoverableError)
    })

    test('wraps the error in one that says to create a new producer', () => {
      const machine = reach(IN_TRANSACTION)
      const cause = createErrorFromCode(PRODUCER_FENCED)

      machine.armFatal(cause)

      const armed = machine.armedError()
      expect(armed).toMatchObject({ category: 'applicationRecoverable', retriable: false })
      expect(armed.cause).toBe(cause)
      expect(armed.message).toContain('create a new producer')
      expect(armed.message).toContain(cause.message)
    })

    test('is terminal: every call throws the armed error and no edge leaves it', () => {
      const machine = reach(FATAL_ERROR)
      const armed = machine.armedError()

      for (const method of METHODS) {
        let thrown
        try {
          machine.guard(method)
        } catch (e) {
          thrown = e
        }
        expect(thrown).toBe(armed)
        expect(thrown).toBeInstanceOf(KafkaJSApplicationRecoverableError)
      }

      machine.armAbortable(new Error('later'))
      machine.armFatal(new Error('later'))
      expect(machine.state()).toEqual(FATAL_ERROR)
      expect(machine.armedError()).toBe(armed)
    })

    test('arms an application-recoverable error as it is', () => {
      const machine = reach(READY)
      const error = new KafkaJSApplicationRecoverableError('fenced')

      machine.armFatal(error)

      expect(machine.armedError()).toBe(error)
    })
  })
})
