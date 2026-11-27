const createAdmin = require('..')
const { KafkaJSInvalidConfigurationError, KafkaJSInvariantViolation } = require('../../errors')

const logger = { namespace: () => logger, debug() {}, info() {}, warn() {}, error() {} }
const retry = { retries: 1, initialRetryTime: 1, maxRetryTime: 1, factor: 0 }
const SHA256 = 1

/** Resolves with the rejection reason, and fails the test when the promise resolves. */
const rejection = (promise) =>
  promise.then(
    () => {
      throw new Error('expected a rejection')
    },
    (error) => error
  )

const userRow = (user, credentialInfos = [{ mechanism: SHA256, iterations: 4096 }]) => ({
  user,
  errorCode: 0,
  credentialInfos,
})

/** An admin whose broker answers every describe with `results`. */
const adminAnswering = (results) => {
  const broker = { describeUserScramCredentials: jest.fn(async () => ({ results })) }
  const cluster = { findConnectedBroker: jest.fn(async () => broker) }
  return { broker, cluster, admin: createAdmin({ cluster, logger, retry }) }
}

describe('SCRAM credential description edges', () => {
  test('a non-retriable failure of the all-users read is not retried', async () => {
    const denied = Object.assign(new Error('denied'), { retriable: false })
    const cluster = { findConnectedBroker: jest.fn().mockRejectedValue(denied) }

    const error = await rejection(
      createAdmin({ cluster, logger, retry }).describeUserScramCredentials()
    )

    expect(error).toBe(denied)
    expect(cluster.findConnectedBroker).toHaveBeenCalledTimes(1)
  })

  test('a retriable failure of the all-users read is retried', async () => {
    const flap = Object.assign(new Error('flap'), { retriable: true })
    const { cluster, admin } = adminAnswering([userRow('alice')])
    cluster.findConnectedBroker.mockRejectedValueOnce(flap)

    const result = await admin.describeUserScramCredentials()

    expect(cluster.findConnectedBroker).toHaveBeenCalledTimes(2)
    expect(result).toEqual([
      {
        user: 'alice',
        errorCode: 0,
        errorMessage: null,
        error: null,
        credentials: [{ mechanism: 'SCRAM-SHA-256', iterations: 4096 }],
      },
    ])
  })

  test('a credentials row that failed locally has an empty credentials list', async () => {
    const unreachable = new Error('unreachable')
    const cluster = { findConnectedBroker: jest.fn().mockRejectedValue(unreachable) }

    const rows = await createAdmin({ cluster, logger, retry }).describeUserScramCredentials({
      users: ['alice'],
    })

    expect(rows).toEqual([
      {
        user: 'alice',
        errorCode: -1,
        errorMessage: null,
        error: unreachable,
        credentials: [],
      },
    ])
  })

  test.each([
    ['results that are not an array', undefined],
    ['an empty user name', [userRow('')]],
    ['a repeated user', [userRow('alice'), userRow('alice')]],
  ])('rejects an all-users answer with %s', async (_label, results) => {
    const { admin } = adminAnswering(results)

    const error = await rejection(admin.describeUserScramCredentials())

    expect(error).toBeInstanceOf(KafkaJSInvariantViolation)
    expect(error.message).toContain('Invalid SCRAM result identity')
  })

  test.each([
    ['a null row', [null]],
    ['a row without credential infos', [userRow('alice', null)]],
  ])('rejects a named-user answer with %s', async (_label, results) => {
    const { admin } = adminAnswering(results)

    const error = await rejection(admin.describeUserScramCredentials({ users: ['alice'] }))

    expect(error).toBeInstanceOf(KafkaJSInvariantViolation)
    expect(error.message).toContain('Invalid SCRAM credentials')
  })

  test.each([
    ['an unknown mechanism', [{ mechanism: 99, iterations: 4096 }]],
    [
      'a mechanism listed twice',
      [
        { mechanism: SHA256, iterations: 4096 },
        { mechanism: SHA256, iterations: 8192 },
      ],
    ],
  ])('rejects a named-user answer with %s', async (_label, credentialInfos) => {
    const { admin } = adminAnswering([userRow('alice', credentialInfos)])

    const error = await rejection(admin.describeUserScramCredentials({ users: ['alice'] }))

    expect(error).toBeInstanceOf(KafkaJSInvariantViolation)
    expect(error.message).toContain('Invalid SCRAM mechanism')
  })
})

// A lone surrogate is written as U+FFFD, so a broker echoes that and not the string that was asked.
describe('SCRAM credential description of a user name with a lone surrogate', () => {
  const echoOf = (name) => Buffer.from(name, 'utf8').toString('utf8')
  /** A broker that names each user as the bytes it read, with `overrides` on each row. */
  const echoing = (overrides = {}) => {
    const asked = []
    const broker = {
      describeUserScramCredentials: jest.fn(async ({ users }) => {
        asked.push(...users.map(({ name }) => name))
        return { results: users.map(({ name }) => ({ ...userRow(echoOf(name)), ...overrides })) }
      }),
    }
    const cluster = { findConnectedBroker: jest.fn(async () => broker) }
    return { asked, broker, cluster, admin: createAdmin({ cluster, logger, retry }) }
  }

  test('is answered under the name the caller gave when the broker echoes the bytes sent', async () => {
    const { admin, asked } = echoing()

    const rows = await admin.describeUserScramCredentials({ users: ['team-\uD800', 'alice'] })

    expect(rows.map(({ user, errorCode }) => [user, errorCode])).toEqual([
      ['team-\uD800', 0],
      ['alice', 0],
    ])
    expect(rows[0].credentials).toEqual([{ mechanism: 'SCRAM-SHA-256', iterations: 4096 }])
    expect(asked.map(echoOf)).toEqual(['team-�', 'alice'])
  })

  test('keeps the error row the broker answered, under the name the caller gave', async () => {
    const { admin, broker } = echoing({ errorCode: 91, credentialInfos: [] })

    const rows = await admin.describeUserScramCredentials({ users: ['team-\uD800'] })

    expect(rows.map(({ user, errorCode }) => [user, errorCode])).toEqual([['team-\uD800', 91]])
    expect(broker.describeUserScramCredentials).toHaveBeenCalledTimes(1)
  })

  test('is refused, like a repeated name, when another user has the same bytes', async () => {
    const { admin, cluster } = echoing()

    const error = await rejection(
      admin.describeUserScramCredentials({ users: ['a\uD800', 'a\uD801'] })
    )

    expect(error).toBeInstanceOf(KafkaJSInvalidConfigurationError)
    expect(cluster.findConnectedBroker).not.toHaveBeenCalled()
  })

  test.each([['bob'], ['team-��']])('is still refused an answer row for %s', async (answered) => {
    const { admin } = adminAnswering([userRow(answered)])

    const error = await rejection(admin.describeUserScramCredentials({ users: ['team-\uD800'] }))

    expect(error).toBeInstanceOf(KafkaJSInvariantViolation)
  })

  test('answers an ordinary Unicode user name under that name', async () => {
    const name = 'über-\u{1F600}'
    const { admin, asked } = echoing()

    const rows = await admin.describeUserScramCredentials({ users: [name] })

    expect(rows.map(({ user }) => user)).toEqual([name])
    expect(asked).toEqual([name])
  })
})

describe('SCRAM credential alteration of a user name with a lone surrogate', () => {
  const echoOf = (name) => Buffer.from(name, 'utf8').toString('utf8')
  const SHA512 = 'SCRAM-SHA-512'
  const remove = (user, mechanism = 'SCRAM-SHA-256') => ({ user, mechanism })
  /** A controller that names each user as the bytes it read, with `errorCode` on each row. */
  const controller = (errorCode = 0) => {
    const sent = []
    const broker = {
      alterUserScramCredentials: jest.fn(async ({ deletions }) => {
        sent.push(...deletions.map(({ name }) => name))
        const users = [...new Set(deletions.map(({ name }) => name))]
        return { results: users.map((user) => ({ user: echoOf(user), errorCode })) }
      }),
    }
    const cluster = {
      refreshMetadata: jest.fn(async () => {}),
      findControllerBroker: jest.fn(async () => broker),
    }
    return { sent, broker, cluster, admin: createAdmin({ cluster, logger, retry }) }
  }

  test('is answered under the name the caller gave when the controller echoes the bytes sent', async () => {
    const { admin, sent } = controller()

    const rows = await admin.alterUserScramCredentials({
      deletions: [remove('team-\uD800'), remove('alice')],
    })

    expect(rows.map(({ user, errorCode }) => [user, errorCode])).toEqual([
      ['team-\uD800', 0],
      ['alice', 0],
    ])
    expect(sent.map(echoOf)).toEqual(['team-�', 'alice'])
  })

  test('keeps the error row the controller answered, under the name the caller gave', async () => {
    const { admin, broker } = controller(41)

    const rows = await admin.alterUserScramCredentials({ deletions: [remove('team-\uD800')] })

    expect(rows.map(({ user, errorCode }) => [user, errorCode])).toEqual([['team-\uD800', 41]])
    expect(broker.alterUserScramCredentials).toHaveBeenCalledTimes(1)
  })

  test('is refused, like a repeated credential, when another user with the same mechanism has the same bytes', async () => {
    const { admin, cluster } = controller()

    const error = await rejection(
      admin.alterUserScramCredentials({ deletions: [remove('a\uD800'), remove('a\uD801')] })
    )

    expect(error).toBeInstanceOf(KafkaJSInvalidConfigurationError)
    expect(cluster.findControllerBroker).not.toHaveBeenCalled()
  })

  test('answers once for two mechanisms of users with the same bytes, as for one user', async () => {
    const { admin, sent } = controller()

    const rows = await admin.alterUserScramCredentials({
      deletions: [remove('a\uD800'), remove('a\uD801', SHA512)],
    })

    expect(rows.map(({ user, errorCode }) => [user, errorCode])).toEqual([['a\uD800', 0]])
    expect(sent.map(echoOf)).toEqual(['a�', 'a�'])
  })

  test.each([['bob'], ['team-��']])('is still refused an answer row for %s', async (answered) => {
    const broker = {
      alterUserScramCredentials: async () => ({ results: [{ user: answered, errorCode: 0 }] }),
    }
    const cluster = {
      refreshMetadata: jest.fn(async () => {}),
      findControllerBroker: jest.fn(async () => broker),
    }

    const error = await rejection(
      createAdmin({ cluster, logger, retry }).alterUserScramCredentials({
        deletions: [remove('team-\uD800')],
      })
    )

    expect(error).toBeInstanceOf(KafkaJSInvariantViolation)
  })

  test('answers an ordinary Unicode user name under that name', async () => {
    const name = 'über-\u{1F600}'
    const { admin, sent } = controller()

    const rows = await admin.alterUserScramCredentials({ deletions: [remove(name)] })

    expect(rows.map(({ user }) => user)).toEqual([name])
    expect(sent).toEqual([name])
  })
})

describe('SCRAM credential alteration edges', () => {
  test('called without options it names the missing credential changes', async () => {
    const cluster = { refreshMetadata: jest.fn(), findControllerBroker: jest.fn() }

    const error = await rejection(
      createAdmin({ cluster, logger, retry }).alterUserScramCredentials()
    )

    expect(error).toBeInstanceOf(KafkaJSInvalidConfigurationError)
    expect(error.message).toContain('Invalid credential changes')
    expect(cluster.findControllerBroker).not.toHaveBeenCalled()
  })
})
