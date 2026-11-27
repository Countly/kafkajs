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

  const RESOURCE_NOT_FOUND = 91
  const DUPLICATE_RESOURCE = 92
  const upsert = (user, mechanism = 'SCRAM-SHA-256') => ({
    user,
    mechanism,
    iterations: 4096,
    password: 'secret',
  })

  /**
   * A controller that answers as Kafka 4.4.0's `ScramControlManager#alterCredentials` does, for a
   * cluster holding the credentials in `existing` (`name/mechanism`, both as written): it reads
   * every deletion and then every upsertion in the order sent, skips every entry of a user that
   * already has an error, gives RESOURCE_NOT_FOUND to a deletion of a credential that is not there,
   * and gives DUPLICATE_RESOURCE to an entry for a user an earlier entry was accepted for, dropping
   * that acceptance. It answers accepted deletions, then accepted upsertions, then errors.
   */
  const kafkaController = (existing = []) => {
    const credentials = new Set(existing)
    const sent = []
    const broker = {
      alterUserScramCredentials: jest.fn(async ({ upsertions, deletions }) => {
        const deleted = new Set()
        const upserted = new Set()
        const errors = new Map()
        for (const { name, mechanism } of deletions) {
          sent.push(name)
          if (errors.has(name)) continue
          if (deleted.delete(name)) errors.set(name, DUPLICATE_RESOURCE)
          else if (!credentials.has(`${name}/${mechanism}`)) errors.set(name, RESOURCE_NOT_FOUND)
          else deleted.add(name)
        }
        for (const { name } of upsertions) {
          sent.push(name)
          if (errors.has(name)) continue
          if (deleted.delete(name) || upserted.delete(name)) errors.set(name, DUPLICATE_RESOURCE)
          else upserted.add(name)
        }
        return {
          results: [
            ...[...deleted, ...upserted].map((name) => ({ user: echoOf(name), errorCode: 0 })),
            ...[...errors].map(([name, errorCode]) => ({ user: echoOf(name), errorCode })),
          ],
        }
      }),
    }
    const cluster = {
      refreshMetadata: jest.fn(async () => {}),
      findControllerBroker: jest.fn(async () => broker),
    }
    return { sent, broker, admin: createAdmin({ cluster, logger, retry }) }
  }

  // The controller answers one result per user, so two mechanisms of one written name are one
  // result, under the first spelling the call gave for the user.
  const twoDeletions = [
    ['one ordinary user', [remove('alice'), remove('alice', SHA512)], 'alice'],
    [
      'a literal U+FFFD name given before a lone-surrogate name with the same bytes',
      [remove('a�'), remove('a\uD800', SHA512)],
      'a�',
    ],
    [
      'a lone-surrogate name given before a literal U+FFFD name with the same bytes',
      [remove('a\uD800'), remove('a�', SHA512)],
      'a\uD800',
    ],
    [
      'two lone-surrogate names with the same bytes',
      [remove('a\uD800'), remove('a\uD801', SHA512)],
      'a\uD800',
    ],
  ]

  test.each(twoDeletions)(
    'answers DUPLICATE_RESOURCE once for two deletions of %s when the first credential exists, under the first spelling',
    async (_label, deletions, first) => {
      const { admin, sent, broker } = kafkaController([`${echoOf(first)}/1`])

      const rows = await admin.alterUserScramCredentials({ deletions })

      expect(rows.map(({ user, errorCode }) => [user, errorCode])).toEqual([
        [first, DUPLICATE_RESOURCE],
      ])
      expect(sent).toEqual([echoOf(first), echoOf(first)])
      expect(broker.alterUserScramCredentials).toHaveBeenCalledTimes(1)
    }
  )

  test.each(twoDeletions)(
    'answers RESOURCE_NOT_FOUND once for two deletions of %s when the first credential is missing, under the first spelling',
    async (_label, deletions, first) => {
      const { admin, broker } = kafkaController([`${echoOf(first)}/2`])

      const rows = await admin.alterUserScramCredentials({ deletions })

      expect(rows.map(({ user, errorCode }) => [user, errorCode])).toEqual([
        [first, RESOURCE_NOT_FOUND],
      ])
      expect(broker.alterUserScramCredentials).toHaveBeenCalledTimes(1)
    }
  )

  // The client names the result by the upsertion, which it reads first; the controller reads the
  // deletion first, so the deletion decides the error.
  test.each([
    ['DUPLICATE_RESOURCE when the deleted credential exists', ['a�/1'], DUPLICATE_RESOURCE],
    ['RESOURCE_NOT_FOUND when it is missing', [], RESOURCE_NOT_FOUND],
  ])(
    'names the result of an upsertion and a deletion of one user by the upsertion, and answers %s',
    async (_label, existing, errorCode) => {
      const { admin, sent } = kafkaController(existing)

      const rows = await admin.alterUserScramCredentials({
        deletions: [remove('a�')],
        upsertions: [upsert('a\uD800', SHA512)],
      })

      expect(rows.map((row) => [row.user, row.errorCode])).toEqual([['a\uD800', errorCode]])
      expect(sent).toEqual(['a�', 'a�'])
    }
  )

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
