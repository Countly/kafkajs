const {
  fenceUntilSettled,
  describeScramUntilApplied,
  scramLoginSettled,
} = require('../adminCatchUp')
const { KafkaJSSASLAuthenticationError, KafkaJSConnectionError } = require('../../src/errors')
const { SCRAM, DIGESTS } = require('../../src/broker/saslAuthenticator/scram')
const { createErrorFromCode } = require('../../src/protocol/error')

const SASL_AUTHENTICATION_FAILED = 58

const CONCURRENT_TRANSACTIONS = 51
const RESOURCE_NOT_FOUND = 91
const CLUSTER_AUTHORIZATION_FAILED = 31
const TRANSACTIONAL_ID_AUTHORIZATION_FAILED = 53

/**
 * A probe that runs each call as given and repeats a poll's check, like the live probes do, but
 * gives up after a few checks instead of waiting out a deadline.
 * @returns {{invoke: Function, poll: Function}} the probe
 */
const probe = () => {
  let checks = 0
  return {
    invoke: (work) => work(),
    poll: async (check) => {
      while (!(await check())) {
        if (++checks >= 10) throw new Error('The wait did not settle')
      }
    },
  }
}

/**
 * @param {Array<object[]>} answers what each successive call resolves with
 * @returns {jest.Mock} a call that answers in order
 */
const answering = (answers) => {
  const call = jest.fn()
  for (const rows of answers) call.mockResolvedValueOnce(rows)
  return call
}

describe('fenceUntilSettled', () => {
  test('repeats the request while the coordinator ends the open transaction, and returns the settled item', async () => {
    const settled = [{ transactionalId: 'tx', producerId: '7', producerEpoch: 2, errorCode: 0 }]
    const admin = {
      fenceProducers: answering([
        [{ transactionalId: 'tx', errorCode: CONCURRENT_TRANSACTIONS }],
        [{ transactionalId: 'tx', errorCode: CONCURRENT_TRANSACTIONS }],
        settled,
      ]),
    }
    expect(await fenceUntilSettled(probe(), admin, 'tx')).toBe(settled)
    expect(admin.fenceProducers).toHaveBeenCalledTimes(3)
    expect(admin.fenceProducers).toHaveBeenCalledWith({ transactionalIds: ['tx'] })
  })

  test('returns any other failure at once, for the caller to judge', async () => {
    const refused = [{ transactionalId: 'tx', errorCode: TRANSACTIONAL_ID_AUTHORIZATION_FAILED }]
    const admin = { fenceProducers: answering([refused]) }
    expect(await fenceUntilSettled(probe(), admin, 'tx')).toBe(refused)
    expect(admin.fenceProducers).toHaveBeenCalledTimes(1)
  })

  test('fails with the probe deadline when the coordinator never finishes', async () => {
    const admin = {
      fenceProducers: jest
        .fn()
        .mockResolvedValue([{ transactionalId: 'tx', errorCode: CONCURRENT_TRANSACTIONS }]),
    }
    await expect(fenceUntilSettled(probe(), admin, 'tx')).rejects.toThrow('did not settle')
    expect(admin.fenceProducers).toHaveBeenCalledTimes(10)
  })
})

describe('describeScramUntilApplied', () => {
  test('repeats the read while the broker has not applied the credential, and returns the applied one', async () => {
    const applied = [
      {
        user: 'u',
        errorCode: 0,
        credentials: [{ mechanism: 'SCRAM-SHA-256', iterations: 4096 }],
      },
    ]
    const admin = {
      describeUserScramCredentials: answering([
        [{ user: 'u', errorCode: RESOURCE_NOT_FOUND }],
        applied,
      ]),
    }
    expect(await describeScramUntilApplied(probe(), admin, 'u')).toBe(applied)
    expect(admin.describeUserScramCredentials).toHaveBeenCalledTimes(2)
    expect(admin.describeUserScramCredentials).toHaveBeenCalledWith({ users: ['u'] })
  })

  test('returns any other failure at once, for the caller to judge', async () => {
    const refused = [{ user: 'u', errorCode: CLUSTER_AUTHORIZATION_FAILED }]
    const admin = { describeUserScramCredentials: answering([refused]) }
    expect(await describeScramUntilApplied(probe(), admin, 'u')).toBe(refused)
    expect(admin.describeUserScramCredentials).toHaveBeenCalledTimes(1)
  })

  test('fails with the probe deadline when the broker never applies the credential', async () => {
    const admin = {
      describeUserScramCredentials: jest
        .fn()
        .mockResolvedValue([{ user: 'u', errorCode: RESOURCE_NOT_FOUND }]),
    }
    await expect(describeScramUntilApplied(probe(), admin, 'u')).rejects.toThrow('did not settle')
    expect(admin.describeUserScramCredentials).toHaveBeenCalledTimes(10)
  })
})

describe('scramLoginSettled', () => {
  const addresses = ['localhost:9094', 'localhost:9097', 'localhost:9100']

  /**
   * Runs the real SCRAM-SHA-256 client against a broker whose first SaslAuthenticate exchange
   * gives `answer`, so each failure arrives exactly as the product wraps it.
   * @param {() => Promise<object>} answer the first exchange's response, or its failure
   * @returns {Promise<void>} the authentication outcome
   */
  const realScram = (answer) =>
    new SCRAM(
      { username: 'user', password: 'secret' },
      'broker',
      9094,
      { debug: () => {}, error: () => {} },
      answer,
      DIGESTS.SHA256
    ).authenticate()

  /** The broker refuses the credential: SaslAuthenticate answers SASL_AUTHENTICATION_FAILED (58). */
  const refused = () =>
    realScram(async () => {
      throw createErrorFromCode(SASL_AUTHENTICATION_FAILED)
    })
  /** The socket fails during the exchange. */
  const transportFailure = () =>
    realScram(async () => {
      throw new KafkaJSConnectionError('socket hang up', { code: 'ECONNRESET' })
    })
  /** The server-first message fails the client's validation. */
  const invalidNonce = () =>
    realScram(async () => ({ r: 'not-the-client-nonce', s: 'c2FsdA==', i: '4096' }))

  /**
   * @param {Error} cause the failure to wrap
   * @returns {Error} a connection error carrying it, as a transport layer may report it
   */
  const wrapped = (cause) => new KafkaJSConnectionError('Connection error: login', { cause })

  /**
   * @param {Record<string, Array<'ok'|(() => Promise<void>)|Error>>} script per-address outcomes
   *   in order; the last one repeats
   * @returns {jest.Mock} a login that follows the script
   */
  const scriptedLogin = (script) => {
    const seen = {}
    return jest.fn(async (address) => {
      const outcomes = script[address]
      const index = Math.min((seen[address] = (seen[address] || 0) + 1) - 1, outcomes.length - 1)
      const outcome = outcomes[index]
      if (outcome === 'ok') return
      if (typeof outcome === 'function') return outcome()
      throw outcome
    })
  }

  /**
   * @param {() => Promise<void>} failing the first broker's login
   * @param {boolean} accepted the mode under test
   * @returns {Promise<{failure: Error, login: jest.Mock}>} the barrier's rejection
   */
  const failingFirstBroker = async (failing, accepted) => {
    const login = scriptedLogin({
      'localhost:9094': [failing],
      'localhost:9097': [accepted ? 'ok' : refused],
      'localhost:9100': [accepted ? 'ok' : refused],
    })
    const failure = await scramLoginSettled(probe(), { addresses, accepted, login }).then(
      () => null,
      (error) => error
    )
    return { failure, login }
  }

  test('waits until a new credential logs in on every broker, not only the first', async () => {
    const login = scriptedLogin({
      'localhost:9094': ['ok'],
      'localhost:9097': [refused, refused, 'ok'],
      'localhost:9100': [refused, 'ok'],
    })

    await scramLoginSettled(probe(), { addresses, accepted: true, login })

    expect(login.mock.calls.map(([address]) => address)).toEqual([
      'localhost:9094',
      'localhost:9097',
      'localhost:9097',
      'localhost:9097',
      'localhost:9100',
      'localhost:9100',
    ])
  })

  test('waits until every broker refuses a deleted credential', async () => {
    const login = scriptedLogin({
      'localhost:9094': [refused],
      'localhost:9097': ['ok', 'ok', refused],
      'localhost:9100': [refused],
    })

    await scramLoginSettled(probe(), { addresses, accepted: false, login })

    expect(login).toHaveBeenCalledTimes(5)
  })

  test('fails at once on a login failure other than an authentication refusal', async () => {
    const broken = new KafkaJSConnectionError('Connection refused')
    const { failure, login } = await failingFirstBroker(async () => {
      throw broken
    }, true)

    expect(login).toHaveBeenCalledTimes(1)
    expect(failure.message).toBe('SCRAM login at localhost:9094 failed: Connection refused')
    expect(failure.cause).toBe(broken)
  })

  test.each([true, false])(
    'a transport failure inside the SCRAM exchange fails at once naming the broker (accepted: %s)',
    async (accepted) => {
      const { failure, login } = await failingFirstBroker(transportFailure, accepted)

      expect(login).toHaveBeenCalledTimes(1)
      expect(failure.message).toBe(
        'SCRAM login at localhost:9094 failed: SASL SCRAM SHA256 authentication failed: Error'
      )
      expect(failure.cause).toBeInstanceOf(KafkaJSSASLAuthenticationError)
    }
  )

  test.each([true, false])(
    'an invalid server nonce fails at once naming the broker (accepted: %s)',
    async (accepted) => {
      const { failure, login } = await failingFirstBroker(invalidNonce, accepted)

      expect(login).toHaveBeenCalledTimes(1)
      expect(failure.message).toBe(
        'SCRAM login at localhost:9094 failed: SASL SCRAM SHA256 authentication failed: Invalid server nonce, it does not start with the client nonce'
      )
    }
  )

  test('an authentication error class without the broker refusal is not a refusal', async () => {
    const { failure, login } = await failingFirstBroker(async () => {
      throw new KafkaJSSASLAuthenticationError('SASL Authentication failed')
    }, false)

    expect(login).toHaveBeenCalledTimes(1)
    expect(failure.message).toBe('SCRAM login at localhost:9094 failed: SASL Authentication failed')
  })

  test('a SCRAM-SHA-512 refusal counts as a refusal', async () => {
    const refused512 = () =>
      new SCRAM(
        { username: 'user', password: 'secret' },
        'broker',
        9094,
        { debug: () => {}, error: () => {} },
        async () => {
          throw createErrorFromCode(SASL_AUTHENTICATION_FAILED)
        },
        DIGESTS.SHA512
      ).authenticate()
    const login = scriptedLogin({
      'localhost:9094': [refused512],
      'localhost:9097': [refused512],
      'localhost:9100': [refused512],
    })

    await scramLoginSettled(probe(), { addresses, accepted: false, login })

    expect(login).toHaveBeenCalledTimes(3)
  })

  test('counts a refusal wrapped in another error as a refusal', async () => {
    const login = scriptedLogin({
      'localhost:9094': [() => refused().catch((error) => Promise.reject(wrapped(error)))],
      'localhost:9097': [refused],
      'localhost:9100': [
        async () => {
          throw wrapped(createErrorFromCode(SASL_AUTHENTICATION_FAILED))
        },
      ],
    })

    await scramLoginSettled(probe(), { addresses, accepted: false, login })

    expect(login).toHaveBeenCalledTimes(3)
  })

  test('a wrapped failure whose cause chain holds no broker refusal fails at once', async () => {
    const { failure, login } = await failingFirstBroker(
      () => transportFailure().catch((error) => Promise.reject(wrapped(error))),
      false
    )

    expect(login).toHaveBeenCalledTimes(1)
    expect(failure.message).toBe('SCRAM login at localhost:9094 failed: Connection error: login')
    expect(failure.cause.cause).toBeInstanceOf(KafkaJSSASLAuthenticationError)
  })

  test('names the broker whose login never settled and its last outcome', async () => {
    const login = scriptedLogin({
      'localhost:9094': ['ok'],
      'localhost:9097': [refused],
      'localhost:9100': ['ok'],
    })

    const failure = await scramLoginSettled(probe(), { addresses, accepted: true, login }).catch(
      (error) => error
    )

    expect(failure.message).toBe(
      'SCRAM login at localhost:9097 was never accepted (last outcome: KafkaJSSASLAuthenticationError SASL SCRAM SHA256 authentication failed: SASL Authentication failed): The wait did not settle'
    )
    expect(failure.cause.message).toBe('The wait did not settle')
  })
})
