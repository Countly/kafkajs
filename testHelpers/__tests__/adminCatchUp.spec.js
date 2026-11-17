const {
  fenceUntilSettled,
  describeScramUntilApplied,
  scramLoginSettled,
} = require('../adminCatchUp')
const { KafkaJSSASLAuthenticationError, KafkaJSConnectionError } = require('../../src/errors')

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
  const refusal = () => new KafkaJSSASLAuthenticationError('SASL Authentication failed')

  /**
   * @param {Record<string, Array<'ok'|'refused'|Error>>} script per-address outcomes in order;
   *   the last one repeats
   * @returns {jest.Mock} a login that follows the script
   */
  const scriptedLogin = (script) => {
    const seen = {}
    return jest.fn(async (address) => {
      const outcomes = script[address]
      const index = Math.min((seen[address] = (seen[address] || 0) + 1) - 1, outcomes.length - 1)
      const outcome = outcomes[index]
      if (outcome === 'ok') return
      throw outcome === 'refused' ? refusal() : outcome
    })
  }

  test('waits until a new credential logs in on every broker, not only the first', async () => {
    const login = scriptedLogin({
      'localhost:9094': ['ok'],
      'localhost:9097': ['refused', 'refused', 'ok'],
      'localhost:9100': ['refused', 'ok'],
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
      'localhost:9094': ['refused'],
      'localhost:9097': ['ok', 'ok', 'refused'],
      'localhost:9100': ['refused'],
    })

    await scramLoginSettled(probe(), { addresses, accepted: false, login })

    expect(login).toHaveBeenCalledTimes(5)
  })

  test('fails at once on a login failure other than an authentication refusal', async () => {
    const broken = new KafkaJSConnectionError('Connection refused')
    const login = scriptedLogin({
      'localhost:9094': [broken],
      'localhost:9097': ['ok'],
      'localhost:9100': ['ok'],
    })

    await expect(scramLoginSettled(probe(), { addresses, accepted: true, login })).rejects.toBe(
      broken
    )
    expect(login).toHaveBeenCalledTimes(1)
  })

  test('counts a refusal wrapped in another error as a refusal', async () => {
    const wrapped = new KafkaJSConnectionError('Connection error: authentication', {
      cause: refusal(),
    })
    const login = scriptedLogin({
      'localhost:9094': [wrapped],
      'localhost:9097': ['refused'],
      'localhost:9100': ['refused'],
    })

    await scramLoginSettled(probe(), { addresses, accepted: false, login })

    expect(login).toHaveBeenCalledTimes(3)
  })

  test('names the broker whose login never settled and its last outcome', async () => {
    const login = scriptedLogin({
      'localhost:9094': ['ok'],
      'localhost:9097': ['refused'],
      'localhost:9100': ['ok'],
    })

    const failure = await scramLoginSettled(probe(), { addresses, accepted: true, login }).catch(
      (error) => error
    )

    expect(failure.message).toBe(
      'SCRAM login at localhost:9097 was never accepted (last outcome: KafkaJSSASLAuthenticationError SASL Authentication failed): The wait did not settle'
    )
    expect(failure.cause.message).toBe('The wait did not settle')
  })
})
