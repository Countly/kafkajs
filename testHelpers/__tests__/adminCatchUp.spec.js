const { fenceUntilSettled, describeScramUntilApplied } = require('../adminCatchUp')

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
