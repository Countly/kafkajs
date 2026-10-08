const TransactionVersion = require('../transactionVersion')
const apiKeys = require('../../../protocol/requests/apiKeys')

/**
 * A transaction coordinator as `Broker` keeps its ApiVersions answer.
 * @param {{ level?: number, epoch?: number, versions?: object }} [options] `level` absent means
 *   the feature is not finalized at all
 */
const coordinatorAt = ({ level, epoch = 1, versions = {} } = {}) => ({
  versions,
  finalizedFeatures:
    level === undefined
      ? {}
      : { 'transaction.version': { minVersionLevel: 0, maxVersionLevel: level } },
  finalizedFeaturesEpoch: epoch,
})

describe('TransactionVersion', () => {
  test('is 1 before the first begin()', () => {
    expect(new TransactionVersion().current()).toEqual(1)
  })

  test.each([
    ['level 2', 2],
    ['a level above 2', 3],
  ])('%s with a valid features epoch is TV2', (_, level) => {
    const version = new TransactionVersion()

    expect(version.begin(coordinatorAt({ level }))).toEqual({ version: 2, bumpRequired: false })
    expect(version.current()).toEqual(2)
  })

  test.each([
    ['level 1', coordinatorAt({ level: 1 })],
    ['the feature absent', coordinatorAt()],
    ['no valid features epoch', coordinatorAt({ level: 2, epoch: -1 })],
    [
      'an ApiVersions v2 broker',
      { versions: {}, finalizedFeatures: {}, finalizedFeaturesEpoch: -1 },
    ],
    ['a broker that kept no features at all', {}],
  ])('%s is TV1', (_, coordinator) => {
    const version = new TransactionVersion()

    expect(version.begin(coordinator)).toEqual({ version: 1, bumpRequired: false })
  })

  test('a broker that offers EndTxn v5 but finalizes no transaction version is TV1', () => {
    const coordinator = coordinatorAt({
      versions: { [apiKeys.EndTxn]: { minVersion: 0, maxVersion: 5 } },
    })

    expect(new TransactionVersion().begin(coordinator).version).toEqual(1)
  })

  test('a feature change during a transaction waits for the next begin()', () => {
    const version = new TransactionVersion()
    const coordinator = coordinatorAt({ level: 1, epoch: 4 })
    version.begin(coordinator)

    coordinator.finalizedFeatures = coordinatorAt({ level: 2 }).finalizedFeatures
    coordinator.finalizedFeaturesEpoch = 5

    expect(version.current()).toEqual(1)
    expect(version.begin(coordinator)).toEqual({ version: 2, bumpRequired: true })
    expect(version.current()).toEqual(2)
  })

  test('the table is read again only for a higher features epoch', () => {
    const version = new TransactionVersion()
    version.begin(coordinatorAt({ level: 2, epoch: 7 }))

    expect(version.begin(coordinatorAt({ level: 1, epoch: 7 })).version).toEqual(2)
    expect(version.begin(coordinatorAt({ level: 1, epoch: 6 })).version).toEqual(2)
    expect(version.begin(coordinatorAt({ level: 1, epoch: 8 })).version).toEqual(1)
  })

  test('a coordinator that reports no features keeps the version it has', () => {
    const version = new TransactionVersion()
    version.begin(coordinatorAt({ level: 2, epoch: 3 }))

    expect(version.begin(coordinatorAt({ epoch: -1 }))).toEqual({ version: 2, bumpRequired: false })
  })

  test('a new features epoch that leaves the version at 1 asks for no bump', () => {
    const version = new TransactionVersion()
    version.begin(coordinatorAt({ level: 1, epoch: 5 }))

    expect(version.begin(coordinatorAt({ level: 1, epoch: 6 }))).toEqual({
      version: 1,
      bumpRequired: false,
    })
  })

  test('features epoch 0 is valid', () => {
    expect(new TransactionVersion().begin(coordinatorAt({ level: 2, epoch: 0 }))).toEqual({
      version: 2,
      bumpRequired: false,
    })
  })

  test('features epoch 0 after a coordinator that reported none is read, with a bump', () => {
    const version = new TransactionVersion()
    version.begin({})

    expect(version.begin(coordinatorAt({ level: 2, epoch: 0 }))).toEqual({
      version: 2,
      bumpRequired: true,
    })
  })

  test('only a move from 1 to 2 asks for an epoch bump, and only once', () => {
    const version = new TransactionVersion()

    expect(version.begin(coordinatorAt({ level: 1, epoch: 1 })).bumpRequired).toBe(false)
    expect(version.begin(coordinatorAt({ level: 2, epoch: 2 })).bumpRequired).toBe(true)
    expect(version.begin(coordinatorAt({ level: 2, epoch: 3 })).bumpRequired).toBe(false)
    expect(version.begin(coordinatorAt({ level: 1, epoch: 4 }))).toEqual({
      version: 1,
      bumpRequired: false,
    })
  })
})
