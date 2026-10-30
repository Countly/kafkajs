const ConsumerGroup = require('../consumerGroup')
const GroupOwnership = require('../groupOwnership')

/**
 * Uses the real seekOffsets path and an inert manager whose resolver completion is controllable.
 * @returns {object} one acquired partition and its manager
 */
const create = () => {
  const group = Object.create(ConsumerGroup.prototype)
  let offset
  const manager = {
    seek: jest.fn(async () => true),
    resolveOffsets: jest.fn(async () => {
      offset = '7'
      return true
    }),
    resolvedOffsetAt: jest.fn(() => offset),
    takeResetRefusals: () => [],
    committedOffsets: () => ({ records: {} }),
  }
  Object.assign(group, {
    offsetManager: manager,
    assignmentEpoch: 1,
    acquired: new Map([['records|0', { acquiredAt: 4, changedAt: 4 }]]),
    haltedPartitions: new Map(),
    ownership: new GroupOwnership(),
    cluster: { topicId: () => 'current-id' },
    seekOffset: { pop: () => null, has: () => false },
    logger: { debugEnabled: () => false },
    isStopping: () => false,
  })
  return {
    group,
    manager,
    move: (value) => {
      offset = value
    },
  }
}

describe('Consumer next position', () => {
  test('awaits the argument-free resolver and reads the latest applicable position', async () => {
    const { group, manager, move } = create()
    let release
    manager.resolveOffsets.mockImplementation(
      () =>
        new Promise((resolve) => {
          release = () => {
            move('19')
            resolve(true)
          }
        })
    )
    const pending = group.position({ topic: 'records', partition: 0 })
    expect(manager.resolvedOffsetAt).not.toHaveBeenCalled()
    move('12')
    release()
    await expect(pending).resolves.toBe('19')
    expect(manager.resolveOffsets.mock.calls).toEqual([[]])
  })
  test.each(['assignment', 'acquisition', 'incarnation', 'stop', 'manager', 'ownership'])(
    'rejects a position that outlives its %s',
    async (boundary) => {
      const { group, manager, move } = create()
      let release
      manager.resolveOffsets.mockImplementation(
        () =>
          new Promise((resolve) => {
            release = resolve
          })
      )
      const pending = group.position({ topic: 'records', partition: 0 })
      if (boundary === 'assignment') group.assignmentEpoch++
      if (boundary === 'acquisition')
        group.acquired.set('records|0', { acquiredAt: 5, changedAt: 5 })
      if (boundary === 'incarnation') group.cluster.topicId = () => 'new-id'
      if (boundary === 'stop') group.isStopping = () => true
      if (boundary === 'manager') group.offsetManager = { ...manager }
      if (boundary === 'ownership') group.ownership.renew()
      move('19')
      release(true)
      await expect(pending).rejects.toMatchObject({ type: 'CONSUMER_STOPPING' })
    }
  )
  test('keeps a same-offset seek within the same acquisition', async () => {
    const { group, manager, move } = create()
    manager.resolveOffsets.mockImplementation(async () => {
      move('7')
      group.acquired.get('records|0').changedAt++
      return true
    })
    await expect(group.position({ topic: 'records', partition: 0 })).resolves.toBe('7')
  })
  test('returns a sparse next position without inventing a record', async () => {
    const { group, manager, move } = create()
    manager.resolveOffsets.mockImplementation(async () => {
      move('90')
      return true
    })
    await expect(group.position({ topic: 'records', partition: 0 })).resolves.toBe('90')
  })
  test('rejects with the stored reset refusal instead of zero', async () => {
    const { group } = create()
    const error = new Error('no offset')
    group.haltedPartitions.set('records|0', { reason: 'no-offset', error })
    await expect(group.position({ topic: 'records', partition: 0 })).rejects.toBe(error)
  })
  test('validates before invoking resolution', async () => {
    const { group, manager } = create()
    await expect(group.position({ topic: 'records', partition: -1 })).rejects.toThrow(
      'integer partition'
    )
    expect(manager.resolveOffsets).not.toHaveBeenCalled()
  })
})
