const { LedgerAcls } = require('../ledgerAcls')
const { ClusterAclPort } = require('../aclPropagation')

const FIELDS = [
  'resourceType',
  'resourceName',
  'resourcePatternType',
  'principal',
  'host',
  'operation',
  'permissionType',
]
const ANY = 1

const grant = (operation, permissionType = 3) => ({
  resourceType: 2,
  resourceName: 'orders',
  resourcePatternType: 3,
  principal: 'User:ledger',
  host: '*',
  operation,
  permissionType,
})

/**
 * @param {object} filter DescribeAcls or DeleteAcls filter
 * @param {object} binding one ACL
 * @returns {boolean} whether the filter selects the binding
 */
const matches = (filter, binding) =>
  FIELDS.every(
    (field) =>
      filter[field] == null ||
      filter[field] === binding[field] ||
      (['operation', 'permissionType'].includes(field) && filter[field] === ANY)
  )

/**
 * A cluster whose controller applies ACL changes at once and whose brokers apply them after their
 * own lag, as each broker's authorizer follows the metadata log. The public client reads the
 * controller; the port reads each broker.
 */
class DelayedAuthorizers {
  /** @param {Record<number, number>} lag per-broker delay in ms */
  constructor(lag) {
    this.lag = lag
    this.changes = []
    this.client = {
      createAcls: jest.fn(async ({ acl }) => {
        for (const binding of acl) this.changes.push({ at: Date.now(), binding, present: true })
        return true
      }),
      deleteAcls: jest.fn(async ({ filters: [filter] }) => {
        const matched = this.served(0).filter((binding) => matches(filter, binding))
        for (const binding of matched)
          this.changes.push({ at: Date.now(), binding, present: false })
        const matchingAcls = matched.map((binding) => ({ ...binding, errorCode: 0 }))
        return { filterResponses: [{ errorCode: 0, matchingAcls }] }
      }),
      describeAcls: jest.fn(async (filter) => ({
        resources: this.served(0).filter((binding) => matches(filter, binding)),
      })),
    }
    this.port = {
      connect: jest.fn(async () => {}),
      close: jest.fn(async () => {}),
      brokerIds: async () => Object.keys(lag).map(Number),
      describeAclsOn: async (nodeId, filter) =>
        this.served(lag[nodeId]).filter((binding) => matches(filter, binding)),
    }
  }

  /** @param {number} lag delay of the reader @returns {object[]} bindings it serves now */
  served(lag) {
    const live = new Map()
    for (const { at, binding, present } of this.changes) {
      if (at + lag > Date.now()) continue
      const key = JSON.stringify(binding)
      present ? live.set(key, binding) : live.delete(key)
    }
    return [...live.values()]
  }
}

/**
 * A probe that bounds calls by its own abort signal, polls every 250 ms and keeps its cleanup.
 * @returns {object} probe with `controller` and `closers`
 */
const fakeProbe = () => {
  const controller = new AbortController()
  const closers = []
  return {
    controller,
    closers,
    invoke: (work) => {
      const { signal } = controller
      signal.throwIfAborted()
      let onAbort
      const aborted = new Promise((_, reject) => {
        onAbort = () => reject(signal.reason)
        signal.addEventListener('abort', onAbort, { once: true })
      })
      return Promise.race([work(signal), aborted]).finally(() =>
        signal.removeEventListener('abort', onAbort)
      )
    },
    poll: async (predicate) => {
      while (!(await predicate())) await new Promise((resolve) => setTimeout(resolve, 250))
    },
    cleanup: { add: (name, close) => closers.push({ name, close }) },
  }
}

/** @param {Promise<any>} promise @returns {{state: string, value?: any, error?: any}} */
const watch = (promise) => {
  const record = { state: 'pending' }
  promise.then(
    (value) => Object.assign(record, { state: 'resolved', value }),
    (error) => Object.assign(record, { state: 'rejected', error })
  )
  return record
}

describe('LedgerAcls', () => {
  beforeEach(() => jest.useFakeTimers({ now: 0 }))
  afterEach(() => jest.useRealTimers())

  test('grant keeps a dependent read waiting until every broker serves each created ACL', async () => {
    const cluster = new DelayedAuthorizers({ 1: 0, 2: 5000 })
    const acls = new LedgerAcls(fakeProbe(), { openPort: () => cluster.port })
    const masks = [grant(3), grant(4, 2)]
    const order = []

    const granted = watch(
      acls.grant(cluster.client, masks).then(() => order.push('dependent read'))
    )
    await jest.advanceTimersByTimeAsync(4900)
    expect(granted.state).toBe('pending')
    expect(order).toEqual([])

    await jest.advanceTimersByTimeAsync(200)
    expect(granted.state).toBe('resolved')
    expect(order).toEqual(['dependent read'])
    expect(cluster.client.createAcls).toHaveBeenCalledTimes(1)
    expect(cluster.client.createAcls).toHaveBeenCalledWith({ acl: masks })
  })

  test('revoke keeps the next case waiting until every broker stops serving the deleted ACLs', async () => {
    const cluster = new DelayedAuthorizers({ 1: 0, 2: 3000 })
    const acls = new LedgerAcls(fakeProbe(), { openPort: () => cluster.port })
    const allow = grant(8)
    const deny = grant(8, 2)
    await cluster.client.createAcls({ acl: [allow, deny] })
    await jest.advanceTimersByTimeAsync(3000)
    const order = []

    const revoked = watch(
      acls
        .revoke(cluster.client, { ...allow, permissionType: ANY }, [deny, allow])
        .then(() => order.push('next case'))
    )
    await jest.advanceTimersByTimeAsync(2900)
    expect(cluster.client.describeAcls).toHaveBeenCalled()
    expect(revoked.state).toBe('pending')
    expect(order).toEqual([])

    await jest.advanceTimersByTimeAsync(200)
    expect(revoked.state).toBe('resolved')
    expect(order).toEqual(['next case'])
  })

  test('revoke rejects a delete that removed other ACLs than expected', async () => {
    const cluster = new DelayedAuthorizers({ 1: 0 })
    const acls = new LedgerAcls(fakeProbe(), { openPort: () => cluster.port })
    await cluster.client.createAcls({ acl: [grant(8), grant(8, 2)] })

    await expect(
      acls.revoke(cluster.client, { ...grant(8), permissionType: ANY }, [grant(8)])
    ).rejects.toThrow()
  })

  test('an acquisition aborted before its connect settles is closed by the cleanup registered first', async () => {
    const probe = fakeProbe()
    let release
    const events = []
    const cluster = {
      connect: jest.fn(async () => {
        await new Promise((resolve) => (release = resolve))
        events.push('late connect')
      }),
      disconnect: jest.fn(async () => events.push('disconnect')),
    }
    const acls = new LedgerAcls(probe, { openPort: () => new ClusterAclPort(cluster) })
    const reason = new Error('scenario aborted')

    const serving = watch(acls.served(grant(3), [grant(3)]))
    await jest.advanceTimersByTimeAsync(0)
    expect(probe.closers.map(({ name }) => name)).toEqual(['ACL port'])
    probe.controller.abort(reason)
    await jest.advanceTimersByTimeAsync(0)
    expect(serving).toEqual({ state: 'rejected', error: reason })

    const closing = watch(probe.closers[0].close())
    await jest.advanceTimersByTimeAsync(0)
    expect(closing.state).toBe('pending')
    release()
    await jest.advanceTimersByTimeAsync(0)

    expect(closing.state).toBe('resolved')
    expect(events).toEqual(['disconnect', 'late connect', 'disconnect'])
  })
})
