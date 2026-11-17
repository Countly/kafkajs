const { waitForAclBindings, ClusterAclPort, closeEach } = require('../aclPropagation')

const TOPIC = 2
const LITERAL = 3
const WRITE = 4
const DENY = 2
const ALLOW = 3
const ANY = 1

const bob = {
  resourceType: TOPIC,
  resourceName: 'orders',
  resourcePatternType: LITERAL,
  principal: 'User:bob',
  host: '*',
  operation: WRITE,
  permissionType: DENY,
}
const alice = { ...bob, principal: 'User:alice' }
const filter = { ...bob, principal: null, host: null, operation: ANY, permissionType: ANY }

/**
 * Groups bindings the way a DescribeAcls response does: one resource row with its ACLs.
 * @param {object[]} bindings bindings on one resource
 * @returns {object[]} DescribeAcls resources
 */
const grouped = (bindings) =>
  bindings.length === 0
    ? []
    : [
        {
          resourceType: bindings[0].resourceType,
          resourceName: bindings[0].resourceName,
          resourcePatternType: bindings[0].resourcePatternType,
          acls: bindings.map(({ principal, host, operation, permissionType }) => ({
            principal,
            host,
            operation,
            permissionType,
          })),
        },
      ]

/**
 * A two-broker port whose brokers start serving their bindings at scripted fake times, the way
 * each broker's authorizer applies the controller's ACL records on its own schedule.
 */
class ScriptedAclPort {
  /**
   * @param {Map<number, {servesAt: number, removedAt?: number, stallsAt?: number, bindings: object[]}>}
   *   brokers per-node script: bindings are served from `servesAt` until `removedAt`, and from
   *   `stallsAt` a read never answers
   */
  constructor(brokers) {
    this.brokers = brokers
    this.calls = []
  }

  /** @returns {Promise<number[]>} scripted node ids */
  async brokerIds() {
    return [...this.brokers.keys()]
  }

  /**
   * @param {number} nodeId broker to read
   * @param {object} acls DescribeAcls filter
   * @returns {Promise<object[]>} grouped resources the broker serves at the current fake time
   */
  async describeAclsOn(nodeId, acls) {
    this.calls.push({ nodeId, filter: acls })
    const {
      servesAt,
      removedAt = Infinity,
      stallsAt = Infinity,
      bindings,
    } = this.brokers.get(nodeId)
    const now = Date.now()
    if (now >= stallsAt) return new Promise(() => {})
    return grouped(now >= servesAt && now < removedAt ? bindings : [])
  }

  /** @returns {Promise<void>} */
  async close() {}

  /** @param {number} nodeId broker @returns {number} reads made from it */
  reads(nodeId) {
    return this.calls.filter((call) => call.nodeId === nodeId).length
  }
}

/**
 * @param {Promise<any>} promise watched promise
 * @returns {{state: string, value?: any, error?: any}} live settlement record
 */
const watch = (promise) => {
  const record = { state: 'pending' }
  promise.then(
    (value) => Object.assign(record, { state: 'resolved', value }),
    (error) => Object.assign(record, { state: 'rejected', error })
  )
  return record
}

describe('ACL propagation barrier', () => {
  beforeEach(() => jest.useFakeTimers({ now: 0 }))
  afterEach(() => jest.useRealTimers())

  /** Controller 1 serves the binding at once, broker 2 only from 5000 ms. */
  const laggingPort = (bindings = [bob]) =>
    new ScriptedAclPort(
      new Map([
        [1, { servesAt: 0, bindings }],
        [2, { servesAt: 5000, bindings }],
      ])
    )

  test('control: an immediate controller-only read misses the lagging broker', async () => {
    const port = laggingPort()

    const controllerRead = await port.describeAclsOn(1, filter)
    const laggingRead = await port.describeAclsOn(2, filter)

    expect(controllerRead).toEqual(grouped([bob]))
    expect(laggingRead).toEqual([])
  })

  test('the barrier waits until the lagging broker serves the binding', async () => {
    const port = laggingPort()
    const wait = watch(waitForAclBindings({ port, filter, expected: [bob], timeout: 30000 }))

    await jest.advanceTimersByTimeAsync(4900)
    expect(wait.state).toBe('pending')
    expect(port.reads(2)).toBeGreaterThan(1)
    expect(port.reads(1)).toBe(port.reads(2))

    await jest.advanceTimersByTimeAsync(200)
    expect(wait).toEqual({ state: 'resolved', value: undefined })
    expect(port.calls.every((call) => call.filter === filter)).toBe(true)

    const reads = port.calls.length
    await jest.advanceTimersByTimeAsync(10000)
    expect(port.calls).toHaveLength(reads)
  })

  test('a count match with a different principal is a mismatch', async () => {
    const port = new ScriptedAclPort(
      new Map([
        [1, { servesAt: 0, bindings: [bob] }],
        [2, { servesAt: 0, bindings: [alice] }],
      ])
    )
    const wait = watch(waitForAclBindings({ port, filter, expected: [bob], timeout: 1000 }))

    await jest.advanceTimersByTimeAsync(1000)

    expect(wait.state).toBe('rejected')
    expect(wait.error.message).toContain(`broker 2 missing [${JSON.stringify(bob)}]`)
    expect(wait.error.message).toContain(`extra [${JSON.stringify(alice)}]`)
    expect(wait.error.message).not.toContain('broker 1')
  })

  test.each([
    ['resourceType', 3],
    ['resourceName', 'payments'],
    ['resourcePatternType', 4],
    ['host', '10.0.0.1'],
    ['operation', 3],
    ['permissionType', ALLOW],
  ])('a binding that differs only in %s is a mismatch', async (field, value) => {
    const other = { ...bob, [field]: value }
    const port = new ScriptedAclPort(
      new Map([
        [1, { servesAt: 0, bindings: [bob] }],
        [2, { servesAt: 0, bindings: [other] }],
      ])
    )
    const wait = watch(waitForAclBindings({ port, filter, expected: [bob], timeout: 1000 }))

    await jest.advanceTimersByTimeAsync(1000)

    expect(wait.state).toBe('rejected')
    expect(wait.error.message).toContain(
      `broker 2 missing [${JSON.stringify(bob)}] extra [${JSON.stringify(other)}]`
    )
  })

  test('grouped describe responses are flattened before comparison', async () => {
    const port = new ScriptedAclPort(
      new Map([
        [1, { servesAt: 0, bindings: [bob, alice] }],
        [2, { servesAt: 0, bindings: [alice, bob] }],
      ])
    )
    expect(grouped([bob, alice])).toHaveLength(1)

    await expect(
      waitForAclBindings({ port, filter, expected: [alice, bob], timeout: 1000 })
    ).resolves.toBeUndefined()
    expect(port.calls.map((call) => call.nodeId)).toEqual([1, 2])
  })

  test('fails at the overall deadline naming the lagging broker and the diff', async () => {
    const port = new ScriptedAclPort(
      new Map([
        [1, { servesAt: 0, bindings: [bob] }],
        [2, { servesAt: Infinity, bindings: [bob] }],
      ])
    )
    const wait = watch(waitForAclBindings({ port, filter, expected: [bob], timeout: 3000 }))

    await jest.advanceTimersByTimeAsync(2950)
    expect(wait.state).toBe('pending')
    await jest.advanceTimersByTimeAsync(50)

    expect(wait.state).toBe('rejected')
    expect(wait.error.message).toBe(
      `ACL bindings were not served by every broker within 3000 ms: broker 2 missing [${JSON.stringify(
        bob
      )}] extra []`
    )
    const reads = port.calls.length
    await jest.advanceTimersByTimeAsync(10000)
    expect(port.calls).toHaveLength(reads)
  })

  test('an aborted wait stops polling', async () => {
    const port = laggingPort()
    const controller = new AbortController()
    const reason = new Error('case ended')
    const wait = watch(
      waitForAclBindings({
        port,
        filter,
        expected: [bob],
        timeout: 30000,
        signal: controller.signal,
      })
    )

    await jest.advanceTimersByTimeAsync(250)
    expect(port.reads(2)).toBeGreaterThan(0)
    controller.abort(reason)
    await jest.advanceTimersByTimeAsync(0)

    expect(wait).toEqual({ state: 'rejected', error: reason })
    const reads = port.calls.length
    await jest.advanceTimersByTimeAsync(10000)
    expect(port.calls).toHaveLength(reads)
  })

  test('waits until every broker stops serving a deleted binding', async () => {
    const port = new ScriptedAclPort(
      new Map([
        [1, { servesAt: 0, removedAt: 0, bindings: [bob] }],
        [2, { servesAt: 0, removedAt: 3000, bindings: [bob] }],
      ])
    )
    const wait = watch(waitForAclBindings({ port, filter, expected: [], timeout: 30000 }))

    await jest.advanceTimersByTimeAsync(2900)
    expect(wait.state).toBe('pending')
    await jest.advanceTimersByTimeAsync(200)
    expect(wait).toEqual({ state: 'resolved', value: undefined })
  })

  test('an aborted wait clears its pause timer at once', async () => {
    const port = laggingPort()
    const controller = new AbortController()
    const reason = new Error('case ended')
    const wait = watch(
      waitForAclBindings({
        port,
        filter,
        expected: [bob],
        timeout: 30000,
        interval: 10000,
        signal: controller.signal,
      })
    )

    await jest.advanceTimersByTimeAsync(1)
    controller.abort(reason)
    await jest.advanceTimersByTimeAsync(0)

    expect(wait).toEqual({ state: 'rejected', error: reason })
    expect(jest.getTimerCount()).toBe(0)
  })

  test('a deadline that expires during a pause clears the pause timer at once', async () => {
    const port = laggingPort()
    const wait = watch(
      waitForAclBindings({ port, filter, expected: [bob], timeout: 1000, interval: 10000 })
    )

    await jest.advanceTimersByTimeAsync(1000)

    expect(wait.state).toBe('rejected')
    expect(jest.getTimerCount()).toBe(0)
  })

  test('a broker that never answers its first read is named with the diff already observed', async () => {
    const port = new ScriptedAclPort(
      new Map([
        [1, { servesAt: Infinity, bindings: [bob] }],
        [2, { servesAt: 0, stallsAt: 0, bindings: [bob] }],
      ])
    )
    const wait = watch(waitForAclBindings({ port, filter, expected: [bob], timeout: 1000 }))

    await jest.advanceTimersByTimeAsync(999)
    expect(wait.state).toBe('pending')
    await jest.advanceTimersByTimeAsync(1)

    expect(wait.state).toBe('rejected')
    expect(wait.error.message).toBe(
      `ACL bindings were not served by every broker within 1000 ms: broker 1 missing [${JSON.stringify(
        bob
      )}] extra []; broker 2 did not answer its read`
    )
    expect(jest.getTimerCount()).toBe(0)
  })

  test('a broker that stalls in a later round is named as unanswered beside the last diffs', async () => {
    const port = new ScriptedAclPort(
      new Map([
        [1, { servesAt: 0, stallsAt: 50, bindings: [bob] }],
        [2, { servesAt: Infinity, bindings: [bob] }],
      ])
    )
    const wait = watch(waitForAclBindings({ port, filter, expected: [bob], timeout: 3000 }))

    await jest.advanceTimersByTimeAsync(3000)

    expect(port.reads(1)).toBe(2)
    expect(port.reads(2)).toBe(1)
    expect(wait.state).toBe('rejected')
    expect(wait.error.message).toBe(
      `ACL bindings were not served by every broker within 3000 ms: broker 1 did not answer its read; broker 2 missing [${JSON.stringify(
        bob
      )}] extra []`
    )
  })

  test('a broker list that is never read fails at the deadline saying so', async () => {
    const port = laggingPort()
    port.brokerIds = () => new Promise(() => {})
    const wait = watch(waitForAclBindings({ port, filter, expected: [bob], timeout: 1000 }))

    await jest.advanceTimersByTimeAsync(1000)

    expect(wait.state).toBe('rejected')
    expect(wait.error.message).toBe(
      'ACL bindings were not served by every broker within 1000 ms: the broker list was not read'
    )
  })

  test('close disconnects the test-owned cluster', async () => {
    const brokers = {
      1: { describeAcls: jest.fn(async () => ({ resources: grouped([bob]) })) },
      2: { describeAcls: jest.fn(async () => ({ resources: [] })) },
    }
    const cluster = {
      connect: jest.fn(async () => {}),
      refreshMetadata: jest.fn(async () => {}),
      getNodeIds: () => ['2', '1'],
      findBroker: jest.fn(async ({ nodeId }) => brokers[nodeId]),
      disconnect: jest.fn(async () => {}),
    }

    const port = await ClusterAclPort.open(cluster)
    expect(cluster.connect).toHaveBeenCalledTimes(1)
    expect(await port.brokerIds()).toEqual([1, 2])
    expect(await port.describeAclsOn(2, filter)).toEqual([])
    expect(brokers[2].describeAcls).toHaveBeenCalledWith(filter)
    expect(brokers[1].describeAcls).not.toHaveBeenCalled()
    expect(cluster.disconnect).not.toHaveBeenCalled()

    await port.close()
    expect(cluster.disconnect).toHaveBeenCalledTimes(1)

    const refused = new Error('connect refused')
    const failing = { ...cluster, connect: jest.fn(async () => Promise.reject(refused)) }
    failing.disconnect = jest.fn(async () => {})
    await expect(ClusterAclPort.open(failing)).rejects.toBe(refused)
    expect(failing.disconnect).toHaveBeenCalledTimes(1)
  })

  /**
   * @returns {{promise: Promise<any>, resolve: Function}} a promise settled by the test
   */
  const deferred = () => {
    let resolve
    const promise = new Promise((done) => (resolve = done))
    return { promise, resolve }
  }

  test('close during a pending connect disconnects again once the connect settles', async () => {
    const connected = deferred()
    const events = []
    const cluster = {
      connect: jest.fn(async () => {
        events.push('connect started')
        await connected.promise
        events.push('connect settled')
      }),
      disconnect: jest.fn(async () => events.push('disconnect')),
    }
    const port = new ClusterAclPort(cluster)

    const connecting = watch(port.connect())
    const closing = watch(port.close())
    await jest.advanceTimersByTimeAsync(0)
    expect(closing.state).toBe('pending')

    connected.resolve()
    await jest.advanceTimersByTimeAsync(0)

    expect(connecting.state).toBe('resolved')
    expect(closing).toEqual({ state: 'resolved', value: undefined })
    expect(events).toEqual(['connect started', 'disconnect', 'connect settled', 'disconnect'])
    await expect(port.brokerIds()).rejects.toThrow('ACL port is closed')
    await expect(port.connect()).rejects.toThrow('ACL port is closed')
  })

  test('a read whose broker lookup finishes after close sends nothing', async () => {
    const found = deferred()
    const broker = { describeAcls: jest.fn(async () => ({ resources: [] })) }
    const cluster = {
      findBroker: jest.fn(() => found.promise),
      disconnect: jest.fn(async () => {}),
    }
    const port = new ClusterAclPort(cluster)

    const read = watch(port.describeAclsOn(2, filter))
    const closing = watch(port.close())
    await jest.advanceTimersByTimeAsync(0)
    found.resolve(broker)
    await jest.advanceTimersByTimeAsync(0)

    expect(read.state).toBe('rejected')
    expect(read.error.message).toBe('ACL port is closed')
    expect(broker.describeAcls).not.toHaveBeenCalled()
    expect(closing.state).toBe('resolved')
    expect(cluster.disconnect).toHaveBeenCalledTimes(2)
  })

  test('a connect that settles during the first disconnect is disconnected again before close resolves', async () => {
    const connected = deferred()
    const firstDisconnect = deferred()
    const events = []
    let connectedNow = false
    const cluster = {
      connect: jest.fn(async () => {
        await connected.promise
        connectedNow = true
        events.push('late connect opens')
      }),
      disconnect: jest.fn(async () => {
        events.push('disconnect')
        if (cluster.disconnect.mock.calls.length === 1) await firstDisconnect.promise
        connectedNow = false
      }),
    }
    const port = new ClusterAclPort(cluster)

    watch(port.connect())
    const closing = watch(port.close())
    await jest.advanceTimersByTimeAsync(0)
    connected.resolve()
    await jest.advanceTimersByTimeAsync(0)
    expect(closing.state).toBe('pending')
    firstDisconnect.resolve()
    await jest.advanceTimersByTimeAsync(0)

    expect(closing).toEqual({ state: 'resolved', value: undefined })
    expect(events).toEqual(['disconnect', 'late connect opens', 'disconnect'])
    expect(connectedNow).toBe(false)
  })

  test('a failed first disconnect still waits for running work and disconnects again', async () => {
    const connected = deferred()
    const refused = new Error('disconnect failed')
    const cluster = {
      connect: jest.fn(() => connected.promise),
      disconnect: jest
        .fn()
        .mockImplementationOnce(async () => Promise.reject(refused))
        .mockImplementation(async () => {}),
    }
    const port = new ClusterAclPort(cluster)

    watch(port.connect())
    const closing = watch(port.close())
    await jest.advanceTimersByTimeAsync(0)
    expect(closing.state).toBe('pending')
    connected.resolve()
    await jest.advanceTimersByTimeAsync(0)

    expect(closing).toEqual({ state: 'rejected', error: refused })
    expect(cluster.disconnect).toHaveBeenCalledTimes(2)
  })
})

describe('closeEach', () => {
  test('runs every closer when an earlier one fails and rethrows the first failure', async () => {
    const adminFailure = new Error('admin disconnect failed')
    const order = []
    const admin = jest.fn(async () => {
      order.push('admin')
      throw adminFailure
    })
    const port = jest.fn(async () => order.push('port'))

    await expect(closeEach(admin, port)).rejects.toBe(adminFailure)
    expect(order).toEqual(['admin', 'port'])
  })

  test('skips absent closers and resolves when every closer succeeds', async () => {
    const port = jest.fn(async () => {})

    await expect(closeEach(null, port, undefined)).resolves.toBeUndefined()
    expect(port).toHaveBeenCalledTimes(1)
  })
})
