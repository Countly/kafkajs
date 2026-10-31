jest.mock('../../broker/saslAuthenticator')

const SASLAuthenticator = require('../../broker/saslAuthenticator')
const Broker = require('../../broker')
const ConnectionPool = require('../../network/connectionPool')
const { ConsumerOperation } = require('../groupProtocol/interface')
const { KafkaJSCommitFailedError } = require('../../errors')
const { createScriptedCluster } = require('../../../testHelpers/scriptedCluster')
const { connectionOpts, newLogger } = require('testHelpers')
const { disposeConsumer } = require('../disposal')
const { CloseRig, GROUP, NODE, TOPIC, gate, pause, until } = require('./fixtures/closeRig')

const BOUND_MS = 300
const CELL = { topic: TOPIC, partition: 0 }

describe('a real broker acquisition whose connect or authentication is held through retirement', () => {
  /** @type {Array<() => Promise<void>>} */
  const closers = []
  afterEach(async () => {
    for (const close of closers.splice(0)) await close()
  })

  /**
   * A broker over a pool on the scripted cluster. The first socket's own connect callback is kept
   * for the test to deliver late; later sockets connect at once.
   * @param {object} [options]
   * @param {object} [options.sasl] SASL options, which makes the pool authenticate
   * @returns {{ broker: Broker, cluster: object, deliver: () => void, sockets: () => number }}
   */
  const open = ({ sasl } = {}) => {
    const cluster = createScriptedCluster({
      nodes: [{ ...NODE, host: 'node0' }],
      ...(sasl && {
        profile: 'sasl',
        advertise: { SaslHandshake: [1, 1], SaslAuthenticate: [2, 2] },
      }),
    })
    let held = null
    const socketFactory = (options) => {
      if (sasl || held) return cluster.socketFactory(options)
      held = options.onConnect
      return cluster.socketFactory({ ...options, onConnect: () => {} })
    }
    const pool = new ConnectionPool(
      connectionOpts({
        host: 'node0',
        port: 9092,
        socketFactory,
        requestTimeout: 5000,
        sasl,
      })
    )
    const broker = new Broker({ connectionPool: pool, logger: newLogger() })
    closers.push(async () => {
      await pool.destroy({ drainTimeout: 0 }).catch(() => {})
    })
    return { broker, cluster, deliver: () => held(), sockets: () => cluster.sockets().length }
  }
  const retired = () => new KafkaJSCommitFailedError('The close retired this acquisition')
  /** @param {object} cluster @returns {string[]} the APIs written so far */
  const written = (cluster) => cluster.requests().map(({ api }) => api)

  test('a socket that connects after retirement is not negotiated by the old call, and a replacement works', async () => {
    const h = open()
    const old = new ConsumerOperation()
    const error = retired()
    const first = h.broker.connect({ guard: old.guard }).catch((e) => e)
    await until(() => h.sockets() === 1, 'the first socket')
    old.retire(error)
    const replacement = new ConsumerOperation()
    const second = h.broker.connect({ guard: replacement.guard })

    h.deliver()

    expect(await first).toBe(error)
    await second
    expect(written(h.cluster)).toEqual(['ApiVersions'])
    expect(h.broker.isConnected()).toBe(true)
    expect(h.sockets()).toBe(1)
    await expect(h.broker.metadata([])).resolves.toBeDefined()
    replacement.finish()
  })

  test('an authentication that finishes after retirement writes nothing for the old call, and a replacement works', async () => {
    const release = gate()
    const authenticate = jest.fn(() => release.promise)
    SASLAuthenticator.mockImplementation(() => ({ authenticate, sessionLifetime: 0 }))
    const h = open({ sasl: { mechanism: 'plain', username: 'u', password: 'p' } })
    const old = new ConsumerOperation()
    const error = retired()
    const first = h.broker.connect({ guard: old.guard }).catch((e) => e)
    await until(() => authenticate.mock.calls.length === 1, 'the held authentication')
    const framesBefore = written(h.cluster)
    old.retire(error)
    const replacement = new ConsumerOperation()
    const second = h.broker.connect({ guard: replacement.guard })

    release.resolve()

    expect(await first).toBe(error)
    await second
    await pause(20)
    expect(written(h.cluster)).toEqual(framesBefore)
    expect(h.sockets()).toBe(1)
    await expect(h.broker.metadata([])).resolves.toBeDefined()
    replacement.finish()
  })
})

describe.each([true, false])('a close racing a restart, with enforcement %p', (enforce) => {
  /** @type {CloseRig} */
  let rig
  afterEach(async () => {
    await rig.dispose()
  })

  /**
   * A consumer whose every record fails, so its run crashes once its retries are spent.
   * @param {Function} restartOnFailure the user's restart policy
   * @param {boolean} [withPeer=false] start a consumer of another group first
   * @returns {Promise<{ consumer: object, crashes: object[], peer?: object }>}
   */
  const crashing = async (restartOnFailure, withPeer = false) => {
    rig = await CloseRig.open({ enforce })
    const peer = withPeer ? await rig.started({ consumer: { groupId: 'peer-group' } }) : undefined
    const consumer = rig.consumer({
      retry: { retries: 0, initialRetryTime: 10, maxRetryTime: 20, restartOnFailure },
    })
    const crashes = []
    consumer.on(consumer.events.CRASH, ({ payload }) => crashes.push(payload))
    await consumer.connect()
    await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
    await consumer.run({
      eachMessage: async () => {
        throw new Error('handler failed')
      },
    })
    return { consumer, crashes, peer }
  }

  test('a restart policy answering true after the user stopped restarts nothing', async () => {
    const policy = gate()
    const restart = jest.fn(async () => {
      await policy.promise
      return true
    })
    const { consumer, crashes } = await crashing(restart)
    await until(() => restart.mock.calls.length === 1, 'the held policy')
    const joins = rig.frames('JoinGroup').length

    await consumer.disconnect()
    policy.resolve()
    await until(() => crashes.length === 1, 'the crash event')
    await pause(250)

    expect(crashes[0].restart).toBe(false)
    expect(rig.frames('JoinGroup')).toHaveLength(joins)
  })

  test('a restart whose connect is held past a forced close joins nothing and reports no second crash', async () => {
    const { consumer, crashes } = await crashing(async () => true)
    const lookup = rig.hold('FindCoordinator')
    await until(() => lookup.hits === 1, 'the restart connect')
    const joins = rig.frames('JoinGroup').length
    expect(crashes).toHaveLength(1)
    expect(crashes[0].restart).toBe(true)

    await consumer.disconnect({ timeout: 0 })
    lookup.release()
    await pause(300)

    expect(rig.frames('JoinGroup')).toHaveLength(joins)
    expect(crashes).toHaveLength(1)
  })

  test('a forced close leaves a peer consumer of another group running', async () => {
    const { consumer, peer } = await crashing(async () => true, true)
    const lookup = rig.hold('FindCoordinator')
    await until(() => lookup.hits === 1, 'the restart connect')
    await consumer.disconnect({ timeout: 0 })
    lookup.release()
    await pause(150)
    await expect(peer.consumer.position(CELL)).resolves.toBe('3')
    await expect(peer.consumer.commitOffsets([{ ...CELL, offset: '3' }])).resolves.toBeUndefined()
  })
})

/**
 * Makes every consumer of the client borrow one cluster, as the admin's temporary consumers do.
 * @param {object} kafka the client
 * @returns {object} the shared cluster
 */
const shareCluster = (kafka) => {
  const symbol = Object.getOwnPropertySymbols(kafka).find(
    ({ description }) => description === 'private:Kafka:createCluster'
  )
  const shared = kafka[symbol]({})
  kafka[symbol] = () => shared
  return shared
}

describe.each([true, false])(
  'a borrowed consumer disposed over a shared cluster, with enforcement %p',
  (enforce) => {
    /** @type {CloseRig} */
    let rig
    let shared
    afterEach(async () => {
      await rig.dispose()
      await shared.disconnect()
    })
    const open = async () => {
      rig = await CloseRig.open({ enforce })
      shared = shareCluster(rig.kafka)
    }
    /** @returns {Promise<object>} a consumer on the shared cluster, running over the scripted log */
    const running = async (options) => {
      const { consumer } = await rig.started(options)
      return consumer
    }
    const entry = () => shared.groupRegistry.entries.get(GROUP)
    /**
     * @param {import('../offsetManager/offsetOperations')} ledger
     * @returns {Promise<void>} resolves when no ticket is open: at once, or as the last one ends
     */
    const settled = (ledger) =>
      new Promise((resolve) => {
        if (ledger.openTickets === 0) return resolve()
        const end = ledger.end
        ledger.end = function (ticket) {
          end.call(this, ticket)
          if (this.openTickets > 0) return
          ledger.end = end
          resolve()
        }
      })

    test('a held temporary start is asked to stop first, so it joins nothing once it is released', async () => {
      await open()
      const peer = await running({ consumer: { groupId: 'peer-group' } })
      const borrowed = rig.consumer()
      await borrowed.connect()
      await borrowed.subscribe({ topic: TOPIC, fromBeginning: true })
      const lookup = rig.hold('FindCoordinator')
      const joins = rig.frames('JoinGroup').length
      const starting = borrowed.run({ eachMessage: async () => {} }).catch(() => {})
      await until(() => lookup.hits === 1, 'the held start')

      const disposing = disposeConsumer(borrowed)
      await pause(20)
      lookup.release()
      await disposing
      await starting

      expect(rig.frames('JoinGroup')).toHaveLength(joins)
      expect(shared.isConnected()).toBe(true)
      await expect(peer.position(CELL)).resolves.toBe('3')
    })

    test('disposal leaves the cluster, the peer and the paired group state usable and releases its own tickets', async () => {
      await open()
      const peer = await running({ consumer: { groupId: GROUP } })
      const borrowed = rig.consumer({ groupId: GROUP })
      await borrowed.connect()
      await borrowed.subscribe({ topic: TOPIC, fromBeginning: true })
      await borrowed.run({ autoCommit: false, eachMessage: async () => {} })
      const holders = entry().holders.size

      await disposeConsumer(borrowed)

      expect(entry().holders.size).toBe(holders)
      await settled(entry().ledger)
      expect(entry().ledger.openTickets).toBe(0)
      expect(shared.isConnected()).toBe(true)
      await expect(peer.commitOffsets([{ ...CELL, offset: '3' }])).resolves.toBeUndefined()
      expect(rig.frames('OffsetCommit').length).toBeGreaterThan(0)
    })

    test('disposal does not wait for an answer it abandoned, and the ticket of that answer ends when it lands', async () => {
      await open()
      const borrowed = rig.consumer({ groupId: GROUP })
      await borrowed.connect()
      await borrowed.subscribe({ topic: TOPIC, fromBeginning: true })
      const fetched = rig.hold('OffsetFetch')
      await borrowed.run({ autoCommit: false, eachMessage: async () => {} })
      await until(() => fetched.hits === 1, 'the held offset fetch')
      const { ledger } = entry()
      const commits = rig.frames('OffsetCommit').length

      await disposeConsumer(borrowed)
      expect(ledger.openTickets).toBe(1)

      fetched.release()
      await settled(ledger)
      expect(ledger.openTickets).toBe(0)
      expect(rig.frames('OffsetFetch')).toHaveLength(1)
      expect(rig.frames('OffsetCommit')).toHaveLength(commits)
      expect(shared.isConnected()).toBe(true)
    })

    test('the last release of the shared cluster retires the paired cache and ledger', async () => {
      await open()
      const peer = await running({ consumer: { groupId: GROUP } })
      const registry = shared.groupRegistry
      expect(registry.entries.has(GROUP)).toBe(true)
      await disposeConsumer(peer)
      await shared.disconnect()
      expect(registry.entries.has(GROUP)).toBe(false)
      expect(registry.operations.has(GROUP)).toBe(false)
      expect(registry.offsets.has(GROUP)).toBe(false)
    })
  }
)

describe.each([true, false])('a forced close at each stage, with enforcement %p', (enforce) => {
  /** @type {CloseRig} */
  let rig
  const gates = []
  /** @type {string[]} what the detached handler's own late commit settled as */
  let late
  beforeEach(() => {
    late = []
  })
  afterEach(async () => {
    for (const held of gates.splice(0)) held.resolve()
    await rig.dispose()
  })

  /** @param {string} api @returns {object[]} the frames of this API written for the closed member's group */
  const ownFrames = (api) => rig.frames(api).filter(({ fields }) => fields.groupId === GROUP)

  const stages = [
    [
      'a handler held in flight',
      async () => {
        const held = gate()
        gates.push(held)
        let entered = false
        const owner = {}
        const { consumer } = await rig.started({
          delivered: 0,
          run: {
            eachMessage: async () => {
              if (entered) return
              entered = true
              await held.promise
              late.push(
                await owner.consumer.commitOffsets([{ ...CELL, offset: '9' }]).then(
                  () => 'accepted',
                  (error) => error.type ?? error.name
                )
              )
            },
          },
        })
        owner.consumer = consumer
        await until(() => entered, 'the handler')
        return consumer
      },
    ],
    [
      'a written final commit',
      async () => {
        const { consumer } = await rig.started()
        rig.hold('OffsetCommit')
        return consumer
      },
    ],
    [
      'a leave held at the coordinator',
      async () => {
        const { consumer } = await rig.started()
        rig.hold('LeaveGroup')
        return consumer
      },
    ],
  ]

  test.each(stages)(
    '%s: the close resolves at its deadline, a peer keeps working and a replacement starts',
    async (_name, prepare) => {
      rig = await CloseRig.open({ enforce })
      const peer = await rig.started({ consumer: { groupId: 'peer-group' } })
      const closing = await prepare()

      const startedAt = Date.now()
      await closing.disconnect({ timeout: BOUND_MS })
      expect(Date.now() - startedAt).toBeLessThan(BOUND_MS + 1500)

      for (const held of rig.holds) held.release()
      for (const held of gates.splice(0)) held.resolve()
      await pause(150)
      if (_name === 'a handler held in flight') expect(late).toEqual(['CONSUMER_STOPPING'])
      const written = ['OffsetCommit', 'LeaveGroup', 'Heartbeat'].map(
        (api) => ownFrames(api).length
      )
      await pause(150)
      expect(
        ['OffsetCommit', 'LeaveGroup', 'Heartbeat'].map((api) => ownFrames(api).length)
      ).toEqual(written)

      await expect(peer.consumer.position(CELL)).resolves.toBe('3')
      await expect(peer.consumer.commitOffsets([{ ...CELL, offset: '3' }])).resolves.toBeUndefined()
      const replacement = await rig.started({ delivered: 3 })
      expect(replacement.delivered).toHaveLength(3)
    }
  )
})
