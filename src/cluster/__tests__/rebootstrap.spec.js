const Cluster = require('../index')
const Encoder = require('../../protocol/encoder')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const apiKeys = require('../../protocol/requests/apiKeys')
const { CLUSTER_REBOOTSTRAP } = require('../instrumentationEvents')
const {
  KafkaJSBrokerNotFound,
  KafkaJSSASLAuthenticationError,
  KafkaJSNonRetriableError,
} = require('../../errors')
const {
  newLogger,
  connectionOpts,
  plainTextBrokers,
  createFakeSocketFactory,
  respondWithFrame,
  readRequestHeader,
} = require('testHelpers')

/** A v3/v4-shaped ApiVersions body advertising Metadata up to v13. */
const apiVersionsBody = () =>
  new Encoder()
    .writeInt16(0)
    .writeUVarIntArray([
      new Encoder().writeInt16(apiKeys.Metadata).writeInt16(0).writeInt16(13).writeTaggedFields(),
      new Encoder().writeInt16(apiKeys.ApiVersions).writeInt16(0).writeInt16(4).writeTaggedFields(),
    ])
    .writeInt32(0)
    .writeTaggedFields().buffer

/** A Metadata v13 response body carrying the given top-level error code. */
const metadataBody = (errorCode = 0) =>
  new Encoder()
    .writeInt32(0)
    .writeUVarIntArray([
      new Encoder()
        .writeInt32(0)
        .writeUVarIntString('127.0.0.1')
        .writeInt32(9092)
        .writeUVarIntString(null)
        .writeTaggedFields(),
    ])
    .writeUVarIntString('test-cluster-id')
    .writeInt32(0)
    .writeUVarIntArray([])
    .writeInt16(errorCode)
    .writeTaggedFields().buffer

const createCluster = (options = {}) => {
  const instrumentationEmitter = new InstrumentationEventEmitter()
  const events = []
  instrumentationEmitter.addListener(CLUSTER_REBOOTSTRAP, (event) => events.push(event.payload))
  const cluster = new Cluster({
    ...connectionOpts(),
    logger: newLogger(),
    instrumentationEmitter,
    metadataRecoveryStrategy: 'rebootstrap',
    metadataRecoveryRebootstrapTriggerMs: 1000,
    ...options,
  })
  return { cluster, events }
}

/**
 * Replaces broker discovery with a stub whose metadata() behaviour the test controls.
 *
 * `pool.brokers` is emptied deliberately. The arming refresh populates it from the response, and
 * because the response's only broker (127.0.0.1:9092) matches the seed's own address, the
 * seed-relabelling branch (`brokerPool.js:208-214`) puts the *seed object itself* under node 0.
 * A later stubSeed() would then be shadowed: `findConnectedBroker()` finds the cached, still
 * "connected" old seed and returns it, so a refresh meant to fail would succeed instead.
 * Tests that need a cached broker set `pool.brokers` explicitly after calling this.
 */
const stubSeed = (pool, metadata) => {
  let seed = {
    nodeId: 0,
    allowAutoTopicCreation: true,
    isConnected: () => true,
    connect: async () => {},
    disconnect: async () => {},
    metadata,
    connectionPool: { host: '127.0.0.1', port: 9092, rack: null },
  }
  pool.brokers = {}
  pool.seedBroker = seed
  pool.createSeedBroker = async () => {
    seed = { ...seed }
    pool.seedBroker = seed
  }
  pool.buildConnectionPool = async (d) => ({ ...d })
  return () => pool.seedBroker
}

const ok = {
  throttleTime: 0,
  clientSideThrottleTime: 0,
  errorCode: 0,
  brokers: [{ nodeId: 0, host: '127.0.0.1', port: 9092, rack: null }],
  clusterId: 'c',
  controllerId: 0,
  topicMetadata: [],
}

describe('BrokerPool > rebootstrap', () => {
  describe('trigger (a) - continuous failure', () => {
    test('idle for 10 minutes, one metadata failure, then success -> no REBOOTSTRAP emitted', async () => {
      const { cluster, events } = createCluster({ metadataRecoveryRebootstrapTriggerMs: 300000 })
      const pool = cluster.brokerPool
      let calls = 0
      stubSeed(pool, async () => {
        calls += 1
        if (calls === 2) throw Object.assign(new Error('boom'), { retriable: false })
        return ok
      })

      await pool.refreshMetadata([]) // success, clock null
      jest.spyOn(Date, 'now').mockReturnValue(Date.now() + 600000) // idle 10 minutes
      await pool.refreshMetadata([]).catch(() => {}) // one failure: starts the clock, does not fire
      Date.now.mockRestore()
      await pool.refreshMetadata([]) // success: clears the clock

      expect(events).toEqual([])
      expect(pool.metadataUnavailableSince).toBeNull()
    })

    test('failing continuously for triggerMs -> exactly one REBOOTSTRAP', async () => {
      const { cluster, events } = createCluster({ metadataRecoveryRebootstrapTriggerMs: 1000 })
      const pool = cluster.brokerPool
      stubSeed(pool, async () => ok)
      await pool.refreshMetadata([]) // arm

      const start = Date.now()
      stubSeed(pool, async () => {
        throw Object.assign(new Error('down'), { retriable: false })
      })
      await pool.refreshMetadata([]).catch(() => {})
      jest.spyOn(Date, 'now').mockReturnValue(start + 2000)
      await pool.refreshMetadata([]).catch(() => {})
      Date.now.mockRestore()

      expect(events).toEqual([{ reason: 'timeout' }])
    })

    test('still failing one window later -> a second one, not a storm', async () => {
      // The case that proves the two flags have different lifetimes. Trace, triggerMs 1000:
      //   start        since=null -> since=start, no event
      //   start+2000   2000 >= 1000 -> REBOOTSTRAP, and rebootstrap() clears since
      //   start+2500   since=null -> since=start+2500, no event
      //   start+5000   2500 >= 1000 -> a second REBOOTSTRAP
      // Gating trigger (a) on rebootstrapArmed as well would stop after the first event, because
      // only a metadata *success* re-arms that flag and there is none here.
      const { cluster, events } = createCluster({ metadataRecoveryRebootstrapTriggerMs: 1000 })
      const pool = cluster.brokerPool
      stubSeed(pool, async () => ok)
      await pool.refreshMetadata([])
      const start = Date.now()
      stubSeed(pool, async () => {
        throw Object.assign(new Error('down'), { retriable: false })
      })

      const nowSpy = jest.spyOn(Date, 'now')
      for (const at of [start, start + 2000, start + 2500, start + 5000]) {
        nowSpy.mockReturnValue(at)
        await pool.refreshMetadata([]).catch(() => {})
      }
      nowSpy.mockRestore()

      expect(events).toEqual([{ reason: 'timeout' }, { reason: 'timeout' }])
    })

    test('a client that never succeeded still rebootstraps once the window elapses', async () => {
      // Trigger (a) has no arming requirement, so a client whose very first metadata attempts
      // all fail is not stuck: the clock starts at its first failure like any other.
      const { cluster, events } = createCluster({ metadataRecoveryRebootstrapTriggerMs: 1000 })
      const pool = cluster.brokerPool
      const start = Date.now()
      stubSeed(pool, async () => {
        throw Object.assign(new Error('down'), { retriable: false })
      })

      const nowSpy = jest.spyOn(Date, 'now')
      for (const at of [start, start + 2000]) {
        nowSpy.mockReturnValue(at)
        await pool.refreshMetadata([]).catch(() => {})
      }
      nowSpy.mockRestore()

      expect(events).toEqual([{ reason: 'timeout' }])
      expect(pool.rebootstrapArmed).toBe(false)
    })

    test('a findConnectedBroker() throw inside the retry callback starts the clock', async () => {
      const { cluster } = createCluster()
      const pool = cluster.brokerPool
      stubSeed(pool, async () => ok)
      await pool.refreshMetadata([])

      pool.findConnectedBroker = async () => {
        throw Object.assign(new Error('nothing reachable'), { retriable: false })
      }
      await pool.refreshMetadata([]).catch(() => {})

      expect(pool.metadataUnavailableSince).not.toBeNull()
    })

    test('N retrier iterations inside one refreshMetadata() advance the clock once, not N times', async () => {
      const { cluster } = createCluster({
        retry: { retries: 3, initialRetryTime: 1, maxRetryTime: 2 },
      })
      const pool = cluster.brokerPool
      stubSeed(pool, async () => ok)
      await pool.refreshMetadata([])

      stubSeed(pool, async () => {
        throw Object.assign(new Error('LEADER_NOT_AVAILABLE'), {
          type: 'LEADER_NOT_AVAILABLE',
          retriable: true,
        })
      })
      const before = Date.now()
      await pool.refreshMetadata([]).catch(() => {})

      expect(pool.metadataUnavailableSince).toBeGreaterThanOrEqual(before)
      const stamped = pool.metadataUnavailableSince
      await pool.refreshMetadata([]).catch(() => {})
      expect(pool.metadataUnavailableSince).toEqual(stamped)
    })

    test('metadataMaxAge = 0 (the admin shape) behaves identically', async () => {
      const { cluster, events } = createCluster({
        metadataMaxAge: undefined,
        metadataRecoveryRebootstrapTriggerMs: 1000,
      })
      const pool = cluster.brokerPool
      expect(pool.metadataMaxAge).toEqual(0)
      stubSeed(pool, async () => ok)
      await pool.refreshMetadataIfNecessary([])
      await pool.refreshMetadataIfNecessary([])
      expect(events).toEqual([])
      expect(pool.metadataUnavailableSince).toBeNull()
    })

    test('a persistent SASL authentication failure for more than the trigger window never rebootstraps', async () => {
      // A rejected credential means the broker is fine and reachable - only this client is at
      // fault, which no rebootstrap can fix - so it must neither start nor advance trigger (a)'s
      // clock, however long it persists.
      const { cluster, events } = createCluster({ metadataRecoveryRebootstrapTriggerMs: 1000 })
      const pool = cluster.brokerPool
      stubSeed(pool, async () => ok)
      await pool.refreshMetadata([]) // arm

      const start = Date.now()
      stubSeed(pool, async () => {
        throw Object.assign(new Error('bad credentials'), {
          name: 'KafkaJSSASLAuthenticationError',
          retriable: false,
        })
      })

      const nowSpy = jest.spyOn(Date, 'now')
      for (const at of [start, start + 2000, start + 5000]) {
        nowSpy.mockReturnValue(at)
        await pool.refreshMetadata([]).catch(() => {})
      }
      nowSpy.mockRestore()

      expect(events).toEqual([])
      expect(pool.metadataUnavailableSince).toBeNull()
    })

    test('a SASL authentication failure wrapped once (the shape connectionPool.js actually blackout-rejects) still never rebootstraps', async () => {
      // Uses the real error class, wrapped once via `.cause` the way a re-thrown error commonly
      // arrives, so this exercises the `.cause` branch of `isAuthenticationFailure` too - the
      // other case only ever checks the top-level `name`.
      const { cluster, events } = createCluster({ metadataRecoveryRebootstrapTriggerMs: 1000 })
      const pool = cluster.brokerPool
      stubSeed(pool, async () => ok)
      await pool.refreshMetadata([]) // arm

      const start = Date.now()
      stubSeed(pool, async () => {
        const saslError = new KafkaJSSASLAuthenticationError('bad credentials')
        throw new KafkaJSNonRetriableError(saslError, { cause: saslError })
      })

      const nowSpy = jest.spyOn(Date, 'now')
      for (const at of [start, start + 2000]) {
        nowSpy.mockReturnValue(at)
        await pool.refreshMetadata([]).catch(() => {})
      }
      nowSpy.mockRestore()

      expect(events).toEqual([])
      expect(pool.metadataUnavailableSince).toBeNull()
    })
  })

  describe('trigger (b) - REBOOTSTRAP_REQUIRED', () => {
    test('a Metadata response carrying top-level error 129 produces exactly one REBOOTSTRAP { reason: "required" }, and the next attempt succeeds', async () => {
      // Driven over the fake socket so the 129 travels through the real v13 codec and the real
      // retrier, rather than being injected as a JavaScript error. `brokers` is mandatory here:
      // this is the one case that does NOT stub buildConnectionPool, so createSeedBroker() goes
      // through the real connectionPoolBuilder, whose validateBrokers() rejects an undefined
      // broker list before a single frame is written. connectionOpts() supplies no `brokers`.
      const { factory, state } = createFakeSocketFactory()
      const { cluster, events } = createCluster({
        brokers: [plainTextBrokers()[0]],
        socketFactory: factory,
        retry: { retries: 0 },
        // This is the only case in the file that connects a real Connection (over the fake
        // socket): without this, Connection's requestTimeout-enforcement interval
        // (network/requestQueue/index.js) is left running after the test, so the process never
        // exits. Every other case in this file stubs the seed and never opens a real connection.
        enforceRequestTimeout: false,
      })

      // `state.writes` is shared across every socket the factory builds, so the cursor is
      // monotonic; `respondWithFrame` answers through `state.socket`, which is always the newest
      // one - the rebootstrap builds a fresh seed, and the old socket's pending requests are
      // rejected by the teardown rather than answered here. `metadataResponses === 2` at the end
      // is what proves both requests were actually answered on the socket that sent them.
      let pumped = 0
      let metadataResponses = 0
      const pump = () => {
        while (state.writes.length > pumped) {
          const header = readRequestHeader(state.writes[pumped])
          pumped += 1
          if (header.apiKey === apiKeys.ApiVersions) {
            respondWithFrame(state, header.correlationId, 0, apiVersionsBody())
          } else {
            metadataResponses += 1
            respondWithFrame(
              state,
              header.correlationId,
              1,
              metadataBody(metadataResponses === 1 ? 129 : 0)
            )
          }
        }
      }
      const interval = setInterval(pump, 1)

      try {
        await cluster.connect()
        await expect(cluster.refreshMetadata()).rejects.toMatchObject({
          type: 'REBOOTSTRAP_REQUIRED',
        })
        // The assertion the spec names by outcome: the caller's next attempt succeeds.
        await expect(cluster.refreshMetadata()).resolves.toBeUndefined()
      } finally {
        clearInterval(interval)
      }

      expect(events).toEqual([{ reason: 'required' }])
      expect(metadataResponses).toEqual(2)
    })

    test('metadataRecoveryStrategy: "none" leaves 129 as the protocol error it already is', async () => {
      const { cluster, events } = createCluster({ metadataRecoveryStrategy: 'none' })
      const pool = cluster.brokerPool
      stubSeed(pool, async () => {
        throw Object.assign(new Error('rebootstrap'), {
          type: 'REBOOTSTRAP_REQUIRED',
          code: 129,
          name: 'KafkaJSProtocolError',
          retriable: false,
        })
      })

      await expect(pool.refreshMetadata([])).rejects.toMatchObject({ type: 'REBOOTSTRAP_REQUIRED' })
      expect(events).toEqual([])
    })

    test('metadataRecoveryStrategy: "none" disables trigger (a) - the window elapses with no event and no clock', async () => {
      const { cluster, events } = createCluster({
        metadataRecoveryStrategy: 'none',
        metadataRecoveryRebootstrapTriggerMs: 1000,
      })
      const pool = cluster.brokerPool
      stubSeed(pool, async () => ok)
      await pool.refreshMetadata([])

      const start = Date.now()
      stubSeed(pool, async () => {
        throw Object.assign(new Error('down'), { retriable: false })
      })
      const nowSpy = jest.spyOn(Date, 'now')
      for (const at of [start, start + 2000, start + 5000]) {
        nowSpy.mockReturnValue(at)
        await pool.refreshMetadata([]).catch(() => {})
      }
      nowSpy.mockRestore()

      expect(events).toEqual([])
      expect(pool.metadataUnavailableSince).toBeNull()
    })

    test('metadataRecoveryStrategy: "none" disables trigger (c) - the untouched seed fallback still runs', async () => {
      const { cluster, events } = createCluster({ metadataRecoveryStrategy: 'none' })
      const pool = cluster.brokerPool
      const seedOf = stubSeed(pool, async () => ok)
      pool.rebootstrapArmed = true
      pool.brokers = {
        1: {
          isConnected: () => false,
          connect: async () => {
            throw new Error('down')
          },
          disconnect: async () => {},
        },
      }
      pool.findBroker = async () => {
        throw new Error('down')
      }
      pool.connect = async () => {}

      const broker = await pool.findConnectedBroker()

      expect(events).toEqual([])
      expect(broker).toBe(seedOf())
    })
  })

  describe('trigger (c) - all brokers unavailable', () => {
    test('fires and emits when every cached broker fails to connect, and returns the post-rebootstrap seed', async () => {
      const { cluster, events } = createCluster()
      const pool = cluster.brokerPool
      const seedOf = stubSeed(pool, async () => ok)
      pool.rebootstrapArmed = true
      pool.brokers = {
        1: {
          isConnected: () => false,
          connect: async () => {
            throw new Error('down')
          },
          disconnect: async () => {},
        },
      }
      pool.findBroker = async () => {
        throw new Error('down')
      }
      pool.connect = async () => {}

      const broker = await pool.findConnectedBroker()

      expect(events).toEqual([{ reason: 'all-brokers-unavailable' }])
      expect(broker).toBe(seedOf())
    })

    test("does not fire when every cached broker's failure is a SASL authentication failure", async () => {
      // Every cached broker is blacked out (or otherwise rejecting credentials) - the cluster is
      // reachable, only this client's credentials are bad, which no rebootstrap can fix.
      const { cluster, events } = createCluster()
      const pool = cluster.brokerPool
      const seedOf = stubSeed(pool, async () => ok)
      pool.rebootstrapArmed = true
      pool.brokers = {
        1: {
          isConnected: () => false,
          connect: async () => {
            throw Object.assign(new Error('blacked out'), {
              name: 'KafkaJSSASLAuthenticationError',
              retriable: false,
            })
          },
          disconnect: async () => {},
        },
      }
      pool.findBroker = async () => {
        throw Object.assign(new Error('blacked out'), {
          name: 'KafkaJSSASLAuthenticationError',
          retriable: false,
        })
      }
      pool.connect = async () => {}

      const broker = await pool.findConnectedBroker()

      expect(events).toEqual([])
      expect(broker).toBe(seedOf())
    })

    test('an empty this.brokers (straight after a rebootstrap) does not re-fire trigger (c)', async () => {
      const { cluster, events } = createCluster()
      const pool = cluster.brokerPool
      stubSeed(pool, async () => ok)
      pool.rebootstrapArmed = true
      pool.brokers = {}
      pool.connect = async () => {}

      await pool.findConnectedBroker()
      expect(events).toEqual([])
    })

    test('a refreshMetadata that retries N times while the cluster is down emits exactly one REBOOTSTRAP', async () => {
      const { cluster, events } = createCluster({
        retry: { retries: 4, initialRetryTime: 1, maxRetryTime: 2 },
      })
      const pool = cluster.brokerPool
      stubSeed(pool, async () => ok)
      await pool.refreshMetadata([]) // arm

      // stubSeed() empties pool.brokers (see its JSDoc), so the cached broker this case needs is
      // set AFTER it - inverting the order would leave nodeIds empty and the trigger would
      // short-circuit on its non-empty precondition.
      stubSeed(pool, async () => {
        throw Object.assign(new Error('LEADER_NOT_AVAILABLE'), {
          type: 'LEADER_NOT_AVAILABLE',
          retriable: true,
        })
      })
      pool.brokers = {
        1: {
          isConnected: () => false,
          connect: async () => {
            throw new Error('down')
          },
          disconnect: async () => {},
        },
      }
      pool.findBroker = async () => {
        throw new Error('down')
      }
      pool.connect = async () => {}

      await pool.refreshMetadata([]).catch(() => {})
      // Iteration 1 fires it; rebootstrap() then empties this.brokers, so iterations 2-5 see
      // nodeIds === [] and the precondition stops them.
      expect(events.filter((e) => e.reason === 'all-brokers-unavailable')).toHaveLength(1)
    })
  })

  describe('teardown and state', () => {
    test('concurrent refreshMetadata + metadata() produce exactly one rebootstrap', async () => {
      // The reachable concurrency, and the reason rebootstrap needs its own shared promise:
      // Cluster has two separate shared-promise guards (cluster/index.js:114-120) and
      // Cluster#metadata() bypasses both by calling brokerPool.refreshMetadataIfNecessary(topics)
      // directly at :194 - so two flows really can be inside BrokerPool#refreshMetadata at once.
      const { cluster, events } = createCluster({ metadataRecoveryRebootstrapTriggerMs: 1000 })
      const pool = cluster.brokerPool
      stubSeed(pool, async () => ok)
      await pool.refreshMetadata([]) // arm

      const start = Date.now()
      stubSeed(pool, async () => {
        await new Promise((resolve) => setTimeout(resolve, 5))
        throw Object.assign(new Error('down'), { retriable: false })
      })
      pool.metadataUnavailableSince = start - 5000

      let seedsBuilt = 0
      const createSeedBroker = pool.createSeedBroker.bind(pool)
      pool.createSeedBroker = async () => {
        seedsBuilt += 1
        return createSeedBroker()
      }

      await Promise.all([
        cluster.refreshMetadata().catch(() => {}),
        cluster.metadata({ topics: ['t'] }).catch(() => {}),
      ])

      expect(events).toEqual([{ reason: 'timeout' }])
      expect(seedsBuilt).toEqual(1)
    })

    test('a concurrent rebootstrap that nulls this.metadata mid-refresh does not throw a TypeError', async () => {
      // refreshMetadata() must read its own local copy of the decoded response, not `this.metadata`,
      // once a rebootstrap can run concurrently: `rebootstrap()` nulls `this.metadata` synchronously
      // on its first tick, and this refresh is still awaiting `buildConnectionPool()` for the
      // second (non-seed) broker in the response when that happens.
      const { cluster } = createCluster()
      const pool = cluster.brokerPool
      const twoBrokerResponse = {
        ...ok,
        brokers: [
          { nodeId: 0, host: '127.0.0.1', port: 9092, rack: null },
          { nodeId: 1, host: '127.0.0.1', port: 9099, rack: null },
        ],
      }
      stubSeed(pool, async () => twoBrokerResponse)

      let triggered = false
      const stubbedBuildConnectionPool = pool.buildConnectionPool.bind(pool)
      pool.buildConnectionPool = async (destination) => {
        if (destination && !triggered) {
          triggered = true
          // A concurrent caller (e.g. trigger (c) inside a different findConnectedBroker() call)
          // rebootstraps while this refresh is still building the broker map for node 1.
          await pool.rebootstrap('required')
        }
        return stubbedBuildConnectionPool(destination)
      }

      await expect(pool.refreshMetadata([])).resolves.toBeUndefined()
    })

    test('two direct rebootstrap() calls join one in-flight teardown', async () => {
      const { cluster, events } = createCluster()
      const pool = cluster.brokerPool
      stubSeed(pool, async () => ok)
      await Promise.all([pool.rebootstrap('timeout'), pool.rebootstrap('timeout')])
      expect(events).toHaveLength(1)
    })

    test('the SASL blackout survives a rebootstrap', async () => {
      const { cluster } = createCluster()
      const pool = cluster.brokerPool
      stubSeed(pool, async () => ok)
      pool.authenticationBlackout.set('127.0.0.1:9092', { until: Date.now() + 60000 })

      await pool.rebootstrap('timeout')

      expect(pool.authenticationBlackout.get('127.0.0.1:9092')).toBeTruthy()
    })

    test('the versions table survives a rebootstrap, the metadata cache does not', async () => {
      const { cluster } = createCluster()
      const pool = cluster.brokerPool
      stubSeed(pool, async () => ok)
      pool.versions = { 3: { minVersion: 0, maxVersion: 13 } }
      cluster.metadataCache.update({
        ...ok,
        topicMetadata: [
          {
            topicErrorCode: 0,
            topic: 't',
            topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
            isInternal: false,
            topicAuthorizedOperations: 0,
            partitionMetadata: [
              {
                partitionErrorCode: 0,
                partitionId: 0,
                leader: 0,
                leaderEpoch: 7,
                replicas: [],
                isr: [],
                offlineReplicas: [],
              },
            ],
          },
        ],
      })

      await pool.rebootstrap('timeout')

      expect(pool.versions).toEqual({ 3: { minVersion: 0, maxVersion: 13 } })
      expect(pool.brokers).toEqual({})
      expect(pool.metadata).toBeNull()
      expect(cluster.metadataCache.topicId('t')).toBeUndefined()
      expect(cluster.metadataCache.leaderEpoch('t', 0)).toEqual(-1)
    })

    test('a findBroker concurrent with a rebootstrap never receives a broker that is mid-disconnect', async () => {
      const { cluster } = createCluster()
      const pool = cluster.brokerPool
      stubSeed(pool, async () => ok)

      let disconnectStarted = false
      pool.brokers = {
        1: {
          isConnected: () => true,
          connect: async () => {},
          disconnect: async () => {
            disconnectStarted = true
            await new Promise((resolve) => setTimeout(resolve, 5))
          },
        },
      }

      const rebootstrap = pool.rebootstrap('timeout')
      await new Promise(setImmediate)
      expect(disconnectStarted).toBe(true)
      await expect(pool.findBroker({ nodeId: 1 })).rejects.toMatchObject({
        name: 'KafkaJSBrokerNotFound',
      })
      await rebootstrap
    })

    test('a rebootstrap mid-refreshMetadata does not leave the loop calling the disconnected broker, and does not create a duplicate seed Broker', async () => {
      const { cluster } = createCluster({
        retry: { retries: 2, initialRetryTime: 1, maxRetryTime: 2 },
      })
      const pool = cluster.brokerPool
      const seedOf = stubSeed(pool, async () => ok)

      const brokersUsed = []
      let attempt = 0
      const originalFind = pool.findConnectedBroker.bind(pool)
      pool.findConnectedBroker = async () => {
        const broker = await originalFind()
        brokersUsed.push(broker)
        return broker
      }
      pool.seedBroker.metadata = async () => {
        attempt += 1
        if (attempt === 1) {
          await pool.rebootstrap('timeout')
          throw Object.assign(new Error('LEADER_NOT_AVAILABLE'), {
            type: 'LEADER_NOT_AVAILABLE',
            retriable: true,
          })
        }
        return ok
      }

      await pool.refreshMetadata([])

      expect(brokersUsed).toHaveLength(2)
      expect(brokersUsed[1]).toBe(seedOf())
      expect(Object.keys(pool.brokers)).toEqual(['0'])
    })
  })
})

describe('Cluster > a failing best-effort metadata refresh never replaces the original error', () => {
  // Four call sites refresh metadata as a side channel inside a `catch` block, purely to fix
  // staleness before the caller's *own* error is rethrown (or before a surrounding retrier tries
  // again): Cluster#findBroker, Cluster#findGroupCoordinator, and two in the producer's send
  // retry (sendMessages.spec.js covers those two, broker-free, with a stubbed cluster). These two
  // cases cover the cluster-level pair without a broker: a failing refresh must never replace the
  // original error the caller was already handling.
  test('Cluster#findBroker: a failing refresh does not replace the original KafkaJSBrokerNotFound', async () => {
    const { cluster } = createCluster()
    const pool = cluster.brokerPool
    pool.findBroker = jest.fn(() => {
      throw new KafkaJSBrokerNotFound('not found')
    })
    cluster.refreshMetadata = jest.fn(() => Promise.reject(new Error('refresh boom')))

    await expect(cluster.findBroker({ nodeId: '1' })).rejects.toMatchObject({
      name: 'KafkaJSBrokerNotFound',
    })
    expect(cluster.refreshMetadata).toHaveBeenCalled()
  })

  test('Cluster#findGroupCoordinator: a failing best-effort refresh does not replace the original error', async () => {
    // retries: 0 so the retrier's own wrap (KafkaJSNumberOfRetriesExceeded) happens on the first
    // failure, whose `cause` is what the retry callback actually threw - the assertion below is
    // on that `cause`, which is exactly what the (buggy) unguarded `await this.refreshMetadata()`
    // would have replaced with "refresh boom" instead of the original KafkaJSBrokerNotFound.
    const { cluster } = createCluster({ retry: { retries: 0 } })
    cluster.findGroupCoordinatorMetadata = jest.fn(() =>
      Promise.reject(new KafkaJSBrokerNotFound('not found'))
    )
    cluster.refreshMetadata = jest.fn(() => Promise.reject(new Error('refresh boom')))

    await expect(cluster.findGroupCoordinator({ groupId: 'g' })).rejects.toMatchObject({
      cause: expect.objectContaining({ name: 'KafkaJSBrokerNotFound' }),
    })
    expect(cluster.refreshMetadata).toHaveBeenCalled()
  })
})
