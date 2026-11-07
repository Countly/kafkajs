const { Kafka, logLevel } = require('../../index')
const { KafkaJSSASLAuthenticationError } = require('../../src/errors')
const Encoder = require('../../src/protocol/encoder')
const apiKeys = require('../../src/protocol/requests/apiKeys')
const {
  createScriptedCluster,
  consumerProtocolScript,
  groupScript,
  producerScript,
  shareScript,
  PROFILES,
  NEGOTIATION_MS,
} = require('../scriptedCluster')
const Broker = require('../../src/broker')
const ConnectionPool = require('../../src/network/connectionPool')
const { createLogger } = require('../../src/loggers')
const { lanes } = require('../lane')
const { Schedule } = require('../schedule')
const metadataV13 = require('../wire/metadataV13')
const { metadataV13: literal } = require('../wire/expected')
const apiVersionsV4 = require('../wire/apiVersionsV4')
const apiVersionsV5 = require('../wire/apiVersionsV5')
const {
  prepareRecordBatches,
  TOPIC,
  TOPIC_ID,
} = require('../../src/consumer/__tests__/fixtures/fetchHarness')

const NODES = [{ id: 0, host: 'node0', port: 9092 }]
const names = (cluster) => cluster.requests().map(({ api }) => api)

/** Polls until `predicate` holds, and fails naming `what` when it never does. */
const until = async (predicate, what) => {
  for (let turn = 0; turn < 400 && !predicate(); turn++) {
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
  if (!predicate()) {
    throw new Error(`${what} did not happen`)
  }
}

/** Waits for a promise, failing with `what` after `ms` instead of leaving the test to time out. */
const within = (promise, ms, what) => {
  let timer
  const timeout = new Promise((resolve, reject) => {
    timer = setTimeout(() => reject(new Error(`${what} did not happen within ${ms} ms`)), ms)
  })
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer))
}

const METADATA_REQUEST = Buffer.from(
  require('../../src/protocol/requests/metadata/fixtures/v13_request_java.json').data
)

/** Puts the size prefix in front of `rest`, as a request frame has it on the wire. */
const sized = (rest) => {
  const size = Buffer.alloc(4)
  size.writeInt32BE(rest.length)
  return Buffer.concat([size, rest])
}

/** One request frame as the client writes it: size, flexible header, then `body`. */
const frameOf = (apiKey, version, body) =>
  sized(
    Buffer.concat([
      new Encoder()
        .writeInt16(apiKey)
        .writeInt16(version)
        .writeInt32(1)
        .writeString('raw')
        .writeTaggedFields().buffer,
      body,
    ])
  )

/** The body of an OffsetFetch v9 request for one group, naming the scripted topic's partition 0. */
const offsetFetchBody = (groupId) =>
  new Encoder()
    .writeUVarIntArray([
      new Encoder()
        .writeUVarIntString(groupId)
        .writeUVarIntString(null)
        .writeInt32(-1)
        .writeUVarIntArray([
          new Encoder()
            .writeUVarIntString(TOPIC)
            .writeUVarIntArray([0], 'int32')
            .writeTaggedFields(),
        ])
        .writeTaggedFields(),
    ])
    .writeBoolean(false)
    .writeTaggedFields().buffer

/** The body of a ListOffsets v9 request: `topics` is `{ name, partitions: number[] }[]`. */
const listOffsetsBody = (topics) =>
  new Encoder()
    .writeInt32(-1)
    .writeInt8(0)
    .writeUVarIntArray(
      topics.map(({ name, partitions }) =>
        new Encoder()
          .writeUVarIntString(name)
          .writeUVarIntArray(
            partitions.map((partition) =>
              new Encoder()
                .writeInt32(partition)
                .writeInt32(-1)
                .writeInt64('-2')
                .writeTaggedFields()
            )
          )
          .writeTaggedFields()
      )
    )
    .writeTaggedFields().buffer

/** Gives the cluster the turns it needs to answer, or to show that it never will. */
const settle = async () => {
  for (let turn = 0; turn < 5; turn++) {
    await new Promise(setImmediate)
  }
}

/** Dials `cluster` and collects every frame the socket receives. */
const dial = (cluster) => {
  const frames = []
  const socket = cluster.socketFactory({ host: 'node0', port: 9092, onConnect: () => {} })
  socket.on('data', (chunk) => frames.push(chunk))
  socket.on('error', () => {})
  return { socket, frames }
}

/** Writes raw bytes to a new socket of `cluster` and gives its answer time to arrive. */
const exchange = async (cluster, bytes) => {
  const dialled = dial(cluster)
  dialled.socket.write(bytes)
  await settle()
  return dialled
}

/**
 * Where the group id sits in an OffsetFetch v9 response frame, read off the layout and never
 * decoded: size, correlation id, header tag buffer, throttle time, the count of groups, then the
 * id as a compact string.
 */
const GROUP_COUNT_AT = 4 + 4 + 1 + 4
const groupOf = (frame) =>
  frame.subarray(GROUP_COUNT_AT + 2, GROUP_COUNT_AT + 1 + frame[GROUP_COUNT_AT + 1]).toString()

describe('ScriptedCluster', () => {
  const closers = []
  /** A client of `cluster`, disconnected after the test whatever it did. */
  const clientOf = (cluster, overrides) => new Kafka(cluster.kafkaOptions(overrides))
  const track = (client) => {
    closers.push(() => client.disconnect().catch(() => {}))
    return client
  }

  beforeAll(() => prepareRecordBatches())

  afterEach(async () => {
    await Promise.all(closers.splice(0).map((close) => close()))
  })

  describe('bootstrap', () => {
    test('answers the ApiVersions request the client opens with, and the Metadata after it', async () => {
      const cluster = createScriptedCluster({ nodes: NODES })
      const admin = track(clientOf(cluster).admin())

      await admin.connect()
      await expect(admin.listTopics()).resolves.toEqual([])

      const [first, second] = cluster.requests()
      expect(first).toMatchObject({
        node: 0,
        api: 'ApiVersions',
        apiKey: 18,
        version: 4,
        generation: 1,
      })
      expect(first.fields).toEqual({
        clientSoftwareName: expect.any(String),
        clientSoftwareVersion: expect.any(String),
      })
      expect(second).toMatchObject({ api: 'Metadata', apiKey: 3, version: 13 })
      expect(second.correlationId).toBeGreaterThan(first.correlationId)
      cluster.assertClean()
    })

    /** The single fresh Metadata request that answers `listTopics`. */
    const answeringListTopics = (cluster) => ({
      api: 'Metadata',
      match: () => cluster.requests().filter(({ api }) => api === 'Metadata').length === 1,
    })
    const topicNamed = (name) => ({ ...literal.response.topics[0], name })

    test('lists the topics a held Metadata answer is released with', async () => {
      const cluster = createScriptedCluster({ nodes: NODES })
      const admin = track(clientOf(cluster).admin())
      await admin.connect()
      const gate = cluster.schedule.hold(answeringListTopics(cluster))

      const listing = admin.listTopics()
      await within(gate.arrived(), 5000, 'the held Metadata call')
      gate.release({ topics: [topicNamed('released-topic')] })

      await expect(listing).resolves.toEqual(['released-topic'])
    })

    test('answers with the bytes it is released with, as given', async () => {
      const cluster = createScriptedCluster({ nodes: NODES })
      const admin = track(clientOf(cluster).admin())
      await admin.connect()
      cluster.schedule.once(
        answeringListTopics(cluster),
        metadataV13.buildResponse({
          brokers: [{ nodeId: 0, host: 'node0', port: 9092, rack: null }],
          topics: [topicNamed('raw-topic')],
        })
      )

      await expect(admin.listTopics()).resolves.toEqual(['raw-topic'])
    })

    test('destroys the socket with the error a held call is failed with', async () => {
      const cluster = createScriptedCluster({ nodes: NODES })
      const admin = track(clientOf(cluster, { retry: { retries: 0 } }).admin())
      await admin.connect()
      const gate = cluster.schedule.hold(answeringListTopics(cluster))

      const listing = admin.listTopics()
      await within(gate.arrived(), 5000, 'the held Metadata call')
      gate.fail(new Error('connection reset by the script'))

      await expect(listing).rejects.toThrow()
      const [socket] = cluster.sockets()
      expect(socket.destroyed).toBe(true)
    })

    test('answers with the latest answerer of an API, which receives the request fields and the logged record', async () => {
      const cluster = createScriptedCluster({ nodes: NODES })
      const brokers = [{ nodeId: 0, host: 'node0', port: 9092, rack: null }]
      const first = jest.fn(() => ({ brokers, topics: [topicNamed('first')] }))
      const second = jest.fn(() => ({ brokers, topics: [topicNamed('second')] }))
      const admin = track(clientOf(cluster).admin())

      expect(cluster.answer('Metadata', first).answer('Metadata', second)).toBe(cluster)
      await admin.connect()
      await expect(admin.listTopics()).resolves.toEqual(['second'])

      const [fields, record] = second.mock.calls[0]
      const logged = cluster.requests().find(({ api }) => api === 'Metadata')
      expect(first).not.toHaveBeenCalled()
      expect(record).toBe(logged)
      expect(fields).toBe(logged.fields)
    })

    test('returns copies of its request log and its failures, so editing them leaves the cluster alone', async () => {
      const cluster = createScriptedCluster({ nodes: NODES })
      const admin = track(clientOf(cluster).admin())
      await admin.connect()
      const logged = cluster.requests().length

      cluster.requests().length = 0
      cluster.failures().push(new Error('not a failure of the cluster'))

      expect(cluster.requests()).toHaveLength(logged)
      expect(cluster.failures()).toEqual([])
      expect(() => cluster.assertClean()).not.toThrow()
    })

    test('tells nodes apart and counts the generations of a node’s connections', async () => {
      const cluster = createScriptedCluster({
        nodes: [
          { id: 0, host: 'node0', port: 9092 },
          { id: 1, host: 'node1', port: 9092 },
        ],
      })

      const sockets = ['node0', 'node1', 'node0'].map((host) =>
        cluster.socketFactory({ host, port: 9092, onConnect: () => {} })
      )

      expect(sockets.map(({ endpoint }) => endpoint.host)).toEqual(['node0', 'node1', 'node0'])
      expect(cluster.sockets()).toEqual(sockets)
    })

    test('answers on a later turn of the event loop than the write, never within its microtasks', async () => {
      const cluster = createScriptedCluster({ nodes: NODES })
      const received = []
      const socket = await new Promise((resolve) => {
        const dialled = cluster.socketFactory({
          host: 'node0',
          port: 9092,
          onConnect: () => resolve(dialled),
        })
      })
      socket.on('data', (chunk) => received.push(chunk))
      const frame = (header, body) =>
        new Encoder()
          .writeInt32(header.size() + body.size())
          .writeEncoder(header)
          .writeEncoder(body).buffer
      const header = new Encoder()
        .writeInt16(18)
        .writeInt16(4)
        .writeInt32(1)
        .writeString('client')
        .writeTaggedFields()
      const body = new Encoder()
        .writeUVarIntString('name')
        .writeUVarIntString('1.0')
        .writeTaggedFields()

      socket.write(frame(header, body))
      for (let turn = 0; turn < 20; turn++) {
        await Promise.resolve()
      }
      expect(received).toEqual([])
      await new Promise(setImmediate)
      await new Promise(setImmediate)

      expect(received).toHaveLength(1)
    })

    test('refuses dials to a node that is down, as a refused connection, until it is up again', async () => {
      const cluster = createScriptedCluster({ nodes: NODES }).down(0)
      const dial = () =>
        new Promise((resolve) => {
          const events = []
          const socket = cluster.socketFactory({
            host: 'node0',
            port: 9092,
            onConnect: () => events.push('connect'),
          })
          socket.on('error', (error) => events.push(error.code))
          socket.on('close', () => resolve({ events, socket }))
          setTimeout(() => resolve({ events, socket }), 50)
        })

      const refused = await dial()
      cluster.up(0)
      const accepted = await dial()

      expect(refused.events).toEqual(['ECONNREFUSED'])
      expect(refused.socket.destroyed).toBe(true)
      expect(accepted.events).toEqual(['connect'])
      expect(accepted.socket.destroyed).toBe(false)
    })

    test('refuses to be dialled at an endpoint that is none of its nodes', () => {
      const cluster = createScriptedCluster({ nodes: NODES })

      expect(() =>
        cluster.socketFactory({ host: 'elsewhere', port: 1, onConnect: () => {} })
      ).toThrow('ScriptedCluster: no node at elsewhere:1')
    })

    test('disables idle reaping by default and preserves explicit idle limits', () => {
      const cluster = createScriptedCluster({ nodes: NODES })

      expect(cluster.kafkaOptions().connectionsMaxIdle).toBe(0)
      expect(cluster.kafkaOptions({ connectionsMaxIdle: 25 }).connectionsMaxIdle).toBe(25)
    })

    test('hands Kafka the endpoints, its own socket factory and the overrides given', () => {
      const cluster = createScriptedCluster({ nodes: NODES })

      expect(cluster.kafkaOptions({ clientId: 'other' })).toMatchObject({
        clientId: 'other',
        brokers: ['node0:9092'],
        socketFactory: cluster.socketFactory,
      })
    })
  })

  describe('profiles and negotiation', () => {
    test.each(Object.keys(PROFILES))(
      'advertises one version per API in the %s profile',
      (profile) => {
        const cluster = createScriptedCluster({ nodes: NODES, profile })

        const table = cluster.apiVersionsTable()

        expect(table.length).toBe(PROFILES[profile].length)
        expect(table.every(({ minVersion, maxVersion }) => minVersion === maxVersion)).toBe(true)
        expect(new Set(table.map(({ apiKey }) => apiKey)).size).toBe(table.length)
      }
    )

    test('differs between profiles in the pairs the profile names', () => {
      const versions = (profile) =>
        Object.fromEntries(PROFILES[profile].map(([api, version]) => [api, version]))

      expect(versions('default')).toMatchObject({ Metadata: 13, OffsetFetch: 9, OffsetCommit: 9 })
      expect(versions('sasl')).toMatchObject({ SaslHandshake: 1, SaslAuthenticate: 2 })
      expect(versions('consumer')).toMatchObject({
        OffsetFetch: 10,
        OffsetCommit: 10,
        ConsumerGroupHeartbeat: 1,
      })
    })

    test('rejects a profile that does not exist', () => {
      expect(() => createScriptedCluster({ nodes: NODES, profile: 'nope' })).toThrow(
        'ScriptedCluster: no profile named nope'
      )
    })

    test('every request of a classic consumer is at the one version its pair advertises', async () => {
      const cluster = groupScript(createScriptedCluster({ nodes: NODES }))
      const consumer = track(clientOf(cluster).consumer({ groupId: 'g', maxWaitTimeInMs: 20 }))
      const delivered = new Promise((resolve) => {
        consumer.connect().then(async () => {
          await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
          await consumer.run({ eachMessage: async () => resolve() })
        })
      })

      await within(delivered, 5000, 'the first record')

      const advertised = Object.fromEntries(PROFILES.default)
      for (const { api, version } of cluster.requests()) {
        expect([api, version]).toEqual([api, advertised[api]])
      }
    })

    test.each([[[12, 13]], [[13, 12]], [[12]], [[12, 12, 12]], [[-1, -1]], [[1.5, 1.5]], [null]])(
      'rejects an advertise override that does not select one non-negative integer version: %j',
      (range) => {
        expect(() =>
          createScriptedCluster({ nodes: NODES, advertise: { Metadata: range } })
        ).toThrow('ScriptedCluster: advertise Metadata must select one version')
      }
    )

    test('copies an advertise override so later caller edits do not change the profile', () => {
      const range = [12, 12]
      const cluster = createScriptedCluster({ nodes: NODES, advertise: { Metadata: range } })
      range[1] = 13

      expect(cluster.apiVersionsTable().find(({ apiKey }) => apiKey === 3)).toEqual({
        apiKey: 3,
        minVersion: 12,
        maxVersion: 12,
      })
    })

    test('makes the client select the older version an advertise override offers', async () => {
      const cluster = createScriptedCluster({ nodes: NODES, advertise: { Metadata: [12, 12] } })
      const admin = track(clientOf(cluster, { retry: { retries: 0 } }).admin())

      await admin.listTopics().catch(() => {})

      const metadata = cluster.requests().find(({ api }) => api === 'Metadata')
      expect(metadata).toMatchObject({ version: 12, fields: null })
    })

    test('refuses a request for a version it has no wire pair for, by name, and fails the test', async () => {
      const cluster = createScriptedCluster({ nodes: NODES, advertise: { Metadata: [12, 12] } })
      const admin = track(clientOf(cluster, { retry: { retries: 0 } }).admin())

      await expect(admin.listTopics()).rejects.toThrow()

      expect(cluster.failures().map(({ message }) => message)).toEqual([
        'ScriptedCluster: no response builder for Metadata v12',
      ])
      expect(() => cluster.assertClean()).toThrow(
        'ScriptedCluster: no response builder for Metadata v12'
      )
      expect(cluster.sockets().every((socket) => socket.destroyed)).toBe(true)
    })

    test.each(Object.keys(PROFILES))(
      'every pair the %s profile advertises has a wire pair, so no request of it is refused for want of one',
      (profile) => {
        for (const [api, version] of PROFILES[profile]) {
          const file = `../wire/${api[0].toLowerCase()}${api.slice(1)}V${version}`

          expect(() => require.resolve(file)).not.toThrow()
        }
      }
    )
  })

  describe('answers nothing scripted', () => {
    test('points a coordinator lookup at its first node, and offers no API outside its profile', async () => {
      const cluster = createScriptedCluster({ nodes: NODES })
      const admin = track(clientOf(cluster, { retry: { retries: 0 } }).admin())

      await expect(admin.describeGroups(['some-group'])).rejects.toMatchObject({
        cause: { name: 'KafkaJSServerDoesNotSupportApiKey' },
      })

      const lookup = cluster.requests().find(({ api }) => api === 'FindCoordinator')
      expect(lookup.fields).toEqual({ keyType: 0, coordinatorKeys: ['some-group'] })
      expect(cluster.failures()).toEqual([])
    })

    test('lists the scripted topic when the whole cluster is asked for', async () => {
      const cluster = groupScript(createScriptedCluster({ nodes: NODES }))
      const admin = track(clientOf(cluster).admin())

      await expect(admin.listTopics()).resolves.toEqual([TOPIC])
    })

    test('does not answer on a socket that was destroyed after the answer was given', async () => {
      const cluster = createScriptedCluster({ nodes: NODES })
      const admin = track(clientOf(cluster, { retry: { retries: 0 } }).admin())
      await admin.connect()
      const gate = cluster.schedule.hold({
        api: 'Metadata',
        match: () => cluster.requests().filter(({ api }) => api === 'Metadata').length === 1,
      })

      const listing = admin.listTopics()
      await within(gate.arrived(), 5000, 'the held Metadata call')
      gate.release()
      cluster.sockets()[0].destroy(new Error('connection reset'))

      await expect(listing).rejects.toThrow()
      expect(cluster.failures()).toEqual([])
    })

    test('holds an empty Fetch answer for the wait the Fetch asked for', async () => {
      const cluster = groupScript(createScriptedCluster({ nodes: NODES }), { batches: 0 })
      const consumer = track(clientOf(cluster).consumer({ groupId: 'g', maxWaitTimeInMs: 50 }))
      await consumer.connect()
      await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
      await consumer.run({ eachMessage: async () => {} })
      await until(() => names(cluster).includes('Fetch'), 'the first fetch')

      await new Promise((resolve) => setTimeout(resolve, 300))

      // answered at once, hundreds would have been asked for in that time
      expect(names(cluster).filter((api) => api === 'Fetch').length).toBeLessThan(20)
    })
  })

  describe('a classic consumer through the real client', () => {
    test('joins, syncs, resolves its position, fetches a record, commits and leaves', async () => {
      const cluster = groupScript(createScriptedCluster({ nodes: NODES }))
      const consumer = track(
        clientOf(cluster).consumer({ groupId: 'scripted-group', maxWaitTimeInMs: 20 })
      )
      const seen = []
      const delivered = new Promise((resolve) => {
        consumer.connect().then(async () => {
          await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
          await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
              seen.push([topic, partition, message.offset, message.value.toString()])
              resolve()
            },
          })
        })
      })

      await within(delivered, 5000, 'the first record')
      await consumer.disconnect()

      expect(seen).toEqual([[TOPIC, 0, '0', 'value']])
      expect(names(cluster)).toEqual(
        expect.arrayContaining([
          'ApiVersions',
          'Metadata',
          'FindCoordinator',
          'JoinGroup',
          'SyncGroup',
          'OffsetFetch',
          'ListOffsets',
          'Fetch',
          'OffsetCommit',
          'LeaveGroup',
        ])
      )
      const request = (api) => cluster.requests().find((record) => record.api === api).fields
      expect(request('FindCoordinator')).toEqual({
        keyType: 0,
        coordinatorKeys: ['scripted-group'],
      })
      expect(request('JoinGroup')).toMatchObject({
        groupId: 'scripted-group',
        protocolType: 'consumer',
        groupProtocols: [{ name: 'RoundRobinAssigner' }],
      })
      expect(request('SyncGroup')).toMatchObject({
        groupId: 'scripted-group',
        generationId: 1,
        memberId: 'member-1',
        groupAssignment: [{ memberId: 'member-1' }],
      })
      expect(request('OffsetFetch')).toMatchObject({
        groupId: 'scripted-group',
        topics: [{ topic: TOPIC, partitions: [{ partition: 0 }] }],
      })
      expect(request('ListOffsets').topics).toEqual([
        {
          name: TOPIC,
          partitions: [expect.objectContaining({ partitionIndex: 0, timestamp: '-2' })],
        },
      ])
      expect(request('Fetch')).toMatchObject({
        maxWaitMs: 20,
        topics: [
          {
            topicId: TOPIC_ID,
            partitions: [expect.objectContaining({ partition: 0, fetchOffset: '0' })],
          },
        ],
      })
      expect(request('OffsetCommit')).toMatchObject({
        groupId: 'scripted-group',
        groupGenerationId: 1,
        topics: [{ topic: TOPIC, partitions: [{ partition: 0, offset: '1' }] }],
      })
      expect(request('LeaveGroup')).toMatchObject({
        groupId: 'scripted-group',
        members: [{ memberId: 'member-1' }],
      })
      cluster.assertClean()
    })

    test('does not ask for the group until the held coordinator lookup is released', async () => {
      const schedule = new Schedule()
      const cluster = groupScript(createScriptedCluster({ nodes: NODES, schedule }))
      const lookup = schedule.hold({ api: 'FindCoordinator' })
      const consumer = track(clientOf(cluster).consumer({ groupId: 'g', maxWaitTimeInMs: 20 }))
      await consumer.connect()
      await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
      const delivered = new Promise((resolve) => {
        consumer.run({ eachMessage: async () => resolve() })
      })

      await within(lookup.arrived(), 5000, 'the coordinator lookup')
      expect(names(cluster)).not.toContain('JoinGroup')
      lookup.release()

      await within(delivered, 5000, 'the first record')
      expect(names(cluster).indexOf('JoinGroup')).toBeGreaterThan(
        names(cluster).indexOf('FindCoordinator')
      )
    })

    test('holds a request picked by its topic and partition until it is released', async () => {
      const cluster = groupScript(createScriptedCluster({ nodes: NODES }))
      const offsets = cluster.schedule.hold({ api: 'OffsetFetch', topic: TOPIC, partition: 0 })
      const consumer = track(clientOf(cluster).consumer({ groupId: 'g', maxWaitTimeInMs: 20 }))
      await consumer.connect()
      await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
      const delivered = new Promise((resolve) => {
        consumer.run({ eachMessage: async () => resolve() })
      })

      const held = await within(offsets.arrived(), 5000, 'the offset fetch')
      expect(held).toMatchObject({ api: 'OffsetFetch', topic: TOPIC, partition: 0 })
      expect(names(cluster)).not.toContain('ListOffsets')
      offsets.release()

      await within(delivered, 5000, 'the first record')
      expect(names(cluster)).toContain('ListOffsets')
    })

    test('sends the Fetch of a second connection on a new generation of the node', async () => {
      const cluster = groupScript(createScriptedCluster({ nodes: NODES }))
      const consumer = track(clientOf(cluster).consumer({ groupId: 'g', maxWaitTimeInMs: 20 }))
      await consumer.connect()
      await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
      const delivered = new Promise((resolve) => {
        consumer.run({ eachMessage: async () => resolve() })
      })

      await within(delivered, 5000, 'the first record')

      const generationOf = (api) =>
        cluster.requests().find((record) => record.api === api).generation
      expect(generationOf('JoinGroup')).toBe(1)
      expect(generationOf('Fetch')).toBe(2)
      expect(cluster.sockets()).toHaveLength(2)
    })
  })

  describe('request frames', () => {
    const valid = frameOf(apiKeys.Metadata, 13, METADATA_REQUEST)
    const resized = (size) => {
      const copy = Buffer.from(valid)
      copy.writeInt32BE(size)
      return copy
    }

    test('answers a complete request frame', async () => {
      const cluster = createScriptedCluster({ nodes: NODES })

      const { frames } = await exchange(cluster, valid)

      expect(frames).toHaveLength(1)
      cluster.assertClean()
    })

    test.each([
      ['a negative size prefix', resized(-1), /its size prefix says -1 bytes, \d+ follow/],
      [
        'a frame cut short, its size prefix unchanged',
        valid.subarray(0, -1),
        /its size prefix says \d+ bytes, \d+ follow/,
      ],
      [
        'a byte after the frame, its size prefix unchanged',
        Buffer.concat([valid, Buffer.from([0xff])]),
        /its size prefix says \d+ bytes, \d+ follow/,
      ],
      ['fewer bytes than a size prefix', Buffer.from([0, 0, 0]), /cannot read the request frame/],
      ['a header cut short', sized(Buffer.from([0, 3, 0])), /cannot read the request frame/],
      [
        'an API key that no API has',
        frameOf(999, 0, Buffer.alloc(0)),
        /cannot read the request frame: .*API key 999/,
      ],
      [
        'a body cut short, its size prefix agreeing',
        sized(valid.subarray(4, -1)),
        /cannot read the Metadata v13 request/,
      ],
      [
        'a byte after the tag buffer of the body, its size prefix agreeing',
        sized(Buffer.concat([valid.subarray(4), Buffer.from([0xff])])),
        /cannot read the Metadata v13 request: .*after its tag buffer/,
      ],
    ])('refuses %s, answers nothing and fails the test', async (_what, bytes, message) => {
      const cluster = createScriptedCluster({ nodes: NODES })

      const { frames, socket } = await exchange(cluster, bytes)

      expect(frames).toHaveLength(0)
      expect(socket.destroyed).toBe(true)
      expect(cluster.failures()).toHaveLength(1)
      expect(() => cluster.assertClean()).toThrow(message)
    })
  })

  describe('ApiVersions discovery', () => {
    /** The body of an ApiVersions v5 request: software, cluster id, node id, no tags. */
    const v5Body = (clusterId, nodeId) =>
      new Encoder()
        .writeUVarIntString('raw')
        .writeUVarIntString('1')
        .writeUVarIntString(clusterId)
        .writeInt32(nodeId)
        .writeTaggedFields().buffer

    /** The body of an ApiVersions v4 request: software and no tags. */
    const v4Body = new Encoder()
      .writeUVarIntString('raw')
      .writeUVarIntString('1')
      .writeTaggedFields().buffer

    /** What a broker that offers ApiVersions up to v4 answers a v5 request, framed for correlation id 1. */
    const UNSUPPORTED_FRAME =
      '00000010' + // size: the correlation id and the 12 bytes of the body
      '00000001' + // correlation id
      '0023' + // UNSUPPORTED_VERSION
      '00000001' + // one entry, in a classic array
      '0012' +
      '0000' +
      '0004' // ApiVersions, 0 to 4

    test('answers the first v5 request of a profile that offers v4 as a v4 broker does, and logs it in the trace only', async () => {
      const cluster = createScriptedCluster({ nodes: NODES })

      const { frames } = await exchange(cluster, frameOf(apiKeys.ApiVersions, 5, v5Body('c', 2)))

      expect(frames.map((frame) => frame.toString('hex'))).toEqual([UNSUPPORTED_FRAME])
      expect(cluster.apiVersionTrace()).toMatchObject([
        { version: 5, discovery: true, fields: { clusterId: 'c', nodeId: 2 } },
      ])
      expect(cluster.requests()).toEqual([])
      expect(cluster.timeline()).toEqual([])
      cluster.assertClean()
    })

    test('answers a v5 request like any other when the profile offers v5', async () => {
      const cluster = createScriptedCluster({ nodes: NODES, advertise: { ApiVersions: [5, 5] } })

      const { frames } = await exchange(cluster, frameOf(apiKeys.ApiVersions, 5, v5Body(null, -1)))

      // One frame: the size, correlation id 1 and no header tags (the ApiVersions response header
      // is v0), then the profile's table as a v5 answer, with the range it advertises for ApiVersions.
      const body = apiVersionsV5.buildResponse(cluster.apiVersionsAnswer())
      expect(frames.map((frame) => frame.toString('hex'))).toEqual([
        sized(Buffer.concat([Buffer.from('00000001', 'hex'), body])).toString('hex'),
      ])
      expect(body.toString('hex')).toContain('0012' + '0005' + '0005' + '00')
      expect(cluster.requests()).toMatchObject([
        { api: 'ApiVersions', version: 5, fields: { clusterId: null, nodeId: -1 } },
      ])
      expect(cluster.requests()[0]).not.toHaveProperty('discovery')
      expect(cluster.apiVersionTrace()).toEqual(cluster.requests())
      cluster.assertClean()
    })

    test('lists a second negotiation on a connection that has not changed and refuses it as a v4 broker does', async () => {
      const cluster = createScriptedCluster({ nodes: NODES })
      const { socket, frames } = dial(cluster)

      for (const [version, body] of [
        [5, v5Body('c', 2)],
        [4, v4Body],
        [5, v5Body('c', 2)],
      ]) {
        socket.write(frameOf(apiKeys.ApiVersions, version, body))
        await settle()
      }

      expect(
        cluster.apiVersionTrace().map(({ version, discovery }) => [version, discovery === true])
      ).toEqual([
        [5, true],
        [4, false],
        [5, false],
      ])
      expect(cluster.requests().map(({ version }) => version)).toEqual([4, 5])
      expect(frames).toHaveLength(3)
      expect(frames[2].toString('hex')).toBe(UNSUPPORTED_FRAME)
      expect(cluster.timeline().map(({ kind, version }) => [kind, version])).toEqual([
        ['request', 4],
        ['response', 4],
        ['request', 5],
        ['response', 5],
      ])
      cluster.assertClean()
    })

    test('keeps one renegotiation per socket opened to the node out of requests(), and lists the next', async () => {
      const cluster = createScriptedCluster({ nodes: NODES })
      const { socket } = dial(cluster)
      const negotiate = async () => {
        for (const [version, body] of [
          [5, v5Body('c', 2)],
          [4, v4Body],
        ]) {
          socket.write(frameOf(apiKeys.ApiVersions, version, body))
          await settle()
        }
      }

      await negotiate()
      dial(cluster)
      await negotiate()
      await negotiate()

      expect(
        cluster.apiVersionTrace().map(({ version, discovery }) => [version, discovery === true])
      ).toEqual([
        [5, true],
        [4, false],
        [5, true],
        [4, false],
        [5, false],
        [4, false],
      ])
      expect(cluster.requests().map(({ version }) => version)).toEqual([4, 4, 5, 4])
      cluster.assertClean()
    })

    test('lists a v5 request that follows an application request', async () => {
      const cluster = createScriptedCluster({ nodes: NODES })
      const { socket } = dial(cluster)

      socket.write(frameOf(apiKeys.Metadata, 13, METADATA_REQUEST))
      await settle()
      socket.write(frameOf(apiKeys.ApiVersions, 5, v5Body('c', 2)))
      await settle()

      expect(cluster.requests().map(({ api, version }) => [api, version])).toEqual([
        ['Metadata', 13],
        ['ApiVersions', 5],
      ])
      expect(cluster.apiVersionTrace()).toHaveLength(1)
      expect(cluster.apiVersionTrace()[0]).not.toHaveProperty('discovery')
      cluster.assertClean()
    })

    test('a client of a profile that offers v4 asks v5 and then v4, and the schedule sees only the second', async () => {
      const cluster = createScriptedCluster({ nodes: NODES })
      const admin = track(clientOf(cluster).admin())
      const gate = cluster.schedule.hold({ api: 'ApiVersions' })

      const connecting = admin.connect()
      const call = await within(gate.arrived(), 5000, 'the held ApiVersions call')

      expect(call).toMatchObject({ api: 'ApiVersions', version: 4 })
      expect(cluster.apiVersionTrace().map(({ version }) => version)).toEqual([5, 4])
      gate.release()
      await connecting
      expect(cluster.requests().map(({ version }) => version)).toEqual([4])
      cluster.assertClean()
    })

    test('does not hand a discovery request to a scripted answer', async () => {
      const cluster = createScriptedCluster({ nodes: NODES })
      const seen = []
      cluster.answer('ApiVersions', (_fields, { version }) => {
        seen.push(version)
        return cluster.apiVersionsAnswer()
      })
      const admin = track(clientOf(cluster).admin())

      await admin.connect()

      expect(seen).toEqual([4])
      cluster.assertClean()
    })

    test('refuses a discovery request with a byte after its tag buffer, answers nothing and fails the test', async () => {
      const cluster = createScriptedCluster({ nodes: NODES })
      const frame = frameOf(apiKeys.ApiVersions, 5, v5Body('c', 2))

      const { frames, socket } = await exchange(
        cluster,
        sized(Buffer.concat([frame.subarray(4), Buffer.from([0xff])]))
      )

      expect(frames).toHaveLength(0)
      expect(socket.destroyed).toBe(true)
      expect(() => cluster.assertClean()).toThrow(
        /cannot read the ApiVersions v5 request: .*after its tag buffer/
      )
    })
  })

  describe('OffsetForLeaderEpoch v4 request bodies', () => {
    const encodeRequest = require('../../src/protocol/requests/offsetForLeaderEpoch/v4/request')
    const java = Buffer.from(
      require('../../src/protocol/requests/offsetForLeaderEpoch/fixtures/v4_request_java.json').data
    )
    const epochFrame = (body) => frameOf(apiKeys.OffsetForLeaderEpoch, 4, body)
    const partitionsOf = (count) =>
      Array.from({ length: count }, (_, partition) => ({ partition, leaderEpoch: 4 }))

    test('answers the request Java writes', async () => {
      const cluster = createScriptedCluster({ nodes: NODES })

      const { frames } = await exchange(cluster, epochFrame(java))

      expect(frames).toHaveLength(1)
      cluster.assertClean()
    })

    test.each([126, 127])('answers a request for %i partitions', async (count) => {
      const cluster = createScriptedCluster({ nodes: NODES })
      const { buffer } = await encodeRequest({
        topics: [{ topic: TOPIC, partitions: partitionsOf(count) }],
      }).encode()

      const { frames } = await exchange(cluster, epochFrame(buffer))

      expect(frames).toHaveLength(1)
      expect(cluster.requests()[0].fields.topics[0].partitions).toHaveLength(count)
      cluster.assertClean()
    })

    const lastByte = (value) => {
      const bytes = Buffer.from(java)
      bytes[bytes.length - 1] = value
      return bytes
    }

    test.each([
      ['01 for its last byte', lastByte(0x01), 'Tried to read 1 byte(s) with readUVarInt'],
      ['80 for its last byte', lastByte(0x80), 'Tried to read 1 byte(s) with readUVarInt'],
      ['ff for its last byte', lastByte(0xff), 'Tried to read 1 byte(s) with readUVarInt'],
      [
        'a byte after its tag buffer',
        Buffer.concat([java, Buffer.from([0xff])]),
        'OffsetForLeaderEpoch v4 request has a byte after its tag buffer',
      ],
    ])(
      'refuses a request with %s, answers nothing and fails the test',
      async (_what, bytes, why) => {
        const cluster = createScriptedCluster({ nodes: NODES })

        const { frames, socket } = await exchange(cluster, epochFrame(bytes))

        expect(frames).toHaveLength(0)
        expect(socket.destroyed).toBe(true)
        expect(cluster.failures()).toHaveLength(1)
        expect(() => cluster.assertClean()).toThrow(
          `cannot read the OffsetForLeaderEpoch v4 request: ${why}`
        )
      }
    )
  })

  describe('requests outside what the cluster advertises', () => {
    test('answers a request for the version it advertises', async () => {
      const cluster = createScriptedCluster({ nodes: NODES, advertise: { Metadata: [13, 13] } })

      const { frames } = await exchange(cluster, frameOf(apiKeys.Metadata, 13, METADATA_REQUEST))

      expect(frames).toHaveLength(1)
      cluster.assertClean()
    })

    test('refuses a request for a version it has a wire pair for but advertises another', async () => {
      const cluster = createScriptedCluster({ nodes: NODES, advertise: { Metadata: [12, 12] } })

      const { frames, socket } = await exchange(
        cluster,
        frameOf(apiKeys.Metadata, 13, METADATA_REQUEST)
      )

      expect(frames).toHaveLength(0)
      expect(socket.destroyed).toBe(true)
      expect(cluster.requests()).toMatchObject([{ api: 'Metadata', version: 13, fields: null }])
      expect(() => cluster.assertClean()).toThrow(
        'ScriptedCluster: Metadata v13 was requested but the cluster advertises v12'
      )
    })

    test('refuses an OffsetFetch v9 request in the profile that advertises v10', async () => {
      const cluster = createScriptedCluster({ nodes: NODES, profile: 'consumer' })

      const { frames } = await exchange(
        cluster,
        frameOf(apiKeys.OffsetFetch, 9, offsetFetchBody('g'))
      )

      expect(frames).toHaveLength(0)
      expect(() => cluster.assertClean()).toThrow(
        'ScriptedCluster: OffsetFetch v9 was requested but the cluster advertises v10'
      )
    })

    test('refuses a request for an API it does not advertise, naming it', async () => {
      const cluster = createScriptedCluster({ nodes: NODES })

      const { frames } = await exchange(
        cluster,
        frameOf(apiKeys.DescribeGroups, 5, Buffer.alloc(0))
      )

      expect(frames).toHaveLength(0)
      expect(() => cluster.assertClean()).toThrow(
        'ScriptedCluster: DescribeGroups v5 was requested but the cluster advertises no version of it'
      )
    })
  })

  describe('answers that carry the request', () => {
    test.each(['scripted-group', 'other'])(
      'answers an OffsetFetch for the group %s with that group id',
      async (groupId) => {
        const cluster = groupScript(createScriptedCluster({ nodes: NODES }))

        const { frames } = await exchange(
          cluster,
          frameOf(apiKeys.OffsetFetch, 9, offsetFetchBody(groupId))
        )

        expect(frames).toHaveLength(1)
        expect(frames[0][GROUP_COUNT_AT]).toBe(2)
        expect(groupOf(frames[0])).toBe(groupId)
      }
    )
  })

  describe('selectors', () => {
    test.each([
      [
        'picks a request naming one topic and one partition, by both',
        [{ name: 'first', partitions: [0] }],
        { topic: 'first', partition: 0 },
        1,
      ],
      [
        'picks a request naming one topic and one partition, by the topic',
        [{ name: 'first', partitions: [0] }],
        { topic: 'first' },
        1,
      ],
      [
        'picks a request naming one topic and two partitions, by the topic',
        [{ name: 'first', partitions: [0, 1] }],
        { topic: 'first' },
        1,
      ],
      [
        'does not pick a request naming one topic and two partitions, by its first partition',
        [{ name: 'first', partitions: [0, 1] }],
        { topic: 'first', partition: 0 },
        0,
      ],
      [
        'does not pick a request naming two topics, by the first',
        [
          { name: 'first', partitions: [] },
          { name: 'second', partitions: [] },
        ],
        { topic: 'first' },
        0,
      ],
      [
        'does not pick a request naming two topics, by the second',
        [
          { name: 'first', partitions: [] },
          { name: 'second', partitions: [] },
        ],
        { topic: 'second' },
        0,
      ],
    ])('a ListOffsets selector %s', async (_what, topics, selector, picked) => {
      const schedule = new Schedule()
      const cluster = createScriptedCluster({ nodes: NODES, schedule })
      const pick = jest.fn(() => ({ topics: [] }))
      schedule.once({ api: 'ListOffsets', ...selector }, pick)

      const { frames } = await exchange(
        cluster,
        frameOf(apiKeys.ListOffsets, 9, listOffsetsBody(topics))
      )

      expect(frames).toHaveLength(1)
      expect(pick).toHaveBeenCalledTimes(picked)
    })
  })

  describe('destroyed sockets', () => {
    test('sends nothing on a socket destroyed while its answer was held, once the answer is released', async () => {
      const cluster = createScriptedCluster({ nodes: NODES })
      const gate = cluster.schedule.hold({ api: 'Metadata' })
      const { socket, frames } = dial(cluster)

      socket.write(frameOf(apiKeys.Metadata, 13, METADATA_REQUEST))
      await within(gate.arrived(), 1000, 'the held Metadata call')
      socket.destroy()
      gate.release()
      await settle()

      expect(frames).toHaveLength(0)
      cluster.assertClean()
    })
  })

  describe('a producer through the real client', () => {
    const TRANSACTION_APIS = ['AddPartitionsToTxn', 'AddOffsetsToTxn', 'TxnOffsetCommit', 'EndTxn']
    const versionsOf = (cluster, apis) =>
      Object.fromEntries(
        cluster
          .requests()
          .filter(({ api }) => apis.includes(api))
          .map(({ api, version }) => [api, version])
      )

    /** One transaction with a record, the offsets of a group, and a commit. */
    const runTransaction = async (cluster, { commit = true } = {}) => {
      const producer = track(
        clientOf(cluster, {
          retry: { retries: 1, initialRetryTime: 10, maxRetryTime: 20 },
        }).producer({
          transactionalId: 'tx-1',
          maxInFlightRequests: 1,
        })
      )
      await producer.connect()
      const transaction = await producer.transaction()
      await transaction.send({ topic: TOPIC, messages: [{ value: 'a' }, { value: 'b' }] })
      await transaction.sendOffsets({
        consumerGroupId: 'g',
        topics: [{ topic: TOPIC, partitions: [{ partition: 0, offset: '5' }] }],
      })
      if (commit) {
        await transaction.commit()
      }
    }

    test('an idempotent producer sends InitProducerId v5 and Produce v13 in the default profile', async () => {
      const cluster = producerScript(createScriptedCluster({ nodes: NODES }))
      const producer = track(clientOf(cluster).producer())

      await producer.connect()
      await producer.send({ topic: TOPIC, messages: [{ value: 'a' }, { value: 'b' }] })

      const init = cluster.requests().find(({ api }) => api === 'InitProducerId')
      const produced = cluster.requests().find(({ api }) => api === 'Produce')
      expect(init).toMatchObject({
        version: 5,
        fields: { transactionalId: null, producerId: '-1' },
      })
      expect(produced).toMatchObject({ version: 13 })
      expect(produced.fields).toMatchObject({
        transactionalId: null,
        acks: -1,
        topics: [
          {
            topicId: TOPIC_ID,
            partitions: [
              {
                partition: 0,
                batches: [
                  {
                    producerId: '1000',
                    producerEpoch: 0,
                    baseSequence: 0,
                    recordCount: 2,
                    transactional: false,
                  },
                ],
              },
            ],
          },
        ],
      })
      cluster.assertClean()
    })

    test('a transaction under tv1 sends InitProducerId v5, AddPartitionsToTxn v3, AddOffsetsToTxn v4, Produce v11, TxnOffsetCommit v4 and EndTxn v4', async () => {
      const cluster = producerScript(createScriptedCluster({ nodes: NODES, profile: 'tv1' }))

      await runTransaction(cluster)

      expect(versionsOf(cluster, ['InitProducerId', 'Produce', ...TRANSACTION_APIS])).toEqual({
        InitProducerId: 5,
        AddPartitionsToTxn: 3,
        AddOffsetsToTxn: 4,
        Produce: 11,
        TxnOffsetCommit: 4,
        EndTxn: 4,
      })
      cluster.assertClean()
    })

    test('a transaction under tv2 sends InitProducerId v5, Produce v12, TxnOffsetCommit v5 and EndTxn v5 and neither AddPartitionsToTxn nor AddOffsetsToTxn', async () => {
      const cluster = producerScript(createScriptedCluster({ nodes: NODES, profile: 'tv2' }))

      await runTransaction(cluster)

      expect(versionsOf(cluster, ['InitProducerId', 'Produce', ...TRANSACTION_APIS])).toEqual({
        InitProducerId: 5,
        Produce: 12,
        TxnOffsetCommit: 5,
        EndTxn: 5,
      })
      expect(names(cluster)).not.toContain('AddPartitionsToTxn')
      expect(names(cluster)).not.toContain('AddOffsetsToTxn')
      cluster.assertClean()
    })

    test.each(['tv1', 'tv2'])(
      'every request is at the one version its %s pair advertises',
      async (profile) => {
        const cluster = producerScript(createScriptedCluster({ nodes: NODES, profile }))

        await runTransaction(cluster)

        const advertised = Object.fromEntries(PROFILES[profile])
        for (const { api, version } of cluster.requests()) {
          expect([api, version]).toEqual([api, advertised[api]])
        }
        cluster.assertClean()
      }
    )

    test.each(['tv1', 'tv2'])(
      'the transaction requests of %s carry the producer identity and the transaction fields the client chose',
      async (profile) => {
        const cluster = producerScript(createScriptedCluster({ nodes: NODES, profile }))

        await runTransaction(cluster)

        const identity = { transactionalId: 'tx-1', producerId: '1000', producerEpoch: 0 }
        const of = (api) => cluster.requests().find((request) => request.api === api).fields
        expect(of('Produce')).toMatchObject({
          transactionalId: 'tx-1',
          topics: [
            {
              topic: TOPIC,
              partitions: [
                {
                  partition: 0,
                  batches: [
                    {
                      producerId: '1000',
                      producerEpoch: 0,
                      baseSequence: 0,
                      recordCount: 2,
                      transactional: true,
                    },
                  ],
                },
              ],
            },
          ],
        })
        expect(of('TxnOffsetCommit')).toMatchObject({
          ...identity,
          groupId: 'g',
          topics: [{ topic: TOPIC, partitions: [{ partition: 0, offset: '5' }] }],
        })
        expect(of('EndTxn')).toMatchObject({ ...identity, transactionResult: true })
        if (profile === 'tv1') {
          expect(of('AddPartitionsToTxn')).toMatchObject({
            ...identity,
            topics: [{ topic: TOPIC, partitions: [0] }],
          })
          expect(of('AddOffsetsToTxn')).toMatchObject({ ...identity, groupId: 'g' })
        }
        cluster.assertClean()
      }
    )

    test.each([
      ['tv1', 'Produce', { Produce: [13, 13] }],
      ['tv1', 'EndTxn', { EndTxn: [5, 5] }],
      ['tv1', 'TxnOffsetCommit', { TxnOffsetCommit: [5, 5] }],
      ['tv2', 'Produce', { Produce: [13, 13] }],
    ])(
      'under %s the client keeps its own %s cap: nothing is sent to a broker that offers only %j',
      async (profile, api, advertise) => {
        const cluster = producerScript(createScriptedCluster({ nodes: NODES, profile, advertise }))

        await expect(runTransaction(cluster)).rejects.toThrow(
          `does not support any version of ${api}`
        )

        expect(names(cluster)).not.toContain(api)
        expect(cluster.failures()).toEqual([])
      }
    )

    test('tv2 finalizes transaction.version at level 2 in its ApiVersions answer, and tv1 does not', () => {
      const answerOf = (profile) =>
        apiVersionsV4.buildResponse(
          createScriptedCluster({ nodes: NODES, profile }).apiVersionsAnswer()
        )
      const level2 = Buffer.from('transaction.version').toString('hex') + '0002' + '0002'

      expect(answerOf('tv2').toString('hex')).toContain(level2)
      expect(answerOf('tv1').toString('hex')).not.toContain(
        Buffer.from('transaction.version').toString('hex')
      )
      expect(answerOf('default').toString('hex')).not.toContain(
        Buffer.from('transaction.version').toString('hex')
      )
    })

    test('the transaction profiles advertise exactly the pairs the client’s caps allow under each transaction version', () => {
      const classic = PROFILES.default.filter(
        ([api]) => api !== 'InitProducerId' && api !== 'Produce'
      )
      const transactional = (profile) =>
        PROFILES[profile].filter(([api]) => !classic.some(([name]) => name === api))

      expect(transactional('tv1')).toEqual([
        ['InitProducerId', 5],
        ['AddPartitionsToTxn', 3],
        ['AddOffsetsToTxn', 4],
        ['Produce', 11],
        ['TxnOffsetCommit', 4],
        ['EndTxn', 4],
      ])
      expect(transactional('tv2')).toEqual([
        ['InitProducerId', 5],
        ['Produce', 12],
        ['TxnOffsetCommit', 5],
        ['EndTxn', 5],
      ])
      expect(PROFILES.default).toEqual(
        expect.arrayContaining([
          ['InitProducerId', 5],
          ['Produce', 13],
        ])
      )
      for (const profile of ['tv1', 'tv2']) {
        expect(PROFILES[profile].filter(([api]) => classic.some(([name]) => name === api))).toEqual(
          classic
        )
      }
    })

    describe('what producerScript answers, from the request that asks', () => {
      const answerOf = (api, fields) =>
        producerScript(createScriptedCluster({ nodes: NODES })).answerers.get(api)(fields)

      test('answers a Produce for the topic names and partitions it carries, with no leader or endpoint', () => {
        expect(
          answerOf('Produce', {
            topics: [{ topic: 'a', partitions: [{ partition: 3 }, { partition: 4 }] }],
          })
        ).toEqual({
          responses: [
            {
              topic: 'a',
              partitions: [3, 4].map((partition) => ({
                partition,
                errorCode: 0,
                baseOffset: '0',
                logAppendTime: '-1',
                logStartOffset: '0',
                leader: null,
              })),
            },
          ],
          nodeEndpoints: [],
        })
      })

      test('answers a Produce keyed by topic id with the id, not a name', () => {
        const { responses } = answerOf('Produce', {
          topics: [{ topicId: 'id-1', partitions: [{ partition: 0 }] }],
        })

        expect(responses).toEqual([
          { topicId: 'id-1', partitions: [expect.objectContaining({ partition: 0 })] },
        ])
      })

      test('answers AddPartitionsToTxn and TxnOffsetCommit for the partitions they carry', () => {
        expect(
          answerOf('AddPartitionsToTxn', { topics: [{ topic: 'a', partitions: [2, 5] }] })
        ).toEqual({
          errors: [
            {
              topic: 'a',
              partitionErrors: [
                { partition: 2, errorCode: 0 },
                { partition: 5, errorCode: 0 },
              ],
            },
          ],
        })
        expect(
          answerOf('TxnOffsetCommit', { topics: [{ topic: 'a', partitions: [{ partition: 7 }] }] })
        ).toEqual({ topics: [{ topic: 'a', partitions: [{ partition: 7, errorCode: 0 }] }] })
      })
    })

    test('marks the order of an event that is not a request among the requests and answers', async () => {
      const cluster = createScriptedCluster({ nodes: NODES })
      const admin = track(clientOf(cluster).admin())
      await admin.connect()

      cluster.mark('between')
      await admin.listTopics()

      const kinds = cluster.timeline().map(({ kind, api, label }) => [kind, api ?? label])
      expect(kinds.slice(0, 3)).toEqual([
        ['request', 'ApiVersions'],
        ['response', 'ApiVersions'],
        ['mark', 'between'],
      ])
      expect(cluster.timeline().map(({ order }) => order)).toEqual(
        [...cluster.timeline().map(({ order }) => order)].sort((a, b) => a - b)
      )
      expect(cluster.requests().every(({ order, answeredOrder }) => answeredOrder > order)).toBe(
        true
      )
      cluster.assertClean()
    })

    test('lists a request that is held as a request with no response yet, and its response once released', async () => {
      const cluster = createScriptedCluster({ nodes: NODES })
      const gate = cluster.schedule.hold({ api: 'ApiVersions' })
      const admin = track(clientOf(cluster).admin())
      const connecting = admin.connect()
      await within(gate.arrived(), 5000, 'the ApiVersions request')

      expect(cluster.timeline().map(({ kind, api }) => [kind, api])).toEqual([
        ['request', 'ApiVersions'],
      ])
      expect(cluster.requests()[0].answeredOrder).toBeUndefined()

      gate.release()
      await connecting

      expect(
        cluster
          .timeline()
          .map(({ kind, api }) => [kind, api])
          .slice(0, 2)
      ).toEqual([
        ['request', 'ApiVersions'],
        ['response', 'ApiVersions'],
      ])
      cluster.assertClean()
    })
  })

  describe('a consumer of the consumer group protocol through the real client', () => {
    const open = async (options = {}) => {
      const cluster = consumerProtocolScript(
        createScriptedCluster({ nodes: NODES, profile: 'consumer' }),
        options
      )
      const consumer = track(
        clientOf(cluster).consumer({
          groupId: 'scripted-group',
          groupProtocol: 'consumer',
          maxWaitTimeInMs: 20,
        })
      )
      const seen = []
      const delivered = new Promise((resolve) => {
        consumer.connect().then(async () => {
          await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
          await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
              seen.push([topic, partition, message.offset, message.value.toString()])
              resolve()
            },
          })
        })
      })
      await within(delivered, 5000, 'the first record')
      await consumer.commitOffsets([{ topic: TOPIC, partition: 0, offset: '1' }])
      await consumer.disconnect()

      return { cluster, seen }
    }
    const requestsOf = (cluster, api) => cluster.requests().filter((record) => record.api === api)

    test('joins, is assigned, resolves its position by topic id, fetches a record, commits and leaves', async () => {
      const { cluster, seen } = await open()

      expect(seen).toEqual([[TOPIC, 0, '0', 'value']])
      const [join, ...heartbeats] = requestsOf(cluster, 'ConsumerGroupHeartbeat')
      expect(join.fields).toMatchObject({
        groupId: 'scripted-group',
        memberEpoch: 0,
        subscribedTopicNames: [TOPIC],
        serverAssignor: null,
      })
      expect(join.fields.memberId).toEqual(expect.any(String))
      expect(heartbeats.length).toBeGreaterThanOrEqual(1)
      expect(heartbeats.at(-1).fields).toMatchObject({
        memberId: join.fields.memberId,
        memberEpoch: expect.any(Number),
      })
      expect(heartbeats.at(-1).fields.memberEpoch).toBeLessThan(0)
      expect(requestsOf(cluster, 'OffsetFetch')[0].fields).toMatchObject({
        groupId: 'scripted-group',
        memberEpoch: 1,
        topics: [{ topicId: TOPIC_ID, partitions: [{ partition: 0 }] }],
      })
      expect(requestsOf(cluster, 'OffsetCommit')[0].fields).toMatchObject({
        groupId: 'scripted-group',
        groupGenerationId: 1,
        memberId: join.fields.memberId,
        topics: [{ topicId: TOPIC_ID, partitions: [{ partition: 0, offset: '1' }] }],
      })
      cluster.assertClean()
    })

    test('every request is at the one version its consumer pair advertises', async () => {
      const { cluster } = await open()

      const advertised = Object.fromEntries(PROFILES.consumer)
      for (const { api, version } of cluster.requests()) {
        expect([api, version]).toEqual([api, advertised[api]])
      }
      expect(names(cluster)).not.toContain('JoinGroup')
      cluster.assertClean()
    })

    test('resumes from the offset the coordinator holds for the group it asked about', async () => {
      const cluster = consumerProtocolScript(
        createScriptedCluster({ nodes: NODES, profile: 'consumer' }),
        { committed: { 0: '1' } }
      )
      const consumer = track(
        clientOf(cluster).consumer({
          groupId: 'scripted-group',
          groupProtocol: 'consumer',
          maxWaitTimeInMs: 20,
        })
      )
      await consumer.connect()
      await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
      await consumer.run({ eachMessage: async () => {} })

      await until(() => requestsOf(cluster, 'Fetch').length > 0, 'the first Fetch')

      expect(requestsOf(cluster, 'OffsetFetch')[0].fields.groupId).toBe('scripted-group')
      expect(names(cluster)).not.toContain('ListOffsets')
      expect(requestsOf(cluster, 'Fetch')[0].fields.topics[0].partitions[0].fetchOffset).toBe('1')
      cluster.assertClean()
    })
  })

  describe('what consumerProtocolScript answers, from the request that asks', () => {
    const answerOf = (cluster, api, fields) => cluster.answerers.get(api)(fields)
    const scripted = (options) =>
      consumerProtocolScript(createScriptedCluster({ nodes: NODES, profile: 'consumer' }), options)

    test('answers a join with epoch 1, the interval it was given and every partition of the topic', () => {
      const cluster = scripted({ partitions: 2, heartbeatIntervalMs: 250 })

      expect(
        answerOf(cluster, 'ConsumerGroupHeartbeat', { memberId: 'm', memberEpoch: 0 })
      ).toEqual({
        memberId: 'm',
        memberEpoch: 1,
        heartbeatIntervalMs: 250,
        assignment: { topicPartitions: [{ topicId: TOPIC_ID, partitions: [0, 1] }] },
      })
    })

    test.each([
      ['a steady heartbeat', 4],
      ['a leave', -1],
      ['a static leave', -2],
    ])('answers %s with the epoch it carried and no assignment', (_what, epoch) => {
      const cluster = scripted()

      expect(
        answerOf(cluster, 'ConsumerGroupHeartbeat', { memberId: 'm', memberEpoch: epoch })
      ).toMatchObject({ memberId: 'm', memberEpoch: epoch, assignment: null })
    })

    test('answers an offset fetch for the group it names and the topic ids it names', () => {
      const cluster = scripted({ committed: { 0: '7' } })

      expect(
        answerOf(cluster, 'OffsetFetch', {
          groupId: 'other-group',
          topics: [{ topicId: 'id-1', partitions: [{ partition: 0 }, { partition: 3 }] }],
        })
      ).toEqual({
        groupId: 'other-group',
        topics: [
          {
            topicId: 'id-1',
            partitions: [
              { partition: 0, offset: '7', leaderEpoch: -1, metadata: null, errorCode: 0 },
              { partition: 3, offset: '-1', leaderEpoch: -1, metadata: null, errorCode: 0 },
            ],
          },
        ],
      })
    })

    test('answers an offset fetch of every topic with the partitions of its own topic', () => {
      const cluster = scripted({ partitions: 2 })

      const { topics } = answerOf(cluster, 'OffsetFetch', { groupId: 'g', topics: null })

      expect(topics).toEqual([
        {
          topicId: TOPIC_ID,
          partitions: [
            expect.objectContaining({ partition: 0 }),
            expect.objectContaining({ partition: 1 }),
          ],
        },
      ])
    })

    test('answers an offset commit for the topic ids and partitions it carries, without error', () => {
      const cluster = scripted()

      expect(
        answerOf(cluster, 'OffsetCommit', {
          topics: [{ topicId: 'id-2', partitions: [{ partition: 5 }] }],
        })
      ).toEqual({ topics: [{ topicId: 'id-2', partitions: [{ partition: 5, errorCode: 0 }] }] })
    })
  })

  describe('SASL PLAIN through the real client', () => {
    const sasl = { mechanism: 'plain', username: 'u', password: 'p' }

    test('negotiates the mechanism and sends the payload RFC 4616 gives for the credentials', async () => {
      const cluster = createScriptedCluster({ nodes: NODES, profile: 'sasl' })
      const admin = track(clientOf(cluster, { sasl }).admin())

      await admin.connect()

      const [versions, handshake, authenticate] = cluster.requests()
      expect([versions.api, handshake.api, authenticate.api]).toEqual([
        'ApiVersions',
        'SaslHandshake',
        'SaslAuthenticate',
      ])
      expect(handshake).toMatchObject({ version: 1, fields: { mechanism: 'PLAIN' } })
      expect(authenticate).toMatchObject({ version: 2 })
      expect(authenticate.fields.authBytes).toEqual(Buffer.from('\u0000u\u0000p'))
      cluster.assertClean()
    })

    test('raises an authentication error, and tells the pool to hold off, when the broker refuses', async () => {
      const cluster = createScriptedCluster({ nodes: NODES, profile: 'sasl' })
      cluster.answer('SaslAuthenticate', () => ({
        errorCode: 58,
        errorMessage: 'Authentication failed: bad credentials',
        authBytes: null,
      }))
      const admin = track(clientOf(cluster, { sasl, retry: { retries: 0 } }).admin())

      await expect(admin.connect()).rejects.toThrow(KafkaJSSASLAuthenticationError)
      await expect(admin.connect()).rejects.toThrow('recently failed')

      expect(requestsOf(cluster, 'SaslAuthenticate')).toHaveLength(1)
      cluster.assertClean()
    })

    const requestsOf = (cluster, api) => cluster.requests().filter((record) => record.api === api)
  })

  describe('the admin APIs the seam carries beyond the bootstrap', () => {
    const advertise = { DescribeTopicPartitions: [0, 0], ListPartitionReassignments: [0, 0] }

    test('pages a DescribeTopicPartitions read, each page asking where the last stopped', async () => {
      const cluster = groupScript(createScriptedCluster({ nodes: NODES, advertise }))
      const topic = (name, partitionIndexes) => ({
        errorCode: 0,
        name,
        topicId: TOPIC_ID,
        isInternal: false,
        partitions: partitionIndexes.map((partitionIndex) => ({
          errorCode: 0,
          partitionIndex,
          leaderId: 0,
          leaderEpoch: 5,
          replicaNodes: [0],
          isrNodes: [0],
          eligibleLeaderReplicas: null,
          lastKnownElr: null,
          offlineReplicas: [],
        })),
        topicAuthorizedOperations: -2147483648,
      })
      cluster.answer('DescribeTopicPartitions', ({ topics, cursor }) => ({
        throttleTime: 0,
        topics: [topic(topics[0], cursor === null ? [0] : [1])],
        nextCursor: cursor === null ? { topicName: topics[0], partitionIndex: 1 } : null,
      }))
      const admin = track(clientOf(cluster).admin())

      const metadata = await admin.fetchTopicMetadata({
        topics: [TOPIC],
        useDescribeTopicPartitions: true,
      })

      const reads = requestsOf(cluster, 'DescribeTopicPartitions')
      expect(reads.map(({ fields }) => fields.cursor)).toEqual([
        null,
        { topicName: TOPIC, partitionIndex: 1 },
      ])
      expect(reads[0].fields.topics).toEqual([TOPIC])
      expect(metadata.topics[0].partitions.map(({ partitionId }) => partitionId)).toEqual([0, 1])
      cluster.assertClean()
    })

    test('asks for every reassignment in flight and reads the answer the broker gives while none is', async () => {
      const cluster = createScriptedCluster({ nodes: NODES, advertise })
      const admin = track(clientOf(cluster).admin())

      const reassignments = await admin.listPartitionReassignments({ topics: null })

      expect(requestsOf(cluster, 'ListPartitionReassignments')[0]).toMatchObject({
        version: 0,
        fields: { timeoutMs: 5000, topics: null },
      })
      expect(reassignments.topics).toEqual([])
      cluster.assertClean()
    })

    test('reads a reassignment in flight as the golden of its bytes holds it', async () => {
      const cluster = createScriptedCluster({ nodes: NODES, advertise })
      cluster.answer('ListPartitionReassignments', () => golden.inFlight.fields)
      const admin = track(clientOf(cluster).admin())

      const reassignments = await admin.listPartitionReassignments({ topics: null })

      expect(reassignments.topics).toEqual([
        {
          name: 't',
          partitions: [
            { partition: 0, replicas: [1, 2], addingReplicas: [3], removingReplicas: [] },
          ],
        },
      ])
      cluster.assertClean()
    })

    const requestsOf = (cluster, api) => cluster.requests().filter((record) => record.api === api)
    const golden = require('../wire/expected').listPartitionReassignmentsV0
  })
})

/**
 * A producer and a consumer, fault-free, on the seam and on a real broker. The scenario holds
 * whether or not anything fails nowhere, so it runs on both lanes from this one file: the seam
 * agreeing with the broker is the drift check. Allowed replay: the durable log none; committed
 * offsets move back only to the last commit; handler delivery at least once after it. A lane
 * sends the fixed key and value of the Fetch fixture and asserts the received payload and offset
 * against the send's acknowledgement. The fake replays that fixed fixture and keeps no broker log.
 */
describe.each(lanes())('A producer and a consumer round trip (%s lane)', (lane) => {
  const closers = []

  /** The client, topic and `done` of the lane. */
  const scenario = async () => {
    if (lane === 'fake') {
      await prepareRecordBatches()
      const cluster = groupScript(producerScript(createScriptedCluster({ nodes: NODES })), {
        batches: 1,
      })
      return {
        kafka: new Kafka(cluster.kafkaOptions()),
        topic: TOPIC,
        done: () => cluster.assertClean(),
      }
    }

    const { plainTextBrokers, createTopic, secureRandom } = require('testHelpers')
    const topic = `seam-round-trip-${secureRandom()}`
    await createTopic({ topic })
    return {
      kafka: new Kafka({
        clientId: 'seam-round-trip',
        brokers: plainTextBrokers(),
        logLevel: logLevel.NOTHING,
      }),
      topic,
      done: () => {},
    }
  }

  afterEach(async () => {
    await Promise.all(closers.splice(0).map((close) => close()))
  })

  test('a send is acknowledged and a consumer from the beginning reads a record and commits', async () => {
    const { kafka, topic, done } = await scenario()
    const producer = kafka.producer()
    closers.push(() => producer.disconnect().catch(() => {}))
    await producer.connect()

    const sent = await producer.send({ topic, messages: [{ key: 'key', value: 'value' }] })

    expect(sent).toEqual([
      expect.objectContaining({
        topicName: topic,
        partition: 0,
        errorCode: 0,
        baseOffset: expect.any(String),
      }),
    ])

    const consumer = kafka.consumer({
      groupId: `seam-round-trip-${Date.now()}`,
      maxWaitTimeInMs: 50,
    })
    closers.push(() => consumer.disconnect().catch(() => {}))
    const received = new Promise((resolve) => {
      consumer.connect().then(async () => {
        await consumer.subscribe({ topic, fromBeginning: true })
        await consumer.run({
          eachMessage: async ({ topic: from, partition, message }) =>
            resolve({
              from,
              partition,
              offset: message.offset,
              key: message.key,
              value: message.value,
            }),
        })
      })
    })

    const record = await within(received, 15000, 'a record')

    expect(record).toEqual({
      from: topic,
      partition: 0,
      offset: sent[0].baseOffset,
      key: Buffer.from('key'),
      value: Buffer.from('value'),
    })
    await expect(
      consumer.commitOffsets([{ topic, partition: 0, offset: String(Number(record.offset) + 1) }])
    ).resolves.toBeUndefined()
    done()
  })
})

describe('ScriptedCluster Fetch session deltas', () => {
  const body = Buffer.from(
    '000013880000000100a0000001000000250000000201020102030405060708090a0b0c0d0e0f1003000000010000000300027200',
    'hex'
  )

  test('records a forgotten UUID and replies with an empty session map', async () => {
    const cluster = createScriptedCluster({ nodes: NODES })
    cluster.answer('Fetch', () => ({
      includeTopic: false,
      sessionId: 37,
      nodeEndpoints: undefined,
    }))
    const { socket, frames } = await exchange(cluster, frameOf(apiKeys.Fetch, 18, body))
    try {
      cluster.assertClean()
      expect(cluster.requests()).toHaveLength(1)
      expect(cluster.requests()[0].fields.forgottenTopics).toEqual([
        { topic: null, topicId: '01020304-0506-0708-090a-0b0c0d0e0f10', partitions: [1, 3] },
      ])
      expect(frames).toHaveLength(1)
      expect(frames[0].subarray(9)).toEqual(Buffer.from('000000000000000000250100', 'hex'))
    } finally {
      socket.destroy()
      cluster.assertClean()
    }
  })

  test('rejects a zero byte after the Fetch terminal tag buffer', async () => {
    const cluster = createScriptedCluster({ nodes: NODES })
    const { socket, frames } = await exchange(
      cluster,
      frameOf(apiKeys.Fetch, 18, Buffer.concat([body, Buffer.from([0])]))
    )
    try {
      expect(frames).toEqual([])
      expect(cluster.failures()).toHaveLength(1)
      expect(() => cluster.assertClean()).toThrow('after its tag buffer')
    } finally {
      socket.destroy()
    }
  })
})

test('a blackholed dial never connects and up only restores future dials', async () => {
  jest.useFakeTimers()
  const cluster = createScriptedCluster({ nodes: NODES })
  const firstConnect = jest.fn()
  const nextConnect = jest.fn()
  const old = cluster.blackhole(0).socketFactory({ ...NODES[0], onConnect: firstConnect })
  try {
    await jest.advanceTimersByTimeAsync(10)
    expect(firstConnect).not.toHaveBeenCalled()
    cluster.up(0)
    const next = cluster.socketFactory({ ...NODES[0], onConnect: nextConnect })
    await jest.advanceTimersByTimeAsync(10)
    expect(firstConnect).not.toHaveBeenCalled()
    expect(nextConnect).toHaveBeenCalledTimes(1)
    expect(() => cluster.pairFor('Produce', 0)).toThrow('advertises')
    next.destroy()
    cluster.assertClean()
  } finally {
    old.destroy()
    jest.useRealTimers()
  }
})

describe('held dials and outages', () => {
  const TWO_NODES = [...NODES, { id: 1, host: 'node1', port: 9092 }]
  const sockets = []
  /** Dials a node of `cluster`, keeps the socket for cleanup and collects what it receives. */
  const dialNode = (cluster, node, onConnect = () => {}) => {
    const socket = cluster.socketFactory({ ...node, onConnect })
    const frames = []
    socket.on('data', (chunk) => frames.push(chunk))
    socket.on('error', () => {})
    sockets.push(socket)
    return { socket, frames }
  }
  /** The top-level error code of a Metadata v13 response frame, which ends with it and a tag buffer. */
  const topLevelCode = (frame) => frame.readInt16BE(frame.length - 3)
  const metadataRequest = () => frameOf(apiKeys.Metadata, 13, METADATA_REQUEST)

  afterEach(() => {
    for (const socket of sockets.splice(0)) socket.destroy()
  })

  test('release connects a blackholed dial once, and the socket then answers requests', async () => {
    const cluster = createScriptedCluster({ nodes: NODES })
    const connected = jest.fn()
    const { socket, frames } = dialNode(cluster.blackhole(0), NODES[0], connected)
    await settle()
    expect(connected).not.toHaveBeenCalled()
    expect(cluster.heldDialsTo(0)).toBe(1)

    expect(cluster.release(0)).toBe(cluster)
    await settle()
    expect(connected).toHaveBeenCalledTimes(1)
    expect(cluster.heldDialsTo(0)).toBe(0)
    socket.write(metadataRequest())
    await settle()
    expect(frames).toHaveLength(1)
    expect(cluster.requests().map(({ api, generation }) => [api, generation])).toEqual([
      ['Metadata', 1],
    ])

    cluster.release(0)
    await settle()
    expect(connected).toHaveBeenCalledTimes(1)
    cluster.assertClean()
  })

  test('release skips a dial the client already destroyed and every dial of another node', async () => {
    const cluster = createScriptedCluster({ nodes: TWO_NODES }).blackhole(0).blackhole(1)
    const destroyed = jest.fn()
    const alive = jest.fn()
    const other = jest.fn()
    const dead = dialNode(cluster, TWO_NODES[0], destroyed)
    dialNode(cluster, TWO_NODES[0], alive)
    dialNode(cluster, TWO_NODES[1], other)
    dead.socket.destroy()
    await settle()
    expect(cluster.heldDialsTo(0)).toBe(1)
    expect(cluster.heldDialsTo(1)).toBe(1)

    cluster.release(0)
    await settle()
    expect(destroyed).not.toHaveBeenCalled()
    expect(alive).toHaveBeenCalledTimes(1)
    expect(other).not.toHaveBeenCalled()
    expect(cluster.heldDialsTo(1)).toBe(1)
  })

  test('a released dial keeps the generation it was dialled with', async () => {
    const cluster = createScriptedCluster({ nodes: NODES })
    const held = dialNode(cluster.blackhole(0), NODES[0])
    cluster.up(0)
    const later = dialNode(cluster, NODES[0])
    cluster.release(0)
    await settle()
    held.socket.write(metadataRequest())
    later.socket.write(metadataRequest())
    await settle()

    expect(cluster.requests().map(({ generation }) => generation)).toEqual([1, 2])
    cluster.assertClean()
  })

  test('an outage lays its fields over the answers of its API until recover', async () => {
    const cluster = createScriptedCluster({ nodes: NODES })
    const { socket, frames } = dialNode(cluster, NODES[0])
    const ask = async () => {
      socket.write(metadataRequest())
      await settle()
      return topLevelCode(frames[frames.length - 1])
    }

    const before = await ask()
    expect(cluster.outage('Metadata', { errorCode: 129 })).toBe(cluster)
    const during = await ask()
    const duringAgain = await ask()
    expect(cluster.recover('Metadata')).toBe(cluster)
    const after = await ask()

    expect([before, during, duringAgain, after]).toEqual([0, 129, 129, 0])
    expect(frames).toHaveLength(4)
    cluster.assertClean()
  })

  test('an outage leaves a Buffer answer released by the test as it is', async () => {
    const cluster = createScriptedCluster({ nodes: NODES }).outage('Metadata', { errorCode: 129 })
    const { socket, frames } = dialNode(cluster, NODES[0])
    const gate = cluster.schedule.hold({ api: 'Metadata' })
    socket.write(metadataRequest())
    await within(gate.arrived(), 1000, 'the held Metadata call')
    gate.release(metadataV13.buildResponse({ errorCode: 0 }))
    await settle()

    expect(frames).toHaveLength(1)
    expect(topLevelCode(frames[0])).toBe(0)
    cluster.assertClean()
  })
})

test('a connect to a profile that offers ApiVersions v4 takes exactly NEGOTIATION_MS fake milliseconds', async () => {
  jest.useFakeTimers()
  const cluster = createScriptedCluster({ nodes: NODES })
  const logger = createLogger({ level: 0, logCreator: () => () => {} })
  const broker = new Broker({
    connectionPool: new ConnectionPool({
      host: NODES[0].host,
      port: NODES[0].port,
      logger,
      socketFactory: cluster.socketFactory,
      connectionTimeout: 300,
      requestTimeout: 1500,
    }),
    logger,
  })
  try {
    let ready = false
    const connecting = broker.connect().then(() => {
      ready = true
    })
    await jest.advanceTimersByTimeAsync(NEGOTIATION_MS - 1)
    expect(ready).toBe(false)
    expect(cluster.apiVersionTrace().map(({ version }) => version)).toEqual([5, 4])
    await jest.advanceTimersByTimeAsync(1)
    await connecting
    expect(ready).toBe(true)
  } finally {
    await broker.disconnect({ drainTimeout: 0 })
    await jest.advanceTimersByTimeAsync(1)
    cluster.assertClean()
    jest.useRealTimers()
  }
})

describe('share profile', () => {
  test.each(PROFILES.share.filter(([api]) => api.includes('Share')))(
    'defaults %s v%i answers to zero throttle and preserves explicit throttles and buffers',
    async (api, version) => {
      const cluster = createScriptedCluster({ nodes: NODES, profile: 'share' })
      const pair = cluster.pairFor(api, version)
      const literal =
        require('../wire/expected')[`${api[0].toLowerCase()}${api.slice(1)}V${version}`]
      const request = frameOf(apiKeys[api], version, Buffer.from(literal.requestHex, 'hex'))
      const { socket, frames } = dial(cluster)
      try {
        socket.write(request)
        await settle()
        cluster.answer(api, () => ({ throttleTime: 43 }))
        socket.write(request)
        await settle()
        const gate = cluster.schedule.hold({ api })
        socket.write(request)
        await within(gate.arrived(), 5000, 'the held share response')
        expect(frames).toHaveLength(2)
        gate.release({})
        await settle()
        const raw = pair.buildResponse({ throttleTime: 71 })
        cluster.schedule.once({ api }, raw)
        socket.write(request)
        await settle()

        expect(frames).toHaveLength(4)
        expect(frames.map((frame) => frame.readInt32BE(9))).toEqual([0, 43, 0, 71])
        expect(frames[3].subarray(9)).toEqual(raw)
        cluster.assertClean()
      } finally {
        socket.destroy()
      }
    }
  )

  test('installs literal rows without simulating group state', () => {
    const cluster = createScriptedCluster({ nodes: NODES, profile: 'share' })
    const response = Object.freeze({ memberEpoch: 7, assignment: null })

    expect(shareScript(cluster, [{ api: 'ShareGroupHeartbeat', response }])).toBe(cluster)
    expect(cluster.answerers.get('ShareGroupHeartbeat')({ memberEpoch: 0 })).toBe(response)
    expect(cluster.answerers.get('ShareGroupHeartbeat')({ memberEpoch: -1 })).toBe(response)
    expect(cluster.requests()).toEqual([])
  })

  test('replaces an earlier answer of the same API with the later row', () => {
    const cluster = createScriptedCluster({ nodes: NODES, profile: 'share' })
    const first = { memberEpoch: 1 }
    const second = { memberEpoch: 2 }

    shareScript(cluster, [
      { api: 'ShareGroupHeartbeat', response: first },
      { api: 'ShareGroupHeartbeat', response: second },
    ])

    expect(cluster.answerers.get('ShareGroupHeartbeat')({})).toBe(second)
  })

  test('advertises only bootstrap and the seven released share versions', () => {
    expect(PROFILES.share).toEqual([
      ['ApiVersions', 4],
      ['Metadata', 13],
      ['FindCoordinator', 6],
      ['ShareGroupHeartbeat', 1],
      ['ShareGroupDescribe', 1],
      ['ShareFetch', 2],
      ['ShareAcknowledge', 2],
      ['DescribeShareGroupOffsets', 1],
      ['AlterShareGroupOffsets', 0],
      ['DeleteShareGroupOffsets', 0],
    ])
    const cluster = createScriptedCluster({ nodes: NODES, profile: 'share' })

    expect(cluster.apiVersionsTable()).toHaveLength(10)
    expect(cluster.apiVersionsAnswer()).toMatchObject({
      finalizedFeaturesEpoch: 7,
      finalizedFeatures: [{ name: 'share.version', minVersionLevel: 1, maxVersionLevel: 1 }],
    })
    for (const [api, version] of PROFILES.share) {
      expect(cluster.apiVersionsTable()).toContainEqual({
        apiKey: api === 'FindCoordinator' ? 10 : apiKeys[api],
        minVersion: version,
        maxVersion: version,
      })
    }
    for (const api of [
      'ApiVersions',
      'Metadata',
      'FindCoordinator',
      'ShareGroupHeartbeat',
      'ShareFetch',
      'ShareAcknowledge',
    ]) {
      const version = Object.fromEntries(PROFILES.share)[api]

      expect(cluster.pairFor(api, version)).toMatchObject({ version })
    }
    cluster.assertClean()
  })

  test('requires all seven concrete share builders without a missing-pair escape', () => {
    const cluster = createScriptedCluster({ nodes: NODES, profile: 'share' })
    const shareApis = PROFILES.share.filter(([api]) => api.includes('Share'))

    expect(shareApis).toHaveLength(7)
    for (const [api, version] of shareApis) {
      expect(cluster.pairFor(api, version)).toMatchObject({ api, version })
    }
    cluster.assertClean()
  })
})

describe('telemetry strict frames', () => {
  const get = require('../../src/protocol/requests/getTelemetrySubscriptions/fixtures/java/getTelemetrySubscriptionsRequest-v0-zero.json')
  const push = require('../../src/protocol/requests/pushTelemetry/fixtures/java/pushTelemetryRequest-v0-empty.json')
  test.each([
    ['GetTelemetrySubscriptions', 71, get],
    ['PushTelemetry', 72, push],
  ])('%s reads literal fields through a flexible frame', async (api, key, vector) => {
    const cluster = createScriptedCluster({ nodes: NODES, profile: 'telemetry' })
    const body = Buffer.from(vector.hex, 'hex')
    const { socket, frames } = await exchange(cluster, frameOf(key, 0, body))
    try {
      expect(cluster.requests()).toHaveLength(1)
      expect(cluster.requests()[0]).toMatchObject({
        api,
        apiKey: key,
        version: 0,
        correlationId: 1,
        generation: 1,
        fields: vector.fields,
      })
      expect(frames).toHaveLength(1)
      expect(frames[0].readInt32BE(4)).toBe(1)
      expect(frames[0][8]).toBe(0)
      cluster.assertClean()
    } finally {
      socket.destroy()
    }
  })
  test.each([
    ['getTelemetrySubscriptionsV0', get],
    ['pushTelemetryV0', push],
  ])('%s skips an unknown request tag and rejects truncated tag values', (module, vector) => {
    const pair = require(`../wire/${module}`)
    const body = Buffer.from(vector.hex, 'hex')
    const tagged = Buffer.concat([body.subarray(0, -1), Buffer.from('010703aabbcc', 'hex')])
    expect(pair.readRequest(tagged)).toEqual(vector.fields)
    expect(() => pair.readRequest(tagged.subarray(0, -1))).toThrow()
  })
  test('telemetry profile adds exactly two pairs to the default profile', () => {
    expect(PROFILES.telemetry).toEqual([
      ...PROFILES.default,
      ['GetTelemetrySubscriptions', 0],
      ['PushTelemetry', 0],
    ])
  })
  test('a missing telemetry pair refuses before a response can be built', () => {
    const cluster = createScriptedCluster({
      nodes: NODES,
      profile: 'telemetry',
      advertise: { PushTelemetry: [1, 1] },
    })
    expect(() => cluster.pairFor('PushTelemetry', 1)).toThrow('no response builder')
    cluster.assertClean()
  })
  test('unadvertised v0 refuses before body parsing', () => {
    const cluster = createScriptedCluster({ nodes: NODES })
    expect(() => cluster.accept(NODES[0], 1, frameOf(71, 0, Buffer.from(get.hex, 'hex')))).toThrow(
      'cluster advertises no version'
    )
    cluster.assertClean()
  })
  test.each([
    ['GetTelemetrySubscriptions', 71, get],
    ['PushTelemetry', 72, push],
  ])('%s refuses trailing bytes and a missing header tag', (_api, key, vector) => {
    const cluster = createScriptedCluster({ nodes: NODES, profile: 'telemetry' })
    const body = Buffer.from(vector.hex, 'hex')
    expect(() =>
      cluster.accept(NODES[0], 1, frameOf(key, 0, Buffer.concat([body, Buffer.from([0])])))
    ).toThrow('its tag buffer')
    const frame = frameOf(key, 0, body)
    const badHeader = Buffer.concat([frame.subarray(0, 17), frame.subarray(18)])
    badHeader.writeInt32BE(badHeader.length - 4, 0)
    expect(() => cluster.accept(NODES[0], 1, badHeader)).toThrow()
    cluster.assertClean()
  })
  test('null Metrics and wrong frame length refuse loudly', () => {
    const pair = require('../wire/pushTelemetryV0')
    const body = Buffer.from(push.hex, 'hex')
    body[22] = 0
    expect(() => pair.readRequest(body)).toThrow('Metrics cannot be null')
    const cluster = createScriptedCluster({ nodes: NODES, profile: 'telemetry' })
    const frame = frameOf(72, 0, Buffer.from(push.hex, 'hex'))
    frame.writeInt32BE(1, 0)
    expect(() => cluster.accept(NODES[0], 1, frame)).toThrow('size prefix')
    cluster.assertClean()
  })
})

describe('bounded capture', () => {
  const live = []
  const metadata = () => frameOf(apiKeys.Metadata, 13, METADATA_REQUEST)
  const v5 = new Encoder()
    .writeUVarIntString('raw')
    .writeUVarIntString('1')
    .writeUVarIntString('c')
    .writeInt32(2)
    .writeTaggedFields().buffer
  /** Dials, keeps the socket for cleanup and returns it with what it receives. */
  const dialLive = (cluster) => {
    const dialled = dial(cluster)
    live.push(dialled.socket)
    return dialled
  }
  /** Destroys a socket and lets its close callbacks and the cluster's cleanup run. */
  const close = async (socket) => {
    socket.destroy()
    await settle()
  }

  afterEach(() => {
    for (const socket of live.splice(0)) socket.destroy()
  })

  test('leaves a default cluster as it was and counts the same requests', async () => {
    const cluster = createScriptedCluster({ nodes: NODES })
    const { socket } = dialLive(cluster)
    socket.write(metadata())
    await settle()

    expect(cluster.requests()).toHaveLength(1)
    expect(['requested', 'answered', 'cancelled'].map((o) => cluster.tally('Metadata', o))).toEqual(
      [1, 1, 0]
    )
    await close(socket)
    expect(cluster.sockets()).toHaveLength(1)
    expect(cluster.seenSockets.size).toBe(1)
  })

  test('refuses to bound a cluster that has already been used', async () => {
    const cluster = createScriptedCluster({ nodes: NODES })
    dialLive(cluster)

    expect(() => cluster.bound()).toThrow(
      'ScriptedCluster: bound() needs a cluster nothing has used'
    )
  })

  test('drops an answered request from the log and keeps it in the tally and for listeners', async () => {
    const cluster = createScriptedCluster({ nodes: NODES, bounded: true })
    const events = []
    cluster.watch((event, record) =>
      events.push([event, record.api, typeof record.fields, record.answeredOrder !== undefined])
    )
    const { socket } = dialLive(cluster)
    socket.write(metadata())
    await settle()

    expect(events).toEqual([
      ['request', 'Metadata', 'object', false],
      ['answered', 'Metadata', 'object', true],
    ])
    expect(cluster.requests()).toEqual([])
    expect(['requested', 'answered', 'cancelled'].map((o) => cluster.tally('Metadata', o))).toEqual(
      [1, 1, 0]
    )
    cluster.assertClean()
  })

  test('keeps a request live until it settles and counts a closed socket as cancelled', async () => {
    const schedule = new Schedule()
    const gate = schedule.hold({ api: 'Metadata' })
    const cluster = createScriptedCluster({ nodes: NODES, schedule, bounded: true })
    const { socket } = dialLive(cluster)
    socket.write(metadata())
    await settle()
    expect(cluster.requests()).toHaveLength(1)

    await close(socket)
    expect(cluster.requests()).toHaveLength(1)
    gate.release()
    await settle()

    expect(cluster.requests()).toEqual([])
    expect(['requested', 'answered', 'cancelled'].map((o) => cluster.tally('Metadata', o))).toEqual(
      [1, 0, 1]
    )
    cluster.assertClean()
  })

  test('keeps generations monotonic after the closed sockets were dropped', async () => {
    const cluster = createScriptedCluster({ nodes: NODES, bounded: true })
    const generations = []
    cluster.watch((event, record) => event === 'request' && generations.push(record.generation))
    for (let i = 0; i < 3; i++) {
      const { socket } = dialLive(cluster)
      socket.write(metadata())
      await settle()
      await close(socket)
    }

    expect(generations).toEqual([1, 2, 3])
    expect(cluster.socketsOpenedTo(NODES[0])).toBe(3)
    expect(cluster.sockets()).toEqual([])
    cluster.assertClean()
  })

  test('forgets the negotiation of a closed socket and still treats each socket as new', async () => {
    const cluster = createScriptedCluster({ nodes: NODES, bounded: true })
    for (let i = 0; i < 3; i++) {
      const { socket, frames } = dialLive(cluster)
      socket.write(frameOf(apiKeys.ApiVersions, 5, v5))
      await settle()
      expect(frames).toHaveLength(1)
      socket.write(frameOf(apiKeys.ApiVersions, 5, v5))
      await settle()
      expect(frames).toHaveLength(2)
      await close(socket)
    }

    expect(['requested', 'answered'].map((o) => cluster.tally('ApiVersions', o))).toEqual([3, 3])
    expect(cluster.negotiatedAt.size).toBe(0)
    expect(cluster.seenSockets.size).toBe(0)
    cluster.assertClean()
  })

  test('fails loudly past the live request bound', async () => {
    const schedule = new Schedule()
    const first = schedule.hold({ api: 'Metadata' })
    const second = schedule.hold({ api: 'Metadata' })
    const cluster = createScriptedCluster({
      nodes: NODES,
      schedule,
      bounded: { maxLiveRequests: 2 },
    })
    const { socket } = dialLive(cluster)
    socket.write(metadata())
    socket.write(metadata())
    await settle()
    expect(cluster.requests()).toHaveLength(2)

    socket.write(metadata())
    await settle()

    expect(cluster.failures().map((error) => error.message)).toEqual([
      'ScriptedCluster: capture overflow, more than 2 live requests',
    ])
    expect(() => cluster.assertClean()).toThrow('more than 2 live requests')
    expect(socket.destroyed).toBe(true)
    first.release()
    second.release()
    await settle()
  })

  test('bounds held dials and drops one whose socket has closed', async () => {
    const cluster = createScriptedCluster({ nodes: NODES, bounded: { maxHeldDials: 1 } })
    cluster.blackhole(0)
    const { socket } = dialLive(cluster)
    expect(cluster.heldDialsTo(0)).toBe(1)

    expect(() => dialLive(cluster)).toThrow('more than 1 held dials')
    expect(cluster.failures().map((error) => error.message)).toEqual([
      'ScriptedCluster: capture overflow, more than 1 held dials',
    ])

    await close(socket)
    expect(cluster.heldDials).toEqual([])
    expect(() => dialLive(cluster)).not.toThrow()
  })

  test('releases the schedule call history only in a bounded cluster', async () => {
    const kept = new Schedule()
    const released = new Schedule()
    for (const [schedule, bounded] of [
      [kept, false],
      [released, true],
    ]) {
      const cluster = createScriptedCluster({ nodes: NODES, schedule, bounded })
      const { socket } = dialLive(cluster)
      socket.write(metadata())
      await settle()
    }

    expect(kept.calls).toHaveLength(1)
    expect(released.calls).toEqual([])
  })

  test.each(['timeline', 'apiVersionTrace'])('%s refuses in bounded mode', (name) => {
    const cluster = createScriptedCluster({ nodes: NODES, bounded: true })

    expect(() => cluster[name]()).toThrow(`ScriptedCluster: ${name}() needs the full history`)
    expect(() => cluster.mark('x')).toThrow('ScriptedCluster: mark() needs the full history')
  })

  test('records a throwing listener as a cluster failure and keeps answering', async () => {
    const cluster = createScriptedCluster({ nodes: NODES, bounded: true })
    cluster.watch(() => {
      throw new Error('listener failed')
    })
    const { socket, frames } = dialLive(cluster)
    socket.write(metadata())
    await settle()

    expect(frames).toHaveLength(1)
    expect(cluster.failures().map((error) => error.message)).toEqual([
      'listener failed',
      'listener failed',
    ])
  })

  test('stops notifying a removed listener', async () => {
    const cluster = createScriptedCluster({ nodes: NODES, bounded: true })
    const seen = []
    const remove = cluster.watch((event) => seen.push(event))
    remove()
    const { socket } = dialLive(cluster)
    socket.write(metadata())
    await settle()

    expect(seen).toEqual([])
    expect(cluster.tally('Metadata')).toBe(1)
  })
})
