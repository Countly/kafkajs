const { Kafka } = require('../../index')
const Encoder = require('../../src/protocol/encoder')
const apiKeys = require('../../src/protocol/requests/apiKeys')
const { createScriptedCluster, groupScript, PROFILES } = require('../scriptedCluster')
const { Schedule } = require('../schedule')
const metadataV13 = require('../wire/metadataV13')
const { metadataV13: literal } = require('../wire/expected')
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

    /** The Metadata request that answers `listTopics`: the second, after the one refreshing the cache. */
    const answeringListTopics = (cluster) => ({
      api: 'Metadata',
      match: () => cluster.requests().filter(({ api }) => api === 'Metadata').length === 2,
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

    test.each([
      ['sasl', 'SaslHandshake v1', { sasl: { mechanism: 'plain', username: 'u', password: 'p' } }],
    ])(
      'refuses the declared but unbuilt pair of the %s profile by name',
      async (profile, pair, extra) => {
        const cluster = createScriptedCluster({ nodes: NODES, profile })
        const admin = track(clientOf(cluster, { retry: { retries: 0 }, ...extra }).admin())

        await expect(admin.connect()).rejects.toThrow()

        expect(cluster.failures().map(({ message }) => message)).toEqual([
          `ScriptedCluster: no response builder for ${pair}`,
        ])
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
        match: () => cluster.requests().filter(({ api }) => api === 'Metadata').length === 2,
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
})
