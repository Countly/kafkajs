const { EventEmitter } = require('events')
const fs = require('fs')
const path = require('path')

jest.mock('testHelpers', () => ({
  ...jest.requireActual('testHelpers'),
  createTopic: jest.fn(async () => {}),
}))
jest.mock('../../../testHelpers/rawLog')
jest.mock('../../../index', () => ({ ...jest.requireActual('../../../index'), Kafka: jest.fn() }))

const Broker = require('../../broker')
const Cluster = require('../../cluster')
const { KafkaJSConnectionError, KafkaJSRequestTimeoutError } = require('../../errors')
const { createErrorFromCode } = require('../../protocol/error')
const ISOLATION_LEVEL = require('../../protocol/isolationLevel')
const apiKeys = require('../../protocol/requests/apiKeys')
const { requests } = require('../../protocol/requests')
const { compressedBatch } = require('../../protocol/message/compression/fixtures/compressedBatch')
const { batch } = require('../../protocol/requests/fetch/__tests__/fixtures/recordBatchBytes')
const { createChaosSocketFactory } = require('../../../testHelpers/chaosSocket')
const { BrokerBouncer } = require('../../../testHelpers/leaderElection')
const rawLog = require('../../../testHelpers/rawLog')
const { createTopic } = require('testHelpers')
const { Kafka } = require('../../../index')
const { waitForCondition } = require('../../consumer/__tests__/fixtures/liveScenario')
const Generation = require('../generation')
const ProducerStateManager = require('../state')
const Accumulator = require('../state/accumulator')
const { Admission } = require('../state/admission')
const { RequestBudget, chargeOf } = require('../state/recordSize')
const {
  CleanupStack,
  ProduceLedger,
  ProducerProbe,
  batchOwnership,
  dataRecords,
  describeError,
  probeLayout,
  readUntilCovered,
  retryCoordinator,
  sequenceGaps,
  settle,
  topicConfig,
  traceCalls,
  varintSize,
  within,
} = require('./fixtures/liveProbe')
const {
  CASE_JOIN_MS,
  LiveRun,
  NEVER,
  STOP_GRACE_MS,
  bounded,
  caseTimeout,
  delay,
  liveCase,
  liveRun,
  pollTolerating,
  pollUntil,
} = require('./fixtures/bounded')
const { bufferedProducer, deferred, watch } = require('./fixtures/bufferHarness')
const { createFakeCluster } = require('./fixtures/fakeCluster')
const { heartbeatInBatch } = require('./fixtures/memberHeartbeat')
const {
  FrameLog,
  ResponseDrop,
  createLiveSockets,
  rawProduceRequest,
  repliesOf,
  scanBatches,
} = require('./fixtures/liveWire')
const {
  CASE_BUDGET_MS,
  CaseTimeline,
  FollowerOutage,
  JOIN_RESERVE_MS,
  PHASE_BUDGET_MS,
  RESTORE_STEP_MS,
  TopologyPin,
  isrIsFull,
} = require('./fixtures/replicaOutage')

describe('Producer > the live scenarios harness', () => {
  afterEach(() => liveRun.reset())

  /** The next case refuses to start, naming `cause`, a pattern. */
  const refusesBecause = (cause) =>
    expect(() => liveRun.assertClean()).toThrow(
      new RegExp(`earlier case may have left work running.*${cause}`)
    )

  describe('probeLayout', () => {
    const identifiers = {
      clientId: 'e2e-p9b-x-0123456789abcdef',
      topic: 'e2e-p9b-probe-0123456789abcdef',
    }

    test.each([
      [33554432, 991, 8602, 1105510],
      [1048576, 31, 29466, 36070],
      [65536, 2, 29004, 3764],
    ])(
      'fills a buffer of %i bytes with %i records and no residual slack',
      (capacity, count, tail, overhead) => {
        for (const maxRequestSize of [262144, 1048576]) {
          const layout = probeLayout({ capacity, maxRequestSize, ...identifiers })

          expect(layout.limit).toBe(32768)
          expect(layout.count).toBe(count)
          expect(layout.lengths.slice(0, -1).every((length) => length === 32768)).toBe(true)
          expect(layout.lengths.at(-1)).toBe(tail)
          expect(layout.overhead).toBe(overhead)
          expect(layout.residualSlack).toBe(0)
        }
      }
    )

    test.each([65536, 100000, 200000, 999999, 1234567, 33554432])(
      'agrees with what the producer charges and plans for a buffer of %i bytes',
      (capacity) => {
        const layout = probeLayout({ capacity, maxRequestSize: 1048576, ...identifiers })
        const messages = layout.lengths.map((length) => ({ value: Buffer.alloc(length) }))
        const budget = new RequestBudget({
          maxRequestSize: 1048576,
          clientId: identifiers.clientId,
        })

        expect(chargeOf([{ topic: identifiers.topic, messages }], capacity).total).toBe(
          layout.charge
        )
        expect(layout.charge + 1024 * layout.count).toBeLessThanOrEqual(capacity)
        expect(layout.room).toBe(budget.chunkCapacity(identifiers.topic, 0))
      }
    )

    test('includes the transactional id in the request framing', () => {
      const plain = probeLayout({ capacity: 65536, maxRequestSize: 1048576, ...identifiers })
      const transactional = probeLayout({
        capacity: 65536,
        maxRequestSize: 1048576,
        transactionalId: 'a-transactional-id',
        ...identifiers,
      })

      expect(transactional.framing).toBe(plain.framing + 'a-transactional-id'.length)
    })

    test('lowers the record cap when the request cannot carry the largest record', () => {
      const layout = probeLayout({
        capacity: 65536,
        maxRequestSize: 4196,
        clientId: 'c',
        topic: 't',
      })

      expect(layout.limit).toBeLessThan(32768)
      expect(layout.lengths.every((n) => n + 23 + varintSize(n) <= layout.room)).toBe(true)
    })

    test('refuses identifiers that leave no room and a buffer that holds no record', () => {
      expect(() =>
        probeLayout({
          capacity: 65536,
          maxRequestSize: 1048576,
          clientId: 'c'.repeat(5000),
          topic: 't',
        })
      ).toThrow(/framing/)
      expect(() =>
        probeLayout({ capacity: 1600, maxRequestSize: 1048576, clientId: 'c', topic: 't' })
      ).toThrow(/holds no probe record/)
    })

    test('derives the zigzag varint size from the encoding', () => {
      for (const [length, size] of [
        [0, 1],
        [63, 1],
        [64, 2],
        [8191, 2],
        [8192, 3],
        [1048575, 3],
        [1048576, 4],
      ]) {
        expect(varintSize(length)).toBe(size)
      }
    })
  })

  describe('scanBatches', () => {
    test('finds real batches among bytes that are not batches', async () => {
      const found = []
      for (const [index, codec] of ['none', 'gzip', 'snappy', 'zstd'].entries()) {
        found.push(await compressedBatch({ codec, records: 3, baseOffset: BigInt(index * 3) }))
      }
      const junk = Buffer.from([0xff, 0xff, 0xff, 0xff, 2, 2, 2, 2, 2, 2])
      const stream = Buffer.concat([junk, found[0], junk, found[1], found[2], junk, found[3]])

      const scanned = scanBatches(stream)

      expect(scanned.map(({ codecName }) => codecName)).toEqual(['none', 'gzip', 'snappy', 'zstd'])
      expect(scanned.map(({ baseOffset }) => baseOffset)).toEqual(['0', '3', '6', '9'])
      expect(
        scanned.every(({ count, lastOffsetDelta }) => count === 3 && lastOffsetDelta === 2)
      ).toBe(true)
      expect(scanned[1].body.length).toBe(scanned[1].bytes.length - 61)
      expect(Buffer.concat(scanned.map(({ bytes }) => bytes))).toEqual(Buffer.concat(found))
      expect(scanBatches(stream.subarray(0, stream.length - 5))).toHaveLength(3)
    })

    test('reports the envelope fields of a stamped transactional batch', () => {
      const stamped = batch({
        attributes: 0x10 | 0x08 | 1,
        baseOffset: 40n,
        firstTimestamp: 1700000000000n,
        producerId: 77n,
        producerEpoch: 3,
        baseSequence: 5,
        lastOffsetDelta: 1,
        count: 2,
        records: [Buffer.from('opaque')],
      })

      const [scanned] = scanBatches(stamped)

      expect(scanned).toMatchObject({
        baseOffset: '40',
        codecName: 'gzip',
        logAppendTime: true,
        transactional: true,
        control: false,
        firstTimestamp: '1700000000000',
        producerId: '77',
        producerEpoch: 3,
        baseSequence: 5,
        count: 2,
      })
    })

    test('skips a batch whose checksum does not match', () => {
      const forged = batch({ attributes: 1, crc: 123, records: [Buffer.from('opaque')] })

      expect(scanBatches(forged)).toEqual([])
    })
  })

  describe('FrameLog', () => {
    const frameOf = (apiKey, apiVersion, correlationId) => {
      const frame = Buffer.alloc(20)
      frame.writeInt32BE(16, 0)
      frame.writeInt16BE(apiKey, 4)
      frame.writeInt16BE(apiVersion, 6)
      frame.writeInt32BE(correlationId, 8)
      return frame
    }

    test('records every written frame, keeps the bytes of the retained apis, and passes all through', () => {
      const written = []
      const log = new FrameLog({ retain: [0] })
      const socket = log.observe(() => ({ write: (data) => written.push(data) }))({
        host: 'h',
        port: 1,
      })
      const produce = frameOf(0, 9, 7)
      const fetch = frameOf(1, 12, 8)

      socket.write(produce, 'binary')
      socket.write(fetch)

      expect(written).toEqual([produce, fetch])
      expect(log.frames).toMatchObject([
        { endpoint: 'h:1', connection: 1, apiKey: 0, apiVersion: 9, correlationId: 7, bytes: 20 },
        { connection: 1, apiKey: 1, apiVersion: 12, correlationId: 8, bytes: 20 },
      ])
      expect(log.frames[0].buffer).toBe(produce)
      expect(log.frames[1].buffer).toBeUndefined()
    })

    test('numbers each socket it observes, and traces the latest frames without their bytes', () => {
      const log = new FrameLog({ retain: [0] })
      const observe = log.observe(() => ({ write: () => {} }))
      const first = observe({ host: 'h', port: 1 })
      const second = observe({ host: 'h', port: 1 })

      first.write(frameOf(0, 9, 1))
      second.write(frameOf(0, 9, 2))
      second.write(frameOf(1, 12, 3))

      expect(log.frames.map(({ connection }) => connection)).toEqual([1, 2, 2])
      expect(log.trace(2)).toMatchObject([{ correlationId: 2 }, { correlationId: 3 }])
      expect(log.trace().every((facts) => !('buffer' in facts))).toBe(true)
    })

    test('counts the frames of one api since a mark, and releases the bytes it kept', () => {
      const log = new FrameLog({ retain: [0] })
      log.record('h:1', frameOf(0, 9, 1))
      const mark = log.mark()
      log.record('h:1', frameOf(0, 9, 2))
      log.record('h:1', frameOf(1, 12, 3))

      expect(log.since(0)).toHaveLength(2)
      expect(log.since(0, mark)).toHaveLength(1)
      expect(log.since(1, mark)).toHaveLength(1)
      expect(log.since(1, mark + 2)).toHaveLength(0)
      log.release()
      expect(log.frames.some(({ buffer }) => buffer !== undefined)).toBe(false)
    })
  })

  describe('ResponseDrop', () => {
    class Inner extends EventEmitter {
      write() {}
      end() {}
      destroy() {
        this.gone = true
      }

      unref() {}
      get destroyed() {
        return Boolean(this.gone)
      }
    }

    const request = (socket, correlationId, apiKey) => {
      const frame = Buffer.alloc(16)
      frame.writeInt32BE(12, 0)
      frame.writeInt16BE(apiKey, 4)
      frame.writeInt32BE(correlationId, 8)
      socket.write(frame)
    }

    const response = (socket, correlationId) => {
      const frame = Buffer.alloc(8)
      frame.writeInt32BE(4, 0)
      frame.writeInt32BE(correlationId, 4)
      socket.inner.emit('data', frame)
    }

    test('drops one answer of one api on one broker, and none after a reconnect', () => {
      const chaos = createChaosSocketFactory(() => new Inner())
      const target = chaos({ host: 'localhost', port: 9092 })
      const other = chaos({ host: 'localhost', port: 9095 })
      const received = []
      target.on('data', (chunk) => received.push(chunk))
      const drop = new ResponseDrop(chaos, { port: 9092, apiKey: 0 }).arm()

      expect(drop.armed.size).toBe(1)
      expect(other.responseRule).toBeNull()
      request(target, 1, 3)
      response(target, 1)
      expect(drop.dropped).toBe(0)
      request(target, 2, 0)
      response(target, 2)
      expect(drop.dropped).toBe(1)
      expect(received).toHaveLength(1)
      const reopened = chaos({ host: 'localhost', port: 9092 })

      expect(reopened.responseRule).toBeNull()
      expect(drop.armed.size).toBe(1)
    })

    test('release closes the connection that dropped its answer and no other', () => {
      const chaos = createChaosSocketFactory(() => new Inner())
      const target = chaos({ host: 'localhost', port: 9092 })
      const idle = chaos({ host: 'localhost', port: 9092 })
      const drop = new ResponseDrop(chaos, { port: 9092, apiKey: 0 }).arm()
      request(target, 1, 0)
      response(target, 1)

      drop.release()

      expect(target.destroyed).toBe(true)
      expect(idle.destroyed).toBe(false)
    })

    test('release resets the connection with ECONNRESET, so the client fails what is in flight on it', () => {
      class NetInner extends Inner {
        destroy(error) {
          super.destroy()
          if (error) this.emit('error', error)
          this.emit('close')
        }
      }
      const chaos = createChaosSocketFactory(() => new NetInner())
      const target = chaos({ host: 'localhost', port: 9092 })
      const idle = chaos({ host: 'localhost', port: 9092 })
      const events = { target: [], idle: [] }
      for (const [name, socket] of [
        ['target', target],
        ['idle', idle],
      ]) {
        socket.on('error', (error) => events[name].push(error.code))
        socket.on('close', () => events[name].push('close'))
      }
      const drop = new ResponseDrop(chaos, { port: 9092, apiKey: 0 }).arm()
      request(target, 1, 0)
      response(target, 1)

      drop.release()

      expect(events.target).toEqual(['ECONNRESET', 'close'])
      expect(events.idle).toEqual([])
    })

    /** Two sockets to one broker, each with its answers collected. */
    const pair = (chaos) => {
      const first = chaos({ host: 'localhost', port: 9092 })
      const second = chaos({ host: 'localhost', port: 9092 })
      const received = { first: [], second: [] }
      first.on('data', (chunk) => received.first.push(chunk))
      second.on('data', (chunk) => received.second.push(chunk))
      return { first, second, received }
    }

    test('drops one answer in total when two sockets were open before it was armed', () => {
      const chaos = createChaosSocketFactory(() => new Inner())
      const { first, second, received } = pair(chaos)
      const drop = new ResponseDrop(chaos, { port: 9092, apiKey: 0 }).arm()
      request(first, 1, 0)
      request(second, 1, 0)

      response(first, 1)
      response(second, 1)

      expect(drop.dropped).toBe(1)
      expect(received.first).toHaveLength(0)
      expect(received.second).toHaveLength(1)
      expect(drop.consumer).toBe(first)
    })

    test('does not drop on a connection opened before the first answer and used after it', () => {
      const chaos = createChaosSocketFactory(() => new Inner())
      const drop = new ResponseDrop(chaos, { port: 9092, apiKey: 0 }).arm()
      const { first, second, received } = pair(chaos)
      request(first, 1, 0)
      request(second, 1, 0)

      response(first, 1)
      response(second, 1)
      request(second, 2, 0)
      response(second, 2)

      expect(drop.dropped).toBe(1)
      expect(received.second).toHaveLength(2)
      expect(drop.consumer).toBe(first)
    })

    test('a stop before any answer disarms the sockets already armed and those opened later', () => {
      const chaos = createChaosSocketFactory(() => new Inner())
      const { first, second, received } = pair(chaos)
      const drop = new ResponseDrop(chaos, { port: 9092, apiKey: 0 }).arm()

      drop.stop()
      const later = chaos({ host: 'localhost', port: 9092 })
      later.on('data', (chunk) => received.first.push(chunk))
      request(first, 1, 0)
      request(second, 1, 0)
      request(later, 1, 0)
      response(first, 1)
      response(second, 1)
      response(later, 1)

      expect(drop.dropped).toBe(0)
      expect(drop.consumer).toBeNull()
      expect(received.first).toHaveLength(2)
      expect(received.second).toHaveLength(1)
      expect(later.responseRule).toBeNull()
    })

    test('a release before any answer closes nothing and restores every socket it armed', () => {
      const chaos = createChaosSocketFactory(() => new Inner())
      const { first, second, received } = pair(chaos)
      const drop = new ResponseDrop(chaos, { port: 9092, apiKey: 0 }).arm()
      const armedLater = chaos({ host: 'localhost', port: 9092 })

      drop.release()
      request(first, 1, 0)
      response(first, 1)

      expect(first.destroyed || second.destroyed || armedLater.destroyed).toBe(false)
      for (const socket of [first, second, armedLater]) {
        expect(Object.hasOwn(socket, 'dropResponse')).toBe(false)
        expect(socket.responseRule).toBeNull()
      }
      expect(received.first).toHaveLength(1)
      expect(drop.dropped).toBe(0)
    })

    test('leaves the answers of other apis alone and does not count them as the drop', () => {
      const chaos = createChaosSocketFactory(() => new Inner())
      const { first, second, received } = pair(chaos)
      const drop = new ResponseDrop(chaos, { port: 9092, apiKey: 0 }).arm()
      request(first, 1, 3)
      request(second, 1, 0)

      response(first, 1)
      response(second, 1)

      expect(received.first).toHaveLength(1)
      expect(received.second).toHaveLength(0)
      expect(drop.consumer).toBe(second)
      drop.release()
      expect(first.destroyed).toBe(false)
      expect(second.destroyed).toBe(true)
    })
  })

  test('repliesOf keeps the latest answers with their api, broker and timing', () => {
    const payload = (correlationId) => ({
      apiName: 'Produce',
      apiKey: 0,
      correlationId,
      broker: 'h:1',
      size: 10,
      sentAt: 5,
      duration: 3,
      clientId: 'dropped',
    })

    expect(repliesOf([payload(1), payload(2), payload(3)], 2)).toEqual([
      {
        apiName: 'Produce',
        apiKey: 0,
        correlationId: 2,
        broker: 'h:1',
        size: 10,
        sentAt: 5,
        duration: 3,
      },
      {
        apiName: 'Produce',
        apiKey: 0,
        correlationId: 3,
        broker: 'h:1',
        size: 10,
        sentAt: 5,
        duration: 3,
      },
    ])
  })

  test('createLiveSockets hands out a chaos factory behind a frame log', () => {
    const sockets = createLiveSockets({ retain: [0] })

    expect(typeof sockets.socketFactory).toBe('function')
    expect(typeof sockets.chaos.arm).toBe('function')
    expect(sockets.frames).toBeInstanceOf(FrameLog)
    expect([...sockets.frames.retain]).toEqual([0])
  })

  test('rawProduceRequest writes the batch it is given into the request as it is', async () => {
    const build = requests.Produce.protocol({ version: 7 })
    const broker = { lookupRequestWithMax: () => build }
    const original = build({
      acks: -1,
      timeout: 30000,
      topicData: [
        {
          topic: 'topic',
          partitions: [{ partition: 0, messages: [{ value: 'a', timestamp: 1 }] }],
        },
      ],
    })
    const encoded = (await original.request.encode()).buffer
    const [stored] = scanBatches(encoded)

    const raw = rawProduceRequest(broker, stored.bytes, 'topic', 0)

    expect(raw.request.apiKey).toBe(apiKeys.Produce)
    expect(raw.request.apiVersion).toBe(7)
    expect((await raw.request.encode()).buffer).toEqual(encoded)
  })

  describe('CleanupStack', () => {
    test('runs every cleanup in reverse and keeps the primary failure first', async () => {
      const order = []
      const stack = new CleanupStack()
      stack.add('a', () => order.push('a'))
      stack.add('b', () => {
        throw new Error('b failed')
      })
      stack.add('c', async () => order.push('c'))
      const primary = new Error('primary')

      await expect(
        stack.run(async () => {
          throw primary
        })
      ).rejects.toBe(primary)

      expect(order).toEqual(['c', 'a'])
      expect(primary.cleanupErrors.map((error) => error.message)).toEqual(['b: b failed'])
    })

    test('throws the cleanup failures together when the work succeeded, and returns its result otherwise', async () => {
      const failing = new CleanupStack()
      failing.add('x', () => {
        throw new Error('x failed')
      })
      const fine = new CleanupStack()
      fine.add('y', () => {})

      await expect(failing.run(async () => 1)).rejects.toBeInstanceOf(AggregateError)
      await expect(fine.run(async () => 5)).resolves.toBe(5)
    })

    test('a cleanup that fails taints the run, and cleanups that succeed leave it clean', async () => {
      const fine = new CleanupStack()
      fine.add('y', () => {})
      await fine.run(async () => 1)
      expect(liveRun.tainted).toBe(false)

      const failing = new CleanupStack()
      failing.add('x', () => {
        throw new Error('x failed')
      })
      await expect(failing.run(async () => 1)).rejects.toBeInstanceOf(AggregateError)

      expect(liveRun.tainted).toBe(true)
      expect(() => liveRun.assertClean()).toThrow(/may have left work running.*x: x failed/)
    })
  })

  describe('ProducerProbe', () => {
    const CAPACITY = 65536
    const TOPIC = 'e2e-p9b-probe-topic'
    const deadline = { remaining: () => 1000 }
    const NOTHING_RETAINED = { pins: {}, sends: [], epochs: [], registrations: 0, chunks: [] }

    afterEach(() => jest.restoreAllMocks())

    test('binds to the Admission and Cluster the producer touched and reads their counters', async () => {
      const admission = new Admission({ capacity: 1000, maxBlockMs: 0 })
      const cluster = { topicPins: new Map() }
      const probe = new ProducerProbe({
        producer: { flush: async () => {} },
        clientId: 'c',
        maxRequestSize: 1048576,
      }).install()

      await probe.capture(async () => {
        Cluster.prototype.holdTopic.call(cluster, 'topic')
        admission.acquire(100, deadline)
      })
      probe.inEntry = true
      const reservation = admission.acquire(200, deadline)
      probe.inEntry = false

      expect(probe.admission).toBe(admission)
      expect(probe.cluster).toBe(cluster)
      expect(probe.acquires.filter(({ inEntry }) => inEntry)).toMatchObject([
        { instance: admission, bytes: 200, immediate: true, error: null },
      ])
      expect(probe.counters()).toEqual({
        available: 700,
        capacity: 1000,
        reserved: 300,
        waitingBytes: 0,
        debts: 0,
        pins: 1,
      })
      probe.restore()
      reservation.releaseAll()
      expect(Admission.prototype.acquire).not.toHaveProperty('mock')
    })

    /**
     * A probe bound, through its spies, to the instances one warm send touched: a real Admission,
     * Accumulator and Generation, and an object with the Cluster pin methods standing in for the
     * cluster. `begin` makes a send the way the producer does and `end` retires everything it took.
     */
    const bound = async ({
      quietBudget = { attempts: 20, delay: 10 },
      readBackBudget = { attempts: 5, delay: 1 },
    } = {}) => {
      const BatchOwnership = await batchOwnership()
      const admission = new Admission({ capacity: CAPACITY, maxBlockMs: 0 })
      const cluster = { topicPins: new Map() }
      const accumulator = new Accumulator({
        linger: 0,
        batchSize: 1024,
        partition: async () => [],
        sendPartitioned: async () => [],
      })
      const generation = new Generation()
      const producer = {
        flush: async () => {
          if (accumulator.unsettled().length > 0) throw new Error('Producer flush timed out')
        },
      }
      const probe = new ProducerProbe({
        producer,
        clientId: 'c',
        maxRequestSize: 1048576,
        quietBudget,
        readBackBudget,
      }).install()
      const begin = (topic, bytes) => {
        const send = {
          topics: topic,
          records: [{}],
          reject: () => {},
          shares: 0,
          pins: 0,
          payload: 0,
          pinner: cluster,
        }
        const chunk = { messages: [{}], owners: [] }
        Cluster.prototype.holdTopic.call(cluster, topic)
        accumulator.track(send)
        chunk.ownership = new BatchOwnership(admission, chunk, false)
        const reservation = admission.acquire(bytes, deadline)
        const end = () => {
          chunk.messages = null
          chunk.owners = null
          send.records = null
          send.reject = null
          send.topics = null
          reservation.releaseAll()
          const { epoch } = send
          send.epoch = null
          epoch.settle()
          Cluster.prototype.releaseTopic.call(cluster, topic)
        }
        return { send, end }
      }
      await probe.capture(async () => begin(TOPIC, 100).end())
      return { admission, cluster, accumulator, generation, probe, begin }
    }

    /** A tracked send that took nothing, with its flush epoch settled: nothing is retained. */
    const retiredSend = ({ accumulator, cluster }) => {
      const send = {
        topics: null,
        records: null,
        reject: null,
        shares: 0,
        pins: 0,
        payload: 0,
        pinner: cluster,
      }
      accumulator.track(send)
      send.epoch.settle()
      send.epoch = null
      return send
    }

    test('quiet waits for the producer to hold nothing and reports the counters at rest', async () => {
      const { probe, begin, admission } = await bound()
      const { end } = begin(TOPIC, 100)
      setTimeout(end, 50)

      const counters = await probe.quiet()
      probe.restore()

      expect(counters).toEqual({
        available: CAPACITY,
        capacity: CAPACITY,
        reserved: 0,
        waitingBytes: 0,
        debts: 0,
        pins: 0,
      })
      expect(admission.available).toBe(CAPACITY)
    })

    test('quiet fails when the producer keeps holding bytes', async () => {
      const { probe, begin } = await bound()
      begin(TOPIC, 100)

      await expect(probe.quiet()).rejects.toThrow()
      probe.restore()
    })

    test('settled reads nothing when no send ever bound the probe', async () => {
      const probe = new ProducerProbe({
        producer: {},
        clientId: 'c',
        maxRequestSize: 1048576,
      }).install()

      await expect(probe.settled()).resolves.toBeNull()
      probe.restore()
    })

    test('records the pins, sends, flush epochs and settlement snapshots of a send and reads them clean', async () => {
      const { probe, begin } = await bound()
      const { end } = begin(TOPIC, 200)

      expect(probe.resources().retained).toMatchObject({ pins: { [TOPIC]: 1 }, epochs: [1] })
      expect(probe.resources().retained.sends).toHaveLength(1)
      end()
      await probe.quiet()

      expect(probe.resources()).toEqual({
        retained: { pins: {}, sends: [], epochs: [], registrations: 0, chunks: [] },
        inventory: [],
        totals: {
          held: 2,
          released: 2,
          tracked: 2,
          retired: 2,
          registered: 0,
          chunks: 2,
          accumulators: 1,
          inventoried: 1,
        },
      })
      probe.restore()
    })

    test('settled fails on a release the cluster ignored, which no counter shows', async () => {
      const { probe, cluster } = await bound()
      Cluster.prototype.releaseTopic.call(cluster, 'ghost')

      expect(probe.counters().pins).toBe(0)
      await expect(probe.settled()).rejects.toThrow(/"ghost": -1/)
      probe.restore()
    })

    test('settled fails while a flush epoch still counts a send, and passes once it settled', async () => {
      const { probe, accumulator, cluster } = await bound()
      const send = retiredSend({ accumulator, cluster })
      accumulator.track(send)

      await expect(probe.settled()).rejects.toThrow(/"epoch": true/)
      expect(probe.resources().retained.epochs).toEqual([1])
      setTimeout(() => {
        send.epoch.settle()
        send.epoch = null
      }, 30)
      await expect(probe.settled()).resolves.toMatchObject({ capacity: CAPACITY })
      probe.restore()
    })

    test.each([
      ['records', [{}], null],
      ['reject', () => {}, null],
      ['topics', TOPIC, null],
      ['shares', 1, 0],
      ['pins', 1, 0],
      ['payload', 5, 0],
    ])('settled fails while a tracked send still holds its %s', async (field, held, retired) => {
      const world = await bound()
      const send = retiredSend(world)
      send[field] = held

      await expect(world.probe.settled()).rejects.toThrow(/"sends"/)
      send[field] = retired
      await expect(world.probe.settled()).resolves.toMatchObject({ capacity: CAPACITY })
      world.probe.restore()
    })

    test('a cancellation counts until it is unregistered or its lifecycle closes, and one a closed lifecycle ran at once never does', async () => {
      const { probe, generation } = await bound()
      const registrations = () => probe.resources().retained.registrations

      const unregister = generation.register(() => {})
      expect(registrations()).toBe(1)
      await expect(probe.settled()).rejects.toThrow(/"registrations": 1/)
      unregister()
      unregister()
      expect(registrations()).toBe(0)

      generation.register(() => {})
      generation.register(() => {})
      expect(registrations()).toBe(2)
      generation.close(new Error('closed'))
      expect(registrations()).toBe(0)
      const late = jest.fn()
      generation.register(late)
      expect(late).toHaveBeenCalledTimes(1)
      expect(registrations()).toBe(0)
      await expect(probe.settled()).resolves.toMatchObject({ capacity: CAPACITY })
      probe.restore()
    })

    test('the settlement snapshot is read right after a flush, and an accumulator that was never flushed fails quiet', async () => {
      const { probe, accumulator, cluster, begin } = await bound()
      const { end } = begin(TOPIC, 100)
      accumulator.unsettled()
      expect(probe.resources().inventory).toEqual([1])
      end()
      await probe.settled()
      expect(probe.resources().inventory).toEqual([1])

      await probe.quiet()

      expect(probe.resources().inventory).toEqual([])
      const unflushed = new ProducerProbe({
        producer: { flush: async () => {} },
        clientId: 'c',
        maxRequestSize: 1048576,
        quietBudget: { attempts: 2, delay: 1 },
      })
      unflushed.admission = probe.admission
      unflushed.cluster = cluster
      unflushed.ledger.accumulators.add(accumulator)
      await expect(unflushed.quiet()).rejects.toThrow()
      probe.restore()
    })

    test('restore removes every spy and forgets what was recorded', async () => {
      const { probe, begin } = await bound()
      begin(TOPIC, 10)

      probe.restore()

      for (const [target, name] of [
        [Admission, 'acquire'],
        [Cluster, 'holdTopic'],
        [Cluster, 'releaseTopic'],
        [Accumulator, 'track'],
        [Accumulator, 'unsettled'],
        [Generation, 'register'],
        [Generation, 'close'],
      ]) {
        expect(target.prototype[name]).not.toHaveProperty('mock')
      }
      expect(
        Object.getOwnPropertyDescriptor((await batchOwnership()).prototype, 'batch')
      ).toBeUndefined()
      expect(probe.resources().totals).toMatchObject({
        held: 0,
        tracked: 0,
        chunks: 0,
        accumulators: 0,
      })
    })

    describe('the chunks of the sends', () => {
      const CHUNK_TOPIC = 'e2e-p9b-probe-chunks'
      const RETIRED = { pins: {}, sends: [], epochs: [], registrations: 0, chunks: [] }

      /**
       * A real Accumulator and Admission, the Cluster pin methods on an in-memory cluster, and a
       * probe bound to them by `capture`. The send boundary records each batch it is handed and
       * runs `after` on it, which settles and retires it, or leaves it as a test needs.
       */
      const chunked = ({ after = () => {} } = {}) => {
        const admission = new Admission({ capacity: CAPACITY, maxBlockMs: 0 })
        const cluster = {
          topicPins: new Map(),
          holdTopic(topic) {
            return Cluster.prototype.holdTopic.call(this, topic)
          },
          releaseTopic(topic) {
            return Cluster.prototype.releaseTopic.call(this, topic)
          },
        }
        const batches = []
        const accumulator = new Accumulator({
          linger: 0,
          batchSize: 16384,
          admission,
          pinner: cluster,
          partition: async ({ records }) =>
            records.map(({ topic, messages }) => ({ topic, partition: 0, messages })),
          sendPartitioned: async ({ batches: handed }) => {
            for (const batch of handed) {
              batches.push(batch)
              after(batch)
            }
            return []
          },
        })
        const producer = {
          flush: async () => {
            const snapshot = accumulator.unsettled()
            await accumulator.flush()
            await Promise.all(snapshot)
          },
        }
        const probe = new ProducerProbe({
          producer,
          clientId: 'c',
          maxRequestSize: 1048576,
          quietBudget: { attempts: 20, delay: 1 },
        }).install()
        const append = (records = 1) =>
          accumulator.append({
            acks: -1,
            topicMessages: [
              {
                topic: CHUNK_TOPIC,
                messages: Array.from({ length: records }, () => ({ value: Buffer.alloc(32, 'x') })),
              },
            ],
          })
        return { probe, batches, append }
      }

      /** Resolves every settler of `batch` the way an acknowledged Produce does. */
      const acknowledge = (batch) =>
        batch.settlers.forEach((settler) =>
          settler.resolve({ topicName: CHUNK_TOPIC, partition: 0, baseOffset: '0' })
        )

      test('a retired chunk holds no record array, and the probe saw the chunk it reports clean', async () => {
        const { probe, batches, append } = chunked({
          after: (batch) => {
            acknowledge(batch)
            batch.retire()
          },
        })
        try {
          await probe.capture(() => append())

          await expect(probe.quiet()).resolves.toMatchObject({ available: CAPACITY, pins: 0 })

          expect(probe.resources().retained).toEqual(RETIRED)
          expect(probe.resources().totals.chunks).toBe(1)
          expect(batches).toHaveLength(1)
          expect(batches[0].messages).toBeNull()
          expect(batches[0].owners).toBeNull()
        } finally {
          probe.restore()
        }
      })

      test('the probe rejects a chunk that was retired but still holds its record array', async () => {
        const { probe, batches, append } = chunked({
          after: (batch) => {
            acknowledge(batch)
            const { messages } = batch
            batch.retire()
            batch.messages = messages
          },
        })
        try {
          await probe.capture(() => append())

          await expect(probe.quiet()).rejects.toThrow(/"chunks"/)

          expect(batches[0].messages).toHaveLength(1)
          expect(probe.resources().retained).toEqual({
            ...RETIRED,
            chunks: [{ records: 1, owners: null, aliasRecords: 0 }],
          })
        } finally {
          probe.restore()
        }
      })

      test('the probe rejects a chunk that was never retired, and reads it clean once it is', async () => {
        const { probe, batches, append } = chunked({ after: acknowledge })
        try {
          await probe.capture(() => append())

          await expect(probe.settled()).rejects.toThrow()
          expect(probe.resources().retained.chunks).toEqual([
            { records: 1, owners: 1, aliasRecords: 0 },
          ])
          batches[0].retire()
          await expect(probe.settled()).resolves.toMatchObject({ available: CAPACITY })
        } finally {
          probe.restore()
        }
      })

      test.each([
        ['cleared', false, true],
        ['left holding a record', true, false],
      ])('a subdivided chunk whose aliases are %s', async (_name, leaks, clean) => {
        const { probe, append } = chunked({
          after: (batch) => {
            const [head, tail] = Accumulator.cut(batch, 2)
            for (const child of [head, tail]) {
              acknowledge(child)
              child.retire()
            }
            if (leaks) head.messages.push({ value: 'left behind' })
          },
        })
        try {
          await probe.capture(() => append(4))

          if (clean) {
            await expect(probe.quiet()).resolves.toMatchObject({ available: CAPACITY })
            expect(probe.resources().retained).toEqual(RETIRED)
          } else {
            await expect(probe.quiet()).rejects.toThrow(/"aliasRecords": 1/)
          }
          expect(probe.resources().totals.chunks).toBe(3)
        } finally {
          probe.restore()
        }
      })

      /** A real producer over a fake cluster whose pin methods are the cluster's own. */
      const realProducer = async (idempotent, options = {}) => {
        const cluster = createFakeCluster({ partitions: 2 })
        cluster.topicPins = new Map()
        cluster.holdTopic = (topic) => Cluster.prototype.holdTopic.call(cluster, topic)
        cluster.releaseTopic = (topic) => Cluster.prototype.releaseTopic.call(cluster, topic)
        const { producer } = bufferedProducer({
          cluster,
          idempotent,
          capacity: CAPACITY,
          ...options,
        })
        const probe = new ProducerProbe({
          producer,
          clientId: 'c',
          maxRequestSize: 1048576,
          quietBudget: { attempts: 20, delay: 10 },
        }).install()
        await producer.connect()
        await probe.capture(() =>
          producer.send({ topic: CHUNK_TOPIC, messages: [{ value: 'warm' }] })
        )
        const send = () =>
          producer.send({
            topic: CHUNK_TOPIC,
            messages: [
              { value: 'a', partition: 0 },
              { value: 'b', partition: 1 },
            ],
          })
        return { producer, probe, send, cluster }
      }

      /** Wraps a method so `before` sees its arguments first; returns what puts it back. */
      const wrapped = (target, name, before) => {
        const original = target[name]
        target[name] = function (...args) {
          before(...args)
          return original.apply(this, args)
        }
        return () => {
          target[name] = original
        }
      }

      /** Makes a batch's retire keep the record array its holder had, as a retire that forgot to clear it. */
      const keepsRecords = (batch) => {
        const holder = batch.ownership.batch
        const { retire } = batch
        if (retire.keeps) return
        const kept = holder.messages
        batch.retire = Object.assign(
          () => {
            retire()
            holder.messages = kept
          },
          { keeps: true }
        )
      }

      test.each([
        ['idempotent', true],
        ['plain', false],
      ])(
        'observes the chunks of a real %s producer and reads them clean',
        async (_kind, idempotent) => {
          const { producer, probe, send } = await realProducer(idempotent)
          try {
            const before = probe.resources().totals.chunks

            await send()

            await expect(probe.quiet()).resolves.toMatchObject({ pins: 0 })
            expect(probe.resources().totals.chunks).toBe(before + 2)
            expect(probe.resources().retained).toEqual(RETIRED)
          } finally {
            probe.restore()
            await producer.disconnect()
          }
        }
      )

      test.each([
        [
          'idempotent',
          true,
          () =>
            wrapped(ProducerStateManager.prototype, 'enqueue', (batches) =>
              batches.forEach(keepsRecords)
            ),
        ],
        ['plain', false, () => wrapped(RequestBudget.prototype, 'batchBytes', keepsRecords)],
      ])(
        'the probe rejects a chunk a real %s producer retired without clearing its record array',
        async (_kind, idempotent, leak) => {
          const { producer, probe, send } = await realProducer(idempotent)
          const unwrap = leak()
          try {
            await send()

            await expect(probe.quiet()).rejects.toThrow(/"chunks"/)
            expect(probe.resources().retained.chunks.length).toBeGreaterThan(0)
          } finally {
            unwrap()
            probe.restore()
            await producer.disconnect()
          }
        }
      )

      test.each([
        ['clears the records', false],
        ['keeps the records', true],
      ])(
        'a plain batch that expires before it is planned is observed at birth, and a retire that %s is judged',
        async (_name, leaks) => {
          const { producer, probe, cluster } = await realProducer(false, { deliveryTimeout: 100 })
          const held = deferred()
          const arrived = deferred()
          try {
            await probe.quiet()
            const before = probe.resources().totals.chunks
            cluster.findBroker.mockImplementation(async () => {
              arrived.resolve()
              return held.promise
            })

            const sending = producer
              .send({ topic: CHUNK_TOPIC, messages: [{ value: 'early' }] })
              .catch((error) => error)
            await arrived.promise
            const [holder] = [...probe.ledger.chunks.keys()]
            expect(holder).toBeDefined()
            const records = holder.messages
            expect((await sending).name).toBe('KafkaJSDeliveryTimeout')
            held.resolve(cluster.broker)
            await new Promise((resolve) => setImmediate(resolve))
            if (leaks) holder.messages = records

            expect(probe.resources().totals.chunks).toBe(before + 1)
            if (leaks) {
              await expect(probe.quiet()).rejects.toThrow(/"chunks"/)
            } else {
              await expect(probe.quiet()).resolves.toMatchObject({ available: CAPACITY })
              expect(holder.messages).toBeNull()
            }
          } finally {
            held.resolve(cluster.broker)
            probe.restore()
            await producer.disconnect()
          }
        }
      )
    })

    describe('closing', () => {
      const PROBE_TOPIC = 'e2e-p9b-probe-closing'
      let stored

      beforeEach(() => {
        stored = []
        createTopic.mockClear()
        Kafka.mockImplementation(() => ({
          admin: () => ({
            connect: async () => {},
            disconnect: async () => {},
            describeConfigs: async () => ({
              resources: [
                {
                  configEntries: [{ configName: 'max.message.bytes', configValue: '1048576' }],
                },
              ],
            }),
          }),
        }))
        rawLog.readLog.mockImplementation(
          async () =>
            new Map([
              [
                0,
                {
                  records: stored.map(({ value }) => ({
                    value,
                    batchContext: { isControlBatch: false },
                  })),
                },
              ],
            ])
        )
      })

      /** A sender that makes the probe send the way the producer does, in the entry turn. */
      const sender = (world, { extra, late } = {}) => ({
        send: ({ topic, messages }) => {
          const charge = chargeOf([{ topic, messages }], CAPACITY).total
          stored = messages
          const start = () => world.begin(topic, charge)
          if (late) {
            return Promise.resolve().then(() => {
              const { end } = start()
              end()
            })
          }
          const { end } = start()
          if (extra)
            Promise.resolve().then(() => world.admission.acquire(10, deadline).releaseAll())
          return new Promise((resolve) => setImmediate(() => resolve(end())))
        },
      })

      test('admits the probe in the entry turn, and reports the resources before, at entry and after', async () => {
        const world = await bound()

        const result = await world.probe.closing({
          sender: sender(world),
          topic: PROBE_TOPIC,
          budgetMs: 1000,
        })
        world.probe.restore()

        expect(result.observed).toMatchObject({ bytes: result.layout.charge, inEntry: true })
        expect(result.resources.before.retained).toEqual(NOTHING_RETAINED)
        expect(result.resources.entry.retained.pins).toEqual({ [PROBE_TOPIC]: 1 })
        expect(result.resources.entry.retained.sends).toHaveLength(1)
        expect(result.resources.entry.totals.tracked).toBe(
          result.resources.before.totals.tracked + 1
        )
        expect(result.resources.after.retained).toEqual(NOTHING_RETAINED)
        expect(result.records).toBe(result.layout.count)
      })

      test('creates its topic without time retention, so the fixed old timestamps are not deleted', async () => {
        const world = await bound()

        await world.probe.closing({
          sender: sender(world),
          topic: PROBE_TOPIC,
          budgetMs: 1000,
        })
        world.probe.restore()

        expect(createTopic).toHaveBeenCalledTimes(1)
        expect(createTopic.mock.calls[0][0].config).toEqual(
          expect.arrayContaining([{ name: 'retention.ms', value: '-1' }])
        )
      })

      test('a signal that already aborted cancels the probe before any step', async () => {
        const world = await bound()
        const send = jest.fn()

        await expect(
          world.probe.closing({
            sender: { send },
            topic: PROBE_TOPIC,
            budgetMs: 1000,
            signal: AbortSignal.abort(new Error('The tail phase did not settle')),
          })
        ).rejects.toThrow('The tail phase did not settle')
        world.probe.restore()

        expect(createTopic).not.toHaveBeenCalled()
        expect(send).not.toHaveBeenCalled()
      })

      test.each([
        ['the quiet wait', 'quiet'],
        ['the topic creation', 'createTopic'],
      ])(
        'an expired closing probe issues no later topic, configuration or send when %s is stuck',
        async (_name, stuck) => {
          const gate = deferred()
          const controller = new AbortController()
          const admin = {
            connect: jest.fn(async () => {}),
            disconnect: jest.fn(async () => {}),
            describeConfigs: async () => ({
              resources: [
                { configEntries: [{ configName: 'max.message.bytes', configValue: '1048576' }] },
              ],
            }),
          }
          Kafka.mockImplementation(() => ({ admin: () => admin }))
          const send = jest.fn()
          const probe = new ProducerProbe({
            producer: { send },
            clientId: 'c',
            maxRequestSize: 1048576,
          })
          probe.quiet = stuck === 'quiet' ? () => gate.promise : async () => ({ capacity: 1048576 })
          if (stuck === 'createTopic') createTopic.mockImplementationOnce(() => gate.promise)

          const running = probe
            .closing({ topic: PROBE_TOPIC, budgetMs: 1000, signal: controller.signal })
            .catch((error) => error)
          await new Promise((resolve) => setImmediate(resolve))
          controller.abort(new Error('The tail phase did not settle'))
          gate.resolve({ capacity: 1048576 })
          const failure = await running

          expect(createTopic).toHaveBeenCalledTimes(stuck === 'createTopic' ? 1 : 0)
          expect(admin.connect).not.toHaveBeenCalled()
          expect(send).not.toHaveBeenCalled()
          expect(failure.message).toBe('The tail phase did not settle')
        }
      )

      test('a configuration read whose owner aborts during the connect issues no read and still closes its admin', async () => {
        const gate = deferred()
        const owner = new AbortController()
        const admin = {
          connect: jest.fn(() => gate.promise),
          disconnect: jest.fn(async () => {}),
          describeConfigs: jest.fn(),
        }
        Kafka.mockImplementation(() => ({ admin: () => admin }))

        const reading = topicConfig(['b:1'], PROBE_TOPIC, 'max.message.bytes', owner.signal).catch(
          (error) => error
        )
        owner.abort(new Error('The tail phase did not settle'))
        gate.resolve()
        const failure = await reading

        expect(failure.message).toBe('The tail phase did not settle')
        expect(admin.describeConfigs).not.toHaveBeenCalled()
        expect(admin.disconnect).toHaveBeenCalledTimes(1)
      })

      describe('a committed read-back', () => {
        /** A read_committed log that shows nothing for its first `empty` reads, then what was sent. */
        const committedLog = (empty) => {
          const reads = { count: 0 }
          rawLog.committedRecords.mockImplementation(({ records }) => records)
          rawLog.readLog.mockImplementation(async () => {
            reads.count += 1
            const visible = reads.count > empty ? stored : []
            return new Map([
              [
                0,
                {
                  records: visible.map(({ value }) => ({
                    value,
                    batchContext: { isControlBatch: false },
                  })),
                  abortedTransactions: [],
                },
              ],
            ])
          })
          return reads
        }

        const closeCommitted = (world, finish) =>
          world.probe.closing({
            sender: sender(world),
            topic: PROBE_TOPIC,
            finish,
            isolationLevel: ISOLATION_LEVEL.READ_COMMITTED,
            budgetMs: 1000,
          })

        test('is read again until it holds the probe records, the markers being written after the commit answers', async () => {
          const world = await bound()
          const reads = committedLog(2)
          const finish = jest.fn(async () => {})

          try {
            const result = await closeCommitted(world, finish)

            expect(result.records).toBe(result.layout.count)
          } finally {
            world.probe.restore()
          }
          expect(finish).toHaveBeenCalledTimes(1)
          expect(reads.count).toBe(3)
        })

        test('that never fills ends at its budget with the ordinary assertion on what it saw', async () => {
          const world = await bound({ readBackBudget: { attempts: 3, delay: 1 } })
          const reads = committedLog(Infinity)

          try {
            await expect(closeCommitted(world, async () => {})).rejects.toThrow(
              /toEqual[\s\S]*Received/
            )
          } finally {
            world.probe.restore()
          }
          expect(reads.count).toBe(3)
        })
      })

      test('refuses a probe that acquires a second time in a later turn', async () => {
        const world = await bound()

        await expect(
          world.probe.closing({
            sender: sender(world, { extra: true }),
            topic: PROBE_TOPIC,
            budgetMs: 1000,
          })
        ).rejects.toThrow(/Expected length: 1/)
        world.probe.restore()
      })

      test('refuses a probe whose only acquire was not made in the entry turn', async () => {
        const world = await bound()

        await expect(
          world.probe.closing({
            sender: sender(world, { late: true }),
            topic: PROBE_TOPIC,
            budgetMs: 1000,
          })
        ).rejects.toThrow(/"inEntry": true/)
        world.probe.restore()
      })
    })
  })

  describe('ProduceLedger and traceCalls', () => {
    test('records what a Produce call carried and what the broker answered', async () => {
      const original = Broker.prototype.produce
      Broker.prototype.produce = async function (request) {
        if (request.fail) throw new Error('lost')
        return [{ topicName: 't', partition: 0, errorCode: 0, baseOffset: '4', error: null }]
      }
      const ledger = new ProduceLedger().install()
      try {
        const topicData = [
          {
            topic: 't',
            partitions: [{ partition: 0, firstSequence: 3, messages: [{ value: 'x' }] }],
          },
        ]
        await Broker.prototype.produce({ producerId: 9n, producerEpoch: 1, acks: -1, topicData })
        await expect(
          Broker.prototype.produce({
            producerId: 9n,
            producerEpoch: 1,
            acks: -1,
            topicData,
            fail: true,
          })
        ).rejects.toThrow('lost')
      } finally {
        ledger.restore()
        Broker.prototype.produce = original
      }

      expect(ledger.of('t')).toHaveLength(2)
      expect(ledger.of('other')).toEqual([])
      expect(ledger.calls[0]).toMatchObject({
        producerId: '9',
        producerEpoch: 1,
        acks: -1,
        partitions: [{ topic: 't', partition: 0, firstSequence: 3, records: 1, first: 'x' }],
        results: [{ topic: 't', partition: 0, errorCode: 0, baseOffset: '4' }],
        error: null,
      })
      expect(ledger.calls[1].error.message).toBe('lost')
    })

    test('traceCalls keeps the calls describe selects and the error each one ended with', async () => {
      class Target {
        async run(value) {
          if (value === 'bad') throw Object.assign(new Error('refused'), { type: 'REFUSED' })
          return value
        }
      }
      const trace = traceCalls(Target.prototype, 'run', (value) =>
        value === 'skip' ? null : { value }
      )

      const target = new Target()
      await target.run('a')
      await target.run('skip')
      await expect(target.run('bad')).rejects.toThrow('refused')
      trace.restore()

      expect(trace.entries.map(({ value, error }) => [value, error?.type ?? null])).toEqual([
        ['a', null],
        ['bad', 'REFUSED'],
      ])
      expect(await target.run('after')).toBe('after')
      expect(trace.entries).toHaveLength(2)
    })
  })

  describe('the follower outage fixtures', () => {
    const states = [
      { topic: 'strict', partition: 0, leader: 1, replicas: [1, 2, 3] },
      { topic: 'relaxed', partition: 0, leader: 2, replicas: [3, 2, 1] },
    ]
    const pin = () => new TopologyPin({ partitions: states, follower: 3 })
    const metadata = ({ strict = {}, relaxed = {} } = {}) => [
      {
        name: 'strict',
        partitions: [{ partitionId: 0, leader: 1, replicas: [1, 2, 3], isr: [3, 1, 2], ...strict }],
      },
      {
        name: 'relaxed',
        partitions: [
          { partitionId: 0, leader: 2, replicas: [3, 2, 1], isr: [2, 1, 3], ...relaxed },
        ],
      },
    ]
    const snapshotOf = (changes) => pin().snapshot(metadata(changes))

    test('the timeline refuses a table the case cannot hold, and the default table fits', () => {
      const phases = Object.values(PHASE_BUDGET_MS).reduce((sum, ms) => sum + ms, 0)
      const steps = Object.values(RESTORE_STEP_MS).reduce((sum, ms) => sum + ms, 0)

      expect(CASE_BUDGET_MS).toBe(180000)
      expect(() => new CaseTimeline()).not.toThrow()
      expect(phases + JOIN_RESERVE_MS).toBeLessThan(CASE_BUDGET_MS)
      expect(Object.keys(PHASE_BUDGET_MS).length * STOP_GRACE_MS).toBeLessThanOrEqual(
        JOIN_RESERVE_MS
      )
      expect(steps).toBeLessThanOrEqual(PHASE_BUDGET_MS.restore)
      expect(() => new CaseTimeline({ phases: { setup: CASE_BUDGET_MS } })).toThrow(/do not fit/)
      expect(
        () => new CaseTimeline({ phases: { a: 1, b: 1, c: 1 }, reserveMs: 2 * STOP_GRACE_MS })
      ).toThrow(/do not fit/)
    })
    test('a pin refuses a follower that is no replica of a partition or leads one', () => {
      expect(() => new TopologyPin({ partitions: states, follower: 4 })).toThrow(
        /Node 4 is not a replica of strict-0/
      )
      expect(() => new TopologyPin({ partitions: states, follower: 1 })).toThrow(
        /Node 1 leads strict-0/
      )
    })

    test('snapshot reads the pinned partitions in pin order with sorted replica sets and ISRs', () => {
      expect(snapshotOf()).toEqual([
        { topic: 'strict', partition: 0, leader: 1, replicas: [1, 2, 3], isr: [1, 2, 3] },
        { topic: 'relaxed', partition: 0, leader: 2, replicas: [1, 2, 3], isr: [1, 2, 3] },
      ])
      expect(() => pin().snapshot(metadata().slice(0, 1))).toThrow(/relaxed-0 is missing/)
    })

    test('whole needs every ISR to hold the whole replica set', () => {
      expect(() => pin().whole(snapshotOf())).not.toThrow()
      expect(() => pin().whole(snapshotOf({ relaxed: { isr: [1, 2] } }))).toThrow(
        /relaxed-0 is not in full sync/
      )
    })

    test('establishing is false while the ISR is whole and true once it is exactly the survivors', () => {
      expect(pin().establishing(snapshotOf())).toBe(false)
      expect(pin().establishing(snapshotOf({ strict: { isr: [1, 2] } }))).toBe(false)
      expect(
        pin().establishing(snapshotOf({ strict: { isr: [1, 2] }, relaxed: { isr: [2, 1] } }))
      ).toBe(true)
    })

    test.each([
      ['a leader that moved', { strict: { leader: 2, isr: [1, 2] } }],
      ['a replica set that changed', { strict: { replicas: [1, 2], isr: [1, 2] } }],
      ['a survivor that left the ISR with the follower', { strict: { isr: [1] } }],
      ['the leader alone left in the ISR', { relaxed: { isr: [2] } }],
      ['a node outside the replicas in the ISR', { strict: { isr: [1, 2, 4] } }],
    ])('establishing refuses %s', (_name, changes) => {
      expect(() => pin().establishing(snapshotOf(changes))).toThrow(/drifted|moved unexpectedly/)
    })

    test('holding needs exactly the two survivors, the pinned leaders and the pinned replicas', () => {
      const held = { strict: { isr: [2, 1] }, relaxed: { isr: [1, 2] } }

      expect(() => pin().holding(snapshotOf(held))).not.toThrow()
      expect(() => pin().holding(snapshotOf())).toThrow(/not held by its survivors/)
      expect(() => pin().holding(snapshotOf({ ...held, relaxed: { isr: [2] } }))).toThrow(
        /relaxed-0 is not held by its survivors/
      )
      expect(() =>
        pin().holding(snapshotOf({ ...held, strict: { isr: [1, 2], leader: 2 } }))
      ).toThrow(/strict-0 drifted/)
    })

    /** A bouncer whose boundary is a log, with the failures a case asks for. */
    const fakeBouncer = ({ stopFails = false, startFails = 0, hold = null } = {}) => {
      const log = []
      let starts = 0
      return {
        log,
        down: new Set(),
        nodes: { kafka3: { host: 'unused', port: 0 } },
        assertAllUp: async () => log.push('assertAllUp'),
        run: async (operation, service) => {
          log.push(`${operation} ${service}`)
          if (operation === 'stop' && stopFails) throw new Error('stop failed')
          if (operation === 'start') {
            starts += 1
            if (hold) await hold
            if (starts <= startFails) throw new Error('start failed')
          }
        },
        probe: async () => log.push('probe') > 0,
      }
    }
    /** An admin whose metadata shows every partition of the topics in full sync. */
    const fullAdmin = (log) => ({
      listTopics: async () => [],
      fetchTopicMetadata: async ({ topics }) => {
        log.push(['metadata', topics])
        return {
          topics: topics.map((name) => ({
            name,
            partitions: [{ partitionId: 0, replicas: [1, 2, 3], isr: [3, 1, 2] }],
          })),
        }
      },
    })
    const outageOf = (bouncer, options = {}) =>
      new FollowerOutage({
        bouncer,
        admin: fullAdmin(bouncer.log),
        topics: ['a', 'b'],
        ...options,
      })

    test('an outage stops the follower, runs the workload, restores it with bounded waits and owns nothing', async () => {
      const bouncer = fakeBouncer()
      const outage = outageOf(bouncer)

      const result = await outage.run('kafka3', async () => {
        expect(outage.open).toBe(true)
        expect(bouncer.down.has('kafka3')).toBe(true)
        return 'workload'
      })

      expect(result).toBe('workload')
      expect(bouncer.log).toEqual([
        'assertAllUp',
        'stop kafka3',
        'start kafka3',
        'probe',
        ['metadata', ['a', 'b']],
      ])
      expect(outage.open).toBe(false)
      expect(bouncer.down.size).toBe(0)
      expect(Object.keys(outage.chronology)).toEqual([
        'stoppedAt',
        'startedAt',
        'reachableAt',
        'fullIsrAt',
      ])
      expect(outage.timeline.timings().map(({ name }) => name)).toEqual(['stop', 'restore'])
      await expect(outage.restore()).resolves.toBeNull()
    })

    test('a failed workload still restores the follower and its error stays the primary one', async () => {
      const bouncer = fakeBouncer({ startFails: 1 })
      const outage = outageOf(bouncer)
      const primary = new Error('workload failed')

      const caught = await outage
        .run('kafka3', async () => {
          throw primary
        })
        .catch((error) => error)

      expect(caught).toBe(primary)
      expect(caught.cleanupErrors.map(({ message }) => message)).toEqual(['start failed'])
      expect(outage.open).toBe(true)
      expect(bouncer.down.has('kafka3')).toBe(true)
    })

    test('a failed stop still restores the follower the stop may have taken down', async () => {
      const bouncer = fakeBouncer({ stopFails: true })
      const outage = outageOf(bouncer)

      await expect(outage.run('kafka3', async () => 'never')).rejects.toThrow('stop failed')

      expect(bouncer.log.filter((entry) => typeof entry === 'string')).toEqual([
        'assertAllUp',
        'stop kafka3',
        'start kafka3',
        'probe',
      ])
      expect(outage.open).toBe(false)
    })

    test('a refused outage owns no follower and starts none', async () => {
      const bouncer = fakeBouncer()
      bouncer.assertAllUp = async () => {
        throw new Error('kafka2 does not answer')
      }
      const outage = outageOf(bouncer)

      await expect(outage.run('kafka3', async () => 'never')).rejects.toThrow('does not answer')

      expect(outage.open).toBe(false)
      expect(bouncer.log).toEqual([])
      expect(bouncer.down.size).toBe(0)
    })

    test('callers share one restore attempt, and a failed attempt is tried again', async () => {
      let release
      const hold = new Promise((resolve) => (release = resolve))
      const bouncer = fakeBouncer({ startFails: 1, hold })
      const outage = outageOf(bouncer)
      await outage.stop('kafka3')

      const first = outage.restore()
      const second = outage.restore()
      release()
      const failures = await Promise.all([first, second])

      expect(first).toBe(second)
      expect(failures.map((failure) => failure?.message)).toEqual(['start failed', 'start failed'])
      expect(bouncer.log.filter((entry) => entry === 'start kafka3')).toHaveLength(1)
      expect(outage.open).toBe(true)
      await expect(outage.restore()).resolves.toBeNull()
      expect(bouncer.log.filter((entry) => entry === 'start kafka3')).toHaveLength(2)
      expect(outage.open).toBe(false)
    })

    test('a restore that fails taints the run, and one that succeeds leaves it clean', async () => {
      const fine = outageOf(fakeBouncer())
      await fine.stop('kafka3')
      await expect(fine.restore()).resolves.toBeNull()
      expect(liveRun.tainted).toBe(false)

      const failing = outageOf(fakeBouncer({ startFails: 1 }))
      await failing.stop('kafka3')
      const failure = await failing.restore()

      expect(failure.message).toBe('start failed')
      expect(failing.open).toBe(true)
      expect(liveRun.tainted).toBe(true)
      expect(() => liveRun.assertClean()).toThrow(/may have left work running.*start failed/)
    })

    describe('work that outlives its owner', () => {
      beforeEach(() => jest.useFakeTimers())
      afterEach(() => jest.useRealTimers())

      const turn = async () => {
        for (let i = 0; i < 30; i += 1) await Promise.resolve()
      }
      /** A bouncer whose preflight, stop and start can be held, counting the starts at once. */
      const heldBouncer = ({ preflight, stop, start } = {}) => {
        const log = []
        const starts = { active: 0, peak: 0, total: 0 }
        return {
          log,
          starts,
          down: new Set(),
          nodes: { kafka3: { host: 'unused', port: 0 } },
          assertAllUp: async () => {
            await preflight
          },
          run: async (operation) => {
            log.push(operation)
            if (operation === 'stop') await stop
            if (operation === 'start') {
              starts.active += 1
              starts.total += 1
              starts.peak = Math.max(starts.peak, starts.active)
              await start
              starts.active -= 1
            }
          },
          probe: async () => log.push('reachable') > 0,
        }
      }
      const timed = (bouncer, phases, options) =>
        new FollowerOutage({
          bouncer,
          admin: {
            listTopics: async () => [],
            fetchTopicMetadata: async () => {
              bouncer.log.push('isr')
              return { topics: [] }
            },
          },
          topics: [],
          phases,
          ...options,
        })

      test('a restore whose readiness read never answers ends inside its phase, shared by every caller, and the outage issues nothing more', async () => {
        const gate = deferred()
        const log = []
        const admin = {
          listTopics: async () => [],
          fetchTopicMetadata: async () => {
            log.push('metadata')
            return gate.promise
          },
        }
        const bouncer = new BrokerBouncer({
          run: async (operation) => log.push(operation),
          answers: async () => true,
          nodes: { kafka3: { host: 'unused', port: 0 } },
        })
        const outage = new FollowerOutage({
          bouncer,
          admin,
          topics: ['t'],
          phases: { stop: 25, restore: 35 },
        })
        await outage.stop('kafka3')

        const restoring = watch(outage.restore())
        const hook = outage.restore()
        await turn()
        await jest.advanceTimersByTimeAsync(60000)

        expect(restoring.state).toBe('fulfilled')
        expect(restoring.value.message).toBe('The restore phase did not settle within 35 ms')
        expect(await hook).toBe(restoring.value)
        expect(outage.open).toBe(true)
        expect(log).toEqual(['stop', 'start', 'metadata'])
        gate.resolve({
          topics: [
            { name: 't', partitions: [{ partitionId: 0, replicas: [1, 2, 3], isr: [1, 2, 3] }] },
          ],
        })
        await jest.advanceTimersByTimeAsync(60000)
        expect(log).toEqual(['stop', 'start', 'metadata'])
        expect(outage.open).toBe(true)
      })

      test('a preflight that outlasts the stop phase cannot issue the stop afterwards', async () => {
        const gate = deferred()
        const bouncer = heldBouncer({ preflight: gate.promise })
        const outage = timed(bouncer, { stop: 10, restore: 10 })
        const running = watch(outage.run('kafka3', async () => bouncer.log.push('workload')))

        await jest.advanceTimersByTimeAsync(10 + STOP_GRACE_MS)

        expect(running.state).toBe('rejected')
        expect(running.value.message).toBe('The stop phase did not settle within 10 ms')
        await expect(outage.restore()).resolves.toBeNull()
        gate.resolve()
        await turn()
        expect(bouncer.log).toEqual([])
        expect(outage.open).toBe(false)
        expect(bouncer.down.size).toBe(0)
      })

      test('a stop issued before its phase ended is finished before the restore starts', async () => {
        const gate = deferred()
        const bouncer = heldBouncer({ stop: gate.promise })
        const outage = timed(bouncer, { stop: 10, restore: 1000 })
        const outcome = watch(outage.run('kafka3', async () => bouncer.log.push('workload')))

        await jest.advanceTimersByTimeAsync(10 + STOP_GRACE_MS)

        expect(bouncer.log).toEqual(['stop'])
        expect(outcome.state).toBe('pending')
        gate.resolve()
        await jest.advanceTimersByTimeAsync(0)
        await turn()
        expect(outcome.state).toBe('rejected')
        expect(outcome.value.message).toBe('The stop phase did not settle within 10 ms')
        expect(bouncer.log).toEqual(['stop', 'start', 'reachable', 'isr'])
        expect(outage.open).toBe(false)
      })

      test('a restore that outlasts its phase stays one attempt, and the next waits for the start it left running', async () => {
        const gate = deferred()
        const bouncer = heldBouncer({ start: gate.promise })
        const outage = timed(bouncer, { stop: 1000, restore: 10 })
        await outage.stop('kafka3')

        const first = outage.restore()
        await jest.advanceTimersByTimeAsync(10)
        const second = outage.restore()
        await turn()

        expect(second).toBe(first)
        expect(bouncer.starts).toMatchObject({ total: 1, peak: 1 })
        await jest.advanceTimersByTimeAsync(STOP_GRACE_MS)
        const failures = await Promise.all([first, second])
        expect(failures.map((failure) => failure?.message)).toEqual(
          Array(2).fill('The restore phase did not settle within 10 ms')
        )
        expect(outage.open).toBe(true)
        const third = watch(outage.restore())
        await jest.advanceTimersByTimeAsync(10 + STOP_GRACE_MS)
        expect(third.state).toBe('fulfilled')
        expect(third.value.message).toBe('The restore phase did not settle within 10 ms')
        expect(bouncer.starts).toMatchObject({ total: 1, peak: 1 })
        gate.resolve()
        await turn()
        await expect(outage.restore()).resolves.toBeNull()
        expect(bouncer.starts).toMatchObject({ total: 2, peak: 1 })
        expect(outage.open).toBe(false)
      })

      test('every step of the restore is bounded by its own budget and issues nothing after it', async () => {
        const gate = deferred()
        const steps = { start: 100, reachable: 50, isr: 50 }
        const stuckStart = heldBouncer({ start: gate.promise })
        const slowStart = timed(stuckStart, { stop: 1000, restore: 5000 }, { steps })
        await slowStart.stop('kafka3')

        const starting = watch(slowStart.restore())
        await jest.advanceTimersByTimeAsync(100 + STOP_GRACE_MS)

        expect(starting.value.message).toBe('The start of kafka3 did not settle within 100 ms')
        expect(stuckStart.log).toEqual(['stop', 'start'])

        const silent = heldBouncer()
        silent.probe = async () => silent.log.push('reachable') < 0
        const unreachable = timed(silent, { stop: 1000, restore: 5000 }, { steps })
        await unreachable.stop('kafka3')
        const waiting = watch(unreachable.restore())
        await jest.advanceTimersByTimeAsync(50 + STOP_GRACE_MS)

        expect(waiting.value.message).toBe(
          'The wait for kafka3 to answer did not settle within 50 ms'
        )
        expect(silent.log).toEqual(['stop', 'start', 'reachable'])
        expect(unreachable.open).toBe(true)
      })

      test('a setup that outlasts its phase issues no later operation, whatever its steps answer', async () => {
        const timeline = new CaseTimeline({ phases: { setup: 15 } })
        const gate = deferred()
        const log = []
        const admin = {
          connect: () => gate.promise,
          createTopics: async () => log.push('createTopics'),
          disconnect: async () => log.push('disconnect'),
        }
        const running = watch(
          timeline.run('setup', async (signal) => {
            await bounded(signal, Infinity, () => admin.connect())
            await bounded(signal, Infinity, () => admin.createTopics())
          })
        )

        await jest.advanceTimersByTimeAsync(15 + STOP_GRACE_MS)

        expect(running.state).toBe('rejected')
        expect(running.value.message).toBe('The setup phase did not settle within 15 ms')
        await admin.disconnect()
        gate.resolve()
        await turn()
        expect(log).toEqual(['disconnect'])
      })

      test('a phase fails at its deadline however long its poll evaluation takes, and polls no more', async () => {
        const timeline = new CaseTimeline({ phases: { shrink: 50 } })
        const gate = deferred()
        let evaluations = 0

        const running = watch(
          timeline.run('shrink', (signal) =>
            pollUntil(signal, Infinity, () => {
              evaluations += 1
              return gate.promise
            })
          )
        )
        await jest.advanceTimersByTimeAsync(50 + STOP_GRACE_MS)
        gate.resolve(true)
        await jest.advanceTimersByTimeAsync(5000)

        expect(running.state).toBe('rejected')
        expect(running.value.message).toBe('The shrink phase did not settle within 50 ms')
        expect(evaluations).toBe(1)
        expect(timeline.timings()).toEqual([
          { name: 'shrink', budgetMs: 50, elapsedMs: 50 + STOP_GRACE_MS },
        ])
      })

      test('a case whose every phase hangs ends inside the case budget, each phase cut at its own', async () => {
        const timeline = new CaseTimeline()
        const stack = new CleanupStack()
        stack.add('stuck', () => new Promise(() => {}))
        const startedAt = performance.now()
        const phases = ['setup', 'stop', 'shrink', 'measure', 'restore', 'tail']

        const running = watch(
          (async () => {
            const failures = []
            for (const name of phases) {
              const signal = name === 'restore' ? NEVER : undefined
              await timeline
                .run(name, () => new Promise(() => {}), signal)
                .catch((error) => failures.push(error))
            }
            timeline.abort()
            const cleanup = await stack
              .run(async () => 'done', { budgetMs: PHASE_BUDGET_MS.cleanup })
              .catch((error) => error)
            return { failures, cleanup }
          })()
        )
        await jest.advanceTimersByTimeAsync(CASE_BUDGET_MS)

        expect(running.state).toBe('fulfilled')
        expect(running.value.failures.map(({ message }) => message)).toEqual(
          phases.map(
            (name) => `The ${name} phase did not settle within ${PHASE_BUDGET_MS[name]} ms`
          )
        )
        expect(running.value.cleanup.errors).toHaveLength(1)
        expect(timeline.timings().map(({ elapsedMs }) => elapsedMs)).toEqual(
          phases.map((name) => PHASE_BUDGET_MS[name] + STOP_GRACE_MS)
        )
        expect(performance.now() - startedAt).toBeLessThanOrEqual(CASE_BUDGET_MS)
      })

      test('cleanup ends inside its aggregate budget with every cleanup attempted and every cleanup that cooperates stopped', async () => {
        const stack = new CleanupStack()
        const gates = [deferred(), deferred(), deferred(), deferred()]
        const started = []
        const log = []
        let active = 0
        gates.forEach((gate, index) =>
          stack.add(String(index), async (signal) => {
            started.push(index)
            active += 1
            try {
              await Promise.race([
                gate.promise,
                new Promise((resolve) => signal?.addEventListener('abort', resolve)),
              ])
              if (signal?.aborted) return
              log.push(index)
            } finally {
              active -= 1
            }
          })
        )
        const startedAt = performance.now()
        const atReturn = {}
        const running = watch(
          stack.closeAll({ budgetMs: 15000 }).then((failures) => {
            Object.assign(atReturn, {
              ms: performance.now() - startedAt,
              failures: failures.length,
              active,
            })
            return failures
          })
        )

        await jest.advanceTimersByTimeAsync(15000)

        expect(running.state).toBe('fulfilled')
        expect(atReturn.ms).toBeLessThanOrEqual(15000)
        expect(atReturn).toMatchObject({ failures: 4, active: 0 })
        expect(started).toEqual([3, 2, 1, 0])
        gates.forEach((gate) => gate.resolve())
        await turn()
        expect(log).toEqual([])
      })

      test('cleanup stays inside its budget when every cleanup ignores its abort, and the cleanups behind a stuck one still run', async () => {
        const order = []
        const stack = new CleanupStack()
        stack.add('last', () => order.push('last'))
        stack.add('stuck', () => new Promise(() => {}))
        stack.add('first', () => order.push('first'))
        const startedAt = performance.now()
        let finishedAt = null

        const running = watch(
          stack
            .run(async () => 'done', { budgetMs: 1000 })
            .finally(() => {
              finishedAt = performance.now() - startedAt
            })
        )
        await jest.advanceTimersByTimeAsync(1000)

        expect(order).toEqual(['first', 'last'])
        expect(finishedAt).toBeLessThanOrEqual(1000)
        expect(running.state).toBe('rejected')
        expect(running.value.errors.map(({ message }) => message)).toEqual([
          expect.stringMatching(/^stuck: The cleanup did not settle within \d+ ms$/),
        ])
      })

      test('a cleanup stack with no budget waits as long as its cleanups take', async () => {
        const stack = new CleanupStack()
        stack.add('slow', () => new Promise((resolve) => setTimeout(resolve, 60000)))

        const running = watch(stack.run(async () => 'done'))
        await jest.advanceTimersByTimeAsync(60000)

        expect(running).toMatchObject({ state: 'fulfilled', value: 'done' })
      })
    })
  })

  describe('bounded work', () => {
    beforeEach(() => jest.useFakeTimers())
    afterEach(() => jest.useRealTimers())

    const turn = async () => {
      for (let i = 0; i < 30; i += 1) await Promise.resolve()
    }

    test('returns what the operation returned and leaves no timer behind', async () => {
      await expect(bounded(NEVER, 1000, async () => 7)).resolves.toBe(7)
      await expect(bounded(NEVER, Infinity, () => 8)).resolves.toBe(8)

      expect(jest.getTimerCount()).toBe(0)
    })

    test('passes the failure of the operation on at once, aborting nothing before it', async () => {
      let scope
      const running = watch(
        bounded(NEVER, 1000, async (own) => {
          scope = own
          throw new Error('boom')
        })
      )
      await turn()

      expect(running.state).toBe('rejected')
      expect(running.value.message).toBe('boom')
      expect(jest.getTimerCount()).toBe(0)
      expect(scope.aborted).toBe(true)
    })

    test('does not start an operation under an owner that aborted', async () => {
      const op = jest.fn()

      await expect(bounded(AbortSignal.abort(new Error('owner gone')), 1000, op)).rejects.toThrow(
        'owner gone'
      )

      expect(op).not.toHaveBeenCalled()
    })

    test('aborts the scope of an operation that outlasts its time, gives it the grace, and rejects', async () => {
      const gate = deferred()
      let scope
      const running = watch(
        bounded(
          NEVER,
          100,
          (own) => {
            scope = own
            return gate.promise
          },
          { label: 'The wait' }
        )
      )

      await jest.advanceTimersByTimeAsync(100)

      expect(scope.aborted).toBe(true)
      expect(running.state).toBe('pending')
      await jest.advanceTimersByTimeAsync(STOP_GRACE_MS)
      expect(running.state).toBe('rejected')
      expect(running.value.message).toBe('The wait did not settle within 100 ms')
      gate.resolve('late')
      await turn()
      expect(running.state).toBe('rejected')
    })

    test('rejects as soon as the aborted operation has stopped, and drops an answer that is late', async () => {
      const stopped = bounded(NEVER, 100, (scope) => delay(scope, 1000))
      const late = deferred()
      const answering = bounded(NEVER, 100, () => late.promise)
      const watching = watch(answering)
      const watched = watch(stopped)

      await jest.advanceTimersByTimeAsync(100)
      expect(watched.state).toBe('rejected')
      expect(watched.value.message).toBe('The operation did not settle within 100 ms')
      expect(jest.getTimerCount()).toBe(1)
      late.resolve('in time for the grace')
      await jest.advanceTimersByTimeAsync(0)
      await turn()

      expect(watching.state).toBe('rejected')
      expect(watching.value.message).toBe('The operation did not settle within 100 ms')
      expect(jest.getTimerCount()).toBe(0)
    })

    test('rejects with the reason of an owner that aborts, and tells the operation', async () => {
      const owner = new AbortController()
      const gate = deferred()
      let scope
      const running = watch(
        bounded(owner.signal, Infinity, (own) => {
          scope = own
          return gate.promise
        })
      )

      owner.abort(new Error('case over'))
      await jest.advanceTimersByTimeAsync(STOP_GRACE_MS)

      expect(scope.aborted).toBe(true)
      expect(running.state).toBe('rejected')
      expect(running.value.message).toBe('case over')
    })

    test('delay waits its time, and a delay whose owner aborts ends at once with no timer', async () => {
      const owner = new AbortController()
      const waited = watch(delay(NEVER, 100))
      const cut = watch(delay(owner.signal, 100000))

      await jest.advanceTimersByTimeAsync(100)
      expect(waited.state).toBe('fulfilled')
      owner.abort(new Error('case over'))
      await turn()

      expect(cut.state).toBe('rejected')
      expect(jest.getTimerCount()).toBe(0)
    })

    test('pollUntil checks until it is true, pausing between the checks', async () => {
      let checks = 0

      const polling = watch(pollUntil(NEVER, 10000, () => ++checks >= 3, { delayMs: 250 }))
      await jest.advanceTimersByTimeAsync(0)
      expect(checks).toBe(1)
      await jest.advanceTimersByTimeAsync(250)
      expect(checks).toBe(2)
      await jest.advanceTimersByTimeAsync(250)

      expect(polling.state).toBe('fulfilled')
      expect(checks).toBe(3)
    })

    test("pollUntil rejects with its label at its budget and checks no more, and what a check throws is the caller's", async () => {
      let checks = 0
      const polling = watch(
        pollUntil(NEVER, 1000, () => (checks += 1) < 0, { delayMs: 300, label: 'The wait' })
      )

      await jest.advanceTimersByTimeAsync(1000 + STOP_GRACE_MS)

      expect(polling.state).toBe('rejected')
      expect(polling.value.message).toBe('The wait did not settle within 1000 ms')
      expect(checks).toBe(4)
      await jest.advanceTimersByTimeAsync(10000)
      expect(checks).toBe(4)
      await expect(
        pollUntil(NEVER, 1000, () => {
          throw new Error('drifted')
        })
      ).rejects.toThrow('drifted')
    })

    test('pollTolerating treats what a check throws as not yet and polls on until the check is true', async () => {
      let checks = 0
      const polling = watch(
        pollTolerating(
          NEVER,
          10000,
          () => {
            if ((checks += 1) < 3) throw new Error(`not yet ${checks}`)
            return true
          },
          { delayMs: 250 }
        )
      )

      await jest.advanceTimersByTimeAsync(0)
      expect(checks).toBe(1)
      expect(polling.state).toBe('pending')
      await jest.advanceTimersByTimeAsync(250)
      expect(checks).toBe(2)
      expect(polling.state).toBe('pending')
      await jest.advanceTimersByTimeAsync(250)

      expect(polling.state).toBe('fulfilled')
      expect(checks).toBe(3)
    })

    test('pollTolerating names the last failure its checks threw in its time-out message and keeps it as the cause', async () => {
      let checks = 0
      const thrown = []
      const polling = watch(
        pollTolerating(
          NEVER,
          1000,
          () => {
            checks += 1
            const error = new Error(`refused ${checks}`)
            thrown.push(error)
            throw error
          },
          { delayMs: 300, label: 'Broker 3' }
        )
      )

      await jest.advanceTimersByTimeAsync(1000 + STOP_GRACE_MS)

      expect(polling.state).toBe('rejected')
      expect(checks).toBe(4)
      expect(polling.value.message).toBe(
        'Broker 3 did not settle within 1000 ms; last error: Error: refused 4'
      )
      expect(polling.value.cause).toBe(thrown[3])
    })

    test('pollTolerating cuts a long last failure message to 150 characters', async () => {
      const polling = watch(
        pollTolerating(
          NEVER,
          1000,
          () => {
            throw new Error('x'.repeat(400))
          },
          { delayMs: 300, label: 'Broker 3' }
        )
      )

      await jest.advanceTimersByTimeAsync(1000 + STOP_GRACE_MS)

      expect(polling.state).toBe('rejected')
      expect(polling.value.message).toBe(
        `Broker 3 did not settle within 1000 ms; last error: Error: ${'x'.repeat(150)}`
      )
    })

    test('pollTolerating adds no last failure to the time-out of checks that only answered false', async () => {
      const polling = watch(pollTolerating(NEVER, 1000, () => false, { delayMs: 300 }))

      await jest.advanceTimersByTimeAsync(1000 + STOP_GRACE_MS)

      expect(polling.state).toBe('rejected')
      expect(polling.value.message).toBe('The wait did not settle within 1000 ms')
      expect(polling.value.cause).toBeUndefined()
    })

    test('pollTolerating does not take the abort that ended a check for a failure of that check', async () => {
      const polling = watch(
        pollTolerating(
          NEVER,
          1000,
          (scope) =>
            new Promise((resolve, reject) =>
              scope.addEventListener('abort', () => reject(scope.reason), { once: true })
            )
        )
      )

      await jest.advanceTimersByTimeAsync(1000 + STOP_GRACE_MS)

      expect(polling.state).toBe('rejected')
      expect(polling.value.message).toBe('The wait did not settle within 1000 ms')
      expect(polling.value.cause).toBeUndefined()
    })

    test('isrIsFull reads the workload and the internal topics and tells whether every ISR is whole', async () => {
      const reads = []
      const admin = (isr) => ({
        listTopics: async () => ['__consumer_offsets', 'other'],
        fetchTopicMetadata: async ({ topics }) => {
          reads.push(topics)
          return {
            topics: [{ name: 't', partitions: [{ partitionId: 0, replicas: [1, 2, 3], isr }] }],
          }
        },
      })

      await expect(isrIsFull(NEVER, admin([1, 2, 3]), ['t'])).resolves.toBe(true)
      await expect(isrIsFull(NEVER, admin([1, 2]), ['t'])).resolves.toBe(false)

      expect(reads[0]).toEqual(['t', '__consumer_offsets'])
    })
  })

  describe('the case wrapper', () => {
    beforeEach(() => jest.useFakeTimers())
    afterEach(() => jest.useRealTimers())

    test('a case gets its scope and its row, returns what the body returned and marks itself admitted', async () => {
      const body = jest.fn(async (scope, kind, copies) => `${kind}:${copies}`)

      await expect(liveCase(1000, body)('idempotent', 1)).resolves.toBe('idempotent:1')

      expect(body).toHaveBeenCalledWith(expect.any(AbortSignal), 'idempotent', 1)
      expect(liveRun.takeAdmission()).toBe(true)
      expect(liveRun.takeAdmission()).toBe(false)
      expect(liveRun.tainted).toBe(false)
    })

    test('a tainted run refuses the case before its body starts and does not mark it admitted', async () => {
      liveRun.admit()
      liveRun.taint(new Error('earlier escaped work'))
      const body = jest.fn()

      await expect(liveCase(1000, body)()).rejects.toThrow(
        /earlier case may have left work running.*earlier escaped work/
      )

      expect(body).not.toHaveBeenCalled()
      expect(liveRun.takeAdmission()).toBe(false)
    })

    test('an admitted case that taints the run is still admitted for its hooks', async () => {
      await liveCase(1000, async () => liveRun.taint(new Error('this case')))()

      expect(liveRun.tainted).toBe(true)
      expect(liveRun.takeAdmission()).toBe(true)
    })

    test('a case that outlasts its budget rejects, taints the run and aborts its scope', async () => {
      let scope
      const running = watch(
        liveCase(100, (own) => {
          scope = own
          return new Promise(() => {})
        })()
      )

      await jest.advanceTimersByTimeAsync(100 + STOP_GRACE_MS)

      expect(running.state).toBe('rejected')
      expect(running.value.message).toBe('The case did not settle within 100 ms')
      expect(scope.aborted).toBe(true)
      refusesBecause('The case did not settle within 100 ms')
    })

    test('the scope of a case is aborted when its body finishes', async () => {
      let scope

      await liveCase(1000, async (own) => {
        scope = own
      })()

      expect(scope.aborted).toBe(true)
    })

    test('the scope of a case is aborted when its body fails', async () => {
      let scope

      await expect(
        liveCase(1000, async (own) => {
          scope = own
          throw new Error('boom')
        })()
      ).rejects.toThrow('boom')

      expect(scope.aborted).toBe(true)
      expect(liveRun.tainted).toBe(false)
    })

    test('the Jest timeout of a case is its budget plus a join reserve longer than a stop grace', () => {
      expect(CASE_JOIN_MS).toBeGreaterThan(STOP_GRACE_MS)
      expect(caseTimeout(60000)).toBe(60000 + CASE_JOIN_MS)
    })
  })

  describe('the taint of a run', () => {
    beforeEach(() => jest.useFakeTimers())
    afterEach(() => jest.useRealTimers())

    const turn = async () => {
      for (let i = 0; i < 60; i += 1) await Promise.resolve()
    }
    test('a run starts clean, keeps the first cause it was tainted with, and refuses with it', () => {
      const run = new LiveRun()
      expect(run.tainted).toBe(false)
      expect(() => run.assertClean()).not.toThrow()

      const first = new Error('first cause')
      run.taint(first)
      run.taint(new Error('second cause'))

      expect(run.tainted).toBe(true)
      expect(() => run.assertClean()).toThrow(/may have left work running.*first cause/)
      let refusal
      try {
        run.assertClean()
      } catch (error) {
        refusal = error
      }
      expect(refusal.cause).toBe(first)
      expect(refusal.message).not.toMatch(/second cause/)
      run.reset()
      expect(run.tainted).toBe(false)
    })

    test('an operation that finishes, or fails on its own, leaves the run clean', async () => {
      await bounded(NEVER, 1000, async () => 1)
      await expect(
        bounded(NEVER, 1000, async () => {
          throw new Error('boom')
        })
      ).rejects.toThrow('boom')
      await expect(
        pollUntil(NEVER, 1000, () => {
          throw new Error('drifted')
        })
      ).rejects.toThrow('drifted')

      expect(liveRun.tainted).toBe(false)
      expect(() => liveRun.assertClean()).not.toThrow()
    })

    test.each([
      [
        'stops inside its grace',
        (scope) => new Promise((resolve) => scope.addEventListener('abort', resolve)),
      ],
      ['ignores its abort', () => new Promise(() => {})],
    ])('a bound that expires taints the run when the operation %s', async (_name, op) => {
      const running = watch(bounded(NEVER, 100, op))
      await jest.advanceTimersByTimeAsync(100 + STOP_GRACE_MS)

      expect(running.state).toBe('rejected')
      expect(liveRun.tainted).toBe(true)
      refusesBecause('The operation did not settle within 100 ms')
    })

    test('an owner that aborts taints the run', async () => {
      const owner = new AbortController()
      const running = watch(bounded(owner.signal, Infinity, () => new Promise(() => {})))

      owner.abort(new Error('The tail phase did not settle'))
      await jest.advanceTimersByTimeAsync(STOP_GRACE_MS)

      expect(running.state).toBe('rejected')
      refusesBecause('The tail phase did not settle')
    })

    test('a topic creation still running after its bound taints the run, and the next case refuses to start', async () => {
      const gate = deferred()
      const admin = {
        connect: () => gate.promise,
        createTopics: jest.fn(async () => {
          throw new Error('stop before propagation')
        }),
        disconnect: jest.fn(async () => {}),
      }
      Kafka.mockImplementation(() => ({ admin: () => admin }))
      const { createTopic: actual } = jest.requireActual('testHelpers')

      const result = watch(bounded(NEVER, 10, () => actual({ topic: 'review' })))
      await jest.advanceTimersByTimeAsync(10 + STOP_GRACE_MS)

      expect(result.state).toBe('rejected')
      refusesBecause('The operation did not settle within 10 ms')
      gate.resolve()
      await turn()
    })

    test('node probes still running after the outage and its restore returned taint the run', async () => {
      const gate = deferred()
      const log = []
      const bouncer = new BrokerBouncer({
        run: async (operation) => log.push(operation),
        answers: async (node) => {
          log.push(node.port)
          return node.port === 1 ? gate.promise : true
        },
        nodes: { kafka1: { port: 1 }, kafka2: { port: 2 }, kafka3: { port: 3 } },
      })
      const outage = new FollowerOutage({
        bouncer,
        admin: {},
        topics: [],
        phases: { stop: 10, restore: 10 },
      })

      const result = watch(outage.run('kafka3', async () => log.push('workload')))
      await jest.advanceTimersByTimeAsync(10 + STOP_GRACE_MS)

      expect(result.state).toBe('rejected')
      await expect(outage.restore()).resolves.toBeNull()
      expect(log).toEqual([1])
      refusesBecause('The stop phase did not settle within 10 ms')
      gate.resolve(true)
      await turn()
    })

    test('a raw read still running after its bound taints the run', async () => {
      const gate = deferred()
      const log = []
      const seed = {
        metadata: () => gate.promise,
        disconnect: async () => log.push('seed disconnected'),
      }
      const leader = {
        listOffsets: async () => {
          log.push('listOffsets')
          return { responses: [{ partitions: [{ offset: '0' }] }] }
        },
        disconnect: async () => log.push('leader disconnected'),
      }
      const { readLog } = jest.requireActual('../../../testHelpers/rawLog')

      const result = watch(
        bounded(NEVER, 10, () =>
          readLog({
            brokers: ['unused:1'],
            topic: 'review',
            open: async ({ port }) => {
              log.push(`open ${port}`)
              return port === 1 ? seed : leader
            },
          })
        )
      )
      await jest.advanceTimersByTimeAsync(10 + STOP_GRACE_MS)

      expect(result.state).toBe('rejected')
      expect(log).toEqual(['open 1'])
      refusesBecause('The operation did not settle within 10 ms')
      gate.resolve({
        brokers: [{ nodeId: 2, host: 'unused', port: 2 }],
        topicMetadata: [{ topic: 'review', partitionMetadata: [{ partitionId: 0, leader: 2 }] }],
      })
      await turn()
    })

    test('a producer disconnect still pending when its cleanup is cut off taints the run', async () => {
      jest.useRealTimers()
      const { producer, cluster } = bufferedProducer()
      await producer.connect()
      jest.useFakeTimers()
      const gate = deferred()
      cluster.disconnect.mockImplementation(() => gate.promise)
      const stack = new CleanupStack()
      let disconnect
      stack.add('producer', () => {
        const closing = producer.disconnect({ timeout: 8000 })
        disconnect = watch(closing)
        return closing
      })

      const result = watch(stack.closeAll({ budgetMs: 15000 }))
      await jest.advanceTimersByTimeAsync(15000)

      expect(result.state).toBe('fulfilled')
      expect(result.value).toHaveLength(1)
      expect(disconnect.state).toBe('pending')
      refusesBecause('The cleanup did not settle within \\d+ ms')
      gate.resolve()
      await turn()
    })

    test('a late ISR topic listing issues no metadata read', async () => {
      const gate = deferred()
      const admin = {
        listTopics: () => gate.promise,
        fetchTopicMetadata: jest.fn(async () => ({ topics: [] })),
      }

      const result = watch(bounded(NEVER, 10, (signal) => isrIsFull(signal, admin, ['review'])))
      await jest.advanceTimersByTimeAsync(10 + STOP_GRACE_MS)

      expect(result.state).toBe('rejected')
      gate.resolve([])
      await turn()
      expect(admin.fetchTopicMetadata).not.toHaveBeenCalled()
    })

    test('a within that answers in time leaves the run clean', async () => {
      await expect(within(1000, Promise.resolve(4), 'The group join')).resolves.toBe(4)

      expect(liveRun.tainted).toBe(false)
      expect(() => liveRun.assertClean()).not.toThrow()
    })

    test('a within expiry taints the run with the error it rejects with', async () => {
      const gate = deferred()
      let completed = false
      const result = watch(
        within(
          10,
          gate.promise.then(() => {
            completed = true
          }),
          'The group join'
        )
      )

      await jest.advanceTimersByTimeAsync(10)

      expect(result.state).toBe('rejected')
      expect(result.value.message).toBe('The group join did not settle within 10 ms')
      expect(completed).toBe(false)
      expect(liveRun.cause).toBe(result.value)
      refusesBecause('The group join did not settle within 10 ms')
      gate.resolve()
      await turn()
      expect(completed).toBe(true)
    })

    describe('the owner observation of a capture', () => {
      let FreshProbe
      let freshOwnership
      let FreshAccumulator
      let hold

      beforeEach(() => {
        jest.isolateModules(() => {
          ;({
            ProducerProbe: FreshProbe,
            batchOwnership: freshOwnership,
          } = require('./fixtures/liveProbe'))
          FreshAccumulator = require('../state/accumulator')
        })
        const gate = deferred()
        const { append } = FreshAccumulator.prototype
        hold = { gate }
        hold.spy = jest
          .spyOn(FreshAccumulator.prototype, 'append')
          .mockImplementation(async function (...args) {
            await gate.promise
            return append.apply(this, args)
          })
      })
      afterEach(() => hold.spy.mockRestore())

      /** The `batch` accessor on the accumulator's `BatchOwnership`, once its discovery is done. */
      const accessor = async () =>
        Object.getOwnPropertyDescriptor((await freshOwnership()).prototype, 'batch')

      test('a discovery that finishes after the bound and the restore installs no setter and calls no work', async () => {
        const probe = new FreshProbe({ producer: {}, clientId: 'review', maxRequestSize: 1048576 })
        probe.install()
        const work = jest.fn(async () => {})
        let installed
        try {
          const result = watch(
            bounded(NEVER, 10, (scope) => probe.capture(work, { signal: scope }))
          )
          await jest.advanceTimersByTimeAsync(10 + STOP_GRACE_MS)
          expect(result.state).toBe('rejected')
          probe.restore()
          expect(probe.trap).toBeNull()

          hold.gate.resolve()
          await jest.advanceTimersByTimeAsync(0)
          await turn()

          expect(work).not.toHaveBeenCalled()
          expect(probe.trap).toBeNull()
          installed = await accessor()
        } finally {
          probe.restore()
        }
        expect(installed).toBeUndefined()
      })

      test('a capture still outstanding when its case ends installs no setter and calls no work', async () => {
        const probe = new FreshProbe({ producer: {}, clientId: 'review', maxRequestSize: 1048576 })
        probe.install()
        const work = jest.fn(async () => {})
        let capture
        try {
          await liveCase(1000, (scope) => {
            capture = watch(probe.capture(work, { signal: scope }))
          })()
          expect(capture.state).toBe('pending')
          probe.restore()

          hold.gate.resolve()
          await jest.advanceTimersByTimeAsync(0)
          await turn()

          expect(work).not.toHaveBeenCalled()
          expect(probe.trap).toBeNull()
          expect(capture.state).toBe('rejected')
          expect(await accessor()).toBeUndefined()
        } finally {
          probe.restore()
        }
      })

      test('a capture of a case that outlasts its budget installs no setter and calls no work', async () => {
        const probe = new FreshProbe({ producer: {}, clientId: 'review', maxRequestSize: 1048576 })
        probe.install()
        const work = jest.fn(async () => {})
        try {
          const result = watch(liveCase(10, (scope) => probe.capture(work, { signal: scope }))())
          await jest.advanceTimersByTimeAsync(10 + STOP_GRACE_MS)
          expect(result.state).toBe('rejected')
          expect(liveRun.tainted).toBe(true)
          probe.restore()

          hold.gate.resolve()
          await jest.advanceTimersByTimeAsync(0)
          await turn()

          expect(work).not.toHaveBeenCalled()
          expect(probe.trap).toBeNull()
          expect(await accessor()).toBeUndefined()
        } finally {
          probe.restore()
        }
      })

      test('a discovery under an owner that aborts installs no setter and calls no work', async () => {
        const probe = new FreshProbe({ producer: {}, clientId: 'review', maxRequestSize: 1048576 })
        probe.install()
        const work = jest.fn(async () => {})
        const owner = new AbortController()
        try {
          const running = watch(probe.observeOwners({ signal: owner.signal }))
          owner.abort(new Error('owner gone'))
          hold.gate.resolve()
          await jest.advanceTimersByTimeAsync(0)
          await turn()

          expect(running.state).toBe('rejected')
          expect(running.value.message).toBe('owner gone')
          expect(probe.trap).toBeNull()
          expect(work).not.toHaveBeenCalled()
          expect(await accessor()).toBeUndefined()
        } finally {
          probe.restore()
        }
      })
    })
  })

  describe('the live specs', () => {
    const SELECTED = {
      deliveryTimeoutLive: '.',
      oversizedRecordLive: '.',
      ackLostLive: '.',
      topicLifecycleProduceLive: '.',
      notEnoughReplicasLive: '.',
      compressionLive: '.',
      decompressionBoundLive: '../../consumer/__tests__',
      transactionRestartLive: '.',
    }

    /** A live spec read as data; none is ever imported here. */
    const sourceOf = (name, dir) =>
      fs.readFileSync(path.join(__dirname, dir, `${name}.spec.js`), 'utf8')

    /** The text of the call whose `(` is at `open`, through its closing `)`. */
    const callAt = (source, open) => {
      let depth = 0
      for (let at = open; at < source.length; at += 1) {
        if (source[at] === '(') depth += 1
        else if (source[at] === ')' && (depth -= 1) === 0) return source.slice(open, at + 1)
      }
      throw new Error('A call is not closed')
    }

    test.each(Object.entries(SELECTED))(
      '%s declares every test through liveCase, with its Jest timeout derived from the budget',
      (name, dir) => {
        const source = sourceOf(name, dir)
        const starts = [...source.matchAll(/^ *test(?:\.each\b)?\(/gm)].map(({ index }) => index)
        const tests = starts.map((from, i) => source.slice(from, starts[i + 1] ?? source.length))

        expect(source).toMatch(
          /const \{[^}]*\bliveCase\b[^}]*\} = require\('[^']*\/fixtures\/bounded'\)/
        )
        expect(source).not.toMatch(/liveRun\.reset/)
        expect(tests.length).toBeGreaterThan(0)
        for (const text of tests) {
          const [, budget] = /liveCase\((\w+),/.exec(text) ?? []
          expect(text).toMatch(/liveCase\(\w+,/)
          expect(text).toContain(`caseTimeout(${budget})`)
          expect(text.match(/liveCase\(/g)).toHaveLength(1)
        }
        expect(source.match(/liveCase\(/g)).toHaveLength(tests.length)
      }
    )

    test.each(Object.entries(SELECTED))(
      '%s hands the case scope to every capture of a producer and its owners',
      (name, dir) => {
        const source = sourceOf(name, dir)
        const captures = [...source.matchAll(/\.(?:capture|observeOwners)\(/g)]
        const sends = [...source.matchAll(/\brig\.send\(/g)]

        for (const { index, 0: head } of captures) {
          expect(callAt(source, index + head.length - 1)).toMatch(/\bsignal: scope\b/)
        }
        for (const { index, 0: head } of sends) {
          expect(callAt(source, index + head.length - 1)).toContain('scope')
        }
      }
    )

    test('the capture of every selected spec is found, so the check above is not empty', () => {
      const total = Object.entries(SELECTED).reduce(
        (sum, [name, dir]) => sum + (sourceOf(name, dir).match(/\.capture\(/g) ?? []).length,
        0
      )

      expect(total).toBeGreaterThanOrEqual(12)
    })

    test('the ack-lost spec creates the topic that receives a fixed old timestamp without time retention', () => {
      const source = sourceOf('ackLostLive', '.')
      const at = source.indexOf('timestamp: FIXED_TIMESTAMP')
      const caseStart = source.lastIndexOf('liveCase(', at)
      const create = source.indexOf('createTopic(', caseStart)

      expect(at).toBeGreaterThan(-1)
      expect(create).toBeGreaterThan(caseStart)
      expect(create).toBeLessThan(at)
      expect(callAt(source, create + 'createTopic'.length)).toMatch(
        /config: \[\{ name: 'retention\.ms', value: '-1' \}\]/
      )
    })

    test('the compression spec gates the setup that creates topics and sends', () => {
      const source = sourceOf('compressionLive', '.')

      expect(source).toMatch(/beforeAll\(async \(\) => \{\n\s+liveRun\.assertClean\(\)/)
    })

    test('the epoch spec reads the transaction version inside its setup phase and keeps no hook outside the case', () => {
      const source = sourceOf('transactionEpochOverflowLive', '.')
      const phase = /timeline\.run\(\s*'setup'/.exec(source)

      expect(phase).not.toBeNull()
      const setup = callAt(source, phase.index + 'timeline.run'.length)
      const read = setup.indexOf('settledTransactionVersion(')
      expect(source).not.toMatch(/\b(?:beforeAll|beforeEach|afterAll|afterEach)\(/)
      expect(source.match(/settledTransactionVersion\(/g)).toHaveLength(1)
      expect(read).toBeGreaterThan(-1)
      expect(read).toBeLessThan(setup.indexOf('scenario.start('))
    })

    describe('the transaction restart teardown', () => {
      beforeEach(() => jest.useFakeTimers())
      afterEach(() => jest.useRealTimers())

      const turn = async () => {
        for (let i = 0; i < 60; i += 1) await Promise.resolve()
      }
      const spec = sourceOf('transactionRestartLive', '.')
      const from = spec.indexOf('  afterEach(async () => {')
      const to = spec.indexOf('\n  }, HOOK_BUDGET_MS)', from)
      const callback = `${spec.slice(from + '  afterEach('.length, to)}\n  }`

      /** The hook's callback over in-memory boundaries; the real gate, stack and wait helper. */
      const teardownOf = ({ clients = [], brokersUp = async () => 3 } = {}) => {
        const admin = { connect: jest.fn(async () => {}), disconnect: jest.fn(async () => {}) }
        const context = {
          expect: { getState: () => ({ currentTestName: 'a normal transaction case' }) },
          liveRun,
          CleanupStack,
          startBroker: jest.fn(),
          STOPPED_SERVICE: 'kafka3',
          child: null,
          kafka: { admin: () => admin },
          clients,
          waitForCondition,
          brokersUp,
          BROKER_BUDGET: { attempts: 2, delay: 10 },
          TEARDOWN_BUDGET_MS: 1000,
          writeEvidence: jest.fn(),
          EVIDENCE: 'transaction-restart',
        }
        // eslint-disable-next-line no-new-func -- the live spec is read as data, never imported
        const hook = new Function(...Object.keys(context), `return (${callback})`)(
          ...Object.values(context)
        )
        return { hook, context, admin }
      }

      test('the hook callback is found in the spec', () => {
        expect(from).toBeGreaterThan(0)
        expect(to).toBeGreaterThan(from)
      })

      test('a teardown that succeeds is a clean control', async () => {
        const client = { disconnect: jest.fn(async () => {}) }
        const { hook, context, admin } = teardownOf({ clients: [client] })
        liveRun.admit()

        await hook()

        expect(context.startBroker).toHaveBeenCalledTimes(1)
        expect(admin.connect).toHaveBeenCalledTimes(1)
        expect(admin.disconnect).toHaveBeenCalledTimes(1)
        expect(client.disconnect).toHaveBeenCalledTimes(1)
        expect(liveRun.tainted).toBe(false)
        expect(() => liveRun.assertClean()).not.toThrow()
      })

      test('a client disconnect still pending when the teardown is cut off fails the hook and taints the run', async () => {
        const gate = deferred()
        let completed = false
        const client = {
          disconnect: () =>
            gate.promise.then(() => {
              completed = true
            }),
        }
        const { hook } = teardownOf({ clients: [client] })
        liveRun.admit()

        const result = watch(hook())
        await jest.advanceTimersByTimeAsync(1000)

        expect(result.state).toBe('rejected')
        expect(completed).toBe(false)
        expect(liveRun.tainted).toBe(true)
        refusesBecause('The cleanup did not settle within \\d+ ms')
        gate.resolve()
        await turn()
        expect(completed).toBe(true)
      })

      test('a client disconnect that rejects fails the hook and taints the run', async () => {
        const client = {
          disconnect: async () => {
            throw new Error('disconnect failed')
          },
        }
        const { hook } = teardownOf({ clients: [client] })
        liveRun.admit()

        await expect(hook()).rejects.toThrow('Cleanup failed')

        expect(liveRun.tainted).toBe(true)
        refusesBecause('client disconnect: disconnect failed')
      })

      test('an admin disconnect that rejects fails the hook and taints the run', async () => {
        const { hook, admin } = teardownOf()
        admin.disconnect.mockRejectedValue(new Error('admin disconnect failed'))
        liveRun.admit()

        await expect(hook()).rejects.toThrow('Cleanup failed')

        refusesBecause('admin disconnect: admin disconnect failed')
      })

      test('a cluster that does not return to three brokers fails the hook, records it and taints the run', async () => {
        const { hook, context } = teardownOf({ brokersUp: async () => 2 })
        liveRun.admit()

        const result = watch(hook())
        await jest.advanceTimersByTimeAsync(1000)

        expect(result.state).toBe('rejected')
        expect(context.writeEvidence).toHaveBeenCalledWith(
          'transaction-restart',
          expect.objectContaining({ scenario: 'teardown', threeBrokers: false })
        )
        refusesBecause('three brokers back: The cluster did not return to three brokers')
      })

      test('a case the gate refused starts no broker and opens no admin', async () => {
        const client = { disconnect: jest.fn(async () => {}) }
        const { hook, context, admin } = teardownOf({ clients: [client] })
        liveRun.admit()
        await hook()
        liveRun.taint(new Error('earlier escaped work'))
        context.startBroker.mockClear()
        admin.connect.mockClear()

        await expect(liveCase(1000, async () => {})()).rejects.toThrow('earlier escaped work')
        await hook()

        expect(context.startBroker).not.toHaveBeenCalled()
        expect(admin.connect).not.toHaveBeenCalled()
        expect(admin.disconnect).toHaveBeenCalledTimes(1)
        expect(client.disconnect).toHaveBeenCalledTimes(1)
      })

      test('a case that tainted the run itself still gets its recovery', async () => {
        const { hook, context, admin } = teardownOf()
        await liveCase(1000, async () => liveRun.taint(new Error('this case')))()

        await hook()

        expect(context.startBroker).toHaveBeenCalledTimes(1)
        expect(admin.connect).toHaveBeenCalledTimes(1)
      })

      test('the admission of one case does not reach the hook of the next', async () => {
        const { hook, context } = teardownOf()
        await liveCase(1000, async () => {})()

        await hook()
        await hook()

        expect(context.startBroker).toHaveBeenCalledTimes(1)
      })
    })

    describe('the setup hooks', () => {
      beforeEach(() => jest.useFakeTimers())
      afterEach(() => jest.useRealTimers())

      /**
       * The `beforeAll` callback of a spec, read as data, over in-memory boundaries and the real
       * harness; `budgetName` is the constant that is the hook's Jest timeout.
       */
      const setupOf = (name, budgetName, boundaries) => {
        const spec = sourceOf(name, '.')
        const open = 'beforeAll(async () => {'
        const from = spec.indexOf(open)
        const to = spec.indexOf(`}, ${budgetName})`, from)
        expect(from).toBeGreaterThan(0)
        expect(to).toBeGreaterThan(from)
        const jestMs = Number(new RegExp(`const ${budgetName} = (\\d+)`).exec(spec)[1])
        const context = {
          bounded,
          NEVER,
          CASE_JOIN_MS,
          liveRun,
          [budgetName]: jestMs,
          ...boundaries,
        }
        const callback = `${spec.slice(from + 'beforeAll('.length, to)}}`
        // eslint-disable-next-line no-new-func -- the live spec is read as data, never imported
        const hook = new Function(...Object.keys(context), `return (${callback})`)(
          ...Object.values(context)
        )
        return { hook, jestMs }
      }

      const compressionBoundaries = (overrides) => ({
        process: { env: {} },
        loadCapture: () => ({ manifest: {} }),
        validateCapture: () => ({}),
        assertFinalArtifact: () => {},
        compression: {},
        secureRandom: () => 'memory',
        plainTextBrokers: () => [],
        CleanupStack,
        createTopic: jest.fn(),
        ...overrides,
      })

      /** The hook with its first boundary held; runs to just before Jest's own deadline. */
      const runHeld = async (hook, jestMs) => {
        const result = watch(hook())
        await jest.advanceTimersByTimeAsync(jestMs - 1)
        return result
      }

      test('the compression setup is found in the spec', () => {
        expect(() =>
          setupOf('compressionLive', 'HOOK_BUDGET_MS', compressionBoundaries())
        ).not.toThrow()
      })

      test('a compression setup held on its first boundary rejects and taints the run before its Jest deadline', async () => {
        const held = deferred()
        const createTopic = jest.fn(() => held.promise)
        const { hook, jestMs } = setupOf(
          'compressionLive',
          'HOOK_BUDGET_MS',
          compressionBoundaries({ createTopic })
        )

        const result = await runHeld(hook, jestMs)

        expect(createTopic).toHaveBeenCalledTimes(1)
        expect(result.state).toBe('rejected')
        expect(result.value.message).toMatch(/The setup did not settle within \d+ ms/)
        expect(liveRun.tainted).toBe(true)
        expect(liveRun.takeAdmission()).toBe(false)
        refusesBecause('The setup did not settle within \\d+ ms')
        held.reject(new Error('memory boundary ended'))
        await jest.advanceTimersByTimeAsync(0)
      })

      test('a compression setup that fails by itself fails the hook without tainting the run', async () => {
        const failure = new Error('no capture')
        const { hook } = setupOf(
          'compressionLive',
          'HOOK_BUDGET_MS',
          compressionBoundaries({
            loadCapture: () => {
              throw failure
            },
          })
        )

        await expect(hook()).rejects.toBe(failure)

        expect(liveRun.tainted).toBe(false)
      })

      test('a compression setup in a tainted run issues nothing', async () => {
        const createTopic = jest.fn()
        const { hook } = setupOf(
          'compressionLive',
          'HOOK_BUDGET_MS',
          compressionBoundaries({ createTopic })
        )
        liveRun.taint(new Error('earlier escaped work'))

        await expect(hook()).rejects.toThrow('earlier escaped work')

        expect(createTopic).not.toHaveBeenCalled()
      })

      /** The transaction feature setup over a version read the test controls. */
      const transactionSetup = (settledTransactionVersion) => {
        const prototype = { initProducerId: async () => {}, endTxn: async () => {} }
        return {
          prototype,
          ...setupOf('transactionRestartLive', 'FEATURE_HOOK_BUDGET_MS', {
            settledTransactionVersion,
            Broker: { prototype },
            jest,
            cluster: undefined,
            initializations: [],
            ends: [],
          }),
        }
      }

      test('the transaction feature setup is found in the spec', () => {
        expect(() => transactionSetup(jest.fn())).not.toThrow()
      })

      test('a transaction feature setup held on its first boundary rejects and taints the run before its Jest deadline', async () => {
        const held = deferred()
        const settledTransactionVersion = jest.fn(() => held.promise)
        const { hook, jestMs } = transactionSetup(settledTransactionVersion)

        const result = await runHeld(hook, jestMs)

        expect(settledTransactionVersion).toHaveBeenCalledTimes(1)
        expect(result.state).toBe('rejected')
        expect(result.value.message).toMatch(/The setup did not settle within \d+ ms/)
        expect(liveRun.tainted).toBe(true)
        expect(liveRun.takeAdmission()).toBe(false)
        refusesBecause('The setup did not settle within \\d+ ms')
        held.reject(new Error('memory boundary ended'))
        await jest.advanceTimersByTimeAsync(0)
      })

      test('a transaction feature setup that answers in time installs its spies and leaves the run clean', async () => {
        const { hook, prototype } = transactionSetup(async () => ({ version: 1 }))
        const { initProducerId, endTxn } = prototype

        await hook()

        expect(prototype.initProducerId).not.toBe(initProducerId)
        expect(prototype.endTxn).not.toBe(endTxn)
        expect(liveRun.tainted).toBe(false)
      })
    })
  })

  describe('reads that wait for the cluster to settle', () => {
    const budget = { attempts: 3, delay: 1 }

    describe('readUntilCovered', () => {
      test('reads again after an answer that does not cover and returns the first that does', async () => {
        const answers = [[], [], ['a', 'b'], ['never read']]
        const read = jest.fn(async () => answers.shift())

        await expect(
          readUntilCovered(NEVER, read, (records) => records.length === 2, {
            attempts: 5,
            delay: 1,
          })
        ).resolves.toEqual(['a', 'b'])
        expect(read).toHaveBeenCalledTimes(3)
      })

      test('returns the last answer when its attempts are spent, without failing or tainting the run', async () => {
        const read = jest.fn(async () => [])

        await expect(
          readUntilCovered(NEVER, read, (records) => records.length > 0, budget)
        ).resolves.toEqual([])
        expect(read).toHaveBeenCalledTimes(3)
        expect(liveRun.tainted).toBe(false)
      })

      test('reads nothing more once its owner aborted', async () => {
        const owner = new AbortController()
        const read = jest.fn(async () => [])
        const covers = () => {
          owner.abort(new Error('case over'))
          return false
        }

        await expect(readUntilCovered(owner.signal, read, covers, budget)).rejects.toThrow(
          'case over'
        )
        expect(read).toHaveBeenCalledTimes(1)
      })
    })

    describe('retryCoordinator', () => {
      const offsets = { groupId: 'g', topics: ['t'] }

      test.each([14, 15, 16])(
        'asks again after the coordinator error %i and returns the answer',
        async (code) => {
          const answer = [{ partitions: [{ offset: '-1' }] }]
          const admin = {
            fetchOffsets: jest
              .fn()
              .mockRejectedValueOnce(createErrorFromCode(code))
              .mockResolvedValue(answer),
          }

          await expect(
            retryCoordinator(NEVER, () => admin.fetchOffsets(offsets), budget)
          ).resolves.toBe(answer)
          expect(admin.fetchOffsets).toHaveBeenCalledTimes(2)
          expect(admin.fetchOffsets).toHaveBeenCalledWith(offsets)
        }
      )

      test.each([
        ['a retriable error of another kind', createErrorFromCode(13)],
        ['a non-retriable protocol error', createErrorFromCode(29)],
        ['a plain failure', new Error('boom')],
      ])('fails at once with %s', async (_name, error) => {
        const admin = { fetchOffsets: jest.fn().mockRejectedValue(error) }

        await expect(
          retryCoordinator(NEVER, () => admin.fetchOffsets(offsets), budget)
        ).rejects.toBe(error)
        expect(admin.fetchOffsets).toHaveBeenCalledTimes(1)
      })

      test('gives up with the last coordinator error when its attempts are spent', async () => {
        const error = createErrorFromCode(16)
        const admin = { fetchOffsets: jest.fn().mockRejectedValue(error) }

        await expect(
          retryCoordinator(NEVER, () => admin.fetchOffsets(offsets), budget)
        ).rejects.toBe(error)
        expect(admin.fetchOffsets).toHaveBeenCalledTimes(3)
      })

      test('asks nothing more once its owner aborted', async () => {
        const owner = new AbortController()
        const op = jest.fn(async () => {
          owner.abort(new Error('case over'))
          throw createErrorFromCode(16)
        })

        await expect(retryCoordinator(owner.signal, op, budget)).rejects.toThrow('case over')
        expect(op).toHaveBeenCalledTimes(1)
      })
    })
  })

  describe('helpers', () => {
    test('sequenceGaps names a skipped and a repeated sequence within one producer epoch only', () => {
      const record = (
        firstOffset,
        producerId,
        producerEpoch,
        firstSequence,
        lastOffsetDelta = 0
      ) => ({
        batchContext: {
          firstOffset,
          producerId,
          producerEpoch,
          firstSequence,
          lastOffsetDelta,
          isControlBatch: false,
        },
      })
      const records = [
        record('0', '5', 0, 0, 1),
        record('2', '5', 0, 2),
        record('3', '5', 0, 5),
        record('4', '5', 0, 5),
        record('5', '5', 1, 0),
        record('6', '-1', -1, -1),
        record('7', '-1', -1, -1),
      ]

      expect(sequenceGaps(records)).toEqual([
        { producerId: '5', producerEpoch: 0, expected: 3, actual: 5 },
        { producerId: '5', producerEpoch: 0, expected: 6, actual: 5 },
      ])
    })

    test('dataRecords leaves control records out and orders by partition then offset', () => {
      const data = (value) => ({ value, batchContext: { isControlBatch: false } })
      const control = { batchContext: { isControlBatch: true } }
      const log = new Map([
        [1, { records: [data('p1')] }],
        [0, { records: [data('p0a'), control, data('p0b')] }],
      ])

      expect(dataRecords(log).map(({ value }) => value)).toEqual(['p0a', 'p0b', 'p1'])
    })

    test('describeError keeps the identifying fields, follows causes and drops the rest', () => {
      const cause = Object.assign(new Error('root'), { type: 'ROOT', code: 7 })
      const error = Object.assign(new Error('top'), {
        name: 'KafkaJSDeliveryTimeout',
        outcome: 'unknown',
        category: 'abortable',
        stack: 'never kept',
        cause,
      })

      expect(describeError(error)).toEqual({
        name: 'KafkaJSDeliveryTimeout',
        type: undefined,
        code: undefined,
        category: 'abortable',
        retriable: undefined,
        outcome: 'unknown',
        reason: undefined,
        message: 'top',
        cause: {
          name: 'Error',
          type: 'ROOT',
          code: 7,
          category: undefined,
          retriable: undefined,
          outcome: undefined,
          reason: undefined,
          message: 'root',
          cause: undefined,
        },
      })
      expect(describeError(null)).toBeNull()
    })

    test('settle and within turn a rejection into a value and a hang into a failure', async () => {
      const failure = new Error('refused')

      await expect(settle(Promise.reject(failure))).resolves.toEqual({
        value: null,
        error: failure,
      })
      await expect(settle(Promise.resolve(3))).resolves.toEqual({ value: 3, error: null })
      await expect(within(10, new Promise(() => {}), 'The wait')).rejects.toThrow(
        'The wait did not settle within 10 ms'
      )
      await expect(within(1000, Promise.resolve(4), 'The wait')).resolves.toBe(4)
    })
  })

  describe('heartbeatInBatch', () => {
    test('returns when the heartbeat does', async () => {
      const heartbeat = jest.fn(async () => {})

      await expect(heartbeatInBatch(heartbeat)).resolves.toBeUndefined()
      expect(heartbeat).toHaveBeenCalledTimes(1)
    })

    test.each([
      ['ILLEGAL_GENERATION', 22],
      ['UNKNOWN_MEMBER_ID', 25],
      ['REBALANCE_IN_PROGRESS', 27],
      ['FENCED_INSTANCE_ID', 82],
      ['FENCED_MEMBER_EPOCH', 110],
    ])('ends the batch with %s, the coordinator saying the group moved on', async (_name, code) => {
      const error = createErrorFromCode(code)

      await expect(
        heartbeatInBatch(async () => {
          throw error
        })
      ).rejects.toBe(error)
    })

    test.each([
      ['a lost connection', new KafkaJSConnectionError('lost')],
      ['a request timeout', new KafkaJSRequestTimeoutError('timed out')],
      ['a loading coordinator', createErrorFromCode(14)],
      ['an unavailable coordinator', createErrorFromCode(15)],
      ['a moved coordinator', createErrorFromCode(16)],
      ['an error that says nothing of the membership', createErrorFromCode(29)],
      ['a plain failure', new Error('boom')],
    ])('leaves %s to the consumer, which rides it out', async (_name, error) => {
      await expect(
        heartbeatInBatch(async () => {
          throw error
        })
      ).resolves.toBeUndefined()
    })
  })
})
