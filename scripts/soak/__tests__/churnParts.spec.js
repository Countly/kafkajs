const { EventEmitter } = require('node:events')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const {
  CHURN,
  PROFILES,
  ProducerChurn,
  RawProducerReader,
  WriteDelay,
  buildProducerChurn,
  forgettingDestroyed,
  instrument,
} = require('../soak')
const {
  ENTRY_BYTES,
  STATUS,
  ObservationJournal,
  ObservedLog,
  OfferLedger,
  decodeId,
  encodeValue,
  mergeDurability,
  sequenceOf,
} = require('../churnLedger')
const { ClosingProbe, PROBE_TIMESTAMP, exactValueBytes, varintSize } = require('../closingProbe')
const { createChaosSocketFactory } = require('../../../testHelpers/chaosSocket')
const Cluster = require('../../../src/cluster')
const Generation = require('../../../src/producer/generation')
const Accumulator = require('../../../src/producer/state/accumulator')
const { Admission } = require('../../../src/producer/state/admission')
const RequestQueue = require('../../../src/network/requestQueue')
const { chargeOf } = require('../../../src/producer/state/recordSize')
const { assertProbe } = require('../../../testHelpers/producerProbe')
const { CAPACITY, refusal } = require('./churnFakes')

const record = (id, { partition = 0, pid = 1, epoch = 0, sequence = 0 } = {}) => ({
  id,
  partition,
  pid,
  epoch,
  sequence,
})

describe('Churn ledger > offered ids', () => {
  test('are dense, start pending and take one outcome each', () => {
    const ledger = new OfferLedger({ capacity: 4, partitions: 2 })

    expect([ledger.offer(0), ledger.offer(0), ledger.offer(0)]).toEqual([0, 1, 2])
    ledger.settle(0, STATUS.ACK)
    ledger.settle(1, STATUS.UNKNOWN)
    ledger.settle(1, STATUS.ACK)
    ledger.settle(2, STATUS.UNSENT)

    expect(Array.from(ledger.status.subarray(0, 3))).toEqual([
      STATUS.ACK,
      STATUS.UNKNOWN,
      STATUS.UNSENT,
    ])
    expect(ledger.counts).toEqual({ pending: 0, ack: 1, unknown: 1, unsent: 1, unexpected: 0 })
  })

  test('refuse another id once full, and keep their storage at the size it was made', () => {
    const ledger = new OfferLedger({ capacity: 2, partitions: 2, sampleEvery: 2 })
    const sizes = [ledger.status.length, ledger.offeredAt.length, ledger.latencies.length]

    expect([ledger.offer(0), ledger.offer(0), ledger.offer(0)]).toEqual([0, 1, -1])
    for (let id = 0; id < 2; id++) ledger.settle(id, STATUS.ACK)

    expect([ledger.status.length, ledger.offeredAt.length, ledger.latencies.length]).toEqual(sizes)
    expect(sizes).toEqual([2, 1, 1])
  })

  test('remember the highest offset acknowledged in each partition', () => {
    const ledger = new OfferLedger({ capacity: 4, partitions: 2 })
    for (let id = 0; id < 3; id++) ledger.offer(0)

    ledger.settle(0, STATUS.ACK, { partition: 1, offset: 9 })
    ledger.settle(1, STATUS.ACK, { partition: 1, offset: 4 })
    ledger.settle(2, STATUS.ACK)

    expect(Array.from(ledger.lastAckOffset)).toEqual([-1, 9])
    expect(Array.from(ledger.lastAckId)).toEqual([-1, 0])
  })
})

describe('Churn ledger > latency', () => {
  const ledgerOf = (sampleEvery = 1) =>
    new OfferLedger({ capacity: 40, partitions: 1, sampleEvery })

  test('counts an id only once it is both acknowledged and seen, in either order', () => {
    const ledger = ledgerOf()
    for (let id = 0; id < 3; id++) ledger.offer(id * 10)

    ledger.observed(0, 5)
    expect(ledger.latency().samples).toBe(0)
    ledger.settle(0, STATUS.ACK)
    ledger.settle(1, STATUS.ACK)
    ledger.observed(1, 17)
    ledger.observed(1, 99)
    ledger.observed(2, 25)

    expect(ledger.latency()).toMatchObject({ samples: 2, invalid: 0, max: 7 })
  })

  test('takes percentiles of the samples and counts a negative one as invalid', () => {
    const ledger = ledgerOf()
    for (let id = 0; id < 11; id++) ledger.offer(0)
    for (let id = 0; id < 10; id++) {
      ledger.settle(id, STATUS.ACK)
      ledger.observed(id, (id + 1) * 10)
    }
    ledger.settle(10, STATUS.ACK)
    ledger.observed(10, -1)

    expect(ledger.latency()).toEqual({
      sampleEvery: 1,
      samples: 10,
      invalid: 1,
      p50: 50,
      p90: 90,
      p99: 100,
      max: 100,
    })
  })

  test('times one id in `sampleEvery` and ignores the rest and ids never offered', () => {
    const ledger = ledgerOf(4)
    for (let id = 0; id < 9; id++) ledger.offer(0)
    for (let id = 0; id < 9; id++) {
      ledger.settle(id, STATUS.ACK)
      ledger.observed(id, 3)
    }
    ledger.observed(39, 3)
    ledger.observed(-1, 3)

    expect(ledger.latency().samples).toBe(3)
  })

  test('reports no percentiles without a sample', () => {
    expect(ledgerOf().latency()).toMatchObject({
      samples: 0,
      p50: null,
      p90: null,
      p99: null,
      max: null,
    })
  })
})

describe('Churn ledger > what was read', () => {
  test('accepts a 65th producer identity', () => {
    const observed = new ObservedLog({ capacity: 65 })
    for (let id = 0; id < 65; id++) observed.add(id, 0, 1, id, 0)
    expect(observed.identityOverflow).toBe(0)
  })

  test('detects exact repeats among identities beyond the former limit', () => {
    const observed = new ObservedLog({ capacity: 256 })
    for (let id = 0; id < 256; id++) observed.add(id, id % 6, id, id % 17, id)
    for (let id = 255; id >= 0; id--) observed.add(id, id % 6, id, id % 17, id)
    expect(observed.identityOverflow).toBe(0)
    expect(observed.repeatedIdentities).toBe(256)
  })

  test('counts an id read again, a repeated identity, an id never offered and a bad sequence', () => {
    const observed = new ObservedLog({ capacity: 8 })
    const add = ({ id, partition, pid, epoch, sequence }) =>
      observed.add(id, partition, pid, epoch, sequence)

    add(record(1, { sequence: 0 }))
    add(record(2, { sequence: 1 }))
    add(record(3, { sequence: 1 }))
    add(record(1, { sequence: 2 }))
    add(record(99))
    add(record(-1))
    add(record(4, { sequence: 8 }))
    add(record(5, { partition: 1 }))
    add(record(6, { partition: 2 }))

    expect(observed).toMatchObject({
      records: 9,
      repeatedIds: 1,
      repeatedIdentities: 1,
      unexpected: 2,
      badSequences: 1,
      identityOverflow: 0,
    })
    expect(observed.seen[1]).toBe(2)
  })

  test('admits exactly capacity keys, rejects one more, and still detects admitted repeats', () => {
    const observed = new ObservedLog({ capacity: 8 })
    const ledger = new OfferLedger({ capacity: 8, partitions: 1 })
    for (let id = 0; id < 8; id++) {
      ledger.offer(0)
      ledger.settle(id, STATUS.ACK)
      observed.add(id, 0, 1, id, 0)
    }
    expect(mergeDurability(ledger, observed).ok).toBe(true)
    observed.add(0, 0, 1, 8, 0)
    observed.add(7, 0, 1, 7, 0)
    expect(observed).toMatchObject({ identities: 8, identityOverflow: 1, repeatedIdentities: 1 })
    expect(mergeDurability(ledger, observed).violations.join()).toContain(
      '1 records exceed the raw-identity table capacity'
    )
  })

  test('keeps all int64 producer bits, including adjacent ids beyond 2^53', () => {
    const observed = new ObservedLog({ capacity: 8 })
    const pids = [
      0n,
      9007199254740992n,
      9007199254740993n,
      9223372036854775807n,
      -9223372036854775808n,
      -1n,
    ]
    pids.forEach((pid, id) => observed.add(id, 0, pid, 0, 0))
    expect(observed.repeatedIdentities).toBe(0)
    pids.forEach((pid, id) => observed.add(id, 0, String(pid), 0, 0))
    expect(observed.repeatedIdentities).toBe(pids.length)
  })

  test.each([9007199254740992, 9223372036854775808n, -9223372036854775809n])(
    'rejects a producer id that cannot be represented losslessly: %s',
    (pid) => {
      const observed = new ObservedLog({ capacity: 8 })
      expect(() => observed.add(0, 0, pid, 0, 0)).toThrow(RangeError)
    }
  )

  test.each([-1, 8, 0.5, NaN, Infinity])('rejects bad sequence %s', (sequence) => {
    const observed = new ObservedLog({ capacity: 8 })
    observed.add(0, 0, 1, 0, sequence)
    expect(observed).toMatchObject({ badSequences: 1, identities: 0 })
  })

  test('tell the same sequence of another partition, pid or epoch apart', () => {
    const observed = new ObservedLog({ capacity: 8 })

    observed.add(0, 0, 1, 0, 0)
    observed.add(1, 1, 1, 0, 0)
    observed.add(2, 0, 2, 0, 0)
    observed.add(3, 0, 1, 1, 0)
    observed.add(4, 65536, 1, 0, 0)

    expect(observed.repeatedIdentities).toBe(0)
    observed.add(4, 65536, 1, 0, 0)
    expect(observed.repeatedIdentities).toBe(1)
  })

  test('forget everything on reset and keep their storage', () => {
    const observed = new ObservedLog({ capacity: 8 })
    const storage = [
      observed.seen,
      observed.pids,
      observed.partitions,
      observed.epochs,
      observed.sequences,
    ]
    for (let id = 0; id < 8; id++) observed.add(id, 0, 1, id, 0)
    observed.add(0, 0, 1, 0, 0)
    observed.add(0, 0, 1, 8, 0)
    observed.add(-1, 0, 1, 0, 0)
    observed.add(0, 0, 1, 0, -1)

    observed.reset()
    observed.add(0, 0, 1, 0, 0)

    expect(observed).toMatchObject({
      records: 1,
      repeatedIds: 0,
      repeatedIdentities: 0,
      unexpected: 0,
      badSequences: 0,
      identityOverflow: 0,
    })
    const reused = [
      observed.seen,
      observed.pids,
      observed.partitions,
      observed.epochs,
      observed.sequences,
    ]
    reused.forEach((buffer, index) => expect(buffer).toBe(storage[index]))
    for (let id = 1; id < 8; id++) observed.add(id, 0, 1, id, 0)
    expect(observed).toMatchObject({ identities: 8, identityOverflow: 0 })
  })
})

describe('Churn ledger > the merge by id', () => {
  const build = (statuses, reads) => {
    const ledger = new OfferLedger({ capacity: statuses.length, partitions: 1 })
    statuses.forEach((status, id) => {
      ledger.offer(0)
      if (status !== STATUS.PENDING) ledger.settle(id, status)
    })
    const observed = new ObservedLog({ capacity: statuses.length })
    reads.forEach((id, index) => observed.add(id, 0, 1, 0, index))
    return mergeDurability(ledger, observed, 'test')
  }

  test('passes when every acknowledged id is read once and no refused id is read', () => {
    const merged = build([STATUS.ACK, STATUS.UNKNOWN, STATUS.UNSENT, STATUS.UNKNOWN], [0, 1])

    expect(merged).toMatchObject({ ok: true, violations: [] })
    expect(merged.counts).toMatchObject({ acknowledgedRead: 1, unknownRead: 1, unknownAbsent: 1 })
  })

  test.each([
    [
      'an acknowledged id read twice',
      [STATUS.ACK],
      [0, 0],
      /acknowledged id 0 was read more than once/,
    ],
    ['an acknowledged id not read', [STATUS.ACK, STATUS.ACK], [1], /acknowledged id 0 is missing/],
    [
      'an unknown id read twice',
      [STATUS.UNKNOWN],
      [0, 0],
      /id 0 of unknown outcome was read more than once/,
    ],
    [
      'an id refused before sending that is read',
      [STATUS.UNSENT],
      [0],
      /id 0 was refused before sending but is in the log/,
    ],
    ['an id with no outcome', [STATUS.PENDING], [], /id 0 never got an outcome/],
    [
      'an unclassified rejection read twice',
      [STATUS.UNEXPECTED],
      [0, 0],
      /id 0 of unknown outcome was read more than once/,
    ],
  ])('fails %s', (_, statuses, reads, message) => {
    const merged = build(statuses, reads)

    expect(merged.ok).toBe(false)
    expect(merged.violations[0]).toMatch(message)
    expect(merged.violations[0]).toMatch(/^durability \(test\): /)
  })
})

describe('Churn ledger > the value of a send', () => {
  test('carries its id and the run tag in the first eight bytes, and nothing else', () => {
    const value = encodeValue(200, 123456, 0xdeadbeef)

    expect(value).toHaveLength(200)
    expect(decodeId(value, 0xdeadbeef)).toBe(123456)
    expect(value.subarray(8).every((byte) => byte === 0)).toBe(true)
  })

  test("is not read as an id when it is another run's, too short or empty", () => {
    expect(decodeId(encodeValue(8, 5, 1), 2)).toBe(-1)
    expect(decodeId(Buffer.alloc(7), 0)).toBe(-1)
    expect(decodeId(null, 0)).toBe(-1)
  })

  test("has the sequence of its batch's first plus its place in the batch", () => {
    expect(
      sequenceOf({ offset: '12', batchContext: { firstOffset: '10', firstSequence: 100 } })
    ).toBe(102)
  })
})

describe('Churn ledger > the journal', () => {
  let directory
  beforeAll(() => {
    directory = fs.mkdtempSync(path.join(os.tmpdir(), 'churn-parts-'))
  })
  afterAll(() => {
    fs.rmSync(directory, { recursive: true, force: true })
  })

  test('writes fixed entries in batches and replays them in order, in bounded chunks', () => {
    const file = path.join(directory, 'a.journal')
    const journal = new ObservationJournal({ file, entries: 3 })
    for (let index = 0; index < 10; index++) {
      journal.append(index, index % 6, 1e15 + index, -1, 7 * index, 1000 + index, index)
    }
    journal.append(-1, 0, 0, 0, 0, 0, 0)

    const replayed = []
    const count = journal.replay((...entry) => replayed.push(entry), 4)

    expect(count).toBe(11)
    expect(fs.statSync(file).size).toBe(11 * ENTRY_BYTES)
    expect(replayed[3]).toEqual([3, 3, 1e15 + 3, -1, 21])
    expect(replayed.slice(0, 10).map(([id]) => id)).toEqual([...Array(10).keys()])
    expect(replayed[10][0]).toBe(0xffffffff)
    journal.close()
  })

  test('replays what was appended after an earlier replay too', () => {
    const journal = new ObservationJournal({ file: path.join(directory, 'b.journal'), entries: 2 })
    journal.append(1, 0, 1, 0, 0, 0, 0)
    journal.replay(() => {})
    journal.append(2, 0, 1, 0, 1, 1, 0)

    const ids = []
    journal.replay((id) => ids.push(id))

    expect(ids).toEqual([1, 2])
    journal.close()
  })

  test('closes once and says when its file ends short', () => {
    const files = {
      openSync: () => 7,
      writeSync: jest.fn(),
      readSync: () => 0,
      closeSync: jest.fn(),
    }
    const journal = new ObservationJournal({ file: 'unused', entries: 1, fs: files })
    journal.append(1, 0, 1, 0, 0, 0, 0)

    expect(() => journal.replay(() => {})).toThrow('The journal ends at 0 of 32 bytes')
    journal.close()
    journal.close()
    expect(files.closeSync).toHaveBeenCalledTimes(1)
  })
})

describe('Churn instrument > the counted factory', () => {
  const fakeSocket = () => Object.assign(new EventEmitter(), { destroyed: false })

  test('measures a raw connection made through it, and a closed one is back at the baseline', async () => {
    const sockets = []
    const { socketFactory, probe, listeners } = instrument(
      {
        brokers: ['unused:1'],
        socketFactory: () => {
          sockets.push(fakeSocket())
          return sockets.at(-1)
        },
      },
      { activeResources: false }
    )
    const baseline = await probe.snapshot()

    const raw = socketFactory({ host: 'h', port: 1 })
    raw.on('data', () => {})
    const open = await probe.snapshot()

    expect(raw).toBe(sockets[0])
    expect(baseline.sockets).toEqual({ created: 0, destroyed: 0, open: 0 })
    expect(open.sockets).toEqual({ created: 1, destroyed: 0, open: 1 })
    expect(open.listeners.sockets).toBe(1)
    expect(listeners()).toEqual({ requestQueues: {}, sockets: { data: 1 } })

    raw.destroyed = true
    const closed = await probe.snapshot()

    expect(closed.sockets).toEqual({ created: 1, destroyed: 1, open: 0 })
    expect(closed.listeners).toEqual(baseline.listeners)
    expect(listeners()).toEqual({ requestQueues: {}, sockets: {} })
  })

  test('still returns the client and its probe, as before', () => {
    const { kafka, probe } = instrument({ brokers: ['unused:1'], socketFactory: fakeSocket })

    expect(kafka.producer).toEqual(expect.any(Function))
    expect(probe.snapshot).toEqual(expect.any(Function))
  })
})

describe('Churn reader', () => {
  const TOPIC = 'churn'
  const message = (offset, { control = false } = {}) => ({
    offset: String(offset),
    value: encodeValue(10, offset, 1),
    batchContext: { firstOffset: String(offset), isControlBatch: control },
  })

  /** Brokers that serve `logs[partition]` from the fetch offset, led as `leaders` says. */
  const cluster = ({ logs, leaders = [1, 1, 2], fail = () => null }) => {
    const brokers = new Map()
    const open = jest.fn(async ({ host, port, socketFactory }) => {
      const socket = socketFactory({ host, port })
      const broker = {
        host,
        socket,
        metadata: jest.fn(async () => ({
          brokers: [
            { nodeId: 1, host: 'h1', port: 1 },
            { nodeId: 2, host: 'h2', port: 2 },
          ],
          topicMetadata: [
            {
              topic: TOPIC,
              topicErrorCode: 0,
              partitionMetadata: leaders.map((leader, partitionId) => ({
                partitionId,
                leader,
                partitionErrorCode: 0,
              })),
            },
          ],
        })),
        fetch: jest.fn(async ({ topics: [{ partitions }] }) => {
          const error = fail(broker)
          if (error) throw error
          return {
            responses: [
              {
                topicName: TOPIC,
                partitions: partitions.map(({ partition }) => ({
                  partition,
                  messages: logs[partition],
                })),
              },
            ],
          }
        }),
        disconnect: jest.fn(async () => {
          socket.destroyed = true
        }),
      }
      brokers.set(host, [...(brokers.get(host) ?? []), broker])
      return broker
    })
    return { open, brokers }
  }

  const makeReader = (options = {}) => {
    const logs = options.logs ?? [[message(0), message(1)], [message(0)], []]
    const fakes = cluster({ ...options, logs })
    const seen = []
    const times = { t: 0 }
    const { socketFactory, probe } = instrument(
      {
        brokers: ['unused:1'],
        socketFactory: () => Object.assign(new EventEmitter(), { destroyed: false }),
      },
      { activeResources: false }
    )
    const record = jest.fn()
    const reader = new RawProducerReader({
      brokers: options.brokers ?? ['seed1:1', 'seed2:2'],
      topic: TOPIC,
      record,
      socketFactory: options.wrap ? options.wrap(socketFactory, () => reader) : socketFactory,
      onRecord: (partition, value) => seen.push([partition, value.offset]),
      partitions: 3,
      openBroker: fakes.open,
      clock: { now: () => (times.t += 1) },
      sleep: jest.fn(async (ms) => {
        times.t += ms
      }),
      retryMs: 7,
      drainMs: 50,
    })
    return { reader, record, seen, probe, times, ...fakes }
  }

  test('reads every partition from its own cursor and passes each record on once', async () => {
    const { reader, seen } = makeReader()

    await reader.start()
    expect(await reader.poll()).toBe(3)
    expect(await reader.poll()).toBe(0)

    expect(seen).toEqual(
      expect.arrayContaining([
        [0, '0'],
        [0, '1'],
        [1, '0'],
      ])
    )
    expect(seen).toHaveLength(3)
    expect(reader.cursors()).toEqual(
      new Map([
        [0, 2],
        [1, 1],
        [2, 0],
      ])
    )
  })

  test('skips what lies before its cursor and the control batches, and moves only past what it read', async () => {
    const logs = [[message(0), message(1, { control: true }), message(2)], [], []]
    const { reader, seen } = makeReader({ logs })

    await reader.start()
    await reader.poll()
    logs[0].push(message(3))
    await reader.poll()

    expect(seen).toEqual([
      [0, '0'],
      [0, '2'],
      [0, '3'],
    ])
    expect(reader.cursors().get(0)).toBe(4)
  })

  test('keeps one connection to each leader and one to ask for metadata', async () => {
    const { reader, open } = makeReader()

    await reader.start()
    await reader.poll()
    await reader.poll()

    expect(open).toHaveBeenCalledTimes(3)
  })

  test('drops the connection of a leader that fails with a retriable error and asks again', async () => {
    let failing = true
    const { reader, brokers, seen, times } = makeReader({
      fail: (broker) =>
        broker.host === 'h2' && failing
          ? Object.assign(new Error('not leader'), { retriable: true })
          : null,
    })

    await reader.start()
    const first = await reader.poll()

    expect(first).toBe(3)
    expect(reader.refetches).toBe(1)
    expect(reader.routed).toBe(false)
    expect(times.t).toBe(7)
    expect(brokers.get('h2')[0].disconnect).toHaveBeenCalledTimes(1)

    failing = false
    await reader.poll()

    expect(brokers.get('seed1')).toHaveLength(2)
    expect(reader.routed).toBe(true)
    expect(seen).toHaveLength(3)
  })

  test('records the routing only when the leaders change', async () => {
    const leaders = [1, 1, 2]
    const { reader, record } = makeReader({ leaders })

    await reader.start()
    await reader.start()
    expect(record).toHaveBeenCalledTimes(1)
    expect(record).toHaveBeenCalledWith({ kind: 'reader', event: 'route', leaders: '1,1,2' })

    leaders[2] = 1
    await reader.start()
    expect(record).toHaveBeenCalledTimes(2)
  })

  test('forgets the sockets it made that are destroyed, so a long outage keeps no list of them', async () => {
    const { reader, brokers } = makeReader()
    await reader.start()
    const first = brokers.get('seed1')[0].socket

    first.destroyed = true
    await reader.start()

    expect(reader.sockets.has(first)).toBe(false)
    expect(reader.sockets.size).toBe(1)
  })

  test('lets an error that is not retriable out', async () => {
    const { reader } = makeReader({
      fail: () => Object.assign(new Error('corrupt'), { retriable: false }),
    })

    await reader.start()

    await expect(reader.poll()).rejects.toThrow('corrupt')
  })

  test('tries the next seed when one cannot be reached, and waits for a leader a partition lacks', async () => {
    const down = Object.assign(new Error('refused'), { retriable: true })
    const { reader, open, times } = makeReader({ leaders: [1, -1, 2] })
    const original = open.getMockImplementation()
    open.mockImplementationOnce(async () => {
      throw down
    })
    open.mockImplementation(original)

    await reader.start()
    const read = await reader.poll()

    expect(open.mock.calls[0][0].host).toBe('seed1')
    expect(open.mock.calls[1][0].host).toBe('seed2')
    expect(read).toBe(2)
    expect(reader.routed).toBe(false)
    expect(times.t).toBe(7)
  })

  test('says when no seed answers, and when the topic has no metadata', async () => {
    const refused = Object.assign(new Error('refused'), { retriable: true })
    const a = makeReader()
    a.open.mockRejectedValue(refused)
    await expect(a.reader.start()).rejects.toThrow('refused')

    const b = makeReader()
    const open = b.open.getMockImplementation()
    b.open.mockImplementation(async (options) => {
      const broker = await open(options)
      broker.metadata.mockResolvedValue({ brokers: [], topicMetadata: [] })
      return broker
    })
    await expect(b.reader.start()).rejects.toThrow('No metadata for churn')
  })

  test('drains until every cursor reaches its frozen end, and says which one is short when time runs out', async () => {
    const { reader } = makeReader()
    await reader.start()

    await reader.drain(
      new Map([
        [0, '2'],
        [1, '1'],
      ])
    )
    expect(reader.cursors().get(0)).toBe(2)

    await expect(reader.drain(new Map([[2, '5']]))).rejects.toThrow(
      'The reader is at 0 of partition 2, below its frozen end 5'
    )
  })

  test('keeps its cursors when closed, and every socket it opened is closed with it', async () => {
    const { reader, probe, brokers } = makeReader()
    const baseline = await probe.snapshot()
    await reader.start()
    await reader.poll()

    const open = await probe.snapshot()
    await reader.close()
    const closed = await probe.snapshot()

    expect(open.sockets.open).toBe(3)
    expect(closed.sockets).toEqual({ created: 3, destroyed: 3, open: 0 })
    expect(closed.listeners).toEqual(baseline.listeners)
    expect(
      [...brokers.values()].flat().every(({ disconnect }) => disconnect.mock.calls.length === 1)
    ).toBe(true)
    expect(reader.cursors().get(0)).toBe(2)

    await reader.start()
    expect(await reader.poll()).toBe(0)
  })

  test('says which sockets are its own, also while one is being made', async () => {
    let owned = null
    const { reader, brokers } = makeReader({
      wrap: (inner, readerOf) => (args) => {
        owned = readerOf().owns(undefined)
        return inner(args)
      },
    })

    await reader.start()

    expect(owned).toBe(true)
    expect(reader.owns(brokers.get('seed1')[0].socket)).toBe(true)
    expect(reader.owns({})).toBe(false)
    await reader.close()
    expect(reader.owns(brokers.get('seed1')[0].socket)).toBe(false)
  })
})

describe('Churn write delay', () => {
  const chaosOf = () => {
    const make = () => Object.assign(new EventEmitter(), { destroyed: false })
    return createChaosSocketFactory(make)
  }

  test("delays the client's sockets, open and later, and not the reader's", () => {
    const chaos = chaosOf()
    const readerSockets = new Set()
    const reader = {
      creating: false,
      owns: (socket) => reader.creating || readerSockets.has(socket),
    }
    const producerSocket = chaos({ host: 'h', port: 1 })
    reader.creating = true
    const readerSocket = chaos({ host: 'h', port: 1 })
    reader.creating = false
    readerSockets.add(readerSocket)
    const delay = new WriteDelay({ chaos, reader })

    delay.delayWrites(5000)
    const later = chaos({ host: 'h', port: 1 })
    reader.creating = true
    const laterReader = chaos({ host: 'h', port: 1 })
    reader.creating = false

    expect([producerSocket, later].map(({ writeDelay }) => writeDelay)).toEqual([5000, 5000])
    expect([readerSocket, laterReader].map(({ writeDelay }) => writeDelay)).toEqual([0, 0])
  })

  test('takes the delay off, stops delaying new sockets and forgets destroyed ones', () => {
    const chaos = chaosOf()
    const reader = { owns: () => false }
    const delay = new WriteDelay({ chaos, reader })
    const first = chaos({ host: 'h', port: 1 })
    const second = chaos({ host: 'h', port: 1 })
    delay.delayWrites(5000)
    second.inner.destroyed = true

    delay.clear()
    const after = chaos({ host: 'h', port: 1 })

    expect(first.writeDelay).toBe(0)
    expect(after.writeDelay).toBe(0)
    expect(chaos.sockets).toEqual([first, after])
  })
})

describe('Churn closing probe', () => {
  const OPTIONS = {
    brokers: ['broker:9092'],
    clientId: 'soak',
    topic: 'churn-probe',
    capacity: CAPACITY,
    maxRequestSize: 8388608,
    budgetMs: 200,
    readBackMs: 100,
    quietMs: 100,
  }

  test('sizes the one value charged exactly the whole buffer from the sizes alone', () => {
    expect(exactValueBytes(4194304)).toBe(4192677)
    expect(exactValueBytes(65536)).toBe(63910)
    expect(varintSize(4192677)).toBe(4)
    expect(() => exactValueBytes(1600)).toThrow(
      'No single value is charged exactly bufferMemory 1600'
    )
  })

  test('agrees with the real accounting: the value is charged exactly the capacity', () => {
    const value = Buffer.alloc(exactValueBytes(CAPACITY))

    const charge = chargeOf(
      [{ topic: 'p', messages: [{ key: null, value, timestamp: PROBE_TIMESTAMP }] }],
      CAPACITY
    )

    expect(charge.total).toBe(CAPACITY)
  })

  test("checks that the record fits one request and that the producer's own chunk size agrees", () => {
    const layout = new ClosingProbe(OPTIONS).layout()

    expect(layout).toEqual({
      valueBytes: 4192677,
      charge: CAPACITY,
      wire: 4192704,
      room: 8388608 - 36 - 43 - 16 - 61,
    })
    expect(() => new ClosingProbe({ ...OPTIONS, maxRequestSize: 4192700 }).layout()).toThrow(
      'The probe record needs 4192704 bytes, a request holds'
    )
  })

  describe('over a real buffer', () => {
    let probe
    afterEach(() => probe?.restore())

    const fixture = ({ send, read, flush = async () => {} }) => {
      const admission = new Admission({ capacity: CAPACITY, maxBlockMs: 2000 })
      const cluster = Object.assign(Object.create(Cluster.prototype), { topicPins: new Map() })
      const queue = {
        createSocketRequest: () => ({}),
        canSendSocketRequestImmediately: () => true,
        sendSocketRequest() {},
      }
      const records = []
      probe = new ClosingProbe({
        ...OPTIONS,
        read: read ?? (async () => new Map([[0, { records }]])),
      })
      probe.install()
      const producer = {
        flush,
        send: (args) => send({ args, admission, cluster, queue, records }),
      }
      return { probe, producer, admission, cluster, queue, records }
    }

    const deliver = ({ args, queue, records }, frame = 4200000) => {
      RequestQueue.prototype.push.call(queue, {
        entry: { apiName: 'Produce', correlationId: 1 },
        frameLength: frame,
      })
      records.push({ value: args.messages[0].value, batchContext: { isControlBatch: false } })
      return [{ partition: 0, baseOffset: '0' }]
    }

    const charge = ({ args }) =>
      chargeOf(
        [
          {
            topic: args.topic,
            messages: [{ key: null, value: args.messages[0].value, timestamp: 0 }],
          },
        ],
        CAPACITY
      ).total

    const grant = (parts) => {
      const { admission, cluster, args } = parts
      const reservation = admission.acquire(charge(parts), { remaining: () => 1000 })
      cluster.holdTopic(args.topic)
      return reservation
    }

    const captured = async (parts) => {
      await probe.capture(async () => {
        parts.admission.acquire(1, { remaining: () => 1 }).releaseAll()
        parts.cluster.holdTopic('w')
        parts.cluster.releaseTopic('w')
      })
    }

    const settleSend = async (parts, reservation, frame) => {
      try {
        await Promise.resolve()
        return deliver(parts, frame)
      } finally {
        Promise.resolve(reservation).then((held) => held.releaseAll())
        parts.cluster.releaseTopic(parts.args.topic)
      }
    }

    test('restores every prototype it wrapped, also when restored twice', () => {
      const names = [
        [Admission.prototype, 'acquire'],
        [Cluster.prototype, 'holdTopic'],
        [Cluster.prototype, 'releaseTopic'],
        [Generation.prototype, 'register'],
        [Generation.prototype, 'close'],
        [Accumulator.prototype, 'unsettled'],
        [RequestQueue.prototype, 'push'],
      ]
      const before = names.map(([target, name]) => target[name])
      const wrapped = new ClosingProbe(OPTIONS).install()

      expect(names.map(([target, name]) => target[name])).not.toEqual(before)
      wrapped.restore()
      wrapped.restore()

      expect(names.map(([target, name]) => target[name])).toEqual(before)
    })

    test('binds to the one buffer and cluster the captured send used', async () => {
      const parts = fixture({ send: () => {} })

      await captured(parts)

      expect(probe.admission).toBe(parts.admission)
      expect(probe.cluster).toBe(parts.cluster)
      expect(probe.retained()).toMatchObject({
        available: CAPACITY,
        capacity: CAPACITY,
        reserved: 0,
        pins: 0,
        registrations: 0,
        settlePromises: 0,
      })
    })

    test('refuses a captured send that used no producer, or two', async () => {
      fixture({ send: () => {} })
      await expect(probe.capture(async () => {})).rejects.toThrow(
        'The captured send used 0 buffers and 0 clusters, not one each'
      )

      const second = new Admission({ capacity: 10, maxBlockMs: 0 })
      await expect(
        probe.capture(async () => {
          second.acquire(1, { remaining: () => 1 })
          new Admission({ capacity: 10, maxBlockMs: 0 }).acquire(1, { remaining: () => 1 })
        })
      ).rejects.toThrow('2 buffers and 0 clusters')
    })

    test('counts lifecycle registrations until they end or the lifecycle closes', async () => {
      const parts = fixture({ send: () => {} })
      await captured(parts)
      const generation = new Generation()

      const unregister = generation.register(() => {})
      generation.register(() => {})
      expect(probe.retained().registrations).toBe(2)
      unregister()
      expect(probe.retained().registrations).toBe(1)
      generation.close(new Error('closed'))
      expect(probe.retained().registrations).toBe(0)
      generation.register(() => {})
      expect(probe.retained().registrations).toBe(0)
    })

    test('reads the settlement snapshot of each accumulator', async () => {
      const original = Accumulator.prototype.unsettled
      Accumulator.prototype.unsettled = () => [Promise.resolve(), Promise.resolve()]
      try {
        const parts = fixture({ send: () => {} })
        await captured(parts)

        Accumulator.prototype.unsettled.call({})

        expect(probe.retained().settlePromises).toBe(2)
      } finally {
        probe.restore()
        Accumulator.prototype.unsettled = original
      }
    })

    test('passes a send admitted in its calling turn, delivered and found once, with everything returned', async () => {
      const parts = fixture({ send: (call) => settleSend(call, grant(call)) })
      await captured(parts)

      const observation = await probe.run({ producer: parts.producer, label: 'steady' })

      expect(() => assertProbe(observation)).not.toThrow()
      expect(observation).toMatchObject({
        instanceMatchesWorkload: true,
        calls: 1,
        bytes: CAPACITY,
        inEntry: true,
        reservationImmediate: true,
        acknowledged: true,
        frame: { largest: 4200000, maxRequestSize: 8388608 },
        readBack: { found: 1 },
        error: null,
      })
    })

    test('records a grant in a later turn as not in the calling turn', async () => {
      const parts = fixture({
        send: async (call) => {
          await Promise.resolve()
          return settleSend(call, grant(call))
        },
      })
      await captured(parts)

      const observation = await probe.run({ producer: parts.producer, label: 'steady' })

      expect(observation.inEntry).toBe(false)
      expect(() => assertProbe(observation)).toThrow(/calling turn/)
    })

    test('records a send that had to wait for room as not immediate', async () => {
      const parts = fixture({
        send: (call) => {
          call.admission.debit(100)
          const reservation = grant(call)
          setTimeout(() => call.admission.repay(100), 5)
          return settleSend(call, reservation)
        },
      })
      await captured(parts)

      const observation = await probe.run({ producer: parts.producer, label: 'steady' })

      expect(observation.reservationImmediate).toBe(false)
      expect(() => assertProbe(observation)).toThrow(/waiter/)
    })

    test('records a send that acquires twice, and one that acquires on another buffer', async () => {
      const parts = fixture({
        send: (call) => {
          const reservation = grant(call)
          reservation.releaseAll()
          call.admission.acquire(1, { remaining: () => 1 }).releaseAll()
          return settleSend(call, reservation)
        },
      })
      await captured(parts)
      const twice = await probe.run({ producer: parts.producer, label: 'a' })

      parts.producer.send = (args) => {
        const call = { args, ...parts }
        const other = new Admission({ capacity: CAPACITY, maxBlockMs: 0 })
        call.cluster.holdTopic(args.topic)
        return settleSend(call, other.acquire(charge(call), { remaining: () => 1000 }))
      }
      const other = await probe.run({ producer: parts.producer, label: 'b' })

      expect(twice.calls).toBe(2)
      expect(() => assertProbe(twice)).toThrow(/acquire count/)
      expect(other.instanceMatchesWorkload).toBe(false)
      expect(() => assertProbe(other)).toThrow(/another Admission/)
    })

    test('records a send the producer rejects, and a record the log never shows, as not acknowledged', async () => {
      const parts = fixture({
        send: async (call) => {
          const reservation = grant(call)
          Promise.resolve(reservation).then((held) => held.releaseAll())
          call.cluster.releaseTopic(call.args.topic)
          throw refusal()
        },
      })
      await captured(parts)

      const rejected = await probe.run({ producer: parts.producer, label: 'a' })

      expect(rejected.acknowledged).toBe(false)
      expect(rejected.error).toMatch(/^KafkaJSBufferExhaustedError: /)

      parts.producer.send = (args) => {
        const call = {
          args,
          admission: parts.admission,
          cluster: parts.cluster,
          queue: parts.queue,
          records: parts.records,
        }
        const reservation = grant(call)
        parts.records.length = 0
        return settleSend(call, reservation).then((result) => {
          parts.records.length = 0
          return result
        })
      }
      const absent = await probe.run({ producer: parts.producer, label: 'b' })

      expect(absent.readBack.found).toBe(0)
      expect(absent.acknowledged).toBe(false)
    })

    test('refuses a Produce frame outside the largest request, and one never seen', async () => {
      const parts = fixture({ send: (call) => settleSend(call, grant(call), 9000000) })
      await captured(parts)

      await expect(probe.run({ producer: parts.producer, label: 'a' })).rejects.toThrow(
        "The probe's largest Produce frame was 9000000 bytes"
      )

      parts.producer.send = (args) => {
        const call = {
          args,
          admission: parts.admission,
          cluster: parts.cluster,
          queue: parts.queue,
          records: parts.records,
        }
        const reservation = grant(call)
        parts.records.push({
          value: args.messages[0].value,
          batchContext: { isControlBatch: false },
        })
        Promise.resolve(reservation).then((held) => held.releaseAll())
        call.cluster.releaseTopic(args.topic)
        return Promise.resolve([{ partition: 0, baseOffset: '0' }])
      }
      await expect(probe.run({ producer: parts.producer, label: 'b' })).rejects.toThrow(
        "The probe's largest Produce frame was 0 bytes"
      )
    })

    test('waits for a producer that is still settling and says when it never goes quiet', async () => {
      const parts = fixture({ send: () => {} })
      await captured(parts)
      const reservation = parts.admission.acquire(50, { remaining: () => 1000 })
      setTimeout(() => reservation.releaseAll(), 30)

      await expect(probe.quiet(parts.producer)).resolves.toMatchObject({ reserved: 0 })

      parts.admission.acquire(50, { remaining: () => 1000 })
      await expect(probe.quiet(parts.producer)).rejects.toThrow('The producer is not quiet')
    })

    test('says when its flush leaves unsettled promises, and when it has captured nothing', async () => {
      const original = Accumulator.prototype.unsettled
      Accumulator.prototype.unsettled = () => [Promise.resolve()]
      try {
        const parts = fixture({
          send: () => {},
          flush: async () => {
            Accumulator.prototype.unsettled.call({})
          },
        })
        await captured(parts)

        await expect(probe.quiet(parts.producer)).rejects.toThrow(
          "The producer's flush left 1 unsettled promises"
        )
      } finally {
        probe.restore()
        Accumulator.prototype.unsettled = original
      }

      expect(() => new ClosingProbe(OPTIONS).retained()).toThrow(
        'The probe has not captured a producer'
      )
    })
  })

  describe('the topic', () => {
    const adminOf = (values) => ({
      connect: jest.fn(async () => {}),
      disconnect: jest.fn(async () => {}),
      createTopics: jest.fn(async () => true),
      describeConfigs: jest.fn(async () => ({
        resources: [
          {
            configEntries: Object.entries(values).map(([configName, configValue]) => ({
              configName,
              configValue,
            })),
          },
        ],
      })),
    })

    test('is made with one partition, three replicas, the size limit and no retention, and read back', async () => {
      const admin = adminOf({ 'max.message.bytes': '8388608', 'retention.ms': '-1' })

      await new ClosingProbe(OPTIONS).prepare({ admin: () => admin })

      expect(admin.createTopics).toHaveBeenCalledWith({
        topics: [
          {
            topic: 'churn-probe',
            numPartitions: 1,
            replicationFactor: 3,
            configEntries: [
              { name: 'max.message.bytes', value: '8388608' },
              { name: 'retention.ms', value: '-1' },
            ],
          },
        ],
        waitForLeaders: true,
      })
      expect(admin.disconnect).toHaveBeenCalledTimes(1)
    })

    test('is refused when the broker did not apply a setting', async () => {
      const admin = adminOf({ 'max.message.bytes': '1048576', 'retention.ms': '-1' })

      await expect(new ClosingProbe(OPTIONS).prepare({ admin: () => admin })).rejects.toThrow(
        'The probe topic has max.message.bytes 1048576, not 8388608'
      )
      expect(admin.disconnect).toHaveBeenCalledTimes(1)
    })
  })
})

describe('Churn wiring', () => {
  test('a socket factory that forgets destroyed sockets keeps the chaos list to the live ones', () => {
    const chaos = createChaosSocketFactory(() =>
      Object.assign(new EventEmitter(), { destroyed: false })
    )
    const factory = forgettingDestroyed(chaos)

    const first = factory({ host: 'h', port: 1 })
    const second = factory({ host: 'h', port: 1 })
    first.inner.destroyed = true
    const third = factory({ host: 'h', port: 1 })

    expect(chaos.sockets).toEqual([second, third])
  })

  test.each([
    [
      'a full run',
      null,
      {
        sampleMs: undefined,
        warmupMs: 60000,
        minLatencySamples: undefined,
        minHeapSamples: undefined,
        requiredKinds: undefined,
      },
    ],
    [
      'a smoke run',
      4,
      { sampleMs: 1000, warmupMs: 0, minLatencySamples: 1, minHeapSamples: 2, requiredKinds: [] },
    ],
  ])('builds the owner of %s from the real parts', (_, phaseSeconds, expected) => {
    const phases = PROFILES['producer-churn']
    const churn = buildProducerChurn({ brokers: ['localhost:1'], phaseSeconds, phases })

    expect(churn).toBeInstanceOf(ProducerChurn)
    expect(churn.reader).toBeInstanceOf(RawProducerReader)
    expect(churn.reader.onRecord).toEqual(expect.any(Function))
    expect(churn.faults).toBeInstanceOf(WriteDelay)
    expect(churn.faults.reader).toBe(churn.reader)
    expect(churn.admissionProbe).toBeInstanceOf(ClosingProbe)
    expect(churn.admissionProbe).toMatchObject({
      capacity: CHURN.producer.bufferMemory,
      maxRequestSize: CHURN.producer.maxRequestSize,
      clientId: 'soak',
    })
    expect(churn.admissionProbe.topic).toBe(`${churn.topic}-probe`)
    expect(churn.bouncer.rollingBounce).toEqual(expect.any(Function))
    expect(churn.settings.minLatencySamples).toBe(
      expected.minLatencySamples ?? CHURN.minLatencySamples
    )
    expect(churn.settings.minHeapSamples).toBe(expected.minHeapSamples ?? CHURN.minHeapSamples)
    expect(churn.settings.requiredKinds).toEqual(expected.requiredKinds ?? ['clean', 'hard'])
    expect(churn.soak.sampleMs).toBe(expected.sampleMs)
    expect(churn.soak.warmupMs).toBe(expected.warmupMs)
    expect(churn.phases).toBe(phases)
  })

  test('has the settings of the plan: one idempotent producer, a 4 MiB buffer, 2000 sends a second', () => {
    expect(CHURN).toMatchObject({
      rate: 2000,
      maxUnsettled: 8192,
      steadyBytes: 200,
      pressureBytes: 1024,
      pressureDelayMs: 5000,
      partitions: 6,
      minInsyncReplicas: 2,
      minLatencySamples: 20000,
      minHeapSamples: 6,
      producer: {
        idempotent: true,
        bufferMemory: 4194304,
        maxBlockMs: 2000,
        maxRequestSize: 8388608,
        deliveryTimeout: 120000,
      },
    })
  })
})
