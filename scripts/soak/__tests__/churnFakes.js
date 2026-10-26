const { KafkaJSBufferExhaustedError } = require('../../../index')
const { PROFILES, ProducerChurn } = require('../soak')

const PARTITIONS = 6
const CAPACITY = 4194304

/** Time that moves only when something sleeps, and a sleep that yields to the real loop once. */
const fakeTime = (onSleep = () => {}) => {
  const clock = {
    t: 0,
    now() {
      return this.t
    },
  }
  const sleep = async (ms, signal) => {
    if (signal?.aborted) return
    clock.t += ms
    onSleep(clock)
    await new Promise((resolve) => setTimeout(resolve, 1))
  }
  return { clock, sleep }
}

/** A topic's partitions as the raw reads see them: decoded records, one batch each. */
class FakeLog {
  constructor(partitions = PARTITIONS) {
    this.partitions = Array.from({ length: partitions }, () => [])
  }

  append(partition, value, { pid = 1, epoch = 0, sequence } = {}) {
    const records = this.partitions[partition]
    const offset = records.length
    records.push({
      offset: String(offset),
      value,
      batchContext: {
        firstOffset: String(offset),
        firstSequence: sequence ?? offset,
        producerId: String(pid),
        producerEpoch: epoch,
        isControlBatch: false,
      },
    })
    return offset
  }

  ends() {
    return new Map(this.partitions.map((records, partition) => [partition, String(records.length)]))
  }

  read() {
    return new Map(
      this.partitions.map((records, partition) => [
        partition,
        { endOffset: String(records.length), records: [...records], abortedTransactions: [] },
      ])
    )
  }
}

const refusal = (reason = 'timeout') =>
  new KafkaJSBufferExhaustedError('The buffer is full', {
    reason,
    requested: 1,
    available: 0,
    capacity: CAPACITY,
    waitedMs: 2000,
  })

/**
 * A producer that appends to a `FakeLog`. `plan(id, producer, value)` names what a send does: `ack`,
 * `lost` (acknowledged, never in the log), `refuse`, `unsent`, `unknown-written`,
 * `unknown-absent`, `unknown-twice` or `unexpected`; or `{ twin: id }`, acknowledged with the
 * identity of another id's record.
 */
class FakeProducer {
  constructor({ log, plan = () => 'ack' }) {
    this.log = log
    this.plan = plan
    this.events = { REQUEST: 'producer.request' }
    this.connects = 0
    this.disconnects = 0
    this.connected = false
    this.delayed = false
    this.pid = 100
    this.hold = false
    this.held = []
    this.peak = 0
    this.sends = 0
    this.identities = new Map()
  }

  on(event, listener) {
    this.listener = listener
    return () => {
      this.listener = null
    }
  }

  async connect() {
    this.connects += 1
    this.connected = true
    this.pid += 1
    this.sequences = Array(PARTITIONS).fill(0)
    this.listener?.({ payload: { apiName: 'InitProducerId', apiVersion: 5 } })
    this.listener?.({ payload: { apiName: 'Produce', apiVersion: 11 } })
  }

  async disconnect() {
    this.disconnects += 1
    this.connected = false
  }

  async flush() {}

  release() {
    for (const settle of this.held.splice(0)) settle()
  }

  send({ topic, messages: [{ value }] }) {
    this.sends += 1
    const id = value.readUInt32BE(0)
    const run = () => this.act(id, topic, value)
    if (!this.hold) return Promise.resolve().then(run)
    return new Promise((resolve, reject) => {
      this.held.push(() => Promise.resolve().then(run).then(resolve, reject))
      this.peak = Math.max(this.peak, this.held.length)
    })
  }

  place(id, value, twin) {
    const partition = id % PARTITIONS
    const identity = twin === undefined ? undefined : this.identities.get(twin)
    const sequence = identity?.sequence ?? this.sequences[partition]++
    const at = { pid: this.pid, epoch: 0, sequence }
    this.identities.set(id, { partition, sequence })
    return { partition, offset: this.log.append(partition, value, at) }
  }

  act(id, topic, value) {
    const action = this.plan(id, this, value)
    const kind = typeof action === 'string' ? action : 'ack'
    const landed = ({ partition, offset }) => [
      { topicName: topic, partition, baseOffset: String(offset) },
    ]
    switch (kind) {
      case 'refuse':
        throw refusal()
      case 'refuse-full':
        throw refusal('queue-full')
      case 'unsent':
        throw Object.assign(new Error('Not sent'), { outcome: 'unsent' })
      case 'unexpected':
        throw new Error('Something else')
      case 'unknown-absent':
        throw Object.assign(new Error('Maybe sent'), { outcome: 'unknown' })
      case 'unknown-written':
        this.place(id, value)
        throw Object.assign(new Error('Maybe sent'), { outcome: 'unknown' })
      case 'unknown-twice':
        this.place(id, value)
        this.place(id, value)
        throw Object.assign(new Error('Maybe sent'), { outcome: 'unknown' })
      case 'lost': {
        const partition = id % PARTITIONS
        return landed({ partition, offset: this.log.partitions[partition].length })
      }
      default:
        return landed(this.place(id, value, action.twin))
    }
  }
}

/** A reader that follows a `FakeLog`; `stall` makes it stop short and say nothing. */
class FakeReader {
  constructor({ log, stall = false }) {
    this.log = log
    this.stall = stall
    this.onRecord = () => {}
    this.cursor = Array(PARTITIONS).fill(0)
    this.starts = 0
    this.closes = 0
    this.polling = false
  }

  async start() {
    this.starts += 1
  }

  async poll() {
    this.polling = true
    await Promise.resolve()
    let read = 0
    this.log.partitions.forEach((records, partition) => {
      for (; this.cursor[partition] < records.length; this.cursor[partition]++) {
        if (this.stall && this.cursor[partition] >= 1) break
        this.onRecord(partition, records[this.cursor[partition]])
        read += 1
      }
    })
    this.polling = false
    return read
  }

  async drain(ends) {
    if (this.stall) return
    for (;;) {
      if ([...ends].every(([partition, end]) => this.cursor[partition] >= Number(end))) return
      await this.poll()
    }
  }

  async close() {
    this.closes += 1
  }

  cursors() {
    return new Map(this.cursor.map((offset, partition) => [partition, offset]))
  }

  owns() {
    return false
  }
}

/** A bouncer that takes one node down for a virtual moment and restores it in a `finally`. */
class FakeBouncer {
  constructor({ sleep }) {
    this.sleep = sleep
    this.calls = []
    this.restored = []
    this.failAt = null
  }

  async rollingBounce({ hard, admin, topics, services, signal }) {
    signal?.throwIfAborted()
    const [service] = services
    this.calls.push({ hard, service, topics, admin })
    try {
      await this.sleep(5)
      if (this.failAt === this.calls.length) throw new Error(`stopping ${service} failed`)
    } finally {
      this.restored.push(service)
    }
    return [{ service, kind: hard ? 'kill' : 'stop', tDown: 0, tRestored: 5, outageMs: 5 }]
  }
}

const goodObservation = (label = 'steady') => ({
  label,
  instanceMatchesWorkload: true,
  calls: 1,
  bytes: CAPACITY,
  capacity: CAPACITY,
  inEntry: true,
  reservationImmediate: true,
  acknowledged: true,
  available: CAPACITY,
  reserved: 0,
  waitingBytes: 0,
  debts: 0,
  pins: 0,
  registrations: 0,
  settlePromises: 0,
})

class FakeProbe {
  constructor({ observe = goodObservation, retained = () => goodObservation() } = {}) {
    this.observe = observe
    this.held = retained
    this.installs = 0
    this.restores = 0
    this.runs = []
  }

  install() {
    this.installs += 1
  }

  restore() {
    this.restores += 1
  }

  async prepare() {}

  capture(work) {
    return work()
  }

  async run({ label }) {
    this.runs.push(label)
    return this.observe(label)
  }

  retained() {
    return this.held()
  }
}

class MemoryJournal {
  constructor() {
    this.entries = []
    this.closes = 0
  }

  append(...entry) {
    this.entries.push(entry.slice(0, 5))
  }

  replay(add) {
    for (const entry of this.entries) add(...entry)
    return this.entries.length
  }

  close() {
    this.closes += 1
  }
}

const MIB = 1024 * 1024

/** A quiet reading with a flat heap. */
const quiet = (overrides = {}) => ({
  listeners: { requestQueues: 0, sockets: 0 },
  sockets: { created: 4, destroyed: 4, open: 0 },
  queueDepth: 0,
  ledgerSize: 0,
  obligationSize: 0,
  activeResources: { Timeout: 1 },
  heapUsed: 50 * MIB,
  ...overrides,
})

/**
 * A producer-churn run over fake ports, in virtual time: short phases, a rate of 100 a second, and
 * a sampler that ticks every 5 ms. Every port can be replaced or reached.
 * @param {object} [options]
 * @returns {object} the churn and every port
 */
const makeChurn = ({
  plan = (id, producer) => (producer.delayed ? 'refuse' : 'ack'),
  settings = {},
  phaseMs = 1000,
  soak = { sampleMs: 5, warmupMs: 0, settleMs: 50 },
  stall = false,
  makeProducer = (parts) => new FakeProducer(parts),
  makeProbe = () => new FakeProbe(),
  probe,
  listeners,
  journal = new MemoryJournal(),
  rawLog,
  snapshot,
  onSleep,
} = {}) => {
  const { clock, sleep } = fakeTime((time) => onSleep?.(time, { producer }))
  const log = new FakeLog()
  const producer = makeProducer({ log, plan })
  const admissionProbe = probe ?? makeProbe({ producer })
  const reader = new FakeReader({ log, stall })
  const bouncer = new FakeBouncer({ sleep })
  const admin = {
    connect: jest.fn(async () => {}),
    disconnect: jest.fn(async () => {}),
    createTopics: jest.fn(async () => true),
    describeConfigs: jest.fn(async () => ({
      resources: [
        {
          configEntries: [
            { configName: 'max.message.bytes', configValue: '8388608' },
            { configName: 'retention.ms', configValue: '-1' },
          ],
        },
      ],
    })),
  }
  const kafka = {
    producer: jest.fn(() => producer),
    admin: jest.fn(() => admin),
    consumer: jest.fn(() => {
      throw new Error('A consumer was built')
    }),
  }
  const faults = {
    delayWrites: jest.fn(() => {
      producer.delayed = true
    }),
    clear: jest.fn(() => {
      producer.delayed = false
    }),
  }
  const lines = []
  const churn = new ProducerChurn({
    kafka,
    probe: { snapshot: snapshot ?? (async () => quiet()) },
    brokers: ['broker:9092'],
    topic: 'churn',
    record: (line) => lines.push(JSON.parse(JSON.stringify(line))),
    phases: PROFILES['producer-churn'].map((phase) => ({ ...phase, ms: phaseMs })),
    bouncer,
    reader,
    admissionProbe,
    faults,
    listeners,
    journal,
    clock,
    sleep,
    rawLog: rawLog ?? { endOffsets: async () => log.ends(), readLog: async () => log.read() },
    soak,
    settings: { rate: 100, pollMs: 1, minLatencySamples: 1, latencySampleEvery: 1, ...settings },
  })
  return {
    churn,
    clock,
    sleep,
    log,
    producer,
    reader,
    bouncer,
    admin,
    kafka,
    faults,
    lines,
    probe: admissionProbe,
    journal,
  }
}

module.exports = {
  CAPACITY,
  FakeBouncer,
  FakeLog,
  FakeProbe,
  FakeProducer,
  FakeReader,
  MemoryJournal,
  PARTITIONS,
  fakeTime,
  goodObservation,
  makeChurn,
  quiet,
  refusal,
}
