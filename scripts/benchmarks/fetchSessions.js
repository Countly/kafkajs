const fs = require('fs')
const net = require('net')
const path = require('path')
const { randomUUID } = require('crypto')
const { Clients } = require('../clients')
const {
  FetchFrameCounter,
  DeliveryAccounting,
  auditTranscript,
  verifyFrames,
} = require('./fetchFrameCounter')

/**
 * Owns raw socket evidence and an independent primary Fetch byte counter.
 * Observations include every API so response correlation excludes unrelated replies.
 * @class
 */
class FetchObservation {
  /**
   * @param {object} [options] frame callbacks and optional off-heap evidence journal
   * @param {Function} [options.onFrame] primary completed-frame observer
   * @param {FrameJournal} [options.journal] finite owned raw journal
   * @returns {FetchObservation} empty per-run observer
   * @throws Never.
   */
  constructor({ onFrame = () => {}, journal } = {}) {
    this.journal = journal
    this.counter = new FetchFrameCounter(undefined, onFrame)
    this.transcript = []
    this.serial = 0
    this.error = null
  }

  /**
   * Records the unmodified chunk before invoking the primary counter.
   * @param {string} connection nonreused socket identity
   * @param {number} nodeId broker identity, independent of connection identity
   * @param {'request'|'response'} direction observation direction
   * @param {Buffer} chunk actual socket bytes
   * @returns {void} independent evidence and primary counters
   * @throws {Error} malformed primary frame
   */
  push(connection, nodeId, direction, chunk) {
    const at = performance.now()
    const entry = {
      connection,
      nodeId,
      direction,
      chunk: Buffer.from(chunk),
      at,
    }
    if (this.journal) this.journal.append(entry)
    else this.transcript.push(entry)
    this.counter.push(connection, direction, chunk, at)
  }

  /**
   * Wraps a public socketFactory without changing write callbacks or backpressure.
   * Attach before the client registers its response listener. A reconnect gets a new key.
   * @param {Function} base existing public socketFactory
   * @param {Function} nodeFor maps host/port to broker ID
   * @returns {Function} factory for Kafka constructor's socketFactory option
   * @throws {Error} underlying factory or invalid observation
   */
  socketFactory(base, nodeFor) {
    return (options) => {
      const socket = base(options)
      const connection = String(++this.serial)
      const nodeId = nodeFor(options)
      const write = socket.write
      socket.write = (...args) => {
        this.push(
          connection,
          nodeId,
          'request',
          Buffer.from(args[0], typeof args[1] === 'string' ? args[1] : undefined)
        )
        try {
          return write.apply(socket, args)
        } catch (error) {
          this.error = error
          throw error
        }
      }
      const data = (chunk) => this.push(connection, nodeId, 'response', chunk)
      socket.prependListener('data', data)
      socket.once('close', () => {
        socket.removeListener('data', data)
        socket.write = write
      })
      return socket
    }
  }

  /**
   * Checks the complete raw transcript independently against primary counters.
   * @param {{sessions?:boolean}} [options] require real Fetch body headers
   * @returns {object} counted frames and transcript totals
   * @throws {Error} partial window or mismatched counters
   */
  result({ sessions = false } = {}) {
    if (this.error) throw this.error
    const audit = auditTranscript(this.journal ? this.journal.read() : this.transcript, {
      sessions,
    })
    verifyFrames(this.counter.result(), audit)
    return audit
  }
}

/**
 * Verifies a declared broker cycle using independently counted request frames.
 * This readiness control checks denominators; session-body correctness belongs later.
 * @param {object[]} frames independent counted transcript
 * @param {{full:boolean,nodes?:number[]}} options cycle mode and broker IDs
 * @returns {number} total request bytes across the whole broker cycle
 * @throws {Error} wrong broker cycle, full size or idle incremental request size
 */
function verifyCycle(frames, { full, nodes = [0, 1, 2] }) {
  const requests = frames.filter((frame) => frame.direction === 'request')
  if (
    requests.length !== nodes.length ||
    nodes.some((node) => requests.filter((frame) => frame.nodeId === node).length !== 1)
  )
    throw new Error('Incomplete broker cycle')
  const bytes = requests.reduce((total, frame) => total + frame.bytes, 0)
  if (full && bytes < 20000) throw new Error('Full cycle below 20000 request bytes')
  if (!full && requests.some((frame) => frame.bytes > 128))
    throw new Error('Idle incremental request above 128 bytes')
  return bytes
}

/**
 * Runs finite, offline frame-denominator controls, using independent header literals.
 * @returns {object} fixed 334/333/333 partition shape and observed full-cycle bytes
 * @throws {Error} broken frame accounting or cycle denominator
 */
function control() {
  const observation = new FetchObservation()
  const incremental = new FetchObservation()
  for (const [nodeId, partitions] of [334, 333, 333].entries()) {
    for (const [count, epoch, id, target] of [
      [partitions, 0, 0, observation],
      [0, 1, 42, incremental],
    ]) {
      const header = Buffer.alloc(22)
      header.writeInt16BE(1, 4)
      header.writeInt16BE(12, 6)
      header.writeInt32BE(7, 8)
      header.writeInt16BE(7, 12)
      header.write('fs-gate', 14)
      const body = Buffer.alloc(25)
      body.writeInt32BE(-1)
      body.writeInt32BE(500, 4)
      body.writeInt32BE(1, 8)
      body.writeInt32BE(10485760, 12)
      body[16] = 1
      body.writeInt32BE(id, 17)
      body.writeInt32BE(epoch, 21)
      const topic = []
      if (count) {
        const length = Buffer.from([((count + 1) % 128) + 128, Math.floor((count + 1) / 128)])
        topic.push(Buffer.from([2, 2, 116]), length)
        for (let partition = 0; partition < count; partition++) {
          const row = Buffer.alloc(33)
          row.writeInt32BE(partition)
          row.writeInt32BE(2, 4)
          row.writeBigInt64BE(5n, 8)
          row.writeInt32BE(-1, 16)
          row.writeBigInt64BE(-1n, 20)
          row.writeInt32BE(32768, 28)
          topic.push(row)
        }
        topic.push(Buffer.from([0]))
      } else topic.push(Buffer.from([1]))
      const request = Buffer.concat([header, body, ...topic, Buffer.from([1, 1, 0])])
      request.writeInt32BE(request.length - 4)
      const response = Buffer.alloc(21)
      response.writeInt32BE(17)
      response.writeInt32BE(7, 4)
      response.writeInt32BE(42, 15)
      response[19] = 1
      target.push(String(nodeId), nodeId, 'request', request)
      target.push(String(nodeId), nodeId, 'response', response)
    }
  }
  const result = observation.result({ sessions: true })
  const idle = incremental.result({ sessions: true })
  const actualShape = result.frames
    .filter((f) => f.direction === 'request')
    .map((f) => f.partitions)
  if (JSON.stringify(actualShape) !== '[334,333,333]') throw new Error('Wrong full partition cycle')
  if (
    !idle.frames
      .filter((f) => f.direction === 'request')
      .every((f) => f.kind === 'incremental' && f.partitions === 0)
  )
    throw new Error('Idle control is not incremental')
  return {
    shape: actualShape,
    requestBytes: verifyCycle(result.frames, { full: true }),
    incrementalRequestBytes: verifyCycle(idle.frames, { full: false }),
    ...result,
  }
}

/**
 * Counts one fixed measurement window, independently from wire observations.
 * Teardown, audit parsing and memory sensitivity allocation are outside this window.
 */
class SessionMeasurement {
  /**
   * @param {object} [options] deterministic driver inputs
   * @param {'eachBatch'|'eachMessage'} [options.handler] callback arm
   * @param {Function} [options.now] monotonic milliseconds
   * @param {Function} [options.cpu] process.cpuUsage-compatible snapshots
   * @returns {SessionMeasurement} an unstarted sample
   * @throws Never for valid options.
   */
  constructor({
    handler = 'eachBatch',
    now = () => performance.now(),
    cpu = () => process.cpuUsage(),
  } = {}) {
    this.accounting = new DeliveryAccounting(handler)
    this.now = now
    this.cpu = cpu
    this.started = null
    this.ended = null
    this.frames = { bytes: 0, requests: 0, responses: 0 }
  }

  /** @returns {void} starts exactly once @throws {Error} repeat start. */
  begin() {
    if (this.started !== null) throw new Error('Measurement already started')
    this.started = this.now()
    this.cpuStart = this.cpu()
  }

  /**
   * @param {number} records actual callback batch length or one for eachMessage
   * @returns {void} counts only entries in the active interval
   * @throws {Error} invalid record count
   */
  enter(records) {
    if (this.started === null || this.ended !== null) return
    if (!Number.isSafeInteger(records) || records < 0)
      throw new Error('Invalid delivered record count')
    this.accounting.enter(records, BigInt(Math.floor(this.now() * 1e6)))
  }

  /**
   * @param {object} frame complete primary frame, timestamped at last observed byte
   * @returns {void} counts Fetch requests and replies only in this interval
   * @throws Never for normalized frame records.
   */
  frame(frame) {
    if (
      frame.apiKey !== 1 ||
      this.started === null ||
      this.ended !== null ||
      frame.at < this.started
    )
      return
    this.frames.bytes += frame.bytes
    this.frames[frame.direction === 'request' ? 'requests' : 'responses']++
  }

  /**
   * Freezes counts, elapsed time and user+system CPU before any close or audit.
   * @returns {object} sample with message/idle denominators kept distinct
   * @throws {Error} zero-length or unstarted interval
   */
  finish() {
    if (this.started === null || this.ended !== null)
      throw new Error('Invalid measurement interval')
    this.ended = this.now()
    const end = this.cpu()
    const elapsedMs = this.ended - this.started
    if (elapsedMs <= 0) throw new Error('Empty measurement interval')
    const counts = this.accounting.result()
    const cpuMicros = end.user - this.cpuStart.user + end.system - this.cpuStart.system
    const messages = counts.deliveredMessages
    return {
      ...counts,
      ...this.frames,
      elapsedMs,
      cpuMicros,
      messagesPerSecond: (messages * 1000) / elapsedMs,
      bytesPerSecond: (this.frames.bytes * 1000) / elapsedMs,
      cpuMicrosPerSecond: (cpuMicros * 1000) / elapsedMs,
      bytesPerMessage: messages ? this.frames.bytes / messages : null,
      cpuMicrosPerMessage: messages ? cpuMicros / messages : null,
    }
  }
}

/**
 * Refuses a sample with independently declared record/callback/gap shortfalls.
 * @param {object} sample measured result
 * @param {object} expected separately generated source transcript and arm requirements
 * @returns {void} valid exact counts and sufficient actual samples
 * @throws {Error} incorrect work or insufficient actual gaps
 */
function verifyDelivery(sample, expected) {
  for (const key of ['deliveredMessages', 'handlerEntries', 'gapSamples']) {
    if (sample[key] !== expected[key]) throw new Error(`Delivery accounting mismatch: ${key}`)
  }
  if (sample.gapSamples < (expected.minimumGaps || 0)) throw new Error('Insufficient actual gaps')
}

/**
 * Totals the independently audited Fetch frames completed in the half-open window.
 * @param {{at:number,bytes:number,direction:string}[]} frames audited Fetch frames
 * @param {number} started window start, inclusive
 * @param {number} ended window end, exclusive
 * @returns {{bytes:number,requests:number,responses:number}} totals comparable with the primary sample
 * @throws Never.
 */
function windowTotals(frames, started, ended) {
  const selected = frames.filter((frame) => frame.at >= started && frame.at < ended)
  return {
    bytes: selected.reduce((sum, frame) => sum + frame.bytes, 0),
    requests: selected.filter((frame) => frame.direction === 'request').length,
    responses: selected.filter((frame) => frame.direction === 'response').length,
  }
}

/**
 * Reads retained heap after the same two collections, 100 ms apart.
 * @param {object} [options] GC and heap inputs, injectable only in offline controls
 * @returns {Promise<number>} observed retained bytes
 * @throws {Error} absent GC or invalid heap observation
 */
async function retained({
  gc = global.gc,
  heap = () => process.memoryUsage().heapUsed,
  pause = (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
} = {}) {
  if (typeof gc !== 'function') throw new Error('Retained heap requires --expose-gc')
  gc()
  await pause(100)
  gc()
  const bytes = heap()
  if (!Number.isFinite(bytes) || bytes <= 0) throw new Error('Invalid retained heap reading')
  return bytes
}

/**
 * Tests the real heap observer with a deliberately large retained object array.
 * Run untimed after ordinary candidate/sessionless readings, using identical GC spacing.
 * @param {object} [options] same observer inputs as retained()
 * @returns {Promise<number>} observed retained increase, strictly above 3 MiB
 * @throws {Error} blind observer or inadequate retained increase
 */
async function memoryControl(options) {
  const before = await retained(options)
  const held = Array.from({ length: 2097152 }, (_, value) => ({ value }))
  const after = await retained(options)
  if (held.length !== 2097152 || held[2097151].value !== 2097151)
    throw new Error('Incomplete memory control')
  const delta = after - before
  if (delta <= 3 * 1024 * 1024) throw new Error('Retained memory observer is blind')
  return delta
}

/**
 * Supplies an untimed same-record-count control with independently declared batch sizes.
 * @returns {object[]} three samples with 12 messages but different actual gaps
 * @throws {Error} a denominator or callback count is wrong
 */
function deliveryControl() {
  return [[6, 6], [3, 3, 3, 3], Array(12).fill(1)].map((sizes) => {
    let clock = 0
    const m = new SessionMeasurement({
      now: () => clock,
      cpu: () => ({ user: clock * 2, system: clock }),
    })
    m.begin()
    for (const records of sizes) {
      clock++
      m.enter(records)
    }
    clock++
    const sample = m.finish()
    verifyDelivery(sample, {
      deliveredMessages: 12,
      handlerEntries: sizes.length,
      gapSamples: sizes.length - 1,
    })
    return sample
  })
}

/**
 * Runs one public consumer arm. Corpus production and topic placement belong to
 * the separate producer/controller process; this process measures consumer CPU only.
 * @param {object} kafka public Kafka instance, already configured with observed sockets
 * @param {object} options fixed arm, topic, sessions and independently owned measurement
 * @param {string} options.arm S1, S2, L1, L2 or L3
 * @param {string} options.topic externally prepared topic
 * @param {boolean} options.sessions explicit candidate setting
 * @param {SessionMeasurement} options.measurement same frame and delivery window
 * @param {Function} [options.wait] untimed warmup/window wait, injectable for finite controls
 * @param {Function} [options.onWindowStart] producer IPC handshake for the L2 window
 * @param {Function} [options.readRetained] identical double-GC observer
 * @returns {Promise<object>} frozen sample, with memory sampled while consumer is retained
 * @throws {Error} startup, drain, missing workload, insufficient samples or cleanup failure
 */
async function runArm(
  kafka,
  {
    arm,
    topic,
    sessions,
    measurement,
    wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
    onWindowStart = () => {},
    readRetained = () => retained(),
  }
) {
  if (!['S1', 'S2', 'L1', 'L2', 'L3'].includes(arm) || typeof sessions !== 'boolean')
    throw new Error('Invalid session measurement arm')
  const active = arm === 'S1' || arm === 'L1'
  const target = arm === 'S1' ? 200001 : arm === 'L1' ? 200000 : null
  let result
  await Clients.use(kafka, async (clients) => {
    const consumer = clients.consumer({
      groupId: `fs-gate-${randomUUID()}`,
      fetchSessions: sessions,
      maxWaitTimeInMs: 500,
      maxBytesPerPartition: 32768,
    })
    await consumer.connect()
    await consumer.subscribe({ topics: [topic], fromBeginning: true })
    let resolveDone, rejectDone, timer
    const done = new Promise((resolve, reject) => {
      resolveDone = resolve
      rejectDone = reject
    })
    done.catch(() => {})
    let count = 0
    try {
      if (active) {
        measurement.begin()
        timer = setTimeout(
          () => rejectDone(new Error(`Session corpus shortfall: ${count}/${target}`)),
          180000
        )
      }
      await consumer.run({
        autoCommit: false,
        eachBatch: async ({ batch }) => {
          measurement.enter(batch.messages.length)
          if (measurement.started === null || measurement.ended !== null) return
          count += batch.messages.length
          if (active && count >= target) {
            try {
              result = measurement.finish()
              resolveDone()
            } catch (error) {
              rejectDone(error)
            }
          }
        },
      })
      if (active) {
        await done
        clearTimeout(timer)
        verifyDelivery(result, {
          deliveredMessages: target,
          handlerEntries: result.deliveredBatches,
          gapSamples: result.deliveredBatches - 1,
          minimumGaps: 1000,
        })
      } else {
        await wait(10000)
        measurement.begin()
        await onWindowStart()
        await wait(60000)
        result = measurement.finish()
        if ((arm === 'S2' || arm === 'L3') && result.deliveredMessages !== 0)
          throw new Error('Idle arm received records')
        if (arm === 'L2' && result.deliveredMessages === 0)
          throw new Error('Mostly idle arm received no records')
      }
      result.heapRetainedBytes = await readRetained()
      await consumer.stop()
    } finally {
      clearTimeout(timer)
    }
  })
  return result
}

/**
 * Spools raw evidence outside retained V8 arrays. Append overhead is included in
 * consumer CPU for every compared arm. Audit replay occurs after measured GC.
 */
class FrameJournal {
  /**
   * @param {string} file unique controller-owned evidence file, created exclusively
   * @returns {FrameJournal} open raw journal
   * @throws {Error} absent directory, existing file or I/O failure
   */
  constructor(file) {
    this.file = file
    this.fd = fs.openSync(file, 'wx', 0o600)
  }

  /** @param {object} entry timestamped raw socket chunk @returns {void} persisted evidence @throws {Error} I/O failure. */
  append(entry) {
    fs.writeSync(
      this.fd,
      JSON.stringify({ ...entry, chunk: entry.chunk.toString('base64') }) + '\n'
    )
  }

  /** @returns {object[]} finite replay after measurement @throws {Error} incomplete or invalid journal. */
  read() {
    return fs
      .readFileSync(this.file, 'utf8')
      .trim()
      .split('\n')
      .filter(Boolean)
      .map((line) => {
        const entry = JSON.parse(line)
        return { ...entry, chunk: Buffer.from(entry.chunk, 'base64') }
      })
  }

  /** @returns {void} closes once, retains evidence file @throws {Error} I/O failure. */
  close() {
    if (this.fd !== null) {
      fs.closeSync(this.fd)
      this.fd = null
    }
  }
}

/**
 * Runs socket-free correctness controls or one explicitly selected public live arm.
 * Live entry requires the controller's node inventory, owned directory and separate
 * producer IPC for L2. It is never invoked locally by the plan writer.
 * @param {string[]} [args] --control, or --arm A --sessions false|true --topic name
 * @returns {Promise<void>} one measured JSON record and independently audited evidence
 * @throws {Error} malformed options, absent GC/IPC/workload or failed observation
 */
async function main(args = process.argv.slice(2)) {
  if (args.length === 1 && args[0] === '--control') {
    process.stdout.write(
      JSON.stringify({
        frames: control(),
        delivery: deliveryControl(),
        retainedControlBytes: await memoryControl(),
      }) + '\n'
    )
    return
  }
  if (
    args.length !== 6 ||
    args[0] !== '--arm' ||
    args[2] !== '--sessions' ||
    args[4] !== '--topic' ||
    !['false', 'true'].includes(args[3])
  )
    throw new Error('Invalid session driver arguments')
  const { Kafka, logLevel } = require('../../index')
  const arm = args[1]
  const sessions = args[3] === 'true'
  const topic = args[5]
  const nodes = JSON.parse(process.env.BENCH_NODES || 'null')
  const directory = process.env.BENCH_EVIDENCE_DIR
  if (
    !Array.isArray(nodes) ||
    nodes.length !== 3 ||
    !directory ||
    !path.isAbsolute(directory) ||
    !fs.statSync(directory).isDirectory()
  )
    throw new Error('Missing benchmark node inventory or owned evidence directory')
  const nodeFor = ({ host, port }) => {
    const node = nodes.find((node) => node.host === host && Number(node.port) === Number(port))
    if (!node) throw new Error('Socket is outside declared broker inventory')
    return node.nodeId
  }
  const measurement = new SessionMeasurement()
  const journal = new FrameJournal(path.join(directory, `fetch-frames-${randomUUID()}.jsonl`))
  const observation = new FetchObservation({
    onFrame: (frame) => measurement.frame(frame),
    journal,
  })
  const socketFactory = observation.socketFactory(({ host, port, ssl, onConnect }) => {
    if (ssl) throw new Error('This controlled benchmark requires its declared plaintext cluster')
    return net.connect({ host, port }, onConnect)
  }, nodeFor)
  try {
    const kafka = new Kafka({
      clientId: 'fs-gate',
      brokers: nodes.map((n) => `${n.host}:${n.port}`),
      requestTimeout: 5000,
      logLevel: logLevel.NOTHING,
      socketFactory,
    })
    const sample = await runArm(kafka, {
      arm,
      sessions,
      topic,
      measurement,
      onWindowStart: async () => {
        if (arm !== 'L2') return
        if (!process.send) throw new Error('Mostly idle producer requires controller IPC')
        await new Promise((resolve, reject) =>
          process.send({ type: 'measurement-ready', started: measurement.started }, (error) =>
            error ? reject(error) : resolve()
          )
        )
      },
    })
    const audit = observation.result({ sessions: true })
    const window = windowTotals(audit.frames, measurement.started, measurement.ended)
    verifyFrames(sample, window)
    process.stdout.write(
      JSON.stringify({
        arm,
        sessions,
        topic,
        sample,
        window,
        journal: journal.file,
        started: measurement.started,
        ended: measurement.ended,
      }) + '\n'
    )
  } finally {
    journal.close()
  }
}
/**
 * Applies fixed predeclared numerical gates only after unconditional correctness.
 * A missing control, sample or arm is inconclusive and cannot select default on.
 * @param {object} evidence independent correctness, controls and same-session samples
 * @param {'passed'|'failed'|'unrun'} evidence.correctness forced-on matrix disposition
 * @param {object} evidence.arms S1/S2/L1/L2/L3 paired {off,on} samples
 * @param {boolean} evidence.controls all transcript, memory and sample controls pass
 * @param {number[]} evidence.aaDeltas identical-tree/config relative deltas, each arm/setting
 * @returns {'blocked'|'false'|'true'} shipping/default decision, never a source mutation
 * @throws Never for missing evidence.
 */
function selectDefault({ correctness, arms, controls, aaDeltas } = {}) {
  if (correctness !== 'passed') return 'blocked'
  if (
    !controls ||
    !arms ||
    !Array.isArray(aaDeltas) ||
    aaDeltas.length !== 10 ||
    aaDeltas.some((delta) => !Number.isFinite(delta) || Math.abs(delta) > 0.03)
  )
    return 'false'
  const ratio = (arm, metric, limit, lower = false) => {
    const off = arms[arm]?.off?.[metric]
    const on = arms[arm]?.on?.[metric]
    if (!Number.isFinite(off) || !Number.isFinite(on) || off <= 0 || on < 0) return false
    return lower ? on / off >= limit : on / off <= limit
  }
  for (const arm of ['S1', 'L1']) {
    if (
      !ratio(arm, 'messagesPerSecond', 0.97, true) ||
      !ratio(arm, 'deliveryGapP99', 1.05) ||
      ![arms[arm]?.off, arms[arm]?.on].every((sample) => sample?.gapSamples >= 1000)
    )
      return 'false'
  }
  if (
    !ratio('S1', 'cpuMicrosPerMessage', 1.03) ||
    !ratio('S2', 'bytesPerSecond', 1) ||
    !ratio('L2', 'bytesPerMessage', 0.1) ||
    !ratio('L2', 'cpuMicrosPerMessage', 0.75) ||
    !ratio('L3', 'bytesPerSecond', 0.1)
  )
    return 'false'
  const offHeap = arms.L2?.off?.heapRetainedBytes
  const onHeap = arms.L2?.on?.heapRetainedBytes
  if (
    !Number.isFinite(offHeap) ||
    !Number.isFinite(onHeap) ||
    offHeap <= 0 ||
    onHeap <= 0 ||
    onHeap - offHeap > 3 * 1024 * 1024
  )
    return 'false'
  return 'true'
}

module.exports = {
  FetchObservation,
  verifyCycle,
  control,
  SessionMeasurement,
  verifyDelivery,
  windowTotals,
  retained,
  memoryControl,
  deliveryControl,
  runArm,
  FrameJournal,
  main,
  selectDefault,
}

if (require.main === module)
  main().catch((error) => {
    process.stderr.write(error.stack + '\n')
    process.exitCode = 1
  })
