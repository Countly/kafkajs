'use strict'

/**
 * One producer process of a load run with `--producer-processes` above 1: it hosts the load
 * producers the parent hands it, offers their share of the rate on its own deadline schedule from
 * the window's start, joins them, and sends back what the oracle and the report need (each
 * incarnation's send ledger, the stats, the latency histogram, the throughput series and its own
 * CPU). A producer lives in exactly one process, so every record key stays single-writer per
 * producer, incarnation and partition, as in one process.
 *
 * Messages from the parent: `setup` `{ profile, runId, topics, producers: [{ id, kind }] }`,
 * `begin` `{ startEpochMs }`, `close`. To the parent: `ready`, `progress` `{ sent }` per
 * producer, `done` `{ producers, latency, series, cpu }`, `closed`, or `failed` `{ error }`.
 */

const { LatencyHistogram } = require('./loadProfile')
const { HostMeter, ThroughputSeries } = require('./loadMeters')

/** How often the window's progress (records sent) goes to the parent. */
const PROGRESS_MS = 2000

/**
 * The child side of a producer process.
 */
class ProducerHost {
  /**
   * @param {object} options
   * @param {object} options.kafka a `Kafka`
   * @param {{ now: () => number, timeOrigin?: number }} options.clock `timeOrigin` turns the
   *   parent's epoch start into this clock's time
   * @param {(ms: number, signal?: AbortSignal) => Promise<void>} options.sleep
   * @param {(message: object) => void} options.send to the parent
   * @param {Function} options.LoadProducer the load producer class
   * @param {HostMeter} [options.host]
   */
  constructor({ kafka, clock, sleep, send, LoadProducer, host = new HostMeter() }) {
    Object.assign(this, { kafka, clock, sleep, send, LoadProducer, host })
    this.producers = []
    this.latency = new LatencyHistogram()
  }

  /**
   * Handles one message from the parent; a failure is sent back as `failed`.
   * @param {{ type: string }} message
   * @returns {Promise<void>}
   */
  async handle(message) {
    try {
      if (message.type === 'setup') await this.#setup(message)
      else if (message.type === 'begin') await this.#window(message.startEpochMs)
      else if (message.type === 'close') await this.#close()
    } catch (error) {
      this.send({ type: 'failed', error: `${error?.name}: ${error?.message}` })
    }
  }

  async #setup({ profile, runId, topics, producers }) {
    this.profile = profile
    this.series = ThroughputSeries.forProfile(profile)
    this.producers = producers.map(
      ({ id, kind }) =>
        new this.LoadProducer({
          kafka: this.kafka,
          id,
          kind,
          profile,
          topics,
          runId,
          latency: this.latency,
          clock: this.clock,
          sleep: this.sleep,
          series: this.series,
        })
    )
    for (const producer of this.producers) await producer.connect()
    this.send({ type: 'ready' })
  }

  /** Offers until the window's end on this clock, then joins and reports. */
  async #window(startEpochMs) {
    const { profile, producers } = this
    const start = startEpochMs - (this.clock.timeOrigin ?? 0)
    const end = start + profile.durationSeconds * 1000
    const tickMs = 1000 / profile.ticksPerSecond
    const read = () => ({ at: this.clock.now(), ...this.host.read() })
    const marks = [read()]
    for (const producer of producers) producer.begin(start)
    this.series.begin(start)
    let nextProgress = start + PROGRESS_MS
    for (let n = 1; ; n++) {
      const now = Math.min(this.clock.now(), end)
      for (const producer of producers) producer.tick(now)
      if (now >= nextProgress) {
        nextProgress += PROGRESS_MS
        this.send({ type: 'progress', sent: producers.map((p) => p.stats.sent) })
      }
      if (now >= end) break
      await this.sleep(Math.max(0, Math.min(end, start + n * tickMs) - this.clock.now()))
    }
    marks.push(read())
    const settled = []
    for (const producer of producers) settled.push(await producer.join(profile.joinSeconds * 1000))
    marks.push(read())
    const span = (a, b) => HostMeter.between(a, b, b.at - a.at)
    this.send({
      type: 'done',
      producers: producers.map((p, i) => ({
        id: p.id,
        stats: p.stats,
        errors: p.errors,
        ledgers: p.ledgerReports(),
        settled: settled[i],
      })),
      latency: this.latency.snapshot(),
      series: this.series.snapshot(),
      cpu: { offer: span(marks[0], marks[1]), join: span(marks[1], marks[2]) },
    })
  }

  async #close() {
    for (const producer of this.producers) await producer.close()
    this.send({ type: 'closed' })
  }
}

if (require.main === module) {
  const { LoadProducer, loadKafka, realSleep } = require('./loadRun')
  const host = new ProducerHost({
    kafka: loadKafka(),
    clock: performance,
    sleep: realSleep,
    send: (message) => process.send(message),
    LoadProducer,
  })
  process.on('message', (message) => {
    host.handle(message).then(() => {
      if (message.type === 'close') process.disconnect()
    })
  })
}

module.exports = { ProducerHost }
