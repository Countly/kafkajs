const crypto = require('node:crypto')
const { CORPUS, ShareExchanges, indexOf, recordId, valueOf, wait } = require('../../telemetryChurn')
const {
  ShareTrace,
  confirmedDecisions,
  heldAcquisitions,
} = require('../../../e2e/telemetry/shareEvidence')

const sha = (value) => crypto.createHash('sha256').update(value).digest('hex')

/** Array journals frozen for lossless replay against the fixed-storage implementation. */
class ShareLedger {
  /**
   * @param {object} options
   * @param {string} options.group the share group @param {{now:function():number}} options.clock
   */
  constructor({ group, clock }) {
    this.group = group
    this.clock = clock
    this.traces = []
    this.exchanges = []
    this.settled = []
  }

  /**
   * @param {object} socket one socket of the share consumer
   * @param {{segment?:string, ordinal?:number}} [owner] the segment and socket it belongs to
   * @returns {object} the same socket
   */
  wrap(socket, owner = {}) {
    const entry = {
      segment: owner.segment ?? null,
      socket: owner.ordinal ?? null,
      trace: new ShareTrace(),
      closed: false,
    }
    entry.trace.wrap(socket)
    socket.once('close', () => {
      entry.closed = true
    })
    this.traces.push(entry)
    return socket
  }

  /**
   * Reads every completed exchange into its facts and forgets its frames; a closed socket's
   * unanswered exchanges are kept without a response and its trace is released.
   * @returns {void}
   * @throws {Error} for a capture failure, an unreadable frame, or facts that differ from what the
   *   existing readers derive from the same frame
   */
  drain() {
    for (const entry of this.traces) {
      if (entry.trace.failures.length) throw new Error(entry.trace.failures[0])
      const done = entry.trace.frames.filter((frame) => frame.responseHex || entry.closed)
      entry.trace.frames = entry.trace.frames.filter((frame) => !done.includes(frame))
      for (const frame of done) {
        const fact = ShareExchanges.fact(frame, entry, this.exchanges.length + 1)
        if (frame.responseHex) this.agree(frame, fact)
        this.exchanges.push(fact)
      }
    }
    this.traces = this.traces.filter((entry) => !entry.closed)
  }

  /**
   * @param {object} frame a completed frame pair @param {object} fact its exchange facts
   * @returns {void} @throws {Error} unless the facts re-derive exactly what the independent readers read
   */
  agree(frame, fact) {
    const strip = (rows) =>
      rows.map(({ topicId, partition, firstOffset, lastOffset, deliveryCount, offset, type }) => ({
        topicId,
        partition,
        firstOffset,
        lastOffset,
        deliveryCount,
        offset,
        type,
      }))
    const held = strip(heldAcquisitions([frame], this.group))
    const derived = strip(ShareExchanges.acquisitions([fact], this.group))
    const confirmed = strip(confirmedDecisions([frame], this.group))
    const settled = strip(
      ShareExchanges.decisions([fact], this.group).settled.flatMap((range) => {
        const out = []
        for (let o = BigInt(range.firstOffset); o <= BigInt(range.lastOffset); o++)
          out.push({ ...range, offset: String(o), firstOffset: undefined, lastOffset: undefined })
        return out
      })
    )
    if (JSON.stringify(held) !== JSON.stringify(derived))
      throw new Error('Share acquisition facts differ from the raw reader')
    if (JSON.stringify(confirmed) !== JSON.stringify(settled))
      throw new Error('Share settlement facts differ from the raw reader')
  }

  /**
   * @param {object} event a public acknowledge event @returns {void} retained with its time
   */
  record(event) {
    this.settled.push({
      topic: event.payload.topic,
      partition: event.payload.partition,
      firstOffset: event.payload.firstOffset,
      lastOffset: event.payload.lastOffset,
      type: event.payload.type,
      error: event.payload.error
        ? { name: event.payload.error.name, reason: event.payload.error.reason }
        : null,
      at: this.clock.now(),
    })
  }

  /** @returns {object} the archived facts, after a final drain, with the hash of the exchanges */
  facts() {
    this.drain()
    return {
      group: this.group,
      exchanges: this.exchanges,
      exchangesSha256: sha(JSON.stringify(this.exchanges)),
      settled: this.settled,
    }
  }
}

class OrdinaryWork {
  /**
   * @param {object} options
   * @param {string} options.kind the client kind @param {object} options.client the target client
   * @param {string} options.stem run prefix @param {string} options.topic the corpus topic
   * @param {string} options.ctl the control topic for work that is not corpus
   * @param {{now:()=>number}} options.clock monotonic time
   * @param {(ms:number)=>Promise<void>} [options.sleep] real delay
   * @param {ShareLedger} [options.ledger] the share ledger of a share consumer
   * @param {number[]} [options.slots] producer batch slots, in ms after the corpus start
   */
  constructor({ kind, client, stem, topic, ctl, clock, sleep = wait, ledger = null, slots = [] }) {
    Object.assign(this, { kind, client, stem, topic, ctl, clock, sleep, ledger, slots })
    this.running = false
    this.paused = false
    this.loop = null
    this.inflight = null
    this.active = 0
    this.lastExit = -Infinity
    this.offered = []
    this.resolved = []
    this.handled = []
    this.processed = new Map()
    this.committed = new Map()
    this.commits = []
    this.pauses = []
    this.late = []
    this.failures = []
    this.listening = false
  }

  /**
   * @returns {Promise<void>} subscribes and runs the consuming kinds, again after each reopen
   * @throws {Error} a refusal of subscribe or run
   */
  async attach() {
    const handle = async ({ partition, message }) => {
      this.active += 1
      const entry = [
        partition,
        Number(message.offset),
        indexOf(String(message.key)),
        this.clock.now(),
        null,
      ]
      this.handled.push(entry)
      if (this.kind === 'consumer')
        this.processed.set(String(partition), String(BigInt(message.offset) + 1n))
      entry[4] = this.clock.now()
      this.lastExit = entry[4]
      this.active -= 1
    }
    if (this.kind === 'shareConsumer') {
      if (!this.listening) {
        this.client.on(this.client.events.ACKNOWLEDGE, (event) => {
          this.ledger.record(event)
          this.ledger.drain()
        })
        this.client.on(this.client.events.FETCH, () => this.ledger.drain())
        this.listening = true
      }
      await this.client.subscribe({ topics: [this.topic] })
      await this.client.run({ eachMessage: handle })
    } else {
      await this.client.subscribe({ topic: this.topic, fromBeginning: true })
      await this.client.run({ autoCommit: false, eachMessage: handle })
    }
  }

  /**
   * Begins the kind's idle work: the consumers attach, the admin starts its paced describes.
   * @returns {Promise<void>} after the first attach
   */
  async start() {
    this.running = true
    if (this.kind === 'consumer' || this.kind === 'shareConsumer') await this.attach()
    if (this.kind === 'admin') {
      this.loop = this.poll()
      this.loop.catch(() => {})
    }
  }

  /**
   * Starts the measured corpus: only the producer offers records here.
   * @returns {void}
   */
  corpus() {
    if (this.kind !== 'producer' || this.loop) return
    this.loop = this.pace()
    this.loop.catch(() => {})
  }

  /** @returns {Promise<void>} the admin's paced metadata requests until `stop` */
  async poll() {
    while (this.running) {
      if (!this.paused) {
        this.inflight = this.client.describeCluster({ timeout: 3000 })
        try {
          await this.inflight
        } catch (error) {
          this.failures.push(error)
          this.running = false
          return
        }
      }
      await this.sleep(CORPUS.batchMs)
    }
  }

  /**
   * Offers the corpus batch by batch, each no earlier than its fixed slot and none while paused.
   * A batch the pause delayed is sent as soon as admission resumes, one batch at a time.
   * @returns {Promise<void>} after the last batch or at `stop`
   */
  async pace() {
    const start = this.clock.now()
    for (let batch = 0; batch < this.slots.length && this.running; batch++) {
      const due = start + (this.slots[batch] - this.slots[0])
      while (this.running && (this.clock.now() < due || this.paused))
        await this.sleep(Math.min(100, Math.max(1, due - this.clock.now())))
      if (!this.running) return
      const ids = Array.from({ length: CORPUS.batch }, (_, i) => batch * CORPUS.batch + i)
      this.late.push(Math.max(0, this.clock.now() - due))
      this.offered.push(...ids)
      this.inflight = this.client.send({
        topic: this.topic,
        acks: -1,
        messages: ids.map((i) => {
          const id = recordId(this.stem, 'producer', i)
          return { key: id, value: valueOf(id) }
        }),
      })
      try {
        await this.inflight
      } catch (error) {
        this.failures.push(error)
        this.running = false
        return
      }
      this.resolved.push(...ids)
    }
  }

  /**
   * Work that is not corpus, used while a telemetry socket is faulted: it must still succeed.
   * @param {number} n a unique number for this call
   * @returns {Promise<void>} after the kind's own ordinary operation, if it has one
   */
  async core(n) {
    if (this.kind === 'admin') await this.client.describeCluster({ timeout: 3000 })
    else if (this.kind === 'producer')
      await this.client.send({
        topic: this.ctl,
        acks: -1,
        messages: [{ key: `${this.stem}:core:${n}`, value: Buffer.from('core') }],
      })
  }

  /** @returns {Promise<void>} commits the processed next offsets of the classic consumer */
  async commit() {
    const offsets = [...this.processed]
      .filter(([partition, offset]) => this.committed.get(partition) !== offset)
      .map(([partition, offset]) => ({
        topic: this.topic,
        partition: Number(partition),
        offset,
      }))
    if (!offsets.length) return
    await this.client.commitOffsets(offsets)
    for (const { partition, offset } of offsets) this.committed.set(String(partition), offset)
    this.commits.push({ at: this.clock.now(), count: offsets.length })
  }

  /**
   * Pauses admission, finishes in-flight work and confirms every commit, ahead of a public close.
   * @returns {Promise<{pausedAt:number, idleAt:number, confirmedAt:number}>} the barrier's receipt
   * @throws {Error} a failure of in-flight work or of the commit, or work that does not go idle
   */
  async pause() {
    const receipt = { pausedAt: this.clock.now(), idleAt: null, confirmedAt: null }
    this.paused = true
    if (this.inflight) await this.inflight
    if (this.kind === 'consumer') {
      await this.client.pause([{ topic: this.topic }])
      const deadline = this.clock.now() + 8000
      while (this.active > 0 || this.clock.now() - this.lastExit < 200) {
        if (this.clock.now() > deadline) throw new Error('The consumer handler did not go idle')
        await this.sleep(50)
      }
      receipt.idleAt = this.clock.now()
      await this.commit()
      receipt.confirmedAt = this.clock.now()
    } else {
      receipt.idleAt = this.clock.now()
      receipt.confirmedAt = receipt.idleAt
    }
    this.pauses.push(receipt)
    return receipt
  }

  /**
   * Resumes admission after the reconnect; a consuming kind runs again on the reconnected object.
   * @returns {Promise<void>} when admission is open again
   */
  async resume() {
    if (this.running && (this.kind === 'consumer' || this.kind === 'shareConsumer'))
      await this.attach()
    if (this.kind === 'consumer') await this.client.resume([{ topic: this.topic }])
    this.paused = false
  }

  /**
   * Stops admission, joins the loop and, for a consuming kind, waits until the whole corpus was
   * handled before confirming the last commit.
   * @param {number} ms how long the consumer may take to finish the corpus
   * @returns {Promise<void>} @throws {Error} the first ordinary failure, or an incomplete corpus
   */
  async stop(ms = 40000) {
    if (this.loop && this.kind === 'producer') await this.loop
    this.running = false
    if (this.loop) await this.loop
    if (this.failures.length) throw this.failures[0]
    if (this.kind === 'consumer' || this.kind === 'shareConsumer') {
      const deadline = this.clock.now() + ms
      while (
        new Set(this.handled.map((e) => e[2])).size < CORPUS.records &&
        this.clock.now() < deadline
      )
        await this.sleep(200)
      if (new Set(this.handled.map((e) => e[2])).size < CORPUS.records)
        throw new Error('The corpus was not fully handled before the close')
      if (this.kind === 'consumer') await this.commit()
    }
  }

  /**
   * @returns {object} the archived identity ledgers of this worker: offered and resolved positions,
   *   every handler entry, processed and committed ends, commit and pause receipts
   */
  ids() {
    return {
      kind: this.kind,
      offered: this.offered,
      resolved: this.resolved,
      handled: this.handled,
      processed: Object.fromEntries(this.processed),
      committed: Object.fromEntries(this.committed),
      commits: this.commits,
      pauses: this.pauses,
      late: this.late,
    }
  }
}

module.exports = { ReferenceWork: OrdinaryWork, ReferenceShareLedger: ShareLedger }
