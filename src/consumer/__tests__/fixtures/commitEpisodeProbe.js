const { delay, pollUntil } = require('../../../producer/__tests__/fixtures/bounded')

const HELD_RESPONSE_MS = 20000
const REPORTED_MS = 15000
const OFFSET_COMMIT = 8

/**
 * Splits an inbound byte stream into whole Kafka frames across any chunk boundary.
 */
class FrameReader {
  /** @param {(frame: Buffer) => void} onFrame called once per complete frame, size prefix included */
  constructor(onFrame) {
    this.onFrame = onFrame
    this.pending = Buffer.alloc(0)
  }

  /** @param {Buffer} chunk bytes as the socket delivered them @returns {void} */
  push(chunk) {
    this.pending = this.pending.length ? Buffer.concat([this.pending, chunk]) : chunk
    while (this.pending.length >= 4) {
      const size = this.pending.readInt32BE(0)
      if (size < 0 || this.pending.length < 4 + size) return
      const frame = this.pending.subarray(0, 4 + size)
      this.pending = this.pending.subarray(4 + size)
      this.onFrame(frame)
    }
  }
}

/**
 * Correlates the OffsetCommit requests and responses of one member and, on demand, holds one
 * response after the broker has sent it. Correlation is by API key and correlation ID from the
 * frames, joined to the member's own `commit_offsets` event for group, member, topic, partition and
 * offset; a join that finds none or more than one candidate fails instead of guessing. The hold
 * releases only through `release`, so a response the broker has acknowledged stays unseen by the
 * member while an independent admin reads the broker.
 */
class CommitEpisodeProbe {
  /**
   * @param {(options: object) => object} inner the socket factory of the member's client
   * @param {() => number} tick the scenario's ordinal clock
   */
  constructor(inner, tick) {
    this.inner = inner
    this.tick = tick
    this.requests = []
    this.commits = []
    this.armed = false
    this.gate = null
    this.socketFactory = (options) => this.wrap(this.inner(options))
  }

  /**
   * @param {object} socket the real socket
   * @returns {object} a socket that reports each request frame and filters each response frame
   */
  wrap(socket) {
    const probe = this
    const proxy = new Proxy(socket, {
      get(target, property) {
        if (property === 'write') {
          return (data, ...rest) => {
            probe.observeRequest(Buffer.isBuffer(data) ? data : Buffer.from(data))
            return target.write(data, ...rest)
          }
        }
        if (property === 'on') {
          return (event, listener) => {
            if (event !== 'data') {
              target.on(event, listener)
              return proxy
            }
            const reader = new FrameReader((frame) => probe.receive(frame, listener))
            target.on('data', (chunk) => reader.push(chunk))
            return proxy
          }
        }
        const value = target[property]
        return typeof value === 'function' ? value.bind(target) : value
      },
    })
    return proxy
  }

  /** @param {Buffer} frame one request frame @returns {void} */
  observeRequest(frame) {
    if (frame.length < 12) return
    const apiKey = frame.readInt16BE(4)
    const correlationId = frame.readInt32BE(8)
    this.requests.push({ apiKey, correlationId, ordinal: this.tick() })
    if (apiKey === OFFSET_COMMIT && this.armed) {
      this.armed = false
      this.gate = { correlationId, held: null }
    }
  }

  /**
   * @param {Buffer} frame one response frame
   * @param {(frame: Buffer) => void} deliver the member's own reader
   * @returns {void}
   */
  receive(frame, deliver) {
    const correlationId = frame.readInt32BE(4)
    if (this.gate && this.gate.correlationId === correlationId && !this.gate.held) {
      this.gate.held = { frame, deliver }
      return
    }
    deliver(frame)
  }

  /** @returns {void} holds the response of the next OffsetCommit this member sends */
  arm() {
    this.armed = true
    this.gate = null
  }

  /**
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<boolean>} whether a response was held within the bound; false when no commit
   *   followed, which is a close that sends none
   */
  async heldResponse(scope) {
    const deadline = performance.now() + HELD_RESPONSE_MS
    while (performance.now() < deadline) {
      if (this.gate?.held) return true
      await delay(scope, 100)
    }
    return false
  }

  /** @returns {void} lets the held response reach the member, once; later calls do nothing */
  release() {
    this.armed = false
    const { held } = this.gate ?? {}
    if (held?.deliver) {
      this.gate.held = { frame: null, deliver: null }
      held.deliver(held.frame)
    }
  }

  /**
   * Registers the release on a cleanup stack. Cleanups run last-in first-out, so call this after
   * the member's own shutdown is registered: a held response is then released before the shutdown
   * is awaited, and a failed episode cannot leave the shutdown waiting on it.
   * @param {{ add: (name: string, close: () => void) => void }} cleanup the case's cleanup stack
   * @param {string} name the member label
   * @returns {void}
   */
  releaseOnCleanup(cleanup, name) {
    cleanup.add(`held response ${name}`, () => this.release())
  }

  /**
   * Records the member's own commit event, which names the partitions a commit carried. The event
   * gives partitions as the string keys of the offset table, so they are stored as numbers.
   * @param {object} payload a `commit_offsets` payload
   * @returns {void}
   */
  recordCommit({ groupId, memberId, topics }) {
    const ordinal = this.tick()
    for (const { topic, partitions } of topics) {
      for (const { partition, offset } of partitions) {
        this.commits.push({
          groupId,
          memberId,
          topic,
          partition: Number(partition),
          offset: Number(offset),
          ordinal,
        })
      }
    }
  }

  /**
   * @param {{ partition: number, after: number, before: number }} query as for `correlate`
   * @returns {object[]} the recorded commit events inside the ordinal window for the partition
   */
  matching({ partition, after, before }) {
    return this.commits.filter(
      (c) => c.partition === partition && c.ordinal > after && c.ordinal < before
    )
  }

  /**
   * Correlates once the member has reported the commit. The member emits `commit_offsets` only
   * after the broker's response reaches it, so a held response must be released first.
   * @param {AbortSignal} scope the case scope
   * @param {object} query as for `correlate`
   * @returns {Promise<object>} the correlated commit
   * @throws {Error} when no event arrives in time, or as `correlate` does
   */
  async correlateReported(scope, query) {
    await pollUntil(scope, REPORTED_MS, () => this.matching(query).length > 0, {
      delayMs: 50,
      label: `The commit event of partition ${query.partition}`,
    })
    return this.correlate(query)
  }

  /**
   * Joins a commit event to the request frame that carried it.
   * @param {object} query
   * @param {number} query.partition the partition of the episode
   * @param {number} query.after only commits recorded after this ordinal count
   * @param {number} query.before only commits recorded before this ordinal count
   * @param {boolean} query.single whether exactly one commit may match
   * @returns {object} the commit with the API key and correlation ID of its request
   * @throws {Error} when no commit, more than one where one is required, or no request, matches
   */
  correlate({ partition, after, before, single }) {
    const matches = this.matching({ partition, after, before })
    if (matches.length === 0 || (single && matches.length > 1)) {
      throw new Error(
        `The commit of partition ${partition} is ${matches.length === 0 ? 'missing' : 'ambiguous'} (${matches.length} events)`
      )
    }
    const commit = matches[matches.length - 1]
    const request = this.requests
      .filter((r) => r.apiKey === OFFSET_COMMIT && r.ordinal < commit.ordinal)
      .pop()
    if (!request) throw new Error(`The commit of partition ${partition} has no request frame`)
    return { ...commit, apiKey: request.apiKey, correlationId: request.correlationId }
  }
}

module.exports = { CommitEpisodeProbe, FrameReader }
