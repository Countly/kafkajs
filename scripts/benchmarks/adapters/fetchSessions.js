/**
 * Report adapter of the fetch-session rows S1, S2, L1, L2, L3 and Memory: `fetchSessions.js`
 * with `fetchSessions: false` and `true` as arms of one interleaved session, each with its A/A
 * copy, on the racks cluster. The node inventory is `BENCH_NODES`, which the workflow reads from
 * the started containers; the row refuses to start without three nodes. Before the session the
 * controller (a process apart from every consumer) creates the row's topic with its leaders laid
 * out 334/333/333 (2/2/2 for six partitions), produces the active arms' corpus, and during each
 * L2 window writes 200 messages per second to 10 of the 1 000 partitions, keeping a receipt of
 * every offered, acknowledged, failed and unsettled send; the window's sends settle before the next
 * run starts, and a window still sending after its settlement limit ends the session (the cleanup
 * then deletes the topic and disconnects the producer). Memory is the L2 run's retained heap, read after two collections 100 ms apart.
 *
 * Controls, from the driver's untimed `--control` per tree: one full fetch cycle is three frames
 * of 334/333/333 partitions with at least 20 000 request bytes; an idle incremental request is at
 * most 128 bytes; the remove-byte-counter mutant is rejected by the transcript comparison; for
 * Memory, 2 097 152 retained objects add more than 3 MiB (else the memory observation is blind).
 * Each measured run's frames are audited against its transcript by the driver, which fails the
 * run otherwise. Every L2 run's producer receipt must show the whole window: 12 000 messages
 * offered and acknowledged on 10 partitions, none failed or unsettled. S1 and L1 need 1 000 batch
 * gaps, else `inconclusive`.
 */
const path = require('node:path')
const fs = require('node:fs')
const { rowAdapter, armsWhere, MIB } = require('./consumerSession')

const CORPUS = { S1: [6, 200001], S2: [6, 0], L1: [1000, 200000], L2: [1000, 0], L3: [1000, 0] }
const RECORD_BYTES = 200
const L2_PARTITIONS = 10
const L2_PER_SECOND = 200
const L2_WINDOW_MS = 60000
const TICK_MS = 100
const L2_PER_TICK = (L2_PER_SECOND * TICK_MS) / 1000
const L2_TOTAL = (L2_PER_SECOND * L2_WINDOW_MS) / 1000
const SETTLE_MS = 30000

/**
 * @param {string|undefined} text `BENCH_NODES`
 * @returns {{ nodeId: number, host: string, port: number }[]} the three brokers
 * @throws {Error} unless it is a JSON array of three nodes with distinct ids
 */
const parseNodes = (text) => {
  if (!text)
    throw new Error('BENCH_NODES is not set: the workflow reads it from the started containers')
  const nodes = JSON.parse(text)
  const valid =
    Array.isArray(nodes) &&
    nodes.length === 3 &&
    new Set(nodes.map((n) => n?.nodeId)).size === 3 &&
    nodes.every(
      (n) =>
        Number.isInteger(n.nodeId) && typeof n.host === 'string' && Number.isInteger(Number(n.port))
    )
  if (!valid) throw new Error('BENCH_NODES is not three nodes of { nodeId, host, port }')
  return nodes
}

/**
 * @param {number} partitions
 * @param {number[]} nodeIds
 * @returns {{ partition: number, replicas: number[] }[]} partition p led by node p mod 3
 */
const layout = (partitions, nodeIds) =>
  Array.from({ length: partitions }, (_, partition) => ({
    partition,
    replicas: [nodeIds[partition % 3]],
  }))

/**
 * The L2 producer of one measurement window: 20 messages every 100 ms to 10 partitions, 600
 * ticks, every send counted when it settles. `settle` stops the window and waits a bounded time
 * for the sends still in flight, so none spills into the next run.
 */
class WindowProducer {
  /**
   * @param {{ producer: { send: Function }, topic: string, settleMs?: number,
   *   now?: () => number }} options
   */
  constructor({ producer, topic, settleMs = SETTLE_MS, now = Date.now }) {
    this.producer = producer
    this.topic = topic
    this.settleMs = settleMs
    this.now = now
    this.value = Buffer.alloc(RECORD_BYTES, 2)
    this.offered = 0
    this.acknowledged = 0
    this.failed = 0
    this.ticks = 0
    this.partitions = new Set()
    this.errors = []
    this.inFlight = new Map()
  }

  /** @returns {void} starts the window's ticks */
  start() {
    this.startedAt = this.now()
    this.timer = setInterval(() => this.tick(), TICK_MS)
  }

  /**
   * Offers one tick's messages and stops the window after its last tick.
   * @returns {void}
   */
  tick() {
    const messages = Array.from({ length: L2_PER_TICK }, (_, i) => ({
      partition: (this.offered + i) % L2_PARTITIONS,
      value: this.value,
    }))
    for (const { partition } of messages) this.partitions.add(partition)
    this.offered += messages.length
    const send = Promise.resolve()
      .then(() => this.producer.send({ topic: this.topic, messages }))
      .then(
        () => {
          this.acknowledged += messages.length
        },
        (error) => {
          this.failed += messages.length
          if (this.errors.length < 5) this.errors.push(error.message)
        }
      )
      .finally(() => this.inFlight.delete(send))
    this.inFlight.set(send, messages.length)
    if (++this.ticks >= L2_WINDOW_MS / TICK_MS) this.stop('window')
  }

  /**
   * @param {string} reason why the window ended: `window`, `child-close` or `settled`
   * @returns {void} no more ticks
   */
  stop(reason) {
    clearInterval(this.timer)
    this.timer = undefined
    this.endedBy ??= reason
    this.stoppedAt ??= this.now()
  }

  /**
   * Stops the window and waits up to `settleMs` for its sends.
   * @returns {Promise<object>} the receipt: offered, acknowledged, failed and unsettled message
   *   counts, the partitions written, ticks, why the window ended, its times and the first errors
   */
  async settle() {
    this.stop('settled')
    let timer
    const limit = new Promise((resolve) => {
      timer = setTimeout(resolve, this.settleMs)
    })
    await Promise.race([Promise.all(this.inFlight.keys()), limit])
    clearTimeout(timer)
    return {
      offered: this.offered,
      acknowledged: this.acknowledged,
      failed: this.failed,
      unsettled: [...this.inFlight.values()].reduce((sum, n) => sum + n, 0),
      partitions: [...this.partitions].sort((a, b) => a - b),
      ticks: this.ticks,
      endedBy: this.endedBy,
      startedAt: this.startedAt,
      stoppedAt: this.stoppedAt,
      settledAt: this.now(),
      errors: this.errors,
    }
  }
}

/**
 * Prepares a row: its topic, corpus, evidence directory and environment. The clients are owned
 * before the first connection: any failure, or the returned `cleanup`, settles an open L2 window,
 * deletes the topic once created and disconnects both clients, trying every step and keeping the
 * first error.
 * @param {{ row: object, name: string, dir: string, env: object }} context
 * @param {object} [deps]
 * @param {Function} [deps.Kafka] the client class, this checkout's by default
 * @returns {Promise<object>} `args`, `env`, `nodes`, `onMessage` and `afterRun` (L2: the window's
 *   receipt, with a `halt` reason when sends are still unsettled), `cleanup`
 * @throws {Error} without a node inventory, or when a client cannot connect or the topic cannot
 *   be created or filled; after the cleanup
 */
const prepare = async ({ row, name, dir, env }, { Kafka } = {}) => {
  const nodes = parseNodes(env.BENCH_NODES)
  const arm = row.args[1]
  const [partitions, records] = CORPUS[arm]
  const evidence = path.join(dir, 'evidence')
  fs.mkdirSync(evidence, { recursive: true })
  const { Kafka: Client, logLevel } = Kafka ? { Kafka, logLevel: {} } : require('../../../index')
  const kafka = new Client({
    clientId: 'fs-gate-producer',
    brokers: nodes.map((n) => `${n.host}:${n.port}`),
    logLevel: logLevel.NOTHING,
  })
  const topic = `bench-${name}-${Date.now()}`
  const admin = kafka.admin()
  const producer = kafka.producer()
  let created = false
  let window
  const cleanup = async () => {
    const failures = []
    const attempt = async (step) => {
      try {
        await step()
      } catch (error) {
        failures.push(error)
      }
    }
    if (window) await attempt(() => window.settle())
    window = undefined
    if (created) await attempt(() => admin.deleteTopics({ topics: [topic] }))
    await attempt(() => producer.disconnect())
    await attempt(() => admin.disconnect())
    if (failures.length) throw failures[0]
  }
  try {
    await admin.connect()
    await producer.connect()
    const replicaAssignment = layout(
      partitions,
      nodes.map((n) => n.nodeId)
    )
    await admin.createTopics({ waitForLeaders: true, topics: [{ topic, replicaAssignment }] })
    created = true
    const value = Buffer.alloc(RECORD_BYTES, 1)
    for (let sent = 0; sent < records;) {
      const count = Math.min(1000, records - sent)
      const messages = Array.from({ length: count }, (_, i) => ({
        partition: (sent + i) % partitions,
        value,
      }))
      await producer.send({ topic, messages })
      sent += count
    }
  } catch (error) {
    await cleanup().catch(() => {})
    throw error
  }
  const l2 = arm === 'L2' && {
    onMessage: (message, child) => {
      if (message?.type !== 'measurement-ready' || window) return
      const current = new WindowProducer({ producer, topic })
      window = current
      current.start()
      child.once('close', () => current.stop('child-close'))
    },
    afterRun: async () => {
      if (!window) return { offered: 0, endedBy: 'not started' }
      const receipt = await window.settle()
      window = undefined
      if (!receipt.unsettled) return receipt
      const halt = `${receipt.unsettled} messages unsettled after ${SETTLE_MS} ms would write into the next run's topic`
      return { ...receipt, halt }
    },
  }
  return {
    args: ['--topic', topic],
    env: { BENCH_NODES: JSON.stringify(nodes), BENCH_EVIDENCE_DIR: evidence },
    nodes,
    ...l2,
    cleanup,
  }
}

/**
 * @param {object} session
 * @param {object} row
 * @returns {import('./consumerSession').Control[]}
 */
const controls = (session, row) => {
  const list = [
    {
      arm: '*',
      name: 'BENCH_NODES from the started containers',
      passed: Array.isArray(session.nodes) && session.nodes.length === 3,
      detail: JSON.stringify(session.nodes ?? null),
    },
  ]
  for (const [label, tree] of Object.entries(session.trees)) {
    if (tree.error) continue
    const control = session.controls[label] ?? { error: 'no control run' }
    const frames = control.frames
    list.push(
      {
        arm: label,
        name: 'full-cycle byte lower bound',
        passed:
          frames?.requests === 3 &&
          frames.requestBytes >= 20000 &&
          JSON.stringify(frames.shape) === '[334,333,333]',
        detail:
          control.error ?? `${frames?.requestBytes} request bytes over ${frames?.requests} frames`,
      },
      {
        arm: label,
        name: 'idle incremental bound',
        passed: frames?.requests > 0 && frames.incrementalRequestBytes / frames.requests <= 128,
        detail:
          control.error ??
          `${frames?.incrementalRequestBytes} bytes over ${frames?.requests} requests`,
      }
    )
    list.push({
      arm: label,
      name: 'remove-byte-counter mutant rejected',
      passed: control.mutant?.rejected === true,
      detail: control.error ?? control.mutant?.reason ?? 'no mutant control',
    })
    if (row.controls.includes('retained-memory positive control')) {
      list.push({
        arm: label,
        name: 'retained-memory positive control',
        passed: control.retainedControlBytes > 3 * MIB,
        detail: control.error ?? `${control.retainedControlBytes} bytes, more than 3 MiB required`,
      })
    }
  }
  if (row.controls.includes('L2 producer window complete')) {
    const complete = (p) =>
      p?.offered === L2_TOTAL &&
      p.acknowledged === L2_TOTAL &&
      p.failed === 0 &&
      p.unsettled === 0 &&
      p.partitions?.length === L2_PARTITIONS
    const short = Object.entries(session.arms).flatMap(([label, { results }]) =>
      results
        .filter((r) => !complete(r.producer))
        .map((r) => `${label}: ${JSON.stringify(r.producer ?? null)}`)
    )
    list.push({
      arm: '*',
      name: 'L2 producer window complete',
      passed: short.length === 0,
      detail: short.length
        ? short.slice(0, 3).join('; ')
        : `${L2_TOTAL} messages offered and acknowledged on ${L2_PARTITIONS} partitions`,
    })
  }
  return list
}

/**
 * @param {object} session
 * @param {object} row
 * @returns {string|undefined} why an active arm is inconclusive
 */
const inconclusive = (session, row) => {
  if (!['S1', 'L1'].includes(row.args[1])) return undefined
  const short = armsWhere(session, (r) => r.sample?.gapSamples >= 1000)
  return short.length ? `fewer than 1000 batch gaps on ${short.join(', ')}` : undefined
}

/** @param {object} r one run @returns {string} its corpus */
const corpus = (r) => `${r.arm} on ${r.topic}`

module.exports = {
  ...rowAdapter({ controls, inconclusive, prepare, corpus }),
  parseNodes,
  layout,
  WindowProducer,
}
