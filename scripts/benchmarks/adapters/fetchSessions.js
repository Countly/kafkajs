/**
 * Report adapter of the fetch-session rows S1, S2, L1, L2, L3 and Memory: `fetchSessions.js`
 * with `fetchSessions: false` and `true` as arms of one interleaved session, each with its A/A
 * copy, on the racks cluster. The node inventory is `BENCH_NODES`, which the workflow reads from
 * the started containers; the row refuses to start without three nodes. Before the session the
 * controller (a process apart from every consumer) creates the row's topic with its leaders laid
 * out 334/333/333 (2/2/2 for six partitions), produces the active arms' corpus, and during each
 * L2 window writes 200 messages per second to 10 of the 1 000 partitions. Memory is the L2 run's
 * retained heap, read after two collections 100 ms apart.
 *
 * Controls, from the driver's untimed `--control` per tree: one full fetch cycle is three frames
 * of 334/333/333 partitions with at least 20 000 request bytes; an idle incremental request is at
 * most 128 bytes; for Memory, 2 097 152 retained objects add more than 3 MiB (else the memory
 * observation is blind). Each measured run's frames are audited against its transcript by the
 * driver, which fails the run otherwise. S1 and L1 need 1 000 batch gaps, else `inconclusive`.
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
 * Prepares a row: its topic, corpus, evidence directory and environment.
 * @param {{ row: object, name: string, dir: string, env: object }} context
 * @param {object} [deps]
 * @param {Function} [deps.Kafka] the client class, this checkout's by default
 * @returns {Promise<object>} `args`, `env`, `nodes`, `onMessage` (L2) and `cleanup`
 * @throws {Error} without a node inventory, or when the topic cannot be created or filled
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
  const timers = new Set()
  await admin.connect()
  await producer.connect()
  const cleanup = async () => {
    for (const timer of timers) clearInterval(timer)
    timers.clear()
    try {
      await admin.deleteTopics({ topics: [topic] })
    } finally {
      await producer.disconnect()
      await admin.disconnect()
    }
  }
  try {
    const replicaAssignment = layout(
      partitions,
      nodes.map((n) => n.nodeId)
    )
    await admin.createTopics({ waitForLeaders: true, topics: [{ topic, replicaAssignment }] })
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
    await cleanup()
    throw error
  }
  const onMessage =
    arm === 'L2'
      ? (message, child) => {
          if (message?.type !== 'measurement-ready') return
          const perTick = (L2_PER_SECOND * TICK_MS) / 1000
          let tick = 0
          const timer = setInterval(() => {
            if (++tick > L2_WINDOW_MS / TICK_MS) return stop()
            const messages = Array.from({ length: perTick }, (_, i) => ({
              partition: (tick * perTick + i) % L2_PARTITIONS,
              value: Buffer.alloc(RECORD_BYTES, 2),
            }))
            producer.send({ topic, messages }).catch(() => {})
          }, TICK_MS)
          const stop = () => {
            clearInterval(timer)
            timers.delete(timer)
          }
          timers.add(timer)
          child.once('close', stop)
        }
      : undefined
  return {
    args: ['--topic', topic],
    env: { BENCH_NODES: JSON.stringify(nodes), BENCH_EVIDENCE_DIR: evidence },
    nodes,
    onMessage,
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
    if (row.controls.includes('retained-memory positive control')) {
      list.push({
        arm: label,
        name: 'retained-memory positive control',
        passed: control.retainedControlBytes > 3 * MIB,
        detail: control.error ?? `${control.retainedControlBytes} bytes, more than 3 MiB required`,
      })
    }
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

module.exports = { ...rowAdapter({ controls, inconclusive, prepare, corpus }), parseNodes, layout }
