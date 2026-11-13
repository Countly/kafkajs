'use strict'

const crypto = require('node:crypto')
const churn = require('../../telemetryChurn')
const { ReferenceWork, ReferenceShareLedger } = require('./journalReference')
const { journalState } = require('../../../../testHelpers/fixedJournal')
const { ProcessMemory } = require('../../../../testHelpers/processMemory')
const { WeakOwners } = require('../../../e2e/telemetry/retained/weakOwners')
const TelemetryMetrics = require('../../../../src/telemetry/metrics')
const { TELEMETRY } = require('../../soak')
const { RetainedCalibration } = require('./journalCalibration')

/** @param {*} value scalar archive @returns {string} order-sensitive comparison hash */
const sha = (value) => crypto.createHash('sha256').update(JSON.stringify(value)).digest('hex')

/**
 * CI-only paired ownership arm. The public ordinary workload uses the real corpus/slot catalogue,
 * with virtual time and broker stand-ins; actual product MetricSnapshots are weakly observed.
 * Its growing weak inventory is disclosed, and none of its readings calibrate retained memory.
 * No live-worker attribution or acceptance is claimed. This process is never an acceptance worker.
 * @param {string} mode legacy or fixed @param {string} kind public workload kind
 * @param {boolean} seeded retain one actual product snapshot plus 4096 bytes per record when true
 * @returns {Promise<object>} lossless replay hashes, real readings and ownership at four boundaries
 * @throws {Error} on missing GC, unknown mode, overflow or workload failure
 */
const run = async (mode, kind, seeded) => {
  if (
    !['legacy', 'fixed'].includes(mode) ||
    !['producer', 'consumer', 'shareConsumer'].includes(kind)
  )
    throw new Error('Unknown journal diagnostic arm')
  const Work = mode === 'legacy' ? ReferenceWork : churn.OrdinaryWork
  const Ledger = mode === 'legacy' ? ReferenceShareLedger : churn.ShareLedger
  const schedule = new churn.TelemetrySchedule(TELEMETRY)
  const slots = schedule.corpusSlots()
  let now = 0.125
  const clock = { now: () => now }
  const memory = new ProcessMemory()
  const owners = new WeakOwners(churn.CORPUS.records + 1)
  const readings = new churn.FixedJournal({
    name: 'diagnostic readings',
    capacity: 16,
    bytes: 262144,
  })
  const ownership = new churn.FixedJournal({
    name: 'diagnostic ownership',
    capacity: 4,
    bytes: 8 * 1024 * 1024,
  })
  const metrics = new TelemetryMetrics({
    kind,
    clock: { wallMs: () => 1000, monotonicNs: () => BigInt(Math.floor(now * 1000000)) },
  })
  metrics.register({ name: 'com.example.journal.diagnostic', type: 'gauge', value: 1 })
  const subscription = { revision: 1n, requestedMetrics: ['com.example.'], deltaTemporality: false }
  metrics.select(subscription)
  const positive = metrics.snapshot(subscription)
  const positiveId = owners.issue(positive, 'positive')
  metrics.abandon(positive)
  const retained = []
  const ledger = kind === 'shareConsumer' ? new Ledger({ group: 'group', clock }) : null
  let handler
  let batches = 0

  /** Creates a real product object without giving any oracle a strong reference to it. */
  const product = () => {
    const snapshot = metrics.snapshot(subscription)
    owners.issue(snapshot, 'record')
    metrics.abandon(snapshot)
    if (seeded) retained.push({ snapshot, bytes: Buffer.alloc(4096, 0x6d) })
  }

  /** @returns {object} weak tables and both reserved scalar archives, without target references */
  const observerState = () => ({
    ...owners.inventory(),
    buffers: 2,
    byteLength: readings.byteLength + ownership.byteLength,
    archives: journalState([readings, ownership]),
  })

  /** Captures only at an original reading, with no extra collections for the ownership observer. */
  const capture = (checkpoint) => {
    const rows = owners.read()
    ownership.push({
      checkpoint,
      journals: storageState(),
      observer: observerState(),
      rows,
      positiveAlive: rows.find((row) => row.id === positiveId)?.alive === true,
      recordTargetsAlive: rows.filter((row) => row.kind === 'record' && row.alive).length,
    })
  }

  /** Collects liveness on a virtual replay grid; these readings never enter a memory verdict. */
  const sample = async (index) => {
    now = TELEMETRY.sampleAt[index]
    const reading = await memory.settled()
    readings.push({
      replayAtMs: now,
      observer: observerState(),
      snapshot: {
        memory: { ...reading, retainedBytes: reading.retained },
        registryDigest: 'diagnostic',
      },
    })
    if (index === 0) capture('first')
    if (index === TELEMETRY.sampleAt.length - 1) capture('last')
  }

  /** Adds the same scalar exchange/ack facts in both share journal modes. */
  const batchDone = async () => {
    batches++
    if (ledger) {
      ledger.exchanges.push({
        n: batches,
        at: now,
        responseAt: now + 0.125,
        request: { groupId: 'group', topics: [] },
        response: null,
      })
      ledger.record({
        payload: {
          topic: 'topic',
          partition: 0,
          firstOffset: String((batches - 1) * churn.CORPUS.batch),
          lastOffset: String(batches * churn.CORPUS.batch - 1),
          type: 'accept',
          error: null,
        },
      })
    }
    if (batches % 10 === 0) await sample(batches / 10)
  }

  const client = {
    events: { ACKNOWLEDGE: 'ack', FETCH: 'fetch' },
    on: () => {},
    subscribe: async () => {},
    run: async ({ eachMessage }) => (handler = eachMessage),
    commitOffsets: async () => {},
    send: async ({ messages }) => {
      for (let record = 0; record < messages.length; record++) product()
      await batchDone()
    },
  }
  const work = new Work({
    kind,
    client,
    stem: 'journal',
    topic: 'topic',
    ctl: 'ctl',
    clock,
    slots,
    ledger,
    sleep: async (ms) => {
      now += ms
    },
  })
  const storageState = () =>
    mode === 'fixed' ? journalState([work, ...(ledger ? [ledger] : [])]) : null
  const beforeStorage = storageState()
  try {
    await work.start()
    const baseline = await memory.settled()
    capture('M')
    await sample(0)
    if (kind === 'producer') await work.pace()
    else
      for (let batch = 0; batch < slots.length; batch++) {
        now = slots[batch]
        for (let record = 0; record < churn.CORPUS.batch; record++) {
          const index = batch * churn.CORPUS.batch + record
          product()
          await handler({
            partition: index % 3,
            message: {
              offset: String(index),
              key: churn.recordId('journal', kind, index),
            },
          })
        }
        await batchDone()
      }
    await work.stop()
    metrics.resetTransient()
    const closed = await memory.settled()
    capture('closed')
    const samples = readings.values()
    // Keep the positive root and seeded subtree through the closed reading and final weak census.
    if (positive.points.length !== 1 || (seeded && retained.length !== churn.CORPUS.records))
      throw new Error('Diagnostic positive root was lost')
    return {
      diagnostic: true,
      acceptanceEligible: false,
      attributionComplete: false,
      scope: 'broker-free measured-worker stand-in; no live-worker ownership claim',
      mode,
      kind,
      seeded,
      corpusSha256: sha(churn.CORPUS),
      scheduleSha256: sha({ slots, sampleAt: TELEMETRY.sampleAt }),
      idsSha256: sha(work.ids()),
      factsSha256: sha(ledger?.facts() ?? null),
      baseline,
      samples,
      closed,
      ownership: ownership.values(),
      storage: { mode, before: beforeStorage, closed: storageState() },
      calibration: null,
      timing: 'virtual-replay-ownership-only',
    }
  } finally {
    memory.stop()
  }
}

if (require.main === module)
  run(process.argv[2], process.argv[3], process.argv[4] === 'true')
    .then((result) => process.stdout.write(JSON.stringify(result)))
    .catch((error) => {
      process.stderr.write(error.stack)
      process.exitCode = 1
    })

module.exports = { run, RetainedCalibration }
