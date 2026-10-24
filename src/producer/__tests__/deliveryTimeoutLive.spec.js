const { Kafka, logLevel } = require('../../../index')
const apiKeys = require('../../protocol/requests/apiKeys')
const { readLog } = require('../../../testHelpers/rawLog')
const {
  createTopic,
  plainTextBrokers,
  secureRandom,
  writeEvidence,
  wireVersionsOf,
} = require('testHelpers')
const { sleep, waitForCondition } = require('../../consumer/__tests__/fixtures/liveScenario')
const { ResponseDrop, createLiveSockets, repliesOf } = require('./fixtures/liveWire')
const {
  CleanupStack,
  ProduceLedger,
  ProducerProbe,
  dataRecords,
  describeError,
  leaderOf,
  sequenceGaps,
  settle,
} = require('./fixtures/liveProbe')
const { caseTimeout, liveCase } = require('./fixtures/bounded')

// Gated: it drops one Produce answer on purpose. Run alone: `npm run test:e2e:delivery-timeout`.
const describeDeadline = process.env.DELIVERY_TIMEOUT_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'delivery-timeout'
const CASE_BUDGET_MS = 60000
const DELIVERY_TIMEOUT_MS = 5000
const REQUEST_TIMEOUT_MS = 1000
const BUFFER_MEMORY = 1048576
const MAX_REQUEST_SIZE = 1048576
const DISCONNECT_BUDGET_MS = 15000
const APPLIED_BUDGET = { attempts: 40, delay: 50 } // about 2 s: well inside the deadline
const QUIET_MS = 1500
const MARKERS = 10

/** The four settings the case declares, plus the buffer the closing probe fills. */
const SETTINGS = Object.freeze({
  requestTimeout: REQUEST_TIMEOUT_MS,
  enforceRequestTimeout: false,
  linger: 0,
  deliveryTimeout: DELIVERY_TIMEOUT_MS,
  idempotent: true,
  acks: -1,
  bufferMemory: BUFFER_MEMORY,
})

const REPLAY = Object.freeze({
  log: 'the applied first record once; an explicit application resend may duplicate it',
  offsets: 'n/a',
  handlers: 'n/a',
})

/**
 * Live proof that a write the broker applied, whose answer never arrives, ends at the delivery
 * deadline with an unknown outcome and wedges nothing: the request timeout is not enforced, so
 * only the delivery deadline can end the send. The producer sends nothing again for the settled
 * record, the markers that follow take the next sequences, and an application-level resend is a
 * new record. The drop is one answer of one connection, counted across reconnects; the log is read
 * through the raw reader while the producer still waits.
 *
 * Replay sets: the physical log holds `lost` once after its expiry and at most twice after the
 * explicit resend; committed offsets and handler deliveries are not part of this scenario.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-91+Provide+Intuitive+User+Timeouts+in+The+Producer
 */
describeDeadline('Producer > a lost Produce answer and the delivery deadline (live)', () => {
  test(
    'an applied write with a lost answer settles at the delivery deadline',
    liveCase(CASE_BUDGET_MS, async (scope) => {
      const id = secureRandom()
      const topic = `e2e-p9b-delivery-timeout-${id}`
      const clientId = `e2e-p9b-delivery-timeout-${id}`
      const brokers = plainTextBrokers()
      const sockets = createLiveSockets()
      const ledger = new ProduceLedger()
      const requests = []
      const cleanup = new CleanupStack()

      /** @returns {Promise<{ values: string[], records: object[] }>} the topic's log, as the broker holds it */
      const rawLog = async () => {
        const records = dataRecords(await readLog({ brokers, topic }))
        return { values: records.map(({ value }) => String(value)), records }
      }

      await cleanup.run(async () => {
        await createTopic({ topic, partitions: 1, replicas: 3 })
        const kafka = new Kafka({
          clientId,
          brokers,
          logLevel: logLevel.NOTHING,
          socketFactory: sockets.socketFactory,
          requestTimeout: REQUEST_TIMEOUT_MS,
          enforceRequestTimeout: false,
        })
        const producer = kafka.producer({
          idempotent: true,
          linger: 0,
          deliveryTimeout: DELIVERY_TIMEOUT_MS,
          bufferMemory: BUFFER_MEMORY,
          maxRequestSize: MAX_REQUEST_SIZE,
        })
        producer.on(producer.events.REQUEST, ({ payload }) => requests.push(payload))
        const probe = new ProducerProbe({
          producer,
          frames: sockets.frames,
          clientId,
          maxRequestSize: MAX_REQUEST_SIZE,
        })
        const admin = new Kafka({ brokers, logLevel: logLevel.NOTHING }).admin()
        ledger.install()
        cleanup.add('produce ledger', () => ledger.restore())
        probe.install()
        cleanup.add('admission probe', () => probe.restore())
        cleanup.add('resources after disconnect', () => probe.settled())
        await admin.connect()
        cleanup.add('admin', () => admin.disconnect())
        await producer.connect()
        cleanup.add('producer', () => producer.disconnect({ timeout: DISCONNECT_BUDGET_MS }))

        await probe.capture(
          () => producer.send({ topic, messages: [{ value: 'warm' }], acks: -1 }),
          { signal: scope }
        )
        const leader = await leaderOf(admin, topic)
        const drop = new ResponseDrop(sockets.chaos, {
          port: leader.port,
          apiKey: apiKeys.Produce,
        }).arm()
        cleanup.add('response drop', () => drop.release())

        const produceMark = sockets.frames.mark()
        const startedAt = performance.now()
        let settledAt = null
        const lost = settle(producer.send({ topic, messages: [{ value: 'lost' }], acks: -1 })).then(
          (outcome) => {
            settledAt = performance.now()
            return outcome
          }
        )

        const applied = await waitForCondition(
          async () => (await rawLog()).values.includes('lost'),
          APPLIED_BUDGET
        )
        const appliedAtMs = performance.now() - startedAt
        const waitingWhenApplied = settledAt === null

        const outcome = await lost
        const elapsedMs = settledAt - startedAt
        const writesAtExpiry = sockets.frames.since(apiKeys.Produce, produceMark).length
        const { error } = outcome
        const protocolErrors = [error?.type, error?.cause?.type].filter(Boolean)

        const markerOutcomes = []
        for (let index = 0; index < MARKERS; index++) {
          markerOutcomes.push(
            await settle(
              producer.send({ topic, messages: [{ value: `marker-${index}` }], acks: -1 })
            )
          )
        }
        markerOutcomes.forEach((marker) => protocolErrors.push(marker.error?.type))
        const beforeResend = await rawLog()
        const resent = await settle(
          producer.send({ topic, messages: [{ value: 'lost' }], acks: -1 })
        )

        drop.release()
        await sleep(QUIET_MS)
        const final = await rawLog()
        const gaps = sequenceGaps(final.records)
        const markerValues = Array.from({ length: MARKERS }, (_, index) => `marker-${index}`)
        const occurrences = final.values.filter((value) => value === 'lost').length

        const closing = await probe.closing({
          topic: `e2e-p9b-delivery-timeout-probe-${id}`,
          budgetMs: DELIVERY_TIMEOUT_MS,
        })
        await producer.disconnect({ timeout: DISCONNECT_BUDGET_MS })
        const afterDisconnect = probe.counters()

        writeEvidence(EVIDENCE, {
          scenario: 'delivery-timeout',
          case: 'applied-write-lost-answer',
          config: SETTINGS,
          replay: REPLAY,
          wireVersions: wireVersionsOf(requests),
          wire: { frames: sockets.frames.trace(), replies: repliesOf(requests) },
          leader,
          drops: drop.dropped,
          timing: { appliedAtMs, waitingWhenApplied, elapsedMs, writesAtExpiry },
          lost: describeError(error),
          resent: describeError(resent.error),
          markerErrors: markerOutcomes.map((marker) => describeError(marker.error)),
          log: { applied, beforeResend: beforeResend.values, final: final.values },
          sequences: ledger.of(topic).map(({ partitions, results, error: callError }) => ({
            firstSequence: partitions[0].firstSequence,
            first: partitions[0].first,
            errorCodes: results?.map((result) => result.errorCode) ?? null,
            error: describeError(callError),
          })),
          gaps,
          closing,
          afterDisconnect,
        })

        expect(applied).toBe(true)
        expect(waitingWhenApplied).toBe(true)
        expect(drop.dropped).toBe(1)
        expect(writesAtExpiry).toBe(1)
        expect(error).toMatchObject({ name: 'KafkaJSDeliveryTimeout', outcome: 'unknown' })
        expect(elapsedMs).toBeGreaterThanOrEqual(DELIVERY_TIMEOUT_MS)
        expect(elapsedMs).toBeLessThanOrEqual(DELIVERY_TIMEOUT_MS + 1000)
        expect(beforeResend.values.filter((value) => value === 'lost')).toHaveLength(1)
        expect(protocolErrors).not.toContain('OUT_OF_ORDER_SEQUENCE_NUMBER')
        expect(markerOutcomes.map((marker) => marker.error)).toEqual(Array(MARKERS).fill(null))
        expect(beforeResend.values.filter((value) => value.startsWith('marker-'))).toEqual(
          markerValues
        )
        expect(resent.error).toBeNull()
        expect(occurrences).toBeGreaterThanOrEqual(1)
        expect(occurrences).toBeLessThanOrEqual(2)
        expect(gaps).toEqual([])
        expect(afterDisconnect).toMatchObject({ available: afterDisconnect.capacity, pins: 0 })
      })
    }),
    caseTimeout(CASE_BUDGET_MS)
  )
})
