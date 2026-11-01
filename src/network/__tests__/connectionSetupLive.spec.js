const { Kafka, logLevel } = require('../../../index')
const defaultSocketFactory = require('../socketFactory')
const { plainTextBrokers, secureRandom, createTopic } = require('testHelpers')
const { readLog } = require('../../../testHelpers/rawLog')
const { writeEvidence, wireVersionsOf } = require('../../../testHelpers/e2eEvidence')

const live = process.env.CONNECT_SETUP_E2E === '1' ? describe : describe.skip
const BUDGET_MS = 90000

/**
 * Withholds transport-ready notification on one real endpoint. The underlying
 * socket still connects to Kafka; timeout and destruction belong to the client.
 * Durations exclude reconnect backoff and caller retry delay. This is a dial-ready
 * fault, not an assertion that application-frame blackholing blocks a TCP SYN.
 */
class SetupProbe {
  /** @returns {SetupProbe} one finite fault schedule */
  constructor() {
    this.attempts = []
    this.sockets = []
    this.block = true
    this.target = null
    this.transport = defaultSocketFactory()
    this.factory = this.factory.bind(this)
  }

  /**
   * @param {object} args public socketFactory arguments
   * @returns {object} an owned real socket
   */
  factory(args) {
    const address = `${args.host}:${args.port}`
    if (this.target === null) this.target = address
    const held = this.block && address === this.target
    const startedAt = performance.now()
    const socket = this.transport({
      ...args,
      onConnect: () => {
        if (!held) args.onConnect()
      },
    })
    this.sockets.push(socket)
    const destroy = socket.destroy.bind(socket)
    let counted = false
    socket.destroy = (...params) => {
      if (held && !counted) {
        counted = true
        this.attempts.push({ address, startedAt, duration: performance.now() - startedAt })
        if (this.attempts.length === 4 || this.attempts.length === 5) this.block = false
      }
      return destroy(...params)
    }
    return socket
  }
}

live('Connection setup live', () => {
  test(
    'setup budgets grow on one node and ready resets the next attempt',
    async () => {
      const probe = new SetupProbe()
      const brokers = [plainTextBrokers()[0]]
      const kafka = new Kafka({
        brokers,
        logLevel: logLevel.NOTHING,
        socketFactory: probe.factory,
        connectionTimeout: 500,
        connectionTimeoutMax: 3000,
        retry: { retries: 5, initialRetryTime: 1, maxRetryTime: 1, factor: 0 },
      })
      const admin = kafka.admin()
      const requests = []
      const off = admin.on(admin.events.REQUEST, ({ payload }) => requests.push(payload))
      const producer = kafka.producer({ idempotent: true })
      const producerOff = producer.on(producer.events.REQUEST, ({ payload }) =>
        requests.push(payload)
      )
      const topic = `e2e-p9d-connect-setup-${secureRandom()}`
      try {
        await admin.connect()
        expect(probe.attempts).toHaveLength(4)
        const windows = [
          [400, 600],
          [800, 1200],
          [1600, 2400],
          [2400, 3000],
        ]
        for (let i = 0; i < windows.length; i++) {
          expect(probe.attempts[i].address).toBe(probe.target)
          expect(probe.attempts[i].duration).toBeGreaterThanOrEqual(windows[i][0])
          expect(probe.attempts[i].duration).toBeLessThanOrEqual(windows[i][1] + 250)
        }
        probe.block = true
        const current = probe.sockets.filter((socket) => !socket.destroyed)
        expect(current.length).toBeGreaterThan(0)
        for (const socket of current) socket.destroy(new Error('transport interrupted'))
        await new Promise(setImmediate)
        await admin.connect()
        expect(probe.attempts).toHaveLength(5)
        expect(probe.attempts[4].duration).toBeGreaterThanOrEqual(400)
        expect(probe.attempts[4].duration).toBeLessThanOrEqual(850)
        await createTopic({ topic, partitions: 1 })
        await producer.connect()
        await producer.send({ topic, messages: [{ value: 'ready-record' }] })
        const log = await readLog({ brokers: plainTextBrokers(), topic })
        expect(
          [...log.values()].flatMap(({ records }) => records).map(({ value }) => value.toString())
        ).toEqual(['ready-record'])
        writeEvidence('connect-setup', {
          kind: 'oracle',
          config: { connectionTimeout: 500, connectionTimeoutMax: 3000, retries: 5 },
          wireVersions: wireVersionsOf(requests),
          attempts: probe.attempts,
          log: ['ready-record'],
          committedOffsets: 'n/a',
          handlerDelivery: 'n/a',
          ok: true,
        })
      } finally {
        off()
        producerOff()
        await Promise.allSettled([producer.disconnect(), admin.disconnect()])
        expect(probe.sockets.filter((socket) => !socket.destroyed)).toHaveLength(0)
      }
    },
    BUDGET_MS
  )

  test(
    'an explicit setup timeout stays flat without a maximum',
    async () => {
      const probe = new SetupProbe()
      const kafka = new Kafka({
        brokers: [plainTextBrokers()[0]],
        socketFactory: probe.factory,
        logLevel: logLevel.NOTHING,
        connectionTimeout: 500,
        retry: { retries: 5, initialRetryTime: 1, maxRetryTime: 1, factor: 0 },
      })
      const admin = kafka.admin()
      const requests = []
      const off = admin.on(admin.events.REQUEST, ({ payload }) => requests.push(payload))
      try {
        await admin.connect()
        expect(probe.attempts).toHaveLength(4)
        for (const attempt of probe.attempts) {
          expect(attempt.duration).toBeGreaterThanOrEqual(500)
          expect(attempt.duration).toBeLessThanOrEqual(750)
        }
        writeEvidence('connect-setup', {
          kind: 'flat',
          config: { connectionTimeout: 500, retries: 5 },
          wireVersions: wireVersionsOf(requests),
          attempts: probe.attempts,
          ok: true,
          log: 'n/a',
          committedOffsets: 'n/a',
          handlerDelivery: 'n/a',
        })
      } finally {
        off()
        await admin.disconnect()
        expect(probe.sockets.filter((socket) => !socket.destroyed)).toHaveLength(0)
      }
    },
    BUDGET_MS
  )
})
