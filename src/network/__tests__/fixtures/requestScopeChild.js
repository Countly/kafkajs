const assert = require('node:assert/strict')
const { setImmediate: turn } = require('node:timers/promises')
const Connection = require('../../connection')
const RequestQueue = require('../../requestQueue')
const Encoder = require('../../../protocol/encoder')
const {
  currentDeadline,
  runWithDeadline,
  shareMetadata,
  withConsumerOperation,
} = require('../../../admin/deadline')
const { createLogger, LEVELS } = require('../../../loggers')
const { createFakeSocketFactory } = require('../../../../testHelpers/fakeSocket')

const logger = createLogger({ level: LEVELS.NOTHING, logCreator: () => () => {} })
const protocol = {
  request: {
    apiKey: 0,
    apiVersion: 7,
    apiName: 'Produce',
    expectResponse: () => false,
    encode: async () => new Encoder(),
  },
  response: {},
}

/**
 * Runs work under a real request scope whose only application payload is a weakly observed marker.
 * @param {string} kind consumer, admin or discovery
 * @param {Function} work request or timer setup
 * @returns {Promise<WeakRef<object>>} no strong scope or marker reference escapes
 */
const scoped = async (kind, work) => {
  const marker = { completedRun: true }
  const weak = new WeakRef(marker)
  if (kind === 'consumer') {
    await withConsumerOperation(
      { marker, check() {}, guard: () => null, register: () => () => {}, wait: (fn) => fn() },
      work
    )
  } else {
    const body = () => {
      currentDeadline().marker = marker
      return work()
    }
    if (kind === 'admin') await runWithDeadline('scope collection', 10000, body)
    else await shareMetadata(body)()
  }
  return weak
}

/**
 * Collects in separate jobs with async major GC, avoiding conservative roots on the calling stack.
 * @returns {Promise<void>} weak keep-alives and completed callbacks have retired
 */
const collect = async () => {
  for (let pass = 0; pass < 5; pass++) {
    await turn()
    await global.gc({ type: 'major', execution: 'async' })
  }
}

/**
 * Keeps the real connection or queue alive across collection, plus an independent positive root.
 * @param {string} mode settled, closed, retired or timer
 * @param {string} kind request scope kind
 * @returns {Promise<object>} scalar collection and lifecycle observations
 */
const run = async (mode, kind) => {
  const positive = { retained: true }
  const retained = new WeakRef(positive)
  if (mode === 'timer') {
    const queue = new RequestQueue({ logger, enforceRequestTimeout: true, requestTimeout: 30000 })
    try {
      const marker = await scoped(kind, () => queue.scheduleRequestTimeoutCheck())
      await collect()
      assert.notEqual(marker.deref(), undefined, 'the active interval must retain its scope')
      queue.destroy()
      queue.destroy()
      await collect()
      return {
        markerCollected: marker.deref() === undefined,
        intervalCleared: queue.requestTimeoutIntervalId === null,
        queueAlive: queue.isIdle(),
        positiveAlive: retained.deref() === positive,
      }
    } finally {
      queue.destroy()
    }
  }

  const { factory } = createFakeSocketFactory()
  const connection = new Connection({
    host: 'node0',
    port: 9092,
    clientId: 'scope-collection',
    logger,
    socketFactory: factory,
    enforceRequestTimeout: false,
  })
  try {
    await connection.connect()
    const marker = await scoped(kind, () => {
      if (mode !== 'retired') return connection.send(protocol)
      connection
        .send({
          ...protocol,
          request: { ...protocol.request, encode: () => new Promise(() => {}) },
        })
        .catch(() => {})
    })
    if (mode === 'retired') {
      await turn()
      assert.equal(connection.preparing, 1, 'the encoder must still be blocked')
    }
    if (mode !== 'settled') await connection.disconnect({ drainTimeout: 0 })
    await collect()
    return {
      markerCollected: marker.deref() === undefined,
      connected: connection.isConnected(),
      queueAlive: connection.requestQueue.isIdle(),
      positiveAlive: retained.deref() === positive,
    }
  } finally {
    await connection.disconnect({ drainTimeout: 0 })
  }
}

run(process.argv[2], process.argv[3]).then(
  (result) => process.stdout.write(JSON.stringify(result)),
  (error) => {
    process.stderr.write(error.stack)
    process.exitCode = 1
  }
)
