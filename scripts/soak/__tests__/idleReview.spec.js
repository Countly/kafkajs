jest.mock('../../../src/utils/shuffle', () => (values) => values)
const { instrument, IdleSocketCheck, PROFILES } = require('../soak')
const { createScriptedCluster, producerScript } = require('../../../testHelpers/scriptedCluster')
const { logLevel } = require('../../../index')

/**
 * Exercises real producer metadata, sockets and reaping on a fixed valid broker order.
 * @param {object} options producer cache age and measured profile length
 * @param {number} [options.metadataMaxAge] omitted preserves the public default
 * @param {number} [options.duration=0] fake-clock profile length; zero runs one cycle
 * @returns {Promise<void>}
 * @throws {Error} an ordinary healthy-success assertion or fixture cleanup failure
 */
const healthyIdle = async ({ metadataMaxAge, duration = 0 } = {}) => {
  jest.useFakeTimers({ doNotFake: ['setImmediate', 'queueMicrotask', 'hrtime'] })
  const nodes = [
    { id: 1, host: 'localhost', port: 9093 },
    { id: 0, host: 'localhost', port: 9092 },
  ]
  const seam = producerScript(createScriptedCluster({ nodes }), { topic: 'idle-topic' })
  const box = instrument(
    {
      brokers: ['localhost:9092'],
      socketFactory: seam.socketFactory,
      logLevel: logLevel.NOTHING,
      connectionsMaxIdle: 5000,
      connectionTimeout: 1000,
    },
    { activeResources: false, socketIdentities: true }
  )
  const producer = box.kafka.producer({
    idempotent: true,
    ...(metadataMaxAge === undefined ? {} : { metadataMaxAge }),
  })
  const readings = []
  /** Reads the real instrumented producer counters without changing them.
   * @returns {Promise<object>} resource reading */
  const probe = {
    snapshot: async () => {
      const row = await pump(box.probe.snapshot())
      readings.push(row)
      return row
    },
  }
  /** Observes and advances one bounded producer operation.
   * @param {Promise<*>} promise producer work, observed immediately
   * @returns {Promise<*>} result
   * @throws {Error} the operation error or fixture progress failure */
  const pump = async (promise) => {
    let finished = false
    let result
    let problem
    promise.then(
      (value) => {
        finished = true
        result = value
      },
      (error) => {
        finished = true
        problem = error
      }
    )
    for (let turn = 0; turn < 2000; turn++) {
      if (finished) break
      await jest.advanceTimersByTimeAsync(1)
    }
    if (!finished)
      throw new Error(
        'Fixture did not settle: ' + JSON.stringify(seam.requests().map(({ api }) => api))
      )
    if (problem) throw problem
    return result
  }
  const check = new IdleSocketCheck({
    producer: { send: (...args) => pump(producer.send(...args)) },
    probe,
    topic: 'idle-topic',
    record: () => {},
    nap: async (ms) => jest.advanceTimersByTimeAsync(ms),
  })
  try {
    await pump(producer.connect())
    const startedAt = Date.now()
    let outcome
    try {
      const deadline = Date.now() + duration
      do {
        await check.cycle()
      } while (Date.now() < deadline)
    } catch (error) {
      outcome = error
    }
    process.stdout.write(
      JSON.stringify({
        metadataMaxAge: metadataMaxAge ?? 'default',
        elapsed: Date.now() - startedAt,
        readingCount: readings.length,
        expansion: readings
          .filter((row, index) => index % 5 === 4 && row.sockets.open === 2)
          .slice(0, 1)
          .map(({ sockets }) => sockets),
        readings: readings.slice(-5).map(({ sockets }) => sockets),
        outcome: outcome?.message,
      }) + '\n'
    )
    expect(outcome).toBeUndefined()
    expect(Date.now() - startedAt).toBeGreaterThanOrEqual(duration)
    if (duration) expect(Date.now() - startedAt).toBeLessThan(duration + 13000)
    if (duration || metadataMaxAge === 9000) {
      expect(readings.some((row, index) => index % 5 === 4 && row.sockets.open === 2)).toBe(true)
      expect(seam.requests().filter(({ api }) => api === 'Metadata').length).toBeGreaterThan(1)
    }
    for (let index = 0; index < readings.length; index += 5) {
      const [warmup, warmIdle, before, idle, reopened] = readings.slice(index, index + 5)
      for (const row of [warmIdle, idle]) {
        expect(row.sockets.open).toBe(0)
        expect(row.sockets.live).toEqual([])
        expect(row.queueDepth).toBe(0)
      }
      for (const [previous, next] of [
        [warmIdle, before],
        [idle, reopened],
      ]) {
        expect(next.sockets.live.length).toBe(next.sockets.open)
        expect(new Set(next.sockets.live.map(({ node }) => node)).size).toBe(next.sockets.open)
        expect(
          next.sockets.live.every(
            ({ id, node }) => id > previous.sockets.created && next.socketNodes.includes(node)
          )
        ).toBe(true)
      }
      expect(warmup.sockets.open).toBeGreaterThan(0)
    }
  } finally {
    await pump(producer.disconnect({ timeout: 0 }))
    seam.assertClean()
    expect(jest.getTimerCount()).toBe(0)
    jest.useRealTimers()
  }
}

test('a healthy idle cycle succeeds when the bootstrap differs from the topic leader', async () => {
  await healthyIdle()
})

test('a healthy idle cycle completes when GC enables delayed heap sampling', async () => {
  const gc = globalThis.gc
  try {
    globalThis.gc = () => {}
    await healthyIdle()
  } finally {
    globalThis.gc = gc
    jest.useRealTimers()
  }
})

test('healthy default metadata expiry does not fail a full duration idle profile', async () => {
  expect(PROFILES.idle).toEqual([{ name: 'idle', workload: 'idle', ms: 420000 }])
  await healthyIdle({ duration: PROFILES.idle[0].ms })
})

test('healthy metadata expiry at 9000 ms permits an additional metadata socket', async () => {
  await healthyIdle({ metadataMaxAge: 9000 })
})

/**
 * Advances bounded fixture time while observing the task from its creation.
 * @param {Promise<*>} work an immediately observed foreground operation
 * @returns {Promise<*>} its result
 * @throws {Error} the operation error or a fixture progress failure
 */
const drive = async (work) => {
  let done = false
  let result
  let problem
  work.then(
    (value) => {
      done = true
      result = value
    },
    (error) => {
      done = true
      problem = error
    }
  )
  for (let turn = 0; turn < 1000; turn++) {
    if (done) break
    await jest.advanceTimersByTimeAsync(50)
  }
  if (!done) throw new Error('Idle main fixture did not settle')
  if (problem) throw problem
  return result
}

test('main routes idle through the measured workload and closes its counted producer', async () => {
  jest.useFakeTimers({ doNotFake: ['setImmediate', 'queueMicrotask', 'hrtime'] })
  jest.setSystemTime(10000)
  const seam = producerScript(
    createScriptedCluster({
      nodes: [
        { id: 1, host: 'localhost', port: 9093 },
        { id: 0, host: 'localhost', port: 9092 },
      ],
    }),
    { topic: 'soak-10000' }
  )
  const rows = []
  const gc = globalThis.gc
  const disconnect = jest.fn(async () => {})
  const consumer = {
    connect: async () => {},
    subscribe: async () => {},
    run: async () => {},
    disconnect,
  }
  const admin = {
    connect: async () => {},
    createTopics: async () => {},
    disconnect: async () => {},
  }
  let driver, running
  try {
    globalThis.gc = () => {}
    jest.doMock('../../../src/network/socketFactory', () => () => seam.socketFactory)
    jest.doMock('../../../testHelpers/e2eEvidence', () => ({
      writeEvidence: (_stem, row) => rows.push(row),
    }))
    jest.doMock('../../../testHelpers/resourceProbe', () => {
      const real = jest.requireActual('../../../testHelpers/resourceProbe')
      return {
        ...real,
        createResourceProbe: (ports) =>
          real.createResourceProbe({ ...ports, activeResources: false }),
      }
    })
    jest.doMock('../../clients', () => ({
      Clients: {
        use: async (_kafka, body) => {
          try {
            return await body({ admin: () => admin, consumer: () => consumer })
          } finally {
            await consumer.disconnect()
          }
        },
      },
    }))
    jest.isolateModules(() => {
      driver = require('../soak')
    })
    running = jest.spyOn(driver.Soak.prototype, 'run').mockImplementation(async function () {
      expect(this.context.idleSocketCheck).toBeInstanceOf(driver.IdleSocketCheck)
      await this.workloads.idle(this.context, 1)
      return { ok: true }
    })
    expect(
      await drive(driver.main('idle', { brokers: ['localhost:9092'], phaseSeconds: 1 }))
    ).toEqual({ ok: true })
    expect(running).toHaveBeenCalledTimes(1)
    const cycles = rows.filter(({ kind }) => kind === 'idle-cycle')
    expect(cycles).toHaveLength(1)
    expect(cycles[0]).toMatchObject({
      config: { connectionsMaxIdle: 5000 },
      wireVersions: { Produce: expect.any(Number) },
      warmIdle: { sockets: { open: 0 } },
      idle: { sockets: { open: 0 } },
      before: { sockets: { open: 1 } },
      reopened: { sockets: { open: 1 } },
    })
    expect(seam.requests().filter(({ api }) => api === 'Produce')).toHaveLength(3)
    expect(seam.sockets().every((socket) => socket.destroyed)).toBe(true)
    expect(disconnect).toHaveBeenCalled()
    seam.assertClean()
  } finally {
    running?.mockRestore()
    jest.dontMock('../../../src/network/socketFactory')
    jest.dontMock('../../../testHelpers/e2eEvidence')
    jest.dontMock('../../clients')
    jest.dontMock('../../../testHelpers/resourceProbe')
    globalThis.gc = gc
    expect(jest.getTimerCount()).toBe(0)
    jest.useRealTimers()
  }
})

test('identity observations stay coherent when heap sampling spans an idle expiry', async () => {
  jest.useFakeTimers({ doNotFake: ['setImmediate', 'queueMicrotask', 'hrtime'] })
  const gc = globalThis.gc
  const seam = producerScript(
    createScriptedCluster({ nodes: [{ id: 0, host: 'localhost', port: 9092 }] }),
    { topic: 'idle-topic' }
  )
  let producer
  try {
    globalThis.gc = () => {}
    const box = instrument(
      {
        brokers: ['localhost:9092'],
        socketFactory: seam.socketFactory,
        logLevel: logLevel.NOTHING,
        connectionsMaxIdle: 5000,
      },
      { activeResources: false, socketIdentities: true }
    )
    producer = box.kafka.producer({ idempotent: true })
    await drive(producer.connect())
    await drive(
      producer.send({ topic: 'idle-topic', messages: [{ partition: 0, value: 'sample' }] })
    )
    await jest.advanceTimersByTimeAsync(4900)
    const row = await drive(box.probe.snapshot())
    expect(row.sockets.open).toBeGreaterThan(0)
    expect(row.sockets.live).toHaveLength(row.sockets.open)
    expect(seam.sockets().every((socket) => socket.destroyed)).toBe(true)
  } finally {
    if (producer) await drive(producer.disconnect({ timeout: 0 }))
    seam.assertClean()
    globalThis.gc = gc
    expect(jest.getTimerCount()).toBe(0)
    jest.useRealTimers()
  }
})
