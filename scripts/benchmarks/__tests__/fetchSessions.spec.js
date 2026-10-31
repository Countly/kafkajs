const {
  FetchObservation,
  verifyCycle,
  control,
  SessionMeasurement,
  deliveryControl,
  verifyDelivery,
  retained,
  memoryControl,
  runArm,
  selectDefault,
  FrameJournal,
  windowTotals,
  main,
} = require('../fetchSessions')
const { EventEmitter } = require('events')
const fs = require('fs')
const os = require('os')
const path = require('path')

test('full-cycle bytes are summed over three brokers with the declared partition shape', () => {
  const result = control()
  expect(result.shape).toEqual([334, 333, 333])
  expect(result.requests).toBe(3)
  expect(result.responses).toBe(3)
  expect(result.requestBytes).toBe(33168)
  expect(result.bytes).toBe(33231)
  expect(result.frames.filter((f) => f.direction === 'request').every((f) => f.bytes < 20000)).toBe(
    true
  )
})
test('idle incremental size and cycle membership have independent controls', () => {
  const frames = [0, 1, 2].map((nodeId) => ({
    direction: 'request',
    nodeId,
    bytes: 128,
  }))
  expect(verifyCycle(frames, { full: false })).toBe(384)
  expect(() =>
    verifyCycle([...frames.slice(0, 2), { ...frames[2], bytes: 129 }], {
      full: false,
    })
  ).toThrow('above 128')
  expect(() => verifyCycle(frames.slice(0, 2), { full: false })).toThrow('Incomplete')
  expect(() => verifyCycle(frames, { full: true })).toThrow('below 20000')
})
test('socket observation preserves write return, callback and arguments and restores on close', () => {
  const observation = new FetchObservation()
  const socket = new EventEmitter()
  const callback = () => {}
  socket.write = jest.fn(() => false)
  const original = socket.write
  const wrapped = observation.socketFactory(
    () => socket,
    () => 0
  )({ host: 'local', port: 1 })
  const request = Buffer.alloc(14)
  request.writeInt32BE(10)
  request.writeInt16BE(1, 4)
  request.writeInt32BE(8, 8)
  expect(wrapped.write(request, callback)).toBe(false)
  expect(original).toHaveBeenCalledWith(request, callback)
  const response = Buffer.alloc(8)
  response.writeInt32BE(4)
  response.writeInt32BE(8, 4)
  wrapped.emit('data', response)
  expect(observation.result()).toMatchObject({
    bytes: 22,
    requests: 1,
    responses: 1,
  })
  wrapped.emit('close')
  expect(wrapped.write).toBe(original)
  expect(wrapped.listenerCount('data')).toBe(0)
})
test('an idle observation without a socket counts no logical fetch as a frame', () => {
  expect(new FetchObservation().result()).toMatchObject({
    bytes: 0,
    requests: 0,
    responses: 0,
    frames: [],
  })
})

const header = (id) => {
  const request = Buffer.alloc(14)
  request.writeInt32BE(10)
  request.writeInt16BE(1, 4)
  request.writeInt32BE(id, 8)
  return request
}
const reply = (id) => {
  const response = Buffer.alloc(8)
  response.writeInt32BE(4)
  response.writeInt32BE(id, 4)
  return response
}
const factoryFor = (observation, write = () => true) =>
  observation.socketFactory(
    () => {
      const socket = new EventEmitter()
      socket.write = jest.fn(write)
      return socket
    },
    () => 0
  )

test('each connection a factory returns is its own socket, so reconnects reuse IDs safely', () => {
  const observation = new FetchObservation()
  const factory = factoryFor(observation)
  const first = factory({})
  const second = factory({})
  first.write(header(7))
  second.write(header(7))
  second.emit('data', reply(7))
  first.emit('data', reply(7))
  expect(observation.result()).toMatchObject({ requests: 2, responses: 2, bytes: 44 })
  expect(new Set(observation.result().frames.map((frame) => frame.connection)).size).toBe(2)
})

test('a failing write passes its error through and leaves the observation invalid', () => {
  const observation = new FetchObservation()
  const failure = new Error('EPIPE')
  const socket = factoryFor(observation, () => {
    throw failure
  })({})
  expect(() => socket.write(header(9), 'utf8')).toThrow(failure)
  expect(() => observation.result()).toThrow(failure)
})

test('closing a socket that never carried traffic still removes its listeners', () => {
  const observation = new FetchObservation()
  const socket = factoryFor(observation)({})
  const wrapped = socket.write
  expect(socket.listenerCount('data')).toBe(1)
  socket.emit('close')
  expect(socket.listenerCount('data')).toBe(0)
  expect(socket.listenerCount('close')).toBe(0)
  expect(socket.write).not.toBe(wrapped)
  expect(jest.isMockFunction(socket.write)).toBe(true)
})

test('only the control and the explicit arm arguments are accepted', async () => {
  for (const args of [[], ['--partitions', '1000'], ['--arm', 'S1', '--sessions', 'maybe']])
    await expect(main(args)).rejects.toThrow('Invalid session driver arguments')
  const write = jest.spyOn(process.stdout, 'write').mockImplementation(() => true)
  const gc = global.gc
  require('v8').setFlagsFromString('--expose-gc')
  global.gc = require('vm').runInNewContext('gc')
  try {
    await main(['--control'])
    const record = JSON.parse(write.mock.calls[0][0])
    expect(record.frames.bytes).toBe(33231)
    expect(record.delivery.map((sample) => sample.handlerEntries)).toEqual([2, 4, 12])
    expect(record.retainedControlBytes).toBeGreaterThan(3 * 1024 * 1024)
  } finally {
    write.mockRestore()
    global.gc = gc
  }
})

test('the same twelve messages in different batches keep messages separate from entries and gaps', () => {
  const samples = deliveryControl()
  expect(samples.map((s) => s.deliveredMessages)).toEqual([12, 12, 12])
  expect(samples.map((s) => s.handlerEntries)).toEqual([2, 4, 12])
  expect(samples.map((s) => s.gapSamples)).toEqual([1, 3, 11])
  expect(() =>
    verifyDelivery(
      { ...samples[0], deliveredMessages: 2 },
      {
        deliveredMessages: 12,
        handlerEntries: 2,
        gapSamples: 1,
      }
    )
  ).toThrow('deliveredMessages')
})
test('CPU is user plus system over the same frozen window and idle never divides by zero', () => {
  let clock = 0
  let cpu = { user: 10, system: 20 }
  const m = new SessionMeasurement({ now: () => clock, cpu: () => cpu })
  m.begin()
  m.frame({ apiKey: 1, at: 1, bytes: 120, direction: 'request' })
  m.frame({ apiKey: 1, at: 2, bytes: 40, direction: 'response' })
  m.frame({ apiKey: 3, at: 2, bytes: 500, direction: 'response' })
  clock = 1000
  cpu = { user: 110, system: 70 }
  const sample = m.finish()
  expect(sample).toMatchObject({
    cpuMicros: 150,
    bytes: 160,
    bytesPerSecond: 160,
    cpuMicrosPerSecond: 150,
    bytesPerMessage: null,
    cpuMicrosPerMessage: null,
    requests: 1,
    responses: 1,
  })
  m.enter(99)
  m.frame({ apiKey: 1, at: 1001, bytes: 10000, direction: 'response' })
  expect(sample.bytes).toBe(160)
  expect(sample.deliveredMessages).toBe(0)
})
test('message denominator determines CPU and bytes per delivered message', () => {
  let clock = 0
  const m = new SessionMeasurement({
    now: () => clock,
    cpu: () => ({ user: clock * 12, system: clock * 6 }),
  })
  m.begin()
  clock = 1
  m.enter(12)
  m.frame({ apiKey: 1, at: 1, bytes: 120, direction: 'response' })
  clock = 2
  expect(m.finish()).toMatchObject({
    deliveredMessages: 12,
    handlerEntries: 1,
    gapSamples: 0,
    cpuMicrosPerMessage: 3,
    bytesPerMessage: 10,
  })
})
test('insufficient actual gaps refuse an otherwise exact message total', () => {
  expect(() =>
    verifyDelivery(
      { deliveredMessages: 200001, handlerEntries: 10, gapSamples: 9 },
      {
        deliveredMessages: 200001,
        handlerEntries: 10,
        gapSamples: 9,
        minimumGaps: 1000,
      }
    )
  ).toThrow('Insufficient')
})
test('retained heap uses two collections separated by exactly one hundred milliseconds', async () => {
  const events = []
  expect(
    await retained({
      gc: () => events.push('gc'),
      pause: async (ms) => events.push(ms),
      heap: () => {
        events.push('heap')
        return 123
      },
    })
  ).toBe(123)
  expect(events).toEqual(['gc', 100, 'gc', 'heap'])
  await expect(retained({ gc: null })).rejects.toThrow('--expose-gc')
})
test('a disabled retained observer fails the large-object sensitivity control', async () => {
  await expect(
    memoryControl({ gc: () => {}, pause: async () => {}, heap: () => 100 })
  ).rejects.toThrow('blind')
})
test('primary Fetch bytes and independent raw transcript agree and no socket counts no frames', () => {
  const result = control()
  expect(result).toMatchObject({ shape: [334, 333, 333], requests: 3, responses: 3 })
  expect(result.requestBytes).toBeGreaterThanOrEqual(20000)
  expect(result.incrementalRequestBytes).toBeLessThanOrEqual(384)
  expect(new FetchObservation().result()).toMatchObject({ bytes: 0, requests: 0, responses: 0 })
})
test('the public arm counts real batch lengths and freezes before retained reading and cleanup', async () => {
  let clock = 0
  let config
  const measurement = new SessionMeasurement({
    now: () => ++clock,
    cpu: () => ({ user: clock * 2, system: clock }),
  })
  const sizes = [...Array(1379).fill(145), 46]
  const consumer = {
    connect: jest.fn(async () => {}),
    subscribe: jest.fn(async () => {}),
    run: jest.fn(async (options) => {
      for (const size of sizes)
        await options.eachBatch({ batch: { messages: Array(size).fill(null) } })
    }),
    stop: jest.fn(async () => {
      clock += 5000
    }),
    disconnect: jest.fn(async () => {
      clock += 5000
    }),
  }
  const sample = await runArm(
    {
      consumer: (options) => {
        config = options
        return consumer
      },
    },
    {
      arm: 'S1',
      topic: 'corpus',
      sessions: true,
      measurement,
      readRetained: async () => {
        clock += 10000
        return 1234
      },
    }
  )
  expect(config).toMatchObject({
    fetchSessions: true,
    maxWaitTimeInMs: 500,
    maxBytesPerPartition: 32768,
  })
  expect(consumer.run.mock.calls[0][0].autoCommit).toBe(false)
  expect(sample).toMatchObject({
    deliveredMessages: 200001,
    handlerEntries: 1380,
    gapSamples: 1379,
    heapRetainedBytes: 1234,
  })
  expect(sample.elapsedMs).toBeLessThan(10000)
  expect(consumer.disconnect).toHaveBeenCalledTimes(1)
})
test('idle public arms use ten seconds of warmup then one sixty-second window', async () => {
  let clock = 0
  const measurement = new SessionMeasurement({
    now: () => clock,
    cpu: () => ({ user: clock, system: 0 }),
  })
  const consumer = {
    connect: async () => {},
    subscribe: async () => {},
    run: async () => {},
    stop: async () => {},
    disconnect: jest.fn(async () => {}),
  }
  const wait = jest.fn(async (ms) => {
    clock += ms
  })
  const sample = await runArm(
    { consumer: () => consumer },
    { arm: 'L3', sessions: false, topic: 'empty', measurement, wait, readRetained: async () => 10 }
  )
  expect(wait.mock.calls).toEqual([[10000], [60000]])
  expect(sample).toMatchObject({
    elapsedMs: 60000,
    deliveredMessages: 0,
    bytesPerMessage: null,
    cpuMicrosPerMessage: null,
  })
  expect(consumer.disconnect).toHaveBeenCalledTimes(1)
})

/** @returns {object} fixed independent gate boundary inputs */
function gateEvidence() {
  const common = {
    messagesPerSecond: 100,
    deliveryGapP99: 100,
    gapSamples: 1000,
    cpuMicrosPerMessage: 100,
  }
  return {
    correctness: 'passed',
    controls: true,
    aaDeltas: Array(10).fill(0),
    arms: {
      S1: {
        off: { ...common },
        on: { ...common, messagesPerSecond: 97, deliveryGapP99: 105, cpuMicrosPerMessage: 103 },
      },
      L1: { off: { ...common }, on: { ...common, messagesPerSecond: 97, deliveryGapP99: 105 } },
      S2: { off: { bytesPerSecond: 100 }, on: { bytesPerSecond: 100 } },
      L2: {
        off: { bytesPerMessage: 100, cpuMicrosPerMessage: 100, heapRetainedBytes: 10000000 },
        on: { bytesPerMessage: 10, cpuMicrosPerMessage: 75, heapRetainedBytes: 13145728 },
      },
      L3: { off: { bytesPerSecond: 100 }, on: { bytesPerSecond: 10 } },
    },
  }
}
test('correctness failed or unrun blocks shipping independently of the selected default', () => {
  for (const correctness of ['failed', 'unrun'])
    expect(selectDefault({ ...gateEvidence(), correctness })).toBe('blocked')
})
test('all fixed gate boundary values pass only with valid controls and both settings A/A', () => {
  expect(selectDefault(gateEvidence())).toBe('true')
  expect(selectDefault({ ...gateEvidence(), controls: false })).toBe('false')
  expect(selectDefault({ ...gateEvidence(), aaDeltas: Array(9).fill(0) })).toBe('false')
  expect(selectDefault({ ...gateEvidence(), aaDeltas: [0.031, ...Array(9).fill(0)] })).toBe('false')
})
test.each([
  ['S1', 'messagesPerSecond', 96.9],
  ['S1', 'deliveryGapP99', 105.1],
  ['S1', 'cpuMicrosPerMessage', 103.1],
  ['S1', 'gapSamples', 999],
  ['L1', 'messagesPerSecond', 96.9],
  ['L1', 'deliveryGapP99', 105.1],
  ['S2', 'bytesPerSecond', 100.1],
  ['L2', 'bytesPerMessage', 10.1],
  ['L2', 'cpuMicrosPerMessage', 75.1],
  ['L2', 'heapRetainedBytes', 13145729],
  ['L3', 'bytesPerSecond', 10.1],
])('a failed %s %s gate keeps the default false', (arm, metric, value) => {
  const evidence = gateEvidence()
  evidence.arms[arm].on[metric] = value
  expect(selectDefault(evidence)).toBe('false')
})

test('a thrown socket write invalidates the observation even if an attempted frame was counted', () => {
  const observation = new FetchObservation()
  const socket = new EventEmitter()
  const error = new Error('write failed')
  socket.write = () => {
    throw error
  }
  const wrapped = observation.socketFactory(
    () => socket,
    () => 0
  )({})
  const request = Buffer.alloc(14)
  request.writeInt32BE(10)
  request.writeInt16BE(1, 4)
  expect(() => wrapped.write(request)).toThrow(error)
  expect(() => observation.result()).toThrow(error)
  wrapped.emit('close')
})

test('frames before the start or after the end are outside the measured window', () => {
  let clock = 5
  const m = new SessionMeasurement({ now: () => clock, cpu: () => ({ user: 0, system: 0 }) })
  m.frame({ apiKey: 1, at: 6, bytes: 1, direction: 'request' })
  m.begin()
  m.frame({ apiKey: 1, at: 4.9, bytes: 10, direction: 'request' })
  m.frame({ apiKey: 1, at: 5, bytes: 100, direction: 'request' })
  expect(() => m.begin()).toThrow('already started')
  clock = 6
  expect(m.finish()).toMatchObject({ bytes: 100, requests: 1, responses: 0 })
  expect(() => m.finish()).toThrow('Invalid measurement interval')
})

test('the audited window uses the same half-open completion-time selection', () => {
  const frames = [
    { at: 9.9, bytes: 1000, direction: 'response' },
    { at: 10, bytes: 100, direction: 'request' },
    { at: 19.9, bytes: 20, direction: 'response' },
    { at: 20, bytes: 5000, direction: 'request' },
  ]
  expect(windowTotals(frames, 10, 20)).toEqual({ bytes: 120, requests: 1, responses: 1 })
})

test('a journal-backed observation audits from the file and retains no raw chunk array', () => {
  const file = path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'journal-')), 'frames.jsonl')
  const journal = new FrameJournal(file)
  try {
    expect(() => new FrameJournal(file)).toThrow('EEXIST')
    const observation = new FetchObservation({ journal })
    const wrapped = observation.socketFactory(
      () => {
        const socket = new EventEmitter()
        socket.write = () => true
        return socket
      },
      () => 2
    )({})
    wrapped.write(header(5))
    wrapped.emit('data', reply(5))
    expect(observation.transcript).toEqual([])
    expect(observation.result()).toMatchObject({ bytes: 22, requests: 1, responses: 1 })
    const [entry] = journal.read()
    expect(entry).toMatchObject({ connection: '1', nodeId: 2, direction: 'request' })
    expect(Buffer.isBuffer(entry.chunk) && entry.chunk.equals(header(5))).toBe(true)
    expect(typeof entry.at).toBe('number')
  } finally {
    journal.close()
    journal.close()
  }
})

test('a zero-length interval is refused', () => {
  const m = new SessionMeasurement({ now: () => 1, cpu: () => ({ user: 0, system: 0 }) })
  m.begin()
  expect(() => m.finish()).toThrow('Empty measurement interval')
})

test('a failing arm propagates its error and still disconnects the consumer', async () => {
  const failure = new Error('run failed')
  const consumer = {
    connect: async () => {},
    subscribe: async () => {},
    run: async () => {
      throw failure
    },
    disconnect: jest.fn(async () => {}),
  }
  const measurement = new SessionMeasurement()
  await expect(
    runArm({ consumer: () => consumer }, { arm: 'S1', topic: 't', sessions: false, measurement })
  ).rejects.toBe(failure)
  expect(consumer.disconnect).toHaveBeenCalledTimes(1)
  await expect(runArm({}, { arm: 'S9', topic: 't', sessions: false, measurement })).rejects.toThrow(
    'Invalid session measurement arm'
  )
  await expect(
    runArm({}, { arm: 'S1', topic: 't', sessions: 'true', measurement })
  ).rejects.toThrow('Invalid session measurement arm')
})

test('idle arms refuse records and an L2 arm refuses a window without records', async () => {
  const run = (arm, sizes) => {
    let clock = 0
    const measurement = new SessionMeasurement({
      now: () => clock,
      cpu: () => ({ user: 0, system: 0 }),
    })
    const consumer = {
      connect: async () => {},
      subscribe: async () => {},
      run: async (options) => {
        consumer.deliver = async () => {
          for (const size of sizes) await options.eachBatch({ batch: { messages: Array(size) } })
        }
      },
      stop: async () => {},
      disconnect: async () => {},
    }
    const wait = async (ms) => {
      clock += ms
      if (clock === 70000) await consumer.deliver()
    }
    const onWindowStart = jest.fn()
    return runArm(
      { consumer: () => consumer },
      {
        arm,
        sessions: false,
        topic: 't',
        measurement,
        wait,
        onWindowStart,
        readRetained: async () => 1,
      }
    )
  }
  await expect(run('S2', [])).resolves.toMatchObject({ deliveredMessages: 0 })
  await expect(run('L3', [3])).rejects.toThrow('Idle arm received records')
  await expect(run('L2', [])).rejects.toThrow('received no records')
  await expect(run('L2', [4, 4])).resolves.toMatchObject({ deliveredMessages: 8, gapSamples: 1 })
})

test('a live arm without a node inventory and owned directory is refused before any client exists', async () => {
  const saved = process.env.BENCH_NODES
  delete process.env.BENCH_NODES
  try {
    await expect(main(['--arm', 'S1', '--sessions', 'true', '--topic', 't'])).rejects.toThrow(
      'Missing benchmark node inventory'
    )
  } finally {
    if (saved !== undefined) process.env.BENCH_NODES = saved
  }
})

test('a measurement that cannot be frozen rejects the active arm instead of waiting for the deadline', async () => {
  const measurement = new SessionMeasurement({ now: () => 1, cpu: () => ({ user: 0, system: 0 }) })
  const consumer = {
    connect: async () => {},
    subscribe: async () => {},
    run: async (options) => options.eachBatch({ batch: { messages: Array(200001) } }),
    stop: async () => {},
    disconnect: jest.fn(async () => {}),
  }
  await expect(
    runArm({ consumer: () => consumer }, { arm: 'S1', topic: 't', sessions: true, measurement })
  ).rejects.toThrow('Empty measurement interval')
  expect(consumer.disconnect).toHaveBeenCalledTimes(1)
})
