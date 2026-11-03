const prototypeFixture = require('./fixtures/prototypeCase')
const { PrototypeCase } = prototypeFixture
const { withDeadline } = require('./fixtures/boundedCleanup')

/** @returns {object} a broker-free client with observable ownership and events. */
const client = () => ({
  connect: jest.fn().mockResolvedValue(),
  disconnect: jest.fn().mockResolvedValue(),
  events: { REQUEST_TIMEOUT: 'timeout', CRASH: 'crash', GROUP_JOIN: 'join' },
  on: jest.fn(),
})

/** @returns {{promise: Promise<unknown>, resolve: Function}} a controlled async boundary. */
const deferred = () => {
  let resolve
  const promise = new Promise((done) => (resolve = done))
  return { promise, resolve }
}

describe('Consumer > prototype case containment', () => {
  test('retirement cancels waits and clears their timers', async () => {
    jest.useFakeTimers()
    try {
      const run = new PrototypeCase('constructor', 'fake')
      const outcome = run.until(() => false, 'delivery').catch((error) => error)
      await jest.advanceTimersByTimeAsync(0)
      expect(jest.getTimerCount()).toBe(1)
      run.retire()
      expect((await outcome).message).toContain('retired')
      expect(jest.getTimerCount()).toBe(0)
      await run.close()
    } finally {
      jest.useRealTimers()
    }
  })

  test('a failed connect retains every allocation and names the incomplete stage', async () => {
    const run = new PrototypeCase('constructor', 'fake', { identity: 'failing row' })
    const first = client()
    const second = client()
    second.connect.mockRejectedValue(new Error('connect failed'))
    await expect(
      run.execute(async () => {
        await run.connected(first, 'producer')
        await run.connected(second, 'consumer')
      })
    ).rejects.toThrow('connect failed')
    await run.close()
    expect(first.disconnect).toHaveBeenCalledTimes(1)
    expect(second.disconnect).toHaveBeenCalledTimes(1)
    expect(JSON.parse(run.report()).incompleteStage).toBe('consumer connect')
    expect(run.report()).toContain('failing row')
    expect(run.failure.message).toBe('connect failed')
  })

  test('event wiring failures retain the client for cleanup', async () => {
    const run = new PrototypeCase('constructor', 'fake')
    const owned = client()
    owned.on.mockImplementation(() => {
      expect(run.clients).toContain(owned)
      throw new Error('wiring failed')
    })
    await expect(run.connected(owned, 'consumer')).rejects.toThrow('wiring failed')
    await run.close()
    expect(owned.connect).not.toHaveBeenCalled()
    expect(owned.disconnect).toHaveBeenCalledTimes(1)
    expect(JSON.parse(run.report()).incompleteStage).toBe('consumer connect')
  })

  test('teardown waits for pending work before disconnecting and prevents late allocation', async () => {
    const run = new PrototypeCase('constructor', 'fake')
    const owned = client()
    let finish
    owned.connect.mockImplementation(() => new Promise((resolve) => (finish = resolve)))
    const connecting = run.connected(owned, 'producer').catch((error) => error)
    await Promise.resolve()
    const closing = run.close()
    expect(owned.disconnect).not.toHaveBeenCalled()
    finish()
    await closing
    expect((await connecting).message).toContain('retired')
    expect(owned.disconnect).toHaveBeenCalledTimes(1)
    await expect(run.connected(client())).rejects.toThrow('retired')
    expect(run.clients).toHaveLength(1)
  })

  test('teardown waits until deletion is observed and disconnects the admin last', async () => {
    const run = new PrototypeCase('constructor', 'docker')
    const admin = client()
    const producer = client()
    const deleting = deferred()
    const deleted = deferred()
    const observing = deferred()
    const absent = deferred()
    admin.deleteTopics = jest.fn(() => {
      deleting.resolve()
      return deleted.promise
    })
    admin.listTopics = jest
      .fn()
      .mockResolvedValueOnce(['constructor'])
      .mockImplementation(() => {
        observing.resolve()
        return absent.promise
      })
    await run.connected(admin, 'admin')
    run.admin = admin
    await run.connected(producer, 'producer')
    const closing = run.close()
    await deleting.promise
    expect(producer.disconnect).toHaveBeenCalledTimes(1)
    expect(admin.disconnect).not.toHaveBeenCalled()
    expect(admin.deleteTopics).toHaveBeenCalledWith({ topics: ['constructor'] })
    deleted.resolve()
    await observing.promise
    expect(admin.disconnect).not.toHaveBeenCalled()
    absent.resolve([])
    await closing
    expect(admin.disconnect).toHaveBeenCalledTimes(1)
  })

  test('a deletion failure still disconnects the owned admin and fails teardown', async () => {
    const run = new PrototypeCase('constructor', 'docker')
    const admin = client()
    admin.listTopics = jest.fn().mockResolvedValue(['constructor'])
    admin.deleteTopics = jest.fn().mockRejectedValue(new Error('deletion failed'))
    await run.connected(admin, 'admin')
    run.admin = admin
    await expect(run.close()).rejects.toThrow('deletion failed')
    expect(admin.disconnect).toHaveBeenCalledTimes(1)
    expect(JSON.parse(run.report()).incompleteStage).toBe('teardown deleteTopics')
  })

  test('disconnect failures fail cleanup after all clients are attempted', async () => {
    const run = new PrototypeCase('constructor', 'fake')
    const first = client()
    const second = client()
    second.disconnect.mockRejectedValue(new Error('disconnect failed'))
    await run.connected(first, 'producer')
    await run.connected(second, 'consumer')
    await expect(run.close()).rejects.toThrow('disconnect failed')
    expect(first.disconnect).toHaveBeenCalledTimes(1)
    expect(JSON.parse(run.report()).incompleteStage).toBe('consumer disconnect')
  })

  test('a retired operation cannot continue into the next setup stage', async () => {
    const run = new PrototypeCase('constructor', 'fake')
    let finish
    const next = jest.fn()
    const body = run
      .execute(async () => {
        await run.stage('admin connect', () => new Promise((resolve) => (finish = resolve)))
        await run.stage('createTopics/leaders', next)
      })
      .catch((error) => error)
    await Promise.resolve()
    run.retire()
    finish()
    await body
    await run.close()
    expect(next).not.toHaveBeenCalled()
    expect(JSON.parse(run.report()).incompleteStage).toBe('admin connect')
  })

  test('pending setup has a failing cleanup deadline and still attempts disconnect', async () => {
    jest.useFakeTimers()
    try {
      const run = new PrototypeCase('constructor', 'fake')
      const owned = client()
      let finish
      owned.connect.mockImplementation(() => new Promise((resolve) => (finish = resolve)))
      const body = run.connected(owned, 'producer').catch((error) => error)
      await Promise.resolve()
      const closing = run.close().catch((error) => error)
      await jest.advanceTimersByTimeAsync(15000)
      expect(await closing).toBeInstanceOf(AggregateError)
      expect(owned.disconnect).toHaveBeenCalledTimes(1)
      expect(run.report()).toContain('producer connect')
      finish()
      await body
      expect(jest.getTimerCount()).toBe(0)
    } finally {
      jest.useRealTimers()
    }
  })

  test('captures timeout, crash and join events without printing on success', async () => {
    const output = jest.spyOn(console, 'error').mockImplementation(() => {})
    try {
      const run = new PrototypeCase('constructor', 'fake')
      const owned = client()
      await run.execute(() => run.connected(owned, 'consumer'))
      for (const [type, listener] of owned.on.mock.calls) {
        listener({ timestamp: 123, payload: { detail: type } })
      }
      await run.close()
      expect(run.report()).toContain('timeout')
      expect(run.report()).toContain('crash')
      expect(run.report()).toContain('join')
      expect(run.bodyDone).toBe(true)
      expect(run.failure).toBeNull()
      expect(output).not.toHaveBeenCalled()
    } finally {
      output.mockRestore()
    }
  })
})

/**
 * Captures the real topic spec's registrar and hooks without registering nested Jest tests.
 * Only the producer row is executed, using an owned broker-free client. The hook's overall
 * deadline is scaled to 40 ms; the fixture's per-operation deadlines stay unchanged.
 * @param {object} producer controlled client with a send method
 * @returns {{setup: Function, cleanup: Function, row: Function, runs: PrototypeCase[]}}
 * captured callbacks and allocated cases
 */
const topicHooks = (producer) => {
  const captured = { runs: [] }
  const register = (title, body) => {
    if (title === 'a producer sends to it, and the metadata it returns is ordinary') {
      captured.row = body
    }
  }
  register.each = () => () => {}
  register.skip = () => {}
  const originals = {
    test: global.test,
    beforeEach: global.beforeEach,
    afterEach: global.afterEach,
  }
  const descriptions = jest
    .spyOn(describe, 'each')
    .mockImplementation((rows) => (_, body) => body(rows[0]))
  try {
    global.test = register
    global.beforeEach = (hook) => (captured.setup = hook)
    global.afterEach = (hook) => (captured.cleanup = hook)
    jest.doMock('./fixtures/prototypeCase', () => ({
      ...prototypeFixture,
      PrototypeCase: jest.fn((...args) => {
        const run = new PrototypeCase(...args)
        jest.spyOn(run, 'kafka').mockResolvedValue({ producer: () => producer })
        jest.spyOn(run, 'execute')
        jest.spyOn(run, 'close')
        captured.runs.push(run)
        return run
      }),
    }))
    jest.doMock('./fixtures/boundedCleanup', () => ({
      withDeadline: (promise, ms) => {
        expect(ms).toBe(75000)
        return withDeadline(promise, 40)
      },
    }))
    jest.doMock('../../../testHelpers/lane', () => ({ lanes: () => ['fake'] }))
    jest.isolateModules(() => require('./prototypeTopics.spec'))
  } finally {
    Object.assign(global, originals)
    descriptions.mockRestore()
    jest.dontMock('./fixtures/prototypeCase')
    jest.dontMock('./fixtures/boundedCleanup')
    jest.dontMock('../../../testHelpers/lane')
  }
  return captured
}

describe('Consumer > prototype topic reporting hooks', () => {
  beforeEach(() => jest.useFakeTimers())
  afterEach(() => jest.useRealTimers())

  test('a passing registered row and cleanup print nothing', async () => {
    const output = jest.spyOn(console, 'error').mockImplementation(() => {})
    try {
      const producer = client()
      producer.send = jest.fn().mockResolvedValue([{ topicName: '__proto__', partition: 0 }])
      const hooks = topicHooks(producer)
      hooks.setup()
      await hooks.row()
      await hooks.cleanup()
      expect(hooks.runs[0].execute).toHaveBeenCalledTimes(1)
      expect(hooks.runs[0].bodyDone).toBe(true)
      expect(producer.disconnect).toHaveBeenCalledTimes(1)
      expect(output).not.toHaveBeenCalled()
      hooks.setup()
      expect(hooks.runs).toHaveLength(2)
      await hooks.row()
      await hooks.cleanup()
      expect(output).not.toHaveBeenCalled()
      expect(jest.getTimerCount()).toBe(0)
    } finally {
      output.mockRestore()
    }
  })

  test('a missed cleanup deadline reports once and fences rows even after late completion', async () => {
    const output = jest.spyOn(console, 'error').mockImplementation(() => {})
    const disconnected = deferred()
    let run
    let hook
    try {
      const producer = client()
      producer.send = jest.fn().mockResolvedValue([{ topicName: '__proto__', partition: 0 }])
      producer.disconnect.mockReturnValue(disconnected.promise)
      const hooks = topicHooks(producer)
      hooks.setup()
      await hooks.row()
      run = hooks.runs[0]
      for (const [type, listener] of producer.on.mock.calls) {
        listener({ timestamp: 123, payload: { detail: type } })
      }
      hook = hooks.cleanup()
      const outcome = withDeadline(hook, 80).catch((error) => error)
      await jest.advanceTimersByTimeAsync(40)
      expect(output).toHaveBeenCalledTimes(1)
      expect(output).toHaveBeenCalledWith(run.report())
      const report = JSON.parse(output.mock.calls[0][0])
      expect(report.incompleteStage).toBe('producer disconnect')
      expect(report.stages).toContainEqual({
        label: 'producer disconnect',
        started: expect.any(String),
      })
      expect(
        report.events.map(({ type, timestamp, payload }) => ({ type, timestamp, payload }))
      ).toEqual(
        ['timeout', 'crash', 'join'].map((type) => ({
          type,
          timestamp: 123,
          payload: { detail: type },
        }))
      )
      const failure = await outcome
      expect(failure.message).toContain('Previous case cleanup failed:')
      expect(failure.cause.message).toBe('Disconnect exceeded 40 ms')
      expect(() => hooks.setup()).toThrow(failure)
      await hooks.cleanup()
      disconnected.resolve()
      await run.close.mock.results[0].value
      await jest.advanceTimersByTimeAsync(80)
      expect(() => hooks.setup()).toThrow(failure)
      await hooks.cleanup()
      expect(hooks.runs).toEqual([run])
      expect(run.execute).toHaveBeenCalledTimes(1)
      expect(output).toHaveBeenCalledTimes(1)
      expect(jest.getTimerCount()).toBe(0)
    } finally {
      disconnected.resolve()
      if (run?.close.mock.results.length) await run.close.mock.results[0].value
      if (hook) await hook.catch(() => {})
      output.mockRestore()
    }
  })
})
