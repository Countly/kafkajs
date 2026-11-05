const { getEventListeners } = require('events')
const mockClients = []
const mockActions = []
let mockConnectFailure
const mockWrite = jest.fn()
const mockExec = jest.fn()
const mockAccess = jest.fn()

jest.mock('fs', () => ({
  ...jest.requireActual('fs'),
  accessSync: (...args) => mockAccess(...args),
}))
jest.mock('child_process', () => ({ execFile: (...args) => mockExec(...args) }))
jest.mock('../../../index', () => ({
  Kafka: jest.fn().mockImplementation(() => ({
    admin: () => {
      const emitter = new (require('events').EventEmitter)()
      const client = {
        events: { REQUEST: 'request' },
        emitter,
        on: (event, listener) => {
          emitter.on(event, listener)
          return () => emitter.off(event, listener)
        },
        connect: jest.fn(async () => {
          mockActions.push('connect')
          if (mockConnectFailure) throw mockConnectFailure
        }),
        disconnect: jest.fn(async () => mockActions.push('disconnect')),
      }
      mockClients.push(client)
      return client
    },
  })),
}))
jest.mock('testHelpers', () => ({
  plainTextBrokers: () => ['fake:1'],
  writeEvidence: (...args) => mockWrite(...args),
  wireVersionsOf: () => ({}),
  disconnectPropagationBrokers: async () => {},
}))

const { Kafka } = require('../../../index')
const { AdminLiveProbe } = require('./fixtures/liveAdmin')
const { liveRun } = require('../../producer/__tests__/fixtures/bounded')
const env = { guard: process.env.P9_CLUSTER_GUARD, manifest: process.env.P9_CLUSTER_MANIFEST }

/** @returns {{probe: AdminLiveProbe, owner: AbortController}} isolated guarded scenario */
const scenario = () => {
  const owner = new AbortController()
  return {
    owner,
    probe: new AdminLiveProbe({ stem: 'admin-ledger', signal: owner.signal, budgetMs: 120000 }),
  }
}

beforeEach(() => {
  liveRun.reset()
  jest.clearAllMocks()
  mockClients.length = 0
  mockActions.length = 0
  mockConnectFailure = undefined
  process.env.P9_CLUSTER_GUARD = 'fake-guard'
  process.env.P9_CLUSTER_MANIFEST = 'fake-manifest'
  mockAccess.mockImplementation(() => {})
  mockExec.mockImplementation((file, argv, options, callback) => {
    const stdin = { end: jest.fn(() => callback(null, 'guard output')) }
    return { stdin }
  })
})

afterEach(() => {
  liveRun.reset()
  for (const [name, value] of [
    ['P9_CLUSTER_GUARD', env.guard],
    ['P9_CLUSTER_MANIFEST', env.manifest],
  ]) {
    if (value === undefined) delete process.env[name]
    else process.env[name] = value
  }
})

test.each(['unreadable', 'abort', 'budget'])(
  'refuses %s before client or guard side effects',
  (reason) => {
    const owner = new AbortController()
    if (reason === 'unreadable')
      mockAccess.mockImplementation(() => {
        throw new Error('unreadable')
      })
    if (reason === 'abort') owner.abort(new Error('cancelled'))
    expect(
      () =>
        new AdminLiveProbe({
          stem: 'admin-ledger',
          signal: owner.signal,
          budgetMs: reason === 'budget' ? 0 : 120000,
        })
    ).toThrow()
    expect(Kafka).not.toHaveBeenCalled()
    expect(mockExec).not.toHaveBeenCalled()
  }
)

test.each(['P9_CLUSTER_GUARD', 'P9_CLUSTER_MANIFEST'])(
  'refuses %s alone as an incomplete ownership environment',
  (name) => {
    delete process.env[name]
    expect(
      () =>
        new AdminLiveProbe({
          stem: 'admin-ledger',
          signal: new AbortController().signal,
          budgetMs: 120000,
        })
    ).toThrow('Incomplete cluster ownership environment')
    expect(Kafka).not.toHaveBeenCalled()
    expect(mockExec).not.toHaveBeenCalled()
  }
)

test('tainted run refuses lazy work and final success', async () => {
  const { probe } = scenario()
  liveRun.taint(new Error('escaped'))
  const work = jest.fn()
  expect(() => probe.invoke(work)).toThrow('earlier case')
  expect(work).not.toHaveBeenCalled()
  await expect(probe.finish(async () => {})).rejects.toThrow('earlier case')
})

test('reserve refuses a mutation before invoking its thunk', () => {
  const { probe } = scenario()
  probe.due = performance.now() + probe.reserve - 1
  const work = jest.fn()
  expect(() => probe.invoke(work)).toThrow('reserve')
  expect(work).not.toHaveBeenCalled()
})

test('timeout taints and cancels a guarded command before another can start', async () => {
  const { probe } = scenario()
  probe.due = performance.now() + probe.reserve + 30
  let commandSignal
  mockExec.mockImplementation((file, argv, options, callback) => {
    commandSignal = options.signal
    commandSignal.addEventListener('abort', () => callback(commandSignal.reason), { once: true })
    return { stdin: { end: jest.fn() } }
  })
  await expect(probe.host('kafka1', ['literal', 'a b'])).rejects.toThrow()
  expect(commandSignal.aborted).toBe(true)
  expect(liveRun.tainted).toBe(true)
  expect(() => probe.host('kafka1', ['again'])).toThrow('earlier case')
  expect(mockExec).toHaveBeenCalledTimes(1)
})

test('guard command keeps literal operands and manifest with bounded output', async () => {
  const { probe } = scenario()
  expect(await probe.host('kafka1', ['literal', 'a b'], 'input')).toBe('guard output')
  const [file, argv, options] = mockExec.mock.calls[0]
  expect(file).toBe('timeout')
  expect(argv.slice(2)).toEqual([
    process.execPath,
    'fake-guard',
    'exec',
    'fake-manifest',
    'kafka1',
    'literal',
    'a b',
  ])
  expect(options.maxBuffer).toBe(4 * 1048576)
  expect(options.signal).toBeInstanceOf(AbortSignal)
})

describe('standalone cluster without a guard', () => {
  beforeEach(() => {
    delete process.env.P9_CLUSTER_GUARD
    delete process.env.P9_CLUSTER_MANIFEST
  })

  test('exec runs the selected Compose service with literal operands and no guard files', async () => {
    const { probe } = scenario()
    expect(await probe.host('kafka1', ['literal', 'a b'], 'input')).toBe('guard output')
    const [file, argv, options] = mockExec.mock.calls[0]
    expect(file).toBe('timeout')
    expect(argv[0]).toBe('--kill-after=5s')
    expect(argv.slice(2, 4)).toEqual(['docker', 'compose'])
    expect(argv[4]).toBe('-f')
    expect(argv[5]).toMatch(/docker-compose[^/]*\.yml$/)
    expect(argv.slice(6)).toEqual(['exec', '-T', 'kafka1', 'literal', 'a b'])
    expect(options.maxBuffer).toBe(4 * 1048576)
    expect(mockAccess).not.toHaveBeenCalled()
  })

  test('control stops a broker through Compose and restores it with start', async () => {
    const { probe } = scenario()
    await probe.finish(async () => {
      await probe.control('stop', 'kafka3')
    })
    const operands = mockExec.mock.calls.map(([, argv]) => argv.slice(6))
    expect(operands).toEqual([
      ['stop', 'kafka3'],
      ['start', 'kafka3'],
    ])
  })
})

describe('raw receipts', () => {
  const os = require('os')
  const fs = require('fs')
  const path = require('path')

  test('append one JSON line under the guarded run directory', () => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'receipt-'))
    const previous = process.env.LIVE_WORK
    process.env.LIVE_WORK = directory
    try {
      const { probe } = scenario()
      probe.receipt('raw.jsonl', { count: 1 })
      probe.receipt('raw.jsonl', { count: 2 })
      expect(fs.readFileSync(path.join(directory, 'raw.jsonl'), 'utf8')).toBe(
        '{"count":1}\n{"count":2}\n'
      )
    } finally {
      if (previous === undefined) delete process.env.LIVE_WORK
      else process.env.LIVE_WORK = previous
      fs.rmSync(directory, { recursive: true, force: true })
    }
  })

  test('default to .tmp/e2e under the working directory, created on demand', () => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'receipt-'))
    const previous = process.env.LIVE_WORK
    const cwd = process.cwd()
    delete process.env.LIVE_WORK
    try {
      const { probe } = scenario()
      process.chdir(directory)
      probe.receipt('raw.jsonl', { count: 3 })
      expect(fs.readFileSync(path.join(directory, '.tmp', 'e2e', 'raw.jsonl'), 'utf8')).toBe(
        '{"count":3}\n'
      )
    } finally {
      process.chdir(cwd)
      if (previous !== undefined) process.env.LIVE_WORK = previous
      fs.rmSync(directory, { recursive: true, force: true })
    }
  })
})

test('failed connection still closes its owned client and removes its request listener', async () => {
  const { probe } = scenario()
  await probe.finish(async () => {
    mockConnectFailure = new Error('connect failed')
    await expect(probe.start()).rejects.toBe(mockConnectFailure)
    expect(mockClients[0].emitter.listenerCount('request')).toBe(1)
  })
  expect(mockActions).toEqual(['connect', 'disconnect'])
  expect(mockClients[0].emitter.listenerCount('request')).toBe(0)
  mockClients[0].emitter.emit('request', { payload: 'late' })
  expect(probe.requests).toEqual([])
})

test('owner cancellation preserves primary failure while effects precede client cleanup', async () => {
  const { probe, owner } = scenario()
  const primary = new Error('primary')
  const credential = Buffer.from('owned-secret')
  probe.credentials.push(credential)
  const before = getEventListeners(owner.signal, 'abort').length
  await expect(
    probe.finish(async () => {
      await probe.start()
      probe.effects.add('effect', async () => {
        mockActions.push('effect')
        await probe.host('kafka1', ['restore'])
      })
      owner.abort(primary)
      throw primary
    })
  ).rejects.toBe(primary)
  expect(mockActions).toEqual(['connect', 'effect', 'disconnect'])
  expect(primary.cleanupErrors).toEqual([])
  expect(mockExec).toHaveBeenCalledTimes(1)
  expect(credential.every((byte) => byte === 0)).toBe(true)
  expect(getEventListeners(owner.signal, 'abort')).toHaveLength(before)
})

test('cleanup budget cancels stuck effect and still attempts later cleanup', async () => {
  const { probe } = scenario()
  let stopped = false
  const began = performance.now()
  await expect(
    probe.finish(async () => {
      probe.due = performance.now() + 120
      probe.cleanup.add('later', async () => mockActions.push('later'))
      probe.effects.add(
        'stuck',
        (signal) =>
          new Promise((resolve) =>
            signal.addEventListener(
              'abort',
              () => {
                stopped = true
                resolve()
              },
              { once: true }
            )
          )
      )
    })
  ).rejects.toThrow('cleanup')
  expect(stopped).toBe(true)
  expect(performance.now() - began).toBeLessThan(350)
  expect(mockActions).toEqual(['later'])
  expect(liveRun.tainted).toBe(true)
})

test('cleanup failure retains primary error and attempts every restoration', async () => {
  const { probe } = scenario()
  const primary = new Error('work')
  await expect(
    probe.finish(async () => {
      probe.effects.add('first', async () => mockActions.push('first'))
      probe.effects.add('second', async () => {
        throw new Error('cleanup')
      })
      throw primary
    })
  ).rejects.toBe(primary)
  expect(primary.cleanupErrors).toHaveLength(1)
  expect(mockActions).toEqual(['first'])
})

test('listener removal failure cannot abandon remaining listeners or credential wiping', async () => {
  const { probe } = scenario()
  const secret = Buffer.from('owned')
  probe.credentials.push(secret)
  probe.removers.push(
    () => {
      throw new Error('remove')
    },
    () => mockActions.push('removed')
  )
  await expect(probe.finish(async () => {})).rejects.toThrow('cleanup')
  expect(mockActions).toEqual(['removed'])
  expect(secret.every((byte) => byte === 0)).toBe(true)
})

test.each([
  { password: 'secret' },
  { tokenId: Buffer.from('secret') },
  { count: new Error('secret') },
  { rawLog: 'secret' },
  { passwordDigest: 'secret' },
  { digest: { nested: 'secret' } },
])('rejects secret-shaped evidence without writing it (%#)', (facts) => {
  const { probe } = scenario()
  expect(() => probe.record('unsafe', facts)).toThrow('evidence')
  expect(mockWrite).not.toHaveBeenCalled()
})

test('records only nonsecret scalar facts after item checks', () => {
  const { probe } = scenario()
  expect(() => probe.items([{ errorCode: 0 }, { errorCode: 42 }])).toThrow('item failed')
  probe.record('observed', {
    count: 1,
    partitionIds: [0, 1],
    corpusDigest: 'abc',
    secretAbsent: true,
  })
  expect(mockWrite).toHaveBeenCalledWith(
    'admin-ledger',
    expect.objectContaining({ case: 'observed', count: 1 })
  )
})

test('refuses final success when a tracked socket remains open', async () => {
  const { probe } = scenario()
  await expect(
    probe.finish(async () => {
      probe.sockets.push({ destroyed: false })
    })
  ).rejects.toThrow('cleanup')
  expect(liveRun.tainted).toBe(true)
})

test('refuses credential markers disguised as ordinary evidence', () => {
  const { probe } = scenario()
  const credential = Buffer.from('private-marker-value')
  probe.credentials.push(credential)
  for (const value of [
    credential.toString(),
    credential.toString('hex'),
    credential.toString('base64'),
  ]) {
    expect(() => probe.record('unsafe', { source: value })).toThrow('evidence')
  }
  expect(mockWrite).not.toHaveBeenCalled()
})

test('cleanup cancellation reaches a running guard child before the inclusive budget', async () => {
  const { probe } = scenario()
  let childSignal
  let cancelledAt
  const began = performance.now()
  mockExec.mockImplementation((file, argv, options, callback) => {
    childSignal = options.signal
    childSignal.addEventListener(
      'abort',
      () => {
        cancelledAt = performance.now()
        callback(childSignal.reason)
      },
      { once: true }
    )
    return { stdin: { end: jest.fn() } }
  })
  await expect(
    probe.finish(async () => {
      probe.due = performance.now() + 1200
      probe.effects.add('guard effect', () => probe.host('kafka1', ['restore']))
      probe.cleanup.add('after guard', async () => {
        expect(childSignal.aborted).toBe(true)
        mockActions.push('after')
      })
    })
  ).rejects.toThrow('cleanup')
  expect(mockActions).toEqual(['after'])
  expect(cancelledAt - began).toBeLessThan(1000)
})

test.each([null, 0, false])('retains a falsy primary failure (%#)', async (primary) => {
  const { probe } = scenario()
  await expect(
    probe.finish(async () => {
      throw primary
    })
  ).rejects.toBe(primary)
})

test.each([
  ['admin-lifecycle', 1000],
  ['admin-ledger', 60000],
  ['tiered-offsets', 60000],
  ['admin-tokens', 30000],
])('refuses an impossible %s allocation before side effects', (stem, reserve) => {
  const owner = new AbortController()
  for (const budgetMs of [reserve - 1, reserve]) {
    expect(() => new AdminLiveProbe({ stem, signal: owner.signal, budgetMs })).toThrow(
      'Scenario budget must exceed cleanup reserve'
    )
  }
  expect(Kafka).not.toHaveBeenCalled()
  expect(mockExec).not.toHaveBeenCalled()
  const probe = new AdminLiveProbe({ stem, signal: owner.signal, budgetMs: reserve + 1 })
  expect(probe.reserve).toBe(reserve)
})

test('inclusive deadline refuses success even when work leaves no cleanup entries', async () => {
  const { probe } = scenario()
  await expect(
    probe.finish(async () => {
      probe.due = performance.now() - 1
    })
  ).rejects.toThrow('cleanup')
  expect(liveRun.tainted).toBe(true)
})

test('refuses a retained proxy listener resource', async () => {
  const { probe } = scenario()
  probe.resourceProbe = {
    snapshot: jest
      .fn()
      .mockResolvedValueOnce({ sockets: { open: 0 }, activeResources: {} })
      .mockResolvedValueOnce({ sockets: { open: 0 }, activeResources: { TCPServerWrap: 1 } }),
  }
  await expect(probe.finish(async () => {})).rejects.toThrow('cleanup')
  expect(liveRun.tainted).toBe(true)
})

test('refuses retained owner signal listeners', async () => {
  const { probe, owner } = scenario()
  const listener = () => {}
  try {
    await expect(
      probe.finish(async () => {
        owner.signal.addEventListener('abort', listener)
      })
    ).rejects.toThrow('cleanup')
    expect(liveRun.tainted).toBe(true)
  } finally {
    owner.signal.removeEventListener('abort', listener)
  }
})
