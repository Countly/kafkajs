const fs = require('fs')
const os = require('os')
const path = require('path')
const { EventEmitter } = require('events')
const helpers = require('../index')
const { createResourceProbe } = require('../resourceProbe')
const { createFakeSocketFactory, socketCounts } = require('../fakeSocket')
const {
  SHARE_TOPIC_PREFIX,
  shareTopicName,
  createShareTopic,
  setShareGroupConfigs,
  resetShareGroupConfigs,
} = require('../shareFixtures')

// Literal wire values (DescribeConfigs ConfigSource, ConfigResource.Type, AlterConfigOp.OpType).
const GROUP_RESOURCE = 32
const TOPIC_RESOURCE = 2
const GROUP_CONFIG = 8
const DEFAULT_CONFIG = 5
const STATIC_BROKER_CONFIG = 4
const SET = 0

const GROUP = 'share-fixtures-group'
const EARLIEST = { 'share.auto.offset.reset': 'earliest' }

/** One DescribeConfigs entry. */
const entry = (configName, configValue, configSource) => ({ configName, configValue, configSource })

/** A describeConfigs answer holding one resource. */
const answer = (configEntries, resourceType = GROUP_RESOURCE, resourceName = GROUP) => ({
  resources: [{ errorCode: 0, errorMessage: null, resourceType, resourceName, configEntries }],
})

/**
 * An admin double: `incrementalAlterConfigs` answers `alterResult` for the one resource it is sent,
 * and the n-th `describeConfigs` answers `reads[n]`, the last one again once they run out.
 */
const fakeAdmin = (reads, alterResult = { errorCode: 0, errorMessage: null }) => {
  let read = 0
  return {
    incrementalAlterConfigs: jest.fn(async ({ resources }) => ({
      resources: [
        { ...alterResult, resourceType: GROUP_RESOURCE, resourceName: resources[0].name },
      ],
    })),
    describeConfigs: jest.fn(async () => reads[Math.min(read++, reads.length - 1)]),
  }
}

/**
 * Follows a promise without awaiting it, so a test can see whether it has settled: a call that
 * must have settled is checked before it is awaited, and never leaves the test waiting on a timer.
 */
const track = (promise) => {
  const state = { settled: false }
  promise.then(
    () => (state.settled = true),
    () => (state.settled = true)
  )
  return state
}

describe('share-group fixtures', () => {
  describe('setShareGroupConfigs', () => {
    beforeEach(() => jest.useFakeTimers())
    afterEach(() => jest.useRealTimers())

    test('sets exactly the given configs on the GROUP resource, as strings, in one request', async () => {
      const admin = fakeAdmin([
        answer([
          entry('share.auto.offset.reset', 'earliest', GROUP_CONFIG),
          entry('share.record.lock.duration.ms', '15000', GROUP_CONFIG),
          entry('share.renew.acknowledge.enable', 'false', GROUP_CONFIG),
        ]),
      ])

      const call = setShareGroupConfigs(admin, GROUP, {
        ...EARLIEST,
        'share.record.lock.duration.ms': 15000,
        'share.renew.acknowledge.enable': false,
      })
      const state = track(call)
      await jest.advanceTimersByTimeAsync(0)

      expect(admin.incrementalAlterConfigs).toHaveBeenCalledTimes(1)
      expect(admin.incrementalAlterConfigs).toHaveBeenCalledWith({
        resources: [
          {
            type: GROUP_RESOURCE,
            name: GROUP,
            configEntries: [
              { name: 'share.auto.offset.reset', operation: SET, value: 'earliest' },
              { name: 'share.record.lock.duration.ms', operation: SET, value: '15000' },
              { name: 'share.renew.acknowledge.enable', operation: SET, value: 'false' },
            ],
          },
        ],
      })
      expect(admin.describeConfigs).toHaveBeenCalledTimes(1)
      expect(admin.describeConfigs).toHaveBeenCalledWith({
        resources: [
          {
            type: GROUP_RESOURCE,
            name: GROUP,
            configNames: [
              'share.auto.offset.reset',
              'share.record.lock.duration.ms',
              'share.renew.acknowledge.enable',
            ],
          },
        ],
      })
      expect(state.settled).toBe(true)
      await expect(call).resolves.toBeUndefined()
    })

    test('keeps reading while the change has not applied, and resolves on the read that shows it', async () => {
      const before = answer([entry('share.auto.offset.reset', 'latest', GROUP_CONFIG)])
      const after = answer([entry('share.auto.offset.reset', 'earliest', GROUP_CONFIG)])
      const admin = fakeAdmin([before, before, after])

      const call = setShareGroupConfigs(admin, GROUP, EARLIEST, { timeout: 5000, interval: 250 })
      const state = track(call)

      await jest.advanceTimersByTimeAsync(0)
      expect(admin.describeConfigs).toHaveBeenCalledTimes(1)
      expect(state.settled).toBe(false)
      await jest.advanceTimersByTimeAsync(250)
      expect(admin.describeConfigs).toHaveBeenCalledTimes(2)
      expect(state.settled).toBe(false)
      await jest.advanceTimersByTimeAsync(250)

      expect(admin.describeConfigs).toHaveBeenCalledTimes(3)
      expect(state.settled).toBe(true)
      await expect(call).resolves.toBeUndefined()
    })

    test('does not resolve on a default equal to the wanted value, or on half the configs', async () => {
      const lock = { 'share.record.lock.duration.ms': '30000', 'share.session.timeout.ms': 6000 }
      const admin = fakeAdmin([
        answer([
          entry('share.record.lock.duration.ms', '30000', DEFAULT_CONFIG),
          entry('share.session.timeout.ms', '6000', GROUP_CONFIG),
        ]),
        answer([
          entry('share.record.lock.duration.ms', '30000', GROUP_CONFIG),
          entry('share.session.timeout.ms', '6000', STATIC_BROKER_CONFIG),
        ]),
        answer([entry('share.record.lock.duration.ms', '30000', GROUP_CONFIG)]),
        answer([entry('share.session.timeout.ms', '6000', GROUP_CONFIG)]),
        answer([entry('share.record.lock.duration.ms', '30000', GROUP_CONFIG)]),
        answer([
          entry('share.record.lock.duration.ms', '30000', GROUP_CONFIG),
          entry('share.session.timeout.ms', '6000', GROUP_CONFIG),
        ]),
      ])

      const call = setShareGroupConfigs(admin, GROUP, lock, { timeout: 5000, interval: 100 })
      const state = track(call)

      await jest.advanceTimersByTimeAsync(0)
      expect(admin.describeConfigs).toHaveBeenCalledTimes(1)
      expect(state.settled).toBe(false)
      await jest.advanceTimersByTimeAsync(100)
      expect(admin.describeConfigs).toHaveBeenCalledTimes(2)
      expect(state.settled).toBe(false)
      for (const read of [3, 4, 5]) {
        await jest.advanceTimersByTimeAsync(100)
        expect(admin.describeConfigs).toHaveBeenCalledTimes(read)
        expect(state.settled).toBe(false)
      }
      await jest.advanceTimersByTimeAsync(100)

      expect(admin.describeConfigs).toHaveBeenCalledTimes(6)
      expect(state.settled).toBe(true)
      await expect(call).resolves.toBeUndefined()
    })

    test('does not count values read back for another resource', async () => {
      const values = [entry('share.auto.offset.reset', 'earliest', GROUP_CONFIG)]
      const admin = fakeAdmin([
        answer(values, TOPIC_RESOURCE, GROUP),
        answer(values, GROUP_RESOURCE, `${GROUP}-other`),
      ])

      const call = setShareGroupConfigs(admin, GROUP, EARLIEST, { timeout: 1000, interval: 250 })
      const state = track(call)
      await jest.advanceTimersByTimeAsync(1000)

      expect(state.settled).toBe(true)
      await expect(call).rejects.toThrow(
        `Group ${GROUP} did not read back share.auto.offset.reset=earliest within 1000 ms`
      )
      expect(admin.describeConfigs).toHaveBeenCalledTimes(5)
    })

    test('fails at the deadline, naming every config not read back, and not before it', async () => {
      const admin = fakeAdmin([
        answer([
          entry('share.auto.offset.reset', 'latest', DEFAULT_CONFIG),
          entry('share.isolation.level', 'read_committed', GROUP_CONFIG),
          entry('share.record.lock.duration.ms', '30000', DEFAULT_CONFIG),
        ]),
      ])
      const kv = {
        ...EARLIEST,
        'share.isolation.level': 'read_committed',
        'share.record.lock.duration.ms': 3000,
      }

      const call = setShareGroupConfigs(admin, GROUP, kv, { timeout: 1000, interval: 250 })
      const state = track(call)

      await jest.advanceTimersByTimeAsync(999)
      expect(state.settled).toBe(false)
      await jest.advanceTimersByTimeAsync(1)

      expect(state.settled).toBe(true)
      await expect(call).rejects.toThrow(
        `Group ${GROUP} did not read back share.auto.offset.reset=earliest, share.record.lock.duration.ms=3000 within 1000 ms`
      )
      expect(admin.describeConfigs).toHaveBeenCalledTimes(5)
    })

    test('fails at once with the broker code and message when the change is refused', async () => {
      const admin = fakeAdmin([answer([])], {
        errorCode: 40,
        errorMessage:
          'share.record.lock.duration.ms must be greater than or equal to group.share.min.record.lock.duration.ms',
      })

      const call = setShareGroupConfigs(admin, GROUP, { 'share.record.lock.duration.ms': 3000 })
      const state = track(call)
      await jest.advanceTimersByTimeAsync(0)

      expect(state.settled).toBe(true)
      await expect(call).rejects.toThrow(
        `Group ${GROUP} refused share.record.lock.duration.ms: error 40 share.record.lock.duration.ms must be greater than or equal to group.share.min.record.lock.duration.ms`
      )
      expect(admin.describeConfigs).not.toHaveBeenCalled()
    })

    test('lets a describe error through at once instead of polling it into a timeout', async () => {
      const admin = fakeAdmin([answer([])])
      admin.describeConfigs.mockRejectedValue(new Error('connection lost'))

      const call = setShareGroupConfigs(admin, GROUP, EARLIEST, { timeout: 5000, interval: 250 })
      const state = track(call)
      await jest.advanceTimersByTimeAsync(0)

      expect(state.settled).toBe(true)
      await expect(call).rejects.toThrow('connection lost')
      expect(admin.describeConfigs).toHaveBeenCalledTimes(1)
    })

    test('refuses an empty set of configs before sending anything', async () => {
      const admin = fakeAdmin([answer([])])

      const call = setShareGroupConfigs(admin, GROUP, {})
      const state = track(call)
      await jest.advanceTimersByTimeAsync(0)

      expect(state.settled).toBe(true)
      await expect(call).rejects.toThrow(`No config to set on group ${GROUP}`)
      expect(admin.incrementalAlterConfigs).not.toHaveBeenCalled()
      expect(admin.describeConfigs).not.toHaveBeenCalled()
    })
  })

  describe('resetShareGroupConfigs', () => {
    beforeEach(() => jest.useFakeTimers())
    afterEach(() => jest.useRealTimers())

    test('deletes once and waits for stale group overrides to disappear', async () => {
      const api = fakeAdmin([
        answer([entry('share.auto.offset.reset', 'earliest', GROUP_CONFIG)]),
        answer([entry('share.auto.offset.reset', 'latest', DEFAULT_CONFIG)]),
      ])
      const pending = resetShareGroupConfigs(api, GROUP, Object.keys(EARLIEST))
      const state = track(pending)
      await jest.advanceTimersByTimeAsync(0)
      expect(state.settled).toBe(false)
      await jest.advanceTimersByTimeAsync(100)
      await expect(pending).resolves.toBeUndefined()
      expect(api.incrementalAlterConfigs).toHaveBeenCalledTimes(1)
      expect(api.incrementalAlterConfigs).toHaveBeenCalledWith({
        resources: [
          {
            type: GROUP_RESOURCE,
            name: GROUP,
            configEntries: [{ name: 'share.auto.offset.reset', operation: 1 }],
          },
        ],
      })
      expect(api.describeConfigs).toHaveBeenCalledTimes(2)
      expect(jest.getTimerCount()).toBe(0)
    })

    test('fails if an override never disappears without repeating the delete', async () => {
      const api = fakeAdmin([answer([entry('share.auto.offset.reset', 'earliest', GROUP_CONFIG)])])
      const pending = resetShareGroupConfigs(api, GROUP, Object.keys(EARLIEST)).catch((e) => e)
      await jest.advanceTimersByTimeAsync(8500)
      expect((await pending).message).toContain('did not settle within 8000 ms')
      expect(api.incrementalAlterConfigs).toHaveBeenCalledTimes(1)
      expect(jest.getTimerCount()).toBe(0)
    })

    test.each([
      answer([], TOPIC_RESOURCE),
      answer([], GROUP_RESOURCE, 'another-group'),
      { resources: [{ ...answer([]).resources[0], errorCode: 30 }] },
    ])('refuses an absent or unsuccessful GROUP readback: %j', async (read) => {
      const api = fakeAdmin([read])
      await expect(resetShareGroupConfigs(api, GROUP, Object.keys(EARLIEST))).rejects.toThrow(
        'Missing successful group config readback'
      )
    })

    test('fails immediately on a refused delete', async () => {
      const api = fakeAdmin([answer([])], { errorCode: 30 })
      await expect(resetShareGroupConfigs(api, GROUP, Object.keys(EARLIEST))).rejects.toThrow(
        'Config reset refused: 30'
      )
      expect(api.describeConfigs).not.toHaveBeenCalled()
    })
  })

  describe('topics', () => {
    afterEach(() => jest.restoreAllMocks())

    test('names a topic e2e-p9e-<scenario>-<random>, fresh each time', () => {
      const first = shareTopicName('delivery')
      const second = shareTopicName('delivery')

      expect(SHARE_TOPIC_PREFIX).toBe('e2e-p9e-')
      expect(first).toMatch(/^e2e-p9e-delivery-[0-9a-f]{20}-\d+-[0-9a-f-]{36}$/)
      expect(second).not.toBe(first)
    })

    test.each(['', 'Delivery', 'ack_types', 'lock.short', '-lock', 'lock-', undefined])(
      'refuses the scenario name %p',
      (scenario) => {
        expect(() => shareTopicName(scenario)).toThrow(
          'A share scenario name is lower-case kebab-case'
        )
      }
    )

    test('creates the topic through createTopic and returns its name', async () => {
      const createTopic = jest.spyOn(helpers, 'createTopic').mockResolvedValue(undefined)

      const topic = await createShareTopic({ scenario: 'lock', partitions: 3 })

      expect(topic).toMatch(/^e2e-p9e-lock-/)
      expect(createTopic).toHaveBeenCalledTimes(1)
      expect(createTopic).toHaveBeenCalledWith({ topic, partitions: 3, replicas: 1 })
    })

    test('refuses an invalid scenario before creating anything', async () => {
      const createTopic = jest.spyOn(helpers, 'createTopic').mockResolvedValue(undefined)

      await expect(createShareTopic({ scenario: 'Lock' })).rejects.toThrow(
        'A share scenario name is lower-case kebab-case'
      )
      expect(createTopic).not.toHaveBeenCalled()
    })
  })

  describe('compose variants', () => {
    const ROOT = path.join(__dirname, '..', '..')
    const lines = (name) => fs.readFileSync(path.join(ROOT, name), 'utf8').split('\n')
    const SHARE = [
      "    KAFKA_GROUP_SHARE_MIN_RECORD_LOCK_DURATION_MS: '1000'",
      "    KAFKA_GROUP_SHARE_SESSION_TIMEOUT_MS: '6000'",
      "    KAFKA_GROUP_SHARE_MIN_SESSION_TIMEOUT_MS: '6000'",
      "    KAFKA_GROUP_SHARE_HEARTBEAT_INTERVAL_MS: '1000'",
      "    KAFKA_GROUP_SHARE_MIN_HEARTBEAT_INTERVAL_MS: '1000'",
    ]
    const LIMIT = [
      ...SHARE,
      "    KAFKA_GROUP_SHARE_MAX_SHARE_SESSIONS: '2'",
      "    KAFKA_GROUP_SHARE_MAX_SIZE: '2'",
    ]

    test.each([
      ['docker-compose.share.yml', SHARE],
      ['docker-compose.share-limit.yml', LIMIT],
    ])(
      '%s is the base cluster with its share settings in the environment every broker shares',
      (name, settings) => {
        const base = lines('docker-compose.yml')
        const variant = lines(name)
        const start = variant.indexOf(base[0])
        const body = variant.slice(start)
        const shared = body.slice(
          body.indexOf('  environment: &kafka-env'),
          body.indexOf('  volumes:')
        )

        expect(start).toBeGreaterThan(0)
        expect(variant.slice(0, start).every((line) => line.startsWith('# '))).toBe(true)
        expect(body.filter((line) => !settings.includes(line))).toEqual(base)
        expect(shared.filter((line) => settings.includes(line))).toEqual(settings)
      }
    )
  })
})

describe('share live fixture observations', () => {
  const {
    shareRecordIdentity,
    ShareHandlerLog,
    verifierMember,
    captureShareCleanupBaseline,
    assertShareCleanup,
  } = require('../shareFixtures')
  const { NEVER, bounded, liveRun } = require('../../src/producer/__tests__/fixtures/bounded')
  const { CleanupStack } = require('../../src/producer/__tests__/fixtures/liveProbe')
  const record = {
    topic: 'records',
    partition: 0,
    offset: '9007199254740993',
    key: Buffer.from('a'),
    value: Buffer.from('b'),
    deliveryCount: 1,
  }
  let directory
  beforeEach(() => {
    liveRun.reset()
    directory = fs.mkdtempSync(path.join(os.tmpdir(), 'share-log-'))
  })
  afterEach(() => {
    jest.useRealTimers()
    fs.rmSync(directory, { recursive: true, force: true })
    liveRun.reset()
  })

  it('keeps decimal offsets and nullable bytes in collision-free identities', () => {
    expect(shareRecordIdentity(record)).toBe('["records",0,"9007199254740993","61","62"]')
    expect(shareRecordIdentity({ ...record, offset: '9007199254740992' })).not.toBe(
      shareRecordIdentity(record)
    )
    expect(shareRecordIdentity({ ...record, value: null })).not.toBe(
      shareRecordIdentity({ ...record, value: Buffer.alloc(0) })
    )
    expect(() => shareRecordIdentity({ ...record, offset: 1 })).toThrow('decimal offset')
  })

  it('records starts durably and reconstructs only the original held invocations', () => {
    const file = path.join(directory, 'member.jsonl')
    const journal = new ShareHandlerLog(file, 'child')
    try {
      const first = journal.start(record)
      const second = journal.start({ ...record, deliveryCount: 2 })
      journal.finish(first)
      expect(ShareHandlerLog.read(file).held).toEqual([shareRecordIdentity(record)])
      expect(
        ShareHandlerLog.read(file).rows.map(({ event, invocation }) => [event, invocation])
      ).toEqual([
        ['start', 1],
        ['start', 2],
        ['finish', 1],
      ])
      expect(
        ShareHandlerLog.read(file)
          .rows.filter(({ event }) => event === 'start')
          .map(({ deliveryCount }) => deliveryCount)
      ).toEqual([1, 2])
      expect(second).toBe(2)
      expect(() => journal.finish(first)).toThrow('Unknown handler invocation')
    } finally {
      journal.close()
    }
    journal.close()
    expect(() => journal.start(record)).toThrow('closed')
  })

  it('refuses truncated journals and duplicate completion instead of widening the held set', () => {
    const file = path.join(directory, 'bad.jsonl')
    fs.writeFileSync(file, '{')
    expect(() => ShareHandlerLog.read(file)).toThrow('Incomplete')
    fs.writeFileSync(file, '{"event":"finish","invocation":1}\n')
    expect(() => ShareHandlerLog.read(file)).toThrow('order')
  })

  it('observes a complete post-join verifier window and disconnects its member', async () => {
    jest.useFakeTimers()
    let handler
    let joined
    const join = new Promise((resolve) => {
      joined = resolve
    })
    const remove = jest.fn()
    const client = {
      events: { CRASH: 'crash' },
      on: jest.fn(() => remove),
      connect: jest.fn(async () => {}),
      subscribe: jest.fn(async () => {}),
      run: jest.fn(async ({ eachMessage }) => {
        handler = eachMessage
        joined()
      }),
      disconnect: jest.fn(async () => {}),
    }
    const kafka = { shareConsumer: jest.fn(() => client) }
    const pending = verifierMember('existing', 'records', { kafka, windowMs: 5000 })
    await join
    await handler({ topic: 'records', partition: 0, message: record })
    await jest.advanceTimersByTimeAsync(4999)
    expect(client.disconnect).not.toHaveBeenCalled()
    await jest.advanceTimersByTimeAsync(1)
    expect(await pending).toEqual([{ identity: shareRecordIdentity(record), deliveryCount: 1 }])
    expect(client.subscribe).toHaveBeenCalledWith({ topics: ['records'] })
    expect(client.disconnect).toHaveBeenCalledWith({ timeout: 2000 })
    expect(remove).toHaveBeenCalledTimes(1)
    expect(jest.getTimerCount()).toBe(0)
  })

  it('keeps verifier join failure primary and taints a failed disconnect', async () => {
    const primary = new Error('join refused')
    const client = {
      events: { CRASH: 'crash' },
      on: () => () => {},
      connect: async () => {
        throw primary
      },
      disconnect: async () => {
        throw new Error('close refused')
      },
    }
    const caught = await verifierMember('existing', 'records', {
      kafka: { shareConsumer: () => client },
    }).catch((e) => e)
    expect(caught).toBe(primary)
    expect(caught.cleanupErrors).toHaveLength(1)
    expect(liveRun.tainted).toBe(true)
  })

  const snapshot = () => ({
    listeners: { client: 0 },
    sockets: { created: 2, destroyed: 2, open: 0 },
    queueDepth: 0,
    ledgerSize: 0,
    activeResources: {},
  })
  const admin = () => ({
    listGroups: async () => ({ groups: [{ groupId: 'ordinary' }] }),
    listTopics: async () => [],
    disconnect: async () => {},
  })

  it.each([true, false])(
    'passes identical real-timer sampling with a cleanup stack: %s',
    async (cleanupStack) => {
      const probe = createResourceProbe({
        emitters: { client: new EventEmitter() },
        sockets: () => ({ created: 0, destroyed: 0 }),
        queueDepth: () => 0,
        ledgerSize: () => 0,
        activeResources: true,
      })
      await bounded(NEVER, 2000, async (scope) => {
        const before = await captureShareCleanupBaseline(probe, { signal: scope, cleanupStack })
        let after
        const prove = async (signal) => {
          after = await assertShareCleanup(admin(), [], [], before, probe, {
            signal,
            disconnectAdmin: true,
          })
        }
        if (cleanupStack) {
          const cleanup = new CleanupStack()
          cleanup.add('prove cleanup', prove)
          await cleanup.run(async () => {}, { budgetMs: 15000 })
        } else {
          await prove(scope)
        }
        expect(after).toEqual(before)
      })
    }
  )

  it.each([false, true])(
    'waits for a foreign socket and its timer to close, then detects a real leak: %s',
    async (leak) => {
      jest.useFakeTimers()
      let foreign = true
      let leaked = false
      const probe = {
        snapshot: jest.fn(async () => ({
          ...snapshot(),
          activeResources: {
            Timeout: 2 + Number(foreign) + Number(leaked),
            ...(foreign || leaked ? { TCPSocketWrap: 1 } : {}),
          },
        })),
      }
      const pending = captureShareCleanupBaseline(probe, { cleanupStack: false })
      const state = track(pending)
      await jest.advanceTimersByTimeAsync(300)
      expect(state.settled).toBe(false)
      foreign = false
      await jest.advanceTimersByTimeAsync(200)
      const before = await pending
      expect(before.activeResources).toEqual({ Timeout: 2 })
      leaked = leak
      const proof = assertShareCleanup(admin(), [], [], before, probe).catch((e) => e)
      await jest.advanceTimersByTimeAsync(1)
      if (leak) {
        expect(await proof).toBeInstanceOf(Error)
        expect((await proof).message).toContain('Share resources did not return to baseline')
      } else {
        expect(await proof).toEqual(before)
      }
      expect(jest.getTimerCount()).toBe(0)
    }
  )

  it('requires consecutive stable timer samples after the foreign sockets close', async () => {
    jest.useFakeTimers()
    let timerClosing = true
    const probe = {
      snapshot: async () => ({
        ...snapshot(),
        activeResources: { Timeout: timerClosing ? 3 : 2 },
      }),
    }
    const pending = captureShareCleanupBaseline(probe, { cleanupStack: false })
    const state = track(pending)
    await jest.advanceTimersByTimeAsync(60)
    timerClosing = false
    await jest.advanceTimersByTimeAsync(70)
    expect(state.settled).toBe(false)
    await jest.advanceTimersByTimeAsync(100)
    expect((await pending).activeResources).toEqual({ Timeout: 2 })
    expect(jest.getTimerCount()).toBe(0)
  })

  it('rejects a persistent foreign socket within the existing baseline bound', async () => {
    jest.useFakeTimers()
    const pending = captureShareCleanupBaseline(
      {
        snapshot: async () => ({ ...snapshot(), activeResources: { TCPSocketWrap: 1 } }),
      },
      { cleanupStack: false }
    ).catch((e) => e)
    await jest.advanceTimersByTimeAsync(10500)
    expect((await pending).message).toContain('did not settle within 10000 ms')
    expect(jest.getTimerCount()).toBe(0)
  })

  it.each(['Timeout', 'socket', 'listener', 'queued request', 'ledger entry'])(
    'rejects one extra client resource under identical sampling bounds: %s',
    async (resource) => {
      const emitter = new EventEmitter()
      const { factory, state } = createFakeSocketFactory()
      const queue = []
      const ledger = new Set()
      const probe = createResourceProbe({
        emitters: { client: emitter },
        sockets: () => socketCounts(state),
        queueDepth: () => queue.length,
        ledgerSize: () => ledger.size,
        activeResources: true,
      })
      const before = await captureShareCleanupBaseline(probe, { cleanupStack: false })
      let timer
      try {
        if (resource === 'Timeout') timer = setTimeout(() => {}, 60000)
        if (resource === 'socket') factory({ host: 'broker', port: 9092, onConnect: () => {} })
        if (resource === 'listener') emitter.on('data', () => {})
        if (resource === 'queued request') queue.push({})
        if (resource === 'ledger entry') ledger.add({})
        const error = await assertShareCleanup(admin(), [], [], before, probe).catch((e) => e)
        expect(error).toBeInstanceOf(Error)
        expect(error.message).toContain('Share resources did not return to baseline')
        expect(error.message).toContain('baseline:')
        expect(error.message).toContain('lastAfter:')
        expect(error.message).toContain('diff:')
        if (resource === 'Timeout') {
          expect(error.message).toContain('diff: { activeResources:')
          expect(error.message).toContain(`Timeout: ${before.activeResources.Timeout + 1}`)
        }
        if (resource === 'socket') {
          expect(error.message).toContain('sockets: { baseline: 0, lastAfter: 1 }')
        }
        if (resource === 'listener') {
          expect(error.message).toContain(
            'listeners: { baseline: { client: 0 }, lastAfter: { client: 1 } }'
          )
        }
        if (resource === 'queued request') {
          expect(error.message).toContain('queueDepth: { baseline: 0, lastAfter: 1 }')
        }
        if (resource === 'ledger entry') {
          expect(error.message).toContain('ledgerSize: { baseline: 0, lastAfter: 1 }')
        }
      } finally {
        clearTimeout(timer)
        state.sockets.forEach((socket) => socket.destroy())
        emitter.removeAllListeners()
      }
    }
  )

  it.each([100, 20000])(
    'reports the last diff when cleanup or its enclosing %d ms deadline expires',
    async (outerMs) => {
      jest.useFakeTimers()
      const before = { ...snapshot(), activeResources: { Timeout: 2 } }
      const last = { ...snapshot(), activeResources: { Timeout: 3 } }
      const pending = bounded(NEVER, outerMs, (signal) =>
        assertShareCleanup(
          admin(),
          [],
          [],
          before,
          { snapshot: async () => last },
          { signal, disconnectAdmin: true }
        )
      ).catch((error) => error)
      await jest.advanceTimersByTimeAsync(10500)
      const error = await pending
      expect(error.message).toContain(`did not settle within ${Math.min(outerMs, 10000)} ms`)
      expect(error.message).toContain('baseline:')
      expect(error.message).toContain('lastAfter:')
      expect(error.message).toContain('diff:')
      expect(error.message).toContain('Timeout: 2')
      expect(error.message).toContain('Timeout: 3')
      expect(jest.getTimerCount()).toBe(0)
    }
  )

  it.each(['listeners', 'sockets', 'queueDepth', 'ledgerSize', 'activeResources'])(
    'reports a missing %s measurement instead of supplying zero',
    async (key) => {
      const after = snapshot()
      delete after[key]
      const error = await assertShareCleanup(admin(), [], [], snapshot(), {
        snapshot: async () => after,
      }).catch((e) => e)
      expect(error.message).toContain(`Missing resource measurement: ${key}`)
      expect(error.message).toContain('baseline:')
      expect(error.message).toContain('lastAfter:')
      expect(error.message).toContain('diff:')
    }
  )

  it('proves owned objects absent and unrelated presence with cumulative socket totals', async () => {
    const after = { ...snapshot(), sockets: { created: 9, destroyed: 9, open: 0 } }
    expect(
      await assertShareCleanup(
        admin(),
        ['owned'],
        ['owned-topic'],
        snapshot(),
        { snapshot: async () => after },
        { unrelated: ['ordinary'] }
      )
    ).toBe(after)
  })

  it.each(['group', 'topic', 'sibling', 'resource', 'measurement'])(
    'rejects a false cleanup proof: %s',
    async (fault) => {
      const api = admin()
      const after = snapshot()
      if (fault === 'group') api.listGroups = async () => ({ groups: [{ groupId: 'owned' }] })
      if (fault === 'topic') api.listTopics = async () => ['owned-topic']
      if (fault === 'sibling') api.listGroups = async () => ({ groups: [] })
      if (fault === 'resource') after.ledgerSize = 1
      if (fault === 'measurement') delete after.activeResources
      const error = await assertShareCleanup(
        api,
        ['owned'],
        ['owned-topic'],
        snapshot(),
        { snapshot: async () => after },
        { unrelated: ['ordinary'] }
      ).catch((e) => e)
      expect(error).toBeInstanceOf(Error)
      expect(error.message).toContain('baseline:')
      expect(error.message).toContain('lastAfter:')
      expect(error.message).toContain('diff:')
    }
  )

  it('refuses missing or invalid measurements even when both snapshots match', async () => {
    for (const corrupt of [
      (value) => delete value.sockets.open,
      (value) => {
        value.queueDepth = -1
      },
      (value) => {
        value.listeners.client = 0.5
      },
      (value) => {
        value.sockets.open = 1
      },
    ]) {
      const before = snapshot()
      corrupt(before)
      await expect(
        assertShareCleanup(admin(), [], [], before, {
          snapshot: async () => structuredClone(before),
        })
      ).rejects.toThrow()
    }
  })

  it('refuses malformed identity and delivery facts in a complete journal line', () => {
    const file = path.join(directory, 'invalid.jsonl')
    const valid = {
      member: 'child',
      event: 'start',
      invocation: 1,
      identity: shareRecordIdentity(record),
      deliveryCount: 1,
    }
    for (const corrupt of [
      (value) => delete value.member,
      (value) => delete value.identity,
      (value) => delete value.deliveryCount,
      (value) => {
        value.identity = '["records",0,"1","xz",null]'
      },
    ]) {
      const row = { ...valid }
      corrupt(row)
      fs.writeFileSync(file, JSON.stringify(row) + '\n')
      expect(() => ShareHandlerLog.read(file)).toThrow()
    }
  })
})
