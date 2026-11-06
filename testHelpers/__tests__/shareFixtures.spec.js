const fs = require('fs')
const path = require('path')
const helpers = require('../index')
const {
  SHARE_TOPIC_PREFIX,
  shareTopicName,
  createShareTopic,
  setShareGroupConfigs,
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
