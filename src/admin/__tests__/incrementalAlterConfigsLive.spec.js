const {
  Kafka,
  logLevel,
  ConfigResourceTypes,
  ConfigSource,
  ConfigTypes,
  AlterConfigOpTypes,
} = require('../../../index')
const {
  createTopic,
  plainTextBrokers,
  writeEvidence,
  evidenceLines,
  wireVersionsOf,
} = require('testHelpers')
const { problemsOf } = require('../../../scripts/evidence/verify')
const { waitForCondition } = require('../../consumer/__tests__/fixtures/liveScenario')

// Gated: it changes cluster-wide configuration (the cluster-default broker config), so it runs
// alone through `npm run test:e2e:config-admin`, never beside other specs.
const describeConfigAdmin = process.env.CONFIG_ADMIN_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'config-admin'
// The lines earlier runs left behind, so the last case judges only this run's.
const linesBefore = evidenceLines(EVIDENCE).length
// Named so each wait says how long it is prepared to be wrong for. The scenario budget is the
// whole file, from the first case to the last; a case has its own, shorter one.
const SCENARIO_BUDGET_MS = 90000
const CASE_BUDGET_MS = 45000
const HOOK_BUDGET_MS = 30000
// A config change is committed by the controller before the call answers, but the broker that
// answers DescribeConfigs applies it when it replays the metadata log, a moment later.
const PROPAGATION_BUDGET = { attempts: 40, delay: 250 } // 10 s

const { TOPIC, BROKER, GROUP, CLIENT_METRICS } = ConfigResourceTypes
const { SET, DELETE, APPEND, SUBTRACT } = AlterConfigOpTypes
const INVALID_CONFIG = 40

/**
 * Live proof of the config APIs on a KRaft broker: `incrementalAlterConfigs` and its operations on
 * a topic, a consumer group and a client-metrics subscription, the per-resource results, and the
 * cluster-default broker config reached through `describeConfigs`, `alterConfigs` and
 * `incrementalAlterConfigs`.
 *
 * Every change is read back with `describeConfigs` until it shows or a bounded wait ends, and the
 * evidence line of each case says how many reads that took. Counts and versions are asserted,
 * never durations.
 *
 * The group case uses a session timeout of 12 s, which the test cluster accepts because it sets
 * the group minimum to 6 s (`docker-compose.yml`); a default broker's minimum is 45 s.
 *
 * @see https://github.com/apache/kafka/blob/4.3.1/metadata/src/main/java/org/apache/kafka/controller/ConfigurationControlManager.java
 * @see https://github.com/apache/kafka/blob/4.3.1/core/src/main/scala/kafka/server/ConfigHelper.scala
 */
describeConfigAdmin('Admin > incrementalAlterConfigs and the config routing (live)', () => {
  const runId = Date.now().toString(36)
  const topic = `e2e-p8-config-admin-${runId}-topic`
  const groupId = `e2e-p8-config-admin-${runId}-group`
  const subscription = `e2e-p8-config-admin-${runId}-metrics`
  const topicResource = { type: TOPIC, name: topic }
  const clusterDefault = { type: BROKER, name: '' }
  /** @type {{ apiName: string, apiVersion: number }[]} */
  const requests = []
  let admin
  let startedAt

  /**
   * @param {{ type: number, name: string }} resource
   * @param {object[]} configEntries
   * @param {boolean} [validateOnly]
   * @returns {Promise<object[]>} the results, one per resource sent
   */
  const incrementalAlter = async (resource, configEntries, validateOnly = false) => {
    const { resources } = await admin.incrementalAlterConfigs({
      resources: [{ ...resource, configEntries }],
      validateOnly,
    })
    return resources
  }

  /**
   * @param {{ type: number, name: string }} resource
   * @param {object} [options] more `describeConfigs` options, e.g. `includeDocumentation`
   * @returns {Promise<Object<string, object>>} the entries of the resource by config name
   */
  const entriesOf = async (resource, options = {}) => {
    const { resources } = await admin.describeConfigs({ resources: [resource], ...options })
    return Object.fromEntries(resources[0].configEntries.map((entry) => [entry.configName, entry]))
  }

  /**
   * Reads the resource until `holds(entries)` is true, and says how it went in the evidence file.
   * @param {string} step what the read waits for
   * @param {{ type: number, name: string }} resource
   * @param {(entries: Object<string, object>) => boolean} holds
   * @returns {Promise<{ step: string, held: boolean, polls: number, entries: object }>}
   */
  const settled = async (step, resource, holds) => {
    let entries = {}
    let polls = 0
    const held = await waitForCondition(async () => {
      polls += 1
      entries = await entriesOf(resource)
      return holds(entries)
    }, PROPAGATION_BUDGET)

    writeEvidence(EVIDENCE, { step, held, polls })
    return { step, held, polls, entries }
  }

  beforeAll(async () => {
    admin = new Kafka({
      clientId: 'config-admin-live',
      brokers: plainTextBrokers(),
      logLevel: logLevel.NOTHING,
    }).admin()
    admin.on(admin.events.REQUEST, ({ payload }) =>
      requests.push({ apiName: payload.apiName, apiVersion: payload.apiVersion })
    )
    await admin.connect()
    await createTopic({ topic })
    startedAt = Date.now()
  }, HOOK_BUDGET_MS)

  afterEach(() => {
    expect(Date.now() - startedAt).toBeLessThan(SCENARIO_BUDGET_MS)
  })

  afterAll(async () => {
    // The cluster-default config and the subscription outlive the topic, so they are put back.
    await incrementalAlter(clusterDefault, [
      { name: 'background.threads', operation: DELETE },
    ]).catch(() => {})
    await incrementalAlter({ type: CLIENT_METRICS, name: subscription }, [
      { name: 'interval.ms', operation: DELETE },
      { name: 'metrics', operation: DELETE },
    ]).catch(() => {})
    await incrementalAlter({ type: GROUP, name: groupId }, [
      { name: 'consumer.session.timeout.ms', operation: DELETE },
    ]).catch(() => {})
    await admin.deleteTopics({ topics: [topic] }).catch(() => {})
    await admin.disconnect()
  }, HOOK_BUDGET_MS)

  test(
    'TOPIC: SET makes a TOPIC_CONFIG entry and DELETE reverts it to the default',
    async () => {
      const set = await incrementalAlter(topicResource, [
        { name: 'retention.ms', operation: SET, value: '60000' },
      ])
      expect(set).toEqual([expect.objectContaining({ errorCode: 0, resourceName: topic })])

      const afterSet = await settled(
        'topic-set',
        topicResource,
        (entries) =>
          entries['retention.ms'].configValue === '60000' &&
          entries['retention.ms'].configSource === ConfigSource.TOPIC_CONFIG
      )
      expect(afterSet).toMatchObject({ held: true })

      const deleted = await incrementalAlter(topicResource, [
        { name: 'retention.ms', operation: DELETE },
      ])
      expect(deleted).toEqual([expect.objectContaining({ errorCode: 0 })])

      const afterDelete = await settled(
        'topic-delete',
        topicResource,
        (entries) =>
          entries['retention.ms'].configSource === ConfigSource.DEFAULT_CONFIG &&
          entries['retention.ms'].isDefault === true
      )
      expect(afterDelete).toMatchObject({ held: true })
    },
    CASE_BUDGET_MS
  )

  test(
    'TOPIC: APPEND and SUBTRACT change the list in cleanup.policy, and fail on a key that is no list',
    async () => {
      const appended = await incrementalAlter(topicResource, [
        { name: 'cleanup.policy', operation: APPEND, value: 'compact' },
      ])
      expect(appended).toEqual([expect.objectContaining({ errorCode: 0 })])
      // an unset list starts from its default, `delete`
      const afterAppend = await settled(
        'topic-append',
        topicResource,
        (entries) => entries['cleanup.policy'].configValue === 'delete,compact'
      )
      expect(afterAppend).toMatchObject({ held: true })

      const subtracted = await incrementalAlter(topicResource, [
        { name: 'cleanup.policy', operation: SUBTRACT, value: 'delete' },
      ])
      expect(subtracted).toEqual([expect.objectContaining({ errorCode: 0 })])
      const afterSubtract = await settled(
        'topic-subtract',
        topicResource,
        (entries) => entries['cleanup.policy'].configValue === 'compact'
      )
      expect(afterSubtract).toMatchObject({ held: true })

      const notAList = await incrementalAlter(topicResource, [
        { name: 'retention.ms', operation: APPEND, value: '1' },
      ])
      writeEvidence(EVIDENCE, { step: 'append-to-a-non-list', result: notAList[0] })
      expect(notAList).toEqual([expect.objectContaining({ errorCode: INVALID_CONFIG })])
    },
    CASE_BUDGET_MS
  )

  test(
    'TOPIC: validateOnly changes nothing',
    async () => {
      await incrementalAlter(topicResource, [
        { name: 'retention.ms', operation: SET, value: '60000' },
      ])
      expect(
        await settled(
          'validate-baseline',
          topicResource,
          (e) => e['retention.ms'].configValue === '60000'
        )
      ).toMatchObject({ held: true })

      const validated = await incrementalAlter(
        topicResource,
        [{ name: 'retention.ms', operation: SET, value: '70000' }],
        true
      )
      expect(validated).toEqual([expect.objectContaining({ errorCode: 0 })])

      // The broker replays the metadata log in order, so once this later change shows, a change the
      // validated request wrongly made would show too.
      await incrementalAlter(topicResource, [
        { name: 'flush.messages', operation: SET, value: '10000' },
      ])
      const barrier = await settled(
        'validate-barrier',
        topicResource,
        (entries) => entries['flush.messages'].configValue === '10000'
      )
      expect(barrier).toMatchObject({ held: true })
      expect(barrier.entries['retention.ms'].configValue).toBe('60000')
    },
    CASE_BUDGET_MS
  )

  test(
    'a good and a bad resource in one call each get their own result, and the good one is applied',
    async () => {
      const { resources } = await admin.incrementalAlterConfigs({
        resources: [
          {
            ...topicResource,
            configEntries: [{ name: 'segment.ms', operation: SET, value: '3600000' }],
          },
          {
            ...topicResource,
            name: `${topic}-missing`,
            configEntries: [{ name: 'segment.ms', operation: SET, value: '3600000' }],
          },
        ],
      })

      expect(resources).toEqual([
        expect.objectContaining({ errorCode: 0, resourceName: topic }),
        expect.objectContaining({ resourceName: `${topic}-missing` }),
      ])
      expect(resources[1].errorCode).not.toBe(0)
      writeEvidence(EVIDENCE, { step: 'good-and-bad', bad: resources[1] })

      const applied = await settled(
        'good-and-bad-applied',
        topicResource,
        (entries) => entries['segment.ms'].configValue === '3600000'
      )
      expect(applied).toMatchObject({ held: true })
    },
    CASE_BUDGET_MS
  )

  test(
    'GROUP: consumer.session.timeout.ms makes a GROUP_CONFIG entry and DELETE removes it',
    async () => {
      const group = { type: GROUP, name: groupId }

      const set = await incrementalAlter(group, [
        { name: 'consumer.session.timeout.ms', operation: SET, value: '12000' },
      ])
      expect(set).toEqual([expect.objectContaining({ errorCode: 0, resourceName: groupId })])
      const afterSet = await settled(
        'group-set',
        group,
        (entries) =>
          entries['consumer.session.timeout.ms'].configValue === '12000' &&
          entries['consumer.session.timeout.ms'].configSource === ConfigSource.GROUP_CONFIG
      )
      expect(afterSet).toMatchObject({ held: true })

      await incrementalAlter(group, [{ name: 'consumer.session.timeout.ms', operation: DELETE }])
      const afterDelete = await settled(
        'group-delete',
        group,
        (entries) =>
          entries['consumer.session.timeout.ms'].configSource !== ConfigSource.GROUP_CONFIG
      )
      expect(afterDelete).toMatchObject({ held: true })
    },
    CASE_BUDGET_MS
  )

  test(
    'CLIENT_METRICS: a subscription is set, described and deleted',
    async () => {
      const metrics = { type: CLIENT_METRICS, name: subscription }

      const set = await incrementalAlter(metrics, [
        { name: 'interval.ms', operation: SET, value: '30000' },
        { name: 'metrics', operation: SET, value: 'org.apache.kafka.consumer.' },
      ])
      expect(set).toEqual([expect.objectContaining({ errorCode: 0, resourceName: subscription })])
      const afterSet = await settled(
        'metrics-set',
        metrics,
        (entries) =>
          entries['interval.ms'].configValue === '30000' &&
          entries['interval.ms'].configSource === ConfigSource.CLIENT_METRICS_CONFIG &&
          entries.metrics.configValue === 'org.apache.kafka.consumer.'
      )
      expect(afterSet).toMatchObject({ held: true })

      await incrementalAlter(metrics, [
        { name: 'interval.ms', operation: DELETE },
        { name: 'metrics', operation: DELETE },
      ])
      const afterDelete = await settled(
        'metrics-delete',
        metrics,
        (entries) => entries['interval.ms'].configSource === ConfigSource.DEFAULT_CONFIG
      )
      expect(afterDelete).toMatchObject({ held: true })
    },
    CASE_BUDGET_MS
  )

  test(
    'BROKER with the empty name is described and altered at the cluster default by all three methods',
    async () => {
      const described = await admin.describeConfigs({ resources: [clusterDefault] })
      expect(described.resources).toEqual([
        expect.objectContaining({ errorCode: 0, resourceType: BROKER, resourceName: '' }),
      ])

      // incrementalAlterConfigs adds the entry; the entries the default has stay as they are
      const incremental = await incrementalAlter(clusterDefault, [
        { name: 'background.threads', operation: SET, value: '11' },
      ])
      expect(incremental).toEqual([expect.objectContaining({ errorCode: 0, resourceName: '' })])
      const afterIncremental = await settled(
        'cluster-default-incremental',
        clusterDefault,
        (entries) =>
          entries['background.threads'] !== undefined &&
          entries['background.threads'].configValue === '11' &&
          entries['background.threads'].configSource === ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG
      )
      expect(afterIncremental).toMatchObject({ held: true })

      // alterConfigs replaces the whole cluster-default dynamic config, so the entries the default
      // already holds are sent back: with eligible leader replicas on (the default for a new 4.1+
      // cluster) the controller refuses to drop the cluster-level `min.insync.replicas`.
      // https://github.com/apache/kafka/blob/4.3.1/metadata/src/main/java/org/apache/kafka/controller/ConfigurationControlManager.java
      const kept = Object.values(afterIncremental.entries)
        .filter(({ configName }) => configName !== 'background.threads')
        .map(({ configName, configValue }) => ({ name: configName, value: configValue }))
      await admin.alterConfigs({
        resources: [
          {
            ...clusterDefault,
            configEntries: [...kept, { name: 'background.threads', value: '12' }],
          },
        ],
      })
      const afterReplace = await settled(
        'cluster-default-replace',
        clusterDefault,
        // A broker that has not applied the change yet may still lack the entry.
        (entries) => entries['background.threads']?.configValue === '12'
      )
      expect(afterReplace).toMatchObject({ held: true })
      for (const { name, value } of kept) {
        expect(afterReplace.entries[name].configValue).toBe(value)
      }

      await incrementalAlter(clusterDefault, [{ name: 'background.threads', operation: DELETE }])
      const afterDelete = await settled(
        'cluster-default-delete',
        clusterDefault,
        (entries) => entries['background.threads'] === undefined
      )
      expect(afterDelete).toMatchObject({ held: true })
    },
    CASE_BUDGET_MS
  )

  test(
    'describeConfigs returns documentation only when asked, and every entry has a configType',
    async () => {
      const without = await entriesOf(topicResource)
      const withDocumentation = await entriesOf(topicResource, { includeDocumentation: true })

      expect(Object.values(without).every(({ documentation }) => documentation === null)).toBe(true)
      expect(typeof withDocumentation['cleanup.policy'].documentation).toBe('string')
      expect(withDocumentation['cleanup.policy'].documentation.length).toBeGreaterThan(0)
      expect(withDocumentation['cleanup.policy'].configType).toBe(ConfigTypes.LIST)
      expect(withDocumentation['retention.ms'].configType).toBe(ConfigTypes.LONG)
      expect(
        Object.values(withDocumentation).every(({ configType }) => Number.isInteger(configType))
      ).toBe(true)
    },
    CASE_BUDGET_MS
  )

  test(
    'the config calls negotiate the newest versions',
    () => {
      const versionsOf = (apiName) => [
        ...new Set(requests.filter((r) => r.apiName === apiName).map((r) => r.apiVersion)),
      ]
      const negotiated = {
        IncrementalAlterConfigs: versionsOf('IncrementalAlterConfigs'),
        DescribeConfigs: versionsOf('DescribeConfigs'),
        AlterConfigs: versionsOf('AlterConfigs'),
      }

      writeEvidence(EVIDENCE, {
        step: 'versions',
        negotiated,
        wireVersions: wireVersionsOf(requests),
      })
      expect(negotiated).toEqual({
        IncrementalAlterConfigs: [1],
        DescribeConfigs: [4],
        AlterConfigs: [2],
      })
    },
    CASE_BUDGET_MS
  )

  test(
    'every evidence line of this run carries the stamps a verifier checks',
    () => {
      const lines = evidenceLines(EVIDENCE).slice(linesBefore)
      const verified = {
        tree: process.env.KAFKAJS_TREE || 'unfrozen',
        node: process.version,
        brokerVersion: process.env.KAFKA_VERSION,
      }

      expect(lines.length).toBeGreaterThan(0)
      for (const line of lines) {
        expect(problemsOf(line, verified)).toEqual([])
      }
      expect(lines.find(({ step }) => step === 'versions').wireVersions).toMatchObject({
        IncrementalAlterConfigs: 1,
        DescribeConfigs: 4,
        AlterConfigs: 2,
      })
    },
    CASE_BUDGET_MS
  )
})
