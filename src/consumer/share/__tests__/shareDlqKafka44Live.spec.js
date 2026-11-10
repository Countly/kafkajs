const {
  resetShareGroupConfigs,
  shareRecordIdentity,
} = require('../../../../testHelpers/shareFixtures')
const { awaitTerminalSharePosition } = require('../../../../testHelpers/shareLiveDiagnostics')
const {
  ShareKafka44Fixture,
  configureShareDlq,
  resetShareDlqClusterConfigs,
} = require('../../../../testHelpers/shareKafka44')
const {
  NEVER,
  bounded,
  caseTimeout,
  delay,
  liveCase,
  liveRun,
  pollUntil,
} = require('../../../producer/__tests__/fixtures/bounded')

// Gated: it needs the Kafka 4.4 share cluster of the dedicated share check, whose setup finalized
// share.version at 2 before Jest started. It runs with SHARE_KAFKA44_E2E=1.
const enabled = process.env.SHARE_KAFKA44_E2E === '1' ? describe : describe.skip

const HOOK_MS = 10000
const POLL_MS = 45000
const CASE_MS = 150000
const SETTLE_MS = 3000
const MEMBER = 'member'
const DELIVERY_LIMIT = 2

// The header names, encodings and messages of a dead-letter record, written out from the broker
// source as literals. They are never read back from the object under test.
const HEADERS = Object.freeze({
  topic: '__dlq.errors.topic',
  partition: '__dlq.errors.partition',
  offset: '__dlq.errors.offset',
  group: '__dlq.errors.group',
  deliveryCount: '__dlq.errors.delivery.count',
  message: '__dlq.errors.message',
})
const REJECT_MESSAGE = 'Offset rejected by client.'
const RELEASE_MESSAGE = 'Offset delivery count exceeded the threshold.'

/**
 * @param {Buffer|Buffer[]|string|undefined} value one header value as a plain consumer reads it
 * @returns {string|undefined} its UTF-8 text, or undefined when the header is absent
 */
const text = (value) => (value === undefined ? undefined : Buffer.from(value).toString('utf8'))

/**
 * Runs one dead-letter case end to end: a record is rejected, or released until the group's
 * delivery limit, and an ordinary consumer reads what the broker wrote to the destination topic.
 * @param {object} scope the case scope
 * @param {{ scenario: string, copyRecord: boolean, release: boolean }} options which record
 *   setting and which terminal path the case proves
 * @returns {Promise<void>}
 */
const runCase = async (scope, { scenario, copyRecord, release }) => {
  const f = new ShareKafka44Fixture(scenario)
  await f.run(async () => {
    await f.open(scope)
    const source = await f.topic(scope)
    const dlqTopic = await f.topic(scope, { name: `dlq.${source}` })
    const {
      topics: [metadata],
    } = await bounded(scope, 10000, () => f.admin.fetchTopicMetadata({ topics: [source] }))
    const group = await f.group(scope, source, {
      'share.auto.offset.reset': 'earliest',
      ...(release && { 'share.delivery.count.limit': DELIVERY_LIMIT }),
    })
    f.stack.add('reset the dead-letter group configs', (signal) =>
      resetShareGroupConfigs(
        f.admin,
        group,
        ['errors.deadletterqueue.topic.name', 'errors.deadletterqueue.copy.record.enable'],
        { signal }
      )
    )
    f.stack.add('reset the dead-letter cluster configs', () => resetShareDlqClusterConfigs(f.admin))
    await bounded(scope, 60000, () =>
      configureShareDlq({
        admin: f.admin,
        groupId: group,
        sourceTopic: source,
        dlqTopic,
        copyRecord,
      })
    )

    const key = Buffer.from(`key-${f.nonce}`)
    const value = Buffer.from(`value-${f.nonce}`)
    const producer = await f.producer(scope)
    await bounded(scope, 15000, () =>
      producer.send({
        topic: source,
        messages: [{ key, value, headers: { 'source-only': Buffer.from('must-not-copy') } }],
      })
    )

    const readerGroup = await f.group(scope, `${source}-reader`)
    const reader = f.client('reader').consumer({ groupId: readerGroup })
    const dead = []
    f.stack.add('disconnect the dead-letter reader', () => reader.disconnect())
    await bounded(scope, 15000, () => reader.connect())
    await bounded(scope, 15000, () => reader.subscribe({ topic: dlqTopic, fromBeginning: true }))
    await bounded(scope, 15000, () =>
      reader.run({
        eachMessage: async ({ message }) => {
          dead.push(message)
        },
      })
    )

    const deliveries = []
    const member = await f.member(
      scope,
      MEMBER,
      { groupId: group, acknowledgementMode: 'explicit' },
      [source],
      {
        eachMessage: async ({ topic, partition, message, acknowledge }) => {
          deliveries.push({
            identity: shareRecordIdentity({ ...message, topic, partition }),
            offset: message.offset,
            partition,
            deliveryCount: message.deliveryCount,
          })
          acknowledge(release ? 'release' : 'reject')
        },
      }
    )

    await pollUntil(scope, POLL_MS, () => dead.length > 0, {
      label: 'The dead-letter record',
    })
    await delay(scope, SETTLE_MS)
    expect(dead).toHaveLength(1)
    const [record] = dead

    expect(deliveries.map(({ deliveryCount }) => deliveryCount)).toEqual(
      release ? [1, DELIVERY_LIMIT] : [1]
    )
    const [delivered] = deliveries
    expect(delivered.identity).toBe(
      shareRecordIdentity({ topic: source, partition: 0, offset: '0', key, value })
    )
    expect(Object.keys(record.headers).sort()).toEqual(Object.values(HEADERS).sort())
    expect(text(record.headers[HEADERS.topic])).toBe(source)
    expect(text(record.headers[HEADERS.partition])).toBe(String(delivered.partition))
    expect(text(record.headers[HEADERS.offset])).toBe(delivered.offset)
    expect(text(record.headers[HEADERS.group])).toBe(group)
    expect(text(record.headers[HEADERS.deliveryCount])).toBe(release ? String(DELIVERY_LIMIT) : '1')
    expect(text(record.headers[HEADERS.message])).toBe(release ? RELEASE_MESSAGE : REJECT_MESSAGE)
    if (copyRecord) {
      expect(record.key).toEqual(key)
      expect(record.value).toEqual(value)
    } else {
      expect(record.key).toBe(null)
      expect(record.value).toBe(null)
    }

    await awaitTerminalSharePosition(
      f.admin,
      { groupId: group, topic: source, partitions: [{ partition: 0, floor: '0', end: '1' }] },
      { signal: scope, timeoutMs: 30000 }
    )
    expect(member.crashes).toEqual([])
    await bounded(scope, 15000, () => member.consumer.disconnect({ timeout: 8000 }))
    f.observe({
      group,
      topicId: metadata.topicId,
      quietMs: SETTLE_MS,
      source: {
        topic: source,
        identity: shareRecordIdentity({ topic: source, partition: 0, offset: '0', key, value }),
        key: key.toString('hex'),
        value: value.toString('hex'),
        headers: { 'source-only': Buffer.from('must-not-copy').toString('hex') },
      },
      deliveryCounts: deliveries.map(({ deliveryCount }) => deliveryCount),
      identities: deliveries.map(({ identity }) => identity),
      records: dead.map(({ key, value, headers }) => ({
        key: key?.toString('hex') ?? null,
        value: value?.toString('hex') ?? null,
        headers: Object.fromEntries(
          Object.entries(headers).map(([name, value]) => [name, Buffer.from(value).toString('hex')])
        ),
      })),
      crashes: member.crashes,
    })
  })
}

enabled('Share DLQ Kafka 4.4 (live)', () => {
  beforeAll(
    () =>
      bounded(NEVER, HOOK_MS - 2000, () => {
        liveRun.assertClean()
      }),
    HOOK_MS
  )

  afterEach(() => {
    if (!liveRun.takeAdmission()) return
    liveRun.assertClean()
  })

  it(
    'reject routes metadata only to the DLQ',
    liveCase(CASE_MS, (scope) =>
      runCase(scope, { scenario: 'dlq-reject-metadata', copyRecord: false, release: false })
    ),
    caseTimeout(CASE_MS)
  )

  it(
    'reject routes original key and value to the DLQ',
    liveCase(CASE_MS, (scope) =>
      runCase(scope, { scenario: 'dlq-reject-copy', copyRecord: true, release: false })
    ),
    caseTimeout(CASE_MS)
  )

  it(
    'release beyond the delivery limit routes metadata only to the DLQ',
    liveCase(CASE_MS, (scope) =>
      runCase(scope, { scenario: 'dlq-release-metadata', copyRecord: false, release: true })
    ),
    caseTimeout(CASE_MS)
  )

  it(
    'release beyond the delivery limit routes original key and value to the DLQ',
    liveCase(CASE_MS, (scope) =>
      runCase(scope, { scenario: 'dlq-release-copy', copyRecord: true, release: true })
    ),
    caseTimeout(CASE_MS)
  )
})
