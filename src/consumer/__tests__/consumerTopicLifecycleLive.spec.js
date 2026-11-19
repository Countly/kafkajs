const { Kafka, logLevel } = require('../../../index')
const { plainTextBrokers, secureRandom, writeEvidence } = require('testHelpers')

// Gated: needs CONSUMER_TOPIC_LIFECYCLE_E2E=1 and the compose cluster (docker-compose.yml). Its
// brokers allow automatic creation, so every client here asks for none.
const describeLifecycle =
  process.env.CONSUMER_TOPIC_LIFECYCLE_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'consumer-topic-lifecycle'
const CASE_MS = 120000
const UNAVAILABLE_WARN =
  'Subscribed topic unavailable; continuing healthy topics and retrying metadata'

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

/**
 * Polls `predicate` every 250 ms for at most 30 s.
 * @param {() => boolean|Promise<boolean>} predicate
 * @param {string} what named in the failure
 * @returns {Promise<void>}
 * @throws {Error} when the predicate never held
 */
const until = async (predicate, what) => {
  for (let attempt = 0; attempt < 120; attempt++) {
    if (await predicate()) return
    await sleep(250)
  }
  throw new Error(`${what} did not happen`)
}

/**
 * A consumer that keeps consuming a healthy topic while another of its topics is deleted, reports
 * the deleted one, and reads it again from its new beginning once it is recreated, with no crash
 * and no restart. Every client asks the brokers not to create topics automatically.
 */
describeLifecycle('Consumer > a subscribed topic deleted and recreated (live)', () => {
  let kafka, admin, producer
  const cleanup = []

  beforeEach(async () => {
    const warnings = []
    kafka = new Kafka({
      clientId: `lifecycle-${secureRandom()}`,
      brokers: plainTextBrokers(),
      logLevel: logLevel.WARN,
      logCreator:
        () =>
        ({ level, log }) => {
          if (level === logLevel.WARN && log.message === UNAVAILABLE_WARN) warnings.push(log)
        },
    })
    kafka.warnings = warnings
    admin = kafka.admin()
    producer = kafka.producer({ allowAutoTopicCreation: false })
    await admin.connect()
    await producer.connect()
  })

  afterEach(async () => {
    for (const step of cleanup.splice(0).reverse()) await step().catch(() => {})
    await producer.disconnect().catch(() => {})
    await admin.disconnect().catch(() => {})
  })

  /** @param {string} topic @returns {Promise<void>} once the broker serves the topic */
  const create = async (topic) => {
    await until(async () => {
      try {
        await admin.createTopics({
          waitForLeaders: true,
          topics: [{ topic, numPartitions: 1, replicationFactor: 1 }],
        })
        return (await admin.listTopics()).includes(topic)
      } catch (error) {
        if (error.type === 'TOPIC_ALREADY_EXISTS') return false
        throw error
      }
    }, `creating ${topic}`)
  }

  /** @param {string} topic @param {string} prefix @param {number} count */
  const produce = (topic, prefix, count) =>
    producer.send({
      topic,
      messages: Array.from({ length: count }, (_, index) => ({ value: `${prefix}-${index}` })),
    })

  /**
   * @param {'classic'|'consumer'|'manual'} mode
   * @returns {Promise<void>}
   */
  const lifecycle = async (mode) => {
    const healthy = `lifecycle-healthy-${secureRandom()}`
    const doomed = `lifecycle-doomed-${secureRandom()}`
    const groupId = `lifecycle-${mode}-${secureRandom()}`
    cleanup.push(() => admin.deleteTopics({ topics: [healthy, doomed] }))
    await create(healthy)
    await create(doomed)
    await produce(healthy, 'before', 3)
    await produce(doomed, 'old', 3)

    const consumer = kafka.consumer({
      groupId,
      groupProtocol: mode === 'consumer' ? 'consumer' : 'classic',
      allowAutoTopicCreation: false,
      metadataMaxAge: 1000,
      maxWaitTimeInMs: 100,
      retry: { retries: 3, restartOnFailure: async () => false },
    })
    cleanup.push(() => consumer.disconnect())
    const observed = { records: [], statuses: [], crashes: [] }
    consumer.on(consumer.events.TOPIC_METADATA_STATUS, ({ payload }) =>
      observed.statuses.push(payload)
    )
    consumer.on(consumer.events.CRASH, ({ payload }) => observed.crashes.push(payload))
    await consumer.connect()
    if (mode === 'manual') {
      await consumer.assign(
        [
          { topic: healthy, partitions: [0] },
          { topic: doomed, partitions: [0] },
        ],
        { offsetReset: 'earliest' }
      )
    } else {
      await consumer.subscribe({ topics: [healthy, doomed], fromBeginning: true })
    }
    await consumer.run({
      eachMessage: async ({ topic, message }) => {
        observed.records.push({ topic, offset: message.offset, value: message.value.toString() })
      },
    })
    const values = (topic) =>
      observed.records.filter((record) => record.topic === topic).map(({ value }) => value)

    await until(
      () => values(healthy).length === 3 && values(doomed).length === 3,
      'the first records of both topics'
    )

    await admin.deleteTopics({ topics: [doomed] })
    await until(async () => !(await admin.listTopics()).includes(doomed), 'the deletion')
    await produce(healthy, 'during', 3)
    await until(() => values(healthy).length === 6, 'healthy records while the topic is gone')
    await until(() => observed.statuses.length === 1, 'the unavailable status')
    expect(await admin.listTopics()).not.toContain(doomed)

    const recreatedAt = observed.records.length
    await create(doomed)
    await produce(doomed, 'new', 2)
    await until(
      () => observed.records.slice(recreatedAt).filter(({ topic }) => topic === doomed).length >= 2,
      'records from the recreated topic'
    )
    await until(() => observed.statuses.length === 2, 'the available status')

    const recreated = observed.records.slice(recreatedAt).filter(({ topic }) => topic === doomed)
    const evidence = {
      mode,
      statuses: observed.statuses,
      crashes: observed.crashes.length,
      warnings: kafka.warnings.length,
      recreated,
    }
    writeEvidence(EVIDENCE, evidence)
    expect(observed.crashes).toEqual([])
    expect(observed.statuses).toEqual([
      {
        groupId,
        topic: doomed,
        status: 'unavailable',
        type: 'UNKNOWN_TOPIC_OR_PARTITION',
        code: 3,
      },
      { groupId, topic: doomed, status: 'available', type: null, code: 0 },
    ])
    expect(kafka.warnings).toEqual([expect.objectContaining({ groupId, topic: doomed, code: 3 })])
    expect(recreated.slice(0, 2)).toEqual([
      { topic: doomed, offset: '0', value: 'new-0' },
      { topic: doomed, offset: '1', value: 'new-1' },
    ])
    expect(values(healthy)).toEqual([
      'before-0',
      'before-1',
      'before-2',
      'during-0',
      'during-1',
      'during-2',
    ])
  }

  test(
    'classic: the healthy topic keeps delivering and the recreated one is read from its start',
    () => lifecycle('classic'),
    CASE_MS
  )

  test(
    'consumer: the healthy topic keeps delivering and the recreated one is read from its start',
    () => lifecycle('consumer'),
    CASE_MS
  )

  test(
    'manual: the healthy topic keeps delivering and the recreated one is read from its start',
    () => lifecycle('manual'),
    CASE_MS
  )
})
