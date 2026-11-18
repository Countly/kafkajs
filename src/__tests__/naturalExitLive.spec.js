const { Kafka, logLevel } = require('../../index')
const { plainTextBrokers, secureRandom, writeEvidence } = require('testHelpers')

// Run only as the child of `node scripts/ci/naturalExit.js ordinary`: the parent proves this
// process exits on its own after the last case's cleanup, which no case can assert itself.
const LIVE = process.env.NATURAL_EXIT_E2E === '1'
const CONTROL = process.env.NATURAL_EXIT_CONTROL === '1'
const describeOrdinary = LIVE && !CONTROL ? describe : describe.skip
const describeControl = LIVE && CONTROL ? describe : describe.skip

const EVIDENCE = CONTROL ? 'natural-exit-ordinary-control' : 'natural-exit-ordinary'
const CONSUMER_CASE_MS = 60000
const CLIENT_CASE_MS = 30000
const CONTROL_CASE_MS = 20000
const STEP_MS = 15000
const DELIVERY_MS = 30000

/**
 * Settles with `promise`, or rejects once `ms` pass; the timer is always cleared, so the bound
 * itself never holds the process open.
 * @template T
 * @param {Promise<T>} promise Step to bound.
 * @param {number} ms Bound in milliseconds.
 * @param {string} what Step name for the timeout error.
 * @returns {Promise<T>} The step's result.
 * @throws {Error} When the step rejects or outlasts its bound.
 */
const within = async (promise, ms, what) => {
  let timer
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error(`${what} took over ${ms} ms`)), ms)
      }),
    ])
  } finally {
    clearTimeout(timer)
  }
}

/**
 * One case: its own clients and topic, a cleanup that disconnects every client even when the
 * work failed, and one evidence row stamped on the OS monotonic clock after that cleanup.
 */
class OrdinaryCase {
  /** @param {string} title Case title, as the parent lists it. */
  constructor(title) {
    this.title = title
    this.id = secureRandom()
    this.kafka = new Kafka({
      clientId: `natural-exit-${this.id}`,
      brokers: plainTextBrokers(),
      logLevel: logLevel.NOTHING,
    })
    this.clients = []
    this.topic = null
  }

  /**
   * Registers a client for cleanup before it connects.
   * @template T
   * @param {T} client Producer, consumer or admin.
   * @returns {T} The same client.
   */
  client(client) {
    this.clients.push(client)
    return client
  }

  /**
   * Creates the case's single-partition topic through the case admin.
   * @returns {Promise<string>} The topic name.
   */
  async createTopic() {
    this.topic = `natural-exit-${this.id}`
    const created = await within(
      this.admin.createTopics({
        topics: [{ topic: this.topic, numPartitions: 1, replicationFactor: 1 }],
        waitForLeaders: true,
      }),
      STEP_MS,
      'createTopics'
    )
    expect(created).toBe(true)
    return this.topic
  }

  /**
   * Sends one message with a plain producer of this case.
   * @param {string} topic Target topic.
   * @returns {Promise<void>}
   */
  async produceOne(topic) {
    const producer = this.client(this.kafka.producer())
    await within(producer.connect(), STEP_MS, 'producer connect')
    await within(producer.send({ topic, messages: [{ value: 'one' }] }), STEP_MS, 'send')
  }

  /**
   * Runs `act` with a connected case admin, then cleans up, then writes the case row.
   * @param {(fixture: OrdinaryCase) => Promise<void>} act The case's unit of work.
   * @returns {Promise<void>}
   * @throws {Error} The work's failure first, else the cleanup's.
   */
  async run(act) {
    let failure
    try {
      this.admin = this.client(this.kafka.admin())
      await within(this.admin.connect(), STEP_MS, 'admin connect')
      await act(this)
    } catch (error) {
      failure = error
    }
    try {
      await this.close()
    } catch (error) {
      failure ??= error
    }
    if (failure) {
      this.note('case-failed', { error: String(failure?.stack ?? failure).slice(0, 4000) })
      throw failure
    }
    this.note('case-complete', {
      status: 'passed',
      tainted: false,
      cleanup: { completedAt: Number(process.hrtime.bigint() / 1000000n) },
    })
  }

  /**
   * Deletes the topic, then disconnects every client, newest first; one failure never skips
   * a later disconnect.
   * @returns {Promise<void>}
   * @throws {AggregateError} When any step failed.
   */
  async close() {
    const errors = []
    if (this.topic && this.admin) {
      await within(
        this.admin.deleteTopics({ topics: [this.topic] }),
        STEP_MS,
        'deleteTopics'
      ).catch((error) => errors.push(error))
    }
    for (const client of this.clients.reverse()) {
      await within(client.disconnect(), STEP_MS, 'disconnect').catch((error) => errors.push(error))
    }
    if (errors.length) throw new AggregateError(errors, `${this.title}: cleanup failed`)
  }

  /**
   * Appends one evidence row for this case.
   * @param {string} event Row kind.
   * @param {object} fields Row fields.
   * @returns {void}
   */
  note(event, fields) {
    writeEvidence(EVIDENCE, { event, case: this.title, nonce: this.id, ...fields })
  }
}

/**
 * Consumes the case's one message with a fresh consumer, then disconnects it.
 * @param {OrdinaryCase} fixture The case.
 * @param {object} options Consumer options beyond the group id.
 * @returns {Promise<string>} The group id.
 */
const consumeOne = async (fixture, options) => {
  const topic = await fixture.createTopic()
  await fixture.produceOne(topic)
  const groupId = `natural-exit-group-${fixture.id}`
  const consumer = fixture.client(
    fixture.kafka.consumer({ groupId, maxWaitTimeInMs: 100, ...options })
  )
  await within(consumer.connect(), STEP_MS, 'consumer connect')
  await within(consumer.subscribe({ topic, fromBeginning: true }), STEP_MS, 'subscribe')
  const received = new Promise((resolve, reject) => {
    within(
      consumer.run({ eachMessage: async ({ message }) => resolve(message.value.toString()) }),
      STEP_MS,
      'run'
    ).catch(reject)
  })
  expect(await within(received, DELIVERY_MS, 'delivery')).toBe('one')
  await within(consumer.disconnect(), STEP_MS, 'consumer disconnect')
  return groupId
}

/**
 * @param {string} title Case title.
 * @param {(fixture: OrdinaryCase) => Promise<void>} act The case's unit of work.
 * @returns {() => Promise<void>} The Jest case body.
 */
const ordinaryCase = (title, act) => () => new OrdinaryCase(title).run(act)

describeOrdinary('Natural exit > ordinary clients', () => {
  const classicDynamic = 'classic dynamic consumer consumes one message and disconnects'
  it(
    classicDynamic,
    ordinaryCase(classicDynamic, (fixture) => consumeOne(fixture, {})),
    CONSUMER_CASE_MS
  )

  const classicStatic = 'classic static consumer keeps its group slot after disconnect'
  it(
    classicStatic,
    ordinaryCase(classicStatic, async (fixture) => {
      const groupId = await consumeOne(fixture, {
        groupInstanceId: `natural-exit-instance-${fixture.id}`,
        sessionTimeout: 30000,
      })
      // A static member sends no LeaveGroup: it keeps its slot until the session timeout.
      const { groups } = await within(fixture.admin.describeGroups([groupId]), STEP_MS, 'describe')
      expect(groups).toHaveLength(1)
      expect(groups[0].members).toHaveLength(1)
      expect(groups[0].state).not.toBe('Empty')
    }),
    CONSUMER_CASE_MS
  )

  const consumerDynamic = 'consumer-protocol dynamic consumer consumes one message and disconnects'
  it(
    consumerDynamic,
    ordinaryCase(consumerDynamic, (fixture) => consumeOne(fixture, { groupProtocol: 'consumer' })),
    CONSUMER_CASE_MS
  )

  const consumerStatic = 'consumer-protocol static consumer consumes one message and disconnects'
  it(
    consumerStatic,
    ordinaryCase(consumerStatic, (fixture) =>
      consumeOne(fixture, {
        groupProtocol: 'consumer',
        groupInstanceId: `natural-exit-instance-${fixture.id}`,
      })
    ),
    CONSUMER_CASE_MS
  )

  const idempotent = 'idempotent producer sends one message and disconnects'
  it(
    idempotent,
    ordinaryCase(idempotent, async (fixture) => {
      const topic = await fixture.createTopic()
      const producer = fixture.client(fixture.kafka.producer({ idempotent: true }))
      await within(producer.connect(), STEP_MS, 'producer connect')
      const sent = await within(
        producer.send({ topic, messages: [{ value: 'one' }] }),
        STEP_MS,
        'send'
      )
      expect(sent).toEqual([
        expect.objectContaining({ topicName: topic, partition: 0, errorCode: 0, baseOffset: '0' }),
      ])
      await within(producer.disconnect(), STEP_MS, 'producer disconnect')
    }),
    CLIENT_CASE_MS
  )

  const transactional = 'transactional producer commits one message and disconnects'
  it(
    transactional,
    ordinaryCase(transactional, async (fixture) => {
      const topic = await fixture.createTopic()
      const producer = fixture.client(
        fixture.kafka.producer({ transactionalId: `natural-exit-txn-${fixture.id}` })
      )
      await within(producer.connect(), STEP_MS, 'producer connect')
      const transaction = await within(producer.transaction(), STEP_MS, 'begin')
      const sent = await within(
        transaction.send({ topic, messages: [{ value: 'one' }] }),
        STEP_MS,
        'send'
      )
      expect(sent).toEqual([
        expect.objectContaining({ topicName: topic, partition: 0, errorCode: 0, baseOffset: '0' }),
      ])
      await within(transaction.commit(), STEP_MS, 'commit')
      expect(transaction.isActive()).toBe(false)
      await within(producer.disconnect(), STEP_MS, 'producer disconnect')
    }),
    CLIENT_CASE_MS
  )

  const admin = 'admin creates and describes a topic and disconnects'
  it(
    admin,
    ordinaryCase(admin, async (fixture) => {
      const topic = await fixture.createTopic()
      const { topics } = await within(
        fixture.admin.fetchTopicMetadata({ topics: [topic] }),
        STEP_MS,
        'fetchTopicMetadata'
      )
      expect(topics.map(({ name, partitions }) => [name, partitions.length])).toEqual([[topic, 1]])
    }),
    CLIENT_CASE_MS
  )
})

describeControl('Natural exit > retained timer control', () => {
  const title = 'leaves one interval alive after its producer disconnects'
  it(
    title,
    async () => {
      await new OrdinaryCase(title).run(async (fixture) => {
        const producer = fixture.client(fixture.kafka.producer())
        await within(producer.connect(), STEP_MS, 'producer connect')
      })
      // The seeded retained handle: the parent must see this process fail to exit.
      setInterval(() => {}, 1000)
    },
    CONTROL_CASE_MS
  )
})
