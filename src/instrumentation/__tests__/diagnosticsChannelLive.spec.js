const diagnosticsChannel = require('node:diagnostics_channel')
const { Kafka, logLevel } = require('../../../index')
const { createTopic, plainTextBrokers, secureRandom, writeEvidence } = require('testHelpers')
const { waitForCondition, withTimeout } = require('../../consumer/__tests__/fixtures/liveScenario')
const { runRecipe } = require('./fixtures/metricsRecipe')

// Gated: it needs a cluster. Run alone: `npm run test:e2e:diagnostics-channel`.
const describeChannelsLive = process.env.DIAGNOSTICS_CHANNEL_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'diagnostics-channel'
// Named so each wait says how long it is prepared to be wrong for.
const SCENARIO_BUDGET_MS = 60000
const DISCONNECT_BUDGET_MS = 15000
const WAIT_BUDGET = { attempts: 120, delay: 250 } // 30 s
const MESSAGES = 20
const CHANNELS = [
  'kafkajs.producer.network.request',
  'kafkajs.consumer.fetch',
  'kafkajs.consumer.end_batch_process',
]

/**
 * Live proof that the instrumentation events reach `node:diagnostics_channel` subscribers from real
 * clients through a produce and consume round trip, with the public event name as their type, and
 * that the documented prom-client recipe, run against a fake registry, counts the consumed
 * messages exactly.
 */
describeChannelsLive('Instrumentation > diagnostics channels (live)', () => {
  /** @type {{ channel: string, type: string, payload: object }[]} */
  const published = []
  const clients = []
  let recipe

  /** Records what a channel delivers, with the type as it is when it is published. */
  const record = (channel) => (message) =>
    published.push({ channel, type: message.type, payload: message.payload })
  const subscribers = CHANNELS.map((channel) => [channel, record(channel)])

  beforeAll(() => {
    recipe = runRecipe()
    for (const [channel, onMessage] of subscribers) {
      diagnosticsChannel.subscribe(channel, onMessage)
    }
  })

  afterAll(async () => {
    for (const [channel, onMessage] of subscribers) {
      diagnosticsChannel.unsubscribe(channel, onMessage)
    }
    recipe.stop()
    for (const client of clients.reverse()) {
      await withTimeout(
        client.disconnect().catch(() => {}),
        DISCONNECT_BUDGET_MS
      )
    }
  }, SCENARIO_BUDGET_MS)

  test(
    'a round trip reaches the channels with public event names, and the recipe counts what was consumed',
    async () => {
      const topic = `e2e-p8-diagnostics-channel-${secureRandom()}`
      await createTopic({ topic, partitions: 3 })
      const kafka = new Kafka({
        clientId: `e2e-p8-diagnostics-channel-${secureRandom()}`,
        brokers: plainTextBrokers(),
        logLevel: logLevel.NOTHING,
      })
      const producer = kafka.producer()
      const consumer = kafka.consumer({ groupId: `e2e-p8-diagnostics-channel-${secureRandom()}` })
      clients.push(producer, consumer)
      let consumed = 0

      await producer.connect()
      await producer.send({
        topic,
        messages: Array.from({ length: MESSAGES }, (_, index) => ({ value: `message-${index}` })),
      })
      await consumer.connect()
      await consumer.subscribe({ topics: [topic], fromBeginning: true })
      await consumer.run({
        eachMessage: async () => {
          consumed += 1
        },
      })
      await waitForCondition(() => consumed >= MESSAGES, WAIT_BUDGET)
      const total = () =>
        recipe
          .metric('kafkajs_consumer_messages_total')
          .samples.reduce((sum, { value }) => sum + value, 0)
      await waitForCondition(() => total() >= MESSAGES, WAIT_BUDGET)

      const on = (channel) => published.filter((event) => event.channel === channel)
      writeEvidence(EVIDENCE, {
        scenario: 'round-trip',
        events: Object.fromEntries(CHANNELS.map((channel) => [channel, on(channel).length])),
        producerApis: [...new Set(on(CHANNELS[0]).map(({ payload }) => payload.apiName))],
        consumed,
        recipeMessages: total(),
      })

      for (const channel of CHANNELS) {
        expect(on(channel).length).toBeGreaterThan(0)
        expect(on(channel).every(({ type }) => type === channel.replace('kafkajs.', ''))).toBe(true)
      }
      expect(on(CHANNELS[0]).map(({ payload }) => payload.apiName)).toContain('Produce')
      expect(on(CHANNELS[1]).every(({ payload }) => typeof payload.duration === 'number')).toBe(
        true
      )
      expect(consumed).toBe(MESSAGES)
      expect(total()).toBe(MESSAGES)
      expect(
        recipe
          .metric('kafkajs_requests_total')
          .samples.some(({ labels }) => labels.kind === 'producer' && labels.api === 'Produce')
      ).toBe(true)
    },
    SCENARIO_BUDGET_MS
  )
})
