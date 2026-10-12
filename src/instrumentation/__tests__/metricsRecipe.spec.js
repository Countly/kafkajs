const diagnosticsChannel = require('node:diagnostics_channel')
const InstrumentationEventEmitter = require('../emitter')
const channels = require('../channels')
const { runRecipe, recipeSource } = require('./fixtures/metricsRecipe')

describe('Instrumentation > the prom-client recipe', () => {
  let recipe
  const emitters = {
    producer: new InstrumentationEventEmitter({ channels: channels.producer }),
    consumer: new InstrumentationEventEmitter({ channels: channels.consumer }),
    admin: new InstrumentationEventEmitter({ channels: channels.admin }),
  }

  /** The `network.request` payload of a request, with the fields a metrics layer must not label by. */
  const request = ({ clientId = 'app', apiName = 'Produce', duration = 250, size = 100 } = {}) => ({
    broker: 'localhost:9092',
    clientId,
    correlationId: 7,
    size,
    createdAt: 1,
    sentAt: 2,
    pendingDuration: 1,
    duration,
    apiName,
    apiKey: 0,
    apiVersion: 9,
  })

  const batch = ({ topic = 'orders', batchSize = 3, duration = 40 } = {}) => ({
    topic,
    partition: 5,
    highWatermark: '10',
    offsetLag: '1',
    offsetLagLow: '0',
    batchSize,
    firstOffset: '7',
    lastOffset: '9',
    duration,
  })

  beforeEach(() => {
    recipe = runRecipe()
  })

  afterEach(() => {
    recipe.stop()
  })

  test('is the first javascript block under its heading, and uses only prom-client and the channels', () => {
    const source = recipeSource()

    expect(source).toContain("require('prom-client')")
    expect(source).toContain("require('node:diagnostics_channel')")
    expect(recipe.metrics.map(({ config }) => config.name)).toEqual([
      'kafkajs_requests_total',
      'kafkajs_request_duration_seconds',
      'kafkajs_consumer_messages_total',
      'kafkajs_consumer_batch_duration_seconds',
      'kafkajs_consumer_commits_total',
      'kafkajs_consumer_group_joins_total',
      'kafkajs_consumer_group_join_duration_seconds',
      'kafkajs_consumer_crashes_total',
    ])
  })

  test.each(['producer', 'consumer', 'admin'])(
    'counts a %s request by API and observes its duration in seconds',
    (kind) => {
      emitters[kind].emit('network.request', request({ apiName: 'Metadata', duration: 250 }))
      emitters[kind].emit('network.request', request({ apiName: 'Metadata', duration: 1500 }))

      const labels = { kind, client_id: 'app', api: 'Metadata' }
      expect(recipe.metric('kafkajs_requests_total').samples).toEqual([
        { labels, value: 1 },
        { labels, value: 1 },
      ])
      expect(recipe.metric('kafkajs_request_duration_seconds').samples).toEqual([
        { labels, value: 0.25 },
        { labels, value: 1.5 },
      ])
    }
  )

  test('a Produce with acks: 0 is counted with the size 0 and duration 0 it is reported with', () => {
    emitters.producer.emit('network.request', request({ size: 0, duration: 0 }))

    expect(recipe.metric('kafkajs_requests_total').samples).toHaveLength(1)
    expect(recipe.metric('kafkajs_request_duration_seconds').samples[0].value).toBe(0)
  })

  test('two clients of one process stay apart by clientId', () => {
    emitters.producer.emit('network.request', request({ clientId: 'checkout' }))
    emitters.producer.emit('network.request', request({ clientId: 'billing' }))

    expect(
      recipe.metric('kafkajs_requests_total').samples.map(({ labels }) => labels.client_id)
    ).toEqual(['checkout', 'billing'])
  })

  test('counts the messages of each batch per topic and observes the handler time in seconds', () => {
    emitters.consumer.emit('consumer.end_batch_process', batch({ topic: 'orders', batchSize: 3 }))
    emitters.consumer.emit('consumer.end_batch_process', batch({ topic: 'audit', batchSize: 5 }))

    expect(recipe.metric('kafkajs_consumer_messages_total').samples).toEqual([
      { labels: { topic: 'orders' }, value: 3 },
      { labels: { topic: 'audit' }, value: 5 },
    ])
    expect(recipe.metric('kafkajs_consumer_batch_duration_seconds').samples).toEqual([
      { labels: { topic: 'orders' }, value: 0.04 },
      { labels: { topic: 'audit' }, value: 0.04 },
    ])
  })

  test('counts offset commits', () => {
    emitters.consumer.emit('consumer.commit_offsets', { groupId: 'g', topics: [] })
    emitters.consumer.emit('consumer.commit_offsets', { groupId: 'g', topics: [] })

    expect(recipe.metric('kafkajs_consumer_commits_total').samples).toEqual([
      { labels: {}, value: 1 },
      { labels: {}, value: 1 },
    ])
  })

  test('counts group joins and their duration by membership protocol', () => {
    emitters.consumer.emit('consumer.group_join', {
      groupId: 'g',
      memberId: 'm-1',
      membershipProtocol: 'classic',
      duration: 2000,
    })
    emitters.consumer.emit('consumer.group_join', {
      groupId: 'g',
      memberId: 'm-2',
      membershipProtocol: 'consumer',
      duration: 120,
    })

    expect(recipe.metric('kafkajs_consumer_group_joins_total').samples).toEqual([
      { labels: { protocol: 'classic' }, value: 1 },
      { labels: { protocol: 'consumer' }, value: 1 },
    ])
    expect(recipe.metric('kafkajs_consumer_group_join_duration_seconds').samples).toEqual([
      { labels: { protocol: 'classic' }, value: 2 },
      { labels: { protocol: 'consumer' }, value: 0.12 },
    ])
  })

  test.each([true, false])('counts a crash that restarts: %s', (restart) => {
    emitters.consumer.emit('consumer.crash', { error: new Error('x'), groupId: 'g', restart })

    expect(recipe.metric('kafkajs_consumer_crashes_total').samples).toEqual([
      { labels: { restart: String(restart) }, value: 1 },
    ])
  })

  test('never labels by anything unbounded', () => {
    emitters.producer.emit('network.request', request())
    emitters.consumer.emit('consumer.end_batch_process', batch())
    emitters.consumer.emit('consumer.group_join', {
      memberId: 'm',
      membershipProtocol: 'classic',
      duration: 1,
    })
    const labelNames = recipe.metrics.flatMap(({ samples }) =>
      samples.flatMap(({ labels }) => Object.keys(labels))
    )

    expect(labelNames.length).toBeGreaterThan(0)
    for (const name of [
      'correlation_id',
      'correlationId',
      'member_id',
      'memberId',
      'partition',
      'broker',
    ]) {
      expect(labelNames).not.toContain(name)
    }
  })

  test('ignores the events it has no metric for', () => {
    emitters.consumer.emit('consumer.fetch', { numberOfBatches: 1, duration: 5 })
    emitters.consumer.emit('consumer.heartbeat', { groupId: 'g' })
    emitters.producer.emit('producer.connect')

    expect(recipe.metrics.flatMap(({ samples }) => samples)).toEqual([])
  })

  test('stops listening once its subscriptions are taken back', () => {
    recipe.stop()

    emitters.producer.emit('network.request', request())

    expect(recipe.metric('kafkajs_requests_total').samples).toEqual([])
  })

  test('leaves no channel with a subscriber once it is stopped', () => {
    const recipeChannels = [
      'kafkajs.producer.network.request',
      'kafkajs.consumer.network.request',
      'kafkajs.admin.network.request',
      'kafkajs.consumer.end_batch_process',
      'kafkajs.consumer.commit_offsets',
      'kafkajs.consumer.group_join',
      'kafkajs.consumer.crash',
    ]
    expect(recipeChannels.every((name) => diagnosticsChannel.hasSubscribers(name))).toBe(true)

    recipe.stop()

    expect(recipeChannels.filter((name) => diagnosticsChannel.hasSubscribers(name))).toEqual([])
  })
})
