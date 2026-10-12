jest.mock('../../producer')
jest.mock('../../consumer')
jest.mock('../../admin')
jest.mock('../../cluster')

const { Kafka } = require('../../../index')
const createProducer = require('../../producer')
const createConsumer = require('../../consumer')
const createAdmin = require('../../admin')
const Cluster = require('../../cluster')
const InstrumentationEventEmitter = require('../emitter')
const channels = require('../channels')

describe('Instrumentation > the clients a Kafka instance builds', () => {
  let kafka

  beforeEach(() => {
    jest.clearAllMocks()
    kafka = new Kafka({ brokers: [] })
  })

  test.each([
    ['producer', () => kafka.producer(), createProducer],
    ['consumer', () => kafka.consumer({ groupId: 'channel-group' }), createConsumer],
    ['admin', () => kafka.admin(), createAdmin],
  ])('a %s publishes on its own kind of channels', (kind, build, factory) => {
    build()

    const { instrumentationEmitter } = factory.mock.calls[0][0]
    expect(instrumentationEmitter).toBeInstanceOf(InstrumentationEventEmitter)
    expect(instrumentationEmitter.channels).toBe(channels[kind])
  })

  test.each([
    ['producer', () => kafka.producer(), createProducer],
    ['consumer', () => kafka.consumer({ groupId: 'channel-group' }), createConsumer],
    ['admin', () => kafka.admin(), createAdmin],
  ])(
    'a %s and its cluster share one emitter, so the network events reach the channels',
    (_, build, factory) => {
      build()

      expect(Cluster.mock.calls[0][0].instrumentationEmitter).toBe(
        factory.mock.calls[0][0].instrumentationEmitter
      )
    }
  )

  test('each client has an emitter of its own', () => {
    kafka.producer()
    kafka.producer()

    const [first, second] = createProducer.mock.calls.map(
      ([options]) => options.instrumentationEmitter
    )
    expect(first).not.toBe(second)
  })
})
