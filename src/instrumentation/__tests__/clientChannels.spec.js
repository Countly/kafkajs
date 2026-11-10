jest.mock('../../producer')
jest.mock('../../consumer')
jest.mock('../../admin')
jest.mock('../../consumer/share', () => ({
  ...jest.requireActual('../../consumer/share'),
  createShareConsumer: jest.fn(),
}))
jest.mock('../../cluster')

const { Kafka } = require('../../../index')
const createProducer = require('../../producer')
const createConsumer = require('../../consumer')
const createAdmin = require('../../admin')
const { createShareConsumer } = require('../../consumer/share')
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
    [
      'shareConsumer',
      () => kafka.shareConsumer({ groupId: 'share-channel-group' }),
      createShareConsumer,
    ],
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
    [
      'shareConsumer',
      () => kafka.shareConsumer({ groupId: 'share-channel-group' }),
      createShareConsumer,
    ],
  ])(
    'a %s and its cluster share one emitter, so the network events reach the channels',
    (_, build, factory) => {
      build()

      expect(Cluster.mock.calls[0][0].instrumentationEmitter).toBe(
        factory.mock.calls[0][0].instrumentationEmitter
      )
    }
  )

  /** One row per client kind: how to build it and the factory that receives its emitter. */
  const KINDS = [
    ['producer', () => kafka.producer(), () => createProducer],
    ['consumer', () => kafka.consumer({ groupId: 'channel-group' }), () => createConsumer],
    ['admin', () => kafka.admin(), () => createAdmin],
    [
      'shareConsumer',
      () => kafka.shareConsumer({ groupId: 'share-channel-group' }),
      () => createShareConsumer,
    ],
  ]

  test.each(KINDS)(
    'two %s clients each have an emitter and a cluster of their own',
    (kind, build, factory) => {
      build()
      build()

      const [first, second] = factory().mock.calls.map(
        ([options]) => options.instrumentationEmitter
      )
      expect(first).not.toBe(second)
      expect(first.channels).toBe(channels[kind])
      expect(second.channels).toBe(channels[kind])
      expect(Cluster.mock.calls).toHaveLength(2)
      expect(Cluster.mock.calls[0][0].instrumentationEmitter).toBe(first)
      expect(Cluster.mock.calls[1][0].instrumentationEmitter).toBe(second)
      expect(Cluster.mock.instances[0]).not.toBe(Cluster.mock.instances[1])
    }
  )

  test('clients of different kinds never share an emitter or a cluster', () => {
    for (const [, build] of KINDS) build()

    const emitters = KINDS.map(([, , factory]) => factory().mock.calls[0][0].instrumentationEmitter)
    expect(new Set(emitters).size).toBe(KINDS.length)
    expect(new Set(Cluster.mock.instances).size).toBe(KINDS.length)
    expect(new Set(Cluster.mock.calls.map(([options]) => options.instrumentationEmitter))).toEqual(
      new Set(emitters)
    )
  })
})
