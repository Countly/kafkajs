jest.mock('../event', () => {
  const InstrumentationEvent = jest.requireActual('../event')
  return jest.fn((type, payload) => new InstrumentationEvent(type, payload))
})

const path = require('path')
const { spawnSync } = require('child_process')
const diagnosticsChannel = require('node:diagnostics_channel')
const InstrumentationEvent = require('../event')
const InstrumentationEventEmitter = require('../emitter')
const channels = require('../channels')
const createProducer = require('../../producer')
const createConsumer = require('../../consumer')
const createAdmin = require('../../admin')
const { KafkaJSError } = require('../../errors')
const baseline = require('../../../testHelpers/compatBaseline/instrumentationEvents.json')
const { newLogger } = require('testHelpers')

const KINDS = ['producer', 'consumer', 'admin']

/** A real client of each kind over a stub cluster, built on the emitter it is given. */
const CLIENTS = {
  producer: (instrumentationEmitter) =>
    createProducer({ cluster: {}, logger: newLogger(), instrumentationEmitter }),
  consumer: (instrumentationEmitter) =>
    createConsumer({
      cluster: { onTopicRecreated: () => () => {} },
      groupId: 'channel-group',
      logger: newLogger(),
      instrumentationEmitter,
    }),
  admin: (instrumentationEmitter) =>
    createAdmin({ cluster: {}, logger: newLogger(), instrumentationEmitter }),
}

describe('Instrumentation > emitter > channels', () => {
  /** @type {(() => void)[]} undoes every subscription a case made */
  let unsubscribers

  /** Subscribes to a channel by name and remembers to unsubscribe. */
  const subscribe = (name, onMessage) => {
    diagnosticsChannel.subscribe(name, onMessage)
    unsubscribers.push(() => diagnosticsChannel.unsubscribe(name, onMessage))
  }

  beforeEach(() => {
    unsubscribers = []
    InstrumentationEvent.mockClear()
  })

  afterEach(() => {
    unsubscribers.forEach((unsubscribe) => unsubscribe())
    jest.restoreAllMocks()
  })

  describe.each(KINDS)('%s', (kind) => {
    test.each(baseline[kind].map(({ publicName, internalName }) => [publicName, internalName]))(
      '%s: a subscriber and a listener receive one event, whose type is the public name',
      (publicName, internalName) => {
        const emitter = new InstrumentationEventEmitter({ channels: channels[kind] })
        const client = CLIENTS[kind](emitter)
        const published = []
        const typesWhenPublished = []
        const listened = []
        subscribe(`kafkajs.${publicName}`, (message) => {
          published.push(message)
          // read before any listener runs: a listener's `on()` wrapper also rewrites `type`
          typesWhenPublished.push(message.type)
        })
        client.on(publicName, (event) => listened.push(event))
        const payload = { probe: true }

        emitter.emit(internalName, payload)

        expect(published).toHaveLength(1)
        expect(listened).toHaveLength(1)
        expect(published[0]).toBe(listened[0])
        expect(typesWhenPublished).toEqual([publicName])
        expect(published[0].payload).toBe(payload)
        expect({
          type: listened[0].type,
          keys: Object.keys(listened[0]).sort(),
        }).toEqual(baseline.listener[publicName])
        expect(InstrumentationEvent).toHaveBeenCalledTimes(1)
      }
    )
  })

  describe('allocation', () => {
    test('an event nobody listens to allocates nothing', () => {
      const emitter = new InstrumentationEventEmitter({ channels: channels.consumer })

      for (let i = 0; i < 100; i += 1) {
        emitter.emit('network.request', { probe: true })
        emitter.emit('consumer.fetch', { probe: true })
      }

      expect(InstrumentationEvent).not.toHaveBeenCalled()
    })

    test('an event only a subscriber listens to allocates one event per emit', () => {
      const emitter = new InstrumentationEventEmitter({ channels: channels.consumer })
      subscribe('kafkajs.consumer.fetch', () => {})

      emitter.emit('consumer.fetch', { probe: true })
      emitter.emit('consumer.fetch', { probe: true })

      expect(InstrumentationEvent).toHaveBeenCalledTimes(2)
    })

    test('an event only a listener listens to allocates one event per emit', () => {
      const emitter = new InstrumentationEventEmitter({ channels: channels.consumer })
      emitter.addListener('consumer.fetch', () => {})

      emitter.emit('consumer.fetch', { probe: true })

      expect(InstrumentationEvent).toHaveBeenCalledTimes(1)
    })

    test('a subscriber and a listener together share the one event', () => {
      const emitter = new InstrumentationEventEmitter({ channels: channels.consumer })
      subscribe('kafkajs.consumer.fetch', () => {})
      emitter.addListener('consumer.fetch', () => {})

      emitter.emit('consumer.fetch', { probe: true })

      expect(InstrumentationEvent).toHaveBeenCalledTimes(1)
    })

    test('a subscriber to another channel allocates nothing', () => {
      const emitter = new InstrumentationEventEmitter({ channels: channels.consumer })
      subscribe('kafkajs.consumer.crash', () => {})

      emitter.emit('consumer.fetch', { probe: true })

      expect(InstrumentationEvent).not.toHaveBeenCalled()
    })

    test('an event only a listener listens to is built under the emitted name, and a client’s on() gives its listener the public one', () => {
      // building the public name is a string per event: it is done for a published event only
      const emitter = new InstrumentationEventEmitter({ channels: channels.consumer })
      const raw = []
      emitter.addListener('network.request', (event) => raw.push(event.type))
      emitter.emit('network.request', { probe: true })
      expect(raw).toEqual(['network.request'])

      const expected = []
      const received = []
      for (const kind of KINDS) {
        for (const { publicName, internalName } of baseline[kind]) {
          const clientEmitter = new InstrumentationEventEmitter({ channels: channels[kind] })
          CLIENTS[kind](clientEmitter).on(publicName, (event) => received.push(event.type))
          clientEmitter.emit(internalName, { probe: true })
          expected.push(publicName)
        }
      }
      expect(received).toEqual(expected)
    })

    test('emit() never asks Node for a channel', () => {
      const emitter = new InstrumentationEventEmitter({ channels: channels.consumer })
      subscribe('kafkajs.consumer.fetch', () => {})
      emitter.addListener('consumer.fetch', () => {})
      const channel = jest.spyOn(diagnosticsChannel, 'channel')

      emitter.emit('consumer.fetch', { probe: true })
      emitter.emit('network.request', { probe: true })

      expect(channel).not.toHaveBeenCalled()
    })
  })

  describe('order and failure', () => {
    test('channel subscribers run before the listeners of the same event', () => {
      const emitter = new InstrumentationEventEmitter({ channels: channels.consumer })
      const order = []
      emitter.addListener('consumer.fetch', () => order.push('listener'))
      subscribe('kafkajs.consumer.fetch', () => order.push('subscriber'))

      emitter.emit('consumer.fetch', { probe: true })

      expect(order).toEqual(['subscriber', 'listener'])
    })

    test('a throwing listener cannot keep the subscribers from the event', () => {
      const emitter = new InstrumentationEventEmitter({ channels: channels.consumer })
      const received = []
      emitter.addListener('consumer.fetch', () => {
        throw new Error('listener failed')
      })
      subscribe('kafkajs.consumer.fetch', (message) => received.push(message))

      expect(() => emitter.emit('consumer.fetch', { probe: true })).toThrow('listener failed')

      expect(received).toHaveLength(1)
    })

    test('a throwing subscriber does not throw out of emit(), and the others still run', () => {
      // Node reports a subscriber's error as an uncaught exception, which Jest's own handler
      // would turn into a failure: the case runs in a child process that installs its own.
      const script = `
        const diagnosticsChannel = require('node:diagnostics_channel')
        const channels = require(${JSON.stringify(path.join(__dirname, '..', 'channels'))})
        const Emitter = require(${JSON.stringify(path.join(__dirname, '..', 'emitter'))})
        process.on('uncaughtException', (error) => console.log('uncaught: ' + error.message))
        diagnosticsChannel.subscribe('kafkajs.producer.connect', () => {
          throw new Error('subscriber failed')
        })
        diagnosticsChannel.subscribe('kafkajs.producer.connect', () => console.log('second ran'))
        const emitter = new Emitter({ channels: channels.producer })
        emitter.addListener('producer.connect', () => console.log('listener ran'))
        emitter.emit('producer.connect')
        console.log('returned')
      `

      const child = spawnSync(process.execPath, ['-e', script], {
        encoding: 'utf8',
        timeout: 20000,
      })

      expect(child.stderr).toBe('')
      expect(child.stdout.trim().split('\n')).toEqual([
        'second ran',
        'listener ran',
        'returned',
        'uncaught: subscriber failed',
      ])
      expect(child.status).toBe(0)
    })
  })

  describe('an emitter without channels', () => {
    test('publishes nothing and gives its listeners the name it was given', () => {
      const emitter = new InstrumentationEventEmitter()
      const published = []
      const listened = []
      subscribe('kafkajs.producer.connect', (message) => published.push(message))
      emitter.addListener('producer.connect', (event) => listened.push(event))

      emitter.emit('producer.connect')

      expect(published).toEqual([])
      expect(listened).toHaveLength(1)
      expect(listened[0].type).toBe('producer.connect')
    })
  })

  test('stops delivering to a listener once the function addListener returned is called', () => {
    const emitter = new InstrumentationEventEmitter({ channels: channels.producer })
    const listener = jest.fn()
    const remove = emitter.addListener('producer.connect', listener)

    emitter.emit('producer.connect')
    remove()
    emitter.emit('producer.connect')

    expect(listener).toHaveBeenCalledTimes(1)
  })

  test('refuses an empty event name', () => {
    const emitter = new InstrumentationEventEmitter({ channels: channels.producer })

    expect(() => emitter.emit('')).toThrow(KafkaJSError)
  })
})
