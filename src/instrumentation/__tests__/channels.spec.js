const diagnosticsChannel = require('node:diagnostics_channel')
const channels = require('../channels')
const baseline = require('../../../testHelpers/compatBaseline/instrumentationEvents.json')

const KINDS = ['producer', 'consumer', 'admin']

describe('Instrumentation > channels', () => {
  test.each(KINDS)('%s: one channel per public event, keyed by the internal name', (kind) => {
    const table = channels[kind]

    expect(Object.keys(table).sort()).toEqual(
      baseline[kind].map(({ internalName }) => internalName).sort()
    )
    for (const { publicName, internalName } of baseline[kind]) {
      expect(table[internalName].name).toBe(`kafkajs.${publicName}`)
    }
  })

  test('forty-two channels, each with a distinct name', () => {
    const names = KINDS.flatMap((kind) => Object.values(channels[kind]).map(({ name }) => name))

    expect(names).toHaveLength(42)
    expect(new Set(names).size).toBe(42)
  })

  test('the channel of a public name is the one Node hands to anybody who asks for it by name', () => {
    for (const kind of KINDS) {
      for (const channel of Object.values(channels[kind])) {
        expect(diagnosticsChannel.channel(channel.name)).toBe(channel)
      }
    }
  })

  test('a subscription made by name reaches the table’s channel', () => {
    const received = []
    const onMessage = (message) => received.push(message)
    diagnosticsChannel.subscribe('kafkajs.producer.connect', onMessage)

    try {
      expect(channels.producer['producer.connect'].hasSubscribers).toBe(true)
      channels.producer['producer.connect'].publish('message')
    } finally {
      diagnosticsChannel.unsubscribe('kafkajs.producer.connect', onMessage)
    }

    expect(received).toEqual(['message'])
    expect(channels.producer['producer.connect'].hasSubscribers).toBe(false)
  })

  test('each table is frozen', () => {
    for (const kind of KINDS) {
      expect(Object.isFrozen(channels[kind])).toBe(true)
    }
  })

  test('the prefix is the one every channel name starts with', () => {
    expect(channels.CHANNEL_PREFIX).toBe('kafkajs.')
    for (const kind of KINDS) {
      for (const { name } of Object.values(channels[kind])) {
        expect(name.startsWith(channels.CHANNEL_PREFIX)).toBe(true)
      }
    }
  })
})
