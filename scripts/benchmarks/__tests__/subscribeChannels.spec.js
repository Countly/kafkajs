const diagnosticsChannel = require('node:diagnostics_channel')
const { Kafka, logLevel } = require('../../../index')
const subscribeChannels = require('../subscribeChannels')

describe('Benchmarks > subscribeChannels', () => {
  const kafka = new Kafka({ brokers: ['localhost:1'], logLevel: logLevel.NOTHING })

  test('subscribes to the channel of every public event, and unsubscribes from each', () => {
    const { channels, unsubscribe } = subscribeChannels(kafka)

    try {
      expect(channels).toHaveLength(43)
      expect(new Set(channels).size).toBe(43)
      expect(channels).toEqual(
        expect.arrayContaining(['kafkajs.producer.connect', 'kafkajs.consumer.fetch'])
      )
      expect(channels.every((channel) => diagnosticsChannel.hasSubscribers(channel))).toBe(true)
    } finally {
      unsubscribe()
    }

    expect(channels.some((channel) => diagnosticsChannel.hasSubscribers(channel))).toBe(false)
  })

  test('unsubscribing leaves a subscriber it did not add', () => {
    const other = jest.fn()
    diagnosticsChannel.subscribe('kafkajs.consumer.fetch', other)

    try {
      subscribeChannels(kafka).unsubscribe()
      diagnosticsChannel.channel('kafkajs.consumer.fetch').publish({ payload: {} })

      expect(other).toHaveBeenCalledTimes(1)
    } finally {
      diagnosticsChannel.unsubscribe('kafkajs.consumer.fetch', other)
    }
  })
})
