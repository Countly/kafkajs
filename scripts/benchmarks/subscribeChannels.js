const diagnosticsChannel = require('node:diagnostics_channel')

/** The subscriber the benchmarks measure: it does nothing. */
const ignore = () => {}

/**
 * Subscribes a function that does nothing to the diagnostics channel of every public
 * instrumentation event, `kafkajs.<event name>`, so a benchmark measures what one subscriber per
 * channel costs. The names are read from the public `events` of a producer, a consumer and an
 * admin of `kafka`; none of them connects.
 * @see https://nodejs.org/api/diagnostics_channel.html
 * @param {import('../../types').Kafka} kafka
 * @returns {{ channels: string[], unsubscribe: () => void }} the channel names, and a function
 *   that removes every subscription it made
 */
const subscribeChannels = (kafka) => {
  const names = new Set(
    [
      kafka.producer().events,
      kafka.consumer({ groupId: 'subscribe-channels' }).events,
      kafka.admin().events,
    ].flatMap((events) => Object.values(events))
  )
  const channels = [...names].map((name) => `kafkajs.${name}`)
  channels.forEach((channel) => diagnosticsChannel.subscribe(channel, ignore))
  return {
    channels,
    unsubscribe: () =>
      channels.forEach((channel) => diagnosticsChannel.unsubscribe(channel, ignore)),
  }
}

module.exports = subscribeChannels
