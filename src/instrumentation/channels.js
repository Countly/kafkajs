const diagnosticsChannel = require('node:diagnostics_channel')
const producerEvents = require('../producer/instrumentationEvents')
const consumerEvents = require('../consumer/instrumentationEvents')
const adminEvents = require('../admin/instrumentationEvents')

/** Every public event name is published on the channel of that name under this prefix. */
const CHANNEL_PREFIX = 'kafkajs.'

/**
 * The channels of one client kind, keyed by the internal name its lower layers emit each event
 * under (`network.request`, `producer.connect`): the key an emitter looks up with.
 * @see https://nodejs.org/api/diagnostics_channel.html
 * @param {{ events: Record<string, string>, unwrap: (publicName: string) => string }} source the
 *   kind's `instrumentationEvents` module
 * @returns {Readonly<Record<string, import('node:diagnostics_channel').Channel>>}
 */
const channelsOf = ({ events, unwrap }) =>
  Object.freeze(
    Object.fromEntries(
      Object.values(events).map((publicName) => [
        unwrap(publicName),
        diagnosticsChannel.channel(`${CHANNEL_PREFIX}${publicName}`),
      ])
    )
  )

/**
 * One channel per public instrumentation event, `kafkajs.<event name>` - 42 in all - created once
 * when this module loads: Node advises against acquiring channels at runtime, and an emit never
 * does. Each client kind has its own table, because the network and cluster events share their
 * internal names across kinds.
 */
module.exports = {
  CHANNEL_PREFIX,
  producer: channelsOf(producerEvents),
  consumer: channelsOf(consumerEvents),
  admin: channelsOf(adminEvents),
}
