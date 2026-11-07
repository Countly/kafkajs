const { EventEmitter } = require('events')
const InstrumentationEvent = require('./event')
const { CHANNEL_PREFIX } = require('./channels')
const { KafkaJSError } = require('../errors')

/**
 * The instrumentation event bus of one client: delivers each event to the listeners its `on()`
 * registered and, when it was built with the client's channel table, mirrors it onto
 * `node:diagnostics_channel` as `kafkajs.<public event name>`.
 */
module.exports = class InstrumentationEventEmitter {
  /**
   * @param {object} [options]
   * @param {Readonly<Record<string, import('node:diagnostics_channel').Channel>>} [options.channels]
   *   the channels of the client this emitter serves, by internal event name; an emitter built
   *   without them publishes nothing
   */
  constructor({ channels } = {}) {
    this.emitter = new EventEmitter()
    this.channels = channels
  }

  /**
   * Checks both observer paths before a caller prepares a notification.
   * @param {string} eventName the internal event name
   * @returns {boolean} whether a listener or diagnostics subscriber can receive this event
   */
  hasListeners(eventName) {
    return (
      this.channels?.[eventName]?.hasSubscribers === true ||
      this.emitter.listenerCount(eventName) > 0
    )
  }

  /**
   * Delivers an event to the `diagnostics_channel` subscribers of its channel, then to the
   * listeners added with `addListener`, as one `InstrumentationEvent` whose `type` is the public
   * event name. Allocates nothing when neither has anybody listening. A channel subscriber that
   * throws does not throw out of here: Node reports its error as an uncaught exception. A listener
   * added with `addListener` that throws does throw out of here, as in KafkaJS 2.2.4.
   * @param {string} eventName the internal event name
   * @param {Object} payload
   * @throws {KafkaJSError} when `eventName` is empty
   */
  emit(eventName, payload) {
    if (!eventName) {
      throw new KafkaJSError('Invalid event name', { retriable: false })
    }

    const channel = this.channels?.[eventName]
    const publishing = channel?.hasSubscribers === true
    const listening = this.emitter.listenerCount(eventName) > 0
    if (!publishing && !listening) {
      return
    }

    const type = publishing ? channel.name.slice(CHANNEL_PREFIX.length) : eventName
    const event = new InstrumentationEvent(type, payload)
    if (publishing) {
      channel.publish(event)
    }
    if (listening) {
      this.emitter.emit(eventName, event)
    }
  }

  /**
   * @param {string} eventName
   * @param {(...args: any[]) => void} listener
   * @returns {import("../../types").RemoveInstrumentationEventListener<string>} removeListener
   */
  addListener(eventName, listener) {
    this.emitter.addListener(eventName, listener)
    return () => this.emitter.removeListener(eventName, listener)
  }
}
