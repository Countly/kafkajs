/**
 * The private disposal path of a consumer built on a cluster it borrows from its creator, as the
 * admin's temporary consumers are. It is not part of the public API: a consumer is registered here
 * by the consumer factory and disposed by the code that built it.
 *
 * `consumer.disconnect()` ends a consumer the way an application does and disconnects its cluster,
 * which would cut off every other operation sharing that cluster. Disposal ends the consumer and
 * releases what it registered on the cluster, and leaves the cluster connected.
 */
const disposers = new WeakMap()

/**
 * @param {import('../../types').Consumer} consumer
 * @param {() => Promise<void>} dispose releases the consumer without disconnecting its cluster
 * @returns {void}
 */
const registerDisposer = (consumer, dispose) => {
  disposers.set(consumer, dispose)
}

/**
 * Stops the consumer, cancelling its restart, and removes the cluster subscriptions it made. The
 * cluster stays connected. Safe to call more than once and from any state of the consumer; the
 * consumer must not be used afterwards. An object that was not built by the consumer factory is
 * ignored.
 * @param {import('../../types').Consumer} consumer
 * @returns {Promise<void>}
 * @throws {Error} what stopping the consumer raised; the subscriptions are released regardless
 */
const disposeConsumer = async (consumer) => {
  await disposers.get(consumer)?.()
}

module.exports = { registerDisposer, disposeConsumer }
