const { events } = require('./instrumentationEvents')

const UNAVAILABLE = 'Subscribed topic unavailable; continuing healthy topics and retrying metadata'
const AVAILABLE = 'Subscribed topic metadata available again'

/**
 * Builds what a consumer's cluster calls when one of its subscribed topics changes between
 * unavailable and available (see `BrokerPool#isolateUnknownTopics`): one WARN when the topic
 * becomes unavailable, one INFO when it is available again, and the `TOPIC_METADATA_STATUS` event
 * for both. `available` means the metadata is back, not that records or an assignment arrived.
 * A listener that throws is logged and cannot fail the metadata refresh that reported it.
 * @param {object} options
 * @param {string|null|undefined} options.groupId the consumer's group, null without one
 * @param {import('../../types').Logger} options.logger the consumer's logger
 * @param {import('../instrumentation/emitter')} options.instrumentationEmitter the consumer's emitter
 * @returns {(status: import('../cluster/brokerPool').TopicMetadataStatus) => void}
 */
const topicMetadataStatusReporter =
  ({ groupId, logger, instrumentationEmitter }) =>
  ({ topic, status, type, code }) => {
    const payload = { groupId: groupId ?? null, topic, status, type, code }
    if (status === 'unavailable') logger.warn(UNAVAILABLE, payload)
    else logger.info(AVAILABLE, payload)
    try {
      instrumentationEmitter.emit(events.TOPIC_METADATA_STATUS, payload)
    } catch (error) {
      logger.error('Topic metadata status listener threw', { topic, error: error.message })
    }
  }

module.exports = topicMetadataStatusReporter
