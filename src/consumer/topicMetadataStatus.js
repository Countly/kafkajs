const { events } = require('./instrumentationEvents')
const describeThrown = require('../network/describeThrown')

const UNAVAILABLE = 'Subscribed topic unavailable; continuing healthy topics and retrying metadata'
const AVAILABLE = 'Subscribed topic metadata available again'

/**
 * Builds what a consumer's cluster calls when one of its subscribed topics changes between
 * unavailable and available (see `BrokerPool#isolateUnknownTopics`): one WARN when the topic
 * becomes unavailable, one INFO when it is available again, and the `TOPIC_METADATA_STATUS` event
 * for both. `available` means the metadata is back, not that records or an assignment arrived.
 * The report is best-effort: a log sink or a listener that throws any value is logged when the
 * logger allows it and never fails the metadata refresh that reported the change.
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
    attempt(logger, topic, () => {
      if (status === 'unavailable') logger.warn(UNAVAILABLE, payload)
      else logger.info(AVAILABLE, payload)
    })
    attempt(logger, topic, () => instrumentationEmitter.emit(events.TOPIC_METADATA_STATUS, payload))
  }

/**
 * Runs one part of a report, logging what it throws; a logger that throws again is ignored.
 * @param {import('../../types').Logger} logger
 * @param {string} topic the reported topic
 * @param {() => void} part
 * @returns {void}
 */
const attempt = (logger, topic, part) => {
  try {
    part()
  } catch (error) {
    try {
      logger.error('Topic metadata status report failed', { topic, error: describeThrown(error) })
    } catch {}
  }
}

module.exports = topicMetadataStatusReporter
