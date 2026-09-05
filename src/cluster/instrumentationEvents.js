const InstrumentationEventType = require('../instrumentation/eventType')
const eventType = InstrumentationEventType('cluster')

/**
 * Cluster-level instrumentation events. These names are neutral on purpose: each client
 * (consumer, producer, admin) re-exposes them under its own prefix through its `wrappedEvents`
 * map, exactly as the `network.*` events are re-exposed, so one emitted name can serve all three.
 */
module.exports = {
  CLUSTER_TOPIC_RECREATED: eventType('topic_recreated'),
  CLUSTER_REBOOTSTRAP: eventType('rebootstrap'),
}
