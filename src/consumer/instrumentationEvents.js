const swapObject = require('../utils/swapObject')
const InstrumentationEventType = require('../instrumentation/eventType')
const networkEvents = require('../network/instrumentationEvents')
const clusterEvents = require('../cluster/instrumentationEvents')
const consumerType = InstrumentationEventType('consumer')

/** @type {import('types').ConsumerEvents} */
const events = {
  HEARTBEAT: consumerType('heartbeat'),
  LOG_TRUNCATION: consumerType('log_truncation'),
  COMMIT_OFFSETS: consumerType('commit_offsets'),
  GROUP_JOIN: consumerType('group_join'),
  FETCH: consumerType('fetch'),
  FETCH_START: consumerType('fetch_start'),
  START_BATCH_PROCESS: consumerType('start_batch_process'),
  END_BATCH_PROCESS: consumerType('end_batch_process'),
  CONNECT: consumerType('connect'),
  DISCONNECT: consumerType('disconnect'),
  STOP: consumerType('stop'),
  CRASH: consumerType('crash'),
  REBALANCING: consumerType('rebalancing'),
  RECEIVED_UNSUBSCRIBED_TOPICS: consumerType('received_unsubscribed_topics'),
  REQUEST: consumerType(networkEvents.NETWORK_REQUEST),
  REQUEST_TIMEOUT: consumerType(networkEvents.NETWORK_REQUEST_TIMEOUT),
  REQUEST_QUEUE_SIZE: consumerType(networkEvents.NETWORK_REQUEST_QUEUE_SIZE),
  AUTHENTICATION_FAILURE: consumerType(networkEvents.NETWORK_AUTHENTICATION_FAILURE),
  TOPIC_RECREATED: consumerType(clusterEvents.CLUSTER_TOPIC_RECREATED),
  REBOOTSTRAP: consumerType(clusterEvents.CLUSTER_REBOOTSTRAP),
}

const wrappedEvents = {
  [events.REQUEST]: networkEvents.NETWORK_REQUEST,
  [events.REQUEST_TIMEOUT]: networkEvents.NETWORK_REQUEST_TIMEOUT,
  [events.REQUEST_QUEUE_SIZE]: networkEvents.NETWORK_REQUEST_QUEUE_SIZE,
  [events.AUTHENTICATION_FAILURE]: networkEvents.NETWORK_AUTHENTICATION_FAILURE,
  [events.TOPIC_RECREATED]: clusterEvents.CLUSTER_TOPIC_RECREATED,
  [events.REBOOTSTRAP]: clusterEvents.CLUSTER_REBOOTSTRAP,
}

const reversedWrappedEvents = swapObject(wrappedEvents)
const unwrap = (eventName) => wrappedEvents[eventName] || eventName
const wrap = (eventName) => reversedWrappedEvents[eventName] || eventName

module.exports = {
  events,
  wrap,
  unwrap,
}
