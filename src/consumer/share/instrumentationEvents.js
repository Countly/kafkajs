const swapObject = require('../../utils/swapObject')
const InstrumentationEventType = require('../../instrumentation/eventType')
const networkEvents = require('../../network/instrumentationEvents')
const clusterEvents = require('../../cluster/instrumentationEvents')
const consumerType = InstrumentationEventType('share_consumer')

/** @type {import('types').ShareConsumerEvents} */
const events = {
  HEARTBEAT: consumerType('heartbeat'),
  GROUP_JOIN: consumerType('group_join'),
  ASSIGNMENT_CHANGED: consumerType('assignment_changed'),
  FETCH: consumerType('fetch'),
  ACKNOWLEDGE: consumerType('acknowledge'),
  SESSION_RESET: consumerType('session_reset'),
  LOCK_EXPIRED: consumerType('lock_expired'),
  HANDLER_ERROR: consumerType('handler_error'),
  CONNECT: consumerType('connect'),
  DISCONNECT: consumerType('disconnect'),
  STOP: consumerType('stop'),
  CRASH: consumerType('crash'),
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
