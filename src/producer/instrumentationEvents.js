const swapObject = require('../utils/swapObject')
const networkEvents = require('../network/instrumentationEvents')
const clusterEvents = require('../cluster/instrumentationEvents')
const InstrumentationEventType = require('../instrumentation/eventType')
const producerType = InstrumentationEventType('producer')

const events = {
  CONNECT: producerType('connect'),
  INITIALIZED: producerType('initialized'),
  DISCONNECT: producerType('disconnect'),
  REQUEST: producerType(networkEvents.NETWORK_REQUEST),
  REQUEST_TIMEOUT: producerType(networkEvents.NETWORK_REQUEST_TIMEOUT),
  REQUEST_QUEUE_SIZE: producerType(networkEvents.NETWORK_REQUEST_QUEUE_SIZE),
  AUTHENTICATION_FAILURE: producerType(networkEvents.NETWORK_AUTHENTICATION_FAILURE),
  LOCK_TIMEOUT: producerType(networkEvents.NETWORK_LOCK_TIMEOUT),
  TOPIC_RECREATED: producerType(clusterEvents.CLUSTER_TOPIC_RECREATED),
}

const wrappedEvents = {
  [events.REQUEST]: networkEvents.NETWORK_REQUEST,
  [events.REQUEST_TIMEOUT]: networkEvents.NETWORK_REQUEST_TIMEOUT,
  [events.REQUEST_QUEUE_SIZE]: networkEvents.NETWORK_REQUEST_QUEUE_SIZE,
  [events.AUTHENTICATION_FAILURE]: networkEvents.NETWORK_AUTHENTICATION_FAILURE,
  [events.LOCK_TIMEOUT]: networkEvents.NETWORK_LOCK_TIMEOUT,
  [events.TOPIC_RECREATED]: clusterEvents.CLUSTER_TOPIC_RECREATED,
}

const reversedWrappedEvents = swapObject(wrappedEvents)
const unwrap = (eventName) => wrappedEvents[eventName] || eventName
const wrap = (eventName) => reversedWrappedEvents[eventName] || eventName

module.exports = {
  events,
  wrap,
  unwrap,
}
