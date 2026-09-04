const swapObject = require('../utils/swapObject')
const networkEvents = require('../network/instrumentationEvents')
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
}

const wrappedEvents = {
  [events.REQUEST]: networkEvents.NETWORK_REQUEST,
  [events.REQUEST_TIMEOUT]: networkEvents.NETWORK_REQUEST_TIMEOUT,
  [events.REQUEST_QUEUE_SIZE]: networkEvents.NETWORK_REQUEST_QUEUE_SIZE,
  [events.AUTHENTICATION_FAILURE]: networkEvents.NETWORK_AUTHENTICATION_FAILURE,
}

const reversedWrappedEvents = swapObject(wrappedEvents)
const unwrap = (eventName) => wrappedEvents[eventName] || eventName
const wrap = (eventName) => reversedWrappedEvents[eventName] || eventName

module.exports = {
  events,
  wrap,
  unwrap,
}
