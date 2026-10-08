const versions = {
  0: ({ transactionalId, producerId, producerEpoch, groupId }) => {
    const request = require('./v0/request')
    const response = require('./v0/response')
    return { request: request({ transactionalId, producerId, producerEpoch, groupId }), response }
  },
  1: ({ transactionalId, producerId, producerEpoch, groupId }) => {
    const request = require('./v1/request')
    const response = require('./v1/response')
    return { request: request({ transactionalId, producerId, producerEpoch, groupId }), response }
  },
  2: ({ transactionalId, producerId, producerEpoch, groupId }) => {
    const request = require('./v2/request')
    const response = require('./v2/response')
    return { request: request({ transactionalId, producerId, producerEpoch, groupId }), response }
  },
  3: ({ transactionalId, producerId, producerEpoch, groupId }) => {
    const request = require('./v3/request')
    const response = require('./v3/response')
    return { request: request({ transactionalId, producerId, producerEpoch, groupId }), response }
  },
  4: ({ transactionalId, producerId, producerEpoch, groupId }) => {
    const request = require('./v4/request')
    const response = require('./v4/response')
    return { request: request({ transactionalId, producerId, producerEpoch, groupId }), response }
  },
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
