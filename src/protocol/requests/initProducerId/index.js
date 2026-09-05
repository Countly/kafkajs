const versions = {
  0: ({ transactionalId, transactionTimeout = 5000 }) => {
    const request = require('./v0/request')
    const response = require('./v0/response')
    return { request: request({ transactionalId, transactionTimeout }), response }
  },
  1: ({ transactionalId, transactionTimeout = 5000 }) => {
    const request = require('./v1/request')
    const response = require('./v1/response')
    return { request: request({ transactionalId, transactionTimeout }), response }
  },
  2: ({ transactionalId, transactionTimeout = 5000 }) => {
    const request = require('./v2/request')
    const response = require('./v2/response')
    return { request: request({ transactionalId, transactionTimeout }), response }
  },
  3: ({ transactionalId, transactionTimeout = 5000 }) => {
    const request = require('./v3/request')
    const response = require('./v3/response')
    return { request: request({ transactionalId, transactionTimeout }), response }
  },
  4: ({ transactionalId, transactionTimeout = 5000 }) => {
    const request = require('./v4/request')
    const response = require('./v4/response')
    return { request: request({ transactionalId, transactionTimeout }), response }
  },
  5: ({ transactionalId, transactionTimeout = 5000 }) => {
    const request = require('./v5/request')
    const response = require('./v5/response')
    return { request: request({ transactionalId, transactionTimeout }), response }
  },
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
