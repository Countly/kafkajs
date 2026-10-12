const versions = {
  0: ({ topicPartitions, timeout, validateOnly }) => {
    const request = require('./v0/request')
    const response = require('./v0/response')
    return { request: request({ topicPartitions, timeout, validateOnly }), response }
  },
  1: ({ topicPartitions, validateOnly, timeout }) => {
    const request = require('./v1/request')
    const response = require('./v1/response')
    return { request: request({ topicPartitions, validateOnly, timeout }), response }
  },
  2: ({ topicPartitions, validateOnly, timeout }) => {
    const request = require('./v2/request')
    const response = require('./v2/response')
    return { request: request({ topicPartitions, validateOnly, timeout }), response }
  },
  3: ({ topicPartitions, validateOnly, timeout }) => {
    const request = require('./v3/request')
    const response = require('./v3/response')
    return { request: request({ topicPartitions, validateOnly, timeout }), response }
  },
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
