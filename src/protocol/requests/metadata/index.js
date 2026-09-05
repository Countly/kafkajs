const versions = {
  0: ({ topics }) => {
    const request = require('./v0/request')
    const response = require('./v0/response')
    return { request: request({ topics }), response }
  },
  1: ({ topics }) => {
    const request = require('./v1/request')
    const response = require('./v1/response')
    return { request: request({ topics }), response }
  },
  2: ({ topics }) => {
    const request = require('./v2/request')
    const response = require('./v2/response')
    return { request: request({ topics }), response }
  },
  3: ({ topics }) => {
    const request = require('./v3/request')
    const response = require('./v3/response')
    return { request: request({ topics }), response }
  },
  4: ({ topics, allowAutoTopicCreation }) => {
    const request = require('./v4/request')
    const response = require('./v4/response')
    return { request: request({ topics, allowAutoTopicCreation }), response }
  },
  5: ({ topics, allowAutoTopicCreation }) => {
    const request = require('./v5/request')
    const response = require('./v5/response')
    return { request: request({ topics, allowAutoTopicCreation }), response }
  },
  6: ({ topics, allowAutoTopicCreation }) => {
    const request = require('./v6/request')
    const response = require('./v6/response')
    return { request: request({ topics, allowAutoTopicCreation }), response }
  },
  7: ({ topics, allowAutoTopicCreation }) => {
    const request = require('./v7/request')
    const response = require('./v7/response')
    return { request: request({ topics, allowAutoTopicCreation }), response }
  },
  8: ({ topics, allowAutoTopicCreation }) => {
    const request = require('./v8/request')
    const response = require('./v8/response')
    return { request: request({ topics, allowAutoTopicCreation }), response }
  },
  9: ({ topics, allowAutoTopicCreation }) => {
    const request = require('./v9/request')
    const response = require('./v9/response')
    return { request: request({ topics, allowAutoTopicCreation }), response }
  },
  10: ({ topics, allowAutoTopicCreation }) => {
    const request = require('./v10/request')
    const response = require('./v10/response')
    return { request: request({ topics, allowAutoTopicCreation }), response }
  },
  11: ({ topics, allowAutoTopicCreation }) => {
    const request = require('./v11/request')
    const response = require('./v11/response')
    return { request: request({ topics, allowAutoTopicCreation }), response }
  },
  12: ({ topics, allowAutoTopicCreation }) => {
    const request = require('./v12/request')
    const response = require('./v12/response')
    return { request: request({ topics, allowAutoTopicCreation }), response }
  },
  13: ({ topics, allowAutoTopicCreation }) => {
    const request = require('./v13/request')
    const response = require('./v13/response')
    return { request: request({ topics, allowAutoTopicCreation }), response }
  },
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
