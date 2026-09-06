const versions = {
  3: ({ acks, timeout, compression, topicData, transactionalId, producerId, producerEpoch }) => {
    const request = require('./v3/request')
    const response = require('./v3/response')
    return {
      request: request({
        acks,
        timeout,
        compression,
        topicData,
        transactionalId,
        producerId,
        producerEpoch,
      }),
      response,
    }
  },
  4: ({ acks, timeout, compression, topicData, transactionalId, producerId, producerEpoch }) => {
    const request = require('./v4/request')
    const response = require('./v4/response')
    return {
      request: request({
        acks,
        timeout,
        compression,
        topicData,
        transactionalId,
        producerId,
        producerEpoch,
      }),
      response,
    }
  },
  5: ({ acks, timeout, compression, topicData, transactionalId, producerId, producerEpoch }) => {
    const request = require('./v5/request')
    const response = require('./v5/response')
    return {
      request: request({
        acks,
        timeout,
        compression,
        topicData,
        transactionalId,
        producerId,
        producerEpoch,
      }),
      response,
    }
  },
  6: ({ acks, timeout, compression, topicData, transactionalId, producerId, producerEpoch }) => {
    const request = require('./v6/request')
    const response = require('./v6/response')
    return {
      request: request({
        acks,
        timeout,
        compression,
        topicData,
        transactionalId,
        producerId,
        producerEpoch,
      }),
      response,
    }
  },
  7: ({ acks, timeout, compression, topicData, transactionalId, producerId, producerEpoch }) => {
    const request = require('./v7/request')
    const response = require('./v7/response')
    return {
      request: request({
        acks,
        timeout,
        compression,
        topicData,
        transactionalId,
        producerId,
        producerEpoch,
      }),
      response,
    }
  },
  8: ({ acks, timeout, compression, topicData, transactionalId, producerId, producerEpoch }) => {
    const request = require('./v8/request')
    const response = require('./v8/response')
    return {
      request: request({
        acks,
        timeout,
        compression,
        topicData,
        transactionalId,
        producerId,
        producerEpoch,
      }),
      response,
    }
  },
  9: ({ acks, timeout, compression, topicData, transactionalId, producerId, producerEpoch }) => {
    const request = require('./v9/request')
    const response = require('./v9/response')
    return {
      request: request({
        acks,
        timeout,
        compression,
        topicData,
        transactionalId,
        producerId,
        producerEpoch,
      }),
      response,
    }
  },
  10: ({ acks, timeout, compression, topicData, transactionalId, producerId, producerEpoch }) => {
    const request = require('./v10/request')
    const response = require('./v10/response')
    return {
      request: request({
        acks,
        timeout,
        compression,
        topicData,
        transactionalId,
        producerId,
        producerEpoch,
      }),
      response,
    }
  },
  11: ({ acks, timeout, compression, topicData, transactionalId, producerId, producerEpoch }) => {
    const request = require('./v11/request')
    const response = require('./v11/response')
    return {
      request: request({
        acks,
        timeout,
        compression,
        topicData,
        transactionalId,
        producerId,
        producerEpoch,
      }),
      response,
    }
  },
  12: ({ acks, timeout, compression, topicData, transactionalId, producerId, producerEpoch }) => {
    const request = require('./v12/request')
    const response = require('./v12/response')
    return {
      request: request({
        acks,
        timeout,
        compression,
        topicData,
        transactionalId,
        producerId,
        producerEpoch,
      }),
      response,
    }
  },
  13: ({
    acks,
    timeout,
    compression,
    topicData,
    topicIds,
    topicNames,
    transactionalId,
    producerId,
    producerEpoch,
  }) => {
    const request = require('./v13/request')
    const response = require('./v13/response')
    return {
      request: request({
        acks,
        timeout,
        compression,
        topicData,
        topicIds,
        transactionalId,
        producerId,
        producerEpoch,
      }),
      // The stateless response module gets this request's own id-to-name map, exactly as
      // fetch/index.js binds `checkCrcs`. Nothing else in the codec layer is per-request.
      response: {
        decode: (rawData) => response.decode(rawData, { topicNames }),
        parse: response.parse,
      },
    }
  },
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
