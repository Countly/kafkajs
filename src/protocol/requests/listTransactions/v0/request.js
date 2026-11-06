const Encoder = require('../../../encoder')
const { ListTransactions: apiKey } = require('../../apiKeys')

/** Encodes ListTransactions v0 with compact collections and struct tags.
 * @param {object} options validated fields; decimal strings preserve int64 values
 * @returns {object} request with header v2 and lazy body encoder
 * @throws {Error} invalid internal fields during encoding
 */
module.exports = (options = {}) => {
  options = {
    ...{ states: [], producerIds: [], durationFilter: '-1', transactionalIdPattern: null },
    ...options,
  }
  return {
    apiKey,
    apiVersion: 0,
    apiName: 'ListTransactions',
    encode: async () =>
      new Encoder()
        .writeUVarIntArray(options.states.map((value) => new Encoder().writeUVarIntString(value)))
        .writeUVarIntArray(options.producerIds.map((value) => new Encoder().writeInt64(value)))
        .writeTaggedFields(),
  }
}

module.exports.schema = {
  api: 'ListTransactions',
  version: 0,
  direction: 'request',
  fields: [
    { name: 'StateFilters', type: '[]string', versions: '0+' },
    { name: 'ProducerIdFilters', type: '[]int64', versions: '0+' },
  ],
}
