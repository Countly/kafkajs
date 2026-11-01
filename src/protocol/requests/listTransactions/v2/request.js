const Encoder = require('../../../encoder')
const { ListTransactions: apiKey } = require('../../apiKeys')

/** Encodes ListTransactions v2 with compact collections and struct tags.
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
    apiVersion: 2,
    apiName: 'ListTransactions',
    encode: async () =>
      new Encoder()
        .writeUVarIntArray(options.states.map((value) => new Encoder().writeUVarIntString(value)))
        .writeUVarIntArray(options.producerIds.map((value) => new Encoder().writeInt64(value)))
        .writeInt64(options.durationFilter)
        .writeUVarIntString(options.transactionalIdPattern)
        .writeTaggedFields(),
  }
}

module.exports.schema = {
  api: 'ListTransactions',
  version: 2,
  direction: 'request',
  fields: [
    { name: 'StateFilters', type: '[]string', versions: '0+' },
    { name: 'ProducerIdFilters', type: '[]int64', versions: '0+' },
    { name: 'DurationFilter', type: 'int64', versions: '1+' },
    { name: 'TransactionalIdPattern', type: 'string', versions: '2+', nullable: true },
  ],
}
