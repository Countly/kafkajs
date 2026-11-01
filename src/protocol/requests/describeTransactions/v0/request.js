const Encoder = require('../../../encoder')
const { DescribeTransactions: apiKey } = require('../../apiKeys')

/** Encodes DescribeTransactions v0 with compact collections and struct tags.
 * @param {object} options validated fields; decimal strings preserve int64 values
 * @returns {object} request with header v2 and lazy body encoder
 * @throws {Error} invalid internal fields during encoding
 */
module.exports = (options = {}) => {
  options = { ...{}, ...options }
  return {
    apiKey,
    apiVersion: 0,
    apiName: 'DescribeTransactions',
    encode: async () =>
      new Encoder()
        .writeUVarIntArray(
          options.transactionalIds.map((value) => new Encoder().writeUVarIntString(value))
        )
        .writeTaggedFields(),
  }
}

module.exports.schema = {
  api: 'DescribeTransactions',
  version: 0,
  direction: 'request',
  fields: [{ name: 'TransactionalIds', type: '[]string', versions: '0+' }],
}
