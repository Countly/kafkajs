const { decode: decodeV0, parse: parseV0 } = require('../v0/response')

module.exports = {
  decode: decodeV0,
  parse: parseV0,
}

module.exports.schema = {
  api: 'SaslHandshake',
  version: 1,
  direction: 'response',
  fields: [
    { name: 'ErrorCode', type: 'int16', versions: '0+' },
    { name: 'Mechanisms', type: '[]string', versions: '0+' },
  ],
}
