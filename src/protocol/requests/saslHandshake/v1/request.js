const requestV0 = require('../v0/request')

module.exports = ({ mechanism }) => ({ ...requestV0({ mechanism }), apiVersion: 1 })

module.exports.schema = {
  api: 'SaslHandshake',
  version: 1,
  direction: 'request',
  fields: [{ name: 'Mechanism', type: 'string', versions: '0+' }],
}
