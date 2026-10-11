const versions = {
  1: ({ resources, includeSynonyms }) => {
    const request = require('./v1/request')
    const response = require('./v1/response')
    return { request: request({ resources, includeSynonyms }), response }
  },
  2: ({ resources, includeSynonyms }) => {
    const request = require('./v2/request')
    const response = require('./v2/response')
    return { request: request({ resources, includeSynonyms }), response }
  },
  3: ({ resources, includeSynonyms, includeDocumentation }) => {
    const request = require('./v3/request')
    const response = require('./v3/response')
    return { request: request({ resources, includeSynonyms, includeDocumentation }), response }
  },
  4: ({ resources, includeSynonyms, includeDocumentation }) => {
    const request = require('./v4/request')
    const response = require('./v4/response')
    return { request: request({ resources, includeSynonyms, includeDocumentation }), response }
  },
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
