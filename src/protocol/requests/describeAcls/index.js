const versions = {
  1: ({
    resourceType,
    resourceName,
    resourcePatternType,
    principal,
    host,
    operation,
    permissionType,
  }) => {
    const request = require('./v1/request')
    const response = require('./v1/response')
    return {
      request: request({
        resourceType,
        resourceName,
        resourcePatternType,
        principal,
        host,
        operation,
        permissionType,
      }),
      response,
    }
  },
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
