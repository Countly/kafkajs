const topLevelOnly = require('./v2/response').parse

/**
 * The entry of a version that removes a list of members (v3 and later).
 *
 * Two call shapes: the single form `{ groupId, memberId, groupInstanceId, reason? }` keeps the
 * throwing parse, which raises the first member's error (the consumer's own leave relies on it);
 * the batch form `{ groupId, members: [{ memberId, groupInstanceId, reason }] }` reads every member's
 * own code and throws only for a top-level error. `reason` is sent from v5 and dropped below it.
 * @param {Function} request the version's request builder
 * @param {object} response the version's response codec
 * @param {object} options
 * @returns {{ request: object, response: object }}
 */
const withMembers = (
  request,
  response,
  { groupId, memberId, groupInstanceId, reason, members }
) => {
  const batch = members != null

  return {
    request: request({
      groupId,
      members: batch ? members : [{ memberId, groupInstanceId, reason }],
    }),
    response: batch ? { decode: response.decode, parse: topLevelOnly } : response,
  }
}

const versions = {
  0: ({ groupId, memberId }) => {
    const request = require('./v0/request')
    const response = require('./v0/response')
    return {
      request: request({ groupId, memberId }),
      response,
    }
  },
  1: ({ groupId, memberId }) => {
    const request = require('./v1/request')
    const response = require('./v1/response')
    return {
      request: request({ groupId, memberId }),
      response,
    }
  },
  2: ({ groupId, memberId }) => {
    const request = require('./v2/request')
    const response = require('./v2/response')
    return {
      request: request({ groupId, memberId }),
      response,
    }
  },
  3: (options) => withMembers(require('./v3/request'), require('./v3/response'), options),
  4: (options) => withMembers(require('./v4/request'), require('./v4/response'), options),
  5: (options) => withMembers(require('./v5/request'), require('./v5/response'), options),
}

module.exports = {
  versions: Object.keys(versions),
  protocol: ({ version }) => versions[version],
}
