const { failure, createErrorFromCode } = require('../../error')

const GROUP_ID_NOT_FOUND = 69

/**
 * The version's error for one DescribeGroups row. From v6 a missing group is a result.
 * @param {number} version negotiated DescribeGroups version
 * @param {{errorCode: number}} row decoded group
 * @returns {Error|undefined} the protocol error, if the version rejects the row
 */
const rowError = (version, { errorCode }) => {
  if (failure(errorCode) && !(version >= 6 && errorCode === GROUP_ID_NOT_FOUND)) {
    return createErrorFromCode(errorCode)
  }
}

/**
 * Builds the regular response parser, which rejects the first error in response order.
 * @param {number} version DescribeGroups version
 * @returns {(data: object) => Promise<object>} parser returning the original response on success
 */
const parseRows = (version) => async (data) => {
  for (const row of data.groups) {
    const error = rowError(version, row)
    if (error) throw error
  }
  return data
}

/**
 * Builds the internal result reader. Membership precedes error construction so foreign rows
 * cannot fail the request or influence the caller's retry state.
 * @param {number} version negotiated DescribeGroups version
 * @param {string[]} groupIds sent wire group ids
 * @returns {(data: object) => object} reader giving each requested row its own outcome
 */
const readResults = (version, groupIds) => {
  const ids = new Set(groupIds)
  return (data) => {
    const groups = []
    for (const row of data.groups) {
      if (!ids.has(row.groupId)) continue
      const error = rowError(version, row)
      groups.push(error ? { ...row, error } : row)
    }
    return { ...data, groups }
  }
}

module.exports = { parseRows, readResults }
