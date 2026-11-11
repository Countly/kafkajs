const { resultProblems } = require('./shareSoakResults')
const Decoder = require('../../src/protocol/decoder')
const { responseHeaderVersion } = require('../../src/protocol/headers')
const { readRequestHeader } = require('../../testHelpers/fakeSocket')

const APIS = Object.freeze({
  0: 'produce',
  1: 'fetch',
  2: 'listOffsets',
  3: 'metadata',
  10: 'findCoordinator',
  16: 'listGroups',
  18: 'apiVersions',
  19: 'createTopics',
  20: 'deleteTopics',
  22: 'initProducerId',
  32: 'describeConfigs',
  42: 'deleteGroups',
  44: 'incrementalAlterConfigs',
  60: 'describeCluster',
  76: 'shareGroupHeartbeat',
  77: 'shareGroupDescribe',
  78: 'shareFetch',
  79: 'shareAcknowledge',
  90: 'describeShareGroupOffsets',
})

/**
 * Collects original nested protocol error codes without trusting a public operation status.
 * @param {*} value Decoded wire response.
 * @returns {number[]} All error fields, including partition errors.
 */
const errors = (value) => {
  if (!value || typeof value !== 'object') return []
  return Object.entries(value).flatMap(([key, item]) =>
    /errorCode$/i.test(key) ? [item] : errors(item)
  )
}

/**
 * Decodes original answered frames with low-level versioned codecs. An answered error is archived
 * but cannot establish successful operation coverage. ApiVersions rejection uses its v0 body.
 * @param {object[]} rows One scope's original wire observations.
 * @returns {Promise<object>} Successful operation/version inventory and decode failures.
 */
const verifyProtocolAnswers = async (rows) => {
  const problems = []
  const successes = []
  const answers = []
  const operationResults = new Map(
    rows
      .filter((r) => r.event === 'soak-wire' && r.kind === 'operation' && r.status === 'passed')
      .map((r) => [r.id, r.result])
  )
  const groupListings = []
  const requests = rows.filter((r) => r.event === 'soak-wire' && r.kind === 'request')
  const replies = new Map(
    rows
      .filter((r) => r.event === 'soak-wire' && r.kind === 'response')
      .map((r) => [`${r.socket}:${r.frame}`, r])
  )
  for (const request of requests) {
    const reply = replies.get(`${request.socket}:${request.frame}`)
    if (!reply || !Object.hasOwn(APIS, request.apiKey)) continue
    try {
      const header = readRequestHeader(Buffer.from(request.bytes, 'base64'))
      const bytes = Buffer.from(reply.bytes, 'base64')
      const decoder = new Decoder(bytes)
      if (decoder.readInt32() !== bytes.length - 4 || decoder.readInt32() !== header.correlationId)
        throw new Error('Response correlation mismatch')
      if (responseHeaderVersion(header.apiKey, header.apiVersion) === 1) decoder.readTaggedFields()
      const body = bytes.subarray(decoder.offset)
      const version = header.apiKey === 18 && body.readInt16BE(0) !== 0 ? 0 : header.apiVersion
      const codec = require(
        `../../src/protocol/requests/${APIS[header.apiKey]}/v${version}/response`
      )
      const decoded = await codec.decode(body)
      const codes = errors(decoded)
      if (codes.some((code) => !Number.isInteger(code))) throw new Error('Invalid wire error code')
      if (codes.some((code) => code !== 0)) {
        if (errors(operationResults.get(request.operationId)).some((code) => code !== 0))
          answers.push({ request, decoded })
        continue
      }
      answers.push({ request, decoded })
      if (header.apiKey === 16)
        groupListings.push({
          operationId: request.operationId,
          groups: decoded.groups,
          at: request.at,
        })
      successes.push({
        apiKey: header.apiKey,
        apiVersion: header.apiVersion,
        role: request.role,
        operation: request.operation,
        operationId: request.operationId,
        socket: request.socket,
        frame: request.frame,
      })
    } catch (error) {
      problems.push(
        `Original protocol answer ${request.apiKey}/${request.apiVersion}: ${error.message}`
      )
    }
  }
  for (const key of Object.keys(APIS))
    if (!successes.some((r) => r.apiKey === Number(key)))
      problems.push(`No decoded successful API ${key}`)
  for (const operation of [
    'send',
    'readLog',
    'createTopics',
    'describeCluster',
    'describeConfigs',
    'incrementalAlterConfigs:0',
    'incrementalAlterConfigs:1',
    'listGroups',
    'describeShareGroups',
    'listShareGroupOffsets:named',
    'listShareGroupOffsets:all',
    'deleteGroups',
    'deleteTopics',
    'fetchTopicMetadata',
    'listTopics',
  ])
    if (!successes.some((r) => r.operation === operation))
      problems.push(`No decoded successful operation ${operation}`)
  problems.push(...resultProblems(rows, answers))
  return { problems, successes, groupListings }
}
module.exports = { verifyProtocolAnswers }
