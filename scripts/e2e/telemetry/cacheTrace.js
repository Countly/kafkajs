const assert = require('assert/strict')
const crypto = require('crypto')
const { canonicalUuid } = require('../../../testHelpers/telemetryFixtures')
const REMOVING = /TRACE Removing client connection id \[([^\]\s]+)\] from the client instance cache/
const MISSING =
  /TRACE Client connection id \[([^\]\s]+)\] is not found in the client instance cache/
const CONNECTION = /^(\S+):(\d+)-(\S+):(\d+)-(\d+)(?:-(\d+))?$/

/**
 * Reads complete disconnect windows, excluding removals followed by a cache miss anywhere in it.
 * @param {string} text complete bounded broker log
 * @returns {object[]} registered retirements with exact server-local/remote tuple, processor and index
 * @throws {Error} for an unrecognized connection identity
 */
const retirementsOf = (text) => {
  const missing = new Set(text.split('\n').flatMap((line) => MISSING.exec(line)?.[1] ?? []))
  return text.split('\n').flatMap((line) => {
    const id = REMOVING.exec(line)?.[1]
    if (!id || missing.has(id)) return []
    const parts = CONNECTION.exec(id)
    if (!parts) throw new Error(`Unrecognized broker connection id ${id}`)
    return [
      {
        connectionId: id,
        localAddress: parts[1],
        localPort: parts[2],
        remoteAddress: parts[3],
        remotePort: parts[4],
        processor: parts[6] === undefined ? undefined : parts[5],
        index: parts[6] ?? parts[5],
        line: line.trim(),
      },
    ]
  })
}

/**
 * Joins one successful captured Get to its broker's completed-request JSON (or legacy text). The request's unique
 * client id, correlation and returned UUID identify the physical connection through Docker NAT;
 * the complete server connection tuple and index are retained, rather than guessing NAT ports.
 * @param {object} options logs, captured Get, connect event and node/listener
 * @returns {object} broker-qualified identity with original client endpoints and successful UUID
 * @throws {Error} for absent/ambiguous request trace, another UUID, node or listener
 */
const connectionIdentity = ({ logs, get, connect, node, localPort }) => {
  assert.equal(get.socketId, connect.socketId)
  assert.equal(get.node, node)
  assert.equal(connect.node, node)
  assert.equal(get.response.errorCode, 0)
  assert(Number.isInteger(connect.localPort) && typeof connect.localAddress === 'string')
  const uuid = canonicalUuid(get.response.uuid)
  assert(typeof logs[node]?.text === 'string' && !logs[node].truncated, 'Get trace unavailable')
  const lines = logs[node].text
    .split('\n')
    .filter(
      (line) => line.includes('Completed request:') && line.includes('GET_TELEMETRY_SUBSCRIPTIONS')
    )
    .map((line) => {
      const body = line.split('Completed request:')[1].trim()
      if (body.startsWith('{')) {
        const end = body.lastIndexOf('}')
        const record = JSON.parse(body.slice(0, end + 1))
        return {
          header: record.requestHeader,
          request: record.request,
          response: record.response,
          connection: record.connection,
        }
      }
      // Older broker traces use generated message toString rather than JSON.
      return {
        header: {
          requestApiKey: 71,
          requestApiVersion: get.apiVersion,
          clientId: /clientId=([^,]+)/.exec(body)?.[1],
          correlationId: Number(/correlationId=(\d+)/.exec(body)?.[1]),
        },
        request: { clientInstanceId: /clientInstanceId=([^,\s)]+)/.exec(body)?.[1] },
        response: {
          errorCode: 0,
          clientInstanceId: /clientInstanceId=([^,\s)]+)/.exec(
            body.split('with response:')[1] ?? ''
          )?.[1],
        },
        connection: /from connection ([^;\s]+)/.exec(body)?.[1],
      }
    })
    .filter(
      ({ header, request }) =>
        header.requestApiKey === 71 &&
        header.requestApiVersion === get.apiVersion &&
        header.clientId === get.clientId &&
        header.correlationId === get.correlation &&
        canonicalUuid(request.clientInstanceId) === get.uuid
    )
  assert.equal(lines.length, 1, 'Missing or ambiguous Get connection trace')
  const [line] = lines
  assert.equal(line.response.errorCode, 0, 'Connection trace Get failed')
  assert.equal(canonicalUuid(line.response.clientInstanceId), uuid, 'Connection trace UUID differs')
  const id = line.connection
  const tuple = CONNECTION.exec(id)
  assert(tuple, 'Missing broker connection tuple')
  assert.equal(tuple[2], localPort, 'Wrong broker listener')
  return {
    node,
    localPort,
    connectionId: id,
    uuid,
    client: {
      localAddress: connect.localAddress,
      localPort: connect.localPort,
      remoteAddress: connect.remoteAddress,
      remotePort: connect.remotePort,
    },
    getRequestKey: get.requestKey,
  }
}

/**
 * Re-derives exact UUID-qualified retirement over a complete bounded disconnect episode. The
 * window is read only after close and replacement work finish, so a later flushed miss rejects.
 * @param {object} options three complete logs, Get/connect, node/listener, UUID and episode bounds
 * @returns {object} exact source-qualified retirement and connection identity
 * @throws {Error} for wrong connection/UUID/node, delayed miss, unavailable logs or incomplete episode
 */
const cacheRetirement = ({
  logs,
  node,
  localPort,
  get,
  connect,
  uuid,
  since,
  completedAt,
  source,
}) => {
  assert.deepEqual(
    Object.keys(logs).sort(),
    ['1', '2', '3'],
    'Expected the logs of brokers 1, 2 and 3'
  )
  assert(Date.parse(completedAt) > Date.parse(since), 'Incomplete disconnect episode')
  assert(source && /^[a-f0-9]{64}$/.test(source.sha256), 'Missing broker source identity')
  const all = []
  for (const name of Object.keys(logs)) {
    const log = logs[name]
    if (typeof log?.text !== 'string' || log.truncated)
      throw new Error(`Broker ${name} log is unavailable or truncated`)
    for (const found of retirementsOf(log.text)) all.push({ ...found, node: name })
  }
  const identity = connectionIdentity({ logs, get, connect, node, localPort })
  assert.equal(identity.uuid, uuid, 'Captured UUID differs')
  const selected = all.filter(
    (entry) => entry.node === node && entry.connectionId === identity.connectionId
  )
  assert.equal(
    selected.length,
    1,
    `Expected one exact connection retirement, found ${selected.length}`
  )
  const [only] = selected
  return {
    source: 'broker-disconnect-trace',
    node,
    connectionIdentity: identity,
    connectionId: only.connectionId,
    line: only.line,
    windowsSha256: Object.fromEntries(
      Object.entries(logs).map(([id, log]) => [
        id,
        crypto.createHash('sha256').update(log.text).digest('hex'),
      ])
    ),
  }
}

module.exports = { cacheRetirement, retirementsOf, connectionIdentity }
