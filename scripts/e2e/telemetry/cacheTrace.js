const REMOVING = /TRACE Removing client connection id \[([^\]\s]+)\] from the client instance cache/
const MISSING =
  /TRACE Client connection id \[([^\]\s]+)\] is not found in the client instance cache/
const CONNECTION = /^(\S+):(\d+)-(\S+):(\d+)-(\d+)$/

/**
 * Finds the connections whose disconnect removed a client instance from the broker cache.
 * Kafka's ClientMetricsManager logs "Removing client connection id [...]" for every closed
 * connection, then "is not found" for the ones that never registered a client instance, so a
 * removal counts only when no not-found line follows it for the same connection.
 * @param {string} text broker log text covering one observation window
 * @returns {{connectionId:string,localPort:string,line:string}[]} successful cache removals
 * @throws {Error} for a connection id that does not have the broker's `local-remote-index` shape
 */
const retirementsOf = (text) => {
  const lines = text.split('\n')
  const missing = new Set()
  for (const line of lines) {
    const hit = MISSING.exec(line)
    if (hit) missing.add(hit[1])
  }
  const found = []
  for (const line of lines) {
    const hit = REMOVING.exec(line)
    if (!hit || missing.has(hit[1])) continue
    const parts = CONNECTION.exec(hit[1])
    if (!parts) throw new Error(`Unrecognized broker connection id ${hit[1]}`)
    found.push({ connectionId: hit[1], localPort: parts[2], line: line.trim() })
  }
  return found
}

/**
 * Proves, from the three brokers' logs of one episode, that exactly one registered client
 * connection was retired, on the expected node and listener. Every broker log must be complete:
 * an unavailable or capped log cannot prove the absence of another retirement.
 * @param {object} options the episode
 * @param {Object<string,{text:string,truncated?:boolean}>} options.logs log of each broker node
 * @param {string} options.node node of the client's captured telemetry socket
 * @param {string} options.localPort broker-side listener port of that socket
 * @returns {{source:string,node:string,connectionIdentity:{node:string,localPort:string},connectionId:string,line:string}}
 *   the source-qualified retirement
 * @throws {Error} for an unavailable log, no retirement, another or several retirements, or a
 *   retirement on another node or listener
 */
const cacheRetirement = ({ logs, node, localPort }) => {
  const nodes = Object.keys(logs).sort()
  if (nodes.join() !== '1,2,3') throw new Error('Expected the logs of brokers 1, 2 and 3')
  const all = []
  for (const name of nodes) {
    const log = logs[name]
    if (typeof log?.text !== 'string' || log.truncated)
      throw new Error(`Broker ${name} log is unavailable or truncated`)
    for (const found of retirementsOf(log.text)) all.push({ ...found, node: name })
  }
  if (all.length === 0) throw new Error('No client instance cache retirement was logged')
  if (all.length > 1) throw new Error(`Expected one cache retirement, found ${all.length}`)
  const [only] = all
  if (only.node !== node) throw new Error(`Retirement on node ${only.node}, expected ${node}`)
  if (only.localPort !== localPort)
    throw new Error(`Retirement on listener ${only.localPort}, expected ${localPort}`)
  return {
    source: 'broker-disconnect-trace',
    node,
    connectionIdentity: { node, localPort },
    connectionId: only.connectionId,
    line: only.line,
  }
}

module.exports = { cacheRetirement, retirementsOf }
