const assert = require('assert/strict')

/**
 * Checks the pinned broker's runtime request logger route before opening observation windows.
 * The request logger is non-additive: changing its level does not send its records to stdout.
 * @param {string} config the owned container's complete log4j2.yaml
 * @returns {void}
 * @throws {Error} if the expected file appender or logger route is absent
 */
const assertRequestLogConfig = (config) => {
  assert(
    /name: RequestAppender\s+fileName: ["']?\$\{sys:kafka.logs.dir\}\/kafka-request\.log["']?\s/.test(
      config
    ),
    'Missing pinned request file appender'
  )
  assert(
    /name: kafka\.request\.logger\s+level: \w+\s+additivity: false\s+AppenderRef:\s+ref: RequestAppender\s/.test(
      config
    ),
    'Missing pinned request logger route'
  )
}

/**
 * Builds a bounded-window read of the pinned request appender with a fixed Bash program and literal
 * positional arguments. Kafka writes completed
 * requests at DEBUG as JSON. Only Get records are needed to join captured sockets through NAT;
 * unrelated traffic is excluded before it can consume the capture bound. UTC matches the owned
 * fixture's default log timestamp zone; half-open windows cannot duplicate boundary Gets.
 * @param {string} service owned broker service
 * @param {string} since inclusive RFC3339 window start
 * @param {string} until exclusive RFC3339 window end
 * @returns {string[]} literal docker compose arguments
 * @throws {Error} for an invalid service, timestamp or reversed window
 */
const requestTraceArgs = (service, since, until) => {
  assert(/^kafka[123]$/.test(service), 'Invalid request-log service')
  const stamp = (value) => {
    assert(new Date(value).toISOString() === value, 'Expected canonical UTC trace time')
    return value.slice(0, -1).replace('T', ' ').replace('.', ',')
  }
  const start = stamp(since)
  const end = stamp(until)
  assert(end >= start, 'Reversed request-log window')
  return [
    'exec',
    '-T',
    service,
    'env',
    'LC_ALL=C',
    'bash',
    '-c',
    `while IFS= read -r line; do
  case "$line" in
    *'Completed request:'*GET_TELEMETRY_SUBSCRIPTIONS*)
      stamp=\${line:1:23}
      if [[ "$stamp" > "$1" || "$stamp" == "$1" ]] && [[ "$stamp" < "$2" ]]; then
        printf '%s\\n' "$line"
      fi
      ;;
  esac
done < "$3"`,
    'request-log-window',
    start,
    end,
    '/opt/kafka/logs/kafka-request.log',
  ]
}

module.exports = { assertRequestLogConfig, requestTraceArgs }
