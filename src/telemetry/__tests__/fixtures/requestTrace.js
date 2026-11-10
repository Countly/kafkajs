const assert = require('assert/strict')

/**
 * Checks the pinned broker's runtime request logger route before opening observation windows.
 * The request logger is non-additive: changing its level does not send its records to stdout.
 * @param {object} config the parsed contents of the owned container's complete log4j2.yaml
 * @returns {void}
 * @throws {Error} if the expected file appender or logger route is absent
 */
const assertRequestLogConfig = (config) => {
  const { Configuration: configuration } = config
  const array = (value) => (Array.isArray(value) ? value : value ? [value] : [])
  const files = array(configuration?.Appenders?.RollingFile).filter(
    (appender) => appender.name === 'RequestAppender'
  )
  assert.equal(files.length, 1, 'Missing or ambiguous request file appender')
  assert.equal(
    files[0].fileName,
    `\${sys:kafka.logs.dir}/kafka-request.log`,
    'Wrong request appender file'
  )
  assert.equal(
    files[0].filePattern,
    `\${sys:kafka.logs.dir}/kafka-request.log.%d{yyyy-MM-dd-HH}`,
    'Wrong request appender rollover pattern'
  )
  assert.deepEqual(
    files[0].TimeBasedTriggeringPolicy,
    { modulate: true, interval: 1 },
    'Wrong request appender rollover policy'
  )
  const loggers = array(configuration?.Loggers?.Logger).filter(
    (logger) => logger.name === 'kafka.request.logger'
  )
  assert.equal(loggers.length, 1, 'Missing or ambiguous request logger')
  assert.equal(loggers[0].additivity, false, 'Unexpected request logger additivity')
  assert.deepEqual(
    array(loggers[0].AppenderRef).map((entry) => entry.ref),
    ['RequestAppender'],
    'Missing pinned request logger route'
  )
}

/**
 * Builds a bounded-window read of the pinned request appender with a fixed Bash program and literal
 * positional arguments. Kafka writes completed requests at DEBUG as JSON. Only Get records are
 * needed to join captured sockets through NAT;
 * unrelated traffic is excluded before it can consume the capture bound. The active file and
 * hourly rolls intersecting the window are read from open descriptors. A changed path/inode
 * snapshot discards the attempt and retries at most three times, without repeating output.
 * Genuine duplicate log records remain visible to the strict connection join. UTC matches the owned
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
    `first="\${1:0:13}"
last="\${2:0:13}"
first="\${first/ /-}"
last="\${last/ /-}"
paths() {
  printf '%s\\n' "$3"
  for file in "$3".????-??-??-??; do
    [[ -f "$file" ]] || continue
    hour="\${file##*.}"
    if [[ "$hour" > "$first" || "$hour" == "$first" ]] && [[ "$hour" < "$last" || "$hour" == "$last" ]]; then
      printf '%s\\n' "$file"
    fi
  done
}
snapshot() {
  local file identity
  while IFS= read -r file; do
    identity=$(stat -Lc '%d:%i' -- "$file" 2>/dev/null) || return 1
    printf '%s %s\\n' "$identity" "$file"
  done < <(paths "$@")
}
for attempt in 1 2 3; do
  before=$(snapshot "$@") || continue
  mapfile -t files < <(paths "$@")
  fds=()
  readable=true
  for file in "\${files[@]}"; do
    if exec {fd}< "$file"; then fds+=("$fd"); else readable=false; break; fi
  done
  output=''
  for fd in "\${fds[@]}"; do
    if [[ "$readable" == true ]]; then
      while IFS= read -r line; do
        case "$line" in
          *'Completed request:'*GET_TELEMETRY_SUBSCRIPTIONS*)
            stamp=\${line:1:23}
            if [[ "$stamp" > "$1" || "$stamp" == "$1" ]] && [[ "$stamp" < "$2" ]]; then
              output+="$line"$'\\n'
              if (( \${#output} > 4194304 )); then
                printf '%s\\n' 'Request trace output exceeds capture bound' >&2
                exit 1
              fi
            fi
            ;;
        esac
      done <&"$fd"
    fi
    exec {fd}<&-
  done
  after=$(snapshot "$@") || continue
  if [[ "$readable" == true && "$before" == "$after" ]]; then
    printf '%s' "$output"
    exit 0
  fi
done
printf '%s\\n' 'Request appender unavailable or rotating during capture' >&2
exit 1`,
    'request-log-window',
    start,
    end,
    '/opt/kafka/logs/kafka-request.log',
  ]
}

module.exports = { assertRequestLogConfig, requestTraceArgs }
