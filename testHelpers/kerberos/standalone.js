const { execFileSync, spawnSync } = require('node:child_process')
const fs = require('node:fs')
const path = require('node:path')
const { composeArgs } = require('../leaderElection')

const ROOT = path.resolve(__dirname, '..', '..')
const PRINCIPAL = 'client@KAFKAJS.TEST'
/** Ticket lifetime in seconds for each cache name the live scenario asks for. */
const LIFETIMES = { fresh: 3600, expired: 60 }
const KAFKA_TOOLS = '/opt/kafka/bin'
const KAFKA_NAME = /^[A-Za-z0-9][A-Za-z0-9._-]{0,248}$/
/** The clock-skew bound between the runner and the KDC container, in milliseconds. */
const MAX_SKEW_MS = 120000
const TIME = '(\\d\\d)/(\\d\\d)/(\\d{4}|\\d\\d) (\\d\\d):(\\d\\d):(\\d\\d)'
const TGT_ROW = new RegExp(`^${TIME}\\s+${TIME}\\s+krbtgt/KAFKAJS\\.TEST@KAFKAJS\\.TEST\\s*$`, 'm')

/**
 * Reads the client's ticket-granting ticket validity from `klist` output (C locale, UTC).
 * @param {string} text `klist` output
 * @returns {{issuedAtMs: number, expiresAtMs: number}} start and end of the krbtgt row
 * @throws {Error} another principal, or no krbtgt row in the expected format
 */
const parseTgt = (text) => {
  const row =
    new RegExp(`^Default principal: ${PRINCIPAL.replaceAll('.', '\\.')}$`, 'm').test(text) &&
    TGT_ROW.exec(text)
  if (!row) throw new Error('Unreadable klist output: no krbtgt row for the client principal')
  const at = (from) => {
    const year = Number(row[from + 2])
    return Date.UTC(
      year < 100 ? 2000 + year : year,
      Number(row[from]) - 1,
      Number(row[from + 1]),
      Number(row[from + 3]),
      Number(row[from + 4]),
      Number(row[from + 5])
    )
  }
  return { issuedAtMs: at(1), expiresAtMs: at(7) }
}

/**
 * Reads one partition's committed and log-end offsets from `kafka-consumer-groups.sh --describe`.
 * @param {string} text the tool's output
 * @param {string} topic topic whose partition 0 is wanted
 * @returns {{committedOffset: string, logEnd: number}} offsets
 * @throws {Error} no header, no row for the partition or a non-numeric offset
 */
const parseCommitted = (text, topic) => {
  const lines = text.split('\n')
  const head = lines.findIndex((line) => /^GROUP\s/.test(line))
  const columns = head < 0 ? [] : lines[head].trim().split(/\s+/)
  const [name, part, current, end] = ['TOPIC', 'PARTITION', 'CURRENT-OFFSET', 'LOG-END-OFFSET'].map(
    (column) => columns.indexOf(column)
  )
  if ([name, part, current, end].includes(-1))
    throw new Error('Unreadable consumer-group description')
  for (const line of lines.slice(head + 1)) {
    const fields = line.trim().split(/\s+/)
    if (fields[name] !== topic || fields[part] !== '0') continue
    if (!/^\d+$/.test(fields[current]) || !/^\d+$/.test(fields[end]))
      throw new Error('Non-numeric offsets in the consumer-group description')
    return { committedOffset: fields[current], logEnd: Number(fields[end]) }
  }
  throw new Error('No partition 0 row in the consumer-group description')
}

/**
 * Blocks the thread for a while; the live scenario reads the fixture's answers synchronously.
 * @param {number} ms delay
 * @returns {void}
 */
const sleepSync = (ms) => Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms)

/**
 * The KDC fixture's operations against the job's own Compose cluster, for a run with no guard.
 * It issues tickets inside the KDC container from its keytab, copies only the credential cache out,
 * and reads the produced log back with Kafka's own tools. Each operation is synchronous.
 */
class StandaloneKdc {
  /**
   * @param {object} [options] replaced by tests
   * @param {string} [options.directory] where caches, the Kerberos config and captures are written
   * @param {(argv: string[], timeoutMs: number) => string} [options.run] runs `docker`, returns stdout
   * @param {(argv: string[], timeoutMs: number) => number|null} [options.status] runs `docker`, returns its exit status
   * @param {() => number} [options.now] wall clock in milliseconds
   * @param {(ms: number) => void} [options.sleep] blocking delay
   */
  constructor({
    directory = path.join(ROOT, '.tmp', 'e2e', 'kerberos'),
    run = (argv, timeoutMs) =>
      execFileSync('docker', argv, { encoding: 'utf8', timeout: timeoutMs, killSignal: 'SIGKILL' }),
    status = (argv, timeoutMs) =>
      spawnSync('docker', argv, { encoding: 'utf8', timeout: timeoutMs, killSignal: 'SIGKILL' })
        .status,
    now = Date.now,
    sleep = sleepSync,
  } = {}) {
    this.directory = directory
    this.run = run
    this.status = status
    this.now = now
    this.sleep = sleep
  }

  /**
   * Runs one operation by its fixture name.
   * @param {string} operation gssapi-ticket, gssapi-expired or gssapi-log
   * @param {string[]} args the operation's literal arguments
   * @returns {object} the record the live scenario reads
   * @throws {Error} an unknown operation, invalid arguments or the operation's own failure
   */
  control(operation, args) {
    if (operation === 'gssapi-ticket' && args.length === 1 && Object.hasOwn(LIFETIMES, args[0]))
      return this.ticket(args[0])
    if (operation === 'gssapi-expired' && args.length === 1 && args[0] === 'expired')
      return this.expired(args[0])
    if (
      operation === 'gssapi-log' &&
      args.length === 2 &&
      args.every((arg) => KAFKA_NAME.test(arg))
    )
      return this.log(args[0], args[1])
    throw new Error('Invalid KDC control')
  }

  /**
   * @param {string} name ticket name
   * @returns {{cache: string, inside: string}} the cache path on the runner and its `FILE:` name in the KDC container
   */
  paths(name) {
    return {
      cache: path.join(this.directory, `${name}.ccache`),
      inside: `FILE:/tmp/kafkajs-gssapi-${name}.ccache`,
    }
  }

  /**
   * @param {string} service Compose service
   * @param {string[]} argv command inside the service
   * @param {string[]} [env] `KEY=value` pairs for the command
   * @returns {string[]} `docker` operands for `compose exec`
   */
  exec(service, argv, env = []) {
    return composeArgs(['exec', '-T', ...env.flatMap((pair) => ['-e', pair]), service, ...argv])
  }

  /**
   * @param {string} inside cache name in the KDC container
   * @returns {{issuedAtMs: number, expiresAtMs: number}} the cache's ticket-granting ticket
   * @throws {Error} a missing cache or unreadable listing
   */
  tgtOf(inside) {
    return parseTgt(
      this.run(this.exec('kdc', ['klist', '-c', inside], ['TZ=UTC', 'LC_ALL=C']), 20000)
    )
  }

  /**
   * Issues the named client ticket in the KDC container and copies only its credential cache, mode
   * 0600, next to a copy of the client's `krb5.conf`; calling it again replaces that cache.
   * @param {'fresh'|'expired'} name 3600 s or 60 s lifetime
   * @returns {{cache: string, config: string, principal: string, issuedAtMs: number, expiresAtMs: number}}
   * @throws {Error} a kinit or copy failure, or a lifetime or clock that does not match the request
   */
  ticket(name) {
    const { cache, inside } = this.paths(name)
    fs.mkdirSync(this.directory, { recursive: true, mode: 0o700 })
    const config = path.join(this.directory, 'krb5.conf')
    fs.copyFileSync(path.join(__dirname, 'krb5.conf'), config)
    this.run(
      this.exec(
        'kdc',
        [
          'kinit',
          '-k',
          '-t',
          '/state/client.keytab',
          '-l',
          `${LIFETIMES[name]}s`,
          '-c',
          inside,
          PRINCIPAL,
        ],
        ['KRB5_CONFIG=/etc/krb5.conf']
      ),
      20000
    )
    const times = this.tgtOf(inside)
    if (
      times.expiresAtMs - times.issuedAtMs !== LIFETIMES[name] * 1000 ||
      Math.abs(times.issuedAtMs - this.now()) > MAX_SKEW_MS
    )
      throw new Error('Ticket lifetime or clock does not match the request')
    const pending = `${cache}.pending`
    fs.rmSync(pending, { force: true })
    this.run(composeArgs(['cp', `kdc:${inside.slice('FILE:'.length)}`, pending]), 20000)
    fs.chmodSync(pending, 0o600)
    fs.renameSync(pending, cache)
    return { cache, config, principal: PRINCIPAL, ...times }
  }

  /**
   * Waits until the issued ticket's end time has passed, then has `klist -s` in the KDC container
   * judge the cache; status 1 means no valid ticket. The cache is listed again afterwards, so a
   * missing cache or a failing exec cannot pass as expiry.
   * @param {'expired'} name the ticket issued by `ticket`
   * @returns {{cache: string, observedAtMs: number, klistStatus: number}} observation after the end time
   * @throws {Error} no such ticket, a ticket still valid or a status other than 1
   */
  expired(name) {
    const { cache, inside } = this.paths(name)
    const { expiresAtMs } = this.tgtOf(inside)
    for (
      let wait = expiresAtMs + 1000 - this.now();
      wait > 0;
      wait = expiresAtMs + 1000 - this.now()
    )
      this.sleep(wait)
    const klistStatus = this.status(
      this.exec('kdc', ['klist', '-s', '-c', inside], ['TZ=UTC', 'LC_ALL=C']),
      20000
    )
    const observedAtMs = this.now()
    if (klistStatus !== 1 || this.tgtOf(inside).expiresAtMs !== expiresAtMs)
      throw new Error(`Expiry not observed: klist -s status ${klistStatus}`)
    return { cache, observedAtMs, klistStatus }
  }

  /**
   * Reads partition 0 of a topic from its earliest offset to the group's log end, and the group's
   * committed offset, with Kafka's own tools over the plaintext internal listener, so independently
   * of the GSSAPI client.
   * @param {string} topic single-partition topic
   * @param {string} groupId the consumer group
   * @returns {{log: Array<{offset: string, value: string}>, committedOffset: string}}
   * @throws {Error} a tool or parse failure, or a log longer than 1000 records
   */
  log(topic, groupId) {
    const bootstrap = ['--bootstrap-server', 'kafka1:29092']
    const { committedOffset, logEnd } = parseCommitted(
      this.run(
        this.exec('kafka1', [
          `${KAFKA_TOOLS}/kafka-consumer-groups.sh`,
          ...bootstrap,
          '--describe',
          '--group',
          groupId,
        ]),
        30000
      ),
      topic
    )
    if (logEnd > 1000) throw new Error('Log too long to read back')
    const lines =
      logEnd === 0
        ? []
        : this.run(
            this.exec('kafka1', [
              `${KAFKA_TOOLS}/kafka-console-consumer.sh`,
              ...bootstrap,
              '--topic',
              topic,
              '--partition',
              '0',
              '--offset',
              'earliest',
              '--max-messages',
              String(logEnd),
              '--timeout-ms',
              '15000',
              '--property',
              'print.offset=true',
              '--property',
              'print.value=true',
            ]),
            45000
          )
            .split('\n')
            .filter(Boolean)
    const log = lines.map((line) => {
      const match = /^Offset:(\d+)\s(.*)$/.exec(line)
      if (!match) throw new Error('Unreadable console-consumer line')
      return { offset: match[1], value: match[2] }
    })
    if (log.length !== logEnd) throw new Error(`Read ${log.length} of ${logEnd} records`)
    return { log, committedOffset }
  }
}

module.exports = { StandaloneKdc, parseTgt, parseCommitted }
