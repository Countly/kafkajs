'use strict'
/**
 * Final soak container dumps: after the supervisor has exited and before anything is deleted, the
 * Apache tools in the first broker's container read the run's two standing topics (main and
 * transactions) and every ordinary consumer group, and their output is written into the artifact so
 * the finalization hashes it. These dumps are the log and commit ground truth: they do not depend
 * on any client under test.
 *
 * Usage (the runbook runs it, from the repository root, with the Compose environment set):
 *
 *   node scripts/soak/finalSoakDump.js --out <artifact dir>
 *
 * Files written under `<out>/dump/`, each tool's output in its own file:
 *
 *   latest-<role>.txt, earliest-<role>.txt   kafka-get-offsets.sh, `topic:partition:offset` lines
 *   log-<role>.txt                           kafka-console-consumer.sh, read_committed, one
 *                                            `Partition:P<TAB>Offset:N<TAB>key` line per record
 *   group-<group>.txt                        kafka-consumer-groups.sh --describe
 *   <name>.cmd                               the exact command line beside each of them
 *   index.json                               every dump, its command and its verdict
 *
 * Every `.txt` ends with one trailer line, `#dump-trailer <json>`, so a truncated file is detected
 * by the absence or the numbers of its trailer. The roles are `main` and `transactions`.
 */
const fs = require('node:fs')
const path = require('node:path')
const { spawn } = require('node:child_process')

const TOOLS = '/opt/kafka/bin'
const BOOTSTRAP = 'kafka1:29092'
const EXEC = ['docker', 'compose', 'exec', '-T', 'kafka1']
/** How long the console consumer waits for a record before it stops, once it has the log's end. */
const IDLE_MS = 30000
/** How long any one tool may run before it is killed and its dump marked incomplete. */
const TOOL_MAX_MS = 30 * 60 * 1000
const TRAILER = '#dump-trailer '

/** @param {string} arg @returns {string} the argument as one shell word */
const quote = (arg) => (/^[\w@%+=:,./-]+$/.test(arg) ? arg : `'${arg.replace(/'/g, "'\\''")}'`)

/**
 * Runs a command, handing its stdout to `onData` as it arrives and keeping a bounded stderr.
 * @param {string[]} argv the command and its arguments
 * @param {(chunk: Buffer) => void} onData receives stdout
 * @returns {Promise<{ code: number|null, signal: string|null, stderr: string }>} how it ended; a
 *   command that cannot start ends with code null and its error in stderr
 */
const runTool = (argv, onData) =>
  new Promise((resolve) => {
    const child = spawn(argv[0], argv.slice(1), { stdio: ['ignore', 'pipe', 'pipe'] })
    let stderr = ''
    child.stdout.on('data', onData)
    child.stderr.on('data', (b) => {
      if (stderr.length < 65536) stderr += b
    })
    const timer = setTimeout(() => child.kill('SIGKILL'), TOOL_MAX_MS)
    child.on('error', (e) => {
      clearTimeout(timer)
      resolve({ code: null, signal: null, stderr: `${stderr}${e.message}` })
    })
    child.on('close', (code, signal) => {
      clearTimeout(timer)
      resolve({ code, signal, stderr })
    })
  })

/**
 * @param {string} text `topic:partition:offset` lines
 * @returns {Record<string, string>} the offset of each partition
 */
const offsetsOf = (text) => {
  const out = {}
  for (const line of text.split('\n')) {
    const m = /^.+:(\d+):(\d+)$/.exec(line.trim())
    if (m) out[m[1]] = m[2]
  }
  return out
}

/** Dumps the run's standing logs and consumer groups from the broker container. */
class ContainerDumper {
  /**
   * @param {object} options
   * @param {string} options.out the artifact directory
   * @param {object} options.config the run's `config.json`
   * @param {object} options.matrix the run's matrix, for the client kinds' roles
   * @param {(argv: string[], onData: (chunk: Buffer) => void) => Promise<{ code: number|null,
   *   signal: string|null, stderr: string }>} [options.run] runs one tool; real processes by default
   * @param {() => number} [options.now] wall-clock ms for the trailers
   */
  constructor({ out, config, matrix, run = runTool, now = Date.now }) {
    Object.assign(this, { out, config, matrix, runTool: run, now })
    this.dir = path.join(out, 'dump')
  }

  /** @returns {string[]} the ordinary (offset-committing) consumer groups, in config order */
  groups() {
    const { clients } = this.config
    return [
      ...new Set(
        clients
          .filter((c) => c.group && this.matrix.clientKinds[c.clientKind].role === 'consumer')
          .map((c) => c.group)
      ),
    ]
  }

  /**
   * Runs one tool into `dump/<name>.txt` with its command beside it and a trailer at the end.
   * @param {string} name the dump's file stem
   * @param {string[]} argv the whole command line
   * @param {(info: object) => { reasons: string[], extra?: object }} assess judges the result;
   *   `info` holds `code`, `signal`, `stderr`, `lines`, `text` (kept only when `keep`), and
   *   `lastOffsets` of console-consumer lines
   * @param {boolean} [keep] whether to hold the stdout text for `assess`
   * @returns {Promise<{ name: string, file: string, cmd: string, complete: boolean,
   *   reasons: string[], exitCode: number|null }>}
   */
  async capture(name, argv, assess, keep = false) {
    const file = path.join(this.dir, `${name}.txt`)
    fs.writeFileSync(path.join(this.dir, `${name}.cmd`), `${argv.map(quote).join(' ')}\n`)
    const fd = fs.openSync(file, 'w')
    const info = { lines: 0, bytes: 0, text: '', lastOffsets: {}, startedAt: this.now() }
    let carry = ''
    const line = (text) => {
      info.lines++
      const m = /^Partition:(\d+)\tOffset:(\d+)\t/.exec(text)
      if (m) info.lastOffsets[m[1]] = m[2]
    }
    const onData = (chunk) => {
      fs.writeSync(fd, chunk)
      info.bytes += chunk.length
      if (keep) info.text += chunk
      carry += chunk
      const parts = carry.split('\n')
      carry = parts.pop()
      for (const part of parts) line(part)
    }
    let result
    try {
      result = await this.runTool(argv, onData)
    } finally {
      if (carry) line(carry)
    }
    const { code, signal, stderr } = result
    const { reasons, extra = {} } = assess({ ...info, code, signal, stderr })
    if (code !== 0) reasons.unshift(`exit ${signal ?? code}`)
    const trailer = {
      name,
      lines: info.lines,
      bytes: info.bytes,
      exitCode: code,
      signal,
      startedAt: info.startedAt,
      endedAt: this.now(),
      stderrBytes: Buffer.byteLength(stderr),
      stderrTail: stderr.slice(-500),
      ...extra,
      complete: reasons.length === 0,
      reasons,
    }
    fs.writeSync(fd, `${TRAILER}${JSON.stringify(trailer)}\n`)
    fs.closeSync(fd)
    return {
      name,
      file: `dump/${name}.txt`,
      cmd: `dump/${name}.cmd`,
      complete: reasons.length === 0,
      reasons,
      exitCode: code,
    }
  }

  /**
   * Dumps both logs and every group. Every dump is attempted and written whatever an earlier one
   * did, so a failed run keeps what could be read.
   * @returns {Promise<{ complete: boolean, files: object[] }>} the index, also written as
   *   `dump/index.json`
   * @throws {Error} naming each dump that is not complete, after the index is written
   */
  async run() {
    fs.mkdirSync(this.dir, { recursive: true })
    const { main, transactions } = this.config.topics
    const files = []
    for (const [role, topic] of [
      ['main', main],
      ['transactions', transactions],
    ]) {
      const bounds = {}
      for (const [stem, time] of [
        ['latest', '-1'],
        ['earliest', '-2'],
      ]) {
        const entry = await this.capture(
          `${stem}-${role}`,
          [
            ...EXEC,
            `${TOOLS}/kafka-get-offsets.sh`,
            '--bootstrap-server',
            BOOTSTRAP,
            '--topic',
            topic,
            '--time',
            time,
          ],
          ({ lines, text }) => {
            bounds[stem] = offsetsOf(text)
            return {
              reasons: lines === 0 ? ['the tool printed no offsets'] : [],
              extra: { offsets: bounds[stem] },
            }
          },
          true
        )
        files.push(entry)
      }
      files.push(await this.dumpLog(role, topic, bounds.earliest ?? {}, bounds.latest ?? {}))
    }
    for (const group of this.groups())
      files.push(
        await this.capture(
          `group-${group}`,
          [
            ...EXEC,
            `${TOOLS}/kafka-consumer-groups.sh`,
            '--bootstrap-server',
            BOOTSTRAP,
            '--describe',
            '--group',
            group,
            '--timeout',
            '60000',
          ],
          ({ lines, stderr }) => ({
            reasons: [
              ...(lines === 0 ? ['the tool printed no rows'] : []),
              ...(/error/i.test(stderr) ? ['the tool reported an error'] : []),
            ],
          })
        )
      )
    const index = { complete: files.every((f) => f.complete), files }
    fs.writeFileSync(path.join(this.dir, 'index.json'), `${JSON.stringify(index, null, 2)}\n`)
    const failed = files.filter((f) => !f.complete)
    if (failed.length > 0)
      throw new Error(
        `dump incomplete: ${failed.map((f) => `${f.name} (${f.reasons.join(', ')})`).join('; ')}`
      )
    return index
  }

  /**
   * Writes the files of a dump that could not be attempted, so its absence is explained.
   * @param {string} name @param {string} reason @returns {object} the dump's index entry
   */
  refuse(name, reason) {
    const trailer = { name, lines: 0, bytes: 0, exitCode: null, complete: false, reasons: [reason] }
    fs.writeFileSync(path.join(this.dir, `${name}.cmd`), '# not run\n')
    fs.writeFileSync(path.join(this.dir, `${name}.txt`), `${TRAILER}${JSON.stringify(trailer)}\n`)
    return {
      name,
      file: `dump/${name}.txt`,
      cmd: `dump/${name}.cmd`,
      complete: false,
      reasons: [reason],
      exitCode: null,
    }
  }

  /**
   * Dumps one topic's records with the console consumer, read_committed from the earliest offset
   * of its one partition, bounded by the log's end offsets read just before. The tool stops by itself
   * once it has been idle for 30 s. The dump is complete when the tool ended cleanly after its own
   * idle timeout, its own record count equals the lines it wrote, the last offset of each partition
   * lies below the end, and, for the non-transactional main log, every offset below the end is
   * present. The transactions log legitimately lacks control markers and aborted records, so its
   * count is judged against the journal, not here.
   * @param {string} role 'main' or 'transactions' @param {string} topic
   * @param {Record<string, string>} starts earliest offsets @param {Record<string, string>} ends latest offsets
   * @returns {Promise<object>} the dump's index entry
   */
  async dumpLog(role, topic, starts, ends) {
    const partitions = Object.keys(ends)
    if (partitions.length !== 1)
      return this.refuse(`log-${role}`, `${topic} has ${partitions.length} partitions, not one`)
    const argvOf = (partition) => [
      ...EXEC,
      `${TOOLS}/kafka-console-consumer.sh`,
      '--bootstrap-server',
      BOOTSTRAP,
      '--topic',
      topic,
      '--partition',
      partition,
      '--offset',
      'earliest',
      '--isolation-level',
      'read_committed',
      '--timeout-ms',
      String(IDLE_MS),
      '--formatter-property',
      'print.partition=true',
      '--formatter-property',
      'print.offset=true',
      '--formatter-property',
      'print.key=true',
      '--formatter-property',
      'print.value=false',
    ]
    return this.capture(`log-${role}`, argvOf(partitions[0]), ({ lines, stderr, lastOffsets }) => {
      const reasons = []
      const processed = /Processed a total of (\d+) messages/.exec(stderr)
      if (!processed) reasons.push('the tool did not report its record count')
      else if (Number(processed[1]) !== lines)
        reasons.push(`the tool counted ${processed[1]} records but wrote ${lines}`)
      const unexpected = stderr
        .split('\n')
        .filter((l) => /ERROR|Exception/.test(l) && !/TimeoutException/.test(l))
      if (unexpected.length > 0) reasons.push(`the tool reported: ${unexpected[0].slice(0, 200)}`)
      for (const partition of partitions) {
        const end = BigInt(ends[partition])
        const start = BigInt(starts[partition] ?? 0)
        const last = lastOffsets[partition]
        if (last !== undefined && BigInt(last) >= end)
          reasons.push(`partition ${partition} has offset ${last} at or beyond its end ${end}`)
        if (role === 'main' && BigInt(lines) !== end - start)
          reasons.push(`main holds ${lines} records for offsets ${start} to ${end}`)
      }
      return {
        reasons,
        extra: {
          startOffsets: starts,
          endOffsets: ends,
          lastOffsets,
          processed: processed ? Number(processed[1]) : null,
        },
      }
    })
  }
}

/**
 * @param {string[]} argv arguments after the script name
 * @returns {{ out: string }} the artifact directory
 * @throws {Error} without `--out`
 */
function parseArgs(argv) {
  const at = argv.indexOf('--out')
  if (at === -1 || !argv[at + 1]) throw new Error('--out <artifact dir> is required')
  return { out: path.resolve(argv[at + 1]) }
}

/** @param {string[]} argv @returns {Promise<void>} dumps the artifact's run; fails loudly */
async function main(argv) {
  const { out } = parseArgs(argv)
  const config = JSON.parse(fs.readFileSync(path.join(out, 'config.json'), 'utf8'))
  const matrix = JSON.parse(
    fs.readFileSync(path.join(out, 'archive/scripts/soak/finalSoakMatrix.json'), 'utf8')
  )
  await new ContainerDumper({ out, config, matrix }).run()
}

if (require.main === module) {
  main(process.argv.slice(2)).catch((e) => {
    console.error(e.message)
    process.exitCode = 1
  })
}

module.exports = { ContainerDumper, parseArgs, offsetsOf, runTool }
