'use strict'
/**
 * Final soak container dumps: after the supervisor has exited and before anything is deleted, the
 * Apache tools in the first broker's container read the run's two standing topics (main and
 * transactions) and every consumer group, and their output is written into the artifact so the
 * finalization hashes it. These dumps are the log and commit ground truth: they do not depend on
 * any client under test.
 *
 * Usage (the runbook runs it, from the repository root, with the Compose environment set):
 *
 *   node scripts/soak/finalSoakDump.js --out <artifact dir>
 *
 * Files written under `<out>/apache/`, each tool's output in its own file:
 *
 *   main.dump, transactions.dump         kafka-console-consumer.sh, read_committed, partition 0
 *                                        from the earliest offset, one line per record:
 *                                        `Partition:P<TAB>Offset:N<TAB>key<TAB>value`
 *   groups/<group>.describe              kafka-consumer-groups.sh --describe --group <group>
 *   <file>.stderr                        the tool's stderr
 *   <file>.end                           one line, `exit=<status> lines=<n>`, written last: a file
 *                                        without it was cut off
 *   <file>.cmd                           the exact command line that produced the file
 *
 * Every dump is attempted whatever an earlier one did, so a failed run keeps what could be read.
 * The dumper fails, after writing them all, when a tool failed or a topic dump's own record count
 * does not match its lines; the runbook still finalizes, so the partial dumps are bound. The share
 * group is described too, for the record, but the consumer-groups tool does not support share
 * groups, so its status is written and never fails the dump.
 */
const fs = require('node:fs')
const path = require('node:path')
const { spawn } = require('node:child_process')

const TOOLS = '/opt/kafka/bin'
const BOOTSTRAP = 'kafka1:29092,kafka2:29095,kafka3:29098'
const EXEC = ['docker', 'compose', 'exec', '-T', 'kafka1']
/** How long the console consumer waits for a record before it stops, once it has the log's end. */
const IDLE_MS = 60000
/** How long any one tool may run before it is killed and its dump marked failed. */
const TOOL_MAX_MS = 30 * 60 * 1000
const STDERR_CAP = 1024 * 1024

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
      if (stderr.length < STDERR_CAP) stderr += b
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

/** Dumps the run's standing logs and consumer groups from the broker container. */
class ContainerDumper {
  /**
   * @param {object} options
   * @param {string} options.out the artifact directory
   * @param {object} options.config the run's `config.json`
   * @param {(argv: string[], onData: (chunk: Buffer) => void) => Promise<{ code: number|null,
   *   signal: string|null, stderr: string }>} [options.run] runs one tool; real processes by default
   */
  constructor({ out, config, run = runTool }) {
    Object.assign(this, { out, config, runTool: run })
    this.dir = path.join(out, 'apache')
  }

  /** @returns {string[]} every distinct consumer group id of the run, in config order */
  groups() {
    return [...new Set(this.config.clients.map((c) => c.group).filter(Boolean))]
  }

  /**
   * Runs one tool into `<rel>` with its stderr, its command line and, last, its `.end` line.
   * @param {string} rel the dump's path under the apache directory
   * @param {string[]} argv the whole command line
   * @returns {Promise<{ file: string, exit: number, lines: number, stderr: string }>}
   */
  async tool(rel, argv) {
    const file = path.join(this.dir, rel)
    fs.mkdirSync(path.dirname(file), { recursive: true })
    fs.writeFileSync(`${file}.cmd`, `${argv.map(quote).join(' ')}\n`)
    const fd = fs.openSync(file, 'w')
    let lines = 0
    let result
    try {
      result = await this.runTool(argv, (chunk) => {
        fs.writeSync(fd, chunk)
        for (let i = chunk.indexOf(0x0a); i !== -1; i = chunk.indexOf(0x0a, i + 1)) lines++
      })
    } finally {
      fs.closeSync(fd)
    }
    const exit = result.code ?? -1
    fs.writeFileSync(`${file}.stderr`, result.stderr)
    fs.writeFileSync(`${file}.end`, `exit=${exit} lines=${lines}\n`)
    return { file: rel, exit, lines, stderr: result.stderr }
  }

  /**
   * Dumps both standing topics and every group.
   * @returns {Promise<{ files: string[] }>} what was dumped
   * @throws {Error} naming each dump whose tool failed or whose record count is wrong, after every
   *   dump has been attempted and written
   */
  async run() {
    const { main, transactions } = this.config.topics
    const results = []
    const failures = []
    for (const [role, topic] of [
      ['main', main],
      ['transactions', transactions],
    ]) {
      const result = await this.tool(`${role}.dump`, [
        ...EXEC,
        `${TOOLS}/kafka-console-consumer.sh`,
        '--bootstrap-server',
        BOOTSTRAP,
        '--topic',
        topic,
        '--partition',
        '0',
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
      ])
      results.push(result)
      const counted = [...result.stderr.matchAll(/Processed a total of (\d+) messages/g)]
      if (counted.length !== 1 || Number(counted[0][1]) !== result.lines)
        failures.push(
          `${result.file} (the tool counted ${counted.map((m) => m[1]).join(',') || 'no'} records, wrote ${result.lines} lines)`
        )
    }
    for (const group of this.groups()) {
      if (!/^[A-Za-z0-9._-]+$/.test(group)) {
        failures.push(`group ${JSON.stringify(group)} has a name no dump file can carry`)
        continue
      }
      results.push(
        await this.tool(`groups/${group}.describe`, [
          ...EXEC,
          `${TOOLS}/kafka-consumer-groups.sh`,
          '--bootstrap-server',
          BOOTSTRAP,
          '--describe',
          '--group',
          group,
        ])
      )
    }
    const share = new Set(
      this.config.clients
        .filter((c) => c.clientKind === 'share')
        .map((c) => `groups/${c.group}.describe`)
    )
    for (const r of results)
      if (r.exit !== 0 && !share.has(r.file)) failures.push(`${r.file} (exit ${r.exit})`)
    if (failures.length > 0) throw new Error(`dump failed: ${failures.join('; ')}`)
    return { files: results.map((r) => r.file) }
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
  await new ContainerDumper({ out, config }).run()
}

if (require.main === module) {
  main(process.argv.slice(2)).catch((e) => {
    console.error(e.message)
    process.exitCode = 1
  })
}

module.exports = { ContainerDumper, parseArgs, runTool }
