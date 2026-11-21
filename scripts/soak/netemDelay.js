'use strict'

const { execFile } = require('node:child_process')
const { promisify } = require('node:util')

/** @param {string} file @param {string[]} args @returns {Promise<{ stdout: string, stderr: string }>} */
const execFileAsync = (file, args) => promisify(execFile)(file, args, { timeout: 30000 })

/**
 * Delays one broker's answers to clients with `tc netem`, applied from the host inside the broker
 * container's network namespace (`sudo nsenter --net`), so neither a new image nor `NET_ADMIN` on
 * the container is needed. Only egress from the client listener's port is delayed: replication and
 * controller traffic stay fast, so the other brokers' partitions are not slowed through their
 * followers. Needs passwordless `sudo`, `nsenter`, `tc` and the kernel's netem qdisc on the host,
 * which a hosted Ubuntu runner has; `check` proves them with a real rule before a run starts.
 */
class NetemDelay {
  /**
   * @param {object} options
   * @param {string} options.container the broker's container name
   * @param {number} options.port the client listener port inside the container
   * @param {number} options.delayMs
   * @param {(file: string, args: string[]) => Promise<{ stdout: string }>} [options.run]
   */
  constructor({ container, port, delayMs, run = execFileAsync }) {
    Object.assign(this, { container, port, delayMs, run })
  }

  /**
   * Applies a 1 ms rule and removes it again.
   * @returns {Promise<void>}
   * @throws {Error} naming the mechanism and the first failure, when any part is unavailable
   */
  async check() {
    try {
      await this.apply(1)
      await this.clear()
    } catch (error) {
      throw new Error(
        `slow-broker needs sudo, nsenter, tc and the netem qdisc on the runner to delay ${this.container}: ${error.message}`
      )
    }
  }

  /**
   * Delays every packet the client port sends by `delayMs`; other traffic passes undelayed.
   * @param {number} [delayMs=this.delayMs]
   * @returns {Promise<void>}
   * @throws {Error} the failed command, after removing whatever part of the rule was applied
   */
  async apply(delayMs = this.delayMs) {
    const tc = await this.#tc()
    try {
      // Everything unfiltered goes to band 1:2 whatever its TOS; only the client port reaches 1:3.
      await tc(`qdisc add dev eth0 root handle 1: prio bands 3 priomap ${'1 '.repeat(16).trim()}`)
      await tc(`qdisc add dev eth0 parent 1:3 handle 30: netem delay ${delayMs}ms`)
      await tc(
        `filter add dev eth0 parent 1: protocol ip prio 1 u32 match ip sport ${this.port} 0xffff flowid 1:3`
      )
    } catch (error) {
      await tc('qdisc del dev eth0 root').catch(() => {})
      throw error
    }
  }

  /** @returns {Promise<void>} removes the rule @throws {Error} the failed command */
  async clear() {
    const tc = await this.#tc()
    await tc('qdisc del dev eth0 root')
  }

  /**
   * @returns {Promise<(line: string) => Promise<object>>} runs one `tc` command line (no shell) in
   *   the container's namespace
   * @throws {Error} when the container is not running
   */
  async #tc() {
    const { stdout } = await this.run('docker', [
      'inspect',
      '--format',
      '{{.State.Pid}}',
      this.container,
    ])
    const pid = stdout.trim()
    if (!/^[1-9]\d*$/.test(pid))
      throw new Error(`${this.container} is not running (pid ${pid || 'none'})`)
    return (line) =>
      this.run('sudo', ['-n', 'nsenter', '--target', pid, '--net', 'tc', ...line.split(' ')])
  }
}

module.exports = { NetemDelay }
