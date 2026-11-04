const fs = require('fs')
const path = require('path')

/** Controls the fixture's shared mount. Each invocation completes one action without retries. */
class AssignmentGate {
  /**
   * @param {string} [directory] the Compose fixture's host mount
   */
  constructor(directory = path.join(__dirname, 'runtime')) {
    this.directory = directory
  }

  /**
   * Publishes arm and release files atomically and returns this case's complete event lines.
   * Retire removes the arm before its release; events remain available for evidence.
   * @param {'arm'|'release'|'read'|'retire'} action
   * @param {string} groupId exact group, without line breaks
   * @param {string} nonce bounded case identity
   * @returns {object[]} matching events, in append order
   * @throws {Error} invalid operands, filesystem failures or malformed complete event lines
   */
  run(action, groupId, nonce) {
    if (
      !['arm', 'release', 'read', 'retire'].includes(action) ||
      !groupId ||
      /[\r\n]/.test(groupId) ||
      !/^[a-zA-Z0-9_-]{1,64}$/.test(nonce)
    ) {
      throw new Error('Invalid assignment gate operation')
    }
    const arm = path.join(this.directory, 'arm')
    const release = path.join(this.directory, 'release')
    if (action === 'arm') {
      fs.rmSync(release, { force: true })
      this.publish(arm, `${groupId}\n${nonce}\n`)
    } else if (action === 'release') {
      this.publish(release, `${nonce}\n`)
    } else if (action === 'retire') {
      fs.rmSync(arm, { force: true })
      fs.rmSync(release, { force: true })
    }
    let events
    try {
      events = fs.readFileSync(path.join(this.directory, 'events.jsonl'), 'utf8')
    } catch (error) {
      if (error.code === 'ENOENT') return []
      throw error
    }
    // A broker may still be appending the last line when the host reads the shared mount.
    return events
      .split('\n')
      .slice(0, -1)
      .filter(Boolean)
      .map((line) => JSON.parse(line))
      .filter((event) => event.groupId === groupId && event.nonce === nonce)
  }

  /**
   * Replaces a control file so brokers never observe a partially written command.
   * @param {string} target control file path
   * @param {string} value complete command
   * @returns {void}
   * @throws {Error} filesystem write or rename failure
   */
  publish(target, value) {
    const temporary = `${target}.${process.pid}`
    try {
      fs.writeFileSync(temporary, value)
      fs.renameSync(temporary, target)
    } finally {
      fs.rmSync(temporary, { force: true })
    }
  }
}

if (require.main === module) {
  const [action, groupId, nonce] = process.argv.slice(2)
  process.stdout.write(JSON.stringify(new AssignmentGate().run(action, groupId, nonce)))
}

module.exports = { AssignmentGate }
