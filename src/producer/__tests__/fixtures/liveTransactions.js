const path = require('path')
const { spawn } = require('child_process')
const Broker = require('../../../broker')
const { createConnectionPool, newLogger, plainTextBrokers } = require('testHelpers')
const { sleep } = require('../../../consumer/__tests__/fixtures/liveScenario')

const CRASH_MEMBER = path.join(
  __dirname,
  '..',
  '..',
  '..',
  '..',
  'scripts',
  'e2e',
  'transactionCrashMember.js'
)
// The transaction version 1 cluster arm lowers the feature from a one-shot service whose attempts
// are each cut off after 20 s, so a slow first attempt can take it past half a minute.
const TV1_SETTLE_MS = 120000
const FEATURE_POLL_MS = 1000

/**
 * The cluster's finalized `transaction.version` level, read from the first broker's ApiVersions
 * response the way the producer reads it.
 * @returns {Promise<{ level: number, epoch: number }>} level 0 when the feature is not finalized
 */
const readTransactionVersion = async () => {
  const broker = new Broker({ connectionPool: createConnectionPool(), logger: newLogger() })
  try {
    await broker.connect()
    const feature = broker.finalizedFeatures['transaction.version']
    return {
      level: broker.finalizedFeaturesEpoch >= 0 && feature ? feature.maxVersionLevel : 0,
      epoch: broker.finalizedFeaturesEpoch,
    }
  } finally {
    await broker.disconnect()
  }
}

/**
 * The cluster's finalized `transaction.version` level once it is the one the started cluster arm
 * finalizes. On the transaction version 1 arm (`COMPOSE_FILE` naming
 * `docker-compose.transaction-v1.yml`), whose level drops to 1 a little after the brokers answer,
 * it waits for that, through failed reads too; on any other cluster one reading is final. The
 * client never sets the level: the answer is always what the broker reports.
 * @returns {Promise<{ level: number, epoch: number }>}
 * @throws {Error} the broker read's own error off the version 1 arm; on it, an error when the
 *   level is still not 1 after 120 s
 */
const settledTransactionVersion = async () => {
  const versionOneArm = /transaction-v1/.test(process.env.COMPOSE_FILE || '')
  const deadline = Date.now() + TV1_SETTLE_MS
  for (;;) {
    const reading = await readTransactionVersion().catch((error) => {
      if (!versionOneArm) throw error
      return { level: null, epoch: -1 }
    })
    if (!versionOneArm || reading.level === 1) {
      return reading
    }
    if (Date.now() >= deadline) {
      throw new Error(
        `The transaction version 1 arm still finalizes transaction.version ${reading.level} after ${TV1_SETTLE_MS} ms`
      )
    }
    await sleep(FEATURE_POLL_MS)
  }
}

/**
 * A settled outcome, so a case can assert on a rejection after the steps that follow it.
 * @param {Promise<unknown>} promise
 * @returns {Promise<{ value: unknown, error: Error|null }>}
 */
const outcomeOf = (promise) =>
  promise.then(
    (value) => ({ value, error: null }),
    (error) => ({ value: null, error })
  )

/**
 * Runs `scripts/e2e/transactionCrashMember.js` and kills it with SIGKILL the moment it reports its
 * records acknowledged, before it could send anything else, so its transaction stays open on the
 * coordinator. Its own request log is the proof no EndTxn left it.
 * @param {object} options
 * @param {string} options.topic
 * @param {string} options.transactionalId
 * @param {number} options.records
 * @param {number} options.deadlineMs how long it may take to report; then it is killed
 * @param {(child: import('child_process').ChildProcess) => void} [options.track] receives the
 *   child, so the caller's teardown can kill it
 * @returns {Promise<{ requests: string[], killedAt: number }>} every request it saw answered
 * @throws {Error} when it exits on its own, or misses the deadline, with its stderr; the spawn
 *   error when it cannot be started
 */
const crashAfterProduce = ({ topic, transactionalId, records, deadlineMs, track = () => {} }) =>
  new Promise((resolve, reject) => {
    const requests = []
    let stderr = ''
    let pending = ''
    let crashed = false
    const child = spawn(
      process.execPath,
      [
        CRASH_MEMBER,
        '--brokers',
        plainTextBrokers().join(','),
        '--topic',
        topic,
        '--transactional-id',
        transactionalId,
        '--records',
        String(records),
      ],
      { stdio: ['ignore', 'pipe', 'pipe'] }
    )
    track(child)
    const deadline = setTimeout(() => {
      child.kill('SIGKILL')
      reject(new Error(`The crash member did not produce within ${deadlineMs} ms: ${stderr}`))
    }, deadlineMs)
    child.stderr.setEncoding('utf8')
    child.stderr.on('data', (chunk) => (stderr += chunk))
    child.stdout.setEncoding('utf8')
    child.stdout.on('data', (chunk) => {
      const lines = (pending + chunk).split('\n')
      pending = lines.pop()
      for (const line of lines) {
        if (line.startsWith('REQUEST ')) {
          requests.push(line.slice('REQUEST '.length))
        } else if (line === `PRODUCED ${records}` && !crashed) {
          crashed = true
          child.kill('SIGKILL')
          clearTimeout(deadline)
          resolve({ requests, killedAt: Date.now() })
        }
      }
    })
    child.on('exit', (code) => {
      if (!crashed) {
        clearTimeout(deadline)
        reject(new Error(`The crash member exited with ${code} before producing: ${stderr}`))
      }
    })
    child.on('error', (error) => {
      if (!crashed) {
        clearTimeout(deadline)
        reject(error)
      }
    })
  })

module.exports = { settledTransactionVersion, outcomeOf, crashAfterProduce }
