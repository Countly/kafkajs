/**
 * Helpers shared by the live consumer scenarios: bounded polling, bounded teardown, and the offset
 * ledger every delivery assertion is made against.
 *
 * Kept out of `testHelpers/` deliberately: `jest.config.js`'s `moduleNameMapper` rewrites any
 * request containing `testHelpers` to `testHelpers/index.js`, so a second module there cannot be
 * required by name.
 */

/** @param {number} ms @returns {Promise<void>} */
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

/**
 * Polls `predicate` on a plain interval until it is true, bounded rather than indefinite: a
 * condition that never holds must fail on the assertion that follows, not hang for the suite
 * timeout.
 * @param {() => boolean|Promise<boolean>} predicate
 * @param {{attempts: number, delay: number}} budget
 * @returns {Promise<boolean>} whether `predicate` became true within the attempt budget
 */
const waitForCondition = async (predicate, budget) => {
  for (let attempt = 0; attempt < budget.attempts; attempt++) {
    if (await predicate()) {
      return true
    }
    await sleep(budget.delay)
  }
  return false
}

/**
 * Awaits `promise` but gives up after `ms`, so a teardown can never outlive its hook budget.
 * @param {Promise<unknown>} promise
 * @param {number} ms
 * @returns {Promise<void>}
 */
const withTimeout = async (promise, ms) => {
  let timer
  const deadline = new Promise((resolve) => {
    timer = setTimeout(resolve, ms)
  })
  try {
    await Promise.race([promise, deadline])
  } finally {
    clearTimeout(timer)
  }
}

/**
 * The record every delivery assertion is made against: what was produced, what was delivered, and
 * by which member.
 *
 * Keyed `topic|partition|offset`, so "delivered twice" and "never delivered" are both countable and
 * a duplicate can be attributed to the member that produced it.
 * @returns {{ produced: (record: {topic: string, partition: number, offset: string}) => void,
 *             delivered: (member: string, record: {topic: string, partition: number, offset: string}) => void,
 *             deliveriesBy: (member: string) => number,
 *             summary: () => { produced: number, delivered: number, duplicates: string[], missing: string[] } }}
 */
const createLedger = () => {
  const produced = new Set()
  /** @type {Map<string, string[]>} key -> the members that delivered it, in order */
  const delivered = new Map()

  const key = ({ topic, partition, offset }) => `${topic}|${partition}|${offset}`

  return {
    produced: (record) => produced.add(key(record)),
    delivered: (member, record) => {
      const k = key(record)
      delivered.set(k, [...(delivered.get(k) || []), member])
    },
    deliveriesBy: (member) =>
      [...delivered.values()].reduce(
        (count, members) => count + members.filter((m) => m === member).length,
        0
      ),
    summary: () => ({
      produced: produced.size,
      delivered: delivered.size,
      duplicates: [...delivered.entries()]
        .filter(([, members]) => members.length > 1)
        .map(([k]) => k),
      missing: [...produced].filter((k) => !delivered.has(k)),
    }),
  }
}

module.exports = { sleep, waitForCondition, withTimeout, createLedger }
