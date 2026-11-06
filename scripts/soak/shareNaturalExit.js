#!/usr/bin/env node
'use strict'

/**
 * Natural-exit probe of the share consumer's close outcomes.
 *
 * Usage: NODE_PATH=. node scripts/soak/shareNaturalExit.js   (from the repository root: the scripted
 * cluster's fixtures require `testHelpers` by its bare name)
 * Prints one JSON line when every outcome was measured:
 *   {"event":"share-natural-exit","outcomes":[{"outcome":"success","leaked":{},"openSockets":0},...]}
 * and exits on its own, with the code 0, when no outcome left a timer, an immediate or an open
 * socket behind. The exit code starts at 1 and is set to 0 only after the last outcome passed, so a
 * hang that empties the event loop, a thrown error or a leak is never reported as a pass. It never
 * calls `process.exit`: a ref'd timer that outlives a close keeps the process alive, and the caller
 * bounds the run (the spec of this file runs it as a child process with a hard timeout).
 *
 * It drives a real `Kafka` client and share consumer with real timers over the in-process scripted
 * cluster, so it needs no broker and no Docker. Each outcome runs on a fresh client, after which
 * every held gate is released. Timers are counted with
 * `process.getActiveResourcesInfo()`, which lists only the ones that keep the event loop alive.
 * Outcomes: a clean stop, a zero-timeout disconnect, a disconnect that outlives its deadline with
 * a written final unanswered, a final that fails on the wire, and a terminal heartbeat failure. Each ends with a zero-timeout
 * `disconnect()`, which closes every socket of the client and needs no gate.
 */
const {
  acquire,
  deferred,
  fetchAnswer,
  shareClient,
  until,
} = require('../../testHelpers/shareHarness')
const { createResourceProbe } = require('../../testHelpers/resourceProbe')

/** The resource types a leaked client timer or retry would show up as. */
const TRACKED = ['Timeout', 'Immediate']
/** How long a closed client may take to hand its last timer back, in milliseconds. */
const SETTLE_MS = 2000
/** Selects the final acknowledgement, the one at epoch -1. */
const isFinal = ({ fields }) => fields.shareSessionEpoch === -1

const probe = createResourceProbe({ activeResources: true })

/**
 * @returns {Promise<Record<string, number>>} The event-loop-holding timers and immediates, by type.
 */
const counts = async () => {
  const { activeResources } = await probe.snapshot()
  return Object.fromEntries(
    TRACKED.filter((type) => activeResources[type]).map((type) => [type, activeResources[type]])
  )
}

/**
 * Polls with immediates, never a timer, until the counts equal the baseline or the time is up.
 * @param {Record<string, number>} baseline The counts before any client existed.
 * @returns {Promise<Record<string, number>>} What is still held beyond the baseline, by type.
 */
const leakedAfterSettling = async (baseline) => {
  const leaked = async () => {
    const now = await counts()
    const extra = {}
    for (const type of TRACKED) {
      const surplus = (now[type] ?? 0) - (baseline[type] ?? 0)
      if (surplus > 0) extra[type] = surplus
    }
    return extra
  }
  const deadline = Date.now() + SETTLE_MS
  let extra = await leaked()
  while (Object.keys(extra).length > 0 && Date.now() < deadline) {
    await new Promise(setImmediate)
    extra = await leaked()
  }
  return extra
}

/**
 * Subscribes and runs a handler that decides its record, then optionally waits for a gate.
 * @param {object} h A `shareClient`.
 * @param {boolean} hold Whether the handler waits for the returned gate.
 * @returns {Promise<{gate: object, onCrash: Promise<void>}>} The handler's gate, and a promise
 *   that settles when the run crashes.
 */
const begin = async (h, hold) => {
  const crashed = deferred()
  h.consumer.on(h.consumer.events.CRASH, () => crashed.resolve())
  await h.consumer.subscribe({ topics: ['topic-a'] })
  h.script('ShareFetch', [fetchAnswer([acquire(0, 0, ['a'])])])
  const gate = deferred()
  const started = deferred()
  await h.consumer.run({
    eachMessage: async ({ acknowledge }) => {
      acknowledge()
      started.resolve()
      if (hold) await gate.promise
    },
  })
  await started.promise
  return { gate, onCrash: crashed.promise }
}

/** One close outcome each: the client is driven to it, and every hold is released afterwards. */
const OUTCOMES = {
  async success(h) {
    await begin(h, false)
    await h.consumer.stop()
  },
  async zero(h) {
    const { gate } = await begin(h, true)
    await h.consumer.disconnect({ timeout: 0 })
    gate.resolve()
  },
  async forced(h) {
    const { gate } = await begin(h, true)
    const final = h.hold({ api: 'ShareAcknowledge', match: isFinal })
    const closing = h.consumer.disconnect({ timeout: 400 })
    await final.arrived()
    await closing
    gate.resolve()
    final.release()
  },
  async failed(h) {
    await begin(h, false)
    const final = h.hold({ api: 'ShareAcknowledge', match: isFinal })
    const closing = h.consumer.stop()
    await final.arrived()
    final.fail(new Error('the final failed on the wire'))
    await closing
  },
  async crash(h) {
    const { gate, onCrash } = await begin(h, true)
    h.trigger.code = 30
    await onCrash
    gate.resolve()
  },
}

/**
 * Drives one outcome on a fresh client, closes it for good and measures what it left behind.
 * @param {string} name The outcome's key in `OUTCOMES`.
 * @param {Record<string, number>} baseline The counts before any client existed.
 * @returns {Promise<{outcome: string, leaked: Record<string, number>, openSockets: number}>}
 */
const measure = async (name, baseline) => {
  const h = shareClient({ interval: 25 })
  h.trigger = { code: 0 }
  h.script(
    'ShareGroupHeartbeat',
    Array.from({ length: 10000 }, () => () => {
      const { code } = h.trigger
      if (code === 0) return undefined
      h.trigger.code = 0
      return { errorCode: code }
    })
  )
  try {
    await OUTCOMES[name](h)
  } finally {
    await h.consumer.disconnect({ timeout: 0 })
  }
  await until(() => h.cluster.sockets().every(({ destroyed }) => destroyed), 'every socket closed')
  return {
    outcome: name,
    leaked: await leakedAfterSettling(baseline),
    openSockets: h.cluster.sockets().filter(({ destroyed }) => !destroyed).length,
  }
}

/**
 * @returns {Promise<void>} Measures every outcome in turn, prints the report and sets the exit code.
 */
const main = async () => {
  process.exitCode = 1
  const baseline = await counts()
  const outcomes = []
  for (const name of Object.keys(OUTCOMES)) outcomes.push(await measure(name, baseline))
  process.stdout.write(`${JSON.stringify({ event: 'share-natural-exit', outcomes })}\n`)
  if (outcomes.every(({ leaked, openSockets }) => openSockets === 0 && !Object.keys(leaked).length))
    process.exitCode = 0
}

main().catch((error) => {
  process.stderr.write(`${error.stack}\n`)
})
