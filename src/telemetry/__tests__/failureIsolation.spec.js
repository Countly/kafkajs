const {
  PublicTelemetryRound,
  TELEMETRY_KINDS,
  telemetryOutcome,
  assertTelemetryOutcome,
} = require('../../../testHelpers/lifecycle')
const gzip = require('../../protocol/message/compression/gzip')

describe('original outcome oracle', () => {
  const sentinel = () => Object.freeze({ marker: Symbol('foreground') })
  test.each(['return', 'throw', 'reject', 'undefined'])(
    'keeps %s identity, receiver, arguments and count',
    async (mode) => {
      const value = sentinel()
      const receiver = {}
      const args = [sentinel()]
      let calls = 0
      const original = function (...received) {
        calls += 1
        expect(this).toBe(receiver)
        expect(received[0]).toBe(args[0])
        if (mode === 'throw') throw value
        if (mode === 'reject') return Promise.reject(value)
        if (mode === 'undefined') {
          // eslint-disable-next-line no-throw-literal
          throw undefined
        }
        return value
      }
      const actual = await telemetryOutcome(() => original.apply(receiver, args))
      const kind = { return: 'return', throw: 'throw', reject: 'reject', undefined: 'throw' }[mode]
      assertTelemetryOutcome(
        actual,
        {
          kind,
          [kind === 'return' ? 'value' : 'reason']: mode === 'undefined' ? undefined : value,
        },
        calls
      )
    }
  )
})

/**
 * Private telemetry boundaries made to fail one at a time, keyed by their plan ids. Each injector
 * returns the number-of-hits reader; every failure is a unique object, never read for diagnostics.
 */
const INJECTORS = {
  O02: { early: true, inject: (round, s) => failing(round, round.metrics, 'observeConnection', s) },
  O03: { early: true, inject: (round, s) => failing(round, round.metrics, 'observeRequest', s) },
  O05: { inject: (round, s) => failing(round, round.metrics, 'snapshot', s) },
  O09: { inject: (round, s) => failing(round, gzip, 'compress', s, true) },
  O11: { inject: (round, s) => failing(round, round.metrics, 'written', s) },
  O15: { inject: (round, s) => failing(round, round.machine, 'run', s, true) },
}

/**
 * Replaces one private function with a counting failure.
 * @param {PublicTelemetryRound} round owner of the patch @param {object} target owner of the function
 * @param {string} key function name @param {object} sentinel unique failure
 * @param {boolean} [async] whether the failure is a rejection @returns {{hits:number}} live counter
 */
function failing(round, target, key, sentinel, async = false) {
  const counter = { hits: 0 }
  round.patch(
    target,
    key,
    () =>
      function () {
        counter.hits += 1
        if (async) return Promise.reject(sentinel)
        throw sentinel
      }
  )
  return counter
}

const settle = async () => {
  for (let turn = 0; turn < 200; turn++) await new Promise(setImmediate)
}

const drive = async (round) => {
  if (round.machine.subscription === null && round.machine.timer !== null) {
    round.tick()
    await round.until(
      () => round.machine.subscription !== null || round.machine.state === 'STOPPED'
    )
  }
  if (round.machine.timer !== null) round.tick()
  await settle()
}

describe.each(TELEMETRY_KINDS)(
  '%s foreground outcome under a private telemetry failure',
  (kind) => {
    const control = async () => {
      const round = new PublicTelemetryRound({ kind })
      const outcomes = {}
      try {
        await round.start({ points: 'large', subscription: { acceptedCompressionTypes: [1, 0] } })
        outcomes.connected = round.machine.connected
        outcomes.close = await telemetryOutcome(() =>
          round.client.disconnect(round.closeOptions(1000))
        )
        return outcomes
      } finally {
        await round.dispose()
      }
    }

    test.each(Object.keys(INJECTORS))(
      '%s leaves connect and close unchanged',
      async (id) => {
        const expected = await control()
        const round = new PublicTelemetryRound({ kind })
        const sentinel = Object.freeze({ marker: Symbol(id) })
        const { early, inject } = INJECTORS[id]
        let injected
        try {
          await round.start({
            points: 'large',
            subscription: { acceptedCompressionTypes: [1, 0] },
            beforeConnect: early ? (started) => (injected = inject(started, sentinel)) : null,
          })
          if (!early) {
            injected = inject(round, sentinel)
            await drive(round)
          }
          const close = await telemetryOutcome(() =>
            round.client.disconnect(round.closeOptions(1000))
          )
          expect(close).toEqual(expected.close)
          expect(close.kind).toBe('return')
          expect(injected.hits).toBeGreaterThan(0)
          expect(round.unhandled).toEqual([])
        } finally {
          await round.dispose()
        }
      },
      60000
    )

    test('a disabled client never reaches any telemetry injector', async () => {
      const round = new PublicTelemetryRound({ kind, enabled: false })
      try {
        await round.start()
        expect(round.machine).toBeNull()
        const close = await telemetryOutcome(() =>
          round.client.disconnect(round.closeOptions(1000))
        )
        expect(close.kind).toBe('return')
        expect(round.telemetryRequests()).toEqual([])
      } finally {
        await round.dispose()
      }
    }, 60000)
  }
)

describe('hostile diagnostics are never read', () => {
  test('a throwing message getter on a telemetry failure is not touched', async () => {
    const round = new PublicTelemetryRound({ kind: 'producer' })
    try {
      await round.start()
      let reads = 0
      const hostile = {
        get message() {
          reads += 1
          throw new Error('read')
        },
      }
      round.patch(round.metrics, 'snapshot', () => () => {
        throw hostile
      })
      round.machine.logger = {
        warn() {
          throw new Error('logger')
        },
      }
      await round.ready().catch(() => {})
      for (let turn = 0; turn < 200; turn++) await new Promise(setImmediate)
      expect(reads).toBe(0)
      expect(round.unhandled).toEqual([])
      await expect(round.client.disconnect({ timeout: 0 })).resolves.toBeUndefined()
    } finally {
      await round.dispose()
    }
  }, 60000)
})
