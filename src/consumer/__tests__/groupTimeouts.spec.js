const resolveGroupTimeouts = require('../groupTimeouts')
const { newLogger } = require('testHelpers')
const { KafkaJSNonRetriableError } = require('../../errors')

const resolve = (options) => {
  const logger = newLogger()
  const warn = jest.spyOn(logger, 'warn')
  return { resolved: resolveGroupTimeouts({ logger, ...options }), warn }
}

describe('Consumer > group timeouts', () => {
  describe('the alias table', () => {
    const ROWS = [
      // [maxPollInterval, rebalanceTimeout, wire, joinGroupReq, syncGroupReq, deadline, warns]
      [undefined, undefined, 300000, 305000, 65000, 300000, false],
      [undefined, 20000, 20000, 25000, 25000, 300000, true],
      [undefined, 0, 0, 35000, 35000, 300000, true],
      [30000, undefined, 30000, 35000, 35000, 30000, false],
      [45000, 45000, 45000, 50000, 50000, 45000, true],
      [45000, 20000, 45000, 50000, 50000, 45000, true],
    ]

    test.each(ROWS)(
      'maxPollInterval=%p rebalanceTimeout=%p',
      // `joinReq` stays in ROWS as documentation only; it is asserted through the codec in
      // syncGroupTimeout.spec.js, so it is not destructured here and nothing is unused.
      (maxPollInterval, rebalanceTimeout, wire, _joinReq, syncReq, deadline, warns) => {
        const { resolved, warn } = resolve({ maxPollInterval, rebalanceTimeout })

        expect(resolved.rebalanceTimeout).toEqual(wire)
        expect(resolved.pollDeadline).toEqual(deadline)
        expect(resolved.syncGroupTimeout).toEqual(syncReq)
        expect(warn.mock.calls.length > 0).toBe(warns)
      }
    )

    it('names both values and the winner when they disagree', () => {
      const { warn } = resolve({ maxPollInterval: 45000, rebalanceTimeout: 20000 })

      expect(warn).toHaveBeenCalledWith(
        expect.stringContaining('maxPollInterval'),
        expect.objectContaining({ maxPollInterval: 45000, rebalanceTimeout: 20000 })
      )
    })
  })

  describe('validation', () => {
    it('throws when heartbeatInterval is not lower than sessionTimeout, with the shipped message', () => {
      expect(() =>
        resolveGroupTimeouts({
          logger: newLogger(),
          heartbeatInterval: 10000,
          sessionTimeout: 10000,
        })
      ).toThrow(
        'Consumer heartbeatInterval (10000) must be lower than sessionTimeout (10000). It is recommended to set heartbeatInterval to approximately a third of the sessionTimeout.'
      )
    })

    it('warns when heartbeatInterval exceeds a third of sessionTimeout without reaching it', () => {
      const { warn } = resolve({ heartbeatInterval: 25000, sessionTimeout: 30000 })

      expect(warn).toHaveBeenCalledWith(
        expect.stringContaining('heartbeatInterval'),
        expect.objectContaining({ heartbeatInterval: 25000, sessionTimeout: 30000 })
      )
    })

    it.each([0, -1, 1.5, '300000', null])(
      'throws for an explicit maxPollInterval of %p',
      (value) => {
        expect(() => resolveGroupTimeouts({ logger: newLogger(), maxPollInterval: value })).toThrow(
          'maxPollInterval'
        )
      }
    )

    it.each([
      ['a Symbol', Symbol('poll'), 'Symbol(poll)'],
      ['a string', '300000', "'300000'"],
    ])('refuses %s as KafkaJSNonRetriableError, showing the value', (_name, value, shown) => {
      const resolveWith = () =>
        resolveGroupTimeouts({ logger: newLogger(), maxPollInterval: value })

      expect(resolveWith).toThrow(KafkaJSNonRetriableError)
      expect(resolveWith).toThrow(`got ${shown}`)
    })

    it.each([0, 1.5])('grandfathers a rebalanceTimeout of %p', (value) => {
      expect(() =>
        resolveGroupTimeouts({ logger: newLogger(), rebalanceTimeout: value })
      ).not.toThrow()
    })
  })

  it('reaches createConsumer from kafka.consumer()', () => {
    const { Kafka, logLevel } = require('../../../index')
    // A logCreator must return a FUNCTION: `src/loggers/index.js:37` calls `logCreator(level)`
    // and then invokes the result. Returning a logger object makes every log call a TypeError,
    // and a spy on a namespaced logger sees nothing, because `namespace()` returns a fresh object.
    const lines = []
    const kafka = new Kafka({
      clientId: 'timeouts',
      brokers: ['localhost:9092'],
      logLevel: logLevel.WARN,
      logCreator: () => (entry) => lines.push(entry),
    })
    const deprecations = () =>
      lines.filter(({ log }) => String(log.message).includes('rebalanceTimeout'))

    // A consumer that never connects still runs the whole construction-time timeout resolution.
    expect(() => kafka.consumer({ groupId: 'g', maxPollInterval: 0 })).toThrow('maxPollInterval')
    expect(() => kafka.consumer({ groupId: 'g', maxPollInterval: 30000 })).not.toThrow()
    expect(deprecations()).toHaveLength(0)

    kafka.consumer({ groupId: 'g', rebalanceTimeout: 20000 })
    expect(deprecations()).toHaveLength(1)
  })
})
