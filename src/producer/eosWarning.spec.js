const createProducer = require('./index')
const { createLogger, LEVELS } = require('../loggers')

const fakeCluster = () => ({ retry: {} })

/** A logger that captures every log entry instead of writing anywhere, so a warn can be asserted on. */
const capturingLogger = () => {
  const entries = []
  const logger = createLogger({
    level: LEVELS.WARN,
    logCreator: () => (entry) => entries.push(entry),
  })
  return { logger, entries }
}

describe('Producer > idempotent retries warning (Countly 2i)', () => {
  const warningsFor = (retries) => {
    const { logger, entries } = capturingLogger()
    createProducer({ cluster: fakeCluster(), logger, idempotent: true, retry: { retries } })
    return entries.filter(({ log }) => /EoS guarantees/.test(log.message))
  }

  it('does not warn for an explicit, generous retry budget', () => {
    expect(warningsFor(100)).toHaveLength(0)
    expect(warningsFor(5)).toHaveLength(0)
    expect(warningsFor(Number.MAX_SAFE_INTEGER)).toHaveLength(0)
  })

  it('still warns when retries are set below the non-idempotent default', () => {
    expect(warningsFor(2)).toHaveLength(1)
  })

  it('still refuses retries < 1', () => {
    const { logger } = capturingLogger()
    expect(() =>
      createProducer({ cluster: fakeCluster(), logger, idempotent: true, retry: { retries: 0 } })
    ).toThrow('Idempotent producer must allow retries')
  })
})
