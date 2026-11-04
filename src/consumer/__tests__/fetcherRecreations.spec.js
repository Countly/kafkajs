const { FetcherRecreations, MAX_TIMES } = require('./fixtures/fetcherRecreations')
const { createLogger, LEVELS } = require('../../loggers')

const message = 'Reconciling fetchers'

describe('Consumer > Fetcher reconciliation observation', () => {
  it('records every reconciliation from the public log creator on the wire clock', () => {
    jest.useFakeTimers()
    try {
      const observation = new FetcherRecreations()
      const logger = createLogger({ level: LEVELS.DEBUG, logCreator: observation.logCreator })
      jest.advanceTimersByTime(3000)
      logger.namespace('FetchManager').debug(message, { added: [4], removed: [] })
      jest.advanceTimersByTime(1000)
      logger.namespace('FetchManager').debug(message, { added: [], removed: [4] })
      expect(observation.times).toEqual([3000, 4000])
      expect(observation.since(3500)).toEqual({
        reconciliations: [{ at: 4000, added: [], removed: [4] }],
        fetchers: [],
        dropped: 0,
      })
    } finally {
      jest.useRealTimers()
    }
  })

  it('records each fetcher start and stop with its node', () => {
    let at = 0
    const observation = new FetcherRecreations(() => (at += 10))
    const log = observation.logCreator(5)
    log({ namespace: 'FetchManager', log: { message: 'Fetcher started', nodeId: 2 } })
    log({ namespace: 'FetchManager', log: { message: 'Fetcher stopped', nodeId: 2 } })
    expect(observation.since(0).fetchers).toEqual([
      { at: 10, nodeId: 2, event: 'started' },
      { at: 20, nodeId: 2, event: 'stopped' },
    ])
  })

  it('ignores other namespaces, messages and consumers', () => {
    const observation = new FetcherRecreations(() => 3000)
    const otherConsumer = new FetcherRecreations(() => 3000)
    const log = observation.logCreator(5)
    log({ namespace: 'Consumer', log: { message } })
    log({ namespace: 'FetchManager', log: { message: 'Stopped fetchers' } })
    otherConsumer.logCreator(5)({ namespace: 'FetchManager', log: { message } })
    expect(observation.times).toEqual([])
    expect(observation.since(0)).toEqual({ reconciliations: [], fetchers: [], dropped: 0 })
  })

  it('keeps a bounded number of entries and counts the rest', () => {
    let at = 0
    const observation = new FetcherRecreations(() => (at += 1))
    const log = observation.logCreator(5)
    for (let n = 0; n < MAX_TIMES + 5; n++) {
      log({ namespace: 'FetchManager', log: { message, added: [], removed: [] } })
    }
    expect(observation.times).toHaveLength(MAX_TIMES)
    expect(observation.since(MAX_TIMES).reconciliations).toHaveLength(1)
    expect(observation.since(MAX_TIMES).dropped).toBe(5)
  })
})
