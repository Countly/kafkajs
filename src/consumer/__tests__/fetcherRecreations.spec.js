const { FetcherRecreations, MAX_TIMES } = require('./fixtures/fetcherRecreations')
const { createLogger, LEVELS } = require('../../loggers')

const message = 'Rebalancing fetchers...'

describe('Consumer > Fetcher recreation observation', () => {
  it('records every recreation from the public log creator on the wire clock', () => {
    jest.useFakeTimers()
    try {
      const observation = new FetcherRecreations()
      const logger = createLogger({ level: LEVELS.DEBUG, logCreator: observation.logCreator })
      jest.advanceTimersByTime(3000)
      logger.namespace('FetchManager').debug(message)
      jest.advanceTimersByTime(1000)
      logger.namespace('FetchManager').debug(message)
      expect(observation.times).toEqual([3000, 4000])
      expect(observation.since(3500)).toEqual({ times: [4000], dropped: 0 })
    } finally {
      jest.useRealTimers()
    }
  })

  it('ignores other namespaces, messages and consumers', () => {
    const observation = new FetcherRecreations(() => 3000)
    const otherConsumer = new FetcherRecreations(() => 3000)
    const log = observation.logCreator(5)
    log({ namespace: 'Consumer', log: { message } })
    log({ namespace: 'FetchManager', log: { message: 'Stopped fetchers' } })
    otherConsumer.logCreator(5)({ namespace: 'FetchManager', log: { message } })
    expect(observation.times).toEqual([])
    expect(observation.since(0)).toEqual({ times: [], dropped: 0 })
  })

  it('keeps a bounded number of times and counts the rest', () => {
    let at = 0
    const observation = new FetcherRecreations(() => (at += 1))
    const log = observation.logCreator(5)
    for (let n = 0; n < MAX_TIMES + 5; n++) log({ namespace: 'FetchManager', log: { message } })
    expect(observation.times).toHaveLength(MAX_TIMES)
    expect(observation.since(MAX_TIMES)).toEqual({ times: [MAX_TIMES], dropped: 5 })
  })
})
