const { FetcherRecreations } = require('./fixtures/fetcherRecreations')
const { createLogger, LEVELS } = require('../../loggers')

const message = 'Rebalancing fetchers...'
const moves = [
  { errorAt: 100, nextAt: 3010 },
  { errorAt: 100, nextAt: 3020 },
]

describe('Consumer > Fetcher recreation observation', () => {
  it('captures the shared drain boundary from the public log creator on the wire clock', () => {
    jest.useFakeTimers()
    try {
      const observation = new FetcherRecreations()
      const logger = createLogger({ level: LEVELS.DEBUG, logCreator: observation.logCreator })
      jest.advanceTimersByTime(3000)
      logger.namespace('FetchManager').debug(message)
      expect(observation.boundary(moves)).toEqual({ nodeSetChanged: true, drainEndAt: 3000 })
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
    expect(observation.boundary(moves)).toEqual({ nodeSetChanged: false, drainEndAt: null })
  })

  it('gives no boundary without a rebalance log', () => {
    expect(new FetcherRecreations().boundary(moves)).toEqual({
      nodeSetChanged: false,
      drainEndAt: null,
    })
  })

  it('excludes logs outside the loss and first destination start', () => {
    let at = 100
    const observation = new FetcherRecreations(() => at)
    const log = observation.logCreator(5)
    for (at of [90, 100, 3010, 3020]) log({ namespace: 'FetchManager', log: { message } })
    expect(observation.boundary(moves)).toEqual({ nodeSetChanged: false, drainEndAt: null })
  })
})
