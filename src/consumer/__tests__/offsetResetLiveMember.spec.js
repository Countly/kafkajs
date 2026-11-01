const { Member } = require('./fixtures/offsetResetMember')

describe('offset-reset live Member > start', () => {
  test('never subscribes or runs once the case scope aborted during connect', async () => {
    const { bounded } = require('../../producer/__tests__/fixtures/bounded')
    const events = {}
    let releaseConnect
    const consumer = {
      events,
      on: jest.fn(),
      connect: jest.fn(() => new Promise((resolve) => (releaseConnect = resolve))),
      subscribe: jest.fn(async () => {}),
      run: jest.fn(async () => {}),
    }
    const caseScope = new AbortController()
    const member = new Member({ consumer, topic: 't', subscribe: {}, scope: caseScope.signal })

    const started = bounded(caseScope.signal, Infinity, () => member.start())
    const outcome = started.catch((error) => error)
    await Promise.resolve()
    expect(consumer.connect).toHaveBeenCalledTimes(1)
    caseScope.abort(new Error('case aborted'))
    expect((await outcome).message).toBe('case aborted')

    releaseConnect()
    await new Promise((resolve) => setImmediate(resolve))
    expect(consumer.subscribe).not.toHaveBeenCalled()
    expect(consumer.run).not.toHaveBeenCalled()
  })
})
