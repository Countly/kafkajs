const { KafkaJSConnectionError, KafkaJSNonRetriableError } = require('../../errors')
const { liveRun } = require('../../producer/__tests__/fixtures/bounded')
const { retryTransient } = require('./fixtures/transientRetry')

const reset = () => new KafkaJSConnectionError('Connection error: read ECONNRESET')

describe('Consumer > live fixtures > retrying a transient lookup', () => {
  afterEach(() => liveRun.reset())

  it('looks again after a reset connection and returns the answer of the same lookup', async () => {
    const op = jest.fn().mockRejectedValueOnce(reset()).mockResolvedValue({ nodeId: 1 })

    await expect(
      retryTransient(new AbortController().signal, 2000, op, { delayMs: 1 })
    ).resolves.toEqual({
      nodeId: 1,
    })
    expect(op).toHaveBeenCalledTimes(2)
  })

  it('stops at the deadline when every attempt is reset', async () => {
    const op = jest.fn().mockRejectedValue(reset())

    await expect(
      retryTransient(new AbortController().signal, 100, op, { delayMs: 10, label: 'The lookup' })
    ).rejects.toThrow('The lookup did not settle within 100 ms')
    expect(op.mock.calls.length).toBeGreaterThan(1)
  })

  it('throws a failure that is not retriable at once', async () => {
    const failure = new KafkaJSNonRetriableError('denied')
    const op = jest.fn().mockRejectedValue(failure)

    await expect(retryTransient(new AbortController().signal, 2000, op)).rejects.toBe(failure)
    expect(op).toHaveBeenCalledTimes(1)
  })

  it('makes no further lookup once the scope has aborted', async () => {
    const owner = new AbortController()
    const reason = new Error('the case ended')
    const op = jest.fn(async () => {
      owner.abort(reason)
      throw reset()
    })

    await expect(retryTransient(owner.signal, 2000, op, { delayMs: 1 })).rejects.toBe(reason)
    expect(op).toHaveBeenCalledTimes(1)
  })

  it('does not start under a scope that has already aborted', async () => {
    const owner = new AbortController()
    owner.abort(new Error('closed'))
    const op = jest.fn()

    await expect(retryTransient(owner.signal, 2000, op)).rejects.toThrow('closed')
    expect(op).not.toHaveBeenCalled()
  })
})
