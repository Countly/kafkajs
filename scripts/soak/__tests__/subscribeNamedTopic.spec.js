const { ChurnMember } = require('../consumerChurnRun')
const { subscribeNamedTopic } = require('../subscribeNamedTopic')

describe('Soak named-topic subscription', () => {
  let consumer
  let member
  const topic = 'soak-created-topic'

  beforeEach(() => {
    jest.useFakeTimers()
    consumer = {
      connect: jest.fn().mockResolvedValue(),
      subscribe: jest.fn().mockResolvedValue(),
      run: jest.fn().mockResolvedValue(),
      disconnect: jest.fn().mockResolvedValue(),
    }
    member = new ChurnMember({ consumer })
  })

  afterEach(() => jest.useRealTimers())

  test('retries raw and wrapped unknown-topic errors before running', async () => {
    consumer.subscribe
      .mockRejectedValueOnce({ type: 'UNKNOWN_TOPIC_OR_PARTITION' })
      .mockRejectedValueOnce({ cause: { type: 'UNKNOWN_TOPIC_OR_PARTITION' } })
    const started = member.start([topic], { fromBeginning: false })
    await jest.advanceTimersByTimeAsync(0)
    expect(consumer.subscribe).toHaveBeenCalledTimes(1)
    expect(consumer.run).not.toHaveBeenCalled()
    await jest.advanceTimersByTimeAsync(100)
    expect(consumer.subscribe).toHaveBeenCalledTimes(2)
    expect(consumer.run).not.toHaveBeenCalled()
    await jest.advanceTimersByTimeAsync(100)
    await expect(started).resolves.toBeUndefined()
    expect(consumer.subscribe.mock.calls).toEqual(Array(3).fill([{ topic, fromBeginning: false }]))
    expect(consumer.subscribe.mock.calls[1][0]).toBe(consumer.subscribe.mock.calls[0][0])
    expect(consumer.subscribe.mock.calls[2][0]).toBe(consumer.subscribe.mock.calls[0][0])
    expect(consumer.run).toHaveBeenCalledTimes(1)
  })

  test('rejects with the last unknown-topic error at the five-second deadline', async () => {
    let lastError
    consumer.subscribe.mockImplementation(async () => {
      lastError = Object.assign(new Error('Topic not visible'), {
        type: 'UNKNOWN_TOPIC_OR_PARTITION',
      })
      throw lastError
    })
    const started = member.start([topic])
    const rejected = started.catch((error) => error)
    let settled = false
    started.catch(() => {
      settled = true
    })
    await jest.advanceTimersByTimeAsync(4999)
    expect(settled).toBe(false)
    expect(consumer.subscribe).toHaveBeenCalledTimes(50)
    await jest.advanceTimersByTimeAsync(1)
    expect(await rejected).toBe(lastError)
    expect(consumer.subscribe).toHaveBeenCalledTimes(50)
    expect(consumer.run).not.toHaveBeenCalled()
  })

  test('rejects other errors immediately', async () => {
    const error = Object.assign(new Error('Authorization failed'), {
      type: 'TOPIC_AUTHORIZATION_FAILED',
    })
    consumer.subscribe.mockRejectedValue(error)
    await expect(member.start([topic])).rejects.toBe(error)
    expect(consumer.subscribe).toHaveBeenCalledTimes(1)
    expect(jest.getTimerCount()).toBe(0)
    expect(consumer.run).not.toHaveBeenCalled()
  })

  test('stopping a member prevents further subscription attempts', async () => {
    consumer.subscribe.mockRejectedValue({ type: 'UNKNOWN_TOPIC_OR_PARTITION' })
    const started = member.start([topic])
    await jest.advanceTimersByTimeAsync(0)
    expect(consumer.subscribe).toHaveBeenCalledTimes(1)
    await member.stop()
    await jest.advanceTimersByTimeAsync(5000)
    await expect(started).resolves.toBeUndefined()
    expect(consumer.subscribe).toHaveBeenCalledTimes(1)
    expect(consumer.run).not.toHaveBeenCalled()
  })

  test('a stopped owner skips the first subscription attempt', async () => {
    await expect(
      subscribeNamedTopic(consumer, { topic, fromBeginning: true }, () => true)
    ).resolves.toBe(false)
    expect(consumer.subscribe).not.toHaveBeenCalled()
  })

  test('stopping during a successful subscription prevents running', async () => {
    let subscribed
    consumer.subscribe.mockImplementation(() => new Promise((resolve) => (subscribed = resolve)))
    const started = member.start([topic])
    await jest.advanceTimersByTimeAsync(0)
    await member.stop()
    subscribed()
    await expect(started).resolves.toBeUndefined()
    expect(consumer.subscribe).toHaveBeenCalledTimes(1)
    expect(consumer.run).not.toHaveBeenCalled()
  })

  test('regex subscription errors are not retried', async () => {
    const error = { type: 'UNKNOWN_TOPIC_OR_PARTITION' }
    consumer.subscribe.mockRejectedValue(error)
    await expect(member.start([/^soak-/])).rejects.toBe(error)
    expect(consumer.subscribe).toHaveBeenCalledTimes(1)
    expect(jest.getTimerCount()).toBe(0)
    expect(consumer.run).not.toHaveBeenCalled()
  })
})
