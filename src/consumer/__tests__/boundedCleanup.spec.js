const { withDeadline, disconnectAll, startTrackedMember } = require('./fixtures/boundedCleanup')

/**
 * @param {Error|null} failure
 * @param {number} [delayMs] when set, the disconnect settles after this long on the fake clock
 * @returns {{disconnect: jest.Mock}}
 */
const client = (failure, delayMs = 0) => ({
  disconnect: jest.fn(
    () =>
      new Promise((resolve, reject) =>
        setTimeout(() => (failure ? reject(failure) : resolve()), delayMs)
      )
  ),
})

describe('Consumer > bounded cleanup', () => {
  beforeEach(() => jest.useFakeTimers())
  afterEach(() => jest.useRealTimers())

  describe('startTrackedMember', () => {
    it('registers before wiring and preserves startup, event and batch handling', async () => {
      const members = []
      const crashes = []
      const received = []
      const consumerOptions = { groupId: 'stub-group', partitionAssigners: [] }
      const runFailure = new Error('run failed')
      const consumer = {
        events: {
          REBALANCING: 'rebalancing',
          GROUP_JOIN: 'join',
          START_BATCH_PROCESS: 'batch',
          CRASH: 'crash',
        },
        on: jest.fn(() => expect(members[0].consumer).toBe(consumer)),
        connect: jest.fn().mockResolvedValue(),
        subscribe: jest.fn().mockResolvedValue(),
        run: jest.fn().mockRejectedValue(runFailure),
      }
      const kafka = { consumer: jest.fn(() => consumer) }
      const member = await startTrackedMember({
        kafka,
        consumerOptions,
        label: 'member',
        members,
        crashes,
        received,
        topic: 'stub-topic',
      })

      expect(kafka.consumer).toHaveBeenCalledWith(consumerOptions)
      expect(members).toEqual([member])
      expect(consumer.connect).toHaveBeenCalledTimes(1)
      expect(consumer.subscribe).toHaveBeenCalledWith({ topic: 'stub-topic', fromBeginning: true })
      expect(consumer.on.mock.calls.map(([type]) => type)).toEqual([
        'rebalancing',
        'join',
        'batch',
        'crash',
      ])
      for (const [, listener] of consumer.on.mock.calls) {
        listener({ timestamp: 123, payload: { detail: 'event' } })
      }
      expect(member.events).toEqual([
        { type: 'rebalancing', timestamp: 123, payload: { detail: 'event' } },
        { type: 'join', timestamp: 123, payload: { detail: 'event' } },
      ])
      expect(member.batches).toEqual([{ timestamp: 123, payload: { detail: 'event' } }])
      expect(crashes).toEqual([
        { label: 'member', error: runFailure },
        { label: 'member', detail: 'event' },
      ])
      await consumer.run.mock.calls[0][0].eachBatch({
        batch: { partition: 2, messages: [{ offset: '4' }, { offset: '5' }] },
      })
      expect(received).toEqual(['2:4', '2:5'])
    })

    it.each(['on', 'connect', 'subscribe'])(
      'retains the allocation for cleanup when %s fails',
      async (method) => {
        const members = []
        const failure = new Error(`${method} failed`)
        const consumer = {
          events: {},
          on: jest.fn(),
          connect: jest.fn().mockResolvedValue(),
          subscribe: jest.fn().mockResolvedValue(),
          run: jest.fn().mockResolvedValue(),
          disconnect: jest.fn().mockResolvedValue(),
        }
        if (method === 'on') {
          consumer.on.mockImplementation(() => {
            throw failure
          })
        } else {
          consumer[method].mockRejectedValue(failure)
        }

        await expect(
          startTrackedMember({
            kafka: { consumer: () => consumer },
            consumerOptions: {},
            label: 'failed-member',
            members,
            crashes: [],
            received: [],
            topic: 'stub-topic',
          })
        ).rejects.toBe(failure)
        await disconnectAll(
          members.map((member) => member.consumer),
          15000
        )

        expect(consumer.disconnect).toHaveBeenCalledTimes(1)
        expect(members).toEqual([{ label: 'failed-member', consumer, events: [], batches: [] }])
        if (method === 'subscribe') {
          expect(consumer.connect).toHaveBeenCalledTimes(1)
        }
        expect(consumer.run).not.toHaveBeenCalled()
        expect(jest.getTimerCount()).toBe(0)
      }
    )
  })

  describe('withDeadline', () => {
    it('resolves and leaves no pending timer when the work finishes first', async () => {
      const done = withDeadline(client(null, 10).disconnect(), 15000)
      await jest.advanceTimersByTimeAsync(10)
      await expect(done).resolves.toBeUndefined()
      expect(jest.getTimerCount()).toBe(0)
    })

    it('rejects at the deadline when the work never settles', async () => {
      const outcome = withDeadline(new Promise(() => {}), 15000).catch((error) => error)
      await jest.advanceTimersByTimeAsync(14999)
      expect(jest.getTimerCount()).toBe(1)
      await jest.advanceTimersByTimeAsync(1)
      expect(await outcome).toEqual(new Error('Disconnect exceeded 15000 ms'))
      expect(jest.getTimerCount()).toBe(0)
    })

    it('passes a rejection through and leaves no pending timer', async () => {
      const failure = new Error('socket closed')
      const outcome = withDeadline(client(failure).disconnect(), 15000).catch((error) => error)
      await jest.advanceTimersByTimeAsync(0)
      expect(await outcome).toBe(failure)
      expect(jest.getTimerCount()).toBe(0)
    })
  })

  describe('disconnectAll', () => {
    it('resolves when every client closes, skipping unallocated ones', async () => {
      const [a, b] = [client(null), client(null)]
      const done = disconnectAll([a, null, b, undefined], 15000)
      await jest.advanceTimersByTimeAsync(10)
      await expect(done).resolves.toBeUndefined()
      expect([a, b].map((c) => c.disconnect.mock.calls.length)).toEqual([1, 1])
      expect(jest.getTimerCount()).toBe(0)
    })

    it('tries every client past a rejection and a missed deadline, then reports all failures', async () => {
      const rejected = new Error('close failed')
      const clients = [client(rejected), { disconnect: () => new Promise(() => {}) }, client(null)]
      const outcome = disconnectAll(clients, 100).catch((error) => error)
      await jest.advanceTimersByTimeAsync(200)

      const error = await outcome
      expect(error).toBeInstanceOf(AggregateError)
      expect(error.errors).toEqual([rejected, new Error('Disconnect exceeded 100 ms')])
      expect(error.message).toBe('2 cleanup failure(s): close failed; Disconnect exceeded 100 ms')
      expect(clients[2].disconnect).toHaveBeenCalledTimes(1)
      expect(jest.getTimerCount()).toBe(0)
    })

    it('reports a disconnect that throws synchronously and keeps going', async () => {
      const thrown = new Error('threw')
      const after = client(null)
      const outcome = disconnectAll(
        [
          {
            disconnect: () => {
              throw thrown
            },
          },
          after,
        ],
        100
      ).catch((error) => error)
      await jest.advanceTimersByTimeAsync(0)

      expect((await outcome).errors).toEqual([thrown])
      expect(after.disconnect).toHaveBeenCalledTimes(1)
    })
  })
})
