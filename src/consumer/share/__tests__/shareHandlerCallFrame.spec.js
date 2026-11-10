const { ShareHandlerCall } = jest.requireActual('../shareHandlerCall')

describe('handler call construction', () => {
  const FRAME_KEYS = ['authority', 'batch', 'ledger', 'mode', 'records']
  /** The constructor arguments of every call the run built, in order. */
  const constructed = []
  const instances = []
  const polled = []
  let harness
  let h

  beforeAll(() => {
    jest.isolateModules(() => {
      jest.doMock('../shareHandlerCall', () => {
        const actual = jest.requireActual('../shareHandlerCall')
        return {
          ...actual,
          ShareHandlerCall: class extends actual.ShareHandlerCall {
            constructor(...args) {
              super(...args)
              constructed.push(args)
              instances.push(this)
            }
          },
        }
      })
      const { SharePollView } = require('../shareMembership')
      const begin = SharePollView.prototype.begin
      jest.spyOn(SharePollView.prototype, 'begin').mockImplementation(function (call) {
        const tracked = begin.call(this, call)
        polled.push([call, tracked])
        return tracked
      })
      harness = require('../../../../testHelpers/shareHarness')
    })
  })

  afterAll(() => {
    jest.restoreAllMocks()
    jest.dontMock('../shareHandlerCall')
  })

  beforeEach(() => {
    constructed.length = 0
    instances.length = 0
    polled.length = 0
  })

  afterEach(async () => {
    await h?.close()
    h = undefined
  })

  /**
   * @param {object} handler The `run` argument.
   * @param {() => boolean} handled Whether every handler of the single fetch answer ran.
   * @returns {Promise<void>} Runs one acquisition of three records through `handler`.
   */
  const runThreeRecords = async (handler, handled) => {
    h = harness.shareClient()
    await h.consumer.subscribe({ topics: ['topic-a'] })
    h.script('ShareFetch', [harness.fetchAnswer([harness.acquire(0, 0, ['a', 'b', 'c'])])])
    await h.consumer.run(handler)
    await harness.until(handled, 'every handler of the acquisition')
  }

  test('builds every eachMessage call of an acquisition from one shared frame', async () => {
    const offsets = []
    await runThreeRecords(
      {
        eachMessage: async ({ message }) => {
          offsets.push(message.offset)
        },
      },
      () => offsets.length === 3
    )

    expect(constructed.map((args) => args.length)).toEqual([3, 3, 3])
    const [frame] = constructed[0]
    expect(constructed.map(([shared]) => shared === frame)).toEqual([true, true, true])
    expect(constructed.map(([, start, end]) => [start, end])).toEqual([
      [0, 1],
      [1, 2],
      [2, 3],
    ])
    expect(Object.keys(frame).sort()).toEqual(FRAME_KEYS)
    expect(frame.batch).toBe(false)
    expect(offsets).toEqual(['0', '1', '2'])
    expect(polled).toHaveLength(3)
    for (let i = 0; i < instances.length; i++) {
      expect(polled[i][0]).toBe(instances[i])
      expect(polled[i][1]).toBe(instances[i])
    }
  })

  test('builds one eachBatch call per acquisition from the frame itself', async () => {
    const sizes = []
    await runThreeRecords(
      {
        eachBatch: async ({ batch }) => {
          sizes.push(batch.messages.length)
        },
      },
      () => sizes.length === 1
    )

    expect(constructed).toHaveLength(1)
    expect(constructed[0]).toHaveLength(1)
    const [frame] = constructed[0]
    expect(Object.keys(frame).sort()).toEqual(FRAME_KEYS)
    expect(frame.batch).toBe(true)
    expect(sizes).toEqual([3])
    expect(polled).toHaveLength(1)
    expect(polled[0][0]).toBe(instances[0])
    expect(polled[0][1]).toBe(instances[0])
  })

  test('reads start and end from the frame unless they are passed, and never changes the frame', () => {
    const records = [{ offset: '0' }, { offset: '1' }, { offset: '2' }]
    const base = Object.freeze({
      ledger: {},
      records,
      mode: 'implicit',
      batch: false,
      authority: {},
    })
    const ranged = Object.freeze({ ...base, start: 1, end: 2 })

    const calls = [
      new ShareHandlerCall(base),
      new ShareHandlerCall(ranged),
      new ShareHandlerCall(base, 2, 3),
      new ShareHandlerCall(ranged, 0, 1),
    ]

    expect(calls.map(({ start, end, size }) => [start, end, size])).toEqual([
      [0, 3, 3],
      [1, 2, 1],
      [2, 3, 1],
      [0, 1, 1],
    ])
  })
})
