const { ShareRun } = require('../shareRun')
const {
  A,
  ShareClock,
  acquire,
  deferred,
  fetchAnswer,
  shareClient,
  until,
} = require('../../../../testHelpers/shareHarness')

const INTERVAL = 5000

describe('share poll recovery and the shared heartbeat scheduler', () => {
  let h
  let clock
  let run
  let handler

  afterEach(async () => {
    handler?.resolve()
    await h?.close()
    clock?.restore()
    jest.restoreAllMocks()
    h = clock = run = handler = undefined
  })

  /** @returns {number[]} The member epoch of every ShareGroupHeartbeat written so far. */
  const epochs = () => h.frames(76).map(({ fields }) => fields.memberEpoch)
  const joins = () => epochs().filter((epoch) => epoch === 0).length

  /**
   * Starts an established run whose first handler is held, then holds its next positive-epoch
   * heartbeat on the wire, invokes the run's poll-expiry callback and lets that recovery reach its
   * tick request while the old heartbeat is still held.
   * @param {(run: ShareRun) => void} [beforeRelease] runs after the request, before the old reply
   * @returns {Promise<{ beat: object, joinsBefore: number, requested: jest.SpyInstance }>}
   */
  const recoverUnderHeldBeat = async (beforeRelease) => {
    clock = new ShareClock().install()
    const start = ShareRun.prototype.start
    jest.spyOn(ShareRun.prototype, 'start').mockImplementation(function () {
      run = this
      return start.call(this)
    })
    h = shareClient({
      interval: INTERVAL,
      topics: { 'topic-a': { topicId: A, partitions: [{ leader: 0, epoch: 1 }] } },
    })
    h.script('ShareFetch', [fetchAnswer([acquire(0, 0, ['first'])])])
    handler = deferred()
    let entered = 0
    await h.consumer.subscribe({ topics: ['topic-a'] })
    await h.consumer.run({
      eachMessage: () => {
        entered++
        return handler.promise
      },
    })
    await until(() => entered === 1, 'the held handler')
    const beat = h.hold({
      api: 'ShareGroupHeartbeat',
      match: ({ fields }) => fields.memberEpoch > 0,
    })
    for (let step = 0; step < 20 && beat.call === null; step++) await clock.advance(INTERVAL / 4)
    expect(beat.call).not.toBeNull()
    const memberEpoch = beat.call.fields.memberEpoch

    const requested = jest.spyOn(run.manager, 'requestTick')
    clock.skew(run.options.maxPollInterval + 1)
    let recovered = false
    const recovery = run.onPollExpired().then(() => {
      recovered = true
    })
    await until(() => epochs().includes(-1), 'the separate leave')
    handler.resolve()
    await until(() => recovered, 'the poll recovery')
    await recovery
    expect(requested).toHaveBeenCalledTimes(1)
    const joinsBefore = joins()
    beforeRelease?.(run)
    beat.release({
      errorCode: 0,
      errorMessage: null,
      memberId: beat.call.fields.memberId,
      memberEpoch,
      heartbeatIntervalMs: INTERVAL,
      assignment: null,
    })
    // Far below one interval: the join follows the settled heartbeat, not the next cadence.
    for (let step = 0; step < 100 && joins() === joinsBefore; step++) await clock.advance(1)
    return { beat, joinsBefore }
  }

  test('a tick the share recovery requests while the old heartbeat is held sends one join when that heartbeat settles, then one cadence', async () => {
    const { joinsBefore } = await recoverUnderHeldBeat()

    expect(joins()).toBe(joinsBefore + 1)
    expect(epochs().at(-1)).toBe(0)
    const sent = epochs().length
    await clock.advance(INTERVAL - 20)
    expect(epochs()).toHaveLength(sent)
    await clock.advance(40)
    expect(epochs()).toHaveLength(sent + 1)
    expect(epochs().at(-1)).toBeGreaterThan(0)
  })

  test('a terminal shutdown after the request and before the old heartbeat settles sends nothing more', async () => {
    let sent = 0
    await recoverUnderHeldBeat((current) => {
      current.manager.shutdown()
      sent = epochs().length
    })

    await clock.advance(INTERVAL * 3)
    expect(epochs()).toHaveLength(sent)
  })
})
