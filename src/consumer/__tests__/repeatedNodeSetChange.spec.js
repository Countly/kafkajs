const { ROUND_ALLOWANCE_MS } = require('./fixtures/fetchRoutingOracle')
const { RoutingSeam, EPOCH, WAKE_MS } = require('./fixtures/routingSeam')

/** The leader the moved partition starts with, the node that is lost. */
const OLD = 3
/** The node with nothing to fetch, which the partition moves to. */
const IDLE = 1
/** The node that keeps a long poll open the whole time. */
const BUSY = 2
/** Partition 0 moves; partition 1 keeps the busy node's poll open. */
const MOVED = 0

describe('Consumer > repeated node-set changes around a leader move', () => {
  let seam

  afterEach(async () => {
    await seam?.close()
    seam = null
  })

  /**
   * Opens a seam of three nodes whose old leader and busy node hold a long poll, and starts it.
   * @returns {Promise<void>}
   */
  const openSeam = async () => {
    seam = await RoutingSeam.open({ nodes: [IDLE, BUSY, OLD], leaders: { 0: OLD, 1: BUSY } })
    seam.hold(BUSY)
    seam.hold(OLD)
    seam.start()
    await seam.advance(WAKE_MS)
  }

  /**
   * The old leader is lost, advertised again and lost again, then the partition moves to the idle
   * node under the answer that follows.
   * @returns {Promise<number>} the fake clock when the move was installed
   */
  const loseReAddLoseThenMove = async () => {
    seam.install({ epoch: EPOCH + 1, brokers: [IDLE, BUSY] })
    await seam.advance(WAKE_MS)
    seam.install({ epoch: EPOCH + 2, brokers: [IDLE, BUSY, OLD] })
    await seam.advance(WAKE_MS)
    seam.install({ epoch: EPOCH + 3, brokers: [IDLE, BUSY] })
    await seam.advance(WAKE_MS)
    const route = seam.now()
    seam.install({ leaders: { [MOVED]: IDLE }, epoch: EPOCH + 4 })
    return route
  }

  /**
   * @param {number} route fake clock when the move was installed
   * @returns {{ withinRound: boolean, waited: number, reconciliations: number[] }} how long the
   *   idle node took to write its first Fetch of the moved partition, and when each fetcher
   *   reconciliation happened, both counted from the route
   */
  const report = (route) => {
    const waited = seam.waited(IDLE, MOVED, route)
    return {
      withinRound: waited <= ROUND_ALLOWANCE_MS,
      waited,
      reconciliations: seam.recreations.times.map((at) => at - route),
    }
  }

  it('starts the idle node within a round of a move that keeps the node set, with a busy poll open', async () => {
    await openSeam()
    const route = seam.now()

    seam.install({ leaders: { [MOVED]: IDLE }, epoch: EPOCH + 1 })
    await seam.advance(ROUND_ALLOWANCE_MS)

    expect(report(route)).toMatchObject({ withinRound: true, reconciliations: [] })
  })

  it('starts the idle node within a round of the move after the node set changed and changed back', async () => {
    await openSeam()

    const route = await loseReAddLoseThenMove()
    await seam.advance(ROUND_ALLOWANCE_MS)

    expect(report(route)).toMatchObject({ withinRound: true })
  })

  it('keeps the busy poll open while the idle node starts, and the lost node is reconciled away', async () => {
    await openSeam()

    const route = await loseReAddLoseThenMove()
    await seam.advance(ROUND_ALLOWANCE_MS)
    const busyFetches = () => seam.fetches.filter(({ node }) => node === BUSY).length

    expect(busyFetches()).toBe(1)
    expect(seam.waited(IDLE, MOVED, route)).toBeLessThanOrEqual(ROUND_ALLOWANCE_MS)
    expect(seam.recreations.since(0).reconciliations.flatMap(({ removed }) => removed)).toContain(
      OLD
    )

    seam.release(BUSY)
    await seam.advance(WAKE_MS)

    expect(busyFetches()).toBeGreaterThan(1)
  })
})
