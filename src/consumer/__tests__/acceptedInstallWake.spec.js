const { RoutingSeam, EPOCH, WAKE_MS } = require('./fixtures/routingSeam')

describe('Consumer > accepted metadata install wake', () => {
  let seam

  afterEach(async () => {
    await seam?.close()
    seam = null
  })

  /**
   * Opens a seam whose partition 0 has no leader and whose two nodes are idle with nothing to fetch.
   * @returns {Promise<void>}
   */
  const idleSeam = async () => {
    seam = await RoutingSeam.open({ nodes: [1, 2], leaders: { 0: -1 } })
    seam.start()
    await seam.advance(WAKE_MS)
    expect(seam.fetches).toEqual([])
  }

  it('starts the Fetch of a partition that moved from no leader to a leader within a few ms', async () => {
    await idleSeam()
    const route = seam.now()

    seam.install({ leaders: { 0: 1 }, epoch: EPOCH + 1 })
    await seam.advance(WAKE_MS)

    expect(seam.waited(1, 0, route)).toBeLessThanOrEqual(WAKE_MS)
  })

  it('ignores an answer with an older epoch and follows the fresh one within a few ms', async () => {
    await idleSeam()

    seam.install({ leaders: { 0: 1 }, epoch: EPOCH - 1 })
    await seam.advance(WAKE_MS)
    expect(seam.fetches).toEqual([])
    const route = seam.now()

    seam.install({ leaders: { 0: 1 }, epoch: EPOCH + 1 })
    await seam.advance(WAKE_MS)

    expect(seam.waited(1, 0, route)).toBeLessThanOrEqual(WAKE_MS)
  })

  it('follows a fresh answer that lands right behind an older one within a few ms', async () => {
    await idleSeam()
    const route = seam.now()

    seam.install({ leaders: { 0: 1 }, epoch: EPOCH - 1 })
    seam.install({ leaders: { 0: 1 }, epoch: EPOCH + 1 })
    await seam.advance(WAKE_MS)

    expect(seam.waited(1, 0, route)).toBeLessThanOrEqual(WAKE_MS)
  })

  it('follows a move that also changes the node set within a few ms', async () => {
    await idleSeam()
    const route = seam.now()

    seam.install({ leaders: { 0: 1 }, epoch: EPOCH + 1, brokers: [1, 2, 4] })
    await seam.advance(WAKE_MS)

    expect(seam.waited(1, 0, route)).toBeLessThanOrEqual(WAKE_MS)
  })
})
