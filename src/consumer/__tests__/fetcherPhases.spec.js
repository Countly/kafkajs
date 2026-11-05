const { FetcherPhases, MAX_PHASES, brokerSetChanges } = require('./fixtures/fetcherPhases')

/** Stand-ins with the method names of the production classes a round runs through. */
class FakeIdleWait {
  async wait() {}
}
class FakeBroker {
  constructor(nodeId) {
    this.nodeId = nodeId
  }

  async fetch() {
    return 'answer'
  }
}
class FakeGroup {
  async prepareFetch() {
    if (this.failPreparation) throw new Error('not prepared')
  }

  async refreshLeaderlessMetadata() {}

  async refreshCluster() {}
}
class FakeRunner {
  constructor() {
    this.group = new FakeGroup()
    this.idle = new FakeIdleWait()
  }

  async fetch(nodeId) {
    await this.group.prepareFetch(nodeId)
    await this.group.refreshLeaderlessMetadata()
    await this.idle.wait(5000)
    await new FakeBroker(nodeId).fetch()
    await this.heartbeat()
    return []
  }

  async heartbeat() {}
}
const seams = {
  Runner: FakeRunner,
  ConsumerGroup: FakeGroup,
  IdleWait: FakeIdleWait,
  Broker: FakeBroker,
}

describe('Consumer > Fetcher phase observation', () => {
  let phases

  beforeEach(() => {
    let at = 0
    phases = new FetcherPhases(() => (at += 1), seams)
  })

  afterEach(() => phases.detach())

  const trail = (nodeId) =>
    phases
      .since(0)
      .rows.filter((row) => row.nodeId === nodeId)
      .map(({ phase, event }) => `${phase} ${event}`)

  it('records the phases of each node through its own round, interleaved or not', async () => {
    phases.attach()
    phases.open()
    const runner = new FakeRunner()

    await Promise.all([runner.fetch(7), runner.fetch(8)])

    const expected = [
      'preparation entered',
      'preparation left',
      'metadata entered',
      'metadata left',
      'idle entered',
      'idle left',
      'fetch written',
      'heartbeat entered',
      'heartbeat left',
    ]
    expect(trail(7)).toEqual(expected)
    expect(trail(8)).toEqual(expected)
  })

  it('records nothing before it is opened or for a call outside a round', async () => {
    phases.attach()
    const runner = new FakeRunner()
    await runner.fetch(7)
    phases.open()
    await runner.heartbeat()
    await runner.group.refreshCluster()

    expect(phases.since(0)).toEqual({ rows: [], dropped: 0 })
  })

  it('leaves a failing step failing and still records where it ended', async () => {
    phases.attach()
    phases.open()
    const runner = new FakeRunner()
    runner.group.failPreparation = true

    await expect(runner.fetch(3)).rejects.toThrow('not prepared')

    expect(trail(3)).toEqual(['preparation entered', 'preparation left'])
  })

  it('restores every watched method on detach', async () => {
    const before = [FakeRunner, FakeGroup, FakeIdleWait, FakeBroker].map((cls) =>
      Object.values(Object.getOwnPropertyDescriptors(cls.prototype)).map(({ value }) => value)
    )
    phases.attach()
    phases.detach()
    phases.detach()

    const after = [FakeRunner, FakeGroup, FakeIdleWait, FakeBroker].map((cls) =>
      Object.values(Object.getOwnPropertyDescriptors(cls.prototype)).map(({ value }) => value)
    )
    expect(after).toEqual(before)
    await new FakeRunner().fetch(1)
    expect(phases.since(0).rows).toEqual([])
  })

  it('keeps a bounded number of rows and counts the rest', () => {
    phases.open()
    for (let n = 0; n < MAX_PHASES + 4; n++) phases.note('fetch', 'written', 1)

    expect(phases.rows).toHaveLength(MAX_PHASES)
    expect(phases.since(0).dropped).toBe(4)
  })
})

describe('Consumer > Fetcher phase observation > broker-set changes', () => {
  const answer = (at, ...nodeIds) => ({
    at,
    decoded: { brokers: nodeIds.map((nodeId) => ({ nodeId })) },
  })

  it('lists only the answers that advertise another node set, oldest first', () => {
    const answers = [
      answer(300, 1, 2),
      answer(100, 1, 2, 3),
      answer(200, 3, 2, 1),
      answer(400, 1, 2, 3),
    ]

    expect(brokerSetChanges(answers, 0)).toEqual({
      changes: [
        { at: 300, nodeIds: [1, 2] },
        { at: 400, nodeIds: [1, 2, 3] },
      ],
      dropped: 0,
    })
  })

  it('starts from the window and compares its first answer with the one before it', () => {
    const answers = [answer(100, 1, 2, 3), answer(200, 1, 2), answer(300, 1, 2)]

    expect(brokerSetChanges(answers, 150).changes).toEqual([{ at: 200, nodeIds: [1, 2] }])
    expect(brokerSetChanges(answers, 250).changes).toEqual([])
  })
})
