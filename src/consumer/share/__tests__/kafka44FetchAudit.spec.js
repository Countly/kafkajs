const {
  A,
  acquire,
  fetchAnswer,
  flush,
  frames,
  harness,
  part,
} = require('../../../../testHelpers/shareHarness')

const SHARE_FETCH = 78
const fetchFrames = (h, node) =>
  frames(h.cluster, SHARE_FETCH).filter((frame) => frame.node === node)

describe('Kafka 4.4 client fix audit', () => {
  let h

  afterEach(async () => {
    if (h) await h.close()
    h = null
  })

  test('KAFKA-20780 share fetch remains outstanding while another node progresses', async () => {
    h = await harness({ nodes: 2 })
    const [n0, n1] = h.members
    n0.want = [{ topicId: A, partition: 0 }]
    n1.want = [{ topicId: A, partition: 1 }]
    h.scriptNodes('ShareFetch', {
      1: [
        fetchAnswer([acquire(1, 300, ['b0', 'b1'])]),
        fetchAnswer([part({ partition: 1 })]),
        fetchAnswer([part({ partition: 1 })]),
      ],
    })
    const held = h.hold({ api: 'ShareFetch', node: 0 })

    const first0 = n0.loop.poll()
    await held.arrived()
    for (let round = 1; round <= 3; round++) {
      await n1.loop.poll()
      await flush()
      expect(fetchFrames(h, 1)).toHaveLength(round)
      expect(fetchFrames(h, 0)).toHaveLength(1)
      expect(n0.loop.poll()).toBe(first0)
    }

    expect(n1.handled.map(({ value }) => value)).toEqual(['b0', 'b1'])
    expect(n0.handled).toEqual([])

    held.release(fetchAnswer([part({ partition: 0 })]))
    await first0
    await n0.loop.poll()
    await flush()

    expect(fetchFrames(h, 0)).toHaveLength(2)
    expect(fetchFrames(h, 1)).toHaveLength(3)
  })
})
