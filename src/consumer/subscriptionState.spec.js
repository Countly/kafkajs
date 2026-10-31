const SubscriptionState = require('./subscriptionState')

describe('Consumer > SubscriptionState > pause / resume', () => {
  let subscriptionState
  const byTopic = (a, b) => a.topic - b.topic

  beforeEach(() => {
    subscriptionState = new SubscriptionState()
  })

  it('pauses the selected topics', () => {
    subscriptionState.pause([{ topic: 'topic1' }, { topic: 'topic2' }])
    expect(subscriptionState.paused().sort(byTopic)).toEqual([])

    subscriptionState.assign([
      { topic: 'topic1', partitions: [0, 1] },
      { topic: 'topic2', partitions: [1, 2] },
    ])
    expect(subscriptionState.paused().sort(byTopic)).toEqual([
      { topic: 'topic1', partitions: [0, 1] },
      { topic: 'topic2', partitions: [1, 2] },
    ])
  })

  it('resumes the selected topics', () => {
    subscriptionState.pause([{ topic: 'topic1' }, { topic: 'topic2' }])
    subscriptionState.assign([
      { topic: 'topic1', partitions: [0, 1] },
      { topic: 'topic2', partitions: [1, 2] },
    ])
    subscriptionState.resume([{ topic: 'topic2' }])

    expect(subscriptionState.paused().sort(byTopic)).toEqual([
      { topic: 'topic1', partitions: [0, 1] },
    ])
  })

  it('pauses the selected partitions', () => {
    subscriptionState.pause([{ topic: 'topic1', partitions: [0, 1] }])
    expect(subscriptionState.paused()).toEqual([])

    subscriptionState.assign([{ topic: 'topic1', partitions: [0, 1, 2, 3] }])

    expect(subscriptionState.paused().sort(byTopic)).toEqual([
      { topic: 'topic1', partitions: [0, 1] },
    ])

    subscriptionState.pause([{ topic: 'topic1', partitions: [1, 2] }])
    expect(subscriptionState.paused().sort(byTopic)).toEqual([
      { topic: 'topic1', partitions: [0, 1, 2] },
    ])

    subscriptionState.pause([{ topic: 'topic1', partitions: [4] }])
    expect(subscriptionState.paused().sort(byTopic)).toEqual([
      { topic: 'topic1', partitions: [0, 1, 2] },
    ])
  })

  it('resumes the selected partitions', () => {
    subscriptionState.pause([{ topic: 'topic1', partitions: [0, 1] }])
    subscriptionState.assign([{ topic: 'topic1', partitions: [0, 1, 2, 3] }])
    subscriptionState.resume([{ topic: 'topic1', partitions: [1] }])
    expect(subscriptionState.paused().sort(byTopic)).toEqual([{ topic: 'topic1', partitions: [0] }])

    subscriptionState.resume([{ topic: 'topic1', partitions: [4] }])
    expect(subscriptionState.paused().sort(byTopic)).toEqual([{ topic: 'topic1', partitions: [0] }])

    subscriptionState.pause([{ topic: 'topic1' }])
    subscriptionState.resume([{ topic: 'topic1', partitions: [1] }])
    expect(subscriptionState.paused().sort(byTopic)).toEqual([
      { topic: 'topic1', partitions: [0, 2, 3] },
    ])
  })
})

describe('Consumer > SubscriptionState > isPaused', () => {
  let subscriptionState

  beforeEach(() => {
    subscriptionState = new SubscriptionState()
  })

  it('can determine whether a topic partition is paused', () => {
    subscriptionState.pause([{ topic: 'topic1', partitions: [0, 1] }, { topic: 'topic2' }])

    expect(subscriptionState.isPaused('topic1', 0)).toEqual(true)
    expect(subscriptionState.isPaused('topic1', 2)).toEqual(false)
    expect(subscriptionState.isPaused('topic2', 0)).toEqual(true)
    expect(subscriptionState.isPaused('topic2', 2)).toEqual(true)
    expect(subscriptionState.isPaused('unknown', 0)).toEqual(false)
  })
})

describe('Consumer > SubcriptionState > assignments', () => {
  let subscriptionState

  beforeEach(() => {
    subscriptionState = new SubscriptionState()
  })

  it('can track assigned partitions per topic', () => {
    subscriptionState.assign([{ topic: 'topic1', partitions: [0, 1] }])
    expect(subscriptionState.active()).toEqual([{ topic: 'topic1', partitions: [0, 1] }])

    subscriptionState.assign([{ topic: 'topic2', partitions: [3, 4] }])
    expect(subscriptionState.active()).toEqual([{ topic: 'topic2', partitions: [3, 4] }])
  })

  it('can return which topic partitions are assigned and not paused', () => {
    subscriptionState.assign([{ topic: 'topic1', partitions: [0, 1] }])
    subscriptionState.pause([{ topic: 'topic1', partitions: [0] }])

    expect(subscriptionState.active()).toEqual([
      {
        topic: 'topic1',
        partitions: [1],
      },
    ])

    subscriptionState.pause([{ topic: 'topic2' }])
    subscriptionState.assign([{ topic: 'topic2', partitions: [0, 1, 2, 6, 7] }])
    expect(subscriptionState.active()).toEqual([
      {
        topic: 'topic2',
        partitions: [],
      },
    ])
  })
})

describe('Consumer > SubscriptionState > assignment change callback', () => {
  it('calls once at the end of assign and update, after the assignment is visible', () => {
    const seen = []
    const state = new SubscriptionState({
      onAssignmentChange: () => seen.push(state.assigned()),
    })
    state.assign([{ topic: 'topic1', partitions: [0] }])
    state.update([{ topic: 'topic1', partitions: [0, 1] }])
    expect(seen).toEqual([
      [{ topic: 'topic1', partitions: [0] }],
      [{ topic: 'topic1', partitions: [0, 1] }],
    ])
  })

  it('needs no callback', () => {
    expect(() => new SubscriptionState().assign([])).not.toThrow()
  })
})

describe('Consumer > SubscriptionState > omitted and unusable arguments', () => {
  let state
  beforeEach(() => {
    state = new SubscriptionState()
  })

  it('treats a bare assign, update, pause and resume as empty', () => {
    state.assign([{ topic: 'topic1', partitions: [0] }])
    state.update()
    expect(state.assigned()).toEqual([])
    state.assign()
    expect(state.assigned()).toEqual([])
    expect(() => state.pause()).not.toThrow()
    expect(() => state.resume()).not.toThrow()
  })

  it('assigns a topic that names no partitions as an empty one', () => {
    state.assign([{ topic: 'topic1' }])
    expect(state.assigned()).toEqual([{ topic: 'topic1', partitions: [] }])
  })

  it('ignores a partitions value that is neither absent nor a list', () => {
    state.assign([{ topic: 'topic1', partitions: [0, 1] }])
    state.pause([{ topic: 'topic1', partitions: [0] }])

    state.pause([{ topic: 'topic1', partitions: null }])
    expect(state.paused()).toEqual([{ topic: 'topic1', partitions: [0] }])

    state.resume([{ topic: 'topic1', partitions: null }])
    expect(state.paused()).toEqual([{ topic: 'topic1', partitions: [0] }])
  })
})
