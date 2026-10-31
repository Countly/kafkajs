const fs = require('fs')
const path = require('path')
const { TOPIC_ID, fetchResponse } = require('./fixtures/fetchHarness')
const {
  FETCH,
  RECREATE_PARTITIONS,
  SessionWire,
  capacityProblems,
  closeProblems,
  commitProblems,
  corpusProblems,
  faultOutcome,
  faultProblems,
  followerProblems,
  monotonicOf,
  moveProblems,
  sessionFacts,
  sessionProblems,
} = require('./fixtures/sessionRig')

const key = (partition) => `${TOPIC_ID}|${partition}`

/** @returns {Buffer} a Fetch answer frame: size, correlation id, empty header tags, body */
const answerFrame = (rows, sessionId, topLevel = 0) =>
  Buffer.concat([
    Buffer.alloc(8),
    Buffer.from([0]),
    fetchResponse(
      rows.map((partition) => ({ partition, errorCode: 0, records: false })),
      { sessionId, topLevel, includeTopic: rows.length > 0 }
    ),
  ])

/**
 * Builds a wire with synthetic Fetch exchanges, which the observer reads the way it reads a real
 * socket's.
 * @param {Array<{node?: number, connection?: number, at: number, sessionId: number,
 *   sessionEpoch: number, topics?: number[], forgotten?: number[], answer?: {rows: number[],
 *   sessionId: number, at?: number, topLevel?: number}|null}>} frames
 * @returns {SessionWire}
 */
const wireOf = (frames) => {
  const wire = new SessionWire(() => {
    throw new Error('no sockets')
  })
  for (const node of [1, 2, 3]) wire.nodes.set(`host:${node}`, node)
  frames.forEach((frame, index) => {
    const {
      node = 1,
      connection = 1,
      at,
      sessionId,
      sessionEpoch,
      topics = [],
      forgotten = [],
    } = frame
    const answer = frame.answer === undefined ? { rows: [], sessionId } : frame.answer
    wire.exchanges.push({
      connection,
      endpoint: `host:${node}`,
      apiKey: FETCH,
      apiVersion: 18,
      correlationId: index,
      at,
      fetch: {
        sessionId,
        sessionEpoch,
        rackId: 'rack-a',
        maxWaitMs: 500,
        topics: topics.length
          ? [
              {
                topic: null,
                topicId: TOPIC_ID,
                partitions: topics.map((partition) => ({ partition })),
              },
            ]
          : [],
        forgottenTopics: forgotten.length
          ? [{ topic: null, topicId: TOPIC_ID, partitions: forgotten }]
          : [],
      },
      ...(answer && {
        answer: {
          at: answer.at ?? at + 1,
          frame: answerFrame(answer.rows, answer.sessionId, answer.topLevel),
        },
      }),
    })
  })
  return wire
}

describe('SessionWire#observations', () => {
  it('replays the held partitions and reads the answers from their bytes', async () => {
    const wire = wireOf([
      {
        at: 1,
        sessionId: 0,
        sessionEpoch: 0,
        topics: [0, 1],
        answer: { rows: [0, 1], sessionId: 7 },
      },
      { at: 3, sessionId: 7, sessionEpoch: 1, topics: [2], answer: { rows: [2], sessionId: 7 } },
      { at: 5, sessionId: 7, sessionEpoch: 2, forgotten: [1], answer: { rows: [], sessionId: 7 } },
      { at: 7, sessionId: 7, sessionEpoch: -1, answer: null },
    ])

    const observed = await wire.observations()

    expect(observed.map((o) => o.held)).toEqual([2, 3, 2, 0])
    expect(observed[0].cells).toEqual([key(0), key(1)])
    expect(observed[2].forgotten).toEqual([key(1)])
    expect(observed[2].heldKeys.sort()).toEqual([key(0), key(2)])
    expect(observed.map((o) => o.answer?.rows ?? null)).toEqual([2, 1, 0, null])
    expect(observed[0].answer.sessionId).toBe(7)
    expect(observed[3].answeredAt).toBeNull()
  })

  it('keeps each node its own session and refuses an undecodable request', async () => {
    const wire = wireOf([
      {
        node: 1,
        at: 1,
        sessionId: 0,
        sessionEpoch: 0,
        topics: [0],
        answer: { rows: [0], sessionId: 5 },
      },
      {
        node: 2,
        at: 2,
        sessionId: 0,
        sessionEpoch: 0,
        topics: [1],
        answer: { rows: [1], sessionId: 6 },
      },
    ])
    expect((await wire.observations()).map((o) => [o.node, o.held])).toEqual([
      [1, 1],
      [2, 1],
    ])

    wire.exchanges[0].fetchError = new Error('cannot decode')
    await expect(wire.observations()).rejects.toThrow('cannot decode')
  })
})

describe('sessionFacts and sessionProblems', () => {
  const healthy = () =>
    wireOf([
      {
        at: 1,
        sessionId: 0,
        sessionEpoch: 0,
        topics: [0, 1, 2],
        answer: { rows: [0, 1, 2], sessionId: 7 },
      },
      { at: 3, sessionId: 7, sessionEpoch: 1, answer: { rows: [1], sessionId: 7 } },
      { at: 5, sessionId: 7, sessionEpoch: 2, answer: { rows: [], sessionId: 7 } },
    ])

  it('classifies full, created, incremental and sparse frames', async () => {
    const facts = sessionFacts(await healthy().observations())

    expect(
      [facts.full, facts.created, facts.incremental, facts.sparse].map((f) => f.length)
    ).toEqual([1, 1, 2, 2])
    expect(sessionProblems(facts)).toEqual([])
  })

  it('names what is missing', async () => {
    const observed = await healthy().observations()

    expect(sessionProblems(sessionFacts(observed.filter((o) => o.sessionEpoch <= 0)))).toEqual([
      'node 1 had no accepted incremental request',
      'no accepted reply was sparse',
    ])
    expect(sessionProblems(sessionFacts([]))).toContain('no full request carried a partition')
  })

  it('reads an ID zero answer as a fallback and a session error as refused', async () => {
    const wire = wireOf([
      { at: 1, sessionId: 0, sessionEpoch: 0, topics: [0], answer: { rows: [0], sessionId: 0 } },
      {
        at: 3,
        sessionId: 0,
        sessionEpoch: 0,
        topics: [0],
        answer: { rows: [], sessionId: 0, topLevel: 70 },
      },
    ])

    const facts = sessionFacts(await wire.observations())

    expect([facts.fallback.length, facts.refused.length, facts.created.length]).toEqual([1, 1, 0])
  })
})

describe('corpusProblems and commitProblems', () => {
  const record = (partition, value, offset) => ({
    topic: 't',
    partition,
    value,
    offset: String(offset),
  })
  const expected = [record(0, 'a', 0), record(0, 'b', 1)]

  it('accepts one delivery of each record in order', () => {
    expect(corpusProblems(expected, expected)).toEqual([])
  })

  it('reports a missing record, a repeat and a step back', () => {
    expect(corpusProblems(expected, [expected[0]])).toEqual(['t|0|b was never handled'])
    expect(corpusProblems(expected, [...expected, expected[0]])).toEqual([
      't|0|a was handled again',
      't|0 went back to offset 0',
    ])
  })

  it('lets a declared replay repeat', () => {
    expect(corpusProblems(expected, [...expected, expected[1]], (k) => k === 't|0|b')).toEqual([])
  })

  describe('with a recreated topic', () => {
    const incarnationOf = (r) => (r.value.startsWith('new') ? 1 : 0)
    const old = [record(0, 'old-a', 0), record(0, 'old-b', 1)]
    const fresh = [record(0, 'new-a', 0), record(0, 'new-b', 1)]
    const both = [...old, ...fresh]

    it('accepts offsets that restart in the new incarnation', () => {
      expect(corpusProblems(both, both, undefined, incarnationOf)).toEqual([])
    })

    it('reports a duplicate inside one incarnation', () => {
      expect(corpusProblems(both, [...both, fresh[0]], undefined, incarnationOf)).toEqual([
        '1|t|0|new-a was handled again',
        '1|t|0 went back to offset 0',
      ])
    })

    it('reports a backward offset inside one incarnation', () => {
      expect(corpusProblems(both, [old[1], old[0], ...fresh], undefined, incarnationOf)).toEqual([
        't|0 went back to offset 0',
      ])
    })

    it('does not let replaying the old incarnation after the new one pass', () => {
      expect(corpusProblems(both, [...both, old[0]], undefined, incarnationOf)).toEqual([
        't|0|old-a was handled again',
        't|0 went back to offset 0',
      ])
    })
  })

  it('reports a handled record the sends never acknowledged', () => {
    expect(corpusProblems(expected, [...expected, record(0, 'fabricated', 2)])).toEqual([
      't|0|fabricated was never acknowledged',
    ])
    expect(corpusProblems(expected, [...expected, record(7, 'a', 0)])).toEqual([
      't|7|a was never acknowledged',
    ])
  })

  it('never lets a declared replay admit an unacknowledged record', () => {
    expect(corpusProblems(expected, [...expected, record(0, 'fabricated', 2)], () => true)).toEqual(
      ['t|0|fabricated was never acknowledged']
    )
  })

  it('reports a commit past the processed position', () => {
    expect(commitProblems(expected, new Map([['t|0', 2]]))).toEqual([])
    expect(commitProblems(expected, new Map([['t|0', 3]]))).toEqual(['t|0 committed 3, past 2'])
  })
})

describe('the move, follower and close oracles', () => {
  it('accepts a move whose first holding request goes to the new leader', async () => {
    const wire = wireOf([
      {
        node: 1,
        at: 1,
        sessionId: 0,
        sessionEpoch: 0,
        topics: [0],
        answer: { rows: [0], sessionId: 5 },
      },
      { node: 1, at: 10, sessionId: 5, sessionEpoch: 1, forgotten: [0] },
      {
        node: 2,
        at: 11,
        sessionId: 0,
        sessionEpoch: 0,
        topics: [0],
        answer: { rows: [0], sessionId: 6 },
      },
    ])
    const observations = await wire.observations()
    const evidence = { observations, key: key(0), movedAt: 5, oldNode: 1, newNode: 2 }

    expect(moveProblems(evidence)).toEqual([])
    expect(moveProblems({ ...evidence, newNode: 3 })).toEqual([
      'no request held the partition on the new leader',
    ])
    const stale = { ...observations[0], node: 1, at: 12 }
    expect(moveProblems({ ...evidence, observations: [...observations, stale] })).toEqual([
      'the old leader held the partition 1 more times',
    ])
  })

  it('judges the follower by the first request that holds the partition after the hint', async () => {
    const wire = wireOf([
      {
        node: 1,
        at: 1,
        sessionId: 0,
        sessionEpoch: 0,
        topics: [0],
        answer: { rows: [0], sessionId: 5 },
      },
      { node: 1, at: 4, sessionId: 5, sessionEpoch: 1, forgotten: [0] },
      {
        node: 2,
        at: 5,
        sessionId: 0,
        sessionEpoch: 0,
        topics: [0],
        answer: { rows: [0], sessionId: 6 },
      },
    ])
    const observations = await wire.observations()
    const evidence = {
      observations,
      hint: { node: 1, at: 2 },
      key: key(0),
      follower: 2,
      rackId: 'rack-a',
    }

    expect(followerProblems(evidence)).toEqual([])
    const ignored = observations.map((o) => (o.at > 2 ? { ...o, node: 1 } : o))
    expect(followerProblems({ ...evidence, observations: ignored })).toEqual([
      'the first request holding the partition went to node 1',
      'the follower never answered the partition',
    ])
    expect(followerProblems({ ...evidence, rackId: 'rack-b' })).toEqual([
      '3 frames carried another rack',
    ])
  })

  it('checks the close against the IDs held, the order of answers and the budget', async () => {
    const wire = wireOf([
      {
        at: 1,
        sessionId: 0,
        sessionEpoch: 0,
        topics: [0],
        answer: { rows: [0], sessionId: 5, at: 2 },
      },
      { at: 3, sessionId: 5, sessionEpoch: 1, answer: { rows: [], sessionId: 5, at: 4 } },
      { at: 6, sessionId: 5, sessionEpoch: -1, answer: { rows: [], sessionId: 0, at: 7 } },
    ])
    const after = await wire.observations()
    const evidence = { before: after.slice(0, 2), after, startedAt: 5, endedAt: 8, budgetMs: 100 }

    expect(closeProblems(evidence)).toEqual([])
    expect(closeProblems({ ...evidence, endedAt: 500 })).toContain('the close took 495 ms')
    expect(closeProblems({ ...evidence, endedAt: 5.5 })).toEqual([
      '1 frames were written after the close resolved',
    ])
    expect(closeProblems({ ...evidence, after: after.slice(0, 2) })).toEqual([
      'node 1 got 0 closes',
    ])
    const early = after.map((o) => (o.sessionEpoch === 1 ? { ...o, answeredAt: 9 } : o))
    expect(closeProblems({ ...evidence, after: early })).toEqual([
      'node 1 closed before its last Fetch was answered',
    ])
  })
})

describe('capacityProblems', () => {
  /** @returns {Promise<object[]>} one consumer's observations: a full request per node */
  const consumer = async (answers) => {
    const frames = answers.flatMap((sessionId, index) => [
      {
        node: index + 1,
        at: 1,
        sessionId: 0,
        sessionEpoch: 0,
        topics: [index],
        answer: { rows: [index], sessionId },
      },
      ...(sessionId
        ? [{ node: index + 1, at: 3, sessionId, sessionEpoch: 1, answer: { rows: [], sessionId } }]
        : []),
    ])
    return wireOf(frames).observations()
  }

  it('needs more consumers than slots on every node, a fallback and a positive session', async () => {
    const holders = await Promise.all(Array.from({ length: 6 }, () => consumer([5, 6])))
    const overflow = await Promise.all(Array.from({ length: 3 }, () => consumer([0, 6])))

    expect(capacityProblems([...holders, ...overflow], 8)).toEqual([])
    expect(capacityProblems(holders, 8)).toEqual([
      'node 1 was asked by 6 consumers',
      'node 2 was asked by 6 consumers',
      'no fallback, rejection or eviction was observed',
    ])
  })

  it('rejects an all-fallback run for lacking a session path', async () => {
    const consumers = await Promise.all(Array.from({ length: 9 }, () => consumer([0, 0])))

    expect(capacityProblems(consumers, 8)).toEqual([
      'no consumer held a positive session',
      'no incremental request was accepted',
    ])
  })
})

describe('the outage clock', () => {
  it('puts a wall-clock moment on the monotonic clock the frames use', () => {
    expect(monotonicOf(1000, 1500, 90)).toBe(-410)
    expect(monotonicOf(1500, 1500, 90)).toBe(90)
    expect(monotonicOf(Date.now() - 50) - performance.now()).toBeLessThan(-40)
  })
})

describe('the fault outcome', () => {
  const NODE = 1
  const sent = (connection, at, answered, sessionEpoch = 1, node = NODE) => ({
    node,
    connection,
    at,
    sessionEpoch,
    answer: answered ? { rows: 0 } : null,
  })
  const other = sent(5, 12, true, 1, 2)

  /**
   * One arming, as the live case sees it: `pre` frames exist when the fault is armed, `post`
   * frames come later, and `armedThrough` is the wire's connection count at the arming, so a
   * socket opened by then is armed even when it has sent nothing. `lost` picks the frame the
   * fault took, if any.
   */
  const scenarios = {
    'already in flight at the arming': {
      armedThrough: 7,
      pre: [sent(4, 10, true), sent(4, 20, false)],
      post: [sent(8, 30, true, 0)],
      lost: (pre) => pre[1],
    },
    'the next frame after the arming': {
      armedThrough: 7,
      pre: [sent(4, 10, true)],
      post: [sent(4, 20, false), sent(8, 30, true, 0)],
      lost: (pre, post) => post[0],
    },
    'an armed socket that has sent nothing yet': {
      armedThrough: 8,
      pre: [sent(4, 10, true), sent(4, 15, false)],
      post: [sent(8, 30, false), sent(10, 40, true, 0)],
      lost: (pre, post) => post[0],
    },
  }

  describe.each(['dropped', 'truncated', 'blackholed'])('%s', (kind) => {
    const gap = kind === 'blackholed' ? 5000 : 0

    it.each(Object.keys(scenarios))(
      'settles on the reconnect and finds the lost Fetch: %s',
      (name) => {
        const { armedThrough, pre, post, lost } = scenarios[name]
        const reconnected = { ...post.at(-1), at: post.at(-1).at + gap }
        const after = [...post.slice(0, -1), reconnected]

        const outcome = faultOutcome([other, ...pre, ...after], NODE, armedThrough)

        expect(outcome.reconnected).toBe(reconnected)
        expect(outcome.lost).toBe(lost(pre, after))
        expect(faultProblems(kind, outcome)).toEqual([])
      }
    )
  })

  it('settles a truncation that closed between two answered Fetches without a lost frame', () => {
    const frames = [sent(8, 10, true), sent(8, 20, true), sent(10, 30, true, 2)]

    const outcome = faultOutcome(frames, NODE, 8)

    expect(outcome).toEqual({ reconnected: frames[2], lost: undefined })
    expect(faultProblems('truncated', outcome)).toEqual([])
  })

  it('does not settle while the node stays on the armed connections or another node reconnects', () => {
    const frames = [sent(4, 10, true), sent(4, 20, false), sent(9, 30, true, 0, 2)]

    expect(faultOutcome(frames, NODE, 7)).toBeUndefined()
    expect(faultOutcome([], NODE, 7)).toBeUndefined()
  })

  it('requires a full request after a lost Fetch and nothing after a clean close', () => {
    const lost = sent(4, 20, false)

    expect(faultProblems('dropped', { lost, reconnected: sent(8, 30, true, 3) })).toEqual([
      'dropped: epoch 3 followed a lost Fetch',
    ])
    expect(faultProblems('dropped', { lost, reconnected: sent(8, 30, true, 0) })).toEqual([])
    expect(faultProblems('truncated', { reconnected: sent(8, 30, true, 3) })).toEqual([])
  })
})

describe('the recreated topic', () => {
  it.each(['docker-compose.racks.yml', 'docker-compose.fetch-sessions.yml'])(
    'has the partition count %s auto-creates a topic with',
    (file) => {
      const compose = fs.readFileSync(path.join(__dirname, '../../..', file), 'utf8')
      const [, count] = compose.match(/KAFKA_NUM_PARTITIONS: '(\d+)'/)
      expect(RECREATE_PARTITIONS).toBe(Number(count))
    }
  )
})
