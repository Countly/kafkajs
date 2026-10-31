const { EventEmitter } = require('node:events')
const { CommitEpisodeProbe } = require('./fixtures/commitEpisodeProbe')
const { exampleTranscript, judge } = require('./fixtures/listenerOracle')
const { CleanupStack } = require('../../producer/__tests__/fixtures/liveProbe')

const OFFSET_COMMIT = 8

/** @param {number} apiKey @param {number} correlationId @returns {Buffer} a request frame head */
const request = (apiKey, correlationId) => {
  const frame = Buffer.alloc(16)
  frame.writeInt32BE(12, 0)
  frame.writeInt16BE(apiKey, 4)
  frame.writeInt32BE(correlationId, 8)
  return frame
}

/** @param {number} correlationId @returns {Buffer} a response frame */
const response = (correlationId) => {
  const frame = Buffer.alloc(8)
  frame.writeInt32BE(4, 0)
  frame.writeInt32BE(correlationId, 4)
  return frame
}

/**
 * A probe over a fake socket, with one armed OffsetCommit whose response is already held.
 * @returns {{ probe: CommitEpisodeProbe, delivered: Buffer[] }}
 */
const heldProbe = () => {
  const socket = Object.assign(new EventEmitter(), { write: jest.fn(() => true) })
  const probe = new CommitEpisodeProbe(
    () => socket,
    () => 1
  )
  const wrapped = probe.socketFactory({})
  const delivered = []
  wrapped.on('data', (frame) => delivered.push(frame))
  probe.arm()
  wrapped.write(request(OFFSET_COMMIT, 7))
  socket.emit('data', response(7))
  return { probe, delivered }
}

describe('CommitEpisodeProbe held response', () => {
  it('holds the armed commit response and releases it exactly once', () => {
    const { probe, delivered } = heldProbe()
    expect(delivered).toHaveLength(0)
    probe.release()
    probe.release()
    expect(delivered).toHaveLength(1)
  })

  it('releases a held response during cleanup, before the member shutdown is awaited, when the episode failed', async () => {
    const { probe, delivered } = heldProbe()
    const cleanup = new CleanupStack()
    cleanup.add('consumer', async (signal) => {
      while (delivered.length === 0 && !signal.aborted) {
        await new Promise((resolve) => setTimeout(resolve, 5))
      }
    })
    probe.releaseOnCleanup(cleanup, 'first')
    const failure = await cleanup
      .run(
        async () => {
          throw new Error('the episode failed')
        },
        { budgetMs: 2000 }
      )
      .catch((error) => error)
    expect(failure.message).toBe('the episode failed')
    expect(failure.cleanupErrors).toEqual([])
    expect(delivered).toHaveLength(1)
  })
})

describe('CommitEpisodeProbe correlation', () => {
  it('finds the member commit event only after the held response is released', async () => {
    let ordinal = 0
    const socket = Object.assign(new EventEmitter(), { write: jest.fn(() => true) })
    const probe = new CommitEpisodeProbe(
      () => socket,
      () => ++ordinal
    )
    const wrapped = probe.socketFactory({})
    wrapped.on('data', () =>
      probe.recordCommit({
        groupId: 'g',
        memberId: 'm',
        topics: [{ topic: 't', partitions: [{ partition: '0', offset: '50' }] }],
      })
    )
    probe.arm()
    wrapped.write(request(OFFSET_COMMIT, 4))
    socket.emit('data', response(4))
    const query = { partition: 0, after: 0, before: Infinity, single: true }
    const scope = new AbortController().signal
    await expect(probe.heldResponse(scope)).resolves.toBe(true)
    expect(() => probe.correlate(query)).toThrow(/missing/)
    probe.release()
    await expect(probe.correlateReported(scope, query)).resolves.toMatchObject({
      partition: 0,
      offset: 50,
      correlationId: 4,
    })
  })

  it('matches a commit event whose partitions are string keys', () => {
    let ordinal = 0
    const socket = Object.assign(new EventEmitter(), { write: jest.fn(() => true) })
    const probe = new CommitEpisodeProbe(
      () => socket,
      () => ++ordinal
    )
    probe.socketFactory({}).write(request(OFFSET_COMMIT, 9))
    probe.recordCommit({
      groupId: 'g',
      memberId: 'm',
      topics: [{ topic: 't', partitions: [{ partition: '0', offset: '50' }] }],
    })
    expect(
      probe.correlate({ partition: 0, after: 0, before: Infinity, single: true })
    ).toMatchObject({ partition: 0, offset: 50, apiKey: OFFSET_COMMIT, correlationId: 9 })
  })
})

describe('listener oracle expected close episode', () => {
  it('accepts the example, whose second close episode is expected and eligible', () => {
    expect(judge(exampleTranscript())).toEqual([])
  })

  it('rejects an expected episode whose held commit response never came', () => {
    const transcript = exampleTranscript()
    Object.assign(transcript.episodes[1], { eligible: false, correlation: null })
    expect(judge(transcript)).toEqual([expect.stringMatching(/expected to commit at close/)])
  })

  it('rejects a transcript with no expected close episode when one is required', () => {
    const transcript = exampleTranscript()
    for (const episode of transcript.episodes) episode.expected = false
    expect(judge(transcript)).toEqual([expect.stringMatching(/no close episode was expected/)])
  })

  it('accepts a legitimately empty close when no episode is required', () => {
    const transcript = exampleTranscript()
    transcript.requireExpected = false
    for (const episode of transcript.episodes) {
      Object.assign(episode, { expected: false, eligible: false, correlation: null })
    }
    expect(judge(transcript)).toEqual([])
  })
})
