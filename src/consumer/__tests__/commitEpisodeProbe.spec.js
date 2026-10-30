const { EventEmitter } = require('node:events')
const { CommitEpisodeProbe } = require('./fixtures/commitEpisodeProbe')
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
