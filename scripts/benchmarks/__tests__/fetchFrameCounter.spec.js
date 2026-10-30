const { FetchFrameCounter, auditTranscript, verifyFrames } = require('../fetchFrameCounter')

/**
 * Independent Kafka request header fixture, with optional body padding.
 * @param {number} api key
 * @param {number} id correlation ID
 * @param {number} [padding] body bytes
 * @returns {Buffer} length prefix and Kafka header
 */
const request = (api, id, padding = 0) => {
  const b = Buffer.alloc(14 + padding)
  b.writeInt32BE(b.length - 4)
  b.writeInt16BE(api, 4)
  b.writeInt16BE(12, 6)
  b.writeInt32BE(id, 8)
  b.writeInt16BE(0, 12)
  return b
}
/**
 * Independent response header fixture.
 * @param {number} id correlation ID
 * @returns {Buffer} empty response with length prefix
 */
const response = (id) => {
  const b = Buffer.alloc(8)
  b.writeInt32BE(4)
  b.writeInt32BE(id, 4)
  return b
}

test('fragmented and coalesced frames correlate per socket and exclude other APIs', () => {
  const counter = new FetchFrameCounter()
  const chunks = []
  const push = (connection, nodeId, direction, chunk) => {
    chunks.push({ connection, nodeId, direction, chunk: Buffer.from(chunk) })
    counter.push(connection, direction, chunk)
  }
  const a = Buffer.concat([request(1, 7), request(3, 8)])
  push('a', 0, 'request', a.subarray(0, 3))
  push('a', 0, 'request', a.subarray(3))
  push('b', 1, 'request', request(1, 7))
  push('b', 1, 'response', response(7))
  const replies = Buffer.concat([response(8), response(7)])
  push('a', 0, 'response', replies.subarray(0, 5))
  push('a', 0, 'response', replies.subarray(5))
  expect(counter.result()).toEqual({ bytes: 44, requests: 2, responses: 2 })
  const audited = auditTranscript(chunks)
  expect(audited.frames).toHaveLength(4)
  expect(audited.bytes).toBe(44)
  expect(() => verifyFrames(counter.result(), audited)).not.toThrow()
  expect(() => verifyFrames({ ...counter.result(), bytes: 0 }, audited)).toThrow(
    'Fetch bytes: expected 44, received 0'
  )
})

test('local idle and logical fetch observations add no wire bytes', () => {
  expect(new FetchFrameCounter().result()).toEqual({
    bytes: 0,
    requests: 0,
    responses: 0,
  })
  expect(auditTranscript([]).frames).toEqual([])
})

test('partial, duplicate and unknown replies invalidate an observation', () => {
  const partial = new FetchFrameCounter()
  partial.push('a', 'request', request(1, 1).subarray(0, 5))
  expect(() => partial.result()).toThrow('Incomplete')
  const duplicate = new FetchFrameCounter()
  duplicate.push('a', 'request', request(1, 1))
  expect(() => duplicate.push('a', 'request', request(1, 1))).toThrow('Duplicate')
  expect(() => new FetchFrameCounter().push('a', 'response', response(99))).toThrow('Uncorrelated')
  expect(() =>
    auditTranscript([
      {
        connection: 'a',
        nodeId: 0,
        direction: 'response',
        chunk: response(99),
      },
    ])
  ).toThrow('Unknown')
})

test('a correlation ID can be reused after its reply on the same socket', () => {
  const chunks = []
  const counter = new FetchFrameCounter()
  for (let i = 0; i < 2; i++)
    for (const [direction, chunk] of [
      ['request', request(1, 7)],
      ['response', response(7)],
    ]) {
      chunks.push({ connection: 'a', nodeId: 0, direction, chunk })
      counter.push('a', direction, chunk)
    }
  expect(counter.result()).toEqual({ bytes: 44, requests: 2, responses: 2 })
  expect(auditTranscript(chunks)).toMatchObject({
    bytes: 44,
    requests: 2,
    responses: 2,
  })
})

const transcriptOf = (entries) =>
  entries.map(([connection, nodeId, direction, chunk]) => ({
    connection,
    nodeId,
    direction,
    chunk,
  }))

test('a length prefix split across chunks and several coalesced replies are counted once each', () => {
  const counter = new FetchFrameCounter()
  const chunks = []
  const push = (direction, chunk) => {
    chunks.push(['a', 0, direction, chunk])
    counter.push('a', direction, chunk)
  }
  const requests = Buffer.concat([request(1, 1), request(1, 2), request(1, 3)])
  push('request', requests.subarray(0, 2))
  push('request', requests.subarray(2, 20))
  push('request', requests.subarray(20))
  push('response', Buffer.concat([response(3), response(1), response(2)]))
  expect(counter.result()).toEqual({ bytes: 66, requests: 3, responses: 3 })
  expect(auditTranscript(transcriptOf(chunks))).toMatchObject({
    bytes: 66,
    requests: 3,
    responses: 3,
  })
})

test('the same correlation ID in flight on two sockets, or after a reconnect, never aliases', () => {
  const counter = new FetchFrameCounter()
  const chunks = []
  for (const connection of ['first', 'second']) {
    chunks.push([connection, 0, 'request', request(1, 7)])
    counter.push(connection, 'request', request(1, 7))
  }
  for (const connection of ['second', 'first']) {
    chunks.push([connection, 0, 'response', response(7)])
    counter.push(connection, 'response', response(7))
  }
  chunks.push(
    ['reconnected', 0, 'request', request(1, 7)],
    ['reconnected', 0, 'response', response(7)]
  )
  counter.push('reconnected', 'request', request(1, 7))
  counter.push('reconnected', 'response', response(7))
  expect(counter.result()).toEqual({ bytes: 66, requests: 3, responses: 3 })
  expect(auditTranscript(transcriptOf(chunks))).toMatchObject({
    bytes: 66,
    requests: 3,
    responses: 3,
  })
})

test('a signed or oversize length is refused by both parsers', () => {
  const signed = Buffer.alloc(4)
  signed.writeInt32BE(-1)
  expect(() => new FetchFrameCounter().push('a', 'request', signed)).toThrow('Invalid frame length')
  const small = Buffer.alloc(4)
  small.writeInt32BE(2)
  expect(() => new FetchFrameCounter().push('a', 'response', small)).toThrow('Invalid frame length')
  const oversize = Buffer.alloc(4)
  oversize.writeInt32BE(200)
  expect(() => new FetchFrameCounter(100).push('a', 'request', oversize)).toThrow(
    'Invalid frame length'
  )
  for (const chunk of [signed, small, oversize.fill(0x7f)]) {
    expect(() =>
      auditTranscript(transcriptOf([['a', 0, 'request', Buffer.concat([chunk, Buffer.alloc(16)])]]))
    ).toThrow('Invalid transcript frame')
  }
})

test('a partial frame, an unanswered request or a changed node ends an observation as invalid', () => {
  const counter = new FetchFrameCounter()
  counter.push('a', 'request', request(1, 1))
  expect(() => counter.result()).toThrow('Incomplete')
  counter.push('a', 'response', response(1).subarray(0, 6))
  expect(() => counter.result()).toThrow('Incomplete')
  expect(() => auditTranscript(transcriptOf([['a', 0, 'request', request(1, 1)]]))).toThrow(
    'Missing'
  )
  expect(() =>
    auditTranscript(transcriptOf([['a', 0, 'request', request(1, 1).subarray(0, 9)]]))
  ).toThrow('Invalid transcript frame')
  expect(() =>
    auditTranscript(transcriptOf([['a', 0, 'request', request(1, 1).subarray(0, 3)]]))
  ).toThrow('Partial')
  expect(() =>
    auditTranscript(
      transcriptOf([
        ['a', 0, 'request', request(1, 1)],
        ['a', 1, 'response', response(1)],
      ])
    )
  ).toThrow('Connection changed node')
})
