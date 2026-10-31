const {
  FetchFrameCounter,
  auditTranscript,
  verifyFrames,
  fetchSessionHeader,
} = require('../fetchFrameCounter')

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

/** @param {number} version wire version @param {number} epoch session epoch @param {number} id session ID @returns {Buffer} independently assembled header and empty Fetch body */
function sessionRequest(version, epoch, id) {
  const header = Buffer.alloc(21)
  header.writeInt16BE(1, 4)
  header.writeInt16BE(version, 6)
  header.writeInt32BE(7, 8)
  header.writeInt16BE(7, 12)
  header.write('fs-gate', 14)
  const body = Buffer.alloc((version <= 14 ? 4 : 0) + 13 + 8)
  if (version <= 14) body.writeInt32BE(-1, 0)
  body.writeInt32BE(id, body.length - 8)
  body.writeInt32BE(epoch, body.length - 4)
  const frame = Buffer.concat([header, Buffer.from([0]), body, Buffer.from([1, 1, 1, 0])])
  frame.writeInt32BE(frame.length - 4)
  return frame
}
/** @param {number} id session ID @param {number} [code] top-level error @returns {Buffer} literal flexible response */
function sessionResponse(id, code = 0) {
  const frame = Buffer.alloc(21)
  frame.writeInt32BE(17)
  frame.writeInt32BE(7, 4)
  frame.writeInt16BE(code, 13)
  frame.writeInt32BE(id, 15)
  frame[19] = 1
  return frame
}
test.each([12, 13, 14, 15, 16, 17, 18])(
  'request v%i has the actual session header after flexible header tags',
  (version) => {
    expect(
      fetchSessionHeader(sessionRequest(version, 3, 42), {
        direction: 'request',
        apiVersion: version,
      })
    ).toMatchObject({ sessionId: 42, sessionEpoch: 3, kind: 'incremental' })
  }
)
test.each([
  [0, 0, 'full'],
  [42, 0, 'full'],
  [0, -1, 'sessionless'],
  [42, -1, 'close'],
])('wire session %i epoch %i is classified as %s', (id, epoch, kind) => {
  expect(
    fetchSessionHeader(sessionRequest(18, epoch, id), { direction: 'request', apiVersion: 18 })
  ).toMatchObject({ kind })
})
test('response classification distinguishes ID zero fallback and explicit session rejection', () => {
  expect(
    fetchSessionHeader(sessionResponse(0), { direction: 'response', apiVersion: 18 })
  ).toMatchObject({ kind: 'fallback' })
  expect(
    fetchSessionHeader(sessionResponse(42, 70), { direction: 'response', apiVersion: 18 })
  ).toMatchObject({ kind: 'rejected', errorCode: 70 })
})
test('fragmented/coalesced frames retain timestamps and correlate independently by connection', () => {
  const primary = new FetchFrameCounter()
  const transcript = []
  for (const connection of ['one', 'two']) {
    const req = sessionRequest(18, 1, 42)
    const reply = sessionResponse(42)
    for (const [direction, chunk, at] of [
      ['request', req.subarray(0, 3), 10],
      ['request', req.subarray(3), 11],
      ['response', reply, 12],
    ]) {
      transcript.push({ connection, nodeId: connection === 'one' ? 0 : 1, direction, chunk, at })
      primary.push(connection, direction, chunk, at)
    }
  }
  const audit = auditTranscript(transcript, { sessions: true })
  verifyFrames(primary.result(), audit)
  expect(audit.frames.map((frame) => frame.at)).toEqual([11, 12, 11, 12])
  expect(
    audit.frames
      .filter((frame) => frame.direction === 'request')
      .every((frame) => frame.kind === 'incremental')
  ).toBe(true)
  expect(() => verifyFrames({ ...primary.result(), bytes: 0 }, audit)).toThrow('Fetch bytes')
})
test('partial headers and unsupported versions void observation instead of guessing offsets', () => {
  expect(() =>
    fetchSessionHeader(Buffer.alloc(8), { direction: 'request', apiVersion: 18 })
  ).toThrow()
  expect(() =>
    fetchSessionHeader(sessionRequest(18, 0, 0), { direction: 'request', apiVersion: 19 })
  ).toThrow('Unsupported')
})

/**
 * @param {number} version wire version, 11 to 18
 * @param {{epoch:number,id:number,partitions?:number[],forgotten?:number[]}} body session fields, partition rows per topic and forgotten partitions per topic
 * @returns {Buffer} independently assembled request with topic rows
 */
function fetchRequest(version, { epoch, id, partitions = [], forgotten = [] }) {
  const flexible = version >= 12
  const int = (value, size = 4) => {
    const bytes = Buffer.alloc(size)
    bytes.writeIntBE(value, 0, size)
    return bytes
  }
  const count = (value) => (flexible ? Buffer.from([value + 1]) : int(value))
  const tags = () => (flexible ? [Buffer.from([0])] : [])
  const topic = () =>
    version >= 13
      ? Buffer.alloc(16, 7)
      : Buffer.concat([flexible ? Buffer.from([2]) : int(1, 2), Buffer.from('t')])
  const row = Buffer.alloc(24 + (version >= 9 ? 4 : 0) + (flexible ? 4 : 0))
  const frame = Buffer.concat([
    int(0),
    int(1, 2),
    int(version, 2),
    int(7),
    int(7, 2),
    Buffer.from('fs-gate'),
    ...tags(),
    ...(version <= 14 ? [int(-1)] : []),
    int(500),
    int(1),
    int(1048576),
    Buffer.from([1]),
    int(id),
    int(epoch),
    count(partitions.length),
    ...partitions.flatMap((rows) => [
      topic(),
      count(rows),
      ...Array(rows).fill(Buffer.concat([row, ...tags()])),
      ...tags(),
    ]),
    count(forgotten.length),
    ...forgotten.flatMap((rows) => [topic(), count(rows), Buffer.alloc(4 * rows), ...tags()]),
    flexible ? Buffer.from([1]) : int(0, 2),
    ...tags(),
  ])
  frame.writeInt32BE(frame.length - 4)
  return frame
}

test.each([11, 12, 13, 15, 18])(
  'request v%i reports actual partition rows and forgotten entries',
  (version) => {
    expect(
      fetchSessionHeader(
        fetchRequest(version, { epoch: 4, id: 9, partitions: [2, 1], forgotten: [1, 1] }),
        {
          direction: 'request',
          apiVersion: version,
        }
      )
    ).toEqual({
      sessionId: 9,
      sessionEpoch: 4,
      kind: 'incremental',
      partitions: 3,
      forgottenPartitions: 2,
    })
  }
)

test('trailing or truncated body bytes void the observation', () => {
  const frame = fetchRequest(18, { epoch: 1, id: 9, partitions: [1] })
  const trailing = Buffer.concat([frame, Buffer.from([0])])
  trailing.writeInt32BE(trailing.length - 4)
  const options = { direction: 'request', apiVersion: 18 }
  expect(() => fetchSessionHeader(trailing, options)).toThrow('Trailing')
  const truncated = frame.subarray(0, frame.length - 6)
  truncated.writeInt32BE(truncated.length - 4)
  expect(() => fetchSessionHeader(truncated, options)).toThrow('Truncated')
  expect(() => fetchSessionHeader(frame, { direction: 'other', apiVersion: 18 })).toThrow('Invalid')
  expect(() => fetchSessionHeader(frame.subarray(1), options)).toThrow(
    'Invalid Fetch observation size'
  )
})

test('the primary frame observer sees complete Fetch frames only, stamped by their last chunk', () => {
  const frames = []
  const counter = new FetchFrameCounter(undefined, (frame) => frames.push(frame))
  const req = sessionRequest(18, 1, 42)
  counter.push('a', 'request', req.subarray(0, 5), 1)
  counter.push('a', 'request', req.subarray(5), 2)
  counter.push('a', 'request', request(3, 8), 3)
  counter.push('a', 'response', Buffer.concat([sessionResponse(42), response(8)]), 4)
  expect(frames).toEqual([
    {
      connection: 'a',
      direction: 'request',
      at: 2,
      bytes: req.length,
      correlationId: 7,
      apiKey: 1,
      apiVersion: 18,
    },
    {
      connection: 'a',
      direction: 'response',
      at: 4,
      bytes: 21,
      correlationId: 7,
      apiKey: 1,
      apiVersion: 18,
    },
  ])
})

test('a reconnect reusing a correlation ID keeps both audited frames and their own timestamps', () => {
  const transcript = []
  for (const [connection, at] of [
    ['first', 10],
    ['second', 20],
  ])
    for (const [direction, chunk] of [
      ['request', sessionRequest(18, 0, 0)],
      ['response', sessionResponse(0)],
    ])
      transcript.push({ connection, nodeId: 0, direction, chunk, at })
  const audit = auditTranscript(transcript, { sessions: true })
  expect(audit.frames.map((frame) => [frame.connection, frame.at, frame.kind])).toEqual([
    ['first', 10, 'full'],
    ['first', 10, 'fallback'],
    ['second', 20, 'full'],
    ['second', 20, 'fallback'],
  ])
})
