const { decode, parse } = require('./response')

const vector = (caseId) =>
  require(`../fixtures/java/consumerGroupHeartbeatResponse-v0-${caseId}.json`)

describe('Protocol > Requests > ConsumerGroupHeartbeat > v0', () => {
  test.each(['assignment', 'emptyAssignment', 'nullAssignment', 'fencedError'])(
    'decodes the %s vector the Apache 4.3.1 codec produced',
    async (caseId) => {
      const { hex, fields } = vector(caseId)
      const data = await decode(Buffer.from(hex, 'hex'))

      expect(data).toEqual({
        throttleTime: 0,
        clientSideThrottleTime: fields.throttleTime,
        errorCode: fields.errorCode,
        errorMessage: fields.errorMessage,
        memberId: fields.memberId,
        memberEpoch: fields.memberEpoch,
        heartbeatIntervalMs: fields.heartbeatIntervalMs,
        assignment: fields.assignment,
      })
    }
  )

  test('a null assignment is null, and an empty one is a present, empty target', async () => {
    // The two mean different things to the state machine: null is "nothing to reconcile toward",
    // an empty assignment is "you own nothing", which is a target like any other. They differ on
    // the wire by one byte, and this is the assertion that keeps the codec from collapsing them.
    const absent = await decode(Buffer.from(vector('nullAssignment').hex, 'hex'))
    const empty = await decode(Buffer.from(vector('emptyAssignment').hex, 'hex'))

    expect(absent.assignment).toBeNull()
    expect(empty.assignment).toEqual({ topicPartitions: [] })
  })

  test('refuses a presence marker that is neither present nor null', async () => {
    // Kafka's generator writes only 1 or -1. Any other byte means the frame is not the message it
    // claims to be, and reading on would decode the rest of it from the wrong offset and report
    // plausible nonsense - an assignment built out of the tag buffers that follow.
    const ASSIGNMENT_MARKER_OFFSET = 27
    const buffer = Buffer.from(vector('nullAssignment').hex, 'hex')
    buffer.writeInt8(0, ASSIGNMENT_MARKER_OFFSET)

    await expect(decode(buffer)).rejects.toThrow(
      'Invalid ConsumerGroupHeartbeatResponse Assignment presence marker 0, expected 1 or -1'
    )
  })

  test('reports a non-zero broker throttle time as the client-side value', async () => {
    // No committed vector carries a non-zero ThrottleTimeMs, so the KIP-219 split - `throttleTime`
    // pinned at 0 because this client never sleeps on it, the wire value reported separately - is
    // pinned from a hand-built frame instead.
    const { hex } = vector('nullAssignment')
    const buffer = Buffer.from(hex, 'hex')
    buffer.writeInt32BE(1234, 0)

    const data = await decode(buffer)

    expect(data.throttleTime).toEqual(0)
    expect(data.clientSideThrottleTime).toEqual(1234)
  })

  test('parse resolves a successful response', async () => {
    const data = await decode(Buffer.from(vector('assignment').hex, 'hex'))

    await expect(parse(data)).resolves.toEqual(data)
  })

  test('parse throws the plain protocol error when the broker sent no message', async () => {
    // Most membership error codes arrive with a null ErrorMessage - UNKNOWN_MEMBER_ID and
    // NOT_COORDINATOR among them - and that path must still raise the typed protocol error.
    await expect(
      parse({ errorCode: 25, errorMessage: null, memberId: null, memberEpoch: 0 })
    ).rejects.toMatchObject({
      type: 'UNKNOWN_MEMBER_ID',
      code: 25,
    })
  })

  test('decode fails fast on UNSUPPORTED_VERSION, as every sibling codec does', async () => {
    // A broker that rejects the negotiated version answers with code 35; reporting it from the
    // decode is this repo's convention, so the caller never sees it as ordinary response data.
    const UNSUPPORTED_VERSION = 35
    const buffer = Buffer.from(vector('nullAssignment').hex, 'hex')
    buffer.writeInt16BE(UNSUPPORTED_VERSION, 4)

    await expect(decode(buffer)).rejects.toMatchObject({ type: 'UNSUPPORTED_VERSION', code: 35 })
  })

  test('parse throws the broker error, carrying its code and its message', async () => {
    const { fields } = vector('fencedError')
    const data = await decode(Buffer.from(vector('fencedError').hex, 'hex'))

    await expect(parse(data)).rejects.toMatchObject({
      type: 'FENCED_MEMBER_EPOCH',
      code: 110,
      // The BROKER's own message, read off the vector rather than typed here: this repo's table
      // has its own text for 110 (`src/protocol/error.js`), the broker sends another, and the
      // codec appends the second to the first. Quoting a literal would let the two drift, and a
      // literal belonging to a different code would pass by accident.
      message: expect.stringContaining(fields.errorMessage),
    })
    await expect(parse(data)).rejects.toMatchObject({
      message: expect.stringContaining('The member epoch is fenced by the group coordinator'),
    })
  })
})
