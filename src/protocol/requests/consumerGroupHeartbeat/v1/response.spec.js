const { decode, parse } = require('./response')

const vector = (caseId) =>
  require(`../fixtures/java/consumerGroupHeartbeatResponse-v1-${caseId}.json`)

describe('Protocol > Requests > ConsumerGroupHeartbeat > v1', () => {
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
    const absent = await decode(Buffer.from(vector('nullAssignment').hex, 'hex'))
    const empty = await decode(Buffer.from(vector('emptyAssignment').hex, 'hex'))

    expect(absent.assignment).toBeNull()
    expect(empty.assignment).toEqual({ topicPartitions: [] })
  })

  test('parse resolves a successful response', async () => {
    const data = await decode(Buffer.from(vector('assignment').hex, 'hex'))

    await expect(parse(data)).resolves.toEqual(data)
  })

  test('parse throws the broker error, carrying its code and its message', async () => {
    const { fields } = vector('fencedError')
    const data = await decode(Buffer.from(vector('fencedError').hex, 'hex'))

    await expect(parse(data)).rejects.toMatchObject({
      type: 'FENCED_MEMBER_EPOCH',
      code: 110,
      message: expect.stringContaining(fields.errorMessage),
    })
  })
})
