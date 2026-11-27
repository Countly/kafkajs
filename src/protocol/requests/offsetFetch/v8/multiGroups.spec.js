const fs = require('node:fs')
const path = require('node:path')
const builders = require('..')
const Encoder = require('../../../encoder')
describe('OffsetFetch independent several-group vectors', () => {
  it.each([8, 9])('keeps both Java directions at version %i', async (version) => {
    const load = (direction) =>
      JSON.parse(
        fs.readFileSync(
          path.join(
            __dirname,
            `../fixtures/java/offsetFetch${direction}-v${version}-severalGroups.json`
          ),
          'utf8'
        )
      )
    const sent = load('Request')
    const received = load('Response')
    const built = builders.protocol({ version })(sent.fields)
    expect((await built.request.encode()).buffer.toString('hex')).toBe(sent.hex)
    const decoded = await built.response.decode(Buffer.from(received.hex, 'hex'))
    expect(decoded.groups.map((g) => g.groupId)).toEqual(['a', 'b'])
    expect(decoded.groups[0].topics[0].partitions[0]).toEqual({
      partition: 0,
      offset: '10',
      committedLeaderEpoch: 3,
      metadata: null,
      errorCode: 0,
    })
    expect(await built.response.parse(decoded)).toBe(decoded)
  })
})

// A lone surrogate is written as U+FFFD, so a broker echoes that and not the string that was asked.
describe('OffsetFetch several-group answers for a group id with a lone surrogate', () => {
  const echoOf = (id) => Buffer.from(id, 'utf8').toString('utf8')
  const partition = () =>
    new Encoder()
      .writeInt32(0)
      .writeInt64('10')
      .writeInt32(3)
      .writeUVarIntString(null)
      .writeInt16(0)
      .writeTaggedFields()
  const topic = () =>
    new Encoder()
      .writeUVarIntString('t')
      .writeUVarIntArray([partition()], 'object')
      .writeTaggedFields()
  const group = (groupId, errorCode = 0) =>
    new Encoder()
      .writeUVarIntString(groupId)
      .writeUVarIntArray(errorCode === 0 ? [topic()] : [], 'object')
      .writeInt16(errorCode)
      .writeTaggedFields()
  const answer = (...groups) =>
    new Encoder().writeInt32(0).writeUVarIntArray(groups, 'object').writeTaggedFields().buffer
  const asked = (groupId) => ({ groupId, topics: [{ topic: 't', partitions: [{ partition: 0 }] }] })
  const decode = (version, requested, ...answered) =>
    builders
      .protocol({ version })({ groups: requested.map(asked) })
      .response.decode(answer(...answered))

  it.each([8, 9])(
    'resolves the answer for a group id the broker echoes as the bytes it read at version %i',
    async (version) => {
      const decoded = await decode(
        version,
        ['team-\uD800', 'g1'],
        group(echoOf('team-\uD800')),
        group('g1')
      )

      expect(decoded.groups.map((g) => [g.groupId, g.errorCode])).toEqual([
        ['team-�', 0],
        ['g1', 0],
      ])
    }
  )

  it.each([8, 9])(
    'resolves a group error row for a group id the broker echoes as the bytes it read at version %i',
    async (version) => {
      const decoded = await decode(version, ['team-\uD800'], group(echoOf('team-\uD800'), 30))

      expect(decoded.groups.map((g) => [g.groupId, g.errorCode])).toEqual([['team-�', 30]])
    }
  )

  it.each([
    ['another group', 'team-other'],
    ['a different group that has a replacement character', 'team-��'],
  ])('still refuses an answer for %s at version 9', async (_, answeredId) => {
    await expect(decode(9, ['team-\uD800'], group(answeredId))).rejects.toMatchObject({
      name: 'KafkaJSInvariantViolation',
    })
  })

  it('still refuses an answer that leaves out a requested group id with a lone surrogate', async () => {
    await expect(decode(9, ['team-\uD800', 'g1'], group('g1'))).rejects.toMatchObject({
      name: 'KafkaJSInvariantViolation',
    })
  })

  it('resolves the answer for an ordinary Unicode group id', async () => {
    const id = 'über-\u{1F600}'
    const decoded = await decode(9, [id], group(id))

    expect(decoded.groups.map((g) => g.groupId)).toEqual([id])
  })
})
