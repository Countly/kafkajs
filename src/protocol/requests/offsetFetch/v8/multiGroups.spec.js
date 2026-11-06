const fs = require('node:fs')
const path = require('node:path')
const builders = require('..')
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
