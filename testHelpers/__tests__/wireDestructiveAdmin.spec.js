const { requests } = require('../../src/protocol/requests')
const expected = require('../wire/expected')
const pairs = [
  require('../wire/deleteRecordsV2'),
  require('../wire/deleteGroupsV3'),
  require('../wire/alterConfigsV2'),
  require('../wire/incrementalAlterConfigsV1'),
  require('../wire/createAclsV3'),
  require('../wire/deleteAclsV3'),
]

it('each new wire module reads the bytes the production encoder writes and builds a body the production decoder reads', async () => {
  const fields = {
    DeleteRecords: 'topics',
    DeleteGroups: 'results',
    AlterConfigs: 'resources',
    IncrementalAlterConfigs: 'resources',
    CreateAcls: 'creationResponses',
    DeleteAcls: 'filterResponses',
  }
  for (const pair of pairs) {
    const key = `${pair.api[0].toLowerCase()}${pair.api.slice(1)}V${pair.version}`
    const fixture = expected[key]
    const { request, response } = requests[pair.api].protocol({ version: pair.version })(
      fixture.request
    )
    const body = (await request.encode()).buffer
    expect(pair.readRequest(body)).toEqual(fixture.request)
    expect(() => pair.readRequest(Buffer.concat([body, Buffer.from([0])]))).toThrow(
      'trailing bytes'
    )
    expect(await response.decode(pair.buildResponse())).toMatchObject({
      [fields[pair.api]]: fixture.response[fields[pair.api]].map((row) =>
        pair.api === 'DeleteRecords'
          ? {
              ...row,
              partitions: row.partitions.map((p) => ({
                ...p,
                lowWatermark: BigInt(p.lowWatermark),
              })),
            }
          : row
      ),
    })
  }
})
