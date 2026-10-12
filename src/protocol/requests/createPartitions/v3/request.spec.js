const RequestV3Protocol = require('./request')
const { protocol, versions } = require('../index')

const vector = (caseId) => require(`../fixtures/java/createPartitionsRequest-v3-${caseId}.json`)

describe('Protocol > Requests > CreatePartitions > v3', () => {
  test.each(['noAssignments', 'assignments'])(
    'encodes the Apache 4.3.1 %s vector byte for byte',
    async (caseId) => {
      const { hex, fields } = vector(caseId)
      const { buffer } = await RequestV3Protocol(fields).encode()

      expect(buffer.toString('hex')).toEqual(hex)
    }
  )

  test('writes an empty list of assignments as the compact null', async () => {
    const { hex, fields } = vector('noAssignments')
    const [topicPartition] = fields.topicPartitions
    const { buffer } = await RequestV3Protocol({
      ...fields,
      topicPartitions: [{ ...topicPartition, assignments: [] }],
    }).encode()

    expect(buffer.toString('hex')).toEqual(hex)
    expect(vector('explicitEmptyAssignments').hex).not.toEqual(hex)
  })

  test('is reachable through the version map as v3 of CreatePartitions', async () => {
    const { hex, fields } = vector('assignments')
    const { request } = protocol({ version: 3 })(fields)

    expect(versions).toEqual(['0', '1', '2', '3'])
    expect(request).toMatchObject({ apiKey: 37, apiVersion: 3, apiName: 'CreatePartitions' })
    expect((await request.encode()).buffer.toString('hex')).toEqual(hex)
  })
})
