const RequestV2Protocol = require('./request')
const { protocol } = require('../index')

const vector = (caseId) => require(`../fixtures/java/createPartitionsRequest-v2-${caseId}.json`)

describe('Protocol > Requests > CreatePartitions > v2', () => {
  test.each(['noAssignments', 'assignments'])(
    'encodes the Apache 4.3.1 %s vector byte for byte',
    async (caseId) => {
      const { hex, fields } = vector(caseId)
      const { buffer } = await RequestV2Protocol(fields).encode()

      expect(buffer.toString('hex')).toEqual(hex)
    }
  )

  test.each([[[]], [undefined], [null]])(
    'writes assignments %j as the compact null, never as the explicit empty list',
    async (assignments) => {
      const { hex, fields } = vector('noAssignments')
      const [topicPartition] = fields.topicPartitions
      const { buffer } = await RequestV2Protocol({
        ...fields,
        topicPartitions: [{ ...topicPartition, assignments }],
      }).encode()

      expect(buffer.toString('hex')).toEqual(hex)
      expect(buffer.toString('hex')).not.toEqual(vector('explicitEmptyAssignments').hex)
    }
  )

  test('sends validateOnly as false and a timeout of 5000 ms when it is given neither', async () => {
    const { hex, fields } = vector('assignments')
    const { buffer } = await RequestV2Protocol({ topicPartitions: fields.topicPartitions }).encode()

    expect(buffer.toString('hex')).toEqual(hex)
  })

  test('is reachable through the version map as v2 of CreatePartitions', async () => {
    const { hex, fields } = vector('assignments')
    const { request } = protocol({ version: 2 })(fields)

    expect(request).toMatchObject({ apiKey: 37, apiVersion: 2, apiName: 'CreatePartitions' })
    expect((await request.encode()).buffer.toString('hex')).toEqual(hex)
  })
})
