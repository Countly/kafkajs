const RequestV2Protocol = require('./request')
const { protocol, versions } = require('../index')

const vector = () => require('../fixtures/java/deleteRecordsRequest-v2-basic.json')

describe('Protocol > Requests > DeleteRecords > v2', () => {
  test('encodes the Apache 4.3.1 basic vector byte for byte, offsets as int64', async () => {
    const { hex, fields } = vector()
    const { buffer } = await RequestV2Protocol(fields).encode()

    expect(buffer.toString('hex')).toEqual(hex)
  })

  test('accepts an offset as a number or a bigint, and truncates to the high watermark at -1', async () => {
    const { hex, fields } = vector()
    const [topic] = fields.topics
    const { buffer } = await RequestV2Protocol({
      timeout: fields.timeout,
      topics: [
        {
          ...topic,
          partitions: [
            { partition: 0, offset: 5 },
            { partition: 2, offset: -1n },
          ],
        },
      ],
    }).encode()

    expect(buffer.toString('hex')).toEqual(hex)
  })

  test('sends a timeout of 5000 ms when it is not given', async () => {
    const { hex, fields } = vector()
    const { buffer } = await RequestV2Protocol({ topics: fields.topics }).encode()

    expect(buffer.toString('hex')).toEqual(hex)
  })

  test('is reachable through the version map as v2 of DeleteRecords', async () => {
    const { hex, fields } = vector()
    const { request } = protocol({ version: 2 })(fields)

    expect(versions).toEqual(['0', '1', '2'])
    expect(request).toMatchObject({ apiKey: 21, apiVersion: 2, apiName: 'DeleteRecords' })
    expect((await request.encode()).buffer.toString('hex')).toEqual(hex)
  })
})
