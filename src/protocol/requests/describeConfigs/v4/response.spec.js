const Encoder = require('../../../encoder')
const { decode, parse } = require('./response')
const { DEFAULT_CONFIG } = require('../../../configSource')

const vector = () => require('../fixtures/java/describeConfigsResponse-v4-entries.json')

describe('Protocol > Requests > DescribeConfigs > v4 response', () => {
  test('decodes the Apache 4.3.1 entries vector, new fields and derived isDefault included', async () => {
    const { hex, fields } = vector()
    const [resource] = fields.resources

    expect(await decode(Buffer.from(hex, 'hex'))).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: fields.throttleTime,
      resources: [
        {
          ...resource,
          configEntries: resource.configEntries.map((entry) => ({
            ...entry,
            isDefault: entry.configSource === DEFAULT_CONFIG,
          })),
        },
      ],
    })
  })

  test('parse throws the error of the first failed resource and resolves a clean one', async () => {
    const data = await decode(Buffer.from(vector().hex, 'hex'))

    await expect(parse(data)).resolves.toBe(data)
    await expect(parse({ resources: [{ errorCode: 0 }, { errorCode: 3 }] })).rejects.toMatchObject({
      type: 'UNKNOWN_TOPIC_OR_PARTITION',
    })
  })

  test('keeps every empty string, in an entry, a synonym and a resource, as an empty string', async () => {
    const body = new Encoder()
      .writeInt32(25)
      .writeUVarIntArray([
        new Encoder()
          .writeInt16(0)
          .writeUVarIntString('')
          .writeInt8(4)
          .writeUVarIntString('')
          .writeUVarIntArray([
            new Encoder()
              .writeUVarIntString('')
              .writeUVarIntString('')
              .writeBoolean(false)
              .writeInt8(5)
              .writeBoolean(false)
              .writeUVarIntArray([
                new Encoder()
                  .writeUVarIntString('')
                  .writeUVarIntString('')
                  .writeInt8(5)
                  .writeTaggedFields(),
              ])
              .writeInt8(7)
              .writeUVarIntString('')
              .writeTaggedFields(),
          ])
          .writeTaggedFields(),
      ])
      .writeTaggedFields().buffer

    expect(await decode(body)).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 25,
      resources: [
        {
          errorCode: 0,
          errorMessage: '',
          resourceType: 4,
          resourceName: '',
          configEntries: [
            {
              configName: '',
              configValue: '',
              readOnly: false,
              isDefault: true,
              configSource: 5,
              isSensitive: false,
              configSynonyms: [{ configName: '', configValue: '', configSource: 5 }],
              configType: 7,
              documentation: '',
            },
          ],
        },
      ],
    })
  })
})
