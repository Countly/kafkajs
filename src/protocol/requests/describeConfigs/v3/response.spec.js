const Encoder = require('../../../encoder')
const { decode, parse } = require('./response')
const { DEFAULT_CONFIG } = require('../../../configSource')

const vector = () => require('../fixtures/java/describeConfigsResponse-v3-entries.json')

describe('Protocol > Requests > DescribeConfigs > v3 response', () => {
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
    const body = new Encoder().writeInt32(25).writeArray([
      new Encoder()
        .writeInt16(0)
        .writeString('')
        .writeInt8(4)
        .writeString('')
        .writeArray([
          new Encoder()
            .writeString('')
            .writeString('')
            .writeBoolean(false)
            .writeInt8(5)
            .writeBoolean(false)
            .writeArray([new Encoder().writeString('').writeString('').writeInt8(5)])
            .writeInt8(7)
            .writeString(''),
        ]),
    ]).buffer

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
