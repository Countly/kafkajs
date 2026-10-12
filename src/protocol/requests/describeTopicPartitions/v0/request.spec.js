const Encoder = require('../../../encoder')
const RequestV0Protocol = require('./request')
const apiKeys = require('../../apiKeys')
const { protocol, versions } = require('../index')
const { requests, selectVersion } = require('../../index')
const { requestHeaderVersion, REQUEST_HEADER_V2 } = require('../../../headers')

const CASES = ['noCursor', 'cursor']
const vector = (caseId) =>
  require(`../fixtures/java/describeTopicPartitionsRequest-v0-${caseId}.json`)

/** @returns {Promise<string>} the hex of the request body the codec writes for `options` */
const hexOf = async (options) => (await RequestV0Protocol(options).encode()).buffer.toString('hex')

describe('Protocol > Requests > DescribeTopicPartitions > v0', () => {
  test.each(CASES)('encodes the Apache 4.3.1 %s vector byte for byte', async (caseId) => {
    const { hex, fields } = vector(caseId)

    expect(await hexOf(fields)).toEqual(hex)
  })

  test('sends the default page limit of 2000 and no cursor when only topics are given', async () => {
    const { hex, fields } = vector('noCursor')

    expect(await hexOf({ topics: fields.topics })).toEqual(hex)
  })

  test.each([[null], [undefined]])(
    'writes the cursor %s as the single marker byte -1',
    async (cursor) => {
      const hex = await hexOf({ topics: ['t'], responsePartitionLimit: 5, cursor })

      // topics [t], limit 5, marker ff, request tag buffer
      expect(hex).toEqual('02' + '0274' + '00' + '00000005' + 'ff' + '00')
    }
  )

  test('writes a cursor on partition 0 as present, not as null', async () => {
    const hex = await hexOf({
      topics: ['t'],
      responsePartitionLimit: 5,
      cursor: { topicName: 't', partitionIndex: 0 },
    })

    // marker 01, cursor topic t, partition 0, cursor tag buffer, request tag buffer
    expect(hex).toEqual(
      '02' + '0274' + '00' + '00000005' + '01' + '0274' + '00000000' + '00' + '00'
    )
  })

  test('writes an empty topic list as the empty compact array, never the null array', async () => {
    // an empty list asks the broker for every topic, a null one is not a legal request
    expect(await hexOf({ topics: [], responsePartitionLimit: 1 })).toEqual(
      '01' + '00000001' + 'ff' + '00'
    )
  })

  test('writes a name by its UTF-8 length and an empty name as the empty compact string', async () => {
    const hex = await hexOf({ topics: ['tópic', ''], responsePartitionLimit: 1 })

    // 'tópic' is 6 bytes, so its length byte is 7; '' is the length byte 1
    expect(hex).toEqual(
      '03' +
        '07' +
        Buffer.from('tópic').toString('hex') +
        '00' +
        '01' +
        '00' +
        '00000001' +
        'ff' +
        '00'
    )
  })

  test('writes the limit as an int32', async () => {
    const hex = await hexOf({ topics: ['t'], responsePartitionLimit: 2147483647 })

    expect(hex).toEqual('02' + '0274' + '00' + '7fffffff' + 'ff' + '00')
  })

  test('is registered for key 75, selected at v0 and sent with request header v2', async () => {
    const { hex, fields } = vector('cursor')
    const definition = requests.DescribeTopicPartitions
    const version = selectVersion({ 75: { minVersion: 0, maxVersion: 0 } }, 75, definition)
    const { request } = protocol({ version })(fields)

    expect(apiKeys.DescribeTopicPartitions).toBe(75)
    expect(versions).toEqual(['0'])
    expect(definition.versions).toEqual(['0'])
    expect(request).toMatchObject({ apiKey: 75, apiVersion: 0, apiName: 'DescribeTopicPartitions' })
    expect(requestHeaderVersion(75, 0)).toBe(REQUEST_HEADER_V2)
    expect((await request.encode()).buffer.toString('hex')).toEqual(hex)
  })

  test('encodes to an Encoder', async () => {
    expect(await RequestV0Protocol({ topics: ['t'] }).encode()).toBeInstanceOf(Encoder)
  })
})
