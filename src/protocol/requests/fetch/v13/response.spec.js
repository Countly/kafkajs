const { decode, parse } = require('./response')

const TOPIC_ID = '01020304-0506-0708-090a-0b0c0d0e0f10'

describe('Protocol > Requests > Fetch > v13', () => {
  test('response is keyed by topic id and nothing else changes', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v13_response.json')))

    expect(data.responses).toEqual([
      {
        topicId: TOPIC_ID,
        partitions: [
          {
            partition: 0,
            errorCode: 0,
            highWatermark: '100',
            lastStableOffset: '100',
            lastStartOffset: '0',
            abortedTransactions: [],
            preferredReadReplica: -1,
            divergingEpoch: null,
            currentLeader: null,
            snapshotId: null,
            messages: [],
          },
        ],
      },
    ])
    expect(data.responses[0].topicName).toBeUndefined()
  })

  test('adds no per-request wrapper of its own, and shares v12 parse', () => {
    const response = require('./response')
    const { parse: parseV12 } = require('../v12/parse')

    expect(response.parse).toBe(parseV12)
    // No exported decoder pair to inspect - it is module-local by design - so the claim is tested
    // where it is observable: two builds of the same request produce the *same* decode function,
    // and the only wrapper between the codec and the caller is `fetch/index.js`'s pre-existing
    // options binder, which allocates one closure per fetch at every version.
    const build = () =>
      require('../index').protocol({ version: '13' })({
        maxWaitTime: 100,
        minBytes: 1,
        maxBytes: 1024,
        topics: [],
        topicIds: new Map(),
        checkCrcs: true,
      })

    expect(build().response.parse).toBe(parseV12)
    expect(build().response.decode).not.toBe(response.decode) // the withOptions binder
    expect(Object.keys(build().response).sort()).toEqual(['decode', 'parse'])
    expect(Object.keys(response).sort()).toEqual(['decode', 'parse', 'schema'])
  })

  test('parse resolves with partition codes intact', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v13_response.json')))
    data.responses[0].partitions[0].errorCode = 100 // UNKNOWN_TOPIC_ID
    await expect(parse(data)).resolves.toBe(data)
  })

  test('decodes the vector Apache own codec produced', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v13_response_java.json')))

    expect(data.responses[0].topicId).toEqual('01020304-0506-0708-090a-0b0c0d0e0f10')
    expect(data.responses[0].partitions[0].highWatermark).toEqual('100')
    expect(data.responses[0].partitions[0].lastStableOffset).toEqual('100')
    expect(data.responses[0].partitions[0].messages).toEqual([])
  })
})
