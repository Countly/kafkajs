const frameRequest = require('../request')
const {
  requestHeaderVersion,
  responseHeaderVersion,
  REQUEST_HEADER_V2,
  RESPONSE_HEADER_V0,
} = require('../headers')
const apiKeys = require('./apiKeys')
const ApiVersionsV5Request = require('./apiVersions/v5/request')
const { decode: decodeTxnOffsetCommit, validate } = require('./txnOffsetCommit/v6/response')
const { decode: decodeDeleteGroups } = require('./deleteGroups/v3/response')

/** Bytes below are written out from the message schemas, never produced by the codecs they check. */
const TOPIC_ID = '01020304-0506-0708-090a-0b0c0d0e0f10'
const OTHER_TOPIC_ID = '11121314-1516-1718-191a-1b1c1d1e1f20'

describe('Kafka 4.4 codec verification', () => {
  test('ApiVersions v5 preserves paired identity and the v0 response header', async () => {
    const { buffer } = await frameRequest({
      correlationId: 7,
      clientId: 'c',
      request: ApiVersionsV5Request({
        clientSoftwareName: 'n',
        clientSoftwareVersion: 'v',
        clusterId: 'k',
        nodeId: 2,
      }),
    })

    // size 23; api key 18, version 5, correlation 7, client id "c", empty header tags;
    // then software name, version, cluster id "k" and node 2 as a pair, empty body tags
    expect(buffer.toString('hex')).toBe(
      '00000017' +
        '0012' +
        '0005' +
        '00000007' +
        '000163' +
        '00' +
        '026e' +
        '0276' +
        '026b' +
        '00000002' +
        '00'
    )
    expect(requestHeaderVersion(apiKeys.ApiVersions, 5)).toBe(REQUEST_HEADER_V2)
    expect(responseHeaderVersion(apiKeys.ApiVersions, 5)).toBe(RESPONSE_HEADER_V0)
  })

  test('TxnOffsetCommit v6 correlates shuffled topic ids to the request', async () => {
    const asked = new Map([
      [TOPIC_ID, 'test-topic'],
      [OTHER_TOPIC_ID, 'other-topic'],
    ])
    // throttle 25; two topics, the second one asked for answered first: its partition 0 fails with
    // 100, then the first one's partitions 0 and 1 with 0
    const body = Buffer.from(
      '00000019' +
        '03' +
        '1112131415161718191a1b1c1d1e1f20' +
        '02' +
        '00000000' +
        '0064' +
        '00' +
        '00' +
        '0102030405060708090a0b0c0d0e0f10' +
        '03' +
        '00000000' +
        '0000' +
        '00' +
        '00000001' +
        '0000' +
        '00' +
        '00' +
        '00',
      'hex'
    )

    const decoded = await decodeTxnOffsetCommit(body, { topicNames: asked })

    expect(decoded.topics).toEqual([
      { topic: 'other-topic', partitions: [{ partition: 0, errorCode: 100 }] },
      {
        topic: 'test-topic',
        partitions: [
          { partition: 0, errorCode: 0 },
          { partition: 1, errorCode: 0 },
        ],
      },
    ])
    expect(() =>
      validate(decoded, [
        { topic: 'test-topic', partitions: [{ partition: 0 }, { partition: 1 }] },
        { topic: 'other-topic', partitions: [{ partition: 0 }] },
      ])
    ).not.toThrow()
  })

  test('DeleteGroups v3 preserves null empty and custom error messages', async () => {
    // throttle 25; three groups "a", "b", "c": a null message with code 0, an empty one with
    // code 134 and the custom message "boom" with code 134
    const body = Buffer.from(
      '00000019' +
        '04' +
        '0261' +
        '0000' +
        '00' +
        '00' +
        '0262' +
        '0086' +
        '01' +
        '00' +
        '0263' +
        '0086' +
        '05626f6f6d' +
        '00' +
        '00',
      'hex'
    )

    const { results } = await decodeDeleteGroups(body)

    expect(results.map(({ errorMessage }) => errorMessage)).toEqual([null, '', 'boom'])
    expect(results.map(({ groupId, errorCode }) => [groupId, errorCode])).toEqual([
      ['a', 0],
      ['b', 134],
      ['c', 134],
    ])
    expect(results[0]).not.toHaveProperty('error')
    for (const { error } of results.slice(1)) {
      expect(error).toMatchObject({ type: 'GROUP_DELETION_FAILED', code: 134, retriable: false })
      expect(error.message).not.toContain('boom')
    }
  })
})
