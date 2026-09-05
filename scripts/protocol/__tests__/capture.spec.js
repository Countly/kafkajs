'use strict'

const {
  splitFrames,
  stripHeader,
  normalise,
  parseArgs,
  findLatestCapture,
  maxVersionsForCapturedApis,
} = require('../capture')
const apiKeys = require('../../../src/protocol/requests/apiKeys')

describe('scripts/protocol > capture', () => {
  test('splits a stream into size-prefixed frames and keeps the remainder', () => {
    const stream = Buffer.from([0, 0, 0, 2, 0xaa, 0xbb, 0, 0, 0, 1, 0xcc, 0, 0])
    expect(splitFrames(stream)).toEqual({
      frames: [Buffer.from([0xaa, 0xbb]), Buffer.from([0xcc])],
      remainder: Buffer.from([0, 0]),
    })
  })

  test('strips a request header v2 and returns the correlation id', () => {
    const Encoder = require('../../../src/protocol/encoder')
    const frame = new Encoder()
      .writeInt16(3)
      .writeInt16(12)
      .writeInt32(77)
      .writeString('c')
      .writeTaggedFields()
      .writeInt8(0x2a).buffer

    expect(stripHeader('request', frame)).toEqual({
      apiKey: 3,
      apiVersion: 12,
      correlationId: 77,
      body: Buffer.from([0x2a]),
    })
  })

  test('replaces a uuid and a cluster id without changing the length', () => {
    const body = Buffer.concat([
      Buffer.from([0x01]),
      Buffer.from('aabbccddeeff00112233445566778899', 'hex'),
      Buffer.from('AAAAAAAAAAAAAAAAAAAAAA'),
    ])
    const out = normalise(body, {
      topicIds: ['aabbccdd-eeff-0011-2233-445566778899'],
      clusterId: 'AAAAAAAAAAAAAAAAAAAAAA',
    })

    expect(out.length).toEqual(body.length)
    expect(out.slice(1, 17).toString('hex')).toEqual('0102030405060708090a0b0c0d0e0f10')
    expect(out.slice(17).toString()).toEqual('kafkajs-capture-clstr0')
  })

  test('parses --name value pairs and treats a bare flag as a boolean', () => {
    expect(parseArgs(['--brokers', 'localhost:9092', '--tag', '431'])).toEqual({
      brokers: 'localhost:9092',
      tag: '431',
    })
    expect(parseArgs(['--print-versions', '--brokers', 'localhost:9092'])).toEqual({
      'print-versions': true,
      brokers: 'localhost:9092',
    })
    expect(parseArgs(['--print-versions'])).toEqual({ 'print-versions': true })
  })

  test('finds the highest-version capture for one api key and direction', () => {
    const captures = new Map([
      [`${apiKeys.Metadata}/12/response`, Buffer.from([0x0c])],
      [`${apiKeys.Metadata}/13/response`, Buffer.from([0x0d])],
      [`${apiKeys.Metadata}/13/request`, Buffer.from([0xff])],
      [`${apiKeys.Produce}/11/response`, Buffer.from([0x0b])],
    ])

    expect(findLatestCapture(captures, apiKeys.Metadata, 'response')).toEqual({
      apiVersion: 13,
      body: Buffer.from([0x0d]),
    })
    expect(findLatestCapture(captures, apiKeys.ListOffsets, 'response')).toBeNull()
  })

  test('reports the broker ceiling for every captured api, and null for one it never advertises', () => {
    const advertised = [
      { apiKey: apiKeys.Produce, minVersion: 0, maxVersion: 13 },
      { apiKey: apiKeys.ListOffsets, minVersion: 0, maxVersion: 11 },
      { apiKey: apiKeys.Metadata, minVersion: 0, maxVersion: 13 },
      { apiKey: apiKeys.OffsetCommit, minVersion: 2, maxVersion: 10 },
      { apiKey: apiKeys.OffsetFetch, minVersion: 1, maxVersion: 10 },
      { apiKey: apiKeys.InitProducerId, minVersion: 0, maxVersion: 6 },
    ]

    expect(maxVersionsForCapturedApis(advertised)).toEqual([
      { name: 'Produce', apiKey: apiKeys.Produce, maxVersion: 13 },
      { name: 'ListOffsets', apiKey: apiKeys.ListOffsets, maxVersion: 11 },
      { name: 'Metadata', apiKey: apiKeys.Metadata, maxVersion: 13 },
      { name: 'OffsetCommit', apiKey: apiKeys.OffsetCommit, maxVersion: 10 },
      { name: 'OffsetFetch', apiKey: apiKeys.OffsetFetch, maxVersion: 10 },
      { name: 'FindCoordinator', apiKey: apiKeys.GroupCoordinator, maxVersion: null },
      { name: 'InitProducerId', apiKey: apiKeys.InitProducerId, maxVersion: 6 },
    ])
  })
})
