'use strict'

const { splitFrames, stripHeader, normalise } = require('../capture')

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
})
