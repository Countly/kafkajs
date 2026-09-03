const os = require('os')
const RequestV2Protocol = require('./request')
const Decoder = require('../../../decoder')
const MessageSetDecoder = require('../../../messageSet/decoder')
const crc32 = require('../../../crc32')
const { Types } = require('../../../message/compression')

const osType = os.type().toLowerCase()

/**
 * Decodes a Produce v2 request body down to its (decompressed) message sets, so a comparison
 * does not depend on the exact bytes the local zlib emits.
 */
const decodeRequest = async (buffer) => {
  const decoder = new Decoder(buffer)
  const acks = decoder.readInt16()
  const timeout = decoder.readInt32()
  const topicData = []

  for (let topicCount = decoder.readInt32(); topicCount > 0; topicCount--) {
    const topic = decoder.readString()
    const partitions = []

    for (let partitionCount = decoder.readInt32(); partitionCount > 0; partitionCount--) {
      const partition = decoder.readInt32()
      partitions.push({ partition, messages: await MessageSetDecoder(decoder) })
    }

    topicData.push({ topic, partitions })
  }

  return { acks, timeout, topicData }
}

const GZIP_MAGIC = Buffer.from([0x1f, 0x8b])
// magicByte(1) + attributes(1) + timestamp(8) + null key(4) + value length(4)
const VALUE_OFFSET_IN_CONTENT = 18

/**
 * Reads the single compressed wrapper message of a Produce v2 request: the CRC the encoder wrote
 * and the exact bytes that CRC covers (magicByte..value, i.e. including the compressed payload).
 */
const readWrapperMessage = (buffer) => {
  const decoder = new Decoder(buffer)
  decoder.readInt16() // acks
  decoder.readInt32() // timeout
  decoder.readInt32() // topicData array length
  decoder.readString() // topic
  decoder.readInt32() // partitions array length
  decoder.readInt32() // partition
  decoder.readInt32() // message set size
  decoder.readInt64() // offset
  const messageSize = decoder.readInt32()
  const crc = decoder.readInt32()
  const content = buffer.slice(decoder.offset, decoder.offset + messageSize - 4)

  return { messageSize, crc, content }
}

describe('Protocol > Requests > Produce > v2', () => {
  let args

  beforeEach(() => {
    args = {
      acks: -1,
      timeout: 30000,
      compression: 0,
      topicData: [
        {
          topic: 'test-topic-9f825c3f60bb0b4db583',
          partitions: [
            {
              partition: 0,
              messages: [
                {
                  key: 'key-bb252ae5801883c12bbd',
                  value: 'some-value-10340c6329f8bbf5b4a2',
                  timestamp: 1509819296569,
                },
              ],
            },
          ],
        },
      ],
    }
  })

  describe('when acks=0', () => {
    test('expectResponse returns false', () => {
      const request = RequestV2Protocol({ ...args, acks: 0 })
      expect(request.expectResponse()).toEqual(false)
    })
  })

  test('request', async () => {
    const { buffer } = await RequestV2Protocol({
      acks: -1,
      timeout: 30000,
      compression: 0,
      topicData: [
        {
          topic: 'test-topic-9f825c3f60bb0b4db583',
          partitions: [
            {
              partition: 0,
              messages: [
                {
                  key: 'key-bb252ae5801883c12bbd',
                  value: 'some-value-10340c6329f8bbf5b4a2',
                  timestamp: 1509819296569,
                },
                {
                  key: 'key-8a14e73a88e93f7c3a39',
                  value: 'some-value-4fa91513bffbcc0e34b3',
                  timestamp: 1509819296569,
                },
                {
                  key: 'key-183a2d8eb3683f080b82',
                  value: 'some-value-938afcf1f2ef0439c752',
                  timestamp: 1509819296569,
                },
              ],
            },
          ],
        },
      ],
    }).encode()
    expect(buffer).toEqual(Buffer.from(require('../fixtures/v2_request.json')))
  })

  test('request with gzip', async () => {
    const { buffer } = await RequestV2Protocol({
      acks: -1,
      timeout: 30000,
      compression: Types.GZIP,
      topicData: [
        {
          topic: 'test-topic-43395f618a885920238c',
          partitions: [
            {
              partition: 0,
              messages: [
                {
                  key: 'key-d27f2271f5447fe62503',
                  value: 'some-value-e64a333e986853959623',
                  timestamp: 1509928155660,
                },
                {
                  key: 'key-3be6f0b8e6c987d0aedc',
                  value: 'some-value-7259046cfda805b0172e',
                  timestamp: 1509928155660,
                },
                {
                  key: 'key-af98821b43a80d6aa4e8',
                  value: 'some-value-94b9e769ec3e401bfd57',
                  timestamp: 1509928155660,
                },
              ],
            },
          ],
        },
      ],
    }).encode()

    // zlib's deflate output is not byte-stable across zlib/Node versions (Node 26 emits a stream
    // three bytes shorter than the recorded fixture), which also shifts the message-set sizes and
    // the wrapper CRC. Compare the decoded, decompressed request instead of the raw bytes.
    const expected = Buffer.from(require(`../fixtures/v2_request_gzip_${osType}.json`))
    expect(await decodeRequest(buffer)).toEqual(await decodeRequest(expected))

    // MessageSetDecoder transparently decompresses, so the wrapper message's own framing is
    // asserted here: its size and its CRC are computed over the compressed payload.
    const { messageSize, crc, content } = readWrapperMessage(buffer)
    expect(messageSize).toEqual(content.length + 4) // the crc int32 plus the content it covers
    expect(crc).toEqual(crc32({ buffer: content }))
    expect(content.readInt32BE(VALUE_OFFSET_IN_CONTENT - 4)).toEqual(
      content.length - VALUE_OFFSET_IN_CONTENT
    )
    expect(content.slice(VALUE_OFFSET_IN_CONTENT, VALUE_OFFSET_IN_CONTENT + 2)).toEqual(GZIP_MAGIC)
  })
})
