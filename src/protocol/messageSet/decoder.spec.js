const Encoder = require('../encoder')
const Decoder = require('../decoder')
const MessageSet = require('./index')
const MessageSetDecoder = require('./decoder')
const MessageV1 = require('../message/v1')
const gzip = require('../message/compression/gzip')
const { Types: Compression } = require('../message/compression')

/**
 * Builds a size-prefixed message set holding one magic 1 gzip wrapper whose inner messages carry
 * the relative offsets 0..count-1.
 * @param {string} wrapperOffset the wrapper's absolute offset, written as an exact int64
 * @param {number} count inner messages
 * @returns {Promise<Decoder>}
 */
const magic1GzipWrapper = async (wrapperOffset, count) => {
  const entries = Array.from({ length: count }, (_, i) => ({
    timestamp: 1,
    key: `key-${i}`,
    value: `value-${i}`,
  }))
  const inner = MessageSet({ messageVersion: 1, compression: Compression.GZIP, entries })
  const wrapper = MessageV1({
    compression: Compression.GZIP,
    timestamp: 1,
    key: null,
    value: await gzip.compress(inner),
  })
  const set = new Encoder()
    .writeInt64(wrapperOffset)
    .writeInt32(wrapper.size())
    .writeEncoder(wrapper)
  return new Decoder(new Encoder().writeInt32(set.size()).writeEncoder(set).buffer)
}

const messages = [
  {
    offset: '0',
    size: 31,
    crc: 120234579,
    magicByte: 0,
    attributes: 0,
    key: Buffer.from('key-0'),
    value: Buffer.from('some-value-0'),
  },
  {
    offset: '1',
    size: 31,
    crc: -141862522,
    magicByte: 0,
    attributes: 0,
    key: Buffer.from('key-1'),
    value: Buffer.from('some-value-1'),
  },
  {
    offset: '2',
    size: 31,
    crc: 1025004472,
    magicByte: 0,
    attributes: 0,
    key: Buffer.from('key-2'),
    value: Buffer.from('some-value-2'),
  },
]

const Fixtures = {
  v0: {
    uncompressed: {
      data: require('./fixtures/messages_v0_uncompressed.json'),
      output: messages,
    },
    gzip: {
      data: require('./fixtures/messages_v0_GZIP.json'),
      output: messages,
    },
  },
}

describe('Protocol > MessageSet > decoder', () => {
  Object.keys(Fixtures).forEach((version) => {
    describe(`message ${version}`, () => {
      Object.keys(Fixtures[version]).forEach((option) => {
        test(`decode ${option} messages`, async () => {
          const { data, output } = Fixtures[version][option]
          const decoder = new Decoder(Buffer.from(data))
          await expect(MessageSetDecoder(decoder)).resolves.toEqual(output)
        })
      })
    })
  })

  describe('a compressed message under a decompression bound', () => {
    const gzipped = () => new Decoder(Buffer.from(Fixtures.v0.gzip.data))
    // the three inner messages: an offset (8), a size (4) and a message of 31 bytes each
    const decompressed = messages.length * (8 + 4 + 31)

    test('is decoded when its output is exactly the bound', async () => {
      await expect(
        MessageSetDecoder(gzipped(), null, { maxDecompressedBatchBytes: decompressed })
      ).resolves.toEqual(messages)
    })

    test('is refused as a corrupt record when its output is one byte over the bound', async () => {
      await expect(
        MessageSetDecoder(gzipped(), null, { maxDecompressedBatchBytes: decompressed - 1 })
      ).rejects.toMatchObject({
        name: 'KafkaJSCorruptRecordError',
        reason: 'decompressed-size',
        codec: 'gzip',
        limit: decompressed - 1,
      })
    })
  })

  test('rebases magic 1 compressed offsets above 2^53 exactly', async () => {
    const one = await MessageSetDecoder(await magic1GzipWrapper('9007199254740993', 1))
    expect(one.map((m) => m.offset)).toEqual(['9007199254740993'])

    const three = await MessageSetDecoder(await magic1GzipWrapper('9007199254740993', 3))
    expect(three.map((m) => m.offset)).toEqual([
      '9007199254740991',
      '9007199254740992',
      '9007199254740993',
    ])
  })

  test('rebases magic 1 compressed offsets below 2^53 as before', async () => {
    const decoded = await MessageSetDecoder(await magic1GzipWrapper('12', 3))
    const fields = decoded.map(({ offset, magicByte, key, value }) => ({
      offset,
      magicByte,
      key,
      value,
    }))
    expect(fields).toEqual([
      { offset: '10', magicByte: 1, key: Buffer.from('key-0'), value: Buffer.from('value-0') },
      { offset: '11', magicByte: 1, key: Buffer.from('key-1'), value: Buffer.from('value-1') },
      { offset: '12', magicByte: 1, key: Buffer.from('key-2'), value: Buffer.from('value-2') },
    ])
  })

  test('skip partial messages', async () => {
    const messageSet = MessageSet({
      messageVersion: 0,
      entries: [{ key: 'v0-key', value: 'v0-value', offset: 10 }],
    })

    // read some bytes to simulate a partial message
    const messageSetBuffer = messageSet.buffer.slice(Decoder.int32Size(), messageSet.buffer.length)

    const temp = new Encoder()
    temp.buffer = messageSetBuffer

    const { buffer } = new Encoder().writeInt32(messageSet.size()).writeEncoder(temp)
    const decoder = new Decoder(buffer)

    await expect(MessageSetDecoder(decoder)).resolves.toEqual([])
  })
})
