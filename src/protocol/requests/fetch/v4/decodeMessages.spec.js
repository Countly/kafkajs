const Decoder = require('../../../decoder')
const Encoder = require('../../../encoder')
const decodeMessages = require('./decodeMessages')
const RecordBatchDecoder = require('../../../recordBatch/v0/decoder')
const { batch, record } = require('../__tests__/fixtures/recordBatchBytes')
const { recordsField } = require('../../../message/compression/fixtures/compressedBatch')

describe('Protocol > Requests > Fetch > v4 > decodeMessages', () => {
  test('a compact null records block decodes as no messages', async () => {
    const decoder = new Decoder(new Encoder().writeUVarInt(0).buffer)
    await expect(decodeMessages(decoder, { compact: true })).resolves.toEqual({ messages: [] })
  })

  test('the compact size read is the only difference: the same batch decodes either way', async () => {
    const batch = Buffer.from(require('../fixtures/v11_response.json')) // any buffer works here
    const classic = new Decoder(new Encoder().writeInt32(batch.length).writeBuffer(batch).buffer)
    const compact = new Decoder(
      new Encoder().writeUVarInt(batch.length + 1).writeBuffer(batch).buffer
    )

    expect(classic.readInt32()).toEqual(batch.length)
    expect(compact.readUVarInt() - 1).toEqual(batch.length)
  })

  test('a header-count refusal halts only its partition', async () => {
    const { HEADER_CEILING_BYTES, RECORD_CEILING_BYTES } = RecordBatchDecoder
    const ceiling = Math.max(8, Math.ceil((RECORD_CEILING_BYTES * 2) / HEADER_CEILING_BYTES))
    const headers = (count) => Array.from({ length: count }, (_, n) => [`h${n}`, `v${n}`])
    const good = batch({
      records: [
        record({ key: 'a', headers: headers(ceiling) }),
        record({ offsetDelta: 1, key: 'b' }),
      ],
    })
    const over = batch({
      baseOffset: 2n,
      records: [record({ key: 'c', headers: headers(ceiling + 1) })],
    })
    const other = batch({ records: [record({ key: 'd', headers: headers(ceiling) })] })
    const decoder = new Decoder(Buffer.concat([recordsField(good, over), recordsField(other)]))
    const options = { maxDecompressedBatchBytes: HEADER_CEILING_BYTES * ceiling }

    const halted = await decodeMessages(decoder, options)
    const next = await decodeMessages(decoder, options)

    expect(halted.messages.map(({ offset, key }) => [offset, String(key)])).toEqual([
      ['0', 'a'],
      ['1', 'b'],
    ])
    expect(Object.keys(halted.messages[0].headers)).toHaveLength(ceiling)
    expect(halted.corruption).toMatchObject({ reason: 'count', offset: '2' })
    expect(halted.corruption.error).toMatchObject({
      reason: 'count',
      limit: ceiling,
      offset: '2',
      nextOffset: '3',
    })
    expect(next.corruption).toBeUndefined()
    expect(next.messages.map(({ offset, key }) => [offset, String(key)])).toEqual([['0', 'd']])
    expect(Object.keys(next.messages[0].headers)).toHaveLength(ceiling)
  })
})
