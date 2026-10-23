const codec = require('../../../src/protocol/message/compression/snappy')
const { expansion, failuresOf } = require('../snappyCodec')

test('maximal-expansion stream has an independently known output', async () => {
  const decoded = await codec.decompress(expansion(1048576))
  expect(decoded).toEqual(Buffer.alloc(1048576, 97))
})

test('the benchmark refuses wrong byte counts and each declared gate breach', () => {
  const base = {
    checkedBytes: 1,
    outputBytes: 1,
    delayMs: 19.9,
    rssGrowth: 10,
    memoryBound: 10,
    decodeMiBs: 80,
    encodeMiBs: 30,
  }
  expect(failuresOf({ mixed: base })).toEqual([])
  for (const change of [
    { checkedBytes: 0 },
    { delayMs: 20 },
    { rssGrowth: 11 },
    { decodeMiBs: 79.9 },
    { encodeMiBs: 29.9 },
  ]) {
    expect(failuresOf({ mixed: { ...base, ...change } })).toHaveLength(1)
  }
})
