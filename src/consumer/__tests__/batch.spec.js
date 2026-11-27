const Batch = require('../batch')
const Long = require('../../utils/long')
const Decoder = require('../../protocol/decoder')
const Encoder = require('../../protocol/encoder')
const decodeMessages = require('../../protocol/requests/fetch/v4/decodeMessages')
const {
  batch: recordBatchBytes,
} = require('../../protocol/requests/fetch/__tests__/fixtures/recordBatchBytes')

describe('Consumer > Batch', () => {
  const topic = 'topic-name'

  it('discards messages with a lower offset than the requested', () => {
    const fetchedOffset = 3
    const batch = new Batch(topic, fetchedOffset, {
      partition: 0,
      highWatermark: '100',
      messages: [
        { offset: '0' },
        { offset: '1' },
        { offset: '2' },
        { offset: '3' },
        { offset: '4' },
        { offset: '5' },
      ],
    })

    expect(batch.messages).toEqual([{ offset: '3' }, { offset: '4' }, { offset: '5' }])
  })

  it('discards control records', () => {
    const fetchedOffset = 0
    const batch = new Batch(topic, fetchedOffset, {
      partition: 0,
      highWatermark: '100',
      messages: [{ offset: '3', isControlRecord: true }, { offset: '4' }, { offset: '5' }],
    })

    expect(batch.messages).toEqual([{ offset: '4' }, { offset: '5' }])
  })

  describe('#isEmpty', () => {
    it('returns true when empty', () => {
      const batch = new Batch(topic, 0, {
        partition: 0,
        highWatermark: '100',
        messages: [],
      })

      expect(batch.isEmpty()).toEqual(true)
    })

    it('returns false when it has messages', () => {
      const batch = new Batch(topic, 0, {
        partition: 0,
        highWatermark: '100',
        messages: [{ offset: '1' }, { offset: '2' }],
      })

      expect(batch.isEmpty()).toEqual(false)
    })
  })

  describe('#firstOffset', () => {
    it('returns the offset of the first message', () => {
      const batch = new Batch(topic, 0, {
        partition: 0,
        highWatermark: '100',
        messages: [{ offset: '1' }, { offset: '2' }],
      })

      expect(batch.firstOffset()).toEqual('1')
    })

    it('returns null when the batch is empty', () => {
      const batch = new Batch(topic, 0, {
        partition: 0,
        highWatermark: '100',
        messages: [],
      })

      expect(batch.firstOffset()).toEqual(null)
    })
  })

  describe('#lastOffset', () => {
    it('returns the offset of the last message', () => {
      const batch = new Batch(topic, 0, {
        partition: 0,
        highWatermark: '100',
        messages: [{ offset: '1' }, { offset: '2' }],
      })

      expect(batch.lastOffset()).toEqual('2')
    })

    it('returns highWatermark - 1 when the batch is empty', () => {
      const batch = new Batch(topic, 0, {
        partition: 0,
        highWatermark: '100',
        messages: [],
      })

      expect(batch.lastOffset()).toEqual('99')
    })
  })

  describe('#offsetLag', () => {
    it('returns the difference between highWatermark - 1 and the last offset', () => {
      const batch = new Batch(topic, 0, {
        partition: 0,
        highWatermark: '100',
        messages: [{ offset: '3' }, { offset: '4' }],
      })

      expect(batch.offsetLag()).toEqual('95')
    })

    it('returns 0 when the batch is empty', () => {
      const batch = new Batch(topic, 0, {
        partition: 0,
        highWatermark: '100',
        messages: [],
      })

      expect(batch.offsetLag()).toEqual('0')
    })
  })

  describe('#offsetLagLow', () => {
    it('returns the difference between highWatermark - 1 and the first offset', () => {
      const batch = new Batch(topic, 0, {
        partition: 0,
        highWatermark: '100',
        messages: [{ offset: '3' }, { offset: '4' }],
      })

      expect(batch.offsetLagLow()).toEqual('96')
    })

    it('returns 0 when the batch is empty', () => {
      const batch = new Batch(topic, 0, {
        partition: 0,
        highWatermark: '100',
        messages: [],
      })

      expect(batch.offsetLagLow()).toEqual('0')
    })
  })

  describe('#isEmptyControlRecord', () => {
    it('returns false for regular batches', () => {
      const batch = new Batch(topic, 0, {
        partition: 0,
        highWatermark: '100',
        messages: [{ offset: '3' }, { offset: '4' }],
      })

      expect(batch.isEmptyControlRecord()).toEqual(false)
    })

    it('returns false for regular empty batches', () => {
      const batch = new Batch(topic, 0, {
        partition: 0,
        highWatermark: '100',
        messages: [],
      })

      expect(batch.isEmptyControlRecord()).toEqual(false)
    })

    it('returns false if there is a control record but some messages are available', () => {
      const batch = new Batch(topic, 0, {
        partition: 0,
        highWatermark: '100',
        messages: [{ offset: '3' }, { offset: '4' }, { isControlRecord: true, offset: '5' }],
      })

      expect(batch.isEmptyControlRecord()).toEqual(false)
    })

    it('returns true if the batch only contains a control record', () => {
      const batch = new Batch(topic, 0, {
        partition: 0,
        highWatermark: '100',
        messages: [{ isControlRecord: true, offset: '5' }],
      })

      expect(batch.isEmptyControlRecord()).toEqual(true)
    })
  })

  describe('#leaderEpochAt', () => {
    /** Records 120 and 121 carry different epochs, so the answer names which record it read. */
    const batch = () =>
      new Batch(topic, '119', {
        partition: 0,
        highWatermark: '200',
        messages: [
          { offset: '119', batchContext: { partitionLeaderEpoch: 6 } },
          { offset: '120', batchContext: { partitionLeaderEpoch: 7 } },
          { offset: '121', batchContext: { partitionLeaderEpoch: 8 } },
        ],
      })

    it.each([
      ['the string 120', '120'],
      ['the number 120', 120],
      ['a Long of 120', Long.fromValue('120')],
      ['the string 0120', '0120'],
    ])('answers the leader epoch of record 120 for %s', (_label, offset) => {
      expect(batch().leaderEpochAt(offset)).toBe(7)
    })

    it('answers the last record by value as well', () => {
      expect(batch().leaderEpochAt(121)).toBe(8)
    })

    it('answers -1 for an offset that is not in the batch or is not a number', () => {
      const fetched = batch()

      expect(fetched.leaderEpochAt(122)).toBe(-1)
      expect(fetched.leaderEpochAt('not a number')).toBe(-1)
    })

    // Record 0 is in the batch and is not its last record, so reading the value as 0 would answer 3.
    it.each([
      ['the string NaN', 'NaN'],
      ['the number NaN', NaN],
      ['the string Infinity', 'Infinity'],
      ['the number Infinity', Infinity],
    ])('answers -1 for %s, not the epoch of record 0', (_label, offset) => {
      const fromZero = new Batch(topic, '0', {
        partition: 0,
        highWatermark: '200',
        messages: [
          { offset: '0', batchContext: { partitionLeaderEpoch: 3 } },
          { offset: '1', batchContext: { partitionLeaderEpoch: 4 } },
        ],
      })

      expect(fromZero.leaderEpochAt('0')).toBe(3)
      expect(fromZero.leaderEpochAt(offset)).toBe(-1)
    })
  })

  describe('the end of the last complete batch', () => {
    const build = (fetchedOffset, partitionData) =>
      new Batch(topic, fetchedOffset, { partition: 0, highWatermark: '500', ...partitionData })

    it('moves an empty batch to the end of the header it carries, with that batch epoch', () => {
      const batch = build(0, { messages: [], nextBatchOffset: '100', nextBatchLeaderEpoch: 3 })

      expect(batch.isEmpty()).toEqual(true)
      expect(batch.isEmptyDueToFiltering()).toEqual(true)
      expect(batch.lastOffset()).toEqual('99')
      expect(batch.leaderEpochAt('99')).toEqual(3)
      expect(batch.leaderEpochAt('98')).toEqual(-1)
    })

    it('resolves with no epoch when the header carries none', () => {
      const batch = build(0, { messages: [], nextBatchOffset: '100' })

      expect(batch.lastOffset()).toEqual('99')
      expect(batch.leaderEpochAt('99')).toEqual(-1)
    })

    it('moves a batch whose records are all below the fetch offset to the header end', () => {
      const batch = build(2, { messages: [{ offset: '1' }], nextBatchOffset: '100' })

      expect(batch.isEmpty()).toEqual(true)
      expect(batch.isEmptyDueToFiltering()).toEqual(true)
      expect(batch.lastOffset()).toEqual('99')
    })

    it('compares the header end with the fetch offset as numbers, not as strings', () => {
      const advancing = build('9', { messages: [], nextBatchOffset: '10' })
      const behind = build('10', { messages: [], nextBatchOffset: '9' })

      expect(advancing.lastOffset()).toEqual('9')
      expect(advancing.isEmptyDueToFiltering()).toEqual(true)
      expect(behind.lastOffset()).toEqual('499')
      expect(behind.isEmptyDueToFiltering()).toEqual(false)
    })

    it('compares offsets above 2^53 exactly', () => {
      const equal = build('9007199254740993', { messages: [], nextBatchOffset: '9007199254740993' })
      const ahead = build('9007199254740993', { messages: [], nextBatchOffset: '9007199254740994' })

      expect(equal.isEmptyDueToFiltering()).toEqual(false)
      expect(ahead.isEmptyDueToFiltering()).toEqual(true)
      expect(ahead.lastOffset()).toEqual('9007199254740993')
    })

    it('changes nothing when the header end is at or below the fetch offset', () => {
      const atOffset = build(10, { messages: [], nextBatchOffset: '10', nextBatchLeaderEpoch: 3 })
      const below = build('10', { messages: [{ offset: '4' }], nextBatchOffset: '8' })

      expect(atOffset.isEmptyDueToFiltering()).toEqual(false)
      expect(atOffset.lastOffset()).toEqual('499')
      expect(atOffset.leaderEpochAt('499')).toEqual(-1)
      expect(below.isEmptyDueToFiltering()).toEqual(true)
      expect(below.lastOffset()).toEqual('10')
    })

    it('gives no header progress when the header end would pass int64', async () => {
      const header = recordBatchBytes({ baseOffset: 9223372036854775807n })
      const decoded = await decodeMessages(
        new Decoder(new Encoder().writeInt32(header.length).writeBuffer(header).buffer)
      )
      const batch = build('9223372036854775807', {
        ...decoded,
        highWatermark: '9223372036854775807',
      })

      expect(decoded).toEqual({ messages: [] })
      expect(batch.isEmptyDueToFiltering()).toEqual(false)
      expect(batch.lastOffset()).toEqual('9223372036854775806')
    })

    it('keeps the last record of a batch with records at or after the fetch offset', () => {
      const batch = build(0, {
        messages: [{ offset: '0' }, { offset: '1' }],
        nextBatchOffset: '100',
        nextBatchLeaderEpoch: 3,
      })

      expect(batch.isEmptyDueToFiltering()).toEqual(false)
      expect(batch.lastOffset()).toEqual('1')
    })

    it('keeps the last record when a control marker sits between two data records', () => {
      const batch = build(0, {
        messages: [{ offset: '0' }, { offset: '1', isControlRecord: true }, { offset: '2' }],
        nextBatchOffset: '100',
      })

      expect(batch.lastOffset()).toEqual('2')
    })

    it('keeps the last record when an aborted transaction hides every record', () => {
      const inTransaction = { producerId: '7', inTransaction: true }
      const batch = build(0, {
        messages: [
          { offset: '0', batchContext: inTransaction },
          {
            offset: '1',
            key: Buffer.from([0, 0, 0, 0]),
            isControlRecord: true,
            batchContext: inTransaction,
          },
        ],
        abortedTransactions: [{ producerId: '7', firstOffset: '0' }],
        nextBatchOffset: '100',
      })

      expect(batch.isEmpty()).toEqual(true)
      expect(batch.isEmptyDueToFiltering()).toEqual(true)
      expect(batch.lastOffset()).toEqual('1')
    })
  })
})
