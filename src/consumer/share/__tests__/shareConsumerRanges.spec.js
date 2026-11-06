const {
  A,
  acquire,
  acquired,
  ackAnswer,
  deferred,
  fetchAnswer,
  flush,
  part,
  recordsHex,
  shareClient,
  until,
} = require('../../../../testHelpers/shareHarness')
const {
  batch,
  controlBatch,
  record,
} = require('../../../protocol/requests/fetch/__tests__/fixtures/recordBatchBytes')
const {
  compressedBatch,
} = require('../../../protocol/message/compression/fixtures/compressedBatch')
const { logLevel } = require('../../../../index')
const { gzipSync } = require('zlib')
const { ShareFetchLoop } = require('../shareFetchLoop')

/** Every timer the client owns is faked; socket answers still travel on real macrotasks. */
const FAKE = { doNotFake: ['setImmediate', 'queueMicrotask', 'nextTick', 'hrtime'] }
const FENCE = { errorCode: 110 }
const MAX_INT64 = 9223372036854775807n
const MISSING = 'Message does not belong to this handler call'
const TWO_PARTITIONS = {
  'topic-a': {
    topicId: A,
    partitions: [
      { leader: 0, epoch: 1 },
      { leader: 0, epoch: 1 },
    ],
  },
}

/**
 * @param {number} ms Fake milliseconds.
 * @returns {Promise<void>} Moves the fake clock and runs the client timers that fall due.
 */
const advance = (ms) => jest.advanceTimersByTimeAsync(ms)

/**
 * @param {object} frame A recorded ShareFetch or ShareAcknowledge frame.
 * @returns {object[]} The acknowledgement batches it carried, in wire order.
 */
const batchesOf = ({ fields }) =>
  fields.topics.flatMap((t) => t.partitions.flatMap((p) => p.acknowledgementBatches))

/**
 * @param {bigint} value An int64.
 * @returns {Buffer} Its eight big-endian bytes, as a frame carries an offset.
 */
const int64 = (value) => {
  const bytes = Buffer.alloc(8)
  bytes.writeBigInt64BE(value)
  return bytes
}

describe('public share range contract', () => {
  let h
  beforeEach(async () => {
    jest.useFakeTimers(FAKE)
    await advance(1000)
  })
  afterEach(async () => {
    try {
      await h?.close()
    } finally {
      h = undefined
      jest.useRealTimers()
      jest.restoreAllMocks()
    }
  })

  /**
   * @param {object} [options] `shareClient` options.
   * @returns {Promise<object>} A subscribed client whose public events are collected in `h.seen`.
   */
  const start = async (options) => {
    h = shareClient({ ...options })
    h.seen = { acks: [], stops: [], crashes: [] }
    const { events, on } = h.consumer
    on(events.ACKNOWLEDGE, ({ payload }) => h.seen.acks.push(payload))
    on(events.STOP, () => h.seen.stops.push(true))
    on(events.CRASH, ({ payload }) => h.seen.crashes.push(payload))
    await h.consumer.subscribe({ topics: ['topic-a'] })
    return h
  }
  /**
   * @param {object} config The `run` argument.
   * @returns {Promise<void>} Resolves once joined, with the heartbeat chain armed.
   */
  const run = async (config) => {
    await h.consumer.run(config)
    await flush()
  }
  /**
   * @param {number} count Heartbeat frames expected once the next tick's request was written.
   * @returns {Promise<void>}
   */
  const beat = async (count) => {
    await advance(50)
    await until(() => h.frames(76).length === count, `heartbeat frame ${count}`)
    await flush()
  }
  /** @returns {object[]} The final frames (ShareAcknowledge at epoch -1). */
  const finals = () => h.frames(79).filter(({ fields }) => fields.shareSessionEpoch === -1)
  /** @returns {Array<Array<string|null>>} Every public acknowledgement as [type, first, last, reason]. */
  const outcomes = () =>
    h.seen.acks.map(({ type, firstOffset, lastOffset, error }) => [
      type,
      firstOffset,
      lastOffset,
      error?.reason ?? null,
    ])

  describe('a halted record block', () => {
    const FIRST = batch({ baseOffset: 0n, records: [record({ key: 'k0', value: 'v' })] })
    const MARKER = controlBatch({ baseOffset: 2n })
    const failed = (forged) => batch({ records: [record({ key: 'k3', value: 'x' })], ...forged })
    const HALTS = [
      ['a checksum mismatch at the next offset', () => failed({ baseOffset: 3n, crc: 1 })],
      [
        'a checksum mismatch with unrelated base and delta',
        () => failed({ baseOffset: 5000n, lastOffsetDelta: 900000, crc: 1 }),
      ],
      ['a length below the magic 2 header', () => failed({ baseOffset: 3n, length: 20 })],
      ['an unknown magic byte', () => failed({ baseOffset: 3n, magic: 3 })],
      ['a record count the bytes cannot hold', () => failed({ baseOffset: 3n, count: 50 })],
      ...[
        ['a gzip body above the decompressed byte ceiling', { baseOffset: 3n }],
        [
          'a gzip body above the byte ceiling with unrelated base and delta',
          { baseOffset: 5000n, lastOffsetDelta: 900000 },
        ],
      ].map(([title, fields]) => [
        title,
        () =>
          compressedBatch({
            codec: 'gzip',
            records: 1,
            body: gzipSync(record({ key: 'k3', value: 'x'.repeat(1024) })),
            ...fields,
          }),
        512,
      ]),
      [
        'a gzip body that does not decompress',
        () =>
          compressedBatch({
            codec: 'gzip',
            records: 1,
            baseOffset: 3n,
            body: Buffer.from('not a gzip stream'),
          }),
      ],
    ]

    test.each(HALTS)(
      'releases a halted undecoded remainder compactly after %s',
      async (_, build, byteLimit) => {
        const logs = []
        if (byteLimit !== undefined) {
          const original = ShareFetchLoop.prototype.send
          jest.spyOn(ShareFetchLoop.prototype, 'send').mockImplementation(function (exchange) {
            this.decode.maxDecompressedBatchBytes = byteLimit
            return original.call(this, exchange)
          })
        }
        await start({
          kafka: {
            logLevel: logLevel.ERROR,
            logCreator:
              () =>
              ({ log }) =>
                logs.push(log),
          },
        })
        const records = Buffer.concat([FIRST, MARKER, await build()]).toString('hex')
        h.script('ShareFetch', [
          fetchAnswer([part({ partition: 0, acquire: [acquired(0, 1000000)], records })]),
        ])
        const handled = []
        await run({ eachMessage: async ({ message }) => handled.push(message.offset) })
        await until(() => h.frames(78).length >= 2 && h.seen.acks.length === 3, 'the settlement')
        expect(handled).toEqual(['0'])
        expect(batchesOf(h.frames(78)[1])).toEqual([
          { firstOffset: '0', lastOffset: '0', acknowledgeTypes: [1] },
          { firstOffset: '1', lastOffset: '2', acknowledgeTypes: [0] },
          { firstOffset: '3', lastOffset: '1000000', acknowledgeTypes: [2] },
        ])
        expect(outcomes()).toEqual([
          ['accept', '0', '0', null],
          ['gap', '1', '2', null],
          ['release', '3', '1000000', null],
        ])
        const halts = logs.filter(({ message }) => message === 'Share fetch decoding halted')
        expect(halts).toHaveLength(1)
        if (byteLimit !== undefined) {
          expect(halts[0].error).toMatchObject({
            name: 'KafkaJSCorruptRecordError',
            reason: 'decompressed-size',
            codec: 'gzip',
            limit: 512,
          })
        }
        expect(h.seen.crashes).toEqual([])
      }
    )
  })

  describe('int64 endpoints', () => {
    test.each(['9007199254740993', '9223372036854775807'])(
      'preserves int64 endpoints through bytes and results (%s)',
      async (offset) => {
        await start()
        h.script('ShareFetch', [
          fetchAnswer([
            part({
              partition: 0,
              acquire: [acquired(offset, offset)],
              records: recordsHex(BigInt(offset), ['v']),
            }),
          ]),
        ])
        const seen = []
        await run({ eachMessage: async ({ message }) => seen.push(message.offset) })
        await until(() => h.frames(78).length >= 2 && h.seen.acks.length === 1, 'the settlement')
        expect(seen).toEqual([offset])
        expect(batchesOf(h.frames(78)[1])).toEqual([
          { firstOffset: offset, lastOffset: offset, acknowledgeTypes: [1] },
        ])
        const written = h.cluster
          .sockets()
          .flatMap(({ writes }) => writes)
          .filter((frame) => frame.readInt16BE(4) === 78)
        expect(written.filter((frame) => frame.includes(int64(BigInt(offset))))).toHaveLength(1)
        const rounded = BigInt(Number(offset))
        expect(String(rounded)).not.toBe(offset)
        if (rounded <= MAX_INT64) {
          expect(written.some((frame) => frame.includes(int64(rounded)))).toBe(false)
        }
        expect(h.seen.acks[0]).toMatchObject({
          firstOffset: offset,
          lastOffset: offset,
          type: 'accept',
          error: null,
        })
      }
    )

    test('keeps negative and overflowing offsets inside the local validator', async () => {
      await start()
      h.script('ShareFetch', [fetchAnswer([acquire(0, 0, ['a'])])])
      const attempts = []
      const offsets = []
      await run({
        eachBatch: async ({ batch: delivered, acknowledge, renew }) => {
          offsets.push(...delivered.messages.map(({ offset }) => offset))
          const refused = [
            () => acknowledge({ offset: '-1' }, 'accept'),
            () => acknowledge({ offset: '9223372036854775808' }, 'accept'),
            () => acknowledge({ offset: -1 }, 'accept'),
            () => renew([{ offset: '-1' }]),
            () => renew([{ offset: '9223372036854775808' }]),
          ]
          for (const attempt of refused) {
            try {
              attempt()
              attempts.push('accepted')
            } catch (error) {
              attempts.push([error.name, error.message])
            }
          }
        },
      })
      await until(() => h.frames(78).length >= 2 && h.seen.acks.length === 1, 'the settlement')
      expect(offsets).toEqual(['0'])
      expect(attempts).toEqual(Array(5).fill(['KafkaJSNonRetriableError', MISSING]))
      expect(h.frames(79)).toHaveLength(0)
      expect(h.frames(78).flatMap(batchesOf)).toEqual([
        { firstOffset: '0', lastOffset: '0', acknowledgeTypes: [1] },
      ])
    })
  })

  describe('acknowledgement answers', () => {
    const SECOND_PARTITION = { topics: TWO_PARTITIONS }

    test('fails omitted partitions and ignores consumed or stale replies', async () => {
      await start(SECOND_PARTITION)
      h.script('ShareFetch', [
        fetchAnswer([acquire(0, 0, ['a']), acquire(1, 0, ['b'])]),
        () => fetchAnswer([part({ partition: 0 })]),
      ])
      await run({ eachMessage: async () => {} })
      await until(() => h.seen.acks.length === 2, 'both settlements')
      expect(h.seen.acks.map(({ partition, type }) => [partition, type])).toEqual([
        [0, 'accept'],
        [1, 'accept'],
      ])
      expect(h.seen.acks[0].error).toBeNull()
      expect(h.seen.acks[1].error).toMatchObject({
        name: 'KafkaJSShareAcknowledgementError',
        reason: 'rejected-by-broker',
        partition: 1,
        ranges: [{ firstOffset: '0', lastOffset: '0' }],
      })
      expect(h.seen.acks[1].error.cause.message).toBe(
        'Acknowledgement response omitted a partition'
      )
      await advance(50)
      await until(() => h.frames(78).length >= 3, 'the next quiet fetch')
      await flush()
      expect(h.frames(78).slice(2).flatMap(batchesOf)).toEqual([])
      expect(h.seen.acks).toHaveLength(2)
    })

    test('fails a repeated partition and settles a later equal-offset acquisition on its own', async () => {
      await start(SECOND_PARTITION)
      h.script('ShareFetch', [
        fetchAnswer([acquire(0, 0, ['a']), acquire(1, 0, ['b'])]),
        fetchAnswer([part({ partition: 0 }), part({ partition: 0 }), part({ partition: 1 })]),
        fetchAnswer([acquire(0, 0, ['a'])]),
      ])
      const handled = []
      await run({ eachMessage: async ({ partition }) => handled.push(partition) })
      await until(() => h.seen.acks.length === 3 && handled.length === 3, 'all settlements')
      expect(handled).toEqual([0, 1, 0])
      expect(h.seen.acks.map(({ partition, type }) => [partition, type])).toEqual([
        [0, 'accept'],
        [1, 'accept'],
        [0, 'accept'],
      ])
      expect(h.seen.acks[0].error).toMatchObject({
        reason: 'rejected-by-broker',
        partition: 0,
        ranges: [{ firstOffset: '0', lastOffset: '0' }],
      })
      expect(h.seen.acks[0].error.cause.message).toBe(
        'Acknowledgement response repeated a partition'
      )
      expect(h.seen.acks[1].error).toBeNull()
      expect(h.seen.acks[2].error).toBeNull()
      await advance(50)
      await flush()
      expect(h.seen.acks).toHaveLength(3)
    })

    test('ignores a consumed reply that arrives again and settles a later equal-offset acquisition once', async () => {
      await start()
      const redelivery = (deliveryCount) =>
        fetchAnswer([
          part({
            partition: 0,
            acquire: [acquired(0, 0, deliveryCount)],
            records: recordsHex(0, ['v']),
          }),
        ])
      h.script('ShareFetch', [redelivery(1), redelivery(2)])
      const second = h.hold({
        api: 'ShareFetch',
        match: ({ fields }) => fields.shareSessionEpoch === 1,
      })
      const third = h.hold({
        api: 'ShareFetch',
        match: ({ fields }) => fields.shareSessionEpoch === 2,
      })
      const counts = []
      await run({ eachMessage: async ({ message }) => counts.push(message.deliveryCount) })
      await second.arrived()
      const socket = h.cluster.sockets()[second.call.generation - 1]
      const replies = []
      const deliver = socket.emit.bind(socket)
      socket.emit = (name, ...args) => {
        if (name === 'data') replies.push(Buffer.from(args[0]))
        return deliver(name, ...args)
      }
      second.release()
      await third.arrived()
      expect(counts).toEqual([1, 2])
      expect(batchesOf(third.call)).toEqual([
        { firstOffset: '0', lastOffset: '0', acknowledgeTypes: [1] },
      ])
      expect(outcomes()).toEqual([['accept', '0', '0', null]])
      expect(replies).toHaveLength(1)
      socket.emit('data', replies[0])
      await flush()
      expect(outcomes()).toEqual([['accept', '0', '0', null]])
      expect(h.frames(78)).toHaveLength(3)
      third.release(fetchAnswer([part({ partition: 0, ack: 3 })]))
      await until(() => h.seen.acks.length === 2, 'the answer to the second acknowledgement')
      expect(outcomes()).toEqual([
        ['accept', '0', '0', null],
        ['accept', '0', '0', 'rejected-by-broker'],
      ])
      expect(counts).toEqual([1, 2])
      expect(h.seen.crashes).toEqual([])
    })
  })

  test('distinguishes repeated renewal outcomes from terminal outcomes', async () => {
    await start()
    const confirming = h.hold({
      api: 'ShareFetch',
      match: ({ fields }) => fields.shareSessionEpoch === 3,
    })
    h.script('ShareFetch', [
      fetchAnswer([
        part({
          partition: 0,
          acquire: [acquired(0, 1000000)],
          records: recordsHex(0, ['a']),
        }),
      ]),
    ])
    h.script('ShareAcknowledge', [
      ackAnswer([{ partition: 0 }], { acquisitionLockTimeoutMs: 2000 }),
      ackAnswer([{ partition: 0 }], { acquisitionLockTimeoutMs: 3000 }),
    ])
    const durations = []
    await run({
      eachMessage: async ({ renew }) => {
        durations.push(await renew())
        durations.push(await renew())
      },
    })
    await confirming.arrived()
    expect(h.consumer.acquisitionLockTimeoutMs()).toBe(3000)
    confirming.release()
    await until(() => h.seen.acks.length === 4, 'two renewals and two terminal outcomes')
    expect(durations).toEqual([2000, 3000])
    expect(outcomes()).toEqual([
      ['renew', '0', '0', null],
      ['renew', '0', '0', null],
      ['accept', '0', '0', null],
      ['gap', '1', '1000000', null],
    ])
    expect(Object.keys(h.seen.acks[3])).not.toContain('offsets')
    const renewals = h.frames(79)
    expect(renewals.map(({ fields }) => fields.isRenewAck)).toEqual([true, true])
    expect(renewals.map(batchesOf)).toEqual([
      [{ firstOffset: '0', lastOffset: '0', acknowledgeTypes: [4] }],
      [{ firstOffset: '0', lastOffset: '0', acknowledgeTypes: [4] }],
    ])
    expect(batchesOf(h.frames(78)[1])).toEqual([
      { firstOffset: '0', lastOffset: '0', acknowledgeTypes: [1] },
      { firstOffset: '1', lastOffset: '1000000', acknowledgeTypes: [0] },
    ])
    expect(h.consumer.acquisitionLockTimeoutMs()).toBe(15000)
  })

  describe('a forced close', () => {
    test.each([
      ['confirmed', ackAnswer([{ partition: 0 }]), null],
      ['refused by the broker', ackAnswer([{ partition: 0, code: 121 }]), 'rejected-by-broker'],
    ])(
      'transfers one captured terminal subset without ordinary lost duplication when the final is %s',
      async (_, answer, reason) => {
        await start()
        h.script('ShareFetch', [
          fetchAnswer([
            part({
              partition: 0,
              acquire: [acquired(0, 7)],
              records: recordsHex(0, ['a', 'b', 'c', 'd', 'e']),
            }),
          ]),
        ])
        h.script('ShareGroupHeartbeat', [undefined, FENCE])
        h.script('ShareAcknowledge', [answer])
        const gate = deferred()
        const decided = deferred()
        let stopping
        await run({
          eachBatch: async ({ batch: delivered, acknowledge }) => {
            acknowledge(delivered.messages[0], 'accept')
            acknowledge(delivered.messages[1], 'reject')
            decided.resolve()
            await gate.promise
          },
        })
        h.consumer.on(h.consumer.events.ACKNOWLEDGE, () => {
          if (stopping) return
          stopping = h.consumer.stop()
          throw new Error('observer')
        })
        await decided.promise
        await beat(2)
        await until(() => h.seen.acks.length === 4 && h.seen.stops.length === 1, 'every outcome')
        await stopping
        expect(batchesOf(finals()[0])).toEqual([
          { firstOffset: '0', lastOffset: '1', acknowledgeTypes: [1, 3] },
          { firstOffset: '5', lastOffset: '7', acknowledgeTypes: [0] },
        ])
        expect(outcomes()).toEqual([
          ['release', '2', '4', 'membership-lost'],
          ['accept', '0', '0', reason],
          ['reject', '1', '1', reason],
          ['gap', '5', '7', reason],
        ])
        expect(finals()).toHaveLength(1)
        expect(h.frames(76).filter(({ fields }) => fields.memberEpoch === -1)).toHaveLength(0)
        gate.resolve()
      }
    )

    test('captures unstarted releases and the gap without an ordinary loss report', async () => {
      await start()
      h.script('ShareFetch', [
        fetchAnswer([
          part({
            partition: 0,
            acquire: [acquired(0, 6)],
            records: recordsHex(0, ['a', 'b', 'c', 'd']),
          }),
        ]),
      ])
      h.script('ShareGroupHeartbeat', [undefined, FENCE])
      const gate = deferred()
      const decided = deferred()
      const started = []
      await run({
        eachMessage: async ({ message, acknowledge }) => {
          started.push(message.offset)
          acknowledge('reject')
          decided.resolve()
          await gate.promise
        },
      })
      await decided.promise
      await beat(2)
      await until(() => finals().length === 1 && h.seen.acks.length === 3, 'the captured final')
      expect(started).toEqual(['0'])
      expect(batchesOf(finals()[0])).toEqual([
        { firstOffset: '0', lastOffset: '3', acknowledgeTypes: [3, 2, 2, 2] },
        { firstOffset: '4', lastOffset: '6', acknowledgeTypes: [0] },
      ])
      expect(outcomes()).toEqual([
        ['reject', '0', '0', null],
        ['release', '1', '3', null],
        ['gap', '4', '6', null],
      ])
      gate.resolve()
    })

    /**
     * Fences a run whose handler decided record 0 and never returns, so its captured final carries
     * one accept; the final is held at the wire when `hold` is set.
     * @param {boolean} hold Whether the test holds the final's answer.
     * @returns {Promise<{final: object|null, gate: object}>} The held final, if any.
     */
    const fenceWithFinal = async (hold) => {
      await start()
      h.script('ShareFetch', [fetchAnswer([acquire(0, 0, ['a'])])])
      h.script('ShareGroupHeartbeat', [undefined, FENCE])
      const final = hold ? h.hold({ api: 'ShareAcknowledge' }) : null
      const gate = deferred()
      const decided = deferred()
      await run({
        eachMessage: async ({ acknowledge }) => {
          acknowledge('accept')
          decided.resolve()
          await gate.promise
        },
      })
      await decided.promise
      await beat(2)
      return { final, gate }
    }

    test('never labels a lost final result accepted', async () => {
      const { final, gate } = await fenceWithFinal(true)
      await final.arrived()
      expect(final.call.fields.shareSessionEpoch).toBe(-1)
      expect(batchesOf(final.call)).toEqual([
        { firstOffset: '0', lastOffset: '0', acknowledgeTypes: [1] },
      ])
      await flush()
      expect(h.seen.acks).toEqual([])
      await advance(5000)
      await until(() => h.seen.acks.length === 1, 'the lost settlement')
      expect(outcomes()).toEqual([['accept', '0', '0', 'session-lost']])
      final.release()
      await flush()
      expect(h.seen.acks).toHaveLength(1)
      expect(finals()).toHaveLength(1)
      gate.resolve()
    })

    test('reports a confirmed final as accepted exactly once', async () => {
      const { gate } = await fenceWithFinal(false)
      await until(() => h.seen.acks.length === 1, 'the confirmed settlement')
      await flush()
      expect(outcomes()).toEqual([['accept', '0', '0', null]])
      expect(finals()).toHaveLength(1)
      gate.resolve()
    })
  })
})
