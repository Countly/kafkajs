/* eslint-disable no-new-wrappers -- boxed primitives are inputs under test */
const { Kafka, CompressionTypes, CompressionCodecs } = require('../../../index')
const {
  KafkaJSDeliveryTimeout,
  KafkaJSInvalidConfigurationError,
  KafkaJSNonRetriableError,
  KafkaJSNotImplemented,
  KafkaJSProducerClosedError,
} = require('../../errors')
const handlerContext = require('../../consumer/handlerContext')
const Cluster = require('../../cluster')
const OffsetOperations = require('../../consumer/offsetManager/offsetOperations')
const TransactionOffsets = require('../state/transactionOffsets')
const apiKeys = require('../../protocol/requests/apiKeys')
const Decoder = require('../../protocol/decoder')
const gzip = require('../../protocol/message/compression/gzip')
const snappy = require('../../protocol/message/compression/snappy')
const lz4 = require('../../protocol/message/compression/lz4')
const zstd = require('../../protocol/message/compression/zstd')
const { Admission } = require('../state/admission')
const Accumulator = require('../state/accumulator')
const { BATCH_DEBIT, RequestBudget, chargeOf } = require('../state/recordSize')
const DeliveryCallbacks = require('../deliveryCallbacks')
const { readRequestHeader } = require('../../../testHelpers/fakeSocket')
const { createScriptedCluster, producerScript } = require('../../../testHelpers/scriptedCluster')
const { TOPIC } = require('../../consumer/__tests__/fixtures/fetchHarness')

const VALUE = 'send-option-row'
const MESSAGES = [{ value: VALUE, timestamp: 1000 }]
const ORIGINAL_CODECS = { ...CompressionCodecs }
/** A registry key that is a symbol, registered as an alias by the rows that need one. */
const ALIAS = Symbol('gzip alias')
/** The real admission, taken before any spy replaces it, for spies that log and call through. */
const ADMIT = Admission.prototype.acquire
const CODECS = { 1: gzip, 2: snappy, 3: lz4, 4: zstd }
/** Short, fixed backoffs: a retried batch waits 10 ms. */
const RETRY = { retries: 10, initialRetryTime: 10, maxRetryTime: 10, factor: 0 }
const PATHS = [
  ['the idempotent path', {}],
  ['the plain path', { idempotent: false }],
]

/** The two sends, each with the request one record and the given `acks` make. */
const SENDS = [
  ['send', (acks) => ({ topic: TOPIC, messages: MESSAGES, acks })],
  ['sendBatch', (acks) => ({ topicMessages: [{ topic: TOPIC, messages: MESSAGES }], acks })],
]

const tick = () => new Promise((resolve) => setImmediate(resolve))

/**
 * Watches a promise for a bounded number of event-loop turns.
 * @param {Promise<*>} promise
 * @param {number} [turns=2000]
 * @returns {Promise<{ status: string, value?: *, error?: Error }>} its state after the turns
 */
const outcome = async (promise, turns = 2000) => {
  const state = { status: 'pending' }
  promise.then(
    (value) => Object.assign(state, { status: 'fulfilled', value }),
    (error) => Object.assign(state, { status: 'rejected', error })
  )
  for (let i = 0; i < turns && state.status === 'pending'; i += 1) await tick()
  return state
}

/**
 * The Produce requests the client wrote, read from their bytes: acks, timeout and, per partition,
 * the codec bits of its one batch, its record count and the batch body after its header.
 * @param {import('../../../testHelpers/scriptedCluster').ScriptedCluster} cluster
 * @returns {Array<{ acks: number, timeout: number, partitions: Array<{ partition: number,
 *   codec: number, count: number, body: Buffer }> }>}
 */
const producedBy = (cluster) =>
  cluster
    .sockets()
    .flatMap((socket) => socket.writes)
    .filter((frame) => frame.readInt16BE(4) === apiKeys.Produce)
    .map((frame) => {
      const decoder = new Decoder(frame.subarray(readRequestHeader(frame).byteLength))
      decoder.readUVarIntString()
      const acks = decoder.readInt16()
      const timeout = decoder.readInt32()
      const partitions = decoder.readUVarIntArray((topic) => {
        if (frame.readInt16BE(6) >= 13) topic.readUuid()
        else topic.readUVarIntString()
        const entries = topic.readUVarIntArray((entry) => {
          const partition = entry.readInt32()
          const records = entry.readUVarIntBytes()
          entry.readTaggedFields()
          return {
            partition,
            codec: records.readInt16BE(21) & 7,
            count: records.readInt32BE(57),
            body: records.subarray(61, 12 + records.readInt32BE(8)),
          }
        })
        topic.readTaggedFields()
        return entries
      })
      return { acks, timeout, partitions: partitions.flat() }
    })

/** A real producer over a scripted cluster whose topic has two partitions led by one node. */
class Rig {
  /**
   * @param {object} [options] producer options over `linger: 0`
   * @param {{ silent?: boolean, partitions?: number, profile?: string }} [setup] `silent`: the
   *   broker never answers a Produce request, as a broker does not answer one with acks 0;
   *   `profile`: what the cluster advertises, `tv2` for a transactional producer
   * @returns {Promise<Rig>} connected
   */
  static async start(options = {}, { silent = false, partitions = 2, profile } = {}) {
    const cluster = producerScript(
      createScriptedCluster({ nodes: [{ id: 0, host: 'node0', port: 9092 }], profile }),
      { topic: TOPIC, partitions }
    )
    if (silent) cluster.answer('Produce', () => new Promise(() => {}))
    const producer = new Kafka(cluster.kafkaOptions()).producer({ linger: 0, ...options })
    await producer.connect()
    return new Rig(cluster, producer)
  }

  constructor(cluster, producer) {
    this.cluster = cluster
    this.producer = producer
  }

  /** @returns {ReturnType<typeof producedBy>} */
  produced() {
    return producedBy(this.cluster)
  }

  /** @returns {Promise<void>} once the producer is closed and the cluster is checked */
  async close() {
    await this.producer.disconnect({ timeout: 0 })
    this.cluster.assertClean()
  }
}

/**
 * @param {Buffer} body a batch body
 * @param {number} codec its codec bits
 * @returns {Promise<boolean>} whether the body, decompressed by the codec its bits name, holds the
 *   row's value
 */
const carriesValue = async (body, codec) => {
  const records = codec === 0 ? body : await CODECS[codec].decompress(body)
  return records.includes(Buffer.from(VALUE))
}

/** @param {number} codec @returns {boolean} whether this platform can run the built-in codec */
const runs = (codec) => (codec === 3 ? lz4.isSupported : codec === 4 ? zstd.isSupported : true)

const acks = (value) => () => ({ acks: value })
const timeout = (value) => () => ({ timeout: value })
const compression = (value) => () => ({ compression: value })

/**
 * @param {Function} method a conversion method
 * @returns {Function} `method` with an own `call` that is not the language's, so only an
 *   invocation that never reads `call` can run it
 */
const shadowed = (method) => Object.defineProperty(method, 'call', { value: undefined })

const ACKS_WRITTEN = [
  ['absent', -1, () => ({})],
  ['undefined', -1, acks(undefined)],
  ['-1', -1, acks(-1)],
  ['1', 1, acks(1)],
  ['0', 0, acks(0)],
  ['-0', 0, acks(-0)],
  ['1.0', 1, acks(1.0)],
  ['1.5', 1, acks(1.5)],
  ['1.9', 1, acks(1.9)],
  ['-1.5', -1, acks(-1.5)],
  ['-1.9', -1, acks(-1.9)],
  ["'0'", 0, acks('0')],
  ["'-0'", 0, acks('-0')],
  ["'+0'", 0, acks('+0')],
  ["' 0 '", 0, acks(' 0 ')],
  ["'00'", 0, acks('00')],
  ["'0.0'", 0, acks('0.0')],
  ["'0x0'", 0, acks('0x0')],
  ["'0b0'", 0, acks('0b0')],
  ["'0o0'", 0, acks('0o0')],
  ["'0e5'", 0, acks('0e5')],
  ["'\\t0\\n'", 0, acks('\t0\n')],
  ["'1'", 1, acks('1')],
  ["'+1'", 1, acks('+1')],
  ["' 1 '", 1, acks(' 1 ')],
  ["'1.0'", 1, acks('1.0')],
  ["'0x1'", 1, acks('0x1')],
  ["'1.5'", 1, acks('1.5')],
  ["'-1'", -1, acks('-1')],
  ["' -1 '", -1, acks(' -1 ')],
  ["'-1.5'", -1, acks('-1.5')],
  ['true', 1, acks(true)],
  ['new Number(1)', 1, () => ({ acks: new Number(1) })],
  ['new Number(-1)', -1, () => ({ acks: new Number(-1) })],
  ['new Number(0)', 0, () => ({ acks: new Number(0) })],
  ['new Number(-0)', 0, () => ({ acks: new Number(-0) })],
  ["new String('1')", 1, () => ({ acks: new String('1') })],
  ["new String('0')", 0, () => ({ acks: new String('0') })],
  ['new Boolean(true)', 1, () => ({ acks: new Boolean(true) })],
  ['[1]', 1, acks([1])],
  ["['1']", 1, acks(['1'])],
  ['[0]', 0, acks([0])],
  ["['0']", 0, acks(['0'])],
  ['{ valueOf: () => 1 }', 1, () => ({ acks: { valueOf: () => 1 } })],
  ["{ [Symbol.toPrimitive]: () => '1' }", 1, () => ({ acks: { [Symbol.toPrimitive]: () => '1' } })],
  ['{ valueOf: () => 0 }', 0, () => ({ acks: { valueOf: () => 0 } })],
  [
    "{ valueOf: () => ({}), toString: () => '1' }",
    1,
    () => ({ acks: { valueOf: () => ({}), toString: () => '1' } }),
  ],
  ["{ toString: () => '-1' }", -1, () => ({ acks: { toString: () => '-1' } })],
  ['a valueOf whose own call is undefined', 1, () => ({ acks: { valueOf: shadowed(() => 1) } })],
  [
    'a toString whose own call is undefined',
    -1,
    () => ({ acks: { toString: shadowed(() => '-1') } }),
  ],
  [
    'a Symbol.toPrimitive whose own call is undefined',
    1,
    () => ({ acks: { [Symbol.toPrimitive]: shadowed(() => '1') } }),
  ],
  ['new Date(1)', 1, () => ({ acks: new Date(1) })],
  ['new Date(-1)', -1, () => ({ acks: new Date(-1) })],
  ['new Date(0)', 0, () => ({ acks: new Date(0) })],
]

const ACKS_REFUSED = [
  ['0.5', acks(0.5)],
  ['-0.5', acks(-0.5)],
  ['1e-9', acks(1e-9)],
  ['2', acks(2)],
  ['-2', acks(-2)],
  ['3', acks(3)],
  ['32767', acks(32767)],
  ['1e3', acks(1e3)],
  ['32768', acks(32768)],
  ['1e10', acks(1e10)],
  ['Infinity', acks(Infinity)],
  ['-Infinity', acks(-Infinity)],
  ['NaN', acks(NaN)],
  ["'0.5'", acks('0.5')],
  ["'-0.5'", acks('-0.5')],
  ["'1e-9999'", acks('1e-9999')],
  ["'-1e-9999'", acks('-1e-9999')],
  ["'1e-400'", acks('1e-400')],
  ["'0_0'", acks('0_0')],
  ["'-0x0'", acks('-0x0')],
  ["''", acks('')],
  ["' '", acks(' ')],
  ["'\\n'", acks('\n')],
  ["'abc'", acks('abc')],
  ["'1a'", acks('1a')],
  ["'1,2'", acks('1,2')],
  ["'Infinity'", acks('Infinity')],
  ["'1e999'", acks('1e999')],
  ["'2'", acks('2')],
  ["'-2'", acks('-2')],
  ['false', acks(false)],
  ['null', acks(null)],
  ['0n', acks(0n)],
  ['1n', acks(1n)],
  ['-1n', acks(-1n)],
  ["Symbol('acks')", acks(Symbol('acks'))],
  ['new Number(0.5)', () => ({ acks: new Number(0.5) })],
  ['new Number(NaN)', () => ({ acks: new Number(NaN) })],
  ["new String('')", () => ({ acks: new String('') })],
  ["new String('1e-9999')", () => ({ acks: new String('1e-9999') })],
  ["['1e-9999']", acks(['1e-9999'])],
  ["{ valueOf: () => '1e-9999' }", () => ({ acks: { valueOf: () => '1e-9999' } })],
  ['new Boolean(false)', () => ({ acks: new Boolean(false) })],
  ['[]', acks([])],
  ['[1, 2]', acks([1, 2])],
  ['{}', acks({})],
  ['{ valueOf: () => false }', () => ({ acks: { valueOf: () => false } })],
  ['{ valueOf: () => null }', () => ({ acks: { valueOf: () => null } })],
  ['{ valueOf: () => undefined }', () => ({ acks: { valueOf: () => undefined } })],
  ['new Date()', () => ({ acks: new Date() })],
  ['new Date(2)', () => ({ acks: new Date(2) })],
  ['new Date(NaN)', () => ({ acks: new Date(NaN) })],
  ['a function', acks(() => 1)],
]

const ZERO_ACCEPTED = ['0', ' 0 ', '+0', '-0', '0.0', '.0', '0.', '0e5', '0x0', '0b0', '0o0', '00']
const ZERO_ACCEPTED_ALSO = ['0.0e5', '-0E-7', '+.0', '0X00', '0O0', '0B00']
const ZERO_REFUSED = ['', ' ', '1e-400', '0.0000001', 'zero', '0n', '０', '0e', '-0b0']

const TIMEOUT_WRITTEN = [
  ['absent', 30000, () => ({})],
  ['2147483647', 2147483647, timeout(2147483647)],
  ['-2147483648', -2147483648, timeout(-2147483648)],
  ['1000', 1000, timeout(1000)],
  ['0', 0, timeout(0)],
  ['1.5', 1, timeout(1.5)],
  ['-1.5', -1, timeout(-1.5)],
  ["'1000'", 1000, timeout('1000')],
  ["' 1000 '", 1000, timeout(' 1000 ')],
  ["'0x10'", 16, timeout('0x10')],
  ['[1000]', 1000, timeout([1000])],
  ['new Number(1000)', 1000, () => ({ timeout: new Number(1000) })],
  ['new Date(1000)', 1000, () => ({ timeout: new Date(1000) })],
  ['null', 0, timeout(null)],
  ['false', 0, timeout(false)],
  ["''", 0, timeout('')],
  ['[]', 0, timeout([])],
  ['true', 1, timeout(true)],
  ['NaN', 0, timeout(NaN)],
  ["'abc'", 0, timeout('abc')],
  ['{}', 0, timeout({})],
  ['{ valueOf: () => undefined }', 0, () => ({ timeout: { valueOf: () => undefined } })],
]

const TIMEOUT_REFUSED = [
  ['2147483647.5', timeout(2147483647.5)],
  ['-2147483648.5', timeout(-2147483648.5)],
  ['2147483648', timeout(2147483648)],
  ['Infinity', timeout(Infinity)],
  ['-Infinity', timeout(-Infinity)],
  ["'1e10'", timeout('1e10')],
  ['0n', timeout(0n)],
  ["Symbol('timeout')", timeout(Symbol('timeout'))],
]

const COMPRESSION_WRITTEN = [
  ['absent', 0, () => ({})],
  ['0', 0, compression(0)],
  ['-0', 0, compression(-0)],
  ['1', 1, compression(1)],
  ['2', 2, compression(2)],
  ['3', 3, compression(3)],
  ['4', 4, compression(4)],
  ["'1'", 1, compression('1')],
  ["'2'", 2, compression('2')],
  ["'3'", 3, compression('3')],
  ["'4'", 4, compression('4')],
  ['new Number(2)', 2, () => ({ compression: new Number(2) })],
  ["new String('3')", 3, () => ({ compression: new String('3') })],
  ['[4]', 4, compression([4])],
  ["['1']", 1, compression(['1'])],
  [
    "{ valueOf: () => 9, toString: () => '1' }",
    1,
    () => ({ compression: { valueOf: () => 9, toString: () => '1' } }),
  ],
  ["'0'", 0, compression('0')],
  ["{ toString: () => '0' }", 0, () => ({ compression: { toString: () => '0' } })],
]

const COMPRESSION_REFUSED = [
  [
    "{ valueOf: () => 2, toString: () => '1' }",
    () => ({ compression: { valueOf: () => 2, toString: () => '1' } }),
  ],
  [
    "{ [Symbol.toPrimitive]: (hint) => (hint === 'number' ? 1 : '0') }",
    () => ({ compression: { [Symbol.toPrimitive]: (hint) => (hint === 'number' ? 1 : '0') } }),
  ],
  ['9', compression(9)],
  ['-7', compression(-7)],
  ['1.5', compression(1.5)],
  ['5', compression(5)],
  ['6', compression(6)],
  ['7', compression(7)],
  ['8', compression(8)],
  ['NaN', compression(NaN)],
  ['Infinity', compression(Infinity)],
  ["' 1'", compression(' 1')],
  ["'01'", compression('01')],
  ["'1.0'", compression('1.0')],
  ["''", compression('')],
  ['null', compression(null)],
  ['true', compression(true)],
  ['false', compression(false)],
  ['0n', compression(0n)],
  ['1n', compression(1n)],
  ["Symbol('compression')", compression(Symbol('compression'))],
  [
    '{ [Symbol.toPrimitive]: () => undefined }',
    () => ({ compression: { [Symbol.toPrimitive]: () => undefined } }),
  ],
]

const distinct = () => require('../../protocol/message/compression/gzip')

const REGISTRY_WRITTEN = [
  [
    '9 with CompressionCodecs[9] = CompressionCodecs[1]',
    1,
    () => {
      CompressionCodecs[9] = CompressionCodecs[1]
    },
    compression(9),
  ],
  [
    "'9' with CompressionCodecs[9] = CompressionCodecs[1]",
    1,
    () => {
      CompressionCodecs[9] = CompressionCodecs[1]
    },
    compression('9'),
  ],
  [
    '[9] with CompressionCodecs[9] = CompressionCodecs[1]',
    1,
    () => {
      CompressionCodecs[9] = CompressionCodecs[1]
    },
    compression([9]),
  ],
  [
    'new Number(9) with CompressionCodecs[9] = CompressionCodecs[1]',
    1,
    () => {
      CompressionCodecs[9] = CompressionCodecs[1]
    },
    () => ({ compression: new Number(9) }),
  ],
  [
    'an object that is 1 as a number and a symbol registered as CompressionCodecs[1] as a string',
    1,
    () => {
      CompressionCodecs[ALIAS] = CompressionCodecs[1]
    },
    () => ({ compression: { [Symbol.toPrimitive]: (hint) => (hint === 'number' ? 1 : ALIAS) } }),
  ],
  [
    "0 with a function registered under '0'",
    0,
    () => {
      CompressionCodecs[0] = CompressionCodecs[1]
    },
    compression(0),
  ],
]

const REGISTRY_REFUSED = [
  [
    '9 with CompressionCodecs[9] = CompressionCodecs[2]',
    () => {
      CompressionCodecs[9] = CompressionCodecs[2]
    },
    compression(9),
  ],
  [
    '9 with a distinct function registered as CompressionCodecs[9]',
    () => {
      CompressionCodecs[9] = distinct
    },
    compression(9),
  ],
  [
    '8 with CompressionCodecs[8] = CompressionCodecs[1]',
    () => {
      CompressionCodecs[8] = CompressionCodecs[1]
    },
    compression(8),
  ],
  [
    "'8' with CompressionCodecs[8] = CompressionCodecs[1]",
    () => {
      CompressionCodecs[8] = CompressionCodecs[1]
    },
    compression('8'),
  ],
  [
    '13 with CompressionCodecs[13] registered',
    () => {
      CompressionCodecs[13] = CompressionCodecs[1]
    },
    compression(13),
  ],
  [
    "'0' with a function registered under '0'",
    () => {
      CompressionCodecs[0] = CompressionCodecs[1]
    },
    compression('0'),
  ],
]

describe('Producer > send options', () => {
  let rig, acquire, debit

  beforeEach(() => {
    acquire = jest.spyOn(Admission.prototype, 'acquire')
    debit = jest.spyOn(Admission.prototype, 'debit')
  })

  afterEach(async () => {
    try {
      if (rig) await rig.close()
    } finally {
      rig = null
      jest.useRealTimers()
      jest.restoreAllMocks()
      for (const key of Reflect.ownKeys(CompressionCodecs)) {
        if (!(key in ORIGINAL_CODECS)) delete CompressionCodecs[key]
      }
      for (const [key, factory] of Object.entries(ORIGINAL_CODECS)) {
        Object.defineProperty(CompressionCodecs, key, {
          value: factory,
          writable: true,
          enumerable: true,
          configurable: true,
        })
      }
    }
  })

  /**
   * Sends one record with `options` from `make()` on a producer started with `producerOptions`,
   * and expects the send to complete with one request that carries what the row says.
   * @returns {Promise<object>} the one Produce request
   */
  const expectWritten = async (make, producerOptions, silent) => {
    rig = await Rig.start(producerOptions, { silent })
    const state = await outcome(rig.producer.send({ topic: TOPIC, messages: MESSAGES, ...make() }))
    expect(state.status).toBe('fulfilled')
    const produced = rig.produced()
    expect(produced).toHaveLength(1)
    return produced[0]
  }

  /**
   * Sends one record with `options` from `make()` on an idempotent producer and expects the send
   * to be refused at the call: nothing admitted, charged or written, idempotence still on.
   * @returns {Promise<Error>} the refusal
   */
  const expectRefused = async (make, type = KafkaJSInvalidConfigurationError) => {
    rig = await Rig.start()
    const error = await rig.producer.send({ topic: TOPIC, messages: MESSAGES, ...make() }).then(
      () => null,
      (failure) => failure
    )
    expect(error).toBeInstanceOf(type)
    await tick()
    expect(acquire).not.toHaveBeenCalled()
    expect(debit).not.toHaveBeenCalled()
    expect(rig.produced()).toEqual([])
    expect(rig.producer.isIdempotent()).toBe(true)
    return error
  }

  describe('acks', () => {
    test.each(ACKS_WRITTEN)('%s is written as %s', async (_, wire, make) => {
      const request = await expectWritten(make, { idempotent: false }, wire === 0)
      expect(request.acks).toBe(wire)
      const [record] = rig.cluster.requests().filter(({ api }) => api === 'Produce')
      expect(record.answeredOrder === undefined).toBe(wire === 0)
    })

    test.each(ACKS_REFUSED)(
      '%s is refused at the call: nothing admitted, written or switched',
      async (_, make) => {
        await expectRefused(make)
      }
    )

    test.each(ZERO_ACCEPTED)('the zero grammar accepts %p', async (text) => {
      const request = await expectWritten(acks(text), { idempotent: false }, true)
      expect(request.acks).toBe(0)
    })

    test.each(ZERO_ACCEPTED_ALSO)('the zero grammar also accepts %p', async (text) => {
      const request = await expectWritten(acks(text), { idempotent: false }, true)
      expect(request.acks).toBe(0)
    })

    test.each(ZERO_REFUSED)('the zero grammar refuses %p', async (text) => {
      await expectRefused(acks(text))
    })

    test.each([
      ["'-1'", acks('-1')],
      ["' -1 '", acks(' -1 ')],
      ['new Number(-1)', () => ({ acks: new Number(-1) })],
      ['[-1]', acks([-1])],
    ])('an explicitly idempotent producer sends %s as -1 and stays idempotent', async (_, make) => {
      const request = await expectWritten(make, { idempotent: true })
      expect(request.acks).toBe(-1)
      expect(rig.producer.isIdempotent()).toBe(true)
    })

    test('an object whose conversion throws is refused with the throw as cause', async () => {
      const thrown = new Error('acks conversion failed')
      const error = await expectRefused(() => ({
        acks: {
          valueOf() {
            throw thrown
          },
        },
      }))
      expect(error.cause).toBe(thrown)
    })

    test('an object that converts to no primitive is refused with a TypeError as cause', async () => {
      const error = await expectRefused(() => ({
        acks: { valueOf: () => ({}), toString: () => ({}) },
      }))
      expect(error.cause).toBeInstanceOf(TypeError)
    })
  })

  describe('timeout', () => {
    test.each(TIMEOUT_WRITTEN)('%s is written as %s', async (_, wire, make) => {
      const request = await expectWritten(make)
      expect(request.timeout).toBe(wire)
    })

    test.each(TIMEOUT_REFUSED)(
      '%s is refused at the call: nothing admitted, written or switched',
      async (_, make) => {
        await expectRefused(make)
      }
    )

    test('an object whose conversion throws is refused with the throw as cause', async () => {
      const thrown = new Error('timeout conversion failed')
      const error = await expectRefused(() => ({
        timeout: {
          valueOf() {
            throw thrown
          },
        },
      }))
      expect(error.cause).toBe(thrown)
    })
  })

  describe('compression', () => {
    test.each(COMPRESSION_WRITTEN)(
      '%s is written with codec bits %s and compressed by that codec',
      async (_, bits, make) => {
        if (!runs(bits)) {
          await expectRefused(make, KafkaJSNotImplemented)
          return
        }
        const { partitions } = await expectWritten(make)
        expect(partitions.map(({ codec }) => codec)).toEqual([bits])
        expect(await carriesValue(partitions[0].body, bits)).toBe(true)
      }
    )

    test.each(COMPRESSION_REFUSED)(
      '%s is refused at the call: nothing admitted, written or switched',
      async (_, make) => {
        await expectRefused(make)
      }
    )

    test('an object whose conversion throws is refused with the throw as cause', async () => {
      const thrown = new Error('compression conversion failed')
      const error = await expectRefused(() => ({
        compression: {
          valueOf() {
            throw thrown
          },
        },
      }))
      expect(error.cause).toBe(thrown)
    })

    test('a symbol key with no codec is refused naming the symbol, not as an unconvertible value', async () => {
      const error = await expectRefused(() => ({
        compression: { [Symbol.toPrimitive]: (hint) => (hint === 'number' ? 1 : ALIAS) },
      }))
      expect(error.message).toContain('CompressionCodecs[Symbol(gzip alias)]')
      expect(error.cause).toBeUndefined()
    })

    test('a built-in codec this platform cannot run is refused at the call with KafkaJSNotImplemented', async () => {
      jest.replaceProperty(zstd, 'isSupported', false)
      const error = await expectRefused(compression(CompressionTypes.ZSTD), KafkaJSNotImplemented)
      expect(error.message).toBe(zstd.NOT_SUPPORTED_MESSAGE)
    })
  })

  describe('the codec registry', () => {
    test.each(REGISTRY_WRITTEN)(
      '%s is written with codec bits %s and compressed by that codec',
      async (_, bits, register, make) => {
        rig = await Rig.start()
        register()
        const state = await outcome(
          rig.producer.send({ topic: TOPIC, messages: MESSAGES, ...make() })
        )
        expect(state.status).toBe('fulfilled')
        const [{ partitions }] = rig.produced()
        expect(partitions.map(({ codec }) => codec)).toEqual([bits])
        expect(await carriesValue(partitions[0].body, bits)).toBe(true)
      }
    )

    test.each(REGISTRY_REFUSED)(
      '%s is refused at the call: nothing admitted, written or switched',
      async (_, register, make) => {
        register()
        await expectRefused(make)
      }
    )

    test('the registry is read once at the key and once at the canonical key, before admission', async () => {
      const log = []
      rig = await Rig.start()
      acquire.mockImplementation(function (...args) {
        log.push('admission')
        return ADMIT.apply(this, args)
      })
      const factory = ORIGINAL_CODECS[CompressionTypes.GZIP]
      for (const key of ['9', '1']) {
        Object.defineProperty(CompressionCodecs, key, {
          configurable: true,
          enumerable: true,
          get: () => {
            log.push(`registry ${key}`)
            return factory
          },
        })
      }

      const state = await outcome(
        rig.producer.send({ topic: TOPIC, messages: MESSAGES, compression: 9 })
      )

      expect(state.status).toBe('fulfilled')
      expect(log).toEqual(['registry 9', 'registry 1', 'admission'])
      const [{ partitions }] = rig.produced()
      expect(partitions.map(({ codec }) => codec)).toEqual([CompressionTypes.GZIP])
      expect(await carriesValue(partitions[0].body, CompressionTypes.GZIP)).toBe(true)
    })
  })

  describe('reads and conversions', () => {
    /**
     * A request whose option reads and conversions are logged; `acks` converts to -1, `timeout`
     * to 1000 and `compression` to `bits` and `String(bits)`. Its other fields are plain.
     * @param {string[]} log
     * @param {number} bits
     * @param {object} fields plain fields, such as `topic` and `messages`
     * @returns {object}
     */
    const recorded = (log, bits, fields) =>
      Object.defineProperties(
        { ...fields },
        {
          acks: {
            enumerable: true,
            get() {
              log.push('read acks')
              return {
                [Symbol.toPrimitive]: (hint) => {
                  log.push(`acks toPrimitive ${hint}`)
                  return -1
                },
              }
            },
          },
          timeout: {
            enumerable: true,
            get() {
              log.push('read timeout')
              return {
                valueOf: () => {
                  log.push('timeout valueOf')
                  return 1000
                },
              }
            },
          },
          compression: {
            enumerable: true,
            get() {
              log.push('read compression')
              return {
                valueOf: () => {
                  log.push('compression valueOf')
                  return bits
                },
                toString: () => {
                  log.push('compression toString')
                  return String(bits)
                },
              }
            },
          },
        }
      )

    const READS = [
      'read acks',
      'read timeout',
      'read compression',
      'acks toPrimitive number',
      'timeout valueOf',
      'compression valueOf',
      'compression toString',
      'admission',
    ]

    /** Logs each admission into `log`, calling through. */
    const logAdmission = (log) => {
      acquire.mockImplementation(function (...args) {
        log.push('admission')
        return ADMIT.apply(this, args)
      })
    }

    test('startedAt is taken before the first option is read: a getter that spends the delivery deadline makes the send time out unsent', async () => {
      rig = await Rig.start({ deliveryTimeout: 5000 })
      const now = Date.now
      let shift = 0
      jest.spyOn(Date, 'now').mockImplementation(() => now() + shift)

      const error = await rig.producer
        .send({
          get acks() {
            shift += 10000
            return -1
          },
          topic: TOPIC,
          messages: MESSAGES,
        })
        .then(
          () => null,
          (failure) => failure
        )

      expect(error).toBeInstanceOf(KafkaJSDeliveryTimeout)
      expect(error.outcome).toBe('unsent')
      expect(rig.produced()).toEqual([])
    })

    test.each(PATHS)(
      'on %s each option is read once and converted once before admission, and never again across a retry',
      async (_, options) => {
        const log = []
        rig = await Rig.start({ ...options, retry: RETRY })
        logAdmission(log)
        const answer = rig.cluster.answerers.get('Produce')
        let answered = 0
        rig.cluster.answer('Produce', (fields, record) => {
          answered += 1
          const response = answer(fields, record)
          if (answered > 1) return response
          return {
            ...response,
            responses: response.responses.map((topic) => ({
              ...topic,
              partitions: topic.partitions.map((partition) => ({
                ...partition,
                errorCode: 19,
                baseOffset: '-1',
              })),
            })),
          }
        })

        // Awaited, not polled by turns: the retry waits on its real-time backoff timer.
        await expect(
          rig.producer.send(
            recorded(log, CompressionTypes.GZIP, { topic: TOPIC, messages: MESSAGES })
          )
        ).resolves.toEqual([expect.objectContaining({ topicName: TOPIC })])

        expect(answered).toBe(2)
        expect(rig.produced()).toHaveLength(2)
        expect(log).toEqual(READS)
      }
    )

    test.each(PATHS)(
      'on %s each option is read once and converted once before admission, and never again across a batch split',
      async (_, options) => {
        const log = []
        CompressionCodecs[CompressionTypes.Snappy] = () => ({
          compress: async ({ buffer }) => Buffer.concat([buffer, Buffer.alloc(buffer.length)]),
          decompress: async (buffer) => buffer,
        })
        rig = await Rig.start({ ...options, retry: RETRY }, { partitions: 3 })
        logAdmission(log)
        const messages = [0, 1, 2].flatMap((partition) =>
          Array.from({ length: 8 }, (_, i) => ({
            value: `${partition}-${i}`.padEnd(40000, 'x'),
            partition,
          }))
        )

        const state = await outcome(
          rig.producer.send(recorded(log, CompressionTypes.Snappy, { topic: TOPIC, messages }))
        )

        expect(state.status).toBe('fulfilled')
        const produced = rig.produced()
        expect(produced.length).toBeGreaterThan(1)
        const counts = produced.flatMap(({ partitions }) => partitions.map(({ count }) => count))
        expect(counts.reduce((sum, count) => sum + count, 0)).toBe(24)
        expect(log).toEqual(READS)
      }
    )

    test('sendBatch reads each option and topicMessages once, and converts each option before admission', async () => {
      const log = []
      rig = await Rig.start()
      logAdmission(log)
      const request = Object.defineProperty(
        recorded(log, CompressionTypes.GZIP, {}),
        'topicMessages',
        {
          enumerable: true,
          get() {
            log.push('read topicMessages')
            return [{ topic: TOPIC, messages: MESSAGES }]
          },
        }
      )

      const state = await outcome(rig.producer.sendBatch(request))

      expect(state.status).toBe('fulfilled')
      expect(log).toEqual([...READS.slice(0, 3), 'read topicMessages', ...READS.slice(3)])
    })
  })

  describe('the codec pinned at the call', () => {
    const MARK = Buffer.from('replaced-codec:')

    /** A gzip-slot codec distinct from the built-in one, whose output starts with `MARK`. */
    const replacement = () =>
      jest.fn(() => ({
        compress: async ({ buffer }) => Buffer.concat([MARK, buffer]),
        decompress: async (buffer) => buffer.subarray(MARK.length),
      }))

    test.each(PATHS)(
      'on %s a codec replaced between two sends to two partitions of one broker: each batch is grouped, planned and compressed by the codec of its own send',
      async (_, options) => {
        const capacity = jest.spyOn(RequestBudget.prototype, 'chunkCapacity')
        const planned = jest.spyOn(RequestBudget.prototype, 'batchBytes')
        const replaced = replacement()
        rig = await Rig.start({ ...options, linger: 60000 })
        const send = (partition) =>
          rig.producer.send({
            topic: TOPIC,
            compression: CompressionTypes.GZIP,
            messages: [{ value: VALUE, timestamp: 1000, partition }],
          })

        const first = send(0)
        CompressionCodecs[CompressionTypes.GZIP] = replaced
        const second = send(1)
        expect(replaced).not.toHaveBeenCalled()
        for (let i = 0; i < 2000 && capacity.mock.calls.length < 4; i += 1) await tick()
        expect(capacity).toHaveBeenCalledTimes(4)
        expect(rig.produced()).toEqual([])

        await rig.producer.flush()
        await expect(Promise.all([first, second])).resolves.toHaveLength(2)

        const [atFirstCall, atSecondCall, atFirstJoin, atSecondJoin] = capacity.mock.results.map(
          ({ value }) => value
        )
        expect(atFirstJoin).toBe(atFirstCall)
        expect(atSecondJoin).toBe(atSecondCall)
        expect(atFirstCall).not.toBe(atSecondCall)
        const plannedFor = (partition) =>
          planned.mock.results[
            planned.mock.calls.findIndex(([batch]) => batch.partition === partition)
          ].value
        expect(plannedFor(0)).toBeGreaterThan(plannedFor(1))

        const produced = rig.produced()
        expect(produced).toHaveLength(2)
        const batches = produced.flatMap(({ partitions }) => partitions)
        const own = (partition) => batches.find((batch) => batch.partition === partition)
        expect(batches.map(({ codec }) => codec)).toEqual([1, 1])
        expect(await carriesValue(own(0).body, 1)).toBe(true)
        expect(own(1).body.subarray(0, MARK.length)).toEqual(MARK)
        expect(replaced).toHaveBeenCalledTimes(1)
      }
    )

    test.each(PATHS)(
      'on %s a codec replaced between two sends to one partition: the second send starts its own batch',
      async (_, options) => {
        const replaced = replacement()
        rig = await Rig.start({ ...options, linger: 60000 })
        const send = () =>
          rig.producer.send({
            topic: TOPIC,
            compression: CompressionTypes.GZIP,
            messages: [{ value: VALUE, timestamp: 1000, partition: 0 }],
          })

        const first = send()
        CompressionCodecs[CompressionTypes.GZIP] = replaced
        const second = send()
        await rig.producer.flush()
        await expect(Promise.all([first, second])).resolves.toHaveLength(2)

        const batches = rig.produced().flatMap(({ partitions }) => partitions)
        expect(batches.map(({ count }) => count)).toEqual([1, 1])
        expect(await carriesValue(batches[0].body, 1)).toBe(true)
        expect(batches[1].body.subarray(0, MARK.length)).toEqual(MARK)
      }
    )

    test.each(PATHS)(
      'on %s a factory that needs the registry as its receiver is called with it for the batch of its send, after the registry entry is replaced',
      async (_, options) => {
        const receivers = []
        const factory = function () {
          receivers.push(this)
          return {
            compress: async ({ buffer }) => Buffer.concat([MARK, buffer]),
            decompress: async (buffer) => buffer.subarray(MARK.length),
          }
        }
        const replaced = replacement()
        rig = await Rig.start({ ...options, linger: 60000 })
        CompressionCodecs[CompressionTypes.GZIP] = factory
        const sent = rig.producer.send({
          topic: TOPIC,
          compression: CompressionTypes.GZIP,
          messages: [{ value: VALUE, timestamp: 1000, partition: 0 }],
        })
        CompressionCodecs[CompressionTypes.GZIP] = replaced
        await rig.producer.flush()
        await expect(sent).resolves.toHaveLength(1)

        expect(receivers.length).toBeGreaterThan(0)
        for (const receiver of receivers) expect(receiver === CompressionCodecs).toBe(true)
        expect(replaced).not.toHaveBeenCalled()
        const [{ partitions }] = rig.produced()
        expect(partitions[0].body.subarray(0, MARK.length)).toEqual(MARK)
      }
    )
  })

  describe('a transaction that ends while a send converts its options', () => {
    /** The Produce requests the cluster saw, whatever version the profile negotiates. */
    const produceRequests = () => rig.cluster.requests().filter(({ api }) => api === 'Produce')

    test.each(SENDS)(
      '%s is refused after the conversion: nothing admitted, charged or written',
      async (method, request) => {
        rig = await Rig.start({ transactionalId: 'converting' }, { profile: 'tv2' })
        const transaction = await rig.producer.transaction()
        let aborted = null
        const acks = {
          valueOf: () => {
            aborted = transaction.abort()
            return -1
          },
        }

        const error = await transaction[method](request(acks)).then(
          () => null,
          (failure) => failure
        )

        expect(error).toBeInstanceOf(KafkaJSNonRetriableError)
        expect(error.message).toBe('Cannot continue to use transaction once ended')
        await expect(aborted).resolves.toBeUndefined()
        expect(acquire).not.toHaveBeenCalled()
        expect(debit).not.toHaveBeenCalled()
        expect(produceRequests()).toEqual([])
        expect(transaction.isActive()).toBe(false)
      }
    )

    test.each(SENDS)(
      '%s of a transaction still open converts its options once and is written',
      async (method, request) => {
        rig = await Rig.start({ transactionalId: 'converting' }, { profile: 'tv2' })
        const transaction = await rig.producer.transaction()
        let conversions = 0
        const acks = {
          valueOf: () => {
            conversions += 1
            return -1
          },
        }

        await expect(transaction[method](request(acks))).resolves.toHaveLength(1)
        await transaction.commit()

        expect(conversions).toBe(1)
        expect(produceRequests()).toHaveLength(1)
      }
    )

    test('a second argument to the public send reaches no refusal', async () => {
      rig = await Rig.start()
      const refusal = jest.fn(() => new Error('refused by the caller'))

      await expect(
        rig.producer.send({ topic: TOPIC, messages: MESSAGES }, refusal)
      ).resolves.toHaveLength(1)

      expect(refusal).not.toHaveBeenCalled()
    })
  })

  describe.each(['tv1', 'tv2'])(
    'a transaction that ends while an offset call reads its input (%s)',
    (profile) => {
      const GROUP = 'reading-group'
      const ROWS = [{ topic: TOPIC, partitions: [{ partition: 0, offset: '5' }] }]
      /** The batch a consumer handler is running, whose offsets tie the transaction to it. */
      const BATCH = {
        topic: TOPIC,
        partition: 0,
        assignmentEpoch: null,
        acquireClock: 1,
        ownershipKey: 'reading',
        groupId: GROUP,
        acquisition: { acquiredAt: 0, changedAt: 0, recreatedAt: 0, soughtAt: 0, retired: false },
      }
      /** Each place the caller's own code runs while the call reads its input. */
      const SITES = [
        [
          'a getter of the offset list',
          (read) => ({
            consumerGroupId: GROUP,
            get topics() {
              read()
              return ROWS
            },
          }),
        ],
        [
          'a getter of the group metadata',
          (read) => ({
            consumerGroupMetadata: {
              groupId: GROUP,
              get generationId() {
                read()
                return 3
              },
              memberId: 'member',
              groupInstanceId: null,
            },
            topics: ROWS,
          }),
        ],
        [
          'a getter of a topic row',
          (read) => ({
            consumerGroupId: GROUP,
            topics: [
              {
                topic: TOPIC,
                get partitions() {
                  read()
                  return ROWS[0].partitions
                },
              },
            ],
          }),
        ],
        [
          'a getter of a partition row',
          (read) => ({
            consumerGroupId: GROUP,
            topics: [
              {
                topic: TOPIC,
                partitions: [
                  {
                    partition: 0,
                    get offset() {
                      read()
                      return '5'
                    },
                  },
                ],
              },
            ],
          }),
        ],
      ]
      let begin, hold, enlist, groups

      /** The offset requests the cluster saw. */
      const offsetRequests = () =>
        rig.cluster
          .requests()
          .filter(({ api }) => api === 'AddOffsetsToTxn' || api === 'TxnOffsetCommit')
          .map(({ api }) => api)

      beforeEach(() => {
        begin = jest.spyOn(OffsetOperations.prototype, 'begin')
        hold = jest.spyOn(TransactionOffsets.prototype, 'hold')
        enlist = jest.spyOn(TransactionOffsets.prototype, 'enlist')
        groups = jest.spyOn(Cluster.prototype, 'offsetOperations')
      })

      test.each(SITES)(
        '%s that aborts is refused after it was read: no fence, no input tie, no offset request',
        async (_, make) => {
          rig = await Rig.start({ transactionalId: 'reading' }, { profile })
          const transaction = await rig.producer.transaction()
          let aborted = null
          const read = () => {
            aborted ??= transaction.abort()
          }

          const error = await handlerContext
            .run(BATCH, () => transaction.sendOffsets(make(read)))
            .then(
              () => null,
              (failure) => failure
            )

          expect(aborted).not.toBeNull()
          expect(error).toBeInstanceOf(KafkaJSNonRetriableError)
          expect(error.message).toBe('Cannot continue to use transaction once ended')
          await expect(aborted).resolves.toBeUndefined()
          expect(begin).not.toHaveBeenCalled()
          expect(hold).not.toHaveBeenCalled()
          expect(enlist).not.toHaveBeenCalled()
          expect(groups).not.toHaveBeenCalled()
          expect(offsetRequests()).toEqual([])
          expect(transaction.isActive()).toBe(false)
        }
      )

      test('a getter that closes the producer is refused with the close error: no fence, no offset request', async () => {
        rig = await Rig.start({ transactionalId: 'reading' }, { profile })
        const transaction = await rig.producer.transaction()
        let closed = null

        const error = await handlerContext
          .run(BATCH, () =>
            transaction.sendOffsets({
              consumerGroupId: GROUP,
              get topics() {
                closed ??= rig.producer.disconnect({ timeout: 0 })
                return ROWS
              },
            })
          )
          .then(
            () => null,
            (failure) => failure
          )
        expect(closed).not.toBeNull()
        await closed

        expect(error).toBeInstanceOf(KafkaJSProducerClosedError)
        expect(begin).not.toHaveBeenCalled()
        expect(hold).not.toHaveBeenCalled()
        expect(enlist).not.toHaveBeenCalled()
        expect(groups).not.toHaveBeenCalled()
        expect(offsetRequests()).toEqual([])
      })

      test('a call whose reads leave the transaction open takes its fence, ties the batch and sends its offsets', async () => {
        rig = await Rig.start({ transactionalId: 'reading' }, { profile })
        const transaction = await rig.producer.transaction()
        let reads = 0

        let conversions = 0
        const partition = {
          valueOf() {
            if (++conversions !== 1) throw new Error('partition read twice')
            return 0
          },
        }
        await handlerContext.run(BATCH, () =>
          transaction.sendOffsets({
            consumerGroupId: GROUP,
            get topics() {
              reads += 1
              return [{ topic: TOPIC, partitions: [{ partition, offset: '5' }] }]
            },
          })
        )
        expect(reads).toBe(1)
        expect(conversions).toBe(1)
        expect(begin).toHaveBeenCalledTimes(1)
        expect(begin).toHaveBeenCalledWith([{ topic: TOPIC, partition: 0 }], { mutation: true })
        const [ledger] = begin.mock.contexts
        expect(ledger.openTickets).toBe(1)
        await transaction.commit()
        expect(ledger.openTickets).toBe(0)
        expect(hold).toHaveBeenCalledTimes(1)
        expect(enlist).toHaveBeenCalledTimes(1)
        expect(offsetRequests()).toEqual(
          profile === 'tv1' ? ['AddOffsetsToTxn', 'TxnOffsetCommit'] : ['TxnOffsetCommit']
        )
      })

      test('a wire field that starts graceful close takes no offset ticket or input dependency', async () => {
        rig = await Rig.start({ transactionalId: 'reading' }, { profile })
        const transaction = await rig.producer.transaction()
        const delivery = rig.cluster.holdReply({ api: 'Produce' })
        const sending = transaction.send({ topic: TOPIC, messages: MESSAGES })
        expect(await delivery.executed()).not.toBeNull()
        let closed
        const leaderEpoch = {
          valueOf() {
            closed = rig.producer.disconnect()
            return 1
          },
        }

        await expect(
          handlerContext.run(BATCH, () =>
            transaction.sendOffsets({
              consumerGroupId: GROUP,
              topics: [{ topic: TOPIC, partitions: [{ partition: 0, offset: '5', leaderEpoch }] }],
            })
          )
        ).rejects.toThrow('The producer is disconnecting')
        expect(closed).toBeDefined()
        expect(groups).not.toHaveBeenCalled()
        expect(begin).not.toHaveBeenCalled()
        expect(hold).not.toHaveBeenCalled()
        expect(enlist).not.toHaveBeenCalled()
        expect(offsetRequests()).toEqual([])
        delivery.deliver()
        await sending
        await closed
      })

      test('a throwing offset value takes no ticket and a later offset call succeeds', async () => {
        rig = await Rig.start({ transactionalId: 'reading' }, { profile })
        const transaction = await rig.producer.transaction()
        const error = new Error('offset cannot be converted')
        const offset = {
          valueOf() {
            throw error
          },
        }
        await expect(
          handlerContext.run(BATCH, () =>
            transaction.sendOffsets({
              consumerGroupId: GROUP,
              topics: [{ topic: TOPIC, partitions: [{ partition: 0, offset }] }],
            })
          )
        ).rejects.toBe(error)
        expect(groups).not.toHaveBeenCalled()
        expect(begin).not.toHaveBeenCalled()
        expect(hold).not.toHaveBeenCalled()
        expect(enlist).not.toHaveBeenCalled()
        expect(offsetRequests()).toEqual([])
        await transaction.sendOffsets({ consumerGroupId: GROUP, topics: ROWS })
        await transaction.commit()
        expect(begin).toHaveBeenCalledTimes(1)
      })
    }
  )

  describe('a producer that a send closes while it converts its options', () => {
    test.each(SENDS)(
      '%s is refused after the conversion: nothing admitted or written, idempotence kept across a reconnect',
      async (method, request) => {
        rig = await Rig.start()
        let closed = null
        const acks = {
          valueOf: () => {
            closed ??= rig.producer.disconnect({ timeout: 0 })
            return 1
          },
        }

        const error = await rig.producer[method](request(acks)).then(
          () => null,
          (failure) => failure
        )
        await closed

        expect(closed).not.toBeNull()
        expect(error).toBeInstanceOf(KafkaJSNonRetriableError)
        expect(error.message).toBe(
          "The producer is disconnecting; therefore, it can't safely accept messages anymore"
        )
        expect(acquire).not.toHaveBeenCalled()
        expect(debit).not.toHaveBeenCalled()
        expect(rig.produced()).toEqual([])
        expect(rig.producer.isIdempotent()).toBe(true)

        await rig.producer.connect()
        await expect(rig.producer[method](request(-1))).resolves.toHaveLength(1)
        expect(rig.producer.isIdempotent()).toBe(true)
        const produced = rig.produced()
        expect(produced).toHaveLength(1)
        expect(produced[0].acks).toBe(-1)
      }
    )
  })

  describe('a send that is refused turns idempotence off for nobody', () => {
    test.each([
      [
        'a record without a value',
        { topic: TOPIC, messages: [{ key: 'k' }] },
        `Invalid message without value for topic "${TOPIC}" at index 0`,
      ],
      [
        'a null record list',
        { topic: TOPIC, messages: null },
        `Invalid messages array [null] for topic "${TOPIC}"`,
      ],
    ])('%s with acks 1 is refused and the producer stays idempotent', async (_, send, message) => {
      rig = await Rig.start()

      await expect(rig.producer.send({ ...send, acks: 1 })).rejects.toThrow(message)

      expect(rig.producer.isIdempotent()).toBe(true)
      expect(rig.produced()).toEqual([])
    })

    test('a send with acks 1 on a disconnected producer is refused and the producer stays idempotent', async () => {
      rig = await Rig.start()
      await rig.producer.disconnect()

      await expect(
        rig.producer.send({ topic: TOPIC, messages: MESSAGES, acks: 1 })
      ).rejects.toThrow('The producer is disconnected')

      expect(rig.producer.isIdempotent()).toBe(true)
    })

    test('a send with acks 1 that is accepted still turns idempotence off', async () => {
      rig = await Rig.start()

      await rig.producer.send({ topic: TOPIC, messages: MESSAGES, acks: 1 })

      expect(rig.producer.isIdempotent()).toBe(false)
    })
  })

  describe('producer input and delivery boundaries', () => {
    /** @returns {object} the request taking these records through the named public method */
    const requestWith = (method, messages) =>
      method === 'send'
        ? { topic: TOPIC, messages }
        : { topicMessages: [{ topic: TOPIC, messages }] }

    test.each(SENDS)(
      '%s validates values before flushing or disabling idempotence',
      async (method) => {
        rig = await Rig.start({ linger: 60000 })
        const older = rig.producer.send({ topic: TOPIC, messages: MESSAGES })
        for (let turn = 0; turn < 2000 && debit.mock.calls.length === 0; turn++) await tick()
        expect(debit).toHaveBeenCalledTimes(1)
        const flush = jest.spyOn(Accumulator.prototype, 'flush')
        acquire.mockClear()
        const messages = [{ value: {} }]
        const request = requestWith(method, messages)

        await expect(rig.producer[method]({ ...request, acks: 1 })).rejects.toThrow('its value')
        expect(flush).not.toHaveBeenCalled()
        expect(acquire).not.toHaveBeenCalled()
        expect(rig.produced()).toEqual([])
        expect(rig.producer.isIdempotent()).toBe(true)
        await rig.producer.flush()
        await expect(older).resolves.toHaveLength(1)
        expect(rig.produced()).toHaveLength(1)
      }
    )

    test.each(SENDS)(
      '%s captures a timestamp conversion before transaction eligibility',
      async (method) => {
        rig = await Rig.start({ transactionalId: 'reading' }, { profile: 'tv2' })
        const transaction = await rig.producer.transaction()
        let aborted
        const timestamp = {
          valueOf() {
            aborted = transaction.abort()
            return 1
          },
        }
        const messages = [{ value: VALUE, timestamp }]
        const request = requestWith(method, messages)

        await expect(transaction[method](request)).rejects.toThrow('transaction once ended')
        expect(aborted).toBeDefined()
        await aborted
        expect(acquire).not.toHaveBeenCalled()
        expect(debit).not.toHaveBeenCalled()
        expect(rig.produced()).toEqual([])
        const next = await rig.producer.transaction()
        await expect(next.send({ topic: TOPIC, messages: MESSAGES })).resolves.toHaveLength(1)
        await next.commit()
      }
    )

    test.each(SENDS)(
      '%s refuses a timestamp that force-closes before admission',
      async (method) => {
        rig = await Rig.start()
        let closed
        const timestamp = {
          valueOf() {
            closed = rig.producer.disconnect({ timeout: 0 })
            return 1
          },
        }
        const request = requestWith(method, [{ value: VALUE, timestamp }])
        await expect(rig.producer[method]({ ...request, acks: 1 })).rejects.toThrow(
          'The producer is disconnecting'
        )
        expect(closed).toBeDefined()
        await closed
        expect(acquire).not.toHaveBeenCalled()
        expect(debit).not.toHaveBeenCalled()
        expect(rig.produced()).toEqual([])
        expect(rig.producer.isIdempotent()).toBe(true)
        await rig.producer.connect()
        await expect(rig.producer[method](requestWith(method, MESSAGES))).resolves.toHaveLength(1)
      }
    )

    const RECORD_FIELDS = {
      key: 'k',
      value: VALUE,
      headers: { name: 'v' },
      partition: 0,
      timestamp: 1000,
    }
    const GETTER_CASES = SENDS.flatMap(([method]) =>
      Object.keys(RECORD_FIELDS).flatMap((field) =>
        ['throw', 'abort', 'force-close', 'graceful close'].map((action) => [method, field, action])
      )
    )

    test.each(GETTER_CASES)(
      '%s refuses a %s getter that initiates %s',
      async (method, field, action) => {
        const transactional = action === 'abort'
        rig = await Rig.start(transactional ? { transactionalId: 'reading' } : {}, {
          profile: 'tv2',
        })
        const transaction = transactional ? await rig.producer.transaction() : null
        const owner = transaction ?? rig.producer
        const error = new Error('getter failed')
        let ended
        let reads = 0
        const message = Object.defineProperty({ value: VALUE }, field, {
          enumerable: true,
          get() {
            reads += 1
            if (action === 'throw') throw error
            ended = transactional
              ? transaction.abort()
              : rig.producer.disconnect(action === 'force-close' ? { timeout: 0 } : undefined)
            return RECORD_FIELDS[field]
          },
        })
        const sent = owner[method](requestWith(method, [message]))
        const reason = transactional ? 'transaction once ended' : 'The producer is disconnecting'
        if (action === 'throw') {
          await expect(sent).rejects.toBe(error)
        } else {
          await expect(sent).rejects.toThrow(reason)
          expect(ended).toBeDefined()
          await ended
        }
        expect(reads).toBe(1)
        expect(acquire).not.toHaveBeenCalled()
        expect(debit).not.toHaveBeenCalled()
        expect(rig.produced()).toEqual([])
        if (action === 'force-close' || action === 'graceful close') await rig.producer.connect()
        await expect(rig.producer[method](requestWith(method, MESSAGES))).resolves.toHaveLength(1)
      }
    )

    /** Waits for a transport turn without advancing fake time. */
    const waitFor = async (predicate) => {
      for (let turn = 0; turn < 2000 && !predicate(); turn += 1) await tick()
      expect(predicate()).toBe(true)
    }

    /** Asserts that the admission owner has retired every hold and debit. */
    const expectReleased = async () => {
      const [admission] = acquire.mock.contexts
      expect(admission).toBeDefined()
      await waitFor(
        () => admission.reserved === 0 && admission.waitingBytes === 0 && admission.debts === 0
      )
    }

    /** Counts real Produce writes for both named and topic-ID request versions. */
    const produceRequests = () => rig.cluster.requests().filter(({ api }) => api === 'Produce')

    /** Parks one custom compressor until the test releases it. */
    const holdCodec = () => {
      let release
      let finish
      const pending = new Promise((resolve) => {
        release = resolve
      })
      const finished = new Promise((resolve) => {
        finish = resolve
      })
      const compress = jest.fn(async (encoder) => {
        await pending
        try {
          return await gzip.compress(encoder)
        } finally {
          finish()
        }
      })
      CompressionCodecs[CompressionTypes.GZIP] = () => ({ ...gzip, compress })
      return { compress, release, finished }
    }

    const compressed = compression(CompressionTypes.GZIP)()
    const externalActions = ['abort', 'force-close', 'commit', 'graceful close']

    /** Starts the operation as an independent caller while one of its sends waits. */
    const endFromOutside = (transaction, action) => {
      if (action === 'abort') return transaction.abort()
      if (action === 'commit') return transaction.commit()
      return rig.producer.disconnect(action === 'force-close' ? { timeout: 0 } : undefined)
    }

    /** @returns {boolean} whether the end drops a send queued or on a request not yet written */
    const dropsUnwritten = (action) => action === 'abort' || action === 'force-close'

    /**
     * @returns {boolean} whether the end also drops a send still being admitted or routed: an
     *   abort delivers it, as it always has, before it ends the transaction
     */
    const dropsUnqueued = (action) => action === 'force-close'

    /** @returns {boolean} whether the end closes the producer and its connections with it */
    const closes = (action) => action.endsWith('close')

    test.each([
      ['explicit plain', { idempotent: false }, -1, false],
      ['downgraded', {}, 1, false],
      ['idempotent', {}, -1, true],
    ])(
      'writes valid uncompressed and custom-compressed Produce on the %s path',
      async (_, options, acks, idempotent) => {
        rig = await Rig.start(options)
        const compress = jest.fn((encoder) => gzip.compress(encoder))
        CompressionCodecs[CompressionTypes.GZIP] = () => ({ ...gzip, compress })
        await expect(
          rig.producer.send({ topic: TOPIC, messages: MESSAGES, acks })
        ).resolves.toHaveLength(1)
        expect(rig.producer.isIdempotent()).toBe(idempotent)
        await expect(
          rig.producer.send({ topic: TOPIC, messages: MESSAGES, ...compressed })
        ).resolves.toHaveLength(1)

        const batches = rig.produced().flatMap(({ partitions }) => partitions)
        expect(batches.map(({ codec, count }) => [codec, count])).toEqual([
          [0, 1],
          [1, 1],
        ])
        for (const { codec, body } of batches) expect(await carriesValue(body, codec)).toBe(true)
        expect(compress).toHaveBeenCalledTimes(1)
        await expectReleased()
      }
    )

    test.each([
      ['send', 'callable'],
      ['send', 'class instance'],
      ['sendBatch', 'callable'],
      ['sendBatch', 'Proxy'],
    ])('%s captures each field of a %s record before admission', async (method, shape) => {
      rig = await Rig.start({ transactionalId: 'record-shapes' }, { profile: 'tv2' })
      const transaction = await rig.producer.transaction()
      let reads = 0
      let entryReads = 0
      let nestedAbort
      const value = () => {
        if (++reads > 1) nestedAbort = transaction.abort()
        return VALUE
      }
      class Record {
        get value() {
          return value()
        }
      }
      const message =
        shape === 'callable'
          ? Object.defineProperty(() => {}, 'value', { get: value })
          : shape === 'Proxy'
            ? new Proxy(new Record(), {})
            : new Record()
      const entry = Object.defineProperties(() => {}, {
        topic: { get: () => TOPIC },
        messages: {
          get() {
            ++entryReads
            return [message]
          },
        },
      })
      const request =
        method === 'send' ? requestWith(method, [message]) : { topicMessages: [entry] }

      await expect(transaction[method](request)).resolves.toHaveLength(1)
      expect(reads).toBe(1)
      expect(entryReads).toBe(method === 'send' ? 0 : 1)
      expect(nestedAbort).toBeUndefined()
      await transaction.commit()
      expect(produceRequests()).toHaveLength(1)
      await expectReleased()
    })

    test('keeps the partitioner view private and encodes the charged records in order', async () => {
      const views = []
      const wrap = jest.spyOn(DeliveryCallbacks.prototype, 'partitioner')
      rig = await Rig.start({
        createPartitioner:
          () =>
          ({ message }) => {
            views.push(message)
            Reflect.set(message, 'value', 'changed')
            Reflect.set(message.headers, 'marker', 'changed')
            Reflect.set(message.headers.marker, 0, 'changed')
            return 0
          },
      })
      const messages = [
        { value: 'first-record', headers: { marker: ['first-header'] }, timestamp: 1000 },
        { value: 'second-record', headers: { marker: ['second-header'] }, timestamp: 1000 },
      ]
      await expect(rig.producer.send({ topic: TOPIC, messages })).resolves.toHaveLength(1)
      expect(views).toHaveLength(2)
      expect(views.every((view) => Object.isFrozen(view) && Object.isFrozen(view.headers))).toBe(
        true
      )
      expect(Reflect.set(views[0], 'value', 'late change')).toBe(false)
      expect(views[1].value).toBe('second-record')
      const [{ body, count }] = rig.produced().flatMap(({ partitions }) => partitions)
      expect(count).toBe(2)
      const text = body.toString()
      expect(text.indexOf('first-record')).toBeGreaterThanOrEqual(0)
      expect(text.indexOf('second-record')).toBeGreaterThan(text.indexOf('first-record'))
      expect(text).toContain('first-header')
      expect(text).toContain('second-header')
      expect(text).not.toContain('changed')
      expect(debit).toHaveBeenCalledWith(BATCH_DEBIT)
      expect(acquire.mock.calls[0][0]).toBe(
        chargeOf([{ topic: TOPIC, messages }], acquire.mock.contexts[0].capacity).total
      )
      await expectReleased()
      await rig.close()

      wrap.mockClear()
      rig = await Rig.start()
      await expect(rig.producer.send({ topic: TOPIC, messages })).resolves.toHaveLength(1)
      expect(wrap).not.toHaveBeenCalled()
      await expectReleased()
    })

    test.each(['partitioner throw', 'partitioner rejection', 'codec throw', 'codec rejection'])(
      'isolates a %s between successful sends to the same partition',
      async (failure) => {
        const error = new KafkaJSNonRetriableError('extension failed')
        const options = { idempotent: false, maxInFlightRequests: 1, batchSize: 1 }
        const partitionerFailure = failure.startsWith('partitioner')
        if (partitionerFailure) {
          options.createPartitioner =
            () =>
            ({ message }) => {
              if (message.value === 'bad') {
                if (failure === 'partitioner throw') throw error
                return Promise.reject(error)
              }
              return 0
            }
        } else {
          CompressionCodecs[CompressionTypes.GZIP] = () => ({
            ...gzip,
            compress() {
              if (failure === 'codec throw') throw error
              return Promise.reject(error)
            },
          })
        }
        rig = await Rig.start(options)
        const send = (value, extra = {}) =>
          rig.producer.send({ topic: TOPIC, messages: [{ value, partition: 0 }], ...extra })
        const first = send('first')
        const failed = send('bad', partitionerFailure ? {} : compressed)
        const last = send('last')
        await expect(failed).rejects.toThrow(
          failure === 'partitioner rejection' ? 'Invalid partition' : 'extension failed'
        )
        await expect(Promise.all([first, last])).resolves.toHaveLength(2)
        const batches = rig.produced().flatMap(({ partitions }) => partitions)
        expect(batches.map(({ count }) => count)).toEqual([1, 1])
        expect(batches[0].body.toString()).toContain('first')
        expect(batches[1].body.toString()).toContain('last')
        await expectReleased()
      }
    )

    test.each(PATHS)(
      'on %s expires a held codec, releases its debit and delivers again before it resolves',
      async (_, options) => {
        jest.useFakeTimers({ doNotFake: ['nextTick', 'setImmediate', 'queueMicrotask'] })
        const held = holdCodec()
        // The least deliveryTimeout accepted: linger 0 plus the 5000 ms requestTimeout.
        rig = await Rig.start({ ...options, deliveryTimeout: 5000, linger: 0 })
        const connected = jest.getTimerCount()
        const send = (extra) =>
          rig.producer.send({ topic: TOPIC, messages: [{ value: VALUE, partition: 0 }], ...extra })
        const expired = send(compressed).catch((error) => error)
        try {
          await waitFor(() => held.compress.mock.calls.length === 1)
          expect(acquire.mock.contexts[0].debts).toBeGreaterThan(0)
          await jest.advanceTimersByTimeAsync(5000)
          expect(await expired).toMatchObject({ name: 'KafkaJSDeliveryTimeout', outcome: 'unsent' })
          expect(rig.produced()).toEqual([])
          await expectReleased()
          await expect(send()).resolves.toHaveLength(1)
          expect(rig.produced()).toHaveLength(1)
          held.release()
          await held.finished
          await tick()
          expect(rig.produced()).toHaveLength(1)
          expect(jest.getTimerCount()).toBe(connected)
        } finally {
          held.release()
        }
      }
    )

    test.each(['partitioner', 'codec factory', 'compressor'])(
      'refuses nested producer and transaction calls from a %s',
      async (extension) => {
        jest.useFakeTimers({ doNotFake: ['nextTick', 'setImmediate', 'queueMicrotask'] })
        let results
        let entered = false
        let continueCodec
        const continuation = new Promise((resolve) => {
          continueCodec = resolve
        })
        const createPartitioner =
          extension === 'partitioner'
            ? () =>
                ({ message }) => {
                  if (message.value === VALUE) results = nested()
                  return 0
                }
            : undefined
        if (extension !== 'partitioner') {
          CompressionCodecs[CompressionTypes.GZIP] = () => {
            if (extension === 'codec factory') results = nested()
            return {
              ...gzip,
              async compress(encoder) {
                if (extension === 'compressor') {
                  entered = true
                  await continuation
                  results = nested()
                  await results
                }
                return gzip.compress(encoder)
              },
            }
          }
        }
        rig = await Rig.start(
          {
            transactionalId: 'callback',
            createPartitioner,
            deliveryTimeout: 10000,
            linger: 0,
          },
          { profile: 'tv2' }
        )
        const { producer } = rig
        const transaction = await producer.transaction()
        const request = { topic: TOPIC, messages: MESSAGES }
        const nested = () =>
          Promise.allSettled([
            producer.send(request),
            producer.sendBatch({ topicMessages: [request] }),
            producer.flush(),
            producer.disconnect({ timeout: 0 }),
            producer.connect(),
            producer.transaction(),
            transaction.send(request),
            transaction.sendBatch({ topicMessages: [request] }),
            transaction.sendOffsets({}),
            transaction.commit(),
            transaction.abort(),
          ])
        const sent = transaction.send(
          extension === 'partitioner' ? request : { ...request, ...compressed }
        )
        const committed = transaction.commit()
        if (extension === 'compressor') {
          await waitFor(() => entered)
          expect(await outcome(committed, 5)).toEqual({ status: 'pending' })
        }
        const own = producer.send({ topic: TOPIC, messages: [{ value: 'independent' }] })
        continueCodec()

        const deliveries = await outcome(Promise.all([sent, committed, own]))
        expect(deliveries.status).toBe('fulfilled')
        const refusals = await results
        expect(refusals).toHaveLength(11)
        for (const result of refusals) {
          expect(result.status).toBe('rejected')
          expect(result.reason).toBeInstanceOf(KafkaJSNonRetriableError)
          expect(result.reason.message).toMatch(/cannot call its own producer or transaction/)
        }
        expect(produceRequests()).toHaveLength(2)
        expect(acquire).toHaveBeenCalledTimes(2)
        await expectReleased()
      }
    )

    test('an ended abort leaves the next transaction to flush its own batch', async () => {
      jest.useFakeTimers({ doNotFake: ['nextTick', 'setImmediate', 'queueMicrotask'] })
      rig = await Rig.start({ transactionalId: 'old-abort', linger: 60000 }, { profile: 'tv2' })
      const first = await rig.producer.transaction()
      await first.abort()
      const next = await rig.producer.transaction()
      const sent = next.send({ topic: TOPIC, messages: MESSAGES })
      await waitFor(() => debit.mock.calls.length === 1)
      const timerCount = jest.getTimerCount()
      expect(timerCount).toBeGreaterThan(0)
      const flush = jest.spyOn(Accumulator.prototype, 'flush')

      await expect(first.abort()).rejects.toThrow('transaction once ended')
      expect(flush).not.toHaveBeenCalled()
      expect(jest.getTimerCount()).toBe(timerCount)
      expect(produceRequests()).toEqual([])
      await next.commit()
      await expect(sent).resolves.toHaveLength(1)
      expect(produceRequests()).toHaveLength(1)
      await expectReleased()
    })

    test('freezes public built-in codecs while keeping replacement registration', async () => {
      for (const type of [1, 2, 3, 4].filter(runs)) {
        const codec = CompressionCodecs[type]()
        const original = codec.compress
        expect(Object.isFrozen(codec)).toBe(true)
        expect(Reflect.set(codec, 'compress', jest.fn())).toBe(false)
        expect(codec.compress).toBe(original)
      }
      const compress = jest.fn((encoder) => gzip.compress(encoder))
      CompressionCodecs[CompressionTypes.GZIP] = () => ({ ...gzip, compress })
      rig = await Rig.start()
      await expect(
        rig.producer.send({ topic: TOPIC, messages: MESSAGES, ...compressed })
      ).resolves.toHaveLength(1)
      expect(compress).toHaveBeenCalledTimes(1)
      await expectReleased()
    })

    test.each(externalActions)(
      'an independent %s owns the outcome of a pending codec',
      async (action) => {
        jest.useFakeTimers({ doNotFake: ['nextTick', 'setImmediate', 'queueMicrotask'] })
        const held = holdCodec()
        rig = await Rig.start({ transactionalId: 'external-end', linger: 0 }, { profile: 'tv2' })
        // The default EndTxn vector throttles the next transaction on the frozen clock.
        rig.cluster.answer('EndTxn', () => ({ throttleTime: 0 }))
        const { producer } = rig
        const transaction = await producer.transaction()
        const connected = jest.getTimerCount()
        const sent = transaction.send({ topic: TOPIC, messages: MESSAGES, ...compressed })
        const delivery = sent.catch((error) => error)
        try {
          await waitFor(() => held.compress.mock.calls.length === 1)
          const ending = endFromOutside(transaction, action)

          if (dropsUnwritten(action)) {
            // The end settles while the codec is still held: nothing waits for its promise.
            expect(await outcome(ending)).toEqual({ status: 'fulfilled', value: undefined })
            expect(held.compress.mock.calls).toHaveLength(1)
            const error = await delivery
            expect(error).toBeInstanceOf(Error)
            if (action === 'abort') {
              expect(error).toMatchObject({
                name: 'KafkaJSTransactionAbortableError',
                message: 'The transaction was aborted while this batch was waiting to be sent',
              })
              expect(transaction.isActive()).toBe(false)
            }
            await expectReleased()
            expect(produceRequests()).toEqual([])
            // A late resolution of the codec writes nothing and holds no debit.
            held.release()
            await held.finished
            await tick()
            expect(produceRequests()).toEqual([])
            await expectReleased()
            if (action === 'abort') {
              const next = await producer.transaction()
              await expect(next.send({ topic: TOPIC, messages: MESSAGES })).resolves.toHaveLength(1)
              await next.commit()
              expect(produceRequests()).toHaveLength(1)
              await expectReleased()
            }
          } else {
            expect(await outcome(ending, 5)).toEqual({ status: 'pending' })
            expect(produceRequests()).toEqual([])
            held.release()
            await ending
            expect(await delivery).toHaveLength(1)
            expect(produceRequests()).toHaveLength(1)
            await expectReleased()
          }
          expect(jest.getTimerCount()).toBe(closes(action) ? 0 : connected)
        } finally {
          held.release()
        }
      }
    )

    test.each(externalActions)(
      'an independent %s drains or cancels a transaction waiting for admission',
      async (action) => {
        jest.useFakeTimers({ doNotFake: ['nextTick', 'setImmediate', 'queueMicrotask'] })
        rig = await Rig.start(
          { transactionalId: 'admission-end', linger: 60000, bufferMemory: 4096 },
          { profile: 'tv2' }
        )
        const transaction = await rig.producer.transaction()
        const connected = jest.getTimerCount()
        const send = (marker) =>
          transaction
            .send({ topic: TOPIC, messages: [{ value: marker.repeat(1500), partition: 0 }] })
            .catch((error) => error)
        const first = send('x')
        await waitFor(() => debit.mock.calls.length === 1)
        const [admission] = acquire.mock.contexts
        const second = send('y')
        expect(admission.waitingBytes).toBeGreaterThan(0)
        expect(produceRequests()).toEqual([])

        await endFromOutside(transaction, action)
        // The first send lingers in the accumulator; the second is still waiting for admission.
        const delivered = []
        if (dropsUnwritten(action)) expect(await first).toBeInstanceOf(Error)
        else delivered.push(['x', await first])
        if (dropsUnqueued(action)) expect(await second).toBeInstanceOf(Error)
        else delivered.push(['y', await second])
        const records = rig.produced().flatMap(({ partitions }) => partitions)
        expect(records.map(({ count }) => count)).toEqual(delivered.map(() => 1))
        delivered.forEach(([marker, metadata], index) => {
          expect(metadata).toHaveLength(1)
          expect(records[index].body.includes(Buffer.from(marker.repeat(1500)))).toBe(true)
        })
        await expectReleased()
        expect(jest.getTimerCount()).toBe(closes(action) ? 0 : connected)
      }
    )

    test.each(externalActions)(
      'an independent %s drains or cancels a transaction waiting to route',
      async (action) => {
        jest.useFakeTimers({ doNotFake: ['nextTick', 'setImmediate', 'queueMicrotask'] })
        rig = await Rig.start({ transactionalId: 'routing-end', linger: 0 }, { profile: 'tv2' })
        const transaction = await rig.producer.transaction()
        const connected = jest.getTimerCount()
        let release
        let entered = false
        const route = new Promise((resolve) => {
          release = resolve
        })
        const refresh = Cluster.prototype.refreshMetadataIfNecessary
        jest
          .spyOn(Cluster.prototype, 'refreshMetadataIfNecessary')
          .mockImplementationOnce(async function (...args) {
            entered = true
            await route
            return refresh.apply(this, args)
          })
        const delivery = transaction
          .send({ topic: TOPIC, messages: MESSAGES })
          .catch((error) => error)
        try {
          await waitFor(() => entered)
          expect(acquire.mock.contexts[0].reserved).toBeGreaterThan(0)
          expect(produceRequests()).toEqual([])
          const ending = endFromOutside(transaction, action)
          release()
          await ending
          if (dropsUnqueued(action)) {
            expect(await delivery).toBeInstanceOf(Error)
            expect(produceRequests()).toEqual([])
          } else {
            expect(await delivery).toHaveLength(1)
            expect(produceRequests()).toHaveLength(1)
          }
          await expectReleased()
          expect(jest.getTimerCount()).toBe(closes(action) ? 0 : connected)
        } finally {
          release()
        }
      }
    )
  })
})
