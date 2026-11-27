/* eslint-disable no-new-wrappers -- boxed primitives are inputs under test */
const { Kafka, CompressionTypes, CompressionCodecs } = require('../../../index')
const {
  KafkaJSDeliveryTimeout,
  KafkaJSInvalidConfigurationError,
  KafkaJSNotImplemented,
} = require('../../errors')
const apiKeys = require('../../protocol/requests/apiKeys')
const Decoder = require('../../protocol/decoder')
const gzip = require('../../protocol/message/compression/gzip')
const snappy = require('../../protocol/message/compression/snappy')
const lz4 = require('../../protocol/message/compression/lz4')
const zstd = require('../../protocol/message/compression/zstd')
const { Admission } = require('../state/admission')
const { RequestBudget } = require('../state/recordSize')
const { readRequestHeader } = require('../../../testHelpers/fakeSocket')
const { createScriptedCluster, producerScript } = require('../../../testHelpers/scriptedCluster')
const { TOPIC } = require('../../consumer/__tests__/fixtures/fetchHarness')

const VALUE = 'send-option-row'
const MESSAGES = [{ value: VALUE, timestamp: 1000 }]
const ORIGINAL_CODECS = { ...CompressionCodecs }
/** The real admission, taken before any spy replaces it, for spies that log and call through. */
const ADMIT = Admission.prototype.acquire
const CODECS = { 1: gzip, 2: snappy, 3: lz4, 4: zstd }
/** Short, fixed backoffs: a retried batch waits 10 ms. */
const RETRY = { retries: 10, initialRetryTime: 10, maxRetryTime: 10, factor: 0 }
const PATHS = [
  ['the idempotent path', {}],
  ['the plain path', { idempotent: false }],
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
        topic.readUuid()
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
   * @param {{ silent?: boolean, partitions?: number }} [setup] `silent`: the broker never answers
   *   a Produce request, as a broker does not answer one with acks 0
   * @returns {Promise<Rig>} connected
   */
  static async start(options = {}, { silent = false, partitions = 2 } = {}) {
    const cluster = producerScript(
      createScriptedCluster({ nodes: [{ id: 0, host: 'node0', port: 9092 }] }),
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
      jest.restoreAllMocks()
      for (const key of Object.keys(CompressionCodecs)) {
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
  })
})
