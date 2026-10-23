const zlib = require('zlib')
const { Kafka, CompressionTypes } = require('../../../../index')
const { KafkaJSInvalidConfigurationError } = require('../../../errors')
const { createScriptedCluster, producerScript } = require('../../../../testHelpers/scriptedCluster')
const { Codecs, Types } = require('./index')
const gzip = require('./gzip')
const zstd = require('./zstd')
const produce = require('../../requests/produce')
const buildRecordBatch = require('../../requests/produce/buildRecordBatch')
const { RecordBatch } = require('../../recordBatch/v0')
const { TOPIC, TOPIC_ID } = require('../../../consumer/__tests__/fixtures/fetchHarness')

const kafka = () => new Kafka({ brokers: ['localhost:9092'], logLevel: 0 })
const messages = [{ value: 'abc'.repeat(10000), timestamp: 1000 }]
const topicData = [{ topic: TOPIC, partitions: [{ partition: 0, messages }] }]
// eslint-disable-next-line n/no-unsupported-features/node-builtins -- feature detection, not a call
const testWithZstd = typeof zlib.zstdCompress === 'function' ? test : test.skip

describe('Producer compression levels', () => {
  test.each([-1, 1, 9])('accepts gzip level %i', (gzip) => {
    expect(() => kafka().producer({ compressionLevels: { gzip } })).not.toThrow()
  })
  test.each([-131072, 0, 22])('accepts zstd level %i', (zstd) => {
    expect(() => kafka().producer({ compressionLevels: { zstd } })).not.toThrow()
  })
  test.each([
    { gzip: -2 },
    { gzip: 0 },
    { gzip: 10 },
    { gzip: 1.5 },
    { gzip: '1' },
    { gzip: NaN },
    { gzip: Symbol('x') },
    { zstd: -131073 },
    { zstd: 23 },
    { zstd: 1.5 },
    { zstd: '3' },
    { lz4: 1 },
    { snappy: 1 },
    { other: 1 },
    null,
    [],
    1,
  ])('refuses invalid compressionLevels %p', (compressionLevels) => {
    expect(() => kafka().producer({ compressionLevels })).toThrow(KafkaJSInvalidConfigurationError)
    expect(() => kafka().producer({ compressionLevels })).toThrow(/compressionLevels/)
  })
  test('explains why the LZ4 binding cannot accept a level', () => {
    expect(() => kafka().producer({ compressionLevels: { lz4: 1 } })).toThrow(/lz4-napi.*level/)
  })

  test.each(Array.from({ length: 11 }, (_, i) => i + 3))(
    'passes a level through the Produce v%i registry and request builder to the codec',
    async (version) => {
      const compress = jest.spyOn(gzip, 'compress')
      try {
        const request = produce.protocol({ version })({
          acks: -1,
          timeout: 1000,
          compression: Types.GZIP,
          compressionLevel: 9,
          topicData,
          topicIds: new Map([[TOPIC, TOPIC_ID]]),
        }).request
        await request.encode()
        expect(compress).toHaveBeenCalledTimes(1)
        expect(compress).toHaveBeenCalledWith(
          expect.objectContaining({ buffer: expect.any(Buffer) }),
          { level: 9 }
        )
      } finally {
        compress.mockRestore()
      }
    }
  )

  test('passes the option through direct batch helpers', async () => {
    const compress = jest.spyOn(gzip, 'compress')
    try {
      await buildRecordBatch({ compression: Types.GZIP, compressionLevel: 1, messages })
      await RecordBatch({ compression: Types.GZIP, compressionLevel: 9, records: [] })
      expect(compress.mock.calls.map(([, options]) => options)).toEqual([
        { level: 1 },
        { level: 9 },
      ])
    } finally {
      compress.mockRestore()
    }
  })

  test('gzip level one produces a larger batch than level nine', async () => {
    const low = await buildRecordBatch({ compression: Types.GZIP, compressionLevel: 1, messages })
    const high = await buildRecordBatch({ compression: Types.GZIP, compressionLevel: 9, messages })
    expect(low.buffer.length).toBeGreaterThan(high.buffer.length)
  })

  test('omitted gzip levels retain the zlib default and remain decodable', async () => {
    const source = Buffer.from('default gzip')
    expect(await gzip.decompress(await gzip.compress({ buffer: source }))).toEqual(source)
  })

  testWithZstd('passes the zstd level to zlib', async () => {
    const compress = jest.spyOn(zlib, 'zstdCompress')
    try {
      let zstd
      jest.isolateModules(() => {
        zstd = require('./zstd')
      })
      await zstd.compress({ buffer: Buffer.from('level') }, { level: 7 })
      expect(compress).toHaveBeenCalledWith(
        expect.any(Buffer),
        {
          // eslint-disable-next-line n/no-unsupported-features/node-builtins -- exercised on supported zstd builds
          params: { [zlib.constants.ZSTD_c_compressionLevel]: 7 },
        },
        expect.any(Function)
      )
    } finally {
      compress.mockRestore()
    }
  })

  test.each([
    ['idempotent', {}, 'default'],
    ['plain', { idempotent: false }, 'default'],
    ['downgraded', {}, 'default'],
    ['transaction version one', { transactionalId: 'compression-test' }, 'tv1'],
    ['transaction version two', { transactionalId: 'compression-test' }, 'tv2'],
  ])('passes configured levels through the public %s producer', async (mode, options, profile) => {
    const cluster = producerScript(
      createScriptedCluster({ nodes: [{ id: 0, host: 'node0', port: 9092 }], profile }),
      { topic: TOPIC, partitions: 1 }
    )
    const compress = jest.spyOn(gzip, 'compress')
    const zstdCompress = jest.spyOn(zstd, 'compress')
    const producer = new Kafka({ ...cluster.kafkaOptions(), logLevel: 0 }).producer({
      ...options,
      linger: 0,
      compressionLevels: { gzip: 1, zstd: 7 },
    })
    try {
      await producer.connect()
      const sender = options.transactionalId ? await producer.transaction() : producer
      await sender.send({
        topic: TOPIC,
        messages,
        compression: CompressionTypes.GZIP,
        ...(mode === 'downgraded' ? { acks: 1 } : {}),
      })
      expect(compress).toHaveBeenCalledWith(expect.anything(), { level: 1 })
      if (zstd.isSupported) {
        await sender.send({
          topic: TOPIC,
          messages,
          compression: CompressionTypes.ZSTD,
          ...(mode === 'downgraded' ? { acks: 1 } : {}),
        })
        expect(zstdCompress).toHaveBeenCalledWith(expect.anything(), { level: 7 })
      }
      if (options.transactionalId) await sender.abort()
    } finally {
      compress.mockRestore()
      zstdCompress.mockRestore()
      await producer.disconnect()
      cluster.assertClean()
    }
  })

  test('a custom codec still receives the optional level', async () => {
    const saved = Codecs[Types.GZIP]
    const compress = jest.fn().mockResolvedValue(Buffer.from('plugin'))
    try {
      Codecs[Types.GZIP] = () => ({ compress })
      await buildRecordBatch({ compression: Types.GZIP, compressionLevel: 9, messages })
      expect(compress).toHaveBeenCalledWith(expect.anything(), { level: 9 })
    } finally {
      Codecs[Types.GZIP] = saved
    }
  })
})
