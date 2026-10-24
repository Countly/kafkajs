const fs = require('fs')
const os = require('os')
const path = require('path')
const { batch, crc32c } = require('../../requests/fetch/__tests__/fixtures/recordBatchBytes')
const { KafkaJSNotImplemented } = require('../../../errors')
const {
  CAPTURE_FILES,
  assertBaselineArtifact,
  assertBaselineDiagnostic,
  assertFinalArtifact,
  diagnosticOf,
  expectedRecords,
  hexOf,
  loadCapture,
  publicRecord,
  restamp,
  sha256,
  snappySlot,
} = require('./fixtures/librdkafkaCapture')

describe('Protocol > the librdkafka capture helpers', () => {
  let directory

  beforeEach(() => {
    directory = fs.mkdtempSync(path.join(os.tmpdir(), 'librdkafka-capture-'))
  })

  afterEach(() => {
    fs.rmSync(directory, { recursive: true, force: true })
  })

  describe('loadCapture', () => {
    test('names every file that is missing and never fabricates one', () => {
      expect(() => loadCapture(directory)).toThrow(
        /authentic librdkafka Snappy capture is missing \(snappy-record-batch\.bin, snappy-record-batch\.json, snappy-record-data\.bin\)/
      )
      fs.writeFileSync(path.join(directory, CAPTURE_FILES.manifest), '{}')
      expect(() => loadCapture(directory)).toThrow(
        /\(snappy-record-batch\.bin, snappy-record-data\.bin\)/
      )
    })

    test('reads the batch, the record section and the parsed manifest as they are', () => {
      fs.writeFileSync(path.join(directory, CAPTURE_FILES.batch), Buffer.from([1, 2, 3]))
      fs.writeFileSync(path.join(directory, CAPTURE_FILES.data), Buffer.from([4, 5]))
      fs.writeFileSync(path.join(directory, CAPTURE_FILES.manifest), '{"sha256":"abc"}')

      const capture = loadCapture(directory)

      expect(capture.batch).toEqual(Buffer.from([1, 2, 3]))
      expect(capture.data).toEqual(Buffer.from([4, 5]))
      expect(capture.manifest).toEqual({ sha256: 'abc' })
    })
  })

  describe('restamp', () => {
    const body = Buffer.from('opaque compressed bytes')
    const stamped = () =>
      batch({
        attributes: 2,
        baseOffset: 9n,
        partitionLeaderEpoch: 4,
        firstTimestamp: 1700000000000n,
        maxTimestamp: 1700000000007n,
        producerId: 77n,
        producerEpoch: 3,
        baseSequence: 5,
        lastOffsetDelta: 6,
        count: 7,
        records: [body],
      })

    test('moves the offset, the leader epoch and the producer stamp, and fixes the CRC', () => {
      const original = stamped()

      const { bytes, delta } = restamp(original)

      expect(delta).toEqual(['baseOffset', 'partitionLeaderEpoch', 'producerStamp', 'crc'])
      expect(bytes.readBigInt64BE(0)).toBe(0n)
      expect(bytes.readInt32BE(12)).toBe(-1)
      expect([bytes.readBigInt64BE(43), bytes.readInt16BE(51), bytes.readInt32BE(53)]).toEqual([
        -1n,
        -1,
        -1,
      ])
      expect(bytes.readUInt32BE(17)).toBe(crc32c(bytes.subarray(21)))
      expect(bytes.subarray(21, 43)).toEqual(original.subarray(21, 43))
      expect(bytes.subarray(61)).toEqual(original.subarray(61))
      expect(original.readBigInt64BE(0)).toBe(9n)
    })

    test('leaves a batch that is already non-idempotent at offset 0 as it is', () => {
      const original = batch({ attributes: 2, partitionLeaderEpoch: -1, records: [body] })

      const { bytes, delta } = restamp(original)

      expect(delta).toEqual([])
      expect(bytes).toEqual(original)
      expect(bytes).not.toBe(original)
    })

    test('recomputes no CRC when only the offset and the leader epoch move', () => {
      const original = batch({
        attributes: 2,
        baseOffset: 5n,
        partitionLeaderEpoch: 2,
        records: [body],
      })

      const { bytes, delta } = restamp(original)

      expect(delta).toEqual(['baseOffset', 'partitionLeaderEpoch'])
      expect(bytes.readUInt32BE(17)).toBe(original.readUInt32BE(17))
      expect(bytes.readUInt32BE(17)).toBe(crc32c(bytes.subarray(21)))
    })
  })

  describe('publicRecord', () => {
    test('shows bytes as hex, null as null and empty as an empty string', () => {
      const projected = publicRecord({
        offset: '3',
        timestamp: '1700000000000',
        key: null,
        value: Buffer.alloc(0),
        headers: {},
      })

      expect(projected).toEqual({
        offset: '3',
        timestamp: '1700000000000',
        keyHex: null,
        valueHex: '',
        headers: [],
      })
    })

    test('wraps a singleton header in an array, keeps a duplicate key in order, sorts keys by their bytes', () => {
      const projected = publicRecord({
        offset: '0',
        timestamp: '0',
        key: Buffer.from('k'),
        value: Buffer.from('v'),
        headers: {
          zeta: Buffer.from([0xff]),
          alpha: [Buffer.from('1'), null, Buffer.from('')],
          Beta: null,
          é: Buffer.from('x'),
        },
      })

      expect(projected.keyHex).toBe('6b')
      expect(projected.headers).toEqual([
        { key: 'Beta', values: [null] },
        { key: 'alpha', values: ['31', null, ''] },
        { key: 'zeta', values: ['ff'] },
        { key: 'é', values: ['78'] },
      ])
    })
  })

  test('expectedRecords moves every offset by the assigned base, in exact arithmetic', () => {
    const manifest = {
      batch: { baseOffset: '9007199254740993' },
      expectedPublicRecords: [
        { offset: '9007199254740993', valueHex: '61' },
        { offset: '9007199254740995', valueHex: '62' },
      ],
    }

    expect(expectedRecords(manifest, 12n)).toEqual([
      { offset: '12', valueHex: '61' },
      { offset: '14', valueHex: '62' },
    ])
    expect(expectedRecords(manifest, '9007199254740999')).toEqual([
      { offset: '9007199254740999', valueHex: '61' },
      { offset: '9007199254741001', valueHex: '62' },
    ])
  })

  describe('the baseline stage', () => {
    const Types = { Snappy: 2 }
    const usable = { lookupCodec: () => ({ compress() {}, decompress() {} }), Types }
    const placeholder = {
      lookupCodec: () => {
        throw new KafkaJSNotImplemented('Snappy compression not implemented')
      },
      Types,
    }
    const absent = { lookupCodec: () => null, Types }

    const writeVersion = (version) =>
      fs.writeFileSync(path.join(directory, 'package.json'), JSON.stringify({ version }))

    test('snappySlot tells a usable codec, an absent one and an unimplemented placeholder apart', () => {
      expect(snappySlot(usable)).toEqual({ state: 'usable', error: null })
      expect(snappySlot(absent)).toEqual({ state: 'absent', error: null })
      expect(snappySlot({ lookupCodec: () => ({ compress() {} }), Types }).state).toBe('absent')
      const { state, error } = snappySlot(placeholder)
      expect(state).toBe('unimplemented')
      expect(error).toBeInstanceOf(KafkaJSNotImplemented)
    })

    test('snappySlot rethrows a failure that says nothing about the slot', () => {
      const broken = {
        lookupCodec: () => {
          throw new TypeError('not a lookup failure')
        },
        Types,
      }

      expect(() => snappySlot(broken)).toThrow(TypeError)
    })

    test('assertBaselineArtifact accepts the pinned version with no Snappy codec', () => {
      writeVersion('3.0.0-alpha.9a')

      expect(
        assertBaselineArtifact({
          root: directory,
          version: '3.0.0-alpha.9a',
          compression: placeholder,
        })
      ).toEqual({ version: '3.0.0-alpha.9a', state: 'unimplemented' })
      expect(
        assertBaselineArtifact({ root: directory, version: '3.0.0-alpha.9a', compression: absent })
          .state
      ).toBe('absent')
    })

    test('assertBaselineArtifact refuses another artifact and a tree that has a Snappy codec', () => {
      writeVersion('3.0.0-alpha.9b')

      expect(() =>
        assertBaselineArtifact({
          root: directory,
          version: '3.0.0-alpha.9a',
          compression: placeholder,
        })
      ).toThrow(/is 3\.0\.0-alpha\.9b, not the pinned 3\.0\.0-alpha\.9a/)
      expect(() =>
        assertBaselineArtifact({ root: directory, version: '3.0.0-alpha.9b', compression: usable })
      ).toThrow(/already has a Snappy codec/)
    })

    test('assertFinalArtifact needs a usable codec', () => {
      expect(() => assertFinalArtifact({ compression: usable })).not.toThrow()
      expect(() => assertFinalArtifact({ compression: placeholder })).toThrow(/unimplemented/)
      expect(() => assertFinalArtifact({ compression: absent })).toThrow(/absent/)
    })

    test('diagnosticOf keeps class, message, code and the cause chain, and names a thrown non-error', () => {
      const cause = Object.assign(new RangeError('root'), { code: 'ERR_ROOT' })
      const error = new KafkaJSNotImplemented('Snappy compression not implemented')
      error.cause = cause

      expect(diagnosticOf(error)).toEqual({
        class: 'KafkaJSNotImplemented',
        message: 'Snappy compression not implemented',
        code: null,
        cause: { class: 'RangeError', message: 'root', code: 'ERR_ROOT', cause: null },
      })
      expect(diagnosticOf('boom')).toEqual({
        class: 'string',
        message: 'boom',
        code: null,
        cause: null,
      })
      expect(diagnosticOf(null)).toBeNull()
      expect(diagnosticOf(undefined)).toBeNull()
    })

    test('assertBaselineDiagnostic holds the failure to the pinned class, message, code and cause', () => {
      const error = new KafkaJSNotImplemented('Snappy compression not implemented')
      const pin = (changes = {}) =>
        JSON.stringify({
          class: 'KafkaJSNotImplemented',
          message: 'Snappy compression not implemented',
          code: null,
          cause: null,
          ...changes,
        })

      expect(() => assertBaselineDiagnostic(error, pin())).not.toThrow()
      expect(() => assertBaselineDiagnostic(error, pin({ message: 'Snappy' }))).toThrow(
        /not the pinned diagnostic/
      )
      expect(() => assertBaselineDiagnostic(error, pin({ class: 'Error' }))).toThrow(
        /not the pinned diagnostic/
      )
      expect(() => assertBaselineDiagnostic(error, pin({ code: 7 }))).toThrow(/not the pinned/)
      expect(() =>
        assertBaselineDiagnostic(
          error,
          pin({ cause: { class: 'Error', message: '', code: null, cause: null } })
        )
      ).toThrow(/not the pinned/)
    })

    test('assertBaselineDiagnostic refuses no failure and a pin of the wrong shape', () => {
      expect(() =>
        assertBaselineDiagnostic(null, '{"class":"A","message":"b","code":null,"cause":null}')
      ).toThrow(/saw no failure/)
      expect(() => assertBaselineDiagnostic(new Error('x'), '{"message":"x"}')).toThrow(
        /not \{ class, message, code, cause \}/
      )
      expect(() => assertBaselineDiagnostic(new Error('x'), 'null')).toThrow(/not \{ class/)
    })
  })

  test('sha256 and hexOf are the lower-case digest and hex of their input', () => {
    expect(sha256(Buffer.from('abc'))).toBe(
      'ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad'
    )
    expect(hexOf(Buffer.from([0, 255]))).toBe('00ff')
    expect(hexOf('é')).toBe('c3a9')
    expect(hexOf(null)).toBeNull()
  })
})
