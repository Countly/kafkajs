const fs = require('fs')
const path = require('path')
const { createHash } = require('crypto')
const { encodeMetricsData, measureMetricsData } = require('../otlp')

const directory = path.join(__dirname, 'fixtures/otlp')
const names = [
  'empty',
  'zero',
  'nested',
  'cumulative',
  'signed-min',
  'signed-max',
  'negative-zero',
  'multibyte',
]
const fixture = (name) => JSON.parse(fs.readFileSync(path.join(directory, `${name}.json`), 'utf8'))
const verification = fixture('java-verification')

/** @param {Buffer} bytes retained source, body or decoded audit @returns {string} SHA-256 hex */
const sha256 = (bytes) => createHash('sha256').update(bytes).digest('hex')

/** Restores exact domains from literal fixture fields; expected bytes never use the writer. */
const restore = (fields) => ({
  ...fields,
  points: fields.points.map((p) => ({
    ...p,
    identity: { ...p.identity, revision: BigInt(p.identity.revision) },
    value: p.type === 'sum' || p.valueKind === 'int' ? BigInt(p.value) : p.value,
    timeUnixNano: BigInt(p.timeUnixNano),
    startTimeUnixNano: p.startTimeUnixNano === undefined ? undefined : BigInt(p.startTimeUnixNano),
  })),
})

/** Reads wire fields with local bounds and no production reader or encoder. */
const fields = (buffer) => {
  const result = []
  let at = 0
  const varint = () => {
    let value = 0
    let factor = 1
    for (let n = 0; n < 8 && at < buffer.length; n++) {
      const byte = buffer[at++]
      value += (byte & 127) * factor
      if (byte < 128 && Number.isSafeInteger(value)) return value
      factor *= 128
    }
    throw new RangeError('Invalid protobuf varint')
  }
  while (at < buffer.length) {
    const key = varint()
    const type = key % 8
    const number = Math.floor(key / 8)
    if (!number) throw new RangeError('Invalid protobuf field')
    let value
    if (type === 0) value = varint()
    else {
      const size = type === 1 ? 8 : type === 2 ? varint() : -1
      if (size < 0 || at + size > buffer.length) throw new RangeError('Invalid protobuf length')
      value = buffer.subarray(at, at + size)
      at += size
    }
    result.push({ number, type, value })
  }
  return result
}
const values = (bytes, number) =>
  fields(bytes)
    .filter((f) => f.number === number)
    .map((f) => f.value)
const metrics = (bytes) => values(values(values(bytes, 1)[0], 2)[0], 2)
const dataPoints = (metric) =>
  values(values(metric, fields(metric).some((f) => f.number === 7) ? 7 : 5)[0], 1)

describe('independent metric protobuf vectors', () => {
  test('independent Java verification covers the current encoder and all cases', () => {
    expect(verification.command).toBe('GenerateTelemetryVectors --verify candidate decoded')
    expect(verification.exitCode).toBe(0)
    expect(verification.result).toBe(
      'All eight production bodies match independent Java semantics and bytes'
    )
    expect(verification.generationDeterministic).toBe(true)
    expect(verification.cases.map((entry) => entry.name).sort()).toEqual([...names].sort())
    expect(verification.productionEncoderSha256).toBe(
      sha256(fs.readFileSync(path.join(__dirname, '../otlp.js')))
    )
    expect(verification.candidateGeneratorSha256).toBe(
      sha256(
        fs.readFileSync(
          path.join(__dirname, '../../../scripts/protocol/javaVectors/generateTelemetryBodies.js')
        )
      )
    )
  })

  test.each(names)('%s matches fixed bytes and exact length', (name) => {
    const vector = fixture(name)
    const snapshot = restore(vector.fields)
    const bytes = encodeMetricsData(snapshot)
    expect(bytes.toString('hex')).toBe(vector.hex)
    expect(measureMetricsData(snapshot)).toBe(vector.hex.length / 2)
    expect(bytes.length).toBe(vector.hex.length / 2)
    expect(vector.provenance.status).toBe('generated with pinned Java bindings')
    expect(vector.provenance.protoTag).toBe('v1.9.0')
    expect(vector.provenance.bindingsArtifact).toBe('otlp-v1.9.0-bindings-unrelocated.jar')
    expect(vector.provenance.bindingsProvenance).toBe('frozen SHA-256-pinned artifact')
    expect(vector.provenance.bindingsSha256).toBe(
      '99676bd7a6eff2b8e068a1ab10fe30cadf27958e82914adc6516699c88e266e7'
    )
    expect(vector.provenance.protobufJavaCoordinates).toBe(
      'com.google.protobuf:protobuf-java:3.25.5'
    )
    expect(vector.provenance.protobufJavaSha1).toBe('5ae5c9ec39930ae9b5a61b32b93288818ec05ec1')
    expect(vector.provenance.protobufJavaSha256).toBe(
      '8540247fad9e06baefa8fb45eb313802d019f485f14300e0f9d6b556ed88e753'
    )
    const source = fs.readFileSync(
      path.join(__dirname, '../../../scripts/protocol/javaVectors/GenerateTelemetryVectors.java')
    )
    expect(vector.provenance.generatorSha256).toBe(sha256(source))
    expect(verification.generatorSha256).toBe(sha256(source))
    expect(verification.cases.find((entry) => entry.name === name)).toEqual({
      name,
      bytes: bytes.length,
      productionBodySha256: sha256(bytes),
      decodedSha256: sha256(fs.readFileSync(path.join(directory, `${name}.decoded.txt`))),
    })
  })

  test('zero-valued points retain their numeric oneof', () => {
    const bytes = encodeMetricsData(restore(fixture('zero').fields))
    const points = metrics(bytes).flatMap(dataPoints)
    expect(points).toHaveLength(2)
    expect(points.map((p) => fields(p).map((f) => [f.number, f.type]))).toEqual([
      [[6, 1]],
      [[4, 1]],
    ])
    expect(points[0].subarray(1).readBigInt64LE()).toBe(0n)
    expect(points[1].subarray(1).readDoubleLE()).toBe(0)
  })

  test('merged gauges and monotonic sum preserve names attributes and resource drop count', () => {
    const bytes = encodeMetricsData(restore(fixture('nested').fields))
    const mm = metrics(bytes)
    expect(mm.map((m) => values(m, 1)[0].toString())).toEqual([
      'com.example.count',
      'com.example.gauge',
    ])
    expect(mm.map((m) => dataPoints(m).length)).toEqual([1, 2])
    const sum = fields(values(mm[0], 7)[0])
    expect(sum.filter((f) => f.type === 0)).toEqual([
      { number: 2, type: 0, value: 1 },
      { number: 3, type: 0, value: 1 },
    ])
    expect(values(dataPoints(mm[0])[0], 6)[0].readBigInt64LE()).toBe(9007199254740993n)
    const resource = values(values(bytes, 1)[0], 1)[0]
    expect(values(resource, 2)).toEqual([1])
    expect(values(resource, 1).map((kv) => values(kv, 1)[0].toString())).toEqual([
      'client_rack',
      'group_id',
    ])
    expect(values(values(dataPoints(mm[1])[1], 7)[0], 2)[0].includes(Buffer.from('温度'))).toBe(
      true
    )
  })

  test('unsigned time and signed integer domains stay distinct', () => {
    const p = dataPoints(metrics(encodeMetricsData(restore(fixture('cumulative').fields)))[0])[0]
    expect(values(p, 2)[0].readBigUInt64LE()).toBe(9223372036854775808n)
    expect(values(p, 3)[0].readBigUInt64LE()).toBe(18446744073709551615n)
    expect(values(p, 6)[0].readBigInt64LE()).toBe(9223372036854775807n)
  })

  test.each([-1n, 1n << 64n, Number.MAX_SAFE_INTEGER])(
    'refuses out-of-domain timestamp %s',
    (time) => {
      const s = restore(fixture('nested').fields)
      s.points[0].timeUnixNano = time
      expect(() => encodeMetricsData(s)).toThrow(RangeError)
    }
  )

  test.each([-(1n << 63n) - 1n, 1n << 63n])(
    'refuses out-of-domain signed primitive %s',
    (value) => {
      const s = restore(fixture('signed-min').fields)
      s.points[0].value = value
      expect(() => encodeMetricsData(s)).toThrow(RangeError)
    }
  )

  test('canonical order and UTF-8 boundaries do not depend on input order', () => {
    const s = restore(fixture('nested').fields)
    const expected = fixture('nested').hex
    s.points.reverse()
    s.resources.reverse()
    expect(encodeMetricsData(s).toString('hex')).toBe(expected)
    const boundary = restore(fixture('multibyte').fields)
    expect(encodeMetricsData(boundary).toString('hex')).toBe(fixture('multibyte').hex)
    const point = dataPoints(metrics(encodeMetricsData(boundary))[0])[0]
    expect(fields(point).find((f) => f.number === 7).type).toBe(2)
  })

  test('exact cap succeeds and one byte less refuses before payload allocation', () => {
    const s = restore(fixture('nested').fields)
    expect(encodeMetricsData(s, { maxBytes: 235 }).length).toBe(235)
    const allocate = jest.spyOn(Buffer, 'alloc')
    allocate.mockClear()
    expect(() => encodeMetricsData(s, { maxBytes: 234 })).toThrow(RangeError)
    expect(allocate).not.toHaveBeenCalled()
    allocate.mockRestore()
  })

  test('uint32 resource count uses an unsigned varint', () => {
    const s = restore(fixture('nested').fields)
    s.droppedResourceAttributesCount = 4294967295
    const bytes = encodeMetricsData(s)
    expect(values(values(values(bytes, 1)[0], 1)[0], 2)).toEqual([4294967295])
  })

  test('negative zero and primitive extrema have literal little-endian value bytes', () => {
    const negzero = values(
      dataPoints(metrics(encodeMetricsData(restore(fixture('negative-zero').fields)))[0])[0],
      4
    )[0]
    expect(negzero.toString('hex')).toBe('0000000000000080')
    for (const [name, hex] of [
      ['signed-min', '0000000000000080'],
      ['signed-max', 'ffffffffffffff7f'],
    ]) {
      const p = dataPoints(metrics(encodeMetricsData(restore(fixture(name).fields)))[0])[0]
      expect(values(p, 6)[0].toString('hex')).toBe(hex)
    }
  })
})
