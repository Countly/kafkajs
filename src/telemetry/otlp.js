const MAX_BYTES = 1048576
const MAX_TIME = (1n << 64n) - 1n
const MIN_INT = -(1n << 63n)
const MAX_INT = (1n << 63n) - 1n

/** Writes only the protobuf primitives used by numeric telemetry, or counts their bytes. */
class ProtoWriter {
  /** @param {Buffer|null} buffer fixed output or null for measurement @returns {ProtoWriter} writer */
  constructor(buffer = null) {
    this.buffer = buffer
    this.offset = 0
  }

  /** @param {number} value unsigned safe integer @returns {void} unsigned LEB128 bytes
   * @throws {RangeError} for an invalid byte count or enum
   */
  varint(value) {
    if (!Number.isSafeInteger(value) || value < 0) throw new RangeError('Invalid protobuf integer')
    do {
      const byte = value % 128
      value = Math.floor(value / 128)
      if (this.buffer) this.buffer[this.offset] = byte + (value ? 128 : 0)
      this.offset += 1
    } while (value)
  }

  /** @param {number} field field number @param {number} value nonnegative integer
   * @returns {void} omitted-default scalar @throws {RangeError} for invalid values
   */
  scalar(field, value) {
    if (!value) return
    this.varint(field * 8)
    this.varint(value)
  }

  /** @param {number} field field number @param {string} value UTF-8 text
   * @param {boolean} [present] retain an empty oneof string @returns {void} encoded field
   */
  string(field, value, present = false) {
    if (!value && !present) return
    const length = Buffer.byteLength(value)
    this.varint(field * 8 + 2)
    this.varint(length)
    if (this.buffer) this.buffer.write(value, this.offset, length, 'utf8')
    this.offset += length
  }

  /** @param {number} field field number @param {Function} body bounded message traversal
   * @returns {void} length-delimited message; never grows the destination buffer
   */
  message(field, body) {
    const size = new ProtoWriter()
    body(size)
    this.varint(field * 8 + 2)
    this.varint(size.offset)
    if (this.buffer) body(this)
    else this.offset += size.offset
  }

  /** @param {number} field field number @param {bigint|number} value exact scalar
   * @param {'time'|'int'|'double'} type wire domain @returns {void} little-endian fixed scalar
   * @throws {RangeError} for an out-of-domain value
   */
  fixed(field, value, type) {
    const valid =
      type === 'double'
        ? typeof value === 'number' && Number.isFinite(value)
        : typeof value === 'bigint' &&
          value >= (type === 'time' ? 0n : MIN_INT) &&
          value <= (type === 'time' ? MAX_TIME : MAX_INT)
    if (!valid) throw new RangeError('Invalid telemetry scalar')
    if (type === 'time' && value === 0n) return
    this.varint(field * 8 + 1)
    if (this.buffer) {
      if (type === 'double') this.buffer.writeDoubleLE(value, this.offset)
      else if (type === 'time') this.buffer.writeBigUInt64LE(value, this.offset)
      else this.buffer.writeBigInt64LE(value, this.offset)
    }
    this.offset += 8
  }
}

/** Traverses the fixed MetricsData Sum/Gauge subset with deterministic field and point ordering. */
class MetricsDataEncoding {
  /** @param {object} snapshot immutable collector sample @param {Array} points selected whole points
   * @returns {MetricsDataEncoding} bounded traversal @throws {RangeError} for excessive cardinality
   */
  constructor(snapshot, points) {
    if (points.length > 257 || snapshot.resources.length > 5)
      throw new RangeError('Invalid telemetry size')
    this.snapshot = snapshot
    this.groups = new Map()
    for (const point of [...points].sort((a, b) =>
      a.identity.key < b.identity.key ? -1 : a.identity.key > b.identity.key ? 1 : 0
    )) {
      if (point.attributes.length > 8) throw new RangeError('Invalid telemetry size')
      const key = JSON.stringify([point.name, point.type, point.unit])
      if (!this.groups.has(key)) this.groups.set(key, [])
      this.groups.get(key).push(point)
    }
  }

  /** @param {ProtoWriter} writer destination @param {Array} attributes string key/value records
   * @param {number} field repeated field number @returns {void} canonical attributes
   */
  attributes(writer, attributes, field) {
    for (const attr of [...attributes].sort((a, b) =>
      a.key < b.key ? -1 : a.key > b.key ? 1 : 0
    )) {
      writer.message(field, (kv) => {
        kv.string(1, attr.key)
        kv.message(2, (value) => value.string(1, attr.value, true))
      })
    }
  }

  /** @param {ProtoWriter} writer destination @param {object} point numeric data point
   * @returns {void} exact timestamp/value fields and canonical attributes
   * @throws {RangeError} for an invalid scalar
   */
  point(writer, point) {
    writer.fixed(2, point.startTimeUnixNano === undefined ? 0n : point.startTimeUnixNano, 'time')
    writer.fixed(3, point.timeUnixNano, 'time')
    writer.fixed(
      typeof point.value === 'bigint' ? 6 : 4,
      point.value,
      typeof point.value === 'bigint' ? 'int' : 'double'
    )
    this.attributes(writer, point.attributes, 7)
  }

  /** @param {ProtoWriter} writer destination @param {Array} points same name/type/unit points
   * @returns {void} one merged Gauge or Sum @throws {RangeError} for mixed or invalid aggregate metadata
   */
  metric(writer, points) {
    const first = points[0]
    if (
      !['sum', 'gauge'].includes(first.type) ||
      points.some(
        (p) =>
          p.aggregationTemporality !== first.aggregationTemporality ||
          p.isMonotonic !== first.isMonotonic
      )
    )
      throw new RangeError('Invalid telemetry aggregate')
    writer.string(1, first.name)
    writer.string(3, first.unit)
    writer.message(first.type === 'sum' ? 7 : 5, (data) => {
      for (const point of points) data.message(1, (w) => this.point(w, point))
      if (first.type === 'sum') {
        if (![1, 2].includes(first.aggregationTemporality))
          throw new RangeError('Invalid telemetry temporality')
        data.scalar(2, first.aggregationTemporality)
        data.scalar(3, first.isMonotonic ? 1 : 0)
      }
    })
  }

  /** @param {ProtoWriter} writer destination @returns {void} zero bytes for an empty point set
   * @throws {RangeError} for an invalid resource drop count
   */
  write(writer) {
    if (!this.groups.size) return
    const s = this.snapshot
    if (
      !Number.isInteger(s.droppedResourceAttributesCount) ||
      s.droppedResourceAttributesCount < 0 ||
      s.droppedResourceAttributesCount > 4294967295
    )
      throw new RangeError('Invalid resource count')
    writer.message(1, (resourceMetrics) => {
      resourceMetrics.message(1, (resource) => {
        this.attributes(resource, s.resources, 1)
        resource.scalar(2, s.droppedResourceAttributesCount)
      })
      resourceMetrics.message(2, (scopeMetrics) => {
        scopeMetrics.message(1, (scope) => {
          scope.string(1, s.scope.name)
          scope.string(2, s.scope.version)
        })
        for (const points of this.groups.values())
          scopeMetrics.message(2, (metric) => this.metric(metric, points))
      })
    })
  }
}

/** Measures the exact supported protobuf body without allocating a payload buffer.
 * @param {object} snapshot immutable MetricSnapshot @param {Array} [points] retained points
 * @returns {number} raw byte count @throws {RangeError} for invalid scalar/cardinality
 */
const measureMetricsData = (snapshot, points = snapshot.points) => {
  const writer = new ProtoWriter()
  new MetricsDataEncoding(snapshot, points).write(writer)
  return writer.offset
}

/** Encodes the supported MetricsData subset into one exactly sized bounded buffer.
 * @param {object} snapshot immutable MetricSnapshot
 * @param {{points?:Array,maxBytes?:number}} [options] selected points and raw cap
 * @returns {Buffer} deterministic bytes, including an empty buffer when no points remain
 * @throws {RangeError} before allocation when raw size exceeds the positive local cap
 */
const encodeMetricsData = (snapshot, { points = snapshot.points, maxBytes = MAX_BYTES } = {}) => {
  if (!Number.isInteger(maxBytes) || maxBytes < 1 || maxBytes > MAX_BYTES)
    throw new RangeError('Invalid telemetry cap')
  const encoding = new MetricsDataEncoding(snapshot, points)
  const size = new ProtoWriter()
  encoding.write(size)
  if (size.offset > maxBytes) throw new RangeError('Telemetry body exceeds cap')
  const buffer = Buffer.alloc(size.offset)
  const writer = new ProtoWriter(buffer)
  encoding.write(writer)
  if (writer.offset !== buffer.length) throw new RangeError('Telemetry length changed')
  return buffer
}

module.exports = { encodeMetricsData, measureMetricsData }
