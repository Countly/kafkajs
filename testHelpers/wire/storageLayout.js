/** Independent schema-layout writer used only by storage wire fixtures. */
class Writer {
  /**
   * @returns {Writer} empty byte accumulator */
  constructor() {
    this.parts = []
  }

  /**
   * @param {Buffer} bytes independent bytes
   * @returns {void} */
  add(bytes) {
    this.parts.push(bytes)
  }

  /**
   * @param {number} n unsigned compact value
   * @returns {void} */
  uint(n) {
    const b = []
    do {
      b.push((n & 127) | (n > 127 ? 128 : 0))
      n = Math.floor(n / 128)
    } while (n)
    this.add(Buffer.from(b))
  }

  /**
   * @param {string} type scalar layout
   * @param {*} value scalar input
   * @param {boolean} flex compact mode
   * @returns {void} */
  scalar(type, value, flex) {
    if (type === 'string') {
      if (flex) this.uint(value === null ? 0 : Buffer.byteLength(value) + 1)
      else this.scalar('int16', value === null ? -1 : Buffer.byteLength(value), false)
      if (value !== null) this.add(Buffer.from(value))
      return
    }
    if (type === 'uuid') {
      this.add(
        Buffer.from((value ?? '00000000-0000-0000-0000-000000000000').replaceAll('-', ''), 'hex')
      )
      return
    }
    const size = { int8: 1, bool: 1, int16: 2, int32: 4, int64: 8 }[type]
    const b = Buffer.alloc(size)
    if (type === 'int64') b.writeBigInt64BE(BigInt(value))
    else if (type === 'bool') b.writeInt8(value ? 1 : 0)
    else b[{ int8: 'writeInt8', int16: 'writeInt16BE', int32: 'writeInt32BE' }[type]](value)
    this.add(b)
  }

  /**
   * @param {Array} fields ordered struct
   * @param {object} row values
   * @param {boolean} flex compact mode
   * @returns {void} */
  struct(fields, row, flex) {
    for (const [key, type] of fields) {
      const value = row[key]
      if (Array.isArray(type)) {
        if (flex) this.uint(value === null ? 0 : value.length + 1)
        else this.scalar('int32', value === null ? -1 : value.length, false)
        if (value !== null)
          for (const entry of value) {
            if (typeof type[0] === 'string') this.scalar(type[0], entry, flex)
            else this.struct(type, entry, flex)
          }
      } else this.scalar(type, value, flex)
    }
    if (flex) this.uint(0)
  }

  /**
   * @returns {Buffer} complete body */
  bytes() {
    return Buffer.concat(this.parts)
  }
}
/** Independent body reader; rejects truncation, invalid compact counts and trailing bytes. */
class Reader {
  /**
   * @param {Buffer} bytes body
   * @returns {Reader} cursor */
  constructor(bytes) {
    this.bytes = bytes
    this.at = 0
  }

  /**
   * @param {number} n count
   * @returns {Buffer} next bytes
   * @throws {Error} short body */
  take(n) {
    if (n < 0 || this.at + n > this.bytes.length) throw new Error('Truncated storage body')
    const b = this.bytes.subarray(this.at, this.at + n)
    this.at += n
    return b
  }

  /**
   * @returns {number} compact integer
   * @throws {Error} oversized integer */
  uint() {
    let n = 0
    for (let s = 0; s <= 28; s += 7) {
      const b = this.take(1)[0]
      n += (b & 127) * 2 ** s
      if (!(b & 128)) return n
    }
    throw new Error('Invalid storage varint')
  }

  /**
   * @param {string} type scalar
   * @param {boolean} flex compact mode
   * @returns {*} decoded value */
  scalar(type, flex) {
    if (type === 'string') {
      const n = flex ? this.uint() - 1 : this.scalar('int16', false)
      return n === -1 ? null : this.take(n).toString()
    }
    if (type === 'uuid') {
      const h = this.take(16).toString('hex')
      return /^0+$/.test(h)
        ? null
        : `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`
    }
    const b = this.take({ int8: 1, bool: 1, int16: 2, int32: 4, int64: 8 }[type])
    if (type === 'int64') return b.readBigInt64BE().toString()
    if (type === 'bool') {
      if (b[0] > 1) throw new Error('Invalid storage boolean')
      return b[0] === 1
    }
    return b[{ int8: 'readInt8', int16: 'readInt16BE', int32: 'readInt32BE' }[type]]()
  }

  /**
   * @param {Array} fields struct
   * @param {boolean} flex compact mode
   * @returns {object} read fields
   * @throws {Error} malformed tags */
  struct(fields, flex) {
    const row = {}
    for (const [key, type] of fields) {
      if (!Array.isArray(type)) {
        row[key] = this.scalar(type, flex)
        continue
      }
      const count = flex ? this.uint() - 1 : this.scalar('int32', false)
      if (count < -1 || count > this.bytes.length) throw new Error('Invalid storage array')
      row[key] =
        count === -1
          ? null
          : Array.from({ length: count }, () =>
              typeof type[0] === 'string' ? this.scalar(type[0], flex) : this.struct(type, flex)
            )
    }
    if (flex) {
      const count = this.uint()
      let last = -1
      for (let i = 0; i < count; i++) {
        const tag = this.uint()
        const size = this.uint()
        if (tag <= last) throw new Error('Invalid storage tags')
        last = tag
        this.take(size)
      }
    }
    return row
  }
}
/** Defines independently transcribed ordered fields from Kafka 4.3.1 JSON.
 *
 * @param {string} api message name
 * @param {number} v version
 * @param {string} direction request or response
 *
 * @returns {Array} field grammar
 */
const layout = (api, v, direction) => {
  const scalarPartitions = ['int32']
  const selection = [
    ['topic', 'string'],
    ['partitions', scalarPartitions],
  ]
  if (api === 'DescribeLogDirs') {
    if (direction === 'request') return [['topics', selection]]
    const partition = [
      ['partition', 'int32'],
      ['size', 'int64'],
      ['offsetLag', 'int64'],
      ['isFuture', 'bool'],
    ]
    const dir = [
      ['errorCode', 'int16'],
      ['path', 'string'],
      [
        'topics',
        [
          ['topic', 'string'],
          ['partitions', partition],
        ],
      ],
    ]
    if (v >= 4) dir.push(['totalBytes', 'int64'], ['usableBytes', 'int64'])
    if (v >= 5) dir.push(['isCordoned', 'bool'])
    return [
      ['throttleTime', 'int32'],
      ...(v >= 3 ? [['errorCode', 'int16']] : []),
      ['logDirs', dir],
    ]
  }
  if (api === 'AlterReplicaLogDirs')
    return direction === 'request'
      ? [
          [
            'dirs',
            [
              ['path', 'string'],
              ['topics', selection],
            ],
          ],
        ]
      : [
          ['throttleTime', 'int32'],
          [
            'topics',
            [
              ['topic', 'string'],
              [
                'partitions',
                [
                  ['partition', 'int32'],
                  ['errorCode', 'int16'],
                ],
              ],
            ],
          ],
        ]
  if (api === 'ListOffsets')
    return direction === 'request'
      ? [
          ['replicaId', 'int32'],
          ['isolationLevel', 'int8'],
          [
            'topics',
            [
              ['topic', 'string'],
              [
                'partitions',
                [
                  ['partition', 'int32'],
                  ['currentLeaderEpoch', 'int32'],
                  ['timestamp', 'int64'],
                ],
              ],
            ],
          ],
          ['timeout', 'int32'],
        ]
      : [
          ['throttleTime', 'int32'],
          [
            'responses',
            [
              ['topic', 'string'],
              [
                'partitions',
                [
                  ['partition', 'int32'],
                  ['errorCode', 'int16'],
                  ['timestamp', 'int64'],
                  ['offset', 'int64'],
                  ['leaderEpoch', 'int32'],
                ],
              ],
            ],
          ],
        ]
  if (api === 'AlterPartitionReassignments')
    return direction === 'request'
      ? [
          ['timeout', 'int32'],
          ['allowReplicationFactorChange', 'bool'],
          [
            'topics',
            [
              ['topic', 'string'],
              [
                'partitionAssignment',
                [
                  ['partition', 'int32'],
                  ['replicas', scalarPartitions],
                ],
              ],
            ],
          ],
        ]
      : [
          ['throttleTime', 'int32'],
          ['allowReplicationFactorChange', 'bool'],
          ['errorCode', 'int16'],
          ['errorMessage', 'string'],
          [
            'responses',
            [
              ['topic', 'string'],
              [
                'partitions',
                [
                  ['partition', 'int32'],
                  ['errorCode', 'int16'],
                  ['errorMessage', 'string'],
                ],
              ],
            ],
          ],
        ]
  if (api === 'DeleteTopics')
    return direction === 'request'
      ? [
          [
            'topics',
            [
              ['topic', 'string'],
              ['topicId', 'uuid'],
            ],
          ],
          ['timeout', 'int32'],
        ]
      : [
          ['throttleTime', 'int32'],
          [
            'topicErrors',
            [
              ['topic', 'string'],
              ['topicId', 'uuid'],
              ['errorCode', 'int16'],
              ['errorMessage', 'string'],
            ],
          ],
        ]
  throw new Error('Unknown storage layout')
}
/** Creates the standard scripted pair without importing a production request codec.
 *
 * @param {string} api message name
 * @param {number} apiKey key
 * @param {number} version version
 * @param {object} expected fixed literals
 *
 * @returns {object} scripted pair plus independent layout functions
 */
module.exports = (api, apiKey, version, expected) => {
  const flex = !(version === 1 && ['DescribeLogDirs', 'AlterReplicaLogDirs'].includes(api))
  return {
    api,
    apiKey,
    version,
    /** Encodes independent response literals.
     *
     * @param {object} [fields] response overrides
     *
     * @returns {Buffer} response body
     *
     * @throws {Error} invalid scalar input
     */
    buildResponse(fields = {}) {
      const w = new Writer()
      w.struct(layout(api, version, 'response'), { ...expected.response, ...fields }, flex)
      return w.bytes()
    },
    /** Reads every independent request field.
     *
     * @param {Buffer} body request bytes
     *
     * @returns {object} literal request fields
     *
     * @throws {Error} truncated or surplus bytes
     */
    readRequest(body) {
      const r = new Reader(body)
      const result = r.struct(layout(api, version, 'request'), flex)
      if (r.at !== body.length) throw new Error('Storage request has trailing bytes')
      return result
    },
  }
}
