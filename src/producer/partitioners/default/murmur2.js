const SEED = 0x9747b28c

// 'm' and 'r' are mixing constants generated offline.
// They're not really 'magic', they just happen to work well.
const M = 0x5bd1e995
const R = 24

/**
 * Kafka's murmur2 hash, as `Utils.murmur2` computes it, so a keyed record lands on the partition a
 * Java producer picks for the same key bytes.
 *
 * Every step is 32-bit integer arithmetic, exactly as in Java: `Math.imul` is Java's wrapping
 * `int` multiply, `>>>` its unsigned shift, and each 4-byte block is read little-endian as Java's
 * `INT_HANDLE` reads it. No 64-bit emulation is needed, so nothing is allocated per block on a
 * path every keyed record takes.
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/common/utils/Utils.java
 * @param {Buffer|string|number} key a Buffer is hashed as is; anything else as the bytes of `String(key)`
 * @returns {number} the signed 32-bit hash
 */
module.exports = (key) => {
  const data = Buffer.isBuffer(key) ? key : Buffer.from(String(key))
  const length = data.length
  const tail = length & ~3
  let h = SEED ^ length

  for (let i = 0; i < tail; i += 4) {
    let k = data.readInt32LE(i)
    k = Math.imul(k, M)
    k ^= k >>> R
    k = Math.imul(k, M)
    h = Math.imul(h, M) ^ k
  }

  switch (length - tail) {
    case 3:
      h ^= data[tail + 2] << 16
    // falls through
    case 2:
      h ^= data[tail + 1] << 8
    // falls through
    case 1:
      h ^= data[tail]
      h = Math.imul(h, M)
  }

  h ^= h >>> 13
  h = Math.imul(h, M)
  return h ^ (h >>> 15)
}
