const crc32C = require('./crc32C')

/**
 * @param {Buffer} buffer
 * @returns {number} unsigned 32-bit CRC32C checksum
 */
module.exports = (buffer) => crc32C(buffer) >>> 0
