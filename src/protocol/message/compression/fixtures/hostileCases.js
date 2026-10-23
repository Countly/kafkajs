/**
 * The hostile compressed input of the codec specs, built in memory each time it is needed: the
 * seeded mutations of a valid body, and one case for each class of malformed input with the outcome
 * it has to have. A bomb is made by streaming a compressor over a repeated chunk and is never
 * stored. Every case is a Fetch v12 response body, so it goes through the same decoder a
 * consumer's fetch does.
 */
const zlib = require('zlib')
const { once } = require('events')
const { fetchResponse } = require('../../../../consumer/__tests__/fixtures/fetchHarness')
const { byteMutations, int32Mutations, repeatedSlices } = require('../../../../../testHelpers/fuzz')
const { frame, block, runBlock } = require('../../../../../testHelpers/lz4Frame')
const { compressedBatch, plainRecords, COMPRESSORS } = require('./compressedBatch')

const MIB = 1048576

/** The bound of the corpus: small enough that every input stays under 16 MiB. */
const BOUND = 4 * MIB

/** The options every case is decoded with: the checksum is off so the mutations reach the codec. */
const OPTIONS = { checkCrcs: false, maxDecompressedBatchBytes: BOUND }

/** @param {Buffer} batchBytes @returns {Buffer} a Fetch v12 response body carrying them */
const bodyOf = (batchBytes) =>
  fetchResponse([{ partition: 0, errorCode: 0, records: batchBytes }], { apiVersion: 12 })

/**
 * Compresses `chunks` copies of a megabyte of zeros by streaming, so the input is never in memory.
 * @param {import('stream').Transform} compressor
 * @param {number} mebibytes how many megabytes of zeros to compress
 * @returns {Promise<Buffer>} the compressed stream
 */
const streamedBomb = async (compressor, mebibytes) => {
  const parts = []
  compressor.on('data', (part) => parts.push(part))
  const ended = once(compressor, 'end')
  const zeros = Buffer.alloc(MIB)

  for (let written = 0; written < mebibytes; written++) {
    if (!compressor.write(zeros)) {
      await once(compressor, 'drain')
    }
  }
  compressor.end()
  await ended
  return Buffer.concat(parts)
}

/**
 * @param {string} codec
 * @param {number} mebibytes
 * @returns {Promise<Buffer>} a stream of the codec that decompresses to `mebibytes` of zeros
 */
const bombOf = async (codec, mebibytes) => {
  if (codec === 'gzip') return streamedBomb(zlib.createGzip(), mebibytes)
  // eslint-disable-next-line n/no-unsupported-features/node-builtins -- Node 22.15+, as the codec
  if (codec === 'zstd') return streamedBomb(zlib.createZstdCompress(), mebibytes)
  if (codec === 'snappy') return COMPRESSORS.snappy(Buffer.alloc(mebibytes * MIB))
  // blocks of 4 MiB: the largest an LZ4 frame holds
  const blocks = Array.from({ length: Math.ceil(mebibytes / 4) }, () => block(runBlock(0, 4 * MIB)))
  return frame({ maxSizeCode: 7, blocks })
}

/**
 * The seeded mutations of a valid three-record batch of one codec: every byte of its Fetch
 * response set to five values, every four bytes to six, and slices repeated.
 * @param {string} codec
 * @param {number} seed
 * @returns {Promise<Array<{ name: string, buffer: Buffer, options: object }>>}
 */
const mutationCases = async (codec, seed) => {
  const valid = bodyOf(await compressedBatch({ codec, records: 3 }))
  const cases = [
    ...byteMutations(valid, seed),
    ...int32Mutations(valid, seed + 1),
    ...repeatedSlices(valid, seed + 2),
  ]

  return cases.map(({ name, buffer }) => ({ name: `${codec} ${name}`, buffer, options: OPTIONS }))
}

/**
 * One case for each class of malformed input, for the codecs it applies to. The outcome each class
 * must have is declared by the spec that runs them.
 * @param {string[]} codecs the codecs available here
 * @returns {Promise<Array<{ name: string, kind: string, codec: string, buffer: Buffer,
 *   options: object }>>} `kind` names the class, `name` the class and the codec
 */
const classCases = async (codecs) => {
  const cases = []
  const plain = plainRecords(3)
  const add = (kind, codec, batch) =>
    cases.push({
      name: `${codec} ${kind}`,
      kind,
      codec,
      buffer: bodyOf(batch),
      options: OPTIONS,
    })

  for (const codec of codecs) {
    const valid = await COMPRESSORS[codec](plain)
    const batch = (fields) => compressedBatch({ codec, records: 3, ...fields })

    add('stream cut short', codec, await batch({ body: valid.subarray(0, valid.length - 5) }))
    add('count past the bytes', codec, await batch({ count: 100 }))
    add('bomb past the bound', codec, await batch({ body: await bombOf(codec, 8) }))
    add('junk', codec, await batch({ body: Buffer.from('this is not a compressed stream') }))
  }

  if (codecs.includes('lz4')) {
    const understated = frame({ contentSize: plain.length - 1, blocks: [block(plain, true)] })
    add(
      'content size understated',
      'lz4',
      await compressedBatch({ codec: 'lz4', body: understated })
    )
  }

  // a raw block declaring 2^31 bytes, and xerial blocks declaring 3 MiB each: neither is allocated
  const declared = (size) => {
    const varint = []
    for (let rest = size; ; rest = Math.floor(rest / 128)) {
      varint.push(rest >= 128 ? (rest % 128) + 128 : rest)
      if (rest < 128) return Buffer.from(varint)
    }
  }
  const xerial = Buffer.from('82534e41505059000000000100000001', 'hex')
  const sized = (size) => {
    const body = Buffer.concat([declared(size), Buffer.from([0, 0x61])])
    const length = Buffer.alloc(4)
    length.writeUInt32BE(body.length)
    return Buffer.concat([length, body])
  }
  add(
    'declared length over the bound',
    'snappy',
    await compressedBatch({
      codec: 'snappy',
      body: Buffer.concat([declared(2 ** 31), Buffer.from([0, 0x61])]),
    })
  )
  add(
    'declared lengths over the bound in all',
    'snappy',
    await compressedBatch({
      codec: 'snappy',
      body: Buffer.concat([xerial, sized(3 * MIB), sized(3 * MIB)]),
    })
  )

  return cases
}

module.exports = { MIB, BOUND, OPTIONS, bodyOf, streamedBomb, bombOf, mutationCases, classCases }
