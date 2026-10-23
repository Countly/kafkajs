const { DEFAULT_MAX_DECOMPRESSED_BATCH_BYTES } = require('../../../constants')

// A consumer has one pair of options, so the cache holds one or two chains; the cap protects a
// process that builds many differently configured consumers.
const MAX_CHAINS = 16

/**
 * Keeps one reader chain per pair of decode options, so a fetch response allocates no closures:
 * the Fetch response modules are stateless singletons shared by every consumer in the process.
 * @param {(options: { checkCrcs: boolean, maxDecompressedBatchBytes: number }) =>
 *   (rawData: Buffer) => Promise<object>} buildDecode builds the chain for one pair
 * @returns {(options?: { checkCrcs?: boolean, maxDecompressedBatchBytes?: number }) =>
 *   (rawData: Buffer) => Promise<object>} the chain for `options`, built on first use. A pair
 *   beyond the cap evicts the oldest chain, which is cheap to build again.
 */
const decoderCache = (buildDecode) => {
  const chains = new Map()

  return ({ checkCrcs, maxDecompressedBatchBytes } = {}) => {
    const options = {
      checkCrcs: checkCrcs !== false,
      maxDecompressedBatchBytes: maxDecompressedBatchBytes ?? DEFAULT_MAX_DECOMPRESSED_BATCH_BYTES,
    }
    const key = `${options.checkCrcs}:${options.maxDecompressedBatchBytes}`
    let chain = chains.get(key)

    if (chain === undefined) {
      if (chains.size >= MAX_CHAINS) {
        chains.delete(chains.keys().next().value)
      }
      chain = buildDecode(options)
      chains.set(key, chain)
    }

    return chain
  }
}

module.exports = decoderCache
