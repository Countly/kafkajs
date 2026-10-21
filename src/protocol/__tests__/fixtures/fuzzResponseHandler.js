const { performance } = require('perf_hooks')
const {
  TYPED_ERRORS,
  PROMPT_MS,
  PROMPT_ATTEMPTS,
  PROTOTYPE_NAMES,
  disableCompression,
  elementsIn,
} = require('./fuzzOracle')
const { loadVectors, codecOf, optionsOf, project, differences } = require('./fuzzResponseVectors')

disableCompression()

/**
 * @param {string} name a vector name, `<api>/<file>`
 * @returns {import('./fuzzResponseVectors').ResponseVector}
 * @throws {Error} when the vectors hold none of that name
 */
const vectorNamed = (name) => {
  const found = loadVectors().vectors.find((vector) => vector.name === name)
  if (!found) throw new Error(`no response vector named ${name}`)
  return found
}

/**
 * Decodes the body, then parses what was decoded, as `Connection` does.
 * @param {object} target the vector, or what stands for one
 * @param {{ decode: Function, parse: Function }} codec
 * @param {Buffer} input
 * @param {{ decodes: number }} counter counts the calls of the codec's `decode`: the proof that the
 *   target ran
 * @returns {Promise<{ decoded?: *, error?: Error }>}
 */
const run = async (target, codec, input, counter) => {
  const decode = (...args) => {
    counter.decodes++
    return codec.decode(...args)
  }
  let decoded
  try {
    decoded = await decode(input, optionsOf(target))
    await codec.parse(decoded)
    return { decoded }
  } catch (error) {
    return { decoded, error }
  }
}

/**
 * @param {object} target
 * @param {*} decoded
 * @returns {boolean} whether the decoded value holds every value of the target's control; a value
 *   the projection cannot read does not
 */
const matchesControl = (target, decoded) => {
  try {
    return differences(target.control, project(target, decoded)) === null
  } catch {
    return false
  }
}

/**
 * What a decoded value breaks, if it breaks anything: allocation beyond the bytes it came from, a
 * change to `Object.prototype`, or, from a body with bytes removed, the values of the whole body.
 * @param {*} decoded
 * @param {Buffer} input
 * @param {object} fuzzCase
 * @param {object} target
 * @returns {string|null}
 */
const brokenBy = (decoded, input, { truncated, tailIsEmptyTags }, target) => {
  if (elementsIn(decoded) > input.length) {
    return `${elementsIn(decoded)} elements from ${input.length} bytes`
  }
  if (Object.getOwnPropertyNames(Object.prototype).join() !== PROTOTYPE_NAMES) {
    return 'Object.prototype changed'
  }
  if (truncated && !tailIsEmptyTags && !target.control.error && matchesControl(target, decoded)) {
    return 'a body with bytes removed decoded to the values of the whole body'
  }
  return null
}

/**
 * Decodes one case through the codec of its response vector and classifies the outcome.
 * @param {object} fuzzCase
 * @param {string} [fuzzCase.vector] the name of the response vector the body is a mutation of
 * @param {object} [fuzzCase.target] a stand-in for a vector, for a corpus case and a self-test:
 *   `{ directory, version, codec, request, control }`
 * @param {Uint8Array} fuzzCase.buffer the body
 * @param {boolean} [fuzzCase.truncated] whether the body is the vector's with bytes removed
 * @param {boolean} [fuzzCase.tailIsEmptyTags] whether every byte removed is an empty tag buffer
 * @param {boolean} [fuzzCase.prompt] whether the decoder has to answer within `PROMPT_MS`
 * @param {{ decoded?: boolean, error?: string }} [fuzzCase.expect] the exact outcome of a case:
 *   that it decodes, or the name of the error
 * @param {{ decode: Function, parse: Function }} [fuzzCase.codec] a codec to use instead of the
 *   vector's, which a case run in this thread can give to show what the oracle makes of it
 * @returns {Promise<{ ran: 0|1, category: string, violation: string|null }>} `category` is
 *   `decoded` or `typed:<error class>`; `ran` is 1 when the target's `decode` was called, and
 *   `violation` says what broke
 */
module.exports = async (fuzzCase) => {
  const input = Buffer.from(fuzzCase.buffer)
  const target = fuzzCase.target || vectorNamed(fuzzCase.vector)
  const codec = fuzzCase.codec || codecOf(target)
  const counter = { decodes: 0 }
  let outcome
  let milliseconds

  for (let attempt = 0; attempt < (fuzzCase.prompt ? PROMPT_ATTEMPTS : 1); attempt++) {
    const startedAt = performance.now()
    outcome = await run(target, codec, input, counter)
    milliseconds = performance.now() - startedAt
    if (milliseconds <= PROMPT_MS) break
  }

  const { decoded, error } = outcome
  const category = error ? `typed:${error.constructor.name}` : 'decoded'
  const violation = (() => {
    if (error && !TYPED_ERRORS.some((type) => error instanceof type)) {
      return `${error.constructor.name}: ${error.message}`
    }
    if (fuzzCase.prompt && milliseconds > PROMPT_MS) return `took ${milliseconds.toFixed(1)} ms`
    if (fuzzCase.expect && fuzzCase.expect.error) {
      return error && error.constructor.name === fuzzCase.expect.error
        ? null
        : `expected ${fuzzCase.expect.error}, got ${category}`
    }
    if (decoded) return brokenBy(decoded, input, fuzzCase, target)
    return fuzzCase.expect ? `expected a decoded value, got ${category}` : null
  })()

  return { ran: counter.decodes > 0 ? 1 : 0, category, violation }
}
