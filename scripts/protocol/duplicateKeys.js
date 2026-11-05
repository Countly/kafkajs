'use strict'

/**
 * Finds object keys written twice in JSON text. `JSON.parse` keeps the last value of a repeated
 * key, so a parsed record cannot show that an earlier one was overwritten.
 *
 * Zero dependencies, CommonJS, Node >= 22.
 */

/** One string, one structural character, or one number or literal; whitespace is skipped. */
const TOKEN = /"[^"\\]*(?:\\.[^"\\]*)*"|[{}[\],:]|[^\s{}[\],:"]+/g

/**
 * Walks every token of the text, so formatting does not matter: keys are found by their position
 * (after `{` or `,` inside an object), and compared decoded, so `"a/b"`, `"a\/b"` and `"a\u002fb"`
 * are one key. Repeats are found in every object, at any depth.
 * @param {string} text JSON that `JSON.parse` accepts
 * @returns {{ record: string|null, key: string }[]} one entry per repeat of a key: `record` is the
 *   top-level key the repeat sits under, or null when the repeated key is itself a top-level one
 */
const duplicateKeys = (text) => {
  const found = []
  /** One set of decoded keys per open object, null per open array. */
  const open = []
  let record = null
  let keyNext = false

  for (const [token] of text.matchAll(TOKEN)) {
    const keys = open[open.length - 1]
    if (token === '{') {
      open.push(new Set())
    } else if (token === '[') {
      open.push(null)
    } else if (token === '}' || token === ']') {
      open.pop()
    } else if (keyNext && keys && token[0] === '"') {
      const key = JSON.parse(token)
      if (open.length === 1) record = key
      if (keys.has(key)) found.push({ record: open.length === 1 ? null : record, key })
      keys.add(key)
    }
    keyNext = token === '{' || token === ','
  }

  return found
}

module.exports = { duplicateKeys }
