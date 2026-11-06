/**
 * Asserts that a share codec call refuses its input with one exact error.
 * The check is on the constructor, not `instanceof`, so a subclass or a sibling error type
 * cannot stand in for the intended one, and on the whole message, so a later unrelated failure
 * of the same type cannot either.
 * @param {Promise<*>} promise the codec call; it must reject
 * @param {Function} ErrorClass the exact constructor of the refusal
 * @param {string} message the exact diagnostic
 * @returns {Promise<void>}
 */
const expectRefusal = async (promise, ErrorClass, message) => {
  const error = await promise.then(
    () => {
      throw new Error('Expected the call to be refused, but it resolved')
    },
    (caught) => caught
  )
  expect(error.constructor).toBe(ErrorClass)
  expect(error.message).toBe(message)
}

module.exports = { expectRefusal }
