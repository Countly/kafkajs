/**
 * Describes a property of an arbitrary thrown value. Access and conversion can run user code,
 * so both stay inside the containment. Unreadable values use a fixed fallback.
 * Called only on failure paths, and never modifies the original value.
 * @param {*} value the original thrown value
 * @param {string} [property='message'] the diagnostic property or classification field
 * @returns {string} text safe to log or interpolate without another conversion
 */
module.exports = function describeThrown(value, property = 'message') {
  try {
    if (value === null || value === undefined) return 'undefined'
    if (typeof value !== 'object' && typeof value !== 'function') {
      return property === 'message' ? String(value) : 'undefined'
    }
    const field = value[property]
    return String(field === undefined && property === 'message' ? value : field)
  } catch {
    return '<unreadable thrown value>'
  }
}
