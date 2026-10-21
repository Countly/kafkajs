/**
 * A lookup table keyed by names that come from outside: topic names, which may be `__proto__`,
 * `constructor`, `toString` or any other name an ordinary object inherits or treats specially.
 * The object has no prototype, so `table[name]` reads only what was stored under that name and
 * `table[name] = value` stores an own property for every name. `Object.keys`, `Object.hasOwn` and
 * `for…in` work as on any object; methods such as `table.hasOwnProperty` do not exist, and the
 * table is not strictly equal to an ordinary object with the same entries. Use it for internal
 * state only: a value handed to an application is copied into an ordinary object first.
 * @template T
 * @param {Iterable<[string, T]>} [entries] stored in order, a later key replacing an earlier one
 * @returns {Record<string, T>} a new object without a prototype
 */
const dictionary = (entries = []) => {
  const table = Object.create(null)
  for (const [key, value] of entries) {
    table[key] = value
  }
  return table
}

module.exports = dictionary
