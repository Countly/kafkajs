/**
 * Groups `array` by what `groupFn` answers for each item, in one pass.
 * @template T, K
 * @param {T[]} array
 * @param {(item: T) => K | Promise<K>} groupFn
 * @returns {Promise<Map<K, T[]>>} the groups in order of first appearance, each holding its items
 *   in input order
 */
module.exports = async (array, groupFn) => {
  const result = new Map()

  for (const item of array) {
    const group = await Promise.resolve(groupFn(item))
    const items = result.get(group)

    if (items) {
      items.push(item)
    } else {
      result.set(group, [item])
    }
  }

  return result
}
