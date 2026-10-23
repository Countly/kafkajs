/** Private identities of partitioners created by the built-in implementations. */
const builtIns = new WeakSet()

/**
 * Registers one built-in instance or its internal instrumentation wrapper.
 * @param {Function} partitioner
 * @returns {Function} the same function
 */
const register = (partitioner) => {
  builtIns.add(partitioner)
  return partitioner
}

/**
 * Attaches the private producer context only to a registered built-in. A custom function's
 * properties are never inspected, including an unrelated or non-callable `attach` property.
 * @param {Function} partitioner
 * @param {{ loadOf: Function, rackOf: Function }} context
 * @returns {void}
 */
const attach = (partitioner, context) => {
  if (builtIns.has(partitioner)) partitioner.attach(context)
}

/**
 * Instruments calls while preserving a built-in's private identity and attachment target.
 * @param {Function} partitioner
 * @param {() => void} called runs once per record
 * @returns {Function} a wrapper, eligible for attachment only when the original was eligible
 */
const observe = (partitioner, called) => {
  const wrapper = (args) => {
    called()
    return partitioner(args)
  }
  if (builtIns.has(partitioner)) {
    wrapper.attach = (context) => partitioner.attach(context)
    register(wrapper)
  }
  return wrapper
}

module.exports = { register, attach, observe }
