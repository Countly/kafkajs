/**
 * The assigner objects this package built whose own rule withholds a partition from a new owner
 * until its previous owner has revoked it. Held weakly, so it retains no assigner.
 * @type {WeakSet<object>}
 */
const guaranteed = new WeakSet()

/**
 * Records that an assigner this package built keeps the cooperative ownership rule itself, so the
 * leader need not check its plans. Only the object recorded here is trusted: an assigner from
 * anywhere else, whatever protocol name it offers, is checked.
 * @template {object} A
 * @param {A} assigner
 * @returns {A} the same assigner
 */
const guaranteeCooperativeOwnership = (assigner) => {
  guaranteed.add(assigner)
  return assigner
}

/**
 * @param {object} assigner
 * @returns {boolean} whether `guaranteeCooperativeOwnership` recorded this very object
 */
const guaranteesCooperativeOwnership = (assigner) => guaranteed.has(assigner)

module.exports = { guaranteeCooperativeOwnership, guaranteesCooperativeOwnership }
