const Encoder = require('../../protocol/encoder')
const { KafkaJSNonRetriableError } = require('../../errors')

/**
 * The protocol names a classic member offers in one JoinGroup, each bound to the assigner that
 * offered it by the name as the request writes it (`Encoder.sentForm`). The coordinator only ever
 * sees and echoes that form, so it is the one identity that can tell two offers apart and name the
 * assigner the coordinator chose. The same name offered twice is one offer, as before: the first
 * assigner wins.
 *
 * One instance is the single snapshot of a join: the rows the request writes, the collision check
 * and the lookup of the selected assigner all read the same captured values. An assigner hands its
 * offer over once, and nothing it keeps and changes afterwards can reach any of the three.
 */
class AssignerOffers {
  /**
   * @param {import('../../../types').Assigner[]} assigners
   * @param {{ name: string, metadata?: Buffer }[]} rows what each assigner offered, in the same
   *   order and already captured; a name that is not a string is left for the request writer to
   *   refuse
   * @throws {KafkaJSNonRetriableError} when two different names are written as the same bytes
   */
  constructor(assigners, rows) {
    /** @type {ReadonlyArray<{ name: string, metadata?: Buffer }>} */
    this.rows = Object.freeze([...rows])
    /** @type {Map<string, { name: string, assigner: import('../../../types').Assigner }>} */
    this.bySentForm = new Map()
    this.rows.forEach(({ name }, at) => {
      if (typeof name !== 'string') return
      const sent = Encoder.sentForm(name)
      const held = this.bySentForm.get(sent)
      if (held === undefined) {
        this.bySentForm.set(sent, { name, assigner: assigners[at] })
      } else if (held.name !== name) {
        throw new KafkaJSNonRetriableError(
          `The partition assigners "${held.name}" and "${name}" are both sent as "${sent}", so the group cannot tell them apart`
        )
      }
    })
  }

  /**
   * Asks each assigner for its offer and takes ownership of what it returns: the name is read and
   * the metadata copied the moment the assigner returns, before anything else runs, so an assigner
   * that reuses its offer object or its metadata buffer cannot change an offer already taken.
   * @param {import('../../../types').Assigner[]} assigners
   * @param {(assigner: import('../../../types').Assigner) => { name: string, metadata?: Buffer }} offerOf
   *   one assigner's offer for this join
   * @returns {AssignerOffers}
   * @throws {KafkaJSNonRetriableError} when two different names are written as the same bytes
   */
  static collect(assigners, offerOf) {
    const rows = assigners.map((assigner) => {
      const { name, metadata } = offerOf(assigner)
      return Object.freeze({
        name,
        metadata: Buffer.isBuffer(metadata) ? Buffer.from(metadata) : metadata,
      })
    })
    return new AssignerOffers(assigners, rows)
  }

  /**
   * The offers as configured, before any JoinGroup has asked the assigners for theirs: each
   * assigner stands for the name it declares. Refuses a configuration whose declared names
   * collide, so the mistake surfaces when the consumer is created. Its rows are names only and are
   * never sent.
   * @param {import('../../../types').Assigner[]} assigners
   * @returns {AssignerOffers}
   * @throws {KafkaJSNonRetriableError} when two different names are written as the same bytes
   */
  static declared(assigners) {
    return new AssignerOffers(
      assigners,
      assigners.map(({ name }) => ({ name }))
    )
  }

  /**
   * @param {string|null} selected the protocol name the coordinator answered with
   * @returns {import('../../../types').Assigner|undefined} the assigner whose offer was sent as
   *   `selected`, or undefined when none was
   */
  assignerFor(selected) {
    return this.bySentForm.get(selected)?.assigner
  }
}

module.exports = AssignerOffers
