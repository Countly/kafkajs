const Encoder = require('../../protocol/encoder')
const { KafkaJSNonRetriableError } = require('../../errors')

/**
 * The protocol names a classic member offers in one JoinGroup, each bound to the assigner that
 * offered it by the name as the request writes it (`Encoder.sentForm`). The coordinator only ever
 * sees and echoes that form, so it is the one identity that can tell two offers apart and name the
 * assigner the coordinator chose. The same name offered twice is one offer, as before: the first
 * assigner wins.
 */
class AssignerOffers {
  /**
   * @param {import('../../../types').Assigner[]} assigners
   * @param {{ name: string }[]} offers what each assigner offers, in the same order; a name that is
   *   not a string is left for the request writer to refuse
   * @throws {KafkaJSNonRetriableError} when two different names are written as the same bytes
   */
  constructor(assigners, offers) {
    /** @type {Map<string, { name: string, assigner: import('../../../types').Assigner }>} */
    this.bySentForm = new Map()
    offers.forEach(({ name }, at) => {
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
   * The offers as configured, before any JoinGroup has asked the assigners for theirs: each
   * assigner stands for the name it declares. Refuses a configuration whose declared names
   * collide, so the mistake surfaces when the consumer is created.
   * @param {import('../../../types').Assigner[]} assigners
   * @returns {AssignerOffers}
   * @throws {KafkaJSNonRetriableError} when two different names are written as the same bytes
   */
  static declared(assigners) {
    return new AssignerOffers(assigners, assigners)
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
