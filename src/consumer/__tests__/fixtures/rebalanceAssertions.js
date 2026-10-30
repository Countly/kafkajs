const ASSERTED = ['classic', 'consumer']

/** Pure assertion and outage-boundary rules shared by the bounce scenario and broker-free controls. */
class RebalanceAssertions {
  /**
   * @param {Record<string, {kind: string, violation: boolean}>} verdicts one arm's group verdicts
   * @returns {[string, object]|undefined} the first short-arm violation, including consumer protocol
   */
  static shortArmStop(verdicts) {
    return Object.entries(verdicts).find(
      ([group, { kind, violation }]) => ASSERTED.includes(group) && kind === 'short' && violation
    )
  }

  /**
   * @param {Record<string, {rebalances: object[]}>} verdict all arms for each group
   * @returns {object[]} every asserted violation, preserving group and arm order
   */
  static violations(verdict) {
    return ASSERTED.flatMap((group) =>
      verdict[group].rebalances.filter(({ violation }) => violation)
    )
  }

  /**
   * Includes events while the foreground stop command is still running in the outage window.
   * @param {{tDown: number, tRestored: number}} outage the bouncer's returned timestamps
   * @param {number} issuedAt issue time, captured before awaiting the command
   * @returns {object} corrected window plus its independently recorded command-return boundary
   * @throws {Error} when timestamps cannot describe the ordered command and restore
   */
  static issueWindow(outage, issuedAt) {
    if (issuedAt > outage.tDown || outage.tDown > outage.tRestored)
      throw new Error('Invalid outage timestamps')
    return {
      ...outage,
      commandReturnedAt: outage.tDown,
      tDown: issuedAt,
      outageMs: outage.tRestored - issuedAt,
    }
  }
}
module.exports = RebalanceAssertions
