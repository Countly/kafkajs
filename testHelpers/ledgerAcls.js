const { createCluster } = require('./index')
const { ClusterAclPort, waitForAclBindings } = require('./aclPropagation')

/**
 * Picks the fields that identify an ACL, from an ACL or a DeleteAcls match.
 * @param {object} acl ACL or match
 * @returns {object} resource, pattern, principal, host, operation and permission
 */
const aclIdentity = ({
  resourceType,
  resourceName,
  resourcePatternType,
  principal,
  host,
  operation,
  permissionType,
}) => ({
  resourceType,
  resourceName,
  resourcePatternType,
  principal,
  host,
  operation,
  permissionType,
})

/**
 * Checks that a DeleteAcls answer to one filter succeeded, in the response and in every match.
 * @param {{filterResponses: object[]}} answer public deleteAcls result
 * @returns {object[]} identities of the ACLs the filter deleted, possibly none
 * @throws {Error} not exactly one filter response, or any error code
 */
const deletedAcls = ({ filterResponses }) => {
  expect(filterResponses).toHaveLength(1)
  expect(filterResponses[0].errorCode).toBe(0)
  for (const match of filterResponses[0].matchingAcls) expect(match.errorCode).toBe(0)
  return filterResponses[0].matchingAcls.map(aclIdentity)
}

/**
 * The admin ledger's ACL lifecycle. Each broker's authorizer applies the controller's ACL records
 * on its own schedule, so `grant` and `revoke` return only once every broker serves the change,
 * and nothing that reads or depends on it can start earlier. The per-broker port is opened once
 * and its close is registered with the probe before the first connect.
 */
class LedgerAcls {
  /**
   * @param {object} probe the scenario's probe: `invoke`, `poll`, `cleanup`, and for the default
   *   port `logger`, `socket` and `baseOptions`
   * @param {object} [options]
   * @param {(label: string|undefined, predicate: () => Promise<boolean>, last: () => any) => Promise<void>} [options.observedPoll]
   *   labelled poll of the public view; defaults to `probe.poll`
   * @param {() => ClusterAclPort} [options.openPort] builds the unconnected port; defaults to one
   *   on the probe's plaintext brokers through its sockets
   */
  constructor(probe, { observedPoll, openPort } = {}) {
    this.probe = probe
    this.observedPoll = observedPoll || ((label, predicate) => probe.poll(predicate))
    this.openPort =
      openPort ||
      (() =>
        new ClusterAclPort(
          createCluster(
            { logger: probe.logger, socketFactory: (args) => probe.socket(args) },
            probe.baseOptions.brokers
          )
        ))
    /** @type {Promise<ClusterAclPort>|null} */
    this.ready = null
  }

  /**
   * Opens the per-broker port once, registering its close before the first connect so an aborted
   * connect cannot leave the cluster without an owner.
   * @returns {Promise<ClusterAclPort>} connected port
   * @throws {Error} connect failure, abort or budget
   */
  port() {
    if (!this.ready) {
      const port = this.openPort()
      this.probe.cleanup.add('ACL port', () => port.close())
      this.ready = this.probe.invoke(() => port.connect()).then(() => port)
    }
    return this.ready
  }

  /**
   * Waits until every broker serves exactly `expected` under `filter`.
   * @param {object} filter complete DescribeAcls filter
   * @param {object[]} expected the bindings every broker must serve, `[]` after a removal
   * @returns {Promise<void>}
   * @throws {Error} a lagging or unanswered broker at the deadline, abort or budget
   */
  async served(filter, expected) {
    const port = await this.port()
    await this.probe.invoke((signal) => waitForAclBindings({ port, filter, expected, signal }))
  }

  /**
   * Creates ACLs in one request, then waits until every broker serves each of them.
   * @param {object} client admin that owns the ACLs
   * @param {object[]} acls complete ACLs
   * @returns {Promise<void>}
   * @throws {Error} a failed create, or a broker that does not serve an ACL in time
   */
  async grant(client, acls) {
    expect(await this.probe.invoke(() => client.createAcls({ acl: acls }))).toBe(true)
    for (const acl of acls) await this.served(acl, [acl])
  }

  /**
   * Deletes ACLs through one filter and proves the outcome: a single successful response that
   * deleted exactly the expected ACLs, after which DescribeAcls finds nothing under the filter,
   * first through the public client and then on every broker.
   * @param {object} client admin that owns the ACLs
   * @param {object} filter complete delete filter, also used to describe what remains
   * @param {object[]} expected the ACLs the filter must delete, whatever order they come back in
   * @param {string} [waitLabel] phase to include with the last public DescribeAcls observation
   * @returns {Promise<void>}
   * @throws {Error} a failed answer, other ACLs deleted, or an ACL that remains until the budget ends
   */
  async revoke(client, filter, expected, waitLabel) {
    const p = this.probe
    const order = (rows) => rows.map((row) => JSON.stringify(aclIdentity(row))).sort()
    const deleted = deletedAcls(await p.invoke(() => client.deleteAcls({ filters: [filter] })))
    expect(order(deleted)).toEqual(order(expected))
    let resources = null
    await this.observedPoll(
      waitLabel,
      async () => {
        ;({ resources } = await p.invoke(() => client.describeAcls(filter)))
        return resources.length === 0
      },
      () => resources
    )
    await this.served(filter, [])
  }
}

module.exports = { LedgerAcls, aclIdentity, deletedAcls }
