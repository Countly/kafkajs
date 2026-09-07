/**
 * The order every built-in assigner sorts its members by.
 *
 * A static member sorts before a dynamic one, and two static members are ordered by their
 * instance ids: that is what makes a static member's assignment stable across a restart, since
 * its member id changes and its instance id does not.
 *
 * @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractPartitionAssignor.java
 * @param {{ memberId: string, groupInstanceId?: string|null }} a
 * @param {{ memberId: string, groupInstanceId?: string|null }} b
 * @returns {number} negative when `a` sorts first
 */
const compareMembers = (a, b) => {
  const aInstance = a.groupInstanceId == null ? null : a.groupInstanceId
  const bInstance = b.groupInstanceId == null ? null : b.groupInstanceId

  if (aInstance !== null && bInstance !== null) {
    return aInstance < bInstance ? -1 : aInstance > bInstance ? 1 : 0
  }

  if (aInstance !== null) {
    return -1
  }

  if (bInstance !== null) {
    return 1
  }

  return a.memberId < b.memberId ? -1 : a.memberId > b.memberId ? 1 : 0
}

module.exports = { compareMembers }
