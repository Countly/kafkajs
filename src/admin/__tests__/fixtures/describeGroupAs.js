const Broker = require('../../../broker')
const { KafkaJSServerDoesNotSupportApiKey } = require('../../../errors')

/** What the broker sends when authorized operations were not requested. */
const NO_AUTHORIZED_OPERATIONS = -2147483648

/**
 * Answers the admin's group description below the describer: the scripted cluster has no
 * ConsumerGroupDescribe or DescribeGroups pair, so the two broker calls are replaced and the real
 * describer routes their rows (a ConsumerGroupDescribe row with code 69 or 35, or a broker without
 * that API, falls back to DescribeGroups).
 * @param {object} rows
 * @param {object|null} [rows.consumer] fields of the ConsumerGroupDescribe row over a missing
 *   group's (code 69); null for a broker that does not offer ConsumerGroupDescribe
 * @param {object} [rows.classic] fields of the DescribeGroups row over an empty classic group's
 * @param {(groupId: string) => string} [rows.echo] the group id a row names for a requested one;
 *   a broker echoes the bytes it read, so the default is the id as requested
 * @returns {{ calls: string[], restore: () => void }} the describe APIs called, in order
 */
const describeGroupAs = ({ consumer = {}, classic = {}, echo = (groupId) => groupId } = {}) => {
  const calls = []
  const consumerSpy = jest
    .spyOn(Broker.prototype, 'consumerGroupDescribe')
    .mockImplementation(async ({ groupIds }) => {
      calls.push('ConsumerGroupDescribe')
      if (consumer === null) {
        throw new KafkaJSServerDoesNotSupportApiKey('ConsumerGroupDescribe is not offered')
      }
      return {
        groups: groupIds.map((groupId) => ({
          errorCode: 69,
          errorMessage: null,
          groupId: echo(groupId),
          groupState: '',
          groupEpoch: -1,
          assignmentEpoch: -1,
          assignorName: '',
          members: [],
          authorizedOperations: NO_AUTHORIZED_OPERATIONS,
          ...consumer,
        })),
      }
    })
  const classicSpy = jest
    .spyOn(Broker.prototype, 'describeGroups')
    .mockImplementation(async ({ groupIds }) => {
      calls.push('DescribeGroups')
      return {
        groups: groupIds.map((groupId) => ({
          errorCode: 0,
          errorMessage: null,
          groupId: echo(groupId),
          state: 'Empty',
          protocolType: 'consumer',
          protocol: '',
          members: [],
          authorizedOperations: NO_AUTHORIZED_OPERATIONS,
          ...classic,
        })),
      }
    })

  return {
    calls,
    restore: () => {
      consumerSpy.mockRestore()
      classicSpy.mockRestore()
    },
  }
}

module.exports = { describeGroupAs }
