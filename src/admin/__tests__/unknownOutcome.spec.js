const apiKeys = require('../../protocol/requests/apiKeys')
const {
  itemsOf,
  isMutation,
  owned,
  SINGLE_SEND,
  resendable,
  mayResend,
} = require('../unknownOutcome')

/** Builds just enough of the private request to test identity extraction. */
const items = (api, args) => itemsOf({ request: { apiKey: apiKeys[api] }, args: owned(args) }, 7)

it('names the items of the six multi-send APIs and none for the single-send APIs', () => {
  const resources = [{ type: 2, name: 't' }]
  expect(items('InitProducerId', { transactionalId: 'tx' })).toEqual(['tx'])
  expect(items('DeleteGroups', ['g'])).toEqual(['g'])
  expect(
    items('DeleteRecords', { topics: [{ topic: 't', partitions: [{ partition: 0 }] }] })
  ).toEqual([{ topic: 't', partition: 0 }])
  expect(items('AlterConfigs', { resources })).toEqual(resources)
  expect(items('IncrementalAlterConfigs', { resources })).toEqual(resources)
  expect(
    items('AlterReplicaLogDirs', { dirs: [{ topics: [{ topic: 't', partitions: [1] }] }] })
  ).toEqual([{ brokerId: 7, topic: 't', partition: 1 }])
  expect(SINGLE_SEND.size).toBe(17)
  for (const apiKey of SINGLE_SEND) expect(itemsOf({ request: { apiKey }, args: {} })).toBeNull()
})

it('a class-instance entry, an ill-formed string or an out-of-width integer makes the request unnamed', () => {
  class Resource {
    type = 2
    name = 't'
  }

  expect(items('AlterConfigs', { resources: [new Resource()] })).toBeNull()
  expect(items('AlterConfigs', { resources: [{ type: 128, name: 't' }] })).toBeNull()
  expect(items('InitProducerId', { transactionalId: '\ud800' })).toBeNull()
  expect(
    items('DeleteRecords', { topics: [{ topic: 't', partitions: [{ partition: 2147483648 }] }] })
  ).toBeNull()
})

it('only refusals made before applying let a call send again', () => {
  const rows = (...codes) => ({
    topicErrors: codes.map(([topic, errorCode]) => ({ topic, topicId: `id-${topic}`, errorCode })),
  })
  expect(resendable(apiKeys.DeleteTopics, rows(['a', 0], ['t', 89]))).toEqual(new Set(['id-t']))
  expect(resendable(apiKeys.DeleteTopics, rows(['a', 3], ['t', 89]))).toBeNull()
  expect(resendable(apiKeys.DeleteTopics, rows(['a', 0], ['t', 0]))).toBeNull()
  for (const code of [7, 41]) {
    expect(resendable(apiKeys.DeleteTopics, rows(['t', code]))).toBeNull()
  }
  for (const api of ['OffsetDelete', 'LeaveGroup']) {
    expect(resendable(apiKeys[api], { errorCode: 14 })).toBe(true)
    for (const errorCode of [0, 7, 15, 16, 41]) {
      expect(resendable(apiKeys[api], { errorCode })).toBeNull()
    }
  }
  expect(resendable(apiKeys.LeaveGroup, { errorCode: 0, members: [{ errorCode: 14 }] })).toBeNull()
  for (const api of [
    'DeleteGroups',
    'AlterShareGroupOffsets',
    'DeleteShareGroupOffsets',
    'InitProducerId',
    'AlterPartitionReassignments',
  ]) {
    expect(resendable(apiKeys[api], { errorCode: 14 })).toBeNull()
    expect(resendable(apiKeys[api], { errorCode: 89 })).toBeNull()
  }

  expect(mayResend(true, {})).toBe(true)
  expect(mayResend(null, { topicIds: ['id-t'] })).toBe(false)
})

it('a DeleteTopics quota refusal permits a resend only by the topic ids it refused', () => {
  const refusal = (...rows) => ({
    topicErrors: rows.map(([topic, topicId, errorCode]) => ({ topic, topicId, errorCode })),
  })
  const refused = resendable(apiKeys.DeleteTopics, refusal(['a', 'id-a', 0], ['t', 'id-t', 89]))
  expect(refused).toEqual(new Set(['id-t']))
  expect(mayResend(refused, { topicIds: ['id-t'] })).toBe(true)
  expect(mayResend(refused, { topics: ['t'] })).toBe(false)
  expect(mayResend(refused, { topicIds: ['id-t', 'id-a'] })).toBe(false)
  expect(mayResend(refused, { topicIds: [] })).toBe(false)
  for (const topicId of [null, undefined]) {
    expect(resendable(apiKeys.DeleteTopics, refusal(['t', topicId, 89]))).toBeNull()
  }
  expect(resendable(apiKeys.DeleteTopics, refusal(['t', 'id-t', 0], ['t', 'id-x', 89]))).toBeNull()
  expect(resendable(apiKeys.DeleteTopics, refusal(['a', 'id-t', 0], ['t', 'id-t', 89]))).toBeNull()
})

it('validateOnly exempts a request only when its version encodes the flag', () => {
  for (const api of ['AlterConfigs', 'IncrementalAlterConfigs', 'AlterClientQuotas']) {
    expect(isMutation(apiKeys[api], 0, { validateOnly: true })).toBe(false)
    expect(isMutation(apiKeys[api], 0, { validateOnly: false })).toBe(true)
  }
  expect(isMutation(apiKeys.UpdateFeatures, 0, { validateOnly: true })).toBe(true)
  expect(isMutation(apiKeys.UpdateFeatures, 1, { validateOnly: true })).toBe(false)
  expect(isMutation(apiKeys.OffsetCommit, 9, {})).toBe(false)
  expect(isMutation(apiKeys.CreateTopics, 7, {})).toBe(false)
  expect(isMutation(apiKeys.CreatePartitions, 3, {})).toBe(false)
})
