const { verifyProtocolAnswers } = require('../shareSoakProtocol')
const { reply } = require('../../soak/__tests__/fixtures/soakReplies')
const recorded = require('./fixtures/shareProduceV13.json')

/** @returns {object[]} Independent copies of the recorded first concurrent send triple. */
const recording = () => JSON.parse(JSON.stringify(recorded.rows))

/**
 * Replaces one real response body with independently encoded upstream fields.
 * @param {object[]} rows Recorded frames and public observations.
 * @param {object} partition Replacement partition response.
 * @param {string} [topicId] Replacement topic UUID.
 * @returns {void}
 */
const answer = (rows, partition, topicId = '7c97b8da-fc88-46d8-9a6a-232a8d644c61') => {
  const request = rows.find((r) => r.kind === 'request' && r.apiKey === 0)
  const response = rows.find((r) => r.kind === 'response' && r.frame === request.frame)
  const bytes = Buffer.concat([
    Buffer.from(response.bytes, 'base64').subarray(0, 9),
    reply('Produce', 13, {
      Responses: [{ TopicId: topicId, PartitionResponses: [partition] }],
    }),
  ])
  bytes.writeInt32BE(bytes.length - 4, 0)
  response.bytes = bytes.toString('base64')
}

/**
 * Re-encodes a recorded partition's request with a valid CRC after a deliberate payload change.
 * @param {object[]} rows Recorded frames and independent public payloads.
 * @param {(messages: object[]) => void} mutate Changes wire records only.
 * @returns {Promise<void>} Replacement request bytes installed.
 */
const records = async (rows, mutate) => {
  const request = rows.find((r) => r.kind === 'request' && r.apiKey === 0)
  const observed = rows.find((r) => r.event === 'soak-producer' && r.partition === 0)
  const messages = observed.messages.map((m) => ({
    ...m,
    key: Buffer.from(m.key, 'hex'),
    value: Buffer.from(m.value, 'hex'),
  }))
  mutate(messages)
  const encoded = await require('../../../src/protocol/requests/produce/v13/request')({
    acks: -1,
    timeout: 30000,
    topicIds: new Map([[observed.topic, '7c97b8da-fc88-46d8-9a6a-232a8d644c61']]),
    topicData: [{ topic: observed.topic, partitions: [{ partition: 0, messages }] }],
  }).encode()
  const { readRequestHeader } = require('../../../testHelpers/fakeSocket')
  const original = Buffer.from(request.bytes, 'base64')
  const bytes = Buffer.concat([
    original.subarray(0, readRequestHeader(original).byteLength),
    encoded.buffer,
  ])
  bytes.writeInt32BE(bytes.length - 4, 0)
  request.bytes = bytes.toString('base64')
}

describe('recorded share Produce evidence', () => {
  test('resolves v13 topic IDs and joins every concurrent send to its own partition and records', async () => {
    const rows = recording()
    expect(
      rows.filter((r) => r.kind === 'request' && r.apiKey === 0).map((r) => r.operationId)
    ).toEqual([196, 196, 196])
    const result = await verifyProtocolAnswers(rows)
    expect(result.problems.filter((p) => p.startsWith('Original'))).toEqual([])
    expect(result.successes.filter((r) => r.apiKey === 0).map((r) => r.apiVersion)).toEqual([
      13, 13, 13,
    ])
  })

  test.each([
    [
      'unknown response topic ID',
      (rows) => answer(rows, { Index: 0, BaseOffset: '0' }, '00000000-0000-4000-8000-000000000099'),
    ],
    ['changed response partition', (rows) => answer(rows, { Index: 99, BaseOffset: '0' })],
    ['changed response offset', (rows) => answer(rows, { Index: 0, BaseOffset: '1' })],
    [
      'changed record value',
      (rows) =>
        records(rows, (messages) => {
          messages[0].value = Buffer.from('changed')
        }),
    ],
    ['changed record order', (rows) => records(rows, (messages) => messages.reverse())],
    [
      'missing wire send',
      (rows) => {
        const request = rows.find((r) => r.kind === 'request' && r.apiKey === 0)
        rows.splice(rows.indexOf(request), 1)
      },
    ],
    [
      'duplicate wire send',
      (rows) => rows.push({ ...rows.find((r) => r.kind === 'request' && r.apiKey === 0) }),
    ],
    [
      'missing public send',
      (rows) => {
        const operation = rows.find(
          (r) => r.kind === 'operation' && r.id === 197 && r.status === 'passed'
        )
        rows.splice(rows.indexOf(operation), 1)
      },
    ],
    [
      'changed observed records',
      (rows) => {
        rows.find((r) => r.event === 'soak-producer').messages[0].value = '00'
      },
    ],
    [
      'request outside its invocation',
      (rows) => {
        const start = rows.find(
          (r) => r.kind === 'operation' && r.id === 196 && r.status === 'started'
        )
        start.at = 9999999
      },
    ],
  ])('rejects %s while the other recorded evidence remains unchanged', async (_name, mutate) => {
    const rows = recording()
    await mutate(rows)
    const result = await verifyProtocolAnswers(rows)
    expect(result.problems.some((p) => p.startsWith('Original'))).toBe(true)
  })

  test('retains internal topics when reconciling the recorded public listTopics result', async () => {
    const rows = JSON.parse(JSON.stringify(recorded.listings))
    const result = await verifyProtocolAnswers(rows)
    expect(result.problems.filter((p) => p.startsWith('Original'))).toEqual([])
  })
})
