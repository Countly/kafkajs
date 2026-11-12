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
 * @param {object[]} [extraTopics] Additional response topics, including empty ones.
 * @returns {void}
 */
const answer = (
  rows,
  partition,
  topicId = '7c97b8da-fc88-46d8-9a6a-232a8d644c61',
  extraTopics = []
) => {
  const request = rows.find((r) => r.kind === 'request' && r.apiKey === 0)
  const response = rows.find((r) => r.kind === 'response' && r.frame === request.frame)
  const bytes = Buffer.concat([
    Buffer.from(response.bytes, 'base64').subarray(0, 9),
    reply('Produce', 13, {
      Responses: [{ TopicId: topicId, PartitionResponses: [partition] }, ...extraTopics],
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

/**
 * Adds an independently encoded Metadata binding without changing the valid Produce topic.
 * @param {object[]} rows Recorded frames.
 * @returns {void} The additional UUID can resolve, but no request asks to produce to it.
 */
const knownExtraTopic = (rows) => {
  const request = rows.find((r) => r.kind === 'request' && r.apiKey === 3)
  const response = rows.find((r) => r.kind === 'response' && r.frame === request.frame)
  const bytes = Buffer.concat([
    Buffer.from(response.bytes, 'base64').subarray(0, 9),
    reply('Metadata', 13, {
      Topics: [
        {
          Name: 'soak-share-prime-f388dcf8-16b3-47c8-a37f-d83f98b49404',
          TopicId: '7c97b8da-fc88-46d8-9a6a-232a8d644c61',
        },
        { Name: 'unrequested-topic', TopicId: '00000000-0000-4000-8000-000000000099' },
      ],
    }),
  ])
  bytes.writeInt32BE(bytes.length - 4, 0)
  response.bytes = bytes.toString('base64')
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
      'Produce topic lacks matching request, response and metadata',
    ],
    [
      'changed response partition',
      (rows) => answer(rows, { Index: 99, BaseOffset: '0' }),
      'Produce response partition differs',
    ],
    [
      'changed response offset',
      (rows) => answer(rows, { Index: 0, BaseOffset: '1' }),
      'Produce partition lacks one original invocation and corpus',
    ],
    [
      'changed record value',
      (rows) =>
        records(rows, (messages) => {
          messages[0].value = Buffer.from('changed')
        }),
      'Decoded Produce records differ',
    ],
    [
      'changed record order',
      (rows) => records(rows, (messages) => messages.reverse()),
      'Decoded Produce records differ',
    ],
    [
      'missing wire send',
      (rows) => {
        const request = rows.find((r) => r.kind === 'request' && r.apiKey === 0)
        rows.splice(rows.indexOf(request), 1)
      },
      'Original Produce corpus lacks unique wire coverage',
    ],
    [
      'duplicate wire send',
      (rows) => rows.push({ ...rows.find((r) => r.kind === 'request' && r.apiKey === 0) }),
      'Produce partition reused original send evidence',
    ],
    [
      'missing public send',
      (rows) => {
        const operation = rows.find(
          (r) => r.kind === 'operation' && r.id === 197 && r.status === 'passed'
        )
        rows.splice(rows.indexOf(operation), 1)
      },
      'Produce partition lacks one original invocation and corpus',
      1,
    ],
    [
      'changed observed records',
      (rows) => {
        rows.find((r) => r.event === 'soak-producer').messages[0].value = '00'
      },
      'Decoded Produce records differ',
      1,
    ],
    [
      'request outside its invocation',
      (rows) => {
        const start = rows.find(
          (r) => r.kind === 'operation' && r.id === 196 && r.status === 'started'
        )
        start.at = 9999999
      },
      'Produce partition lacks one original invocation and corpus',
    ],
    ...[
      [
        'wrong public acknowledgement topic name',
        (rows) => {
          rows.find(
            (r) => r.kind === 'operation' && r.status === 'passed' && r.id === 197
          ).result[0].topicName = 'wrong-topic'
        },
        'Public Produce acknowledgement topic differs from metadata',
      ],
      [
        'wrong observed acknowledgement topic name',
        (rows) => {
          rows.find((r) => r.event === 'soak-producer' && r.partition === 1).sent[0].topicName =
            'wrong-topic'
        },
        'Observed Produce acknowledgement topic differs from metadata',
      ],
      [
        'missing public acknowledgement topic name',
        (rows) => {
          delete rows.find((r) => r.kind === 'operation' && r.status === 'passed' && r.id === 197)
            .result[0].topicName
        },
        'Public Produce acknowledgement topic differs from metadata',
      ],
      [
        'missing observed acknowledgement topic name',
        (rows) => {
          delete rows.find((r) => r.event === 'soak-producer' && r.partition === 1).sent[0]
            .topicName
        },
        'Observed Produce acknowledgement topic differs from metadata',
      ],
    ].map(([name, mutate, reason]) => [name, mutate, reason, 1]),
    [
      'unknown empty response topic',
      (rows) => {
        answer(rows, { Index: 0, BaseOffset: '0' }, undefined, [
          { TopicId: '00000000-0000-4000-8000-000000000099', PartitionResponses: [] },
        ])
      },
      'Produce response topic inventory differs from request and metadata',
    ],
    [
      'unrequested known empty response topic',
      (rows) => {
        knownExtraTopic(rows)
        answer(rows, { Index: 0, BaseOffset: '0' }, undefined, [
          { TopicId: '00000000-0000-4000-8000-000000000099', PartitionResponses: [] },
        ])
      },
      'Produce response topic inventory differs from request and metadata',
    ],
    [
      'duplicate empty response topic',
      (rows) => {
        answer(rows, { Index: 0, BaseOffset: '0' }, undefined, [
          { TopicId: '7c97b8da-fc88-46d8-9a6a-232a8d644c61', PartitionResponses: [] },
        ])
      },
      'Produce topic lacks matching request, response and metadata',
    ],
  ])(
    'rejects %s while the other recorded evidence remains unchanged',
    async (_name, mutate, reason, partition = 0) => {
      const rows = recording()
      const pristine = await verifyProtocolAnswers(rows)
      expect(pristine.problems.filter((p) => p.startsWith('Original'))).toEqual([])
      const request = rows.filter((r) => r.kind === 'request' && r.apiKey === 0)[partition]
      const expected = reason.startsWith('Original ')
        ? reason
        : `Original Produce ${request.socket}:${request.frame}: ${reason}`
      await mutate(rows)
      const result = await verifyProtocolAnswers(rows)
      expect(result.problems).toContain(expected)
    }
  )

  test('retains internal topics when reconciling the recorded public listTopics result', async () => {
    const rows = JSON.parse(JSON.stringify(recorded.listings))
    const result = await verifyProtocolAnswers(rows)
    expect(result.problems.filter((p) => p.startsWith('Original'))).toEqual([])
  })
})
