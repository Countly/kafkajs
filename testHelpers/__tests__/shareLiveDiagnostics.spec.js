const {
  terminalSharePosition,
  ShareAcknowledgementObserver,
  assertQuietTerminalGroup,
} = require('../shareLiveDiagnostics')
const fixtures = require('../shareFixtures')

const TOPIC_ID = '12345678-1234-1234-1234-123456789abc'

/** @returns {object} Independent end and agreeing persisted observations. */
const observation = () => ({
  topicId: TOPIC_ID,
  ends: [{ partition: 0, high: '201' }],
  named: [
    {
      topic: 'records',
      topicId: TOPIC_ID,
      partitions: [{ partition: 0, startOffset: '200', lag: '0', errorCode: 0, leaderEpoch: 7 }],
    },
  ],
  all: [
    {
      topic: 'records',
      topicId: TOPIC_ID,
      partitions: [{ partition: 0, startOffset: '200', lag: '0', errorCode: 0, leaderEpoch: 7 }],
    },
  ],
})
const target = {
  groupId: 'group',
  topic: 'records',
  partitions: [{ partition: 0, floor: '0', end: '201' }],
}

/** @param {object} value Observation. @returns {object[]} Both owned partition rows. */
const rows = (value) => [value.named[0].partitions[0], value.all[0].partitions[0]]

describe('terminal share position', () => {
  it('accepts marker 200 below independent end 201 with known zero lag', () => {
    expect(terminalSharePosition(target, observation()).settled).toBe(true)
  })

  it('accepts the delivery start 100 below independent end 200 with known zero lag', () => {
    const value = observation()
    rows(value).forEach((row) => {
      row.startOffset = '100'
    })
    value.ends[0].high = '200'
    expect(
      terminalSharePosition(
        { ...target, partitions: [{ partition: 0, floor: '0', end: '200' }] },
        value
      ).settled
    ).toBe(true)
  })

  it('accepts a trailing start at the initial floor and preserves decimal precision', () => {
    const value = observation()
    const end = '9007199254740995'
    rows(value).forEach((row) => {
      row.startOffset = '9007199254740993'
    })
    value.ends[0].high = end
    expect(
      terminalSharePosition(
        { ...target, partitions: [{ partition: 0, floor: '9007199254740993', end }] },
        value
      ).settled
    ).toBe(true)
  })

  it.each([null, undefined, '-1', '-2', '1', '01', 0])('rejects terminal lag %p', (lag) => {
    const value = observation()
    rows(value).forEach((row) => {
      row.lag = lag
    })
    expect(terminalSharePosition(target, value).settled).toBe(false)
  })

  it.each([null, undefined, '-1', '202', '0200', '+200', '2e2', '', 200])(
    'rejects invalid or out-of-range start %p',
    (startOffset) => {
      const value = observation()
      rows(value).forEach((row) => {
        row.startOffset = startOffset
      })
      expect(terminalSharePosition(target, value).settled).toBe(false)
    }
  )

  it('rejects a start below the altered floor', () => {
    const value = observation()
    rows(value).forEach((row) => {
      row.startOffset = '7'
    })
    expect(
      terminalSharePosition(
        { ...target, partitions: [{ partition: 0, floor: '8', end: '201' }] },
        value
      ).settled
    ).toBe(false)
  })

  it.each(['startOffset', 'lag', 'errorCode', 'leaderEpoch'])(
    'rejects disagreement in %s',
    (field) => {
      const value = observation()
      value.all[0].partitions[0][field] = field === 'startOffset' ? '199' : 1
      expect(terminalSharePosition(target, value).settled).toBe(false)
    }
  )

  it.each(['named', 'all'])('rejects missing and duplicate rows and wrong UUIDs in %s', (form) => {
    for (const change of ['missing', 'duplicate', 'uuid', 'error', 'topic', 'alias']) {
      const value = observation()
      const topic = value[form][0]
      if (change === 'missing') topic.partitions = []
      if (change === 'duplicate') topic.partitions.push({ ...topic.partitions[0] })
      if (change === 'uuid') topic.topicId = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'
      if (change === 'error') topic.partitions[0].errorCode = 6
      if (change === 'topic') value[form].push({ ...topic })
      if (change === 'alias') value[form].push({ ...topic, topic: 'alias' })
      expect(terminalSharePosition(target, value).settled).toBe(false)
    }
  })

  it('rejects matching wrong UUIDs and group-level failures', () => {
    const value = observation()
    value.named[0].topicId = value.all[0].topicId = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'
    expect(terminalSharePosition(target, value).settled).toBe(false)
    expect(terminalSharePosition(target, { ...observation(), error: { code: 15 } }).settled).toBe(
      false
    )
  })

  it('rejects the data count 200 as an independent end for the marker corpus', () => {
    const value = observation()
    value.ends[0].high = '200'
    expect(terminalSharePosition(target, value).settled).toBe(false)
  })

  it('compares partition sets without depending on their order', () => {
    const value = observation()
    for (const form of ['named', 'all']) {
      value[form][0].partitions.push({ ...value[form][0].partitions[0], partition: 1 })
    }
    value.all[0].partitions.reverse()
    value.ends.push({ partition: 1, high: '201' })
    expect(
      terminalSharePosition(
        { ...target, partitions: [...target.partitions, { partition: 1, floor: '0', end: '201' }] },
        value
      ).settled
    ).toBe(true)
  })
})

describe('successful acknowledgement coverage', () => {
  /** @returns {ShareAcknowledgementObserver} An original request and its partition response. */
  const observer = () => {
    const observed = new ShareAcknowledgementObserver()
    observed.exchanges.push({
      apiKey: 79,
      request: {
        groupId: 'group',
        isRenewAck: false,
        topics: [
          {
            topicId: TOPIC_ID,
            partitions: [
              {
                partitionIndex: 0,
                acknowledgementBatches: [
                  { firstOffset: '0', lastOffset: '0', acknowledgeTypes: [1] },
                  { firstOffset: '200', lastOffset: '200', acknowledgeTypes: [0] },
                ],
              },
            ],
          },
        ],
      },
      response: {
        errorCode: 0,
        responses: [{ topicId: TOPIC_ID, partitions: [{ partitionIndex: 0, errorCode: 0 }] }],
      },
    })
    return observed
  }

  it('requires successful original Accept and marker Gap results', () => {
    expect([...observer().types('group', TOPIC_ID, 0)]).toEqual([
      ['0', [1]],
      ['200', [0]],
    ])
    for (const failure of [
      'lost',
      'failed',
      'group-error',
      'duplicate',
      'missing',
      'renew',
      'wrong-group',
      'wrong-uuid',
    ]) {
      const observed = observer()
      const exchange = observed.exchanges[0]
      if (failure === 'lost') delete exchange.response
      if (failure === 'failed') exchange.response.responses[0].partitions[0].errorCode = 6
      if (failure === 'group-error') exchange.response.errorCode = 122
      if (failure === 'duplicate')
        exchange.response.responses[0].partitions.push({ partitionIndex: 0, errorCode: 0 })
      if (failure === 'missing') exchange.response.responses[0].partitions = []
      if (failure === 'renew') exchange.request.isRenewAck = true
      if (failure === 'wrong-group') exchange.request.groupId = 'other'
      if (failure === 'wrong-uuid') exchange.response.responses[0].topicId = 'other'
      expect([...observed.types('group', TOPIC_ID, 0)]).toEqual([])
    }
    const fetch = observer()
    fetch.exchanges[0].apiKey = 78
    expect([...fetch.types('group', TOPIC_ID, 0)]).toEqual([])
    const result = fetch.exchanges[0].response.responses[0].partitions[0]
    result.acknowledgeErrorCode = 121
    expect([...fetch.types('group', TOPIC_ID, 0)]).toEqual([])
    result.acknowledgeErrorCode = 0
    expect(fetch.types('group', TOPIC_ID, 0).get('200')).toEqual([0])
    const observed = observer()
    observed.exchanges[0].request.topics[0].partitions[0].acknowledgementBatches.pop()
    expect(observed.types('group', TOPIC_ID, 0).has('200')).toBe(false)
  })
})

it('fails quiet verification on any delivered identity', async () => {
  const verify = jest
    .spyOn(fixtures, 'verifierMember')
    .mockResolvedValue([{ identity: 'unexpected', deliveryCount: 2 }])
  try {
    await expect(
      assertQuietTerminalGroup({
        kafka: {},
        admin: {},
        ...target,
        signal: new AbortController().signal,
        timeoutMs: 30000,
      })
    ).rejects.toThrow('unexpected')
  } finally {
    verify.mockRestore()
  }
})
