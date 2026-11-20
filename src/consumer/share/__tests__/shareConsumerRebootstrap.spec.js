const { ShareRun } = require('../shareRun')
const {
  fetchAnswer,
  flush,
  part,
  shareClient,
  until,
} = require('../../../../testHelpers/shareHarness')

const REBOOTSTRAP = 'share_consumer.cluster.rebootstrap'
const REBOOTSTRAP_REQUIRED = 129

describe('share consumer rebootstrap event', () => {
  let h
  afterEach(async () => {
    await h?.close()
    h = undefined
    jest.restoreAllMocks()
  })

  /**
   * Fails the first `subscribe` with a top-level REBOOTSTRAP_REQUIRED Metadata answer, ends the
   * outage, and subscribes again, with a failing listener of `kind` registered before an observing
   * one on the same event.
   * @param {'none'|'throw'|'reject'} kind What the failing listener does; `none` registers none.
   * @param {object} [kafka] Overrides of the Kafka options.
   * @returns {Promise<object>} The observed events, the call order, the listener-failure logs, the
   *   Metadata frames written and the outcome of both subscribe calls.
   */
  const run = async (kind, kafka = {}) => {
    h = shareClient({ interval: 5, kafka })
    const { consumer } = h
    const logged = jest.spyOn(consumer.logger(), 'error')
    const seen = []
    const trace = []
    if (kind !== 'none') {
      consumer.on(consumer.events.REBOOTSTRAP, () => {
        trace.push('listener')
        if (kind === 'throw') throw new Error('sync rebootstrap')
        return Promise.reject(new Error('async rebootstrap'))
      })
    }
    consumer.on(consumer.events.REBOOTSTRAP, (event) => {
      trace.push('observer')
      seen.push({ type: event.type, payload: event.payload })
    })
    h.cluster.outage('Metadata', { errorCode: REBOOTSTRAP_REQUIRED })
    const failure = await consumer.subscribe({ topics: ['topic-a'] }).catch((error) => error)
    await flush()
    const during = {
      failure: [failure?.name, failure?.type],
      subscription: consumer.subscription(),
      metadataFrames: h.cluster.requests().filter(({ api }) => api === 'Metadata').length,
    }
    h.cluster.recover('Metadata')
    await consumer.subscribe({ topics: ['topic-a'] })
    await flush()
    const logs = logged.mock.calls
      .filter(([message]) => message.startsWith('Failed to execute listener'))
      .map(([message, meta]) => [message, meta.eventName])
    return { seen, trace, logs, during, after: consumer.subscription() }
  }

  test('publishes one rebootstrap event with its reason when metadata answers REBOOTSTRAP_REQUIRED', async () => {
    const result = await run('none')

    expect(result.seen).toEqual([{ type: REBOOTSTRAP, payload: { reason: 'required' } }])
    expect(result.trace).toEqual(['observer'])
    expect(result.logs).toEqual([])
    expect(result.during).toEqual({
      failure: ['KafkaJSProtocolError', 'REBOOTSTRAP_REQUIRED'],
      subscription: [],
      metadataFrames: 1,
    })
    expect(result.after).toEqual(['topic-a'])
  })

  test.each([
    ['throw', 'sync'],
    ['reject', 'async'],
  ])(
    'contains a %s listener once without changing the recovery or the subscribe outcomes',
    async (kind, word) => {
      const result = await run(kind)

      expect(result.seen).toEqual([{ type: REBOOTSTRAP, payload: { reason: 'required' } }])
      expect(result.trace).toEqual(['listener', 'observer'])
      expect(result.logs).toEqual([
        [`Failed to execute listener: ${word} rebootstrap`, REBOOTSTRAP],
      ])
      expect(result.during).toEqual({
        failure: ['KafkaJSProtocolError', 'REBOOTSTRAP_REQUIRED'],
        subscription: [],
        metadataFrames: 1,
      })
      expect(result.after).toEqual(['topic-a'])
    }
  )

  test('publishes no rebootstrap event when the recovery strategy is none', async () => {
    const result = await run('none', { metadataRecoveryStrategy: 'none' })

    expect(result.seen).toEqual([])
    expect(result.trace).toEqual([])
    expect(result.during).toEqual({
      failure: ['KafkaJSProtocolError', 'REBOOTSTRAP_REQUIRED'],
      subscription: [],
      metadataFrames: 1,
    })
    expect(result.after).toEqual(['topic-a'])
  })
})

describe('share membership across a rebootstrap', () => {
  const UNKNOWN_TOPIC_OR_PARTITION = 3
  let h
  let run

  afterEach(async () => {
    await h?.close()
    h = run = undefined
    jest.restoreAllMocks()
  })

  /** @returns {object[]} The share heartbeats the client wrote, oldest first. */
  const beats = () => h.frames(76)

  /**
   * Runs a joined share consumer, then makes its fetch report the topic gone while metadata
   * answers REBOOTSTRAP_REQUIRED, so the fetch loop's metadata refresh rebootstraps the client
   * and retires every broker the run cached, the coordinator included.
   * @param {number} fenceCode The code the coordinator answers the next written heartbeat with.
   * @returns {Promise<{warn: jest.SpyInstance, token: symbol, beatsBefore: number}>}
   */
  const rebootstrapWhileRunning = async (fenceCode) => {
    const start = ShareRun.prototype.start
    jest.spyOn(ShareRun.prototype, 'start').mockImplementation(function () {
      run = this
      return start.call(this)
    })
    h = shareClient({ interval: 20 })
    const rebootstraps = []
    h.consumer.on(h.consumer.events.REBOOTSTRAP, (event) => rebootstraps.push(event.payload))
    await h.consumer.subscribe({ topics: ['topic-a'] })
    await h.consumer.run({ eachMessage: async () => {} })
    await until(() => run?.membership.memberEpoch === 1, 'the first join')
    await until(() => h.frames(78).length > 0, 'the first fetch')
    const warn = jest.spyOn(run.options.logger, 'warn')
    const coordinator = run.membership.coordinator
    const token = run.membership.token

    h.cluster.outage('Metadata', { errorCode: REBOOTSTRAP_REQUIRED })
    h.script('ShareFetch', [
      fetchAnswer([part({ partition: 0, code: UNKNOWN_TOPIC_OR_PARTITION })]),
    ])
    await until(() => rebootstraps.length > 0, 'the rebootstrap')
    expect(coordinator.broker.retired).toBe(true)
    h.script('ShareGroupHeartbeat', [{ errorCode: fenceCode, memberEpoch: 0 }])
    h.cluster.recover('Metadata')
    return { warn, token, beatsBefore: beats().length }
  }

  test.each([
    ['UNKNOWN_MEMBER_ID', 25],
    ['FENCED_MEMBER_EPOCH', 110],
  ])(
    'looks the coordinator up again after the rebootstrap retired it and rejoins at epoch 0 on %s',
    async (_name, code) => {
      const { token, beatsBefore } = await rebootstrapWhileRunning(code)

      await until(
        () => beats().some(({ fields }, i) => i >= beatsBefore && fields.memberEpoch === 0),
        'the rejoin at epoch 0'
      )
      await until(() => run.membership.memberEpoch === 2, 'the second join')
      const after = beats().slice(beatsBefore)
      expect(after[0].fields.memberEpoch).toBe(1)
      expect(after[1].fields).toMatchObject({
        memberEpoch: 0,
        memberId: run.membership.memberId,
        subscribedTopicNames: ['topic-a'],
      })
      expect(run.membership.token).not.toBe(token)
      expect(run.membership.coordinator.broker.retired).toBe(false)
      const fetches = h.frames(78).length
      await until(() => h.frames(78).length > fetches, 'fetching after the rejoin')
    }
  )

  test('logs the name, type and code of each failing share heartbeat', async () => {
    const { warn } = await rebootstrapWhileRunning(14)

    await until(() => run.membership.coordinator?.broker.retired === false, 'the new coordinator')
    await until(
      () => warn.mock.calls.some(([, extra]) => extra?.code === 14),
      'the logged broker answer'
    )
    const failures = warn.mock.calls
      .filter(([message]) => message === 'Share heartbeat failed')
      .map(([, extra]) => extra)
    expect(failures[0]).toEqual({
      error: 'This broker connection was replaced',
      name: 'KafkaJSConnectionError',
      type: undefined,
      code: undefined,
    })
    expect(failures).toContainEqual({
      error: expect.any(String),
      name: 'KafkaJSProtocolError',
      type: 'GROUP_LOAD_IN_PROGRESS',
      code: 14,
    })
  })
})
