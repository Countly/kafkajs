const { flush, shareClient } = require('../../../../testHelpers/shareHarness')

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
