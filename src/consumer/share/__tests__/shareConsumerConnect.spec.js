const {
  A,
  deferred,
  flush,
  shareClient,
  topError,
  until,
} = require('../../../../testHelpers/shareHarness')
const {
  KafkaJSNonRetriableError,
  KafkaJSServerDoesNotSupportApiKey,
  KafkaJSShareGroupsNotSupportedError,
} = require('../../../errors')

const FATAL_JOIN_CODES = [30, 29, 81, 42, 35]

describe('public share connection contract', () => {
  let clients = []
  /** @param {object} [options] Options of `shareClient`. @returns {object} A tracked client. */
  const open = (options) => {
    const h = shareClient(options)
    clients.push(h)
    return h
  }
  /** @param {object} h A client. @returns {{crashes: object[], joins: object[]}} Observed events. */
  const observe = (h) => {
    const seen = { crashes: [], joins: [], connects: [] }
    h.consumer.on(h.consumer.events.CRASH, ({ payload }) => seen.crashes.push(payload))
    h.consumer.on(h.consumer.events.GROUP_JOIN, ({ payload }) => seen.joins.push(payload))
    h.consumer.on(h.consumer.events.CONNECT, () => seen.connects.push(true))
    return seen
  }
  /** @param {object} h A client. @returns {Promise<void>} Subscribes to the one scripted topic. */
  const subscribe = (h) => h.consumer.subscribe({ topics: ['topic-a'] })
  /** @param {object} h A client. @param {object} [more] Handler overrides. @returns {Promise<void>} */
  const run = (h, more) => h.consumer.run({ eachMessage: async () => {}, ...more })
  /** @param {object} h A client. @param {object} features Replacement finalized-feature fields. */
  const advertise = (h, features) =>
    h.cluster.answer('ApiVersions', () => ({ ...h.cluster.apiVersionsAnswer(), ...features }))

  afterEach(async () => {
    const closing = clients
    clients = []
    for (const h of closing) await h.close()
  })

  test('refuses missing share APIs before sending a share frame', async () => {
    const h = open({ profile: 'default' })
    const error = await h.consumer.connect().catch((e) => e)
    expect(error).toBeInstanceOf(KafkaJSShareGroupsNotSupportedError)
    expect(error).toBeInstanceOf(KafkaJSServerDoesNotSupportApiKey)
    expect(error).toMatchObject({ reason: 'api-not-advertised', apiKey: 76, retriable: false })
    expect(h.frames(76, 78, 79)).toHaveLength(0)
    const control = open()
    await expect(control.consumer.connect()).resolves.toBeUndefined()
  })

  test('refuses a preview-only fetch version before sending a share frame', async () => {
    const h = open({ advertise: { ShareFetch: [1, 1] } })
    const error = await h.consumer.connect().catch((e) => e)
    expect(error).toBeInstanceOf(KafkaJSShareGroupsNotSupportedError)
    expect(error).toMatchObject({
      reason: 'api-version-range',
      apiKey: 78,
      brokerRange: [1, 1],
      clientRange: [2, 2],
    })
    expect(h.frames(76, 78, 79)).toHaveLength(0)
  })

  test('checks finalized share support after API versions', async () => {
    const missing = open()
    advertise(missing, { finalizedFeatures: [], finalizedFeaturesEpoch: 7 })
    await expect(missing.consumer.connect()).rejects.toMatchObject({ reason: 'feature-disabled' })
    const disabled = open()
    advertise(disabled, {
      finalizedFeatures: [{ name: 'share.version', minVersionLevel: 0, maxVersionLevel: 0 }],
    })
    await expect(disabled.consumer.connect()).rejects.toMatchObject({
      reason: 'feature-disabled',
      shareVersion: 0,
    })
    const noApi = open({ profile: 'default' })
    advertise(noApi, { finalizedFeatures: [], finalizedFeaturesEpoch: 7 })
    await expect(noApi.consumer.connect()).rejects.toMatchObject({ reason: 'api-not-advertised' })
    const badRange = open({ advertise: { ShareFetch: [1, 1] } })
    advertise(badRange, { finalizedFeatures: [], finalizedFeaturesEpoch: 7 })
    await expect(badRange.consumer.connect()).rejects.toMatchObject({ reason: 'api-version-range' })
    for (const h of [missing, disabled, noApi, badRange])
      expect(h.frames(76, 78, 79)).toHaveLength(0)
    await expect(open().consumer.connect()).resolves.toBeUndefined()
  })

  test('defers unknown finalized features to the runtime answer', async () => {
    const restartOnFailure = jest.fn(async () => true)
    const h = open({ share: { retry: { restartOnFailure } } })
    advertise(h, { finalizedFeatures: [], finalizedFeaturesEpoch: -1 })
    const seen = observe(h)
    await subscribe(h)
    h.script('ShareGroupHeartbeat', [{ errorCode: 35 }])
    const error = await run(h).catch((e) => e)
    expect(error).toBeInstanceOf(KafkaJSShareGroupsNotSupportedError)
    expect(error).toMatchObject({ reason: 'refused-by-broker', apiKey: 76 })
    await until(() => seen.crashes.length === 1, 'the crash event')
    expect(seen.crashes[0].error).toBe(error)
    expect(seen.crashes[0].restart).toBe(false)
    expect(restartOnFailure).toHaveBeenCalledTimes(1)
    expect(restartOnFailure).toHaveBeenCalledWith(error)

    const fetching = open({ share: { retry: { restartOnFailure } } })
    advertise(fetching, { finalizedFeatures: [], finalizedFeaturesEpoch: -1 })
    const second = observe(fetching)
    await subscribe(fetching)
    fetching.script('ShareFetch', [topError(35)])
    await run(fetching)
    await until(() => second.crashes.length === 1, 'the fetch crash event')
    expect(second.crashes[0].error).toBeInstanceOf(KafkaJSShareGroupsNotSupportedError)
    expect(second.crashes[0].error).toMatchObject({ reason: 'refused-by-broker', apiKey: 78 })
    expect(second.crashes[0].restart).toBe(false)
  })

  test('gates each new leader before a share request', async () => {
    const h = open({
      nodes: 2,
      topics: { 'topic-a': { topicId: A, partitions: [{ leader: 1, epoch: 1 }] } },
    })
    h.cluster.answer('ApiVersions', (fields, record) => {
      const base = h.cluster.apiVersionsAnswer()
      return record.node === 1
        ? { ...base, apiKeys: base.apiKeys.filter(({ apiKey }) => apiKey !== 78) }
        : base
    })
    const seen = observe(h)
    await subscribe(h)
    await run(h)
    await until(() => seen.crashes.length === 1, 'the crash event')
    expect(seen.crashes[0].error).toBeInstanceOf(KafkaJSShareGroupsNotSupportedError)
    expect(seen.crashes[0].error).toMatchObject({ reason: 'api-not-advertised', apiKey: 78 })
    expect(h.frames(78, 79).filter(({ node }) => node === 1)).toHaveLength(0)
    await expect(h.consumer.stop()).resolves.toBeUndefined()
  })

  test('coalesces repeated connect and preserves independent clients', async () => {
    const h = open()
    const seen = observe(h)
    await Promise.all([h.consumer.connect(), h.consumer.connect()])
    const negotiations = h.cluster.apiVersionTrace().length
    await h.consumer.connect()
    expect(h.cluster.apiVersionTrace()).toHaveLength(negotiations)
    expect(seen.connects).toHaveLength(1)
    const ordinary = h.client.consumer({ groupId: 'ordinary' })
    const producer = h.client.producer()
    const admin = h.client.admin()
    expect(Object.keys(ordinary.events)).toHaveLength(25)
    expect(Object.keys(h.consumer.events)).toHaveLength(18)
    expect(ordinary.events).not.toBe(h.consumer.events)
    expect(typeof ordinary.commitOffsets).toBe('function')
    expect(typeof producer.send).toBe('function')
    expect(typeof admin.describeShareGroups).toBe('function')
    expect(h.consumer.commitOffsets).toBeUndefined()
    expect(h.consumer.seek).toBeUndefined()
  })

  test('cleans up a failed connection without leaving a ready client', async () => {
    const h = open()
    const seen = observe(h)
    advertise(h, { finalizedFeatures: [], finalizedFeaturesEpoch: 7 })
    await expect(h.consumer.connect()).rejects.toMatchObject({ reason: 'feature-disabled' })
    expect(seen.connects).toHaveLength(0)
    h.cluster.answer('ApiVersions', () => h.cluster.apiVersionsAnswer())
    await expect(h.consumer.connect()).resolves.toBeUndefined()
    expect(seen.connects).toHaveLength(1)

    const raced = open()
    const second = observe(raced)
    const gate = raced.hold({ api: 'ApiVersions' })
    const connecting = raced.consumer.connect().catch((e) => e)
    await gate.arrived()
    const closing = raced.consumer.disconnect({ timeout: 0 })
    gate.release()
    const error = await connecting
    await closing
    expect(error).toBeInstanceOf(Error)
    expect(second.connects).toHaveLength(0)
    await expect(raced.consumer.connect()).resolves.toBeUndefined()
    expect(second.connects).toHaveLength(1)
  })

  test('resolves run after the first accepted join', async () => {
    const h = open()
    const seen = observe(h)
    const handled = jest.fn()
    await subscribe(h)
    const gate = h.hold({ api: 'ShareGroupHeartbeat' })
    let settled = false
    const running = run(h, { eachMessage: handled }).then(() => {
      settled = true
    })
    await gate.arrived()
    await flush()
    expect(settled).toBe(false)
    expect(handled).not.toHaveBeenCalled()
    h.script('ShareFetch', [])
    gate.release()
    await running
    expect(seen.joins).toEqual([{ groupId: 'group', memberId: expect.any(String), memberEpoch: 1 }])

    const empty = open({ topics: {} })
    const second = observe(empty)
    empty.table.set('topic-a', { topicId: A, partitions: [] })
    await subscribe(empty)
    await run(empty)
    expect(second.joins).toHaveLength(1)
  })

  test('keeps a retriable join pending until stop settles it', async () => {
    const h = open()
    await subscribe(h)
    h.script(
      'ShareGroupHeartbeat',
      Array.from({ length: 50 }, () => ({ errorCode: 15 }))
    )
    let settled = false
    const running = run(h).then(() => {
      settled = true
    })
    await until(() => h.frames(76).length >= 3, 'repeated join attempts')
    expect(settled).toBe(false)
    await h.consumer.stop()
    await running
    const sent = h.frames(76).length
    await flush()
    await new Promise((resolve) => setTimeout(resolve, 200))
    expect(h.frames(76)).toHaveLength(sent)
    expect(h.frames(76).every(({ fields }) => fields.memberEpoch === 0)).toBe(true)
  })

  test.each(FATAL_JOIN_CODES)(
    'rejects fatal first joins and reports later terminal errors once (%p)',
    async (code) => {
      const restartOnFailure = jest.fn(async () => true)
      const h = open({ share: { retry: { restartOnFailure } } })
      const seen = observe(h)
      await subscribe(h)
      h.script('ShareGroupHeartbeat', [{ errorCode: code }])
      const error = await run(h).catch((e) => e)
      expect(error).toBeInstanceOf(KafkaJSNonRetriableError)
      if (code !== 35) expect(error.cause.code).toBe(code)
      await until(() => seen.crashes.length === 1, 'the crash event')
      await flush()
      expect(seen.crashes).toHaveLength(1)
      expect(seen.crashes[0].error).toBe(error)
      expect(seen.crashes[0]).toMatchObject({ groupId: 'group', restart: false })
      expect(restartOnFailure).toHaveBeenCalledTimes(1)
    }
  )

  test('reports a terminal error after the join once, without rejecting run again', async () => {
    const h = open()
    const seen = observe(h)
    await subscribe(h)
    h.script('ShareGroupHeartbeat', [undefined, { errorCode: 30 }])
    await run(h)
    await until(() => seen.crashes.length === 1, 'the crash event')
    expect(seen.crashes[0].error.cause.code).toBe(30)
    expect(seen.crashes[0].restart).toBe(false)
    await flush()
    expect(seen.crashes).toHaveLength(1)
  })

  test('restarts with a fresh member only after bounded cleanup', async () => {
    const h = open({ share: { retry: { retries: 1 } } })
    const seen = observe(h)
    h.table.set('topic-a', { topicId: A, partitions: [{ leader: 5, epoch: 1 }] })
    await subscribe(h)
    h.consumer.on(h.consumer.events.CRASH, () => {
      h.table.set('topic-a', { topicId: A, partitions: [{ leader: 0, epoch: 1 }] })
    })
    await run(h)
    await until(() => seen.crashes.length === 1, 'the crash event')
    expect(seen.crashes[0].restart).toBe(true)
    await until(() => seen.joins.length === 2, 'the restarted join')
    const joins = h.frames(76).filter(({ fields }) => fields.memberEpoch === 0)
    expect(new Set(joins.map(({ fields }) => fields.memberId)).size).toBe(2)
    expect(h.consumer.subscription()).toEqual(['topic-a'])
    expect(seen.joins[0].memberId).not.toBe(seen.joins[1].memberId)
  })

  test('does not restart after stop while the callback is pending', async () => {
    const decision = deferred()
    let verdict
    const restartOnFailure = jest.fn(
      () =>
        new Promise((resolve) => {
          verdict = resolve
          decision.resolve()
        })
    )
    const h = open({ share: { retry: { retries: 1, restartOnFailure } } })
    const seen = observe(h)
    h.table.set('topic-a', { topicId: A, partitions: [{ leader: 5, epoch: 1 }] })
    await subscribe(h)
    await run(h)
    await decision.promise
    await h.consumer.stop()
    verdict(true)
    await until(() => seen.crashes.length === 1, 'the crash event')
    expect(seen.crashes[0].restart).toBe(false)
    const joins = h.frames(76).length
    await flush()
    await new Promise((resolve) => setTimeout(resolve, 200))
    expect(h.frames(76)).toHaveLength(joins)
    expect(seen.joins).toHaveLength(1)
  })

  test('logs a rejected restart callback and restarts while active', async () => {
    const restartOnFailure = jest.fn(async () => {
      throw new Error('callback failed')
    })
    const h = open({ share: { retry: { retries: 1, restartOnFailure } } })
    const seen = observe(h)
    h.table.set('topic-a', { topicId: A, partitions: [{ leader: 5, epoch: 1 }] })
    await subscribe(h)
    h.consumer.on(h.consumer.events.CRASH, () => {
      h.table.set('topic-a', { topicId: A, partitions: [{ leader: 0, epoch: 1 }] })
    })
    await run(h)
    await until(() => seen.joins.length === 2, 'the restarted join')
    expect(seen.crashes[0].restart).toBe(true)
  })
})
