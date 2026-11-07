const { ShareRun } = require('../shareRun')
const { createErrorFromCode } = require('../shareErrorContract')
const {
  A,
  ShareClock,
  acquire,
  deferred,
  fetchAnswer,
  flush,
  shareClient,
  until,
} = require('../../../../testHelpers/shareHarness')

describe('share disconnect after coordinator transport recovery', () => {
  let h
  let clock
  let run
  let handler
  let warning
  const handled = []

  afterEach(async () => {
    handler?.resolve()
    await Promise.allSettled(handled.splice(0))
    await h?.close()
    clock?.restore()
    jest.restoreAllMocks()
    h = clock = run = handler = warning = undefined
  })

  /**
   * @param {object} frame A recorded request.
   * @returns {object} The scripted socket carrying that frame.
   */
  const socketOf = ({ node, generation }) =>
    h.cluster.sockets().filter(({ endpoint }) => endpoint.host === `node${node}`)[generation - 1]

  /** @returns {object[]} Actual epoch -1 heartbeat writes. */
  const leaves = () => h.frames(76).filter(({ fields }) => fields.memberEpoch === -1)

  /**
   * Starts an established session, then holds its next fetch on the wire. Only the recovery
   * acquisition's handler blocks, so disconnect can capture that recovered session's final.
   * @param {number} [dataNode=0] The data leader; the coordinator is always node 0.
   * @param {boolean} [holdFirst=false] Hold the initial handler instead of a later fetch.
   * @param {number} [nodes=dataNode+1] Broker count, including unused discovery targets.
   * @returns {Promise<object>} The held fetch and the original membership and transport identities.
   */
  const begin = async (dataNode = 0, holdFirst = false, nodes = dataNode + 1) => {
    clock = new ShareClock().install()
    const start = ShareRun.prototype.start
    jest.spyOn(ShareRun.prototype, 'start').mockImplementation(function () {
      run = this
      return start.call(this)
    })
    h = shareClient({
      nodes,
      interval: 600000,
      kafka: { requestTimeout: 1000, enforceRequestTimeout: false },
      topics: { 'topic-a': { topicId: A, partitions: [{ leader: dataNode, epoch: 1 }] } },
    })
    const fetch = h.hold({
      api: 'ShareFetch',
      node: dataNode,
      match: ({ fields }) => fields.shareSessionEpoch > 0,
    })
    h.script('ShareFetch', [fetchAnswer([acquire(0, 0, ['first'])])])
    handler = deferred()
    await h.consumer.subscribe({ topics: ['topic-a'] })
    await h.consumer.run({
      eachMessage: ({ message }) => {
        const pending = holdFirst || message.offset === '1' ? handler.promise : Promise.resolve()
        handled.push(pending)
        return pending
      },
    })
    await until(
      () => (holdFirst ? handled.length === 1 : fetch.call !== null),
      'the established session'
    )
    expect(run.membership.memberEpoch).toBeGreaterThan(0)
    expect(run.nodes.get(dataNode).session.epoch).toBeGreaterThan(0)
    await run.heartbeat()
    expect(run.coordinator.current()).toBe(true)
    warning = jest.spyOn(run.options.logger, 'warn')
    return {
      fetch,
      dataNode,
      binding: run.coordinator,
      token: run.membership.token,
      memberId: run.membership.memberId,
      memberEpoch: run.membership.memberEpoch,
    }
  }

  /**
   * Closes the in-flight data socket and lets the original Broker retire its sibling and the
   * actual ShareRun recover. No heartbeat refreshes the saved coordinator binding.
   * @param {object} original Identities returned by `begin`.
   * @returns {Promise<void>} Resolves with the recovered acquisition's handler still blocked.
   */
  const recover = async (original) => {
    const { fetch, dataNode, binding, token, memberId, memberEpoch } = original
    h.script('ShareFetch', [fetchAnswer([acquire(0, 1, ['recovered'])])])
    const coordinatorSocket = binding.connection.socket
    socketOf(fetch.call).destroy()
    await flush()
    if (dataNode === 0) expect(coordinatorSocket.destroyed).toBe(true)
    for (let step = 0; step < 20 && handled.length < 2; step++) await clock.advance(50)
    expect(handled).toHaveLength(2)
    expect(run.nodes.get(dataNode).session.epoch).toBeGreaterThan(0)
    expect(run.membership.token).toBe(token)
    expect(run.membership.memberId).toBe(memberId)
    expect(run.membership.memberEpoch).toBe(memberEpoch)
    expect(h.frames(76)).toHaveLength(2)
    expect(binding.current()).toBe(dataNode !== 0)
    expect(binding.broker.connectionPool).toBe(binding.pool)
    expect(binding.pool.pool[0].isConnected()).toBe(true)
  }

  test.each([0, 1])(
    'writes and answers one leave after the data socket on node %p is lost',
    async (dataNode) => {
      const original = await begin(dataNode)
      await recover(original)
      const socket = original.binding.pool.pool[0].socket
      const leave = h.hold({
        api: 'ShareGroupHeartbeat',
        match: ({ fields }) => fields.memberEpoch === -1,
      })
      handler.resolve()
      let disconnected = false
      const closing = h.consumer.disconnect({ timeout: 400 }).then(() => {
        disconnected = true
      })
      await flush()
      expect(leaves()).toHaveLength(1)
      expect(disconnected).toBe(false)
      expect(socketOf(leaves()[0])).toBe(socket)
      expect(leaves()[0].fields).toMatchObject({
        groupId: 'group',
        memberId: original.memberId,
        memberEpoch: -1,
      })
      const [final] = h.frames(79)
      expect(final.fields.shareSessionEpoch).toBe(-1)
      expect(final.answeredOrder).toBeLessThan(leaves()[0].order)
      leave.release({
        errorCode: 0,
        memberId: original.memberId,
        memberEpoch: -1,
        heartbeatIntervalMs: 0,
        assignment: null,
      })
      await closing
      expect(leaves()[0].answeredOrder).toBeGreaterThan(leaves()[0].order)
      expect(leaves()).toHaveLength(1)
      expect(h.frames(76)).toHaveLength(3)
      expect(disconnected).toBe(true)
    }
  )

  test('keeps the coordinator binding when only the healthy data slot reconnects', async () => {
    const original = await begin(0, true)
    const { binding } = original
    const socket = binding.connection.socket
    const data = run.nodes.get(0).loop.broker.connectionPool.pool[1]
    await data.disconnect({ drainTimeout: 0 })
    await data.connect()
    expect(binding.current()).toBe(true)
    handler.resolve()
    await h.consumer.disconnect({ timeout: 400 })
    expect(leaves()).toHaveLength(1)
    expect(socketOf(leaves()[0])).toBe(socket)
    expect(leaves()[0].answeredOrder).toBeGreaterThan(leaves()[0].order)
    expect(socket.destroyed).toBe(true)
  })

  test('reports an expired disconnect without leaving the recovered membership', async () => {
    const original = await begin()
    await recover(original)
    await h.consumer.disconnect({
      get timeout() {
        clock.skew(400)
        return 400
      },
    })
    expect(leaves()).toHaveLength(0)
    expect(h.frames(79)).toHaveLength(0)
    expect(warning).toHaveBeenCalledWith(
      'Share leave skipped',
      expect.objectContaining({ reason: 'close-deadline' })
    )
  })

  test.each([25, 110])(
    'reports a fenced membership after error %p without leaving',
    async (code) => {
      const original = await begin()
      await recover(original)
      h.script('ShareGroupHeartbeat', [{ errorCode: code }])
      await expect(run.heartbeat()).rejects.toHaveProperty('code', code)
      await h.consumer.disconnect({ timeout: 400 })
      expect(leaves()).toHaveLength(0)
      expect(warning).toHaveBeenCalledWith(
        'Share leave skipped',
        expect.objectContaining({ reason: 'fenced' })
      )
    }
  )

  test('reports an unknown coordinator without adopting another membership', async () => {
    const original = await begin()
    await recover(original)
    h.script('ShareGroupHeartbeat', [{ errorCode: 16 }])
    await expect(run.heartbeat()).rejects.toHaveProperty('code', 16)
    expect(run.membership.token).toBe(original.token)
    handler.resolve()
    await h.consumer.disconnect({ timeout: 400 })
    expect(leaves()).toHaveLength(0)
    expect(h.frames(76)).toHaveLength(3)
    expect(warning).toHaveBeenCalledWith(
      'Share leave skipped',
      expect.objectContaining({ reason: 'coordinator-unknown' })
    )
  })

  test('retires a held coordinator lookup before an omitted leave finishes its final', async () => {
    const original = await begin(1, true, 3)
    h.script('ShareGroupHeartbeat', [{ errorCode: 16 }])
    await expect(run.heartbeat()).rejects.toHaveProperty('code', 16)
    await clock.advance(50)
    const lookup = h.hold({ api: 'FindCoordinator' })
    let heartbeatDone = false
    const heartbeat = run.heartbeat().finally(() => {
      heartbeatDone = true
    })
    await until(() => lookup.call !== null, 'the ordinary coordinator lookup')
    const final = h.hold({ api: 'ShareAcknowledge' })
    handler.resolve()
    let disconnected = false
    const closing = h.consumer.disconnect({ timeout: 400 }).then(() => {
      disconnected = true
    })
    await until(() => final.call !== null, 'the captured final')
    const membership = run.membership.snapshot()
    const facade = run.facade
    const sockets = h.cluster.sockets().length
    const frames = h.frames(76).length
    const publish = jest.spyOn(run.runner, 'emit')
    lookup.release({
      coordinators: [
        { key: 'group', nodeId: 2, host: 'node2', port: 9092, errorCode: 0, errorMessage: null },
      ],
    })
    await until(() => heartbeatDone, 'the retired heartbeat completion')
    await heartbeat
    await flush()
    expect(disconnected).toBe(false)
    expect(final.call.answeredOrder).toBeUndefined()
    expect(h.frames(76)).toHaveLength(frames)
    expect(h.cluster.sockets()).toHaveLength(sockets)
    expect(run.facade).toBe(facade)
    expect(run.coordinator).toBe(original.binding)
    expect(run.membership.snapshot()).toEqual(membership)
    expect(publish).not.toHaveBeenCalled()
    expect(warning).toHaveBeenCalledWith(
      'Share leave skipped',
      expect.objectContaining({ reason: 'coordinator-unknown', outcome: 'not-sent' })
    )
    final.release()
    await until(() => disconnected, 'the disconnect after the captured final')
    await closing
    expect(leaves()).toHaveLength(0)
  })

  test('discards an ordinary heartbeat answer held over a coordinator-unknown close', async () => {
    const original = await begin(1, true)
    const ordinary = h.hold({
      api: 'ShareGroupHeartbeat',
      match: ({ fields }) => fields.memberEpoch > 0,
    })
    let heartbeatDone = false
    const heartbeat = run.heartbeat().finally(() => {
      heartbeatDone = true
    })
    await until(() => ordinary.call !== null, 'the written ordinary heartbeat')
    run.membership.onMembershipError(createErrorFromCode(16), original.token)
    expect(run.membership.coordinator).toBeNull()
    const final = h.hold({ api: 'ShareAcknowledge' })
    handler.resolve()
    let disconnected = false
    const closing = h.consumer.disconnect({ timeout: 400 }).then(() => {
      disconnected = true
    })
    await until(() => final.call !== null, 'the captured final')
    const membership = run.membership.snapshot()
    const sockets = h.cluster.sockets().length
    const frames = h.frames(76).length
    const publish = jest.spyOn(run.runner, 'emit')
    ordinary.release({
      errorCode: 0,
      memberId: original.memberId,
      memberEpoch: original.memberEpoch + 1,
      heartbeatIntervalMs: 600000,
      assignment: { topicPartitions: [] },
    })
    await until(() => heartbeatDone, 'the retired heartbeat answer')
    await heartbeat
    expect(final.call.answeredOrder).toBeUndefined()
    expect(h.frames(76)).toHaveLength(frames)
    expect(h.cluster.sockets()).toHaveLength(sockets)
    expect(run.coordinator).toBe(original.binding)
    expect(run.membership.snapshot()).toEqual(membership)
    expect(publish).not.toHaveBeenCalled()
    final.release()
    await until(() => disconnected, 'the disconnect after the captured final')
    await closing
    expect(leaves()).toHaveLength(0)
  })

  test('rejoins after a poll close omits leave for an unknown coordinator', async () => {
    const original = await begin(1, true)
    run.membership.onMembershipError(createErrorFromCode(16), original.token)
    clock.skew(run.options.maxPollInterval + 1)
    let recovered = false
    const recovery = run.onPollExpired().then(() => {
      recovered = true
    })
    await until(() => run.closing === null, 'the poll close completion')
    expect(run.membership.state).toBe('STALE')
    expect(leaves()).toHaveLength(0)
    expect(recovered).toBe(false)
    handler.resolve()
    await until(() => recovered, 'the drained poll recovery')
    await recovery
    await clock.advance(0)
    await until(() => run.membership.memberEpoch > 0, 'the fresh join')
    expect(run.membership.memberId).toBe(original.memberId)
    expect(run.membership.token).not.toBe(original.token)
    expect(h.frames(76).map(({ fields }) => fields.memberEpoch)).toEqual([0, 1, 0])
  })

  test('refuses a coordinator replacement after the close boundary and reports the omission', async () => {
    const original = await begin()
    await recover(original)
    const final = h.hold({ api: 'ShareAcknowledge' })
    handler.resolve()
    const closing = h.consumer.disconnect({ timeout: 400 })
    await until(() => final.call !== null, 'the captured final')
    const connection = original.binding.pool.pool[0]
    await connection.disconnect({ drainTimeout: 0 })
    await connection.connect()
    final.release()
    await closing
    expect(leaves()).toHaveLength(0)
    expect(warning).toHaveBeenCalledWith(
      'Share leave skipped',
      expect.objectContaining({ reason: 'coordinator-stale' })
    )
  })

  test.each(['deadline', 'late response', 'coordinator replacement'])(
    'reports a written leave with a held response as unknown after %s',
    async (refusal) => {
      const original = await begin(1, true)
      const leave = h.hold({
        api: 'ShareGroupHeartbeat',
        match: ({ fields }) => fields.memberEpoch === -1,
      })
      handler.resolve()
      let disconnected = false
      const closing = h.consumer.disconnect({ timeout: 400 }).then(() => {
        disconnected = true
      })
      await until(() => leave.call !== null, 'the written leave')
      expect(disconnected).toBe(false)
      expect(warning).not.toHaveBeenCalledWith('Share leave skipped', expect.anything())
      if (refusal === 'deadline') {
        await clock.advance(400)
        expect(disconnected).toBe(true)
      } else if (refusal === 'late response') clock.skew(400)
      else {
        await original.binding.connection.disconnect({ drainTimeout: 0 })
        await expect(original.binding.connection.connect()).rejects.toMatchObject({
          name: 'KafkaJSConnectionError',
          message: 'Connection closed while connecting',
        })
      }
      leave.release({
        errorCode: 0,
        memberId: original.memberId,
        memberEpoch: -1,
        heartbeatIntervalMs: 0,
        assignment: null,
      })
      await until(() => disconnected, 'the bounded disconnect completion')
      await closing
      await flush()
      expect(leaves()).toHaveLength(1)
      expect(warning).not.toHaveBeenCalledWith('Share leave skipped', expect.anything())
      expect(
        warning.mock.calls.filter(([message]) => message === 'Share leave outcome unknown')
      ).toEqual([
        [
          'Share leave outcome unknown',
          expect.objectContaining({
            reason: refusal === 'coordinator replacement' ? 'coordinator-stale' : 'close-deadline',
            outcome: 'sent-unknown',
          }),
        ],
      ])
    }
  )
})
