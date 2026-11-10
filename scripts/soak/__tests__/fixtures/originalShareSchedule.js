const { EventEmitter } = require('events')
const {
  ShareSoak,
  SharePriming,
  ShareSoakDriver,
  ShareFetchTap,
  SHARE_SOAK,
  instrumentShare,
} = require('../../soak')
const { ShareTimerObserver } = require('../../shareTimerObserver')
const { CleanupStack } = require('../../../../src/producer/__tests__/fixtures/liveProbe')
const { LiveRun, delay } = require('../../../../src/producer/__tests__/fixtures/bounded')

const {
  ShareClock,
  shareClient,
  fetchAnswer,
  ackAnswer,
  part,
  acquired,
  flush,
} = require('../../../../testHelpers/shareHarness')
const {
  batch,
  record,
} = require('../../../../src/protocol/requests/fetch/__tests__/fixtures/recordBatchBytes')
const fixtures = require('../../../../testHelpers/shareFixtures')
const diagnostics = require('../../../../testHelpers/shareLiveDiagnostics')

/**
 * Script only external I/O. Membership, session loss/reopen, acquisition, dispatch, ledger
 * decisions, acknowledgements, window observation, resource judging and reconstruction call
 * through their original implementations. The raw corpus is independent of handler evidence.
 * @param {object} [options] Selector, one/two scopes and bounded external fault controls.
 * @param {Function} [options.processResources] Scripts process gauges from real timer ownership; omitted gauges are empty.
 * @param {boolean} [options.twoScopes=false] Runs paced priming/archive controls instead of the original immediate-reply fault schedule.
 * @param {boolean} [options.correlateCleanup=false] Scripts the OS clock port from the fake schedule for archive controls.
 * @returns {Promise<object>} Original owner/evidence and weak first-use roots only.
 * @throws {Error} A scripted transport, cleanup or schedule failure.
 */
const originalShareSchedule = async ({
  short = true,
  correlateCleanup = false,
  twoScopes = false,
  retainPrime,
  checkBoundaryRoots = async () => {},
  damage = () => {},
  wire = () => {},
  finalDamage = () => {},
  io = async () => {},
  assignmentDelay = 0,
  idleWakeMs = 0,
  replayAt,
  drainReplay = false,
  socketClosed = () => {},
  closeOnly = false,
  observeRow = () => {},
  processResources,
} = {}) => {
  const clock = new ShareClock().install()
  const osOrigin = process.hrtime.bigint()
  const cleanupClock = correlateCleanup
    ? jest
        .spyOn(process.hrtime, 'bigint')
        .mockImplementation(() => osOrigin + BigInt(Math.floor(performance.now() * 1000000)))
    : null
  const exposed = globalThis.gc
  globalThis.gc = undefined
  const transport = shareClient({ nodes: 3, topics: {} })
  const { cluster, table } = transport
  const tap = new ShareFetchTap(64, { now: () => performance.now() })
  const { SoakTransport } = require('./soakTransport')
  const external = new SoakTransport(tap)
  /**
   * Models transport reset and close even though the wire archive retains destroyed sockets.
   * Rejects destroyed-stream writes before they can apply acknowledgements at the broker.
   * Connection removes its end listener before deliberate shutdown; destroying a live stream
   * instead reports the injected reset, settling original requests before close releases listeners.
   * The close-only variant preserves the fake socket's original destroy and write semantics.
   * @param {object} options Socket endpoint and connection callback.
   * @returns {object} The scripted socket with asynchronous stream-write failures.
   */
  const socketFactory = (options) => {
    const socket = cluster.socketFactory(options)
    if (!closeOnly) {
      const destroy = socket.destroy.bind(socket)
      socket.destroy = (error) =>
        destroy(
          error ??
            (socket.listenerCount('end') > 0
              ? Object.assign(new Error('Connection reset'), { code: 'ECONNRESET' })
              : undefined)
        )
    }
    socket.once('close', () => {
      socket.removeAllListeners()
      socketClosed(socket)
    })
    if (!closeOnly) {
      const write = socket.write.bind(socket)
      socket.write = (...args) => {
        if (!socket.destroyed) return write(...args)
        const error = Object.assign(new Error('Cannot call write after a stream was destroyed'), {
          code: 'ERR_STREAM_DESTROYED',
        })
        queueMicrotask(() => {
          if (socket.listenerCount('error')) socket.emit('error', error)
        })
        return false
      }
    }
    return socket
  }
  // Timer attribution must call through when a resource script consumes the original owners.
  const installTimers = processResources
    ? null
    : jest.spyOn(ShareTimerObserver.prototype, 'install').mockImplementation(() => {})
  let instrumented
  try {
    instrumented = instrumentShare(
      cluster.kafkaOptions({
        socketFactory: tap.factory(socketFactory),
        retry: { initialRetryTime: 50, maxRetryTime: 100, factor: 0, retries: 3 },
      }),
      { activeResources: false }
    )
  } finally {
    installTimers?.mockRestore()
  }
  const resources = instrumented.shareResources
  const originalSnapshot = resources.snapshot
  // Heap and process resources are scripted inputs; original client state is always measured.
  resources.snapshot = async () => {
    const original = await originalSnapshot()
    const { counts, owners } = processResources
      ? processResources(original)
      : { counts: {}, owners: { rows: [], dropped: 0 } }
    return {
      ...original,
      heapUsed: 1024,
      activeResources: { ...counts },
      activeResourcesBeforeCollection: { ...counts },
      timerOwners: {
        beforeCollection: ShareTimerObserver.evidence(owners, counts),
        afterCollection: ShareTimerObserver.evidence(owners, counts),
      },
    }
  }
  const gate = new LiveRun()
  const rows = []
  const routes = new Map()
  for (const api of ['ShareGroupHeartbeat', 'ShareFetch', 'ShareAcknowledge'])
    cluster.answer(api, (fields, call) => {
      const route = routes.get(fields.groupId)
      if (!route) throw new Error('A request escaped its original group scope')
      return api === 'ShareGroupHeartbeat'
        ? route.heartbeat(fields, call)
        : route.answer(fields, call)
    })
  let primeRoots
  let boundaryChecked = false
  let replayDue
  let idleUntil
  const createScope = (priming = false) => {
    const SOAK_UUID = priming
      ? '00000000-0000-4000-8000-000000000002'
      : '00000000-0000-4000-8000-000000000001'
    const realKafka = priming
      ? instrumented.createKafka({ clientId: 'original-prime' })
      : instrumented.kafka
    const records = [[], [], []]
    const members = new Map()
    const configs = new Map()
    const sessions = new Map()
    let groupId
    let topic
    let groupPresent = false
    let listingNode = 0
    let firstWindow
    const verifierStarts = []
    const seenMembers = new Map()
    const assignedMembers = new Set()
    const assignment = (id) => {
      const ids = [...members.keys()]
      return [{ topic, partitions: ids.length < 2 ? [0, 1, 2] : id === ids[0] ? [0, 1] : [2] }]
    }
    const wireAssignment = (id) => ({
      topicPartitions: assignment(id).map(({ partitions }) => ({ topicId: SOAK_UUID, partitions })),
    })
    const heartbeat = async (fields) => {
      if (fields.memberEpoch === -1) {
        members.delete(fields.memberId)
        return {
          errorCode: 0,
          memberId: fields.memberId,
          memberEpoch: -1,
          heartbeatIntervalMs: 5000,
          assignment: null,
        }
      }
      members.set(fields.memberId, true)
      if (!seenMembers.has(fields.memberId)) seenMembers.set(fields.memberId, performance.now())
      const waiting = performance.now() - seenMembers.get(fields.memberId) < 5000
      if (!waiting && !assignedMembers.has(fields.memberId)) {
        assignedMembers.add(fields.memberId)
        // Delay the first assignment reply without requiring a heartbeat beyond the join bound.
        if (
          assignmentDelay &&
          (driver.lastCycle?.members.aLeft || owner.counts.cycles === SHARE_SOAK.cycles)
        )
          await new Promise((resolve) => setTimeout(resolve, assignmentDelay))
      }
      return {
        errorCode: 0,
        memberId: fields.memberId,
        memberEpoch: 1,
        heartbeatIntervalMs: 5000,
        assignment: waiting ? null : wireAssignment(fields.memberId),
      }
    }
    const answer = (fields, call) => {
      const key = `${fields.memberId}:${call.node}:${call.generation}`
      if (fields.shareSessionEpoch === 0) sessions.set(key, new Set())
      const wanted = sessions.get(key) ?? new Set()
      for (const { partitions } of fields.topics)
        for (const { partitionIndex, acknowledgementBatches } of partitions) {
          wanted.add(partitionIndex)
          for (const ack of acknowledgementBatches)
            for (const row of records[partitionIndex])
              if (
                BigInt(row.offset) >= BigInt(ack.firstOffset) &&
                BigInt(row.offset) <= BigInt(ack.lastOffset) &&
                row.owner === key &&
                ack.acknowledgeTypes.every((type) => type === 1)
              )
                row.settled = true
        }
      sessions.set(key, wanted)
      for (const { partitions } of fields.forgottenTopicsData ?? [])
        for (const partition of partitions) wanted.delete(partition)
      if (call.apiKey === 79)
        return ackAnswer([...wanted].map((partition) => ({ topicId: SOAK_UUID, partition })))
      const parts = [...wanted].map((partition) => {
        const available = records[partition].filter((row) => !row.settled && row.owner !== key)
        const acquire = []
        let bytes = ''
        if (available.length) {
          for (const row of available) {
            row.owner = key
            row.count++
          }
          acquire.push(acquired(available[0].offset, available.at(-1).offset, available[0].count))
          bytes = batch({
            baseOffset: BigInt(available[0].offset),
            records: available.map((row, index) =>
              record({ offsetDelta: index, key: row.key, value: row.value })
            ),
          }).toString('hex')
        }
        return part({ topicId: SOAK_UUID, partition, acquire, records: bytes })
      })
      const result = fetchAnswer(parts, { acquisitionLockTimeoutMs: 30000 })
      if (
        fields.shareSessionEpoch > 0 &&
        (driver.lastCycle?.members.aLeft || owner.counts.cycles === SHARE_SOAK.cycles)
      )
        result.responses = result.responses.flatMap((topic) => {
          const partitions = topic.partitions.filter(
            (partition) => partition.errorCode !== 0 || partition.acquiredRecords.length > 0
          )
          return partitions.length ? [{ ...topic, partitions }] : []
        })
      wire({ fields, call, result, owner, records, members })
      if (
        twoScopes &&
        fields.maxWaitMs > 0 &&
        fields.shareSessionEpoch >= 0 &&
        result.responses.every((t) => t.partitions.every((p) => p.acquiredRecords.length === 0))
      )
        return new Promise((resolve) => setTimeout(() => resolve(result), fields.maxWaitMs))
      return result
    }
    const admin = () => {
      const events = new EventEmitter()
      return {
        events: { REQUEST: 'request' },
        on: (event, listener) => {
          events.on(event, listener)
          return () => events.off(event, listener)
        },
        connect: async () => {
          if (twoScopes && !priming && !boundaryChecked) {
            boundaryChecked = true
            await checkBoundaryRoots(primeRoots)
          }
        },
        disconnect: async () => io('admin-disconnect', { owner, driver, clock }),
        createTopics: async ({ topics: [created] }) => {
          topic = created.topic
          table.set(topic, {
            topicId: SOAK_UUID,
            partitions: [0, 1, 2].map((leader) => ({ leader, epoch: 1 })),
          })
        },
        deleteTopics: async () => {
          await io('delete-topic', { owner, driver, clock })
          return table.delete(topic)
        },
        listTopics: async () => {
          events.emit('request', {
            payload: { apiName: 'Metadata', broker: `node${listingNode++ % 3}:9092` },
          })
          return [...table.keys()]
        },
        fetchTopicMetadata: async () => ({
          topics: [
            {
              name: topic,
              topicId: SOAK_UUID,
              partitions: [0, 1, 2].map((partitionId) => ({
                partitionId,
                leader: partitionId,
                replicas: [0, 1, 2],
              })),
            },
          ],
        }),
        describeCluster: async () => ({
          brokers: [0, 1, 2].map((nodeId) => ({ nodeId, host: `node${nodeId}`, port: 9092 })),
        }),
        listGroups: async () => ({ groups: groupPresent ? [{ groupId }] : [] }),
        deleteGroups: async () => {
          groupPresent = false
          routes.delete(groupId)
        },
        describeShareGroups: async ([id]) => ({
          groups: [
            {
              groupId: id,
              errorCode: groupPresent ? 0 : 69,
              state: members.size ? 'Stable' : 'Empty',
              members: [...members.keys()].map((memberId) => ({
                memberId,
                assignment: assignment(memberId),
              })),
            },
          ],
        }),
        listShareGroupOffsets: async () => [
          {
            topic,
            topicId: SOAK_UUID,
            partitions: records.map((rows, partition) => ({
              partition,
              startOffset: '0',
              lag: String(rows.filter(({ settled }) => !settled).length),
              errorCode: 0,
              leaderEpoch: 1,
            })),
          },
        ],
        fetchTopicOffsets: async () => {
          await io('terminal-ends', { owner, driver, clock })
          return records.map((rows, partition) => ({ partition, high: String(rows.length) }))
        },
        incrementalAlterConfigs: async ({ resources: [resource] }) => {
          groupId = resource.name
          routes.set(groupId, { heartbeat, answer })
          groupPresent = true
          for (const { name, value, operation } of resource.configEntries)
            if (operation === 1) configs.delete(name)
            else configs.set(name, value)
          return {
            resources: [{ errorCode: 0, resourceName: resource.name, resourceType: resource.type }],
          }
        },
        describeConfigs: async ({ resources: [resource] }) => {
          const values =
            resource.type === 2
              ? new Map([
                  ['min.insync.replicas', '2'],
                  ['retention.ms', '-1'],
                  ['cleanup.policy', 'delete'],
                ])
              : resource.type === 32
                ? new Map([
                    ['share.heartbeat.interval.ms', '5000'],
                    ['share.session.timeout.ms', '45000'],
                    ...configs,
                  ])
                : new Map([
                    ['group.share.heartbeat.interval.ms', '5000'],
                    ['group.share.min.heartbeat.interval.ms', '5000'],
                    ['group.share.max.heartbeat.interval.ms', '15000'],
                    ['group.share.session.timeout.ms', '45000'],
                    ['group.share.min.session.timeout.ms', '45000'],
                    ['group.share.max.session.timeout.ms', '60000'],
                    ['group.share.assignment.interval.ms', '1000'],
                    ['group.share.min.assignment.interval.ms', '0'],
                    ['group.share.max.assignment.interval.ms', '15000'],
                    ['group.share.assignors', 'simple'],
                  ])
          return {
            resources: [
              {
                resourceName: resource.name,
                resourceType: resource.type,
                errorCode: 0,
                configEntries: [...values]
                  .filter(([name]) => !resource.configNames || resource.configNames.includes(name))
                  .map(([configName, configValue]) => ({
                    configName,
                    configValue,
                    configSource: configs.has(configName) ? 8 : resource.type === 32 ? 5 : 4,
                    configSynonyms: [],
                  })),
              },
            ],
          }
        },
      }
    }
    const kafka = {
      shareConsumer: (options) => {
        const client = tap.client('share', realKafka.shareConsumer(options))
        if (!driver.lastCycle?.members.aLeft && owner.counts.cycles !== 30)
          return {
            ...client,
            disconnect: async (options) => {
              await io('workload-disconnect', { owner, driver, clock })
              return client.disconnect(options)
            },
          }
        const assignment = client.assignment
        const disconnect = client.disconnect
        // The public consumer is frozen: wrap a copy instead of mutating it.
        const observed = { ...client }
        let fetching = false
        let started = false
        let gate
        const remove = client.on(client.events.REQUEST, ({ payload }) => {
          if (payload.apiName === 'ShareFetch') fetching = true
        })
        const replay = () =>
          fetchAnswer(
            [
              part({
                topicId: SOAK_UUID,
                partition: 0,
                acquire: [acquired(0, 0, 2)],
                records: batch({
                  baseOffset: 0n,
                  records: [record({ key: records[0][0].key, value: records[0][0].value })],
                }).toString('hex'),
              }),
            ],
            { acquisitionLockTimeoutMs: 30000 }
          )
        observed.assignment = () => {
          const current = assignment()
          if (!started && fetching && current[0]?.partitions.length === 3) {
            started = true
            verifierStarts.push(performance.now())
            if (replayAt !== undefined || drainReplay) {
              gate = transport.hold({ api: 'ShareFetch', node: 0 })
              if (!drainReplay) {
                replayDue = performance.now() + replayAt
                setTimeout(() => gate.release(replay()), replayAt)
              }
            }
          }
          return current
        }
        observed.disconnect = async (options) => {
          await io('verifier-disconnect', { owner, driver, clock })
          if (drainReplay && gate?.settle) {
            gate.release(replay())
            await flush()
          }
          try {
            return await disconnect(options)
          } finally {
            remove()
          }
        }
        return observed
      },
      admin: () => external.admin(admin()),
      producer: () =>
        external.producer({
          connect: async () => io('phase-open', { owner, driver, clock }),
          disconnect: async () => {},
          send: async ({ messages }) => {
            const partition = messages[0].partition
            const baseOffset = String(records[partition].length)
            for (const message of messages)
              records[partition].push({
                ...message,
                offset: String(records[partition].length),
                settled: false,
                owner: null,
                count: 0,
              })
            return [{ partition, baseOffset, errorCode: 0 }]
          },
        }),
    }
    const driver = new ShareSoakDriver({
      kafka,
      shares: resources,
      tap,
      prefix: priming ? 'soak-share-prime' : 'soak-share',
      brokers: ['node0:9092'],
      socketFactory: instrumented.socketFactory,
      ports: {
        fixtures,
        diagnostics,
        bounded: require('../../../../src/producer/__tests__/fixtures/bounded'),
        probe: require('../../../../src/producer/__tests__/fixtures/liveProbe'),
        readLog: external.reader(
          async () =>
            new Map(
              records.map((rows, partition) => [
                partition,
                {
                  endOffset: String(rows.length),
                  records: rows.map(({ offset, key, value }) => ({ offset, key, value })),
                },
              ])
            )
        ),
      },
    })
    const Owner = priming ? SharePriming : ShareSoak
    const owner = new Owner({
      driver,
      kafka,
      shares: resources,
      short: priming || short,
      createPriming:
        twoScopes && !priming
          ? () => {
              const prime = createScope(true)
              primeRoots = {
                driver: new WeakRef(prime.driver),
                corpus: new WeakRef(prime.driver.acknowledged),
                rawCorpus: new WeakRef(prime.records),
              }
              if (retainPrime)
                owner.retainedPrime =
                  retainPrime === 'driver' ? prime.driver : prime.driver.acknowledged
              return prime.owner
            }
          : undefined,
      stack: new CleanupStack(),
      clock: { now: () => performance.now() },
      pause: async (scope, ms) => {
        idleUntil = performance.now() + ms
        try {
          await delay(scope, ms)
          clock.skew(idleWakeMs)
        } finally {
          idleUntil = undefined
        }
      },
      gate,
      evidence: {
        read: () => rows,
        write: (row) => {
          if (row.event === 'soak-cycle') {
            if (row.verifier) firstWindow ??= JSON.parse(JSON.stringify(row.verifier))
            if (row.status === 'passed') damage(row, { owner, firstWindow, rows, clock })
          }
          if (row.event === 'soak-resources' && row.kind === 'teardown')
            finalDamage(owner.final, { owner, rows, clock })
          rows.push(JSON.parse(JSON.stringify(row)))
          observeRow(row, { owner, driver, clock })
        },
      },
    })
    if (!twoScopes)
      owner.prime = async () => {
        owner.priming = { status: 'passed', topic: 'primed', groupId: 'primed', topicId: 'primed' }
      }
    return { owner, driver, records, verifierStarts }
  }
  const { owner, records, verifierStarts } = createScope()
  let result
  let error
  let done = false
  const work = owner
    .run()
    .then(
      (value) => {
        result = value
      },
      (failure) => {
        error = failure
      }
    )
    .finally(() => {
      done = true
    })
  const cleanupErrors = []
  try {
    for (let tick = 0; tick <= owner.budget.totalMs / 250; tick++) {
      if (done) break
      // Drain response macrotasks at the replay boundary before advancing past the window.
      const step = replayDue === undefined ? 250 : Math.max(0, replayDue - performance.now())
      // No workload is pending during padding; Jest still delivers every sample timer in order.
      const idleStep = twoScopes && idleUntil !== undefined ? idleUntil - performance.now() : 250
      await clock.advance(Math.min(idleStep, replayDue === undefined ? idleStep : step))
      if (performance.now() >= replayDue) replayDue = undefined
    }
    if (!done) throw new Error('The scripted schedule did not finish within its frozen envelope')
    await work
    cluster.assertClean()
  } finally {
    let closed = false
    const closing = owner
      .close()
      .catch((failure) => cleanupErrors.push(failure))
      .finally(() => {
        closed = true
      })
    for (let tick = 0; tick < 100; tick++) {
      if (closed) break
      await clock.advance(250)
    }
    try {
      await closing
      await transport.close().catch((failure) => cleanupErrors.push(failure))
      await flush()
    } finally {
      external.close()
      resources.restore()
      globalThis.gc = exposed
      cleanupClock?.mockRestore()
      clock.restore()
    }
  }
  if (cleanupErrors.length) {
    if (error) error.cleanupErrors = cleanupErrors
    else if (result?.ok === false) result.cleanupErrors = cleanupErrors
    else throw cleanupErrors[0]
  }
  return {
    owner,
    rows,
    primeRoots,
    result,
    error,
    records,
    verifierStarts,
    reason: error?.message ?? result?.violations.join('; '),
  }
}

module.exports = { originalShareSchedule }
