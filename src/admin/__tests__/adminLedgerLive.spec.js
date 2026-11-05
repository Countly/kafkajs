const { randomBytes, pbkdf2Sync } = require('crypto')
const { Kafka, AlterConfigOpTypes } = require('../../../index')
const { inspect } = require('util')
const { AdminLiveProbe } = require('./fixtures/liveAdmin')
const Broker = require('../../broker')
const Pool = require('../../network/connectionPool')
const { saslBrokers, saslConnectionOpts } = require('testHelpers')
const { readData } = require('../../producer/__tests__/fixtures/liveProbe')
const { liveCase, caseTimeout, pollUntil } = require('../../producer/__tests__/fixtures/bounded')

const suite = process.env.ADMIN_LEDGER_E2E ? describe : describe.skip
const budget = 540000

/**
 * Maps a config entry as requested to the shape DescribeConfigs answers with.
 * @param {{name: string, value: string}} entry request-side entry
 * @returns {{configName: string, configValue: string}} response-side entry
 */
const asDescribed = ({ name, value }) => ({ configName: name, configValue: value })

/**
 * Picks the fields that identify an ACL, from an ACL or a DeleteAcls match.
 * @param {object} acl ACL or match
 * @returns {object} resource, pattern, principal, host, operation and permission
 */
const aclIdentity = ({
  resourceType,
  resourceName,
  resourcePatternType,
  principal,
  host,
  operation,
  permissionType,
}) => ({
  resourceType,
  resourceName,
  resourcePatternType,
  principal,
  host,
  operation,
  permissionType,
})

/**
 * Checks that a DeleteAcls answer to one filter succeeded, in the response and in every match.
 * @param {{filterResponses: object[]}} answer public deleteAcls result
 * @returns {object[]} identities of the ACLs the filter deleted, possibly none
 * @throws {Error} not exactly one filter response, or any error code
 */
const deletedAcls = ({ filterResponses }) => {
  expect(filterResponses).toHaveLength(1)
  expect(filterResponses[0].errorCode).toBe(0)
  for (const match of filterResponses[0].matchingAcls) expect(match.errorCode).toBe(0)
  return filterResponses[0].matchingAcls.map(aclIdentity)
}

/** Serial broker observations with separate physical, committed and handler identities. */
class AdminLedgerScenario {
  /** @param {AdminLiveProbe} probe bounded owner @returns {AdminLedgerScenario} scenario */
  constructor(probe) {
    this.probe = probe
    this.records = new Set()
    this.id = randomBytes(8).toString('hex')
  }

  /**
   * Opens an independent raw source and registers close before connection.
   * @param {string} host broker host
   * @param {number} port broker port
   * @returns {Promise<Broker>} connected source
   * @throws {Error} connection or budget failure
   */
  async raw(host, port) {
    const p = this.probe
    const broker = new Broker({
      logger: p.logger,
      connectionPool: new Pool({
        host,
        port,
        logger: p.logger,
        socketFactory: (args) => p.socket(args),
        connectionTimeout: 3000,
        requestTimeout: 30000,
      }),
    })
    p.cleanup.add('independent source', () =>
      broker.disconnect({ drainTimeout: Math.min(5000, p.remaining()) })
    )
    await p.invoke(() => broker.connect())
    return broker
  }

  /**
   * Deletes ACLs through one filter and proves the outcome: a single successful response that
   * deleted exactly the expected ACLs, after which DescribeAcls finds nothing under the filter.
   * @param {object} client admin that owns the ACLs
   * @param {object} filter complete delete filter, also used to describe what remains
   * @param {object[]} expected the ACLs the filter must delete, whatever order they come back in
   * @returns {Promise<void>}
   * @throws {Error} a failed answer, other ACLs deleted, or an ACL that remains until the budget ends
   */
  async removeAcls(client, filter, expected) {
    const p = this.probe
    const order = (rows) => rows.map((row) => JSON.stringify(aclIdentity(row))).sort()
    const deleted = deletedAcls(await p.invoke(() => client.deleteAcls({ filters: [filter] })))
    expect(order(deleted)).toEqual(order(expected))
    await p.poll(
      async () => (await p.invoke(() => client.describeAcls(filter))).resources.length === 0
    )
  }

  /**
   * Emits one observed case only once, after all its assertions.
   * @param {string} name case identity
   * @param {object} facts nonsecret scalar evidence
   * @returns {void}
   * @throws {Error} duplicate case or invalid evidence
   */
  record(name, facts) {
    expect(this.records.has(name)).toBe(false)
    this.probe.record(name, facts)
    this.records.add(name)
  }

  /**
   * Finds five fresh classic identities on exactly two independently observed coordinators.
   * @returns {Promise<void>}
   * @throws {Error} fixture distribution, mask, routing or broker failure
   */
  async authorization() {
    const p = this.probe
    this.admin = await p.start()
    this.topic = await p.topic({ name: 'permissions' })
    const [host, port] = p.baseOptions.brokers[0].split(':')
    this.seed = await this.raw(host, Number(port))
    const buckets = new Map()
    for (let index = 0; index < 100; index++) {
      const groupId = `e2e-ledger-${this.id}-${index}`
      const { coordinator } = await p.invoke(() =>
        this.seed.findGroupCoordinator({ groupId, coordinatorType: 0 })
      )
      if (!buckets.has(coordinator.nodeId) && buckets.size === 2) continue
      if (!buckets.has(coordinator.nodeId))
        buckets.set(coordinator.nodeId, { coordinator, ids: [] })
      const bucket = buckets.get(coordinator.nodeId)
      if (bucket.ids.length < 3) bucket.ids.push(groupId)
      if (
        buckets.size === 2 &&
        [...buckets.values()].every(({ ids }) => ids.length >= 2) &&
        [...buckets.values()].reduce((sum, { ids }) => sum + ids.length, 0) >= 5
      )
        break
    }
    expect(buckets.size).toBe(2)
    const selected = [...buckets.values()]
    this.groupIds = [
      selected[0].ids[0],
      selected[1].ids[0],
      selected[0].ids[1],
      selected[1].ids[1],
      selected.find(({ ids }) => ids.length === 3).ids[2],
    ]
    expect(this.groupIds).toHaveLength(5)
    p.cleanup.add('delete classic groups', async () => {
      const result = await this.admin.deleteGroups(this.groupIds)
      p.items(result)
      expect(result.map(({ groupId }) => groupId).sort()).toEqual([...this.groupIds].sort())
    })
    for (const groupId of this.groupIds)
      await p.invoke(() =>
        this.admin.setOffsets({
          groupId,
          topic: this.topic,
          partitions: [{ partition: 0, offset: '0' }],
        })
      )
    const independentGroupMasks = []
    for (const groupId of this.groupIds) {
      const { coordinator } = await p.invoke(() =>
        this.seed.findGroupCoordinator({ groupId, coordinatorType: 0 })
      )
      const broker = await this.raw(coordinator.host, coordinator.port)
      const response = await p.invoke(() =>
        broker.describeGroups({ groupIds: [groupId], includeAuthorizedOperations: true })
      )
      expect(response.groups[0].errorCode).toBe(0)
      independentGroupMasks.push(response.groups[0].authorizedOperations)
    }
    const kafka = new Kafka({
      ...p.baseOptions,
      socketFactory: (args) => p.socket(args),
      logCreator: () => (entry) => p.logs.push(entry),
    })
    const factoryKey = Object.getOwnPropertySymbols(kafka).find(
      (key) => key.description === 'private:Kafka:createCluster'
    )
    expect(factoryKey).toBeDefined()
    const factory = kafka[factoryKey]
    let observedCluster
    kafka[factoryKey] = (options) => {
      observedCluster = factory(options)
      return observedCluster
    }
    const admin = kafka.admin()
    p.cleanup.add('metadata cache witness admin', () =>
      admin.disconnect({ timeout: p.wholeMs(5000) })
    )
    p.removers.push(admin.on(admin.events.REQUEST, ({ payload }) => p.requests.push(payload)))
    await p.invoke(() => admin.connect())
    await p.invoke(() => observedCluster.addTargetTopic(this.topic))
    const targetBaseline = [...observedCluster.targetTopics].sort()
    p.requests.length = 0
    const descriptions = await p.invoke(() =>
      admin.describeGroups(this.groupIds, { includeAuthorizedOperations: true })
    )
    expect(descriptions.groups.map(({ groupId }) => groupId).sort()).toEqual(
      [...this.groupIds].sort()
    )
    const requests = [...p.requests]
    const findCoordinatorRequests = requests.filter(({ apiName }) => apiName === 'FindCoordinator')
    const consumerDescribeRequests = requests.filter(
      ({ apiName }) => apiName === 'ConsumerGroupDescribe'
    )
    const classicDescribeRequests = requests.filter(({ apiName }) => apiName === 'DescribeGroups')
    expect(findCoordinatorRequests).toHaveLength(5)
    expect(consumerDescribeRequests).toHaveLength(2)
    expect(classicDescribeRequests).toHaveLength(2)
    expect(new Set(classicDescribeRequests.map(({ broker }) => broker)).size).toBe(2)
    const groupMasks = descriptions.groups.map(({ authorizedOperations }) => authorizedOperations)
    expect(groupMasks).toEqual(independentGroupMasks)
    for (const mask of groupMasks) {
      expect(Number.isInteger(mask)).toBe(true)
      expect((mask >>> 0) & (1 << 8)).toBe(1 << 8)
    }
    for (const options of [{}, { includeAuthorizedOperations: false }]) {
      expect(
        (await p.invoke(() => admin.describeGroups(this.groupIds, options))).groups.map(
          ({ authorizedOperations }) => authorizedOperations
        )
      ).toEqual(this.groupIds.map(() => null))
    }
    const independent = await p.invoke(() =>
      this.seed.metadata([this.topic], { includeTopicAuthorizedOperations: true })
    )
    const independentTopicMask = independent.topicMetadata[0].topicAuthorizedOperations
    const metadata = await p.invoke(() =>
      admin.fetchTopicMetadata({ topics: [this.topic], includeTopicAuthorizedOperations: true })
    )
    const topicMask = metadata.topics[0].authorizedOperations
    expect(topicMask).toBe(independentTopicMask)
    expect((topicMask >>> 0) & (1 << 8)).toBe(1 << 8)
    for (const options of [{}, { includeTopicAuthorizedOperations: false }])
      expect(
        (await p.invoke(() => admin.fetchTopicMetadata({ topics: [this.topic], ...options })))
          .topics[0].authorizedOperations
      ).toBeNull()
    const all = await p.invoke(() => admin.fetchTopicMetadata())
    expect(all.topics.map(({ name }) => name).sort()).toEqual(
      (await p.invoke(() => admin.listTopics())).sort()
    )
    for (const topic of all.topics) {
      expect(typeof topic.topicId).toBe('string')
      expect(typeof topic.isInternal).toBe('boolean')
    }
    expect([...observedCluster.targetTopics].sort()).toEqual(targetBaseline)
    this.record('authorization masks and coordinator reuse', {
      cacheTargetIds: targetBaseline,
      cacheRestored: true,
      groupIds: this.groupIds,
      topicMask,
      groupMasks,
      coordinatorBrokers: classicDescribeRequests.map(({ broker }) => broker),
      findCoordinatorCount: 5,
      classicDescribeCount: 2,
      consumerDescribeCount: 2,
    })
  }

  /**
   * Observes a frozen HW100 corpus, actual first batch and raw commit positions.
   * @returns {Promise<void>}
   * @throws {Error} any offset, batch, isolation or deletion mismatch
   */
  async offsets() {
    const p = this.probe
    const admin = this.admin
    const topic = await p.topic({ name: 'lag', replicationFactor: 2 })
    const producer = await p.client('producer', { idempotent: true, batchSize: 1048576, linger: 0 })
    for (let batch = 0; batch < 5; batch++) {
      const result = await p.invoke(() =>
        producer.send({
          topic,
          messages: Array.from({ length: 20 }, (_, index) => ({
            value: `lag-${batch * 20 + index}`,
          })),
        })
      )
      expect(result).toHaveLength(1)
      expect(result[0].baseOffset).toBe(String(batch * 20))
    }
    const physical = (await p.physical(topic)).get(0)
    expect(physical.endOffset).toBe('100')
    expect([
      ...new Set(physical.records.map(({ batchContext }) => batchContext.firstOffset)),
    ]).toEqual(['0', '20', '40', '60', '80'])
    const groupId = this.groupIds[0]
    const consumer = await p.client('consumer', {
      groupId: `e2e-batch-${this.id}`,
      maxBytesPerPartition: 1,
      maxBytes: 1,
      minBytes: 1,
    })
    await p.invoke(() => consumer.subscribe({ topic, fromBeginning: true }))
    const actualBatchOffsets = []
    let actualBatchNext
    await p.invoke(() =>
      consumer.run({
        autoCommit: false,
        eachBatchAutoResolve: false,
        eachBatch: async ({ batch, pause }) => {
          if (actualBatchNext !== undefined) throw new Error('Unexpected second batch')
          actualBatchOffsets.push(...batch.messages.map(({ offset }) => offset))
          actualBatchNext = String(BigInt(batch.lastOffset()) + 1n)
          pause()
        },
      })
    )
    await p.poll(async () => actualBatchNext !== undefined)
    expect(actualBatchOffsets).toEqual(Array.from({ length: 20 }, (_, index) => String(index)))
    expect(actualBatchNext).toBe('20')
    await p.invoke(() => consumer.stop())
    const rawCommittedPositions = []
    const committedLags = []
    const batchLag = String(100n - BigInt(actualBatchNext))
    expect(batchLag).toBe('80')
    for (const offset of ['0', '20', '50']) {
      const highBefore = (await p.invoke(() => admin.fetchTopicOffsets(topic)))[0].high
      expect(highBefore).toBe('100')
      await p.invoke(() =>
        admin.setOffsets({ groupId, topic, partitions: [{ partition: 0, offset }] })
      )
      const fetched = await p.invoke(() =>
        admin.fetchOffsets({ groupId, topics: [topic], resolveOffsets: false })
      )
      const raw = fetched[0].partitions[0].offset
      expect(raw).toBe(offset)
      rawCommittedPositions.push(raw)
      committedLags.push(String(100n - BigInt(raw)))
      const highAfter = (await p.invoke(() => admin.fetchTopicOffsets(topic)))[0].high
      expect(highAfter).toBe('100')
      expect(batchLag === committedLags.at(-1)).toBe(offset === '20')
    }
    expect(rawCommittedPositions).toEqual(['0', '20', '50'])
    expect(committedLags).toEqual(['100', '80', '50'])
    const groups = this.groupIds.slice(0, 3)
    for (const [index, id] of groups.entries())
      await p.invoke(() =>
        admin.setOffsets({
          groupId: id,
          topic,
          partitions: [{ partition: 0, offset: String(index * 20) }],
        })
      )
    p.requests.length = 0
    const rows = await p.invoke(() =>
      admin.fetchOffsetsForGroups({
        groups: groups.map((id) => ({ groupId: id, topics: [{ topic, partitions: [0] }] })),
        requireStable: true,
      })
    )
    p.items(rows)
    const offsetFetchRequests = p.requests.filter(({ apiName }) => apiName === 'OffsetFetch')
    expect(offsetFetchRequests).toHaveLength(2)
    expect(new Set(offsetFetchRequests.map(({ broker }) => broker)).size).toBe(2)
    for (const row of rows)
      for (const requestedTopic of row.topics) p.items(requestedTopic.partitions)
    expect(rows.map(({ groupId: id }) => id)).toEqual(groups)
    expect(rows.map((row) => row.topics[0].partitions[0].offset)).toEqual(['0', '20', '40'])
    const deleted = await p.invoke(() =>
      admin.deleteOffsets({ groupId, topics: [{ topic, partitions: [0] }] })
    )
    const deletedOffsetPartition = deleted.topics[0].partitions[0]
    expect(deletedOffsetPartition.errorCode).toBe(0)
    const rawOffsetAfterDelete = (
      await p.invoke(() => admin.fetchOffsets({ groupId, topics: [topic], resolveOffsets: false }))
    )[0].partitions[0].offset
    expect(rawOffsetAfterDelete).toBe('-1')
    expect(
      await p.invoke(() =>
        admin.deleteTopicRecords({ topic, partitions: [{ partition: 0, offset: '10' }] })
      )
    ).toBeUndefined()
    expect((await p.invoke(() => admin.fetchTopicOffsets(topic)))[0].low).toBe('10')
    this.record('committed lag and raw offset deletion', {
      topicId: p.topics.find(({ name }) => name === topic).topicId,
      batchNextOffset: actualBatchNext,
      batchBoundaries: actualBatchOffsets,
      batchLagOffset: batchLag,
      commitBoundaries: rawCommittedPositions,
      lagBoundaries: committedLags,
      deletedOffset: rawOffsetAfterDelete,
      retainedLow: '10',
    })
  }

  /**
   * Aborts and fences real open transactions; independently reads physical and visible sets.
   * @returns {Promise<void>}
   * @throws {Error} diagnostics, abort visibility or fencing contract mismatch
   */
  async transactions() {
    const p = this.probe
    const admin = this.admin
    const topic = await p.topic({ name: 'transactions' })
    const transactionalId = `e2e-txn-${this.id}`
    const producer = await p.client('producer', {
      transactionalId,
      idempotent: true,
      maxInFlightRequests: 1,
    })
    const warmup = await p.invoke(() => producer.transaction())
    await p.invoke(() => warmup.send({ topic, messages: [{ value: 'committed' }] }))
    await p.invoke(() => warmup.commit())
    const transaction = await p.invoke(() => producer.transaction())
    await p.invoke(() => transaction.send({ topic, messages: [{ value: 'aborted' }] }))
    p.effects.add('settle orphan transaction', () =>
      admin.fenceProducers({ transactionalIds: [transactionalId] })
    )
    const trueHighWatermark = (await p.physical(topic)).get(0).endOffset
    const committedHigh = (
      await p.invoke(() => admin.fetchTopicOffsets(topic, { isolationLevel: 'read_committed' }))
    )[0].high
    const uncommittedHigh = (
      await p.invoke(() => admin.fetchTopicOffsets(topic, { isolationLevel: 'read_uncommitted' }))
    )[0].high
    expect(BigInt(committedHigh)).toBeLessThan(BigInt(trueHighWatermark))
    expect(uncommittedHigh).toBe(trueHighWatermark)
    const description = (
      await p.invoke(() => admin.describeTransactions({ transactionalIds: [transactionalId] }))
    )[0]
    p.items(description)
    expect(description.state).toBe('Ongoing')
    expect(description.topics).toContainEqual({ topic, partitions: [0] })
    const topicPartitions = [{ topic, partitions: [0] }]
    const producers = await p.invoke(() => admin.describeProducers({ topicPartitions }))
    const partition = producers[0].partitions[0]
    p.items(partition)
    const state = partition.activeProducers.find(
      ({ producerId }) => producerId === description.producerId
    )
    expect(state.producerEpoch).toBe(description.producerEpoch)
    expect(state.coordinatorEpoch).toBeGreaterThanOrEqual(0)
    expect(BigInt(state.currentTxnStartOffset)).toBeGreaterThanOrEqual(0n)
    const metadata = await p.invoke(() => admin.fetchTopicMetadata({ topics: [topic] }))
    const leader = metadata.topics[0].partitions[0].leader
    const explicit = await p.invoke(() =>
      admin.describeProducers({ topicPartitions, brokerId: leader })
    )
    p.items(explicit[0].partitions)
    expect(explicit[0].partitions[0].activeProducers).toContainEqual(state)
    const ranges = await p.invoke(() => this.seed.apiVersions())
    const offered = ranges[66]
    expect(offered).toBeDefined()
    const filters = { states: ['Ongoing'], producerIds: [state.producerId] }
    for (const [option, needed, value] of [
      ['durationFilter', 1, 0],
      ['transactionalIdPattern', 2, `^${transactionalId}$`],
    ]) {
      if (offered.maxVersion >= needed) filters[option] = value
      else {
        const refusal = await p
          .invoke(() => admin.listTransactions({ ...filters, [option]: value }))
          .then(
            () => null,
            (error) => error
          )
        expect(refusal).toMatchObject({
          name: 'KafkaJSServerDoesNotSupportApiKey',
          apiName: 'ListTransactions',
        })
        expect(refusal.message).toContain(`option ${option} needs v${needed}`)
        expect(refusal.message).toContain(
          `broker offers v${offered.minVersion}-v${offered.maxVersion}`
        )
      }
    }
    const listed = await p.invoke(() => admin.listTransactions(filters))
    expect(listed.transactions).toContainEqual({
      transactionalId,
      producerId: state.producerId,
      state: 'Ongoing',
    })
    expect(
      await p.invoke(() =>
        admin.abortTransaction({
          topic,
          partition: 0,
          producerId: state.producerId,
          producerEpoch: state.producerEpoch,
          coordinatorEpoch: state.coordinatorEpoch,
        })
      )
    ).toBeUndefined()
    const physicalAbortedIds = (await p.physical(topic))
      .get(0)
      .records.filter(({ value }) => value.toString() === 'aborted')
      .map(({ value }) => value.toString())
    const expectedAbortedIds = ['aborted']
    expect(physicalAbortedIds).toEqual(expectedAbortedIds)
    let committedVisibleIds
    await p.poll(async () => {
      committedVisibleIds = (
        await p.invoke(() => readData(p.baseOptions.brokers, topic, { committed: true }))
      ).map(({ value }) => value.toString())
      return committedVisibleIds.length > 0
    })
    const expectedCommittedIds = ['committed']
    expect(committedVisibleIds).toEqual(expectedCommittedIds)
    const handlerIds = []
    const consumer = await p.client('consumer', {
      groupId: `e2e-visible-${this.id}`,
      readUncommitted: false,
    })
    await p.invoke(() => consumer.subscribe({ topic, fromBeginning: true }))
    await p.invoke(() =>
      consumer.run({
        autoCommit: false,
        eachBatch: async ({ batch, pause }) => {
          handlerIds.push(...batch.messages.map(({ value }) => value.toString()))
          pause()
        },
      })
    )
    await p.poll(async () => handlerIds.length > 0)
    await p.invoke(() => consumer.stop())
    expect(handlerIds).toEqual(expectedCommittedIds)
    const fencedId = `e2e-fence-${this.id}`
    const fencedProducer = await p.client('producer', {
      transactionalId: fencedId,
      idempotent: true,
      maxInFlightRequests: 1,
    })
    const open = await p.invoke(() => fencedProducer.transaction())
    await p.invoke(() => open.send({ topic, messages: [{ value: 'fenced' }] }))
    const before = (
      await p.invoke(() => admin.describeTransactions({ transactionalIds: [fencedId] }))
    )[0]
    p.items(before)
    p.effects.add('settle fenced transaction', () =>
      admin.fenceProducers({ transactionalIds: [fencedId] })
    )
    const fenced = (await p.invoke(() => admin.fenceProducers({ transactionalIds: [fencedId] })))[0]
    p.items(fenced)
    expect(
      fenced.producerId !== before.producerId || fenced.producerEpoch !== before.producerEpoch
    ).toBe(true)
    let fencedSendCause = await p
      .invoke(() => open.send({ topic, messages: [{ value: 'must-refuse' }] }))
      .then(
        () => null,
        (error) => error
      )
    while (fencedSendCause?.cause) fencedSendCause = fencedSendCause.cause
    expect(fencedSendCause.code).toBe(90)
    this.record('transaction isolation administrative abort and fence', {
      transactionalId,
      producerId: state.producerId,
      producerEpochCode: state.producerEpoch,
      coordinatorEpochCode: state.coordinatorEpoch,
      physicalIds: physicalAbortedIds,
      committedIds: committedVisibleIds,
      handlerIds,
      physicalCount: physicalAbortedIds.length,
      committedCount: committedVisibleIds.length,
      handlerCount: handlerIds.length,
      committedHigh,
      uncommittedHigh,
      fencedCode: fencedSendCause.code,
    })
  }

  /**
   * Reads the finalized feature levels from each broker's own metadata.
   * @param {number[]} nodeIds brokers to ask
   * @returns {Promise<object[]>} finalized levels by feature name, one entry per broker in order
   * @throws {Error} broker failure or budget exhaustion
   */
  async finalizedLevels(nodeIds) {
    const p = this.probe
    const levels = []
    for (const brokerId of nodeIds)
      levels.push(
        (await p.invoke(() => this.admin.describeFeatures({ brokerId }))).finalizedFeatures
      )
    return levels
  }

  /**
   * Creates a topic and waits until every broker's own metadata holds it. A broker that has the
   * topic has applied every metadata record committed before this call, so a later read from it
   * cannot be stale about an earlier change.
   * @param {{host: string, port: number}[]} brokers brokers that must have caught up
   * @returns {Promise<void>}
   * @throws {Error} broker failure or budget exhaustion before every broker has the topic
   */
  async metadataBarrier(brokers) {
    const p = this.probe
    const topic = await p.topic({ name: 'barrier' })
    const sources = []
    for (const { host, port } of brokers) sources.push(await this.raw(host, port))
    await p.poll(async () => {
      for (const source of sources) {
        const { topicMetadata } = await p.invoke(() =>
          source.metadata([topic], { allowAutoTopicCreation: false, lenient: true })
        )
        if (topicMetadata[0]?.topicErrorCode !== 0) return false
      }
      return true
    })
  }

  /**
   * Restores a fenced node, validates features and removes a real metrics subscription.
   * @returns {Promise<void>}
   * @throws {Error} broker health, election, feature or resource mismatch
   */
  async cluster() {
    const p = this.probe
    const admin = this.admin
    const before = await p.invoke(() => admin.describeCluster())
    expect(typeof before.clusterId).toBe('string')
    expect(before.clusterId.length).toBeGreaterThan(0)
    expect(before.brokers).toHaveLength(3)
    expect(before.brokers.map(({ nodeId }) => nodeId)).toContain(before.controller)
    await p.poll(async () => {
      const metadata = await p.invoke(() => admin.fetchTopicMetadata({ topics: [this.topic] }))
      return metadata.topics[0].partitions.every(
        ({ replicas, isr }) => replicas.length === isr.length
      )
    })
    const thirdId = Number((await p.host('kafka3', ['printenv', 'KAFKA_NODE_ID'])).trim())
    const third = before.brokers.find(({ nodeId }) => nodeId === thirdId)
    expect(third).toBeDefined()
    await p.control('stop', 'kafka3')
    let fencedBroker
    await p.poll(async () => {
      const observation = await p.invoke(() =>
        admin.describeCluster({ includeFencedBrokers: true })
      )
      fencedBroker = observation.brokers.find(({ nodeId }) => nodeId === third.nodeId)
      return fencedBroker?.fenced === true
    })
    expect(fencedBroker.fenced).toBe(true)
    await p.control('start', 'kafka3')
    await p.poll(async () => {
      const all = await p.invoke(() => admin.describeCluster({ includeFencedBrokers: true }))
      const metadata = await p.invoke(() => admin.fetchTopicMetadata({ topics: [this.topic] }))
      return (
        all.brokers.length === 3 &&
        all.brokers.every(({ fenced }) => !fenced) &&
        metadata.topics[0].partitions.every(({ replicas, isr }) => replicas.length === isr.length)
      )
    })
    const featuresBeforeValidation = await p.invoke(() => admin.describeFeatures())
    const lowered = Object.entries(featuresBeforeValidation.finalizedFeatures)
      .filter(
        ([key, { maxVersionLevel }]) =>
          !['metadata.version', 'kraft.version'].includes(key) &&
          featuresBeforeValidation.supportedFeatures[key]?.minVersion < maxVersionLevel
      )
      .sort(([a], [b]) => a.localeCompare(b))[0]
    if (!lowered) throw new Error('No finalized feature has a lower supported level')
    const [name, { maxVersionLevel: original }] = lowered
    const nodeIds = before.brokers.map(({ nodeId }) => nodeId)
    p.effects.add('restore validated feature level', async (signal) => {
      p.items(
        await admin.updateFeatures({
          features: [{ name, maxVersionLevel: original, upgradeType: 'upgrade' }],
        })
      )
      await pollUntil(signal, p.remaining(), async () =>
        (await this.finalizedLevels(nodeIds)).every(
          (levels) => levels[name]?.maxVersionLevel === original
        )
      )
    })
    const updated = await p.invoke(() =>
      admin.updateFeatures({
        validateOnly: true,
        features: [{ name, maxVersionLevel: original - 1, upgradeType: 'safe-downgrade' }],
      })
    )
    p.items(updated)
    expect(updated.map(({ feature }) => feature)).toEqual([name])
    await this.metadataBarrier(before.brokers)
    expect(await this.finalizedLevels(nodeIds)).toEqual(
      nodeIds.map(() => featuresBeforeValidation.finalizedFeatures)
    )
    const resourceName = `e2e-metrics-${this.id}`
    const entries = [
      { name: 'interval.ms', value: '1000' },
      { name: 'match', value: 'client_id=e2e-live-admin' },
    ]
    const remove = () =>
      admin.incrementalAlterConfigs({
        resources: [
          {
            type: 16,
            name: resourceName,
            configEntries: entries.map(({ name: key }) => ({
              name: key,
              operation: AlterConfigOpTypes.DELETE,
            })),
          },
        ],
      })
    p.effects.add('remove metrics subscription', remove)
    const created = await p.invoke(() =>
      admin.incrementalAlterConfigs({
        resources: [
          {
            type: 16,
            name: resourceName,
            configEntries: entries.map((entry) => ({
              ...entry,
              operation: AlterConfigOpTypes.SET,
            })),
          },
        ],
      })
    )
    p.items(created.resources)
    const resources = await p.invoke(() => admin.listConfigResources({ resourceTypes: [16] }))
    const metricsResourcePresent = resources.resources.some(
      ({ name: key, type }) => key === resourceName && type === 16
    )
    expect(metricsResourcePresent).toBe(true)
    const described = await p.invoke(() =>
      admin.describeConfigs({
        resources: [
          { type: 16, name: resourceName, configNames: entries.map(({ name: key }) => key) },
        ],
      })
    )
    p.items(described.resources)
    for (const entry of entries)
      expect(described.resources[0].configEntries).toContainEqual(
        expect.objectContaining(asDescribed(entry))
      )
    p.items((await p.invoke(remove)).resources)
    let metricsResourceAbsentAfterDeletion = false
    await p.poll(async () => {
      const listed = await p.invoke(() => admin.listConfigResources({ resourceTypes: [16] }))
      metricsResourceAbsentAfterDeletion = !listed.resources.some(
        ({ name: key }) => key === resourceName
      )
      return metricsResourceAbsentAfterDeletion
    })
    expect(metricsResourceAbsentAfterDeletion).toBe(true)
    this.record('cluster features and configuration resources', {
      clusterId: before.clusterId,
      fencedBroker: third.nodeId,
      brokerRestored: true,
      metricsPresent: metricsResourcePresent,
      metricsAbsent: metricsResourceAbsentAfterDeletion,
      featuresRestored: true,
    })
  }

  /**
   * Exercises legacy returns, validation-only state, reassignment and canonical ID deletion.
   * @returns {Promise<void>}
   * @throws {Error} changed legacy contract or failed readback
   */
  async legacy() {
    const p = this.probe
    const admin = this.admin
    const topic = `e2e-p9d-admin-ledger-legacy-${this.id}`
    p.cleanup.add('delete recreated topic', () => admin.deleteTopics({ topics: [topic] }))
    const definition = { topic, numPartitions: 1, replicationFactor: 2 }
    expect(
      await p.invoke(() => admin.createTopics({ validateOnly: true, topics: [definition] }))
    ).toBe(true)
    expect(await p.invoke(() => admin.listTopics())).not.toContain(topic)
    expect(await p.invoke(() => admin.createTopics({ topics: [definition] }))).toBe(true)
    expect(await p.invoke(() => admin.createTopics({ topics: [definition] }))).toBe(false)
    const metadata = () => p.invoke(() => admin.fetchTopicMetadata({ topics: [topic] }))
    const original = (await metadata()).topics[0]
    expect(
      await p.invoke(() =>
        admin.createPartitions({ validateOnly: true, topicPartitions: [{ topic, count: 2 }] })
      )
    ).toBe(true)
    expect((await metadata()).topics[0].partitions).toHaveLength(1)
    expect(
      await p.invoke(() => admin.createPartitions({ topicPartitions: [{ topic, count: 2 }] }))
    ).toBe(true)
    expect((await metadata()).topics[0].partitions).toHaveLength(2)
    const resource = { type: 2, name: topic }
    const configs = () =>
      p.invoke(() =>
        admin.describeConfigs({ resources: [{ ...resource, configNames: ['retention.ms'] }] })
      )
    const before = await configs()
    p.items(before.resources)
    const candidate = [{ name: 'retention.ms', value: '600000' }]
    p.items(
      (
        await p.invoke(() =>
          admin.alterConfigs({
            validateOnly: true,
            resources: [{ ...resource, configEntries: candidate }],
          })
        )
      ).resources
    )
    expect(await configs()).toEqual(before)
    p.items(
      (
        await p.invoke(() =>
          admin.alterConfigs({
            validateOnly: false,
            resources: [{ ...resource, configEntries: candidate }],
          })
        )
      ).resources
    )
    expect((await configs()).resources[0].configEntries).toContainEqual(
      expect.objectContaining(asDescribed(candidate[0]))
    )
    const changes = [{ name: 'retention.ms', value: '-1', operation: AlterConfigOpTypes.SET }]
    p.items(
      (
        await p.invoke(() =>
          admin.incrementalAlterConfigs({
            validateOnly: true,
            resources: [{ ...resource, configEntries: changes }],
          })
        )
      ).resources
    )
    expect((await configs()).resources[0].configEntries).toContainEqual(
      expect.objectContaining(asDescribed(candidate[0]))
    )
    p.items(
      (
        await p.invoke(() =>
          admin.incrementalAlterConfigs({ resources: [{ ...resource, configEntries: changes }] })
        )
      ).resources
    )
    expect((await configs()).resources[0].configEntries).toContainEqual(
      expect.objectContaining(asDescribed({ name: 'retention.ms', value: '-1' }))
    )
    const replicas = (await metadata()).topics[0].partitions[0].replicas
    const reassignment = (nodes) =>
      admin.alterPartitionReassignments({
        allowReplicationFactorChange: false,
        topics: [{ topic, partitionAssignment: [{ partition: 0, replicas: nodes }] }],
      })
    await expect(p.invoke(() => reassignment(replicas.slice(0, 1)))).rejects.toThrow()
    expect((await metadata()).topics[0].partitions[0].replicas).toEqual(replicas)
    const reordered = [...replicas].reverse()
    expect(await p.invoke(() => reassignment(reordered))).toBeUndefined()
    await p.invoke(() => admin.listPartitionReassignments({ topics: [{ topic, partitions: [0] }] }))
    await p.poll(
      async () =>
        JSON.stringify((await metadata()).topics[0].partitions[0].replicas) ===
        JSON.stringify(reordered)
    )
    const elected = await p.invoke(() =>
      admin.electLeaders({
        electionType: 'preferred',
        topicPartitions: [{ topic, partitions: [0] }],
      })
    )
    for (const row of elected)
      for (const partition of row.partitions) expect([0, 84]).toContain(partition.errorCode)
    await p.poll(async () => (await metadata()).topics[0].partitions[0].leader === reordered[0])
    const nodes = (await p.invoke(() => admin.describeCluster())).brokers.map(
      ({ nodeId }) => nodeId
    )
    const replacement = nodes.find((nodeId) => !replicas.includes(nodeId))
    expect(Number.isInteger(replacement)).toBe(true)
    const movingTopic = await p.topic({
      name: 'ongoing',
      replicaAssignment: [{ partition: 0, replicas }],
    })
    const movementProducer = await p.client('producer', {
      idempotent: true,
      maxInFlightRequests: 1,
    })
    for (let batch = 0; batch < 16; batch++) {
      await p.invoke(() =>
        movementProducer.send({
          topic: movingTopic,
          acks: -1,
          messages: Array.from({ length: 4 }, (_, record) => {
            const value = Buffer.alloc(65536)
            value.write(`move-${batch}-${record}`)
            return { partition: 0, value }
          }),
        })
      )
    }
    const throttleKeys = [
      'leader.replication.throttled.rate',
      'follower.replication.throttled.rate',
    ]
    const throttleResources = nodes.map((nodeId) => ({
      type: 4,
      name: String(nodeId),
      configNames: throttleKeys,
    }))
    const originalThrottles = await p.invoke(() =>
      admin.describeConfigs({ resources: throttleResources })
    )
    p.items(originalThrottles.resources)
    p.effects.add('restore reassignment rate overrides', async () => {
      const restored = await admin.incrementalAlterConfigs({
        resources: originalThrottles.resources.map((row) => ({
          type: row.resourceType,
          name: row.resourceName,
          configEntries: row.configEntries.map((entry) => ({
            name: entry.configName,
            operation:
              entry.configSource === 2 ? AlterConfigOpTypes.SET : AlterConfigOpTypes.DELETE,
            value: entry.configSource === 2 ? entry.configValue : null,
          })),
        })),
      })
      p.items(restored.resources)
      const readback = await admin.describeConfigs({ resources: throttleResources })
      p.items(readback.resources)
      expect(readback.resources).toEqual(originalThrottles.resources)
    })
    p.effects.add('remove topic reassignment throttles', async () => {
      const removed = await admin.incrementalAlterConfigs({
        resources: [
          {
            type: 2,
            name: movingTopic,
            configEntries: [
              'leader.replication.throttled.replicas',
              'follower.replication.throttled.replicas',
            ].map((name) => ({ name, operation: AlterConfigOpTypes.DELETE })),
          },
        ],
      })
      p.items(removed.resources)
    })
    const throttled = await p.invoke(() =>
      admin.incrementalAlterConfigs({
        resources: [
          ...nodes.map((nodeId) => ({
            type: 4,
            name: String(nodeId),
            configEntries: throttleKeys.map((name) => ({
              name,
              value: '262144',
              operation: AlterConfigOpTypes.SET,
            })),
          })),
          {
            type: 2,
            name: movingTopic,
            configEntries: [
              'leader.replication.throttled.replicas',
              'follower.replication.throttled.replicas',
            ].map((name) => ({ name, value: '*', operation: AlterConfigOpTypes.SET })),
          },
        ],
      })
    )
    p.items(throttled.resources)
    const desired = [replicas[0], replacement]
    expect(
      await p.invoke(() =>
        admin.alterPartitionReassignments({
          allowReplicationFactorChange: false,
          topics: [
            { topic: movingTopic, partitionAssignment: [{ partition: 0, replicas: desired }] },
          ],
        })
      )
    ).toBeUndefined()
    const ongoing = await p.invoke(() =>
      admin.listPartitionReassignments({ topics: [{ topic: movingTopic, partitions: [0] }] })
    )
    const moving = ongoing.topics
      .find(({ name }) => name === movingTopic)
      ?.partitions.find(({ partition }) => partition === 0)
    expect(moving).toBeDefined()
    expect(moving.addingReplicas).toContain(replacement)
    expect(moving.removingReplicas).toContain(replicas[1])
    p.record('observed ongoing replica copy', {
      topicId: p.topics.find(({ name }) => name === movingTopic).topicId,
      replicaIds: moving.replicas,
      addingReplicaIds: moving.addingReplicas,
      removingReplicaIds: moving.removingReplicas,
    })
    await p.poll(async () => {
      const state = await p.invoke(() => admin.fetchTopicMetadata({ topics: [movingTopic] }))
      return (
        JSON.stringify(state.topics[0].partitions[0].replicas) === JSON.stringify(desired) &&
        state.topics[0].partitions[0].isr.length === 2
      )
    })
    const low = (await p.invoke(() => admin.fetchTopicOffsets(topic)))[0].low
    expect(
      (await p.invoke(() => admin.fetchTopicOffsetsByTimestamp(topic, 'earliestLocal')))[0].offset
    ).toBe(low)
    expect(
      (await p.invoke(() => admin.fetchTopicOffsetsByTimestamp(topic, 'latestTiered')))[0].offset
    ).toBe('-1')
    expect(
      (await p.invoke(() => admin.fetchTopicOffsetsByTimestamp(topic, 'earliestPendingUpload')))[0]
        .offset
    ).toBe('-1')
    const directories = await p.invoke(() =>
      admin.describeLogDirs({ topicPartitions: [{ topic, partitions: [0] }] })
    )
    p.items(directories)
    for (const broker of directories) p.items(broker.logDirs)
    const listed = await p.invoke(() => admin.listGroups())
    for (const groupId of this.groupIds)
      expect(listed.groups.map((group) => group.groupId)).toContain(groupId)
    const memberGroup = `e2e-member-${this.id}`
    const groupInstanceId = `instance-${this.id}`
    p.cleanup.add('delete member group', async () =>
      p.items(await admin.deleteGroups([memberGroup]))
    )
    const member = await p.client('consumer', {
      groupId: memberGroup,
      groupInstanceId,
      groupProtocol: 'classic',
    })
    await p.invoke(() => member.subscribe({ topic, fromBeginning: true }))
    await p.invoke(() => member.run({ eachMessage: async () => {} }))
    await p.poll(async () => {
      const description = await p.invoke(() => admin.describeGroups([memberGroup]))
      return description.groups[0].members.some((row) => row.groupInstanceId === groupInstanceId)
    })
    await p.invoke(() => member.stop())
    const removed = await p.invoke(() =>
      admin.removeMembersFromConsumerGroup({ groupId: memberGroup, members: [{ groupInstanceId }] })
    )
    p.items(removed)
    expect(removed).toHaveLength(1)
    expect(removed[0].groupInstanceId).toBe(groupInstanceId)
    await p.invoke(() => member.disconnect())
    const acl = {
      resourceType: 2,
      resourceName: topic,
      resourcePatternType: 3,
      principal: `User:e2e-acl-${this.id}`,
      host: '*',
      operation: 8,
      permissionType: 3,
    }
    p.effects.add('remove topic ACL', async () => {
      deletedAcls(await admin.deleteAcls({ filters: [acl] }))
    })
    expect(await p.invoke(() => admin.createAcls({ acl: [acl] }))).toBe(true)
    const described = await p.invoke(() => admin.describeAcls(acl))
    expect(described.resources).toHaveLength(1)
    await this.removeAcls(admin, acl, [acl])
    const deletedTopicId = original.topicId
    expect(await p.invoke(() => admin.deleteTopics({ topicIds: [deletedTopicId] }))).toBeUndefined()
    await p.poll(async () => !(await p.invoke(() => admin.listTopics())).includes(topic))
    expect(await p.invoke(() => admin.createTopics({ topics: [definition] }))).toBe(true)
    const recreated = (await metadata()).topics[0]
    const newTopicId = recreated.topicId
    expect(newTopicId).not.toBe(deletedTopicId)
    expect(recreated.partitions).toHaveLength(1)
    this.record('legacy administration reassignment and topic identity', {
      deletedTopicId,
      newTopicId,
      earliestLocalOffset: low,
      latestTieredOffset: '-1',
      earliestPendingUploadOffset: '-1',
      replicaIds: reordered,
      directoryCount: directories.length,
    })
  }

  /**
   * Authenticates fresh TLS connections before and after credential deletion in both diagnostic modes.
   * @returns {Promise<void>}
   * @throws {Error} credential result, authentication or secret-hygiene mismatch
   */
  async scram() {
    const p = this.probe
    const admin = this.admin
    const { ssl } = saslConnectionOpts()
    const previousBuffers = process.env.KAFKAJS_DEBUG_PROTOCOL_BUFFERS
    const restoreBuffers = () => {
      if (previousBuffers === undefined) delete process.env.KAFKAJS_DEBUG_PROTOCOL_BUFFERS
      else process.env.KAFKAJS_DEBUG_PROTOCOL_BUFFERS = previousBuffers
    }
    p.effects.add('restore buffer diagnostics', restoreBuffers)
    for (const mechanism of ['SCRAM-SHA-256', 'SCRAM-SHA-512']) {
      const user = `e2e-scram-${this.id}-${mechanism.slice(-3)}`
      const password = randomBytes(32).toString('hex')
      const salt = randomBytes(32)
      const digest = mechanism === 'SCRAM-SHA-256' ? 'sha256' : 'sha512'
      const salted = pbkdf2Sync(password, salt, 4096, digest === 'sha256' ? 32 : 64, digest)
      p.credentials.push(Buffer.from(password), salt, salted)
      const diagnosticErrors = []
      const deletions = [{ user, mechanism }]
      p.effects.add('remove SCRAM credentials', async () => {
        const result = await admin.alterUserScramCredentials({ deletions })
        for (const row of result) if (![0, 91].includes(row.errorCode)) p.items(row)
      })
      p.items(
        await p.invoke(() =>
          admin.alterUserScramCredentials({
            upsertions: [{ user, mechanism, iterations: 4096, password, salt }],
          })
        )
      )
      const described = await p.invoke(() => admin.describeUserScramCredentials({ users: [user] }))
      p.items(described)
      expect(described[0].credentials).toEqual([{ mechanism, iterations: 4096 }])
      for (const buffers of ['0', '1']) {
        process.env.KAFKAJS_DEBUG_PROTOCOL_BUFFERS = buffers
        const client = await p.start({
          brokers: saslBrokers(),
          ssl,
          sasl: { mechanism: mechanism.toLowerCase(), username: user, password },
        })
        expect(Array.isArray(await p.invoke(() => client.listTopics()))).toBe(true)
        await p.invoke(() => client.disconnect({ timeout: 5000 }))
      }
      p.items(await p.invoke(() => admin.alterUserScramCredentials({ deletions })))
      for (const buffers of ['0', '1']) {
        process.env.KAFKAJS_DEBUG_PROTOCOL_BUFFERS = buffers
        let error = await p
          .start({
            brokers: saslBrokers(),
            ssl,
            sasl: { mechanism: mechanism.toLowerCase(), username: user, password },
          })
          .then(
            () => null,
            (failure) => failure
          )
        const failures = []
        diagnosticErrors.push(error)
        while (error) {
          failures.push(error)
          error = error.cause
        }
        const scramAuthenticationAfterDeletion = !failures.some(
          (failure) =>
            failure.name === 'KafkaJSSASLAuthenticationError' ||
            failure.type === 'SASL_AUTHENTICATION_FAILED'
        )
        expect(scramAuthenticationAfterDeletion).toBe(false)
      }
      const diagnostics = inspect(
        { logs: p.logs, requests: p.requests, errors: diagnosticErrors },
        { depth: null, maxArrayLength: null }
      )
      const leakedSecret = [
        password,
        salt.toString('hex'),
        salt.toString('base64'),
        salted.toString('hex'),
        salted.toString('base64'),
      ].some((marker) => diagnostics.includes(marker))
      expect(leakedSecret).toBe(false)
    }
    restoreBuffers()
    this.record('SCRAM lifecycle and diagnostic hygiene', {
      mechanismCount: 2,
      authenticationBeforePresent: true,
      authenticationAfterAbsent: true,
      secretAbsent: true,
    })
  }

  /**
   * Proves minimum grants and typed denials with fresh nonsuperuser connections.
   * This same case also runs from cold under the ACL profile.
   * @returns {Promise<void>}
   * @throws {Error} missing positive control, wrong failure level or cleanup failure
   */
  async permissions() {
    const p = this.probe
    const root = this.admin
    const user = `e2e-permissions-${this.id}`
    const password = randomBytes(32).toString('hex')
    p.credentials.push(Buffer.from(password))
    const mechanism = 'SCRAM-SHA-256'
    p.effects.add('delete permission principal', () =>
      root.alterUserScramCredentials({ deletions: [{ user, mechanism }] })
    )
    p.items(
      await p.invoke(() =>
        root.alterUserScramCredentials({
          upsertions: [{ user, mechanism, iterations: 4096, password }],
        })
      )
    )
    const { ssl } = saslConnectionOpts()
    const client = await p.start({
      brokers: saslBrokers(),
      ssl,
      sasl: { mechanism: 'scram-sha-256', username: user, password },
    })
    const topic = this.topic
    const groupId = this.groupIds[0]
    const maskAcls = [
      { resourceType: 2, resourceName: topic, operation: 3, permissionType: 3 },
      { resourceType: 2, resourceName: topic, operation: 10, permissionType: 3 },
      { resourceType: 2, resourceName: topic, operation: 4, permissionType: 2 },
      { resourceType: 3, resourceName: groupId, operation: 3, permissionType: 3 },
      { resourceType: 3, resourceName: groupId, operation: 6, permissionType: 2 },
    ].map((row) => ({ ...row, resourcePatternType: 3, principal: `User:${user}`, host: '*' }))
    for (const acl of maskAcls)
      p.effects.add('remove mask ACL', async () => {
        deletedAcls(await root.deleteAcls({ filters: [acl] }))
      })
    expect(await p.invoke(() => root.createAcls({ acl: maskAcls }))).toBe(true)
    const [maskHost, maskPort] = saslBrokers()[0].split(':')
    const maskRaw = new Broker({
      logger: p.logger,
      connectionPool: new Pool({
        host: maskHost,
        port: Number(maskPort),
        ssl,
        sasl: { mechanism: 'scram-sha-256', username: user, password },
        logger: p.logger,
        socketFactory: (args) => p.socket(args),
      }),
    })
    p.cleanup.add('known authorizer raw source', () =>
      maskRaw.disconnect({ drainTimeout: Math.min(5000, p.remaining()) })
    )
    await p.invoke(() => maskRaw.connect())
    const coordinator = (
      await p.invoke(() => maskRaw.findGroupCoordinator({ groupId, coordinatorType: 0 }))
    ).coordinator
    const groupRaw = new Broker({
      logger: p.logger,
      connectionPool: new Pool({
        host: coordinator.host,
        port: coordinator.port,
        ssl,
        sasl: { mechanism: 'scram-sha-256', username: user, password },
        logger: p.logger,
        socketFactory: (args) => p.socket(args),
      }),
    })
    p.cleanup.add('known authorizer group source', () =>
      groupRaw.disconnect({ drainTimeout: Math.min(5000, p.remaining()) })
    )
    await p.invoke(() => groupRaw.connect())
    const rawTopicMask = (
      await p.invoke(() => maskRaw.metadata([topic], { includeTopicAuthorizedOperations: true }))
    ).topicMetadata[0].topicAuthorizedOperations
    const rawGroup = (
      await p.invoke(() =>
        groupRaw.describeGroups({ groupIds: [groupId], includeAuthorizedOperations: true })
      )
    ).groups[0]
    p.items(rawGroup)
    const publicTopicMask = (
      await p.invoke(() =>
        client.fetchTopicMetadata({ topics: [topic], includeTopicAuthorizedOperations: true })
      )
    ).topics[0].authorizedOperations
    const publicGroupMask = (
      await p.invoke(() => client.describeGroups([groupId], { includeAuthorizedOperations: true }))
    ).groups[0].authorizedOperations
    expect(publicTopicMask).toBe(rawTopicMask)
    expect(publicGroupMask).toBe(rawGroup.authorizedOperations)
    expect(publicTopicMask >>> 0).toBe((1 << 3) | (1 << 8) | (1 << 10))
    expect(publicGroupMask >>> 0).toBe((1 << 3) | (1 << 8))
    p.record('known authorizer allow and deny masks', {
      topicMask: publicTopicMask,
      groupMask: publicGroupMask,
      groupId,
    })
    for (const acl of maskAcls) await this.removeAcls(root, acl, [acl])
    const txId = `e2e-acl-txn-${this.id}`
    const producer = await p.client('producer', { transactionalId: txId, idempotent: true })
    const warm = await p.invoke(() => producer.transaction())
    await p.invoke(() => warm.send({ topic, messages: [{ value: 'acl-warm' }] }))
    await p.invoke(() => warm.commit())
    const open = await p.invoke(() => producer.transaction())
    await p.invoke(() => open.send({ topic, messages: [{ value: 'acl-open' }] }))
    p.effects.add('settle permission transaction', () =>
      root.fenceProducers({ transactionalIds: [txId] })
    )
    const partition = (
      await p.invoke(() =>
        root.describeProducers({ topicPartitions: [{ topic, partitions: [0] }] })
      )
    )[0].partitions[0]
    p.items(partition)
    const txn = (await p.invoke(() => root.describeTransactions({ transactionalIds: [txId] })))[0]
    p.items(txn)
    const state = partition.activeProducers.find(({ producerId }) => producerId === txn.producerId)
    const features = await p.invoke(() => root.describeFeatures())
    const [feature, level] = Object.entries(features.finalizedFeatures)[0]
    const meta = await p.invoke(() => root.fetchTopicMetadata({ topics: [topic] }))
    const replicas = meta.topics[0].partitions[0].replicas
    const targets = [{ topic, partitions: [0] }]
    const deletionTopic = await p.topic({ name: 'acl-deletion' })
    let deletionId = p.topics.find(({ name }) => name === deletionTopic).topicId
    const directoryRows = await p.invoke(() =>
      root.describeReplicaLogDirs({ replicas: [{ topic, partition: 0, brokerId: replicas[0] }] })
    )
    p.items(directoryRows)
    const checks = [
      {
        name: 'metadata',
        resourceType: 2,
        resourceName: topic,
        operation: 8,
        code: 29,
        call: () =>
          client.fetchTopicMetadata({ topics: [topic], includeTopicAuthorizedOperations: true }),
      },
      {
        name: 'groups',
        item: (rows) => rows.groups[0],
        resourceType: 3,
        resourceName: groupId,
        operation: 8,
        code: 30,
        call: () => client.describeGroups([groupId], { includeAuthorizedOperations: true }),
      },
      {
        name: 'offset deletion',
        resourceType: 3,
        resourceName: groupId,
        operation: 6,
        code: 30,
        call: () => client.deleteOffsets({ groupId, topics: targets }),
      },
      {
        name: 'producer diagnostics',
        resourceType: 2,
        resourceName: topic,
        operation: 3,
        code: 29,
        call: () => client.describeProducers({ topicPartitions: targets }),
        item: (rows) => rows[0].partitions[0],
      },
      {
        name: 'transaction diagnostics',
        resourceType: 5,
        resourceName: txId,
        operation: 8,
        code: 53,
        call: () => client.describeTransactions({ transactionalIds: [txId] }),
        item: (rows) => rows[0],
      },
      {
        name: 'transaction abort',
        resourceType: 4,
        resourceName: 'kafka-cluster',
        operation: 9,
        code: 31,
        call: () =>
          client.abortTransaction({
            topic,
            partition: 0,
            producerId: state.producerId,
            producerEpoch: state.producerEpoch,
            coordinatorEpoch: state.coordinatorEpoch,
          }),
      },
      {
        name: 'producer fencing',
        resourceType: 5,
        resourceName: txId,
        operation: 4,
        code: 53,
        call: () => client.fenceProducers({ transactionalIds: [txId] }),
        item: (rows) => rows[0],
      },
      {
        name: 'leader election',
        resourceType: 4,
        resourceName: 'kafka-cluster',
        operation: 7,
        code: 31,
        call: () => client.electLeaders({ electionType: 'preferred', topicPartitions: targets }),
        item: (rows) => rows[0].partitions[0],
        positiveCodes: [0, 84],
      },
      {
        name: 'features',
        resourceType: 4,
        resourceName: 'kafka-cluster',
        operation: 7,
        code: 31,
        call: () =>
          client.updateFeatures({
            validateOnly: true,
            features: [{ name: feature, maxVersionLevel: level.maxVersionLevel }],
          }),
        item: (rows) => rows[0],
      },
      {
        name: 'configuration resources',
        resourceType: 4,
        resourceName: 'kafka-cluster',
        operation: 10,
        code: 31,
        call: () => client.listConfigResources({ resourceTypes: [16] }),
      },
      {
        name: 'directories',
        resourceType: 4,
        resourceName: 'kafka-cluster',
        operation: 8,
        code: 31,
        call: () => client.describeLogDirs({ topicPartitions: targets }),
        item: (rows) => rows[0],
      },
      {
        name: 'reassignment',
        resourceType: 4,
        resourceName: 'kafka-cluster',
        operation: 7,
        code: 31,
        call: () =>
          client.alterPartitionReassignments({
            topics: [{ topic, partitionAssignment: [{ partition: 0, replicas }] }],
            allowReplicationFactorChange: false,
          }),
      },
      {
        name: 'directory mutation',
        resourceType: 4,
        resourceName: 'kafka-cluster',
        operation: 7,
        code: 31,
        call: () =>
          client.alterReplicaLogDirs({
            replicas: [
              {
                topic,
                partition: 0,
                brokerId: replicas[0],
                logDir: directoryRows[0].currentLogDir,
              },
            ],
          }),
        item: (rows) => rows[0],
      },
      {
        name: 'list transactions',
        resourceType: 4,
        resourceName: 'kafka-cluster',
        operation: 8,
        code: 31,
        call: () => client.listTransactions({}),
      },
      {
        name: 'config mutation',
        legacyItem: true,
        resourceType: 2,
        resourceName: topic,
        operation: 11,
        code: 29,
        call: () =>
          client.incrementalAlterConfigs({
            validateOnly: true,
            resources: [
              {
                type: 2,
                name: topic,
                configEntries: [
                  { name: 'retention.ms', value: '-1', operation: AlterConfigOpTypes.SET },
                ],
              },
            ],
          }),
        item: (rows) => rows.resources[0],
      },
      {
        name: 'quota description',
        resourceType: 4,
        resourceName: 'kafka-cluster',
        operation: 10,
        code: 31,
        call: () => client.describeClientQuotas({ components: [], strict: false }),
      },
      {
        name: 'SCRAM mutation',
        resourceType: 4,
        resourceName: 'kafka-cluster',
        operation: 7,
        code: 31,
        call: () =>
          client.alterUserScramCredentials({
            upsertions: [{ user, mechanism, iterations: 4096, password }],
          }),
        item: (rows) => rows[0],
      },
      {
        name: 'ID deletion',
        resourceType: 2,
        resourceName: deletionTopic,
        operation: 6,
        code: 29,
        call: () => client.deleteTopics({ topicIds: [deletionId] }),
        destructive: true,
      },
      {
        name: 'quota',
        resourceType: 4,
        resourceName: 'kafka-cluster',
        operation: 11,
        code: 31,
        call: () =>
          client.alterClientQuotas({
            validateOnly: true,
            entries: [
              {
                entity: [{ entityType: 'user', entityName: user }],
                ops: [{ key: 'producer_byte_rate', value: 65536 }],
              },
            ],
          }),
        item: (rows) => rows[0],
      },
      {
        name: 'SCRAM',
        resourceType: 4,
        resourceName: 'kafka-cluster',
        operation: 8,
        code: 31,
        call: () => client.describeUserScramCredentials({ users: [user] }),
        positiveItem: (rows) => rows[0],
      },
    ]
    const observedCodes = []
    const observedTypes = []
    for (const check of checks) {
      const acl = {
        resourceType: check.resourceType,
        resourceName: check.resourceName,
        resourcePatternType: 3,
        principal: `User:${user}`,
        host: '*',
        operation: check.operation,
        permissionType: 3,
      }
      p.effects.add('remove permission ACL', async () => {
        deletedAcls(await root.deleteAcls({ filters: [{ ...acl, permissionType: 1 }] }))
      })
      const dependencies = []
      if (['offset deletion'].includes(check.name))
        dependencies.push({ resourceType: 2, resourceName: topic, operation: 3 })
      if (
        [
          'producer diagnostics',
          'transaction abort',
          'reassignment',
          'directory mutation',
        ].includes(check.name)
      )
        dependencies.push({ resourceType: 2, resourceName: topic, operation: 8 })
      if (check.name === 'list transactions')
        dependencies.push({ resourceType: 5, resourceName: txId, operation: 8 })
      const dependencyAcls = dependencies.map((entry) => ({ ...acl, ...entry }))
      for (const dependency of dependencyAcls) {
        p.effects.add('remove dependency ACL', async () => {
          deletedAcls(await root.deleteAcls({ filters: [dependency] }))
        })
        expect(await p.invoke(() => root.createAcls({ acl: [dependency] }))).toBe(true)
      }
      expect(await p.invoke(() => root.createAcls({ acl: [acl] }))).toBe(true)
      {
        const positive = await p.invoke(check.call)
        if (check.item || check.positiveItem)
          expect(check.positiveCodes || [0]).toContain(
            (check.item || check.positiveItem)(positive).errorCode
          )
      }
      if (check.destructive) {
        await p.poll(async () => !(await p.invoke(() => root.listTopics())).includes(deletionTopic))
        expect(
          await p.invoke(() =>
            root.createTopics({
              topics: [{ topic: deletionTopic, numPartitions: 1, replicationFactor: 2 }],
            })
          )
        ).toBe(true)
        const recreated = await p.invoke(() => root.fetchTopicMetadata({ topics: [deletionTopic] }))
        const replacement = recreated.topics[0].topicId
        expect(replacement).not.toBe(deletionId)
        deletionId = replacement
      }
      expect(await p.invoke(() => root.createAcls({ acl: [{ ...acl, permissionType: 2 }] }))).toBe(
        true
      )
      if (check.item) {
        const failed = check.item(await p.invoke(check.call))
        expect(failed.errorCode).toBe(check.code)
        if (check.legacyItem) {
          expect(typeof failed.errorMessage).toBe('string')
          observedTypes.push('legacy item errorCode')
        } else {
          expect(failed.error).toMatchObject({ code: check.code, name: 'KafkaJSProtocolError' })
          observedTypes.push(failed.error.type)
        }
      } else {
        const failure = await p.invoke(check.call).then(
          () => null,
          (error) => error
        )
        expect(failure).toMatchObject({ code: check.code, name: 'KafkaJSProtocolError' })
        observedTypes.push(failure.type)
      }
      observedCodes.push(check.code)
      await this.removeAcls(root, { ...acl, permissionType: 1 }, [
        acl,
        { ...acl, permissionType: 2 },
      ])
      for (const dependency of dependencyAcls) await this.removeAcls(root, dependency, [dependency])
      p.record(`permission ${check.name}`, {
        operationId: check.name,
        authorizationCode: check.code,
        dependencyCount: dependencyAcls.length,
        positivePresent: true,
      })
    }
    this.record('ACL positive controls and typed permission failures', {
      permissionCount: checks.length,
      operationIds: checks.map(({ name }) => name),
      authorizationTypeIds: observedTypes,
      authorizationCode: observedCodes,
      principalId: user,
    })
  }
}

suite('admin ledger', () => {
  test(
    'completes the admin ledger against broker state',
    liveCase(budget, async (signal) => {
      const probe = new AdminLiveProbe({ stem: 'admin-ledger', signal, budgetMs: budget })
      const ledger = new AdminLedgerScenario(probe)
      await probe.finish(async () => {
        await ledger.authorization()
        await ledger.offsets()
        await ledger.transactions()
        await ledger.cluster()
        await ledger.legacy()
        await ledger.scram()
        await ledger.permissions()
        expect(ledger.records.size).toBe(7)
      })
    }),
    caseTimeout(budget)
  )
})
