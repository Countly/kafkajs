/** Independently reconstructs archived acquisitions, receipts, terminality and membership.
 * This module never imports the workload owner or its judge. */
class ArchivedCycle {
  /**
   * @param {number} cycle zero-based cycle, 0 to 29
   * @returns {{ cycle: number, phase: string, reset: boolean, records: object[],
   *   perPartition: number, first: bigint, end: string }} the fixed corpus of the cycle: sixty
   *   records with explicit partitions `i % 3` and the immutable sequence `cycle * 60 + i`, and
   *   the cumulative per-partition log end the cycle must reach
   */
  static plan(cycle) {
    const recordsPerCycle = 60
    const partitions = 3
    const cyclesPerPhase = 10
    const phases = ['steady', 'reset', 'members']
    const kind = phases[Math.floor(cycle / cyclesPerPhase)]
    const perPartition = recordsPerCycle / partitions
    return {
      cycle,
      phase: `share-${kind}`,
      reset: kind === 'reset',
      records: Array.from({ length: recordsPerCycle }, (_, i) => {
        const sequence = cycle * recordsPerCycle + i
        return {
          sequence,
          partition: i % partitions,
          key: `soak-key-${sequence}`,
          value: `soak-value-${sequence}`,
        }
      }),
      perPartition,
      first: BigInt(perPartition) * BigInt(cycle),
      end: String(perPartition * (cycle + 1)),
    }
  }

  /**
   * Checks one cycle's observations against its plan.
   * @param {ReturnType<typeof ArchivedCycle.plan>} plan the cycle's fixed corpus
   * @param {object} observation what the cycle saw
   * @param {Array<{ partition: number, identity: string }>} observation.produced acknowledged records
   * @param {string[]} observation.log identities of this cycle's offsets in the raw log
   * @param {Record<number, { end: string, count: number }>} observation.logs each partition's raw
   *   log end and record count
   * @param {Array<{ identity: string, deliveryCount: number }>} observation.handler handler entries
   * @param {string[]} observation.held identities started and held when the reset was forced
   * @param {Array<{ partition: number, firstOffset: string, lastOffset: string }>} observation.accepted
   *   successful public Accept results
   * @param {object[]} observation.broker the three terminal partition rows
   * @param {object} observation.members join, leave, additional member add and remove facts
   * @param {object|null} observation.reset the observed reset and reopen, or null
   * @param {{ active: boolean, delivered: string[] }} observation.verifier the same-group verifier
   * @param {boolean} observation.clientsDisconnected whether every cycle client left
   * @returns {string[]} one line for every check that failed; empty when the cycle holds
   */
  static verify(plan, observation) {
    const violations = []
    const fail = (message) => violations.push(`cycle ${plan.cycle}: ${message}`)
    const corpus = ArchivedCycle.corpus(plan, observation.produced ?? [], fail)
    ArchivedCycle.log(plan, corpus, observation, fail)
    ArchivedCycle.handler(plan, corpus, observation, fail)
    ArchivedCycle.accepts(corpus, observation, fail)
    ArchivedCycle.broker(plan, observation.broker ?? [], fail)
    ArchivedCycle.membership(plan, observation, fail)
    if (observation.positions?.length !== 2)
      fail('both terminal position observations are required')
    for (const position of observation.positions ?? []) {
      ArchivedCycle.position(plan, position, fail)
      if (
        position.topicId !== observation.verifier?.topicId ||
        position.target?.groupId !== observation.verifier?.groupId ||
        position.target?.topic !== JSON.parse(observation.produced?.[0]?.identity ?? '[]')[0]
      )
        fail('the terminal proof belongs to another group or topic')
    }
    ArchivedCycle.verifier(observation.verifier, 5000, fail)
    ArchivedCycle.sequence(observation, fail)
    if (observation.timing?.cycle !== plan.cycle || observation.timing?.kind !== 'cycle')
      fail('the proof belongs to another cycle')

    return violations
  }

  /**
   * @param {ReturnType<typeof ArchivedCycle.plan>} plan the cycle
   * @param {Array<{ identity: string }>} produced acknowledged records in send order
   * @param {(message: string) => void} fail
   * @returns {Map<string, { partition: number, offset: bigint }>} the acknowledged identities
   */
  static corpus(plan, produced, fail) {
    const corpus = new Map()
    const next = new Map()
    if (produced.length !== plan.records.length) {
      fail(`${produced.length} acknowledged records, ${plan.records.length} expected`)
    }
    plan.records.forEach((record, i) => {
      let tuple
      try {
        tuple = JSON.parse(produced[i]?.identity)
      } catch {
        fail(`acknowledged record ${i} has no identity`)
        return
      }
      const [, partition, offset, key, value] = tuple
      const index = next.get(record.partition) ?? 0
      next.set(record.partition, index + 1)
      const hex = (text) => Buffer.from(text).toString('hex')
      if (
        partition !== record.partition ||
        key !== hex(record.key) ||
        value !== hex(record.value) ||
        offset !== String(plan.first + BigInt(index))
      ) {
        fail(`acknowledged record ${i} differs from the corpus`)
      }
      if (corpus.has(produced[i].identity)) fail(`acknowledged record ${i} repeats an identity`)
      corpus.set(produced[i].identity, { partition: record.partition, offset: BigInt(offset) })
    })

    return corpus
  }

  /**
   * @param {ReturnType<typeof ArchivedCycle.plan>} plan the cycle
   * @param {Map<string, object>} corpus the acknowledged identities
   * @param {object} observation the cycle's observations
   * @param {(message: string) => void} fail
   * @returns {void}
   */
  static log(plan, corpus, { log = [], logs = {}, prefix = [], acknowledgedPrefix = [] }, fail) {
    if (
      acknowledgedPrefix.length !== (plan.cycle + 1) * 60 ||
      new Set(acknowledgedPrefix).size !== acknowledgedPrefix.length ||
      JSON.stringify([...prefix].sort()) !== JSON.stringify([...acknowledgedPrefix].sort())
    )
      fail('the cumulative raw log identities differ from the acknowledged prefix')
    const once = new Map()
    for (const identity of log) once.set(identity, (once.get(identity) ?? 0) + 1)
    if (log.length !== corpus.size || [...corpus.keys()].some((id) => once.get(id) !== 1)) {
      fail('the raw log does not hold the acknowledged corpus exactly once')
    }
    for (let partition = 0; partition < 3; partition++) {
      const { end, count } = logs[partition] ?? {}
      if (end !== plan.end || count !== Number(plan.end)) {
        fail(`partition ${partition} raw log is not the cumulative prefix ending at ${plan.end}`)
      }
    }
  }

  /**
   * Every acknowledged identity reaches a handler, and a repeat is allowed only for an identity of
   * the held set frozen before the reset, with a strictly higher delivery count each time.
   * @param {ReturnType<typeof ArchivedCycle.plan>} plan the cycle
   * @param {Map<string, object>} corpus the acknowledged identities
   * @param {object} observation the cycle's observations
   * @param {(message: string) => void} fail
   * @returns {void}
   */
  static handler(plan, corpus, { handler = [], held = [], provenance = {}, verifier = {} }, fail) {
    const heldSet = new Set(held)
    if (!plan.reset && heldSet.size > 0) fail('a held set was frozen outside a reset cycle')
    if (plan.reset && heldSet.size === 0) fail('no held set was frozen before the reset')
    if ([...heldSet].some((id) => !corpus.has(id))) fail('a held identity is outside the corpus')
    const entries = new Map()
    const dispatched = new Set()
    for (const { identity, deliveryCount, acquisition, dispatch } of handler) {
      const row = provenance.acquisitions?.find((row) => row.id === acquisition)
      const exchange = provenance.exchanges?.find((exchange) => exchange.id === row?.exchange)
      const source = ArchivedCycle.partitionResponse(exchange, row)
      const offset = corpus.get(identity)?.offset
      const ranges =
        row?.acquiredRecords?.filter(
          (range) =>
            offset !== undefined &&
            BigInt(range.firstOffset) <= offset &&
            offset <= BigInt(range.lastOffset)
        ) ?? []
      const original = `${exchange?.frames?.[0]?.socket}:${exchange?.frames?.[0]?.id}:${row?.topicId}:${row?.partition}:${offset}`
      if (
        !row ||
        !source ||
        exchange.apiKey !== 78 ||
        !Number.isInteger(dispatch) ||
        row.partition !== corpus.get(identity)?.partition ||
        row.topic !== JSON.parse(identity)[0] ||
        row.topicId !== verifier.topicId ||
        exchange.groupId !== verifier.groupId ||
        ranges.length !== 1 ||
        ranges[0].deliveryCount !== deliveryCount ||
        source.acquiredRecords?.every(
          (range, index) => JSON.stringify(range) === JSON.stringify(row.acquiredRecords[index])
        ) !== true ||
        source.acquiredRecords.length !== row.acquiredRecords.length
      )
        fail('a handler lacks original successful acquisition provenance')
      if (dispatched.has(original)) fail('one original acquisition was dispatched more than once')
      dispatched.add(original)
      entries.set(identity, [...(entries.get(identity) ?? []), deliveryCount])
    }
    if ([...corpus.keys()].some((id) => !entries.has(id)))
      fail('an identity never reached a handler')
    for (const [identity, counts] of entries) {
      const valid = (count) => Number.isInteger(count) && count >= 1 && count <= 32767
      if (!corpus.has(identity)) fail('a handler received an identity outside the corpus')
      else if (!counts.every(valid)) fail('a delivery count is not a valid acquisition count')
      else if (counts.length > 1 && !heldSet.has(identity)) {
        fail('a duplicate delivery falls outside the frozen held set')
      } else if (heldSet.has(identity) && !counts.slice(1).every((c, i) => c > counts[i])) {
        fail('a replay did not raise its delivery count')
      }
    }
    for (const identity of heldSet) {
      if ((entries.get(identity)?.length ?? 0) < 2) fail('a held identity was not recovered')
    }
  }

  /**
   * @param {Map<string, { partition: number, offset: bigint }>} corpus the acknowledged identities
   * @param {object} observation Successful public ranges and their original attempt journals.
   * @param {object[]} observation.accepted Public Accept results.
   * @param {object} observation.provenance Original acquisitions, tickets and matching wire answers.
   * @param {(message: string) => void} fail
   * @returns {void}
   */
  static accepts(corpus, { accepted = [], provenance = {} }, fail) {
    const receipts = []
    const used = new Set()
    for (const range of accepted) {
      const matches = (provenance.outcomes ?? []).filter(
        (outcome) =>
          outcome.type === 'accept' &&
          !outcome.error &&
          outcome.topic === range.topic &&
          outcome.partition === range.partition &&
          outcome.firstOffset === range.firstOffset &&
          outcome.lastOffset === range.lastOffset
      )
      const covered = matches.filter((outcome) => {
        const acquisition = provenance.acquisitions?.find((row) => row.id === outcome.acquisition)
        const source = provenance.exchanges?.find(
          (exchange) => exchange.id === acquisition?.exchange
        )
        if (!source || !ArchivedCycle.partitionResponse(source, acquisition)) return false
        return (provenance.exchanges ?? []).some((exchange) => {
          const ticket = exchange.tickets.filter(
            (ticket) => ticket.acquisition === acquisition.id && !ticket.isRenewAck
          )
          if (ticket.length !== 1) return false
          const result = ArchivedCycle.partitionResponse(exchange, acquisition)
          if (
            !source ||
            !result ||
            (exchange.apiKey === 78 && result.acknowledgeErrorCode !== 0) ||
            exchange.open !== source.open ||
            exchange.pool !== source.pool ||
            exchange.generation !== source.generation ||
            exchange.memberId !== source.memberId ||
            exchange.frames[0].socket !== source.frames[0].socket ||
            exchange.request.isRenewAck ||
            exchange.frames[0].request.isRenewAck
          )
            return false
          const parts =
            exchange.frames[0].request.topics
              ?.filter((topic) => topic.topicId === acquisition.topicId)
              .flatMap((topic) =>
                topic.partitions.filter((part) => part.partitionIndex === range.partition)
              ) ?? []
          return (
            ticket.length === 1 &&
            parts.length === 1 &&
            ticket[0].batches.some(
              (batch) =>
                batch.firstOffset === range.firstOffset &&
                batch.lastOffset === range.lastOffset &&
                batch.acknowledgeTypes.every((type) => type === 1) &&
                parts[0].acknowledgementBatches.some(
                  (written) => JSON.stringify(written) === JSON.stringify(batch)
                )
            )
          )
        })
      })
      if (covered.length !== 1 || used.has(covered[0])) {
        fail('a successful Accept lacks a unique original request and partition receipt')
      } else {
        used.add(covered[0])
        receipts.push({ ...range, acquisition: covered[0].acquisition })
      }
    }
    const uncovered = [...corpus].filter(
      ([, { partition, offset }]) =>
        !receipts.some(
          (range) =>
            range.partition === partition &&
            BigInt(range.firstOffset) <= offset &&
            offset <= BigInt(range.lastOffset) &&
            provenance.acquisitions.some(
              (row) =>
                row.id === range.acquisition &&
                row.partition === partition &&
                row.acquiredRecords.some(
                  (acquired) =>
                    BigInt(acquired.firstOffset) <= offset && offset <= BigInt(acquired.lastOffset)
                )
            )
        )
    )
    if (uncovered.length > 0) fail(`${uncovered.length} identities lack a successful Accept`)
  }

  /**
   * @param {object} exchange Original exchange with socket-scoped wire identity.
   * @returns {object|null} Wire-validated successful response, including empty session deltas.
   */
  static response(exchange) {
    const frame = exchange?.frames?.[0]
    if (
      exchange?.error ||
      exchange?.frames?.length !== 1 ||
      ![78, 79].includes(exchange.apiKey) ||
      frame.apiKey !== exchange.apiKey ||
      frame.apiVersion !== 2 ||
      !frame.bytes ||
      !frame.responseBytes ||
      !Number.isInteger(exchange.responseIdentity) ||
      !Number.isInteger(frame.id) ||
      frame.request.groupId !== exchange.groupId ||
      frame.request.memberId !== exchange.memberId ||
      frame.endpoint !== exchange.endpoint ||
      exchange.response?.errorCode !== 0
    )
      return null
    let wire
    try {
      const { readRequestHeader } = require('../../testHelpers/fakeSocket')
      const bytes = Buffer.from(frame.bytes, 'base64')
      const header = readRequestHeader(bytes)
      const reader = require(
        exchange.apiKey === 78
          ? '../../testHelpers/wire/shareFetchV2'
          : '../../testHelpers/wire/shareAcknowledgeV2'
      )
      const written = reader.readRequest(bytes.subarray(header.byteLength))
      if (
        bytes.readInt32BE(0) !== bytes.length - 4 ||
        header.apiKey !== exchange.apiKey ||
        header.apiVersion !== 2 ||
        header.correlationId !== frame.correlationId ||
        JSON.stringify(written) !== JSON.stringify(frame.request)
      )
        return null
      wire = ArchivedCycle.wireResponse(frame)
      const fields = (responses) =>
        responses.map(({ topicId, partitions }) => ({
          topicId,
          partitions: partitions.map(
            ({ partitionIndex, errorCode, acknowledgeErrorCode, acquiredRecords }) => ({
              partitionIndex,
              errorCode,
              acknowledgeErrorCode,
              acquiredRecords,
            })
          ),
        }))
      if (
        wire?.errorCode !== 0 ||
        JSON.stringify(fields(wire.responses)) !==
          JSON.stringify(fields(exchange.response.responses))
      )
        return null
    } catch {
      return null
    }
    return wire
  }

  /**
   * @param {object} exchange Original exchange with socket-scoped wire identity.
   * @param {object} acquisition Original partition acquisition.
   * @returns {object|null} Sole successful matching partition response, or no receipt.
   */
  static partitionResponse(exchange, acquisition) {
    if (!acquisition) return null
    const response = ArchivedCycle.response(exchange)
    const parts =
      response?.responses
        .filter((topic) => topic.topicId === acquisition.topicId)
        .flatMap((topic) =>
          topic.partitions.filter((part) => part.partitionIndex === acquisition.partition)
        ) ?? []
    return parts.length === 1 && parts[0].errorCode === 0 ? parts[0] : null
  }

  /**
   * Reads partition errors/acquisition ranges directly from the matching socket response bytes.
   * @param {object} frame Original request and answer frames.
   * @returns {object|null} Raw top-level and partition results, including omitted partitions.
   * @throws {Error} On malformed frame bounds or fields; the receipt predicate rejects it.
   */
  static wireResponse(frame) {
    const Decoder = require('../../src/protocol/decoder')
    const bytes = Buffer.from(frame.responseBytes, 'base64')
    const decoder = new Decoder(bytes)
    if (decoder.readInt32() !== bytes.length - 4 || decoder.readInt32() !== frame.correlationId)
      return null
    decoder.readTaggedFields()
    decoder.readInt32()
    const errorCode = decoder.readInt16()
    decoder.readUVarIntString()
    decoder.readInt32()
    const topics = decoder.readUVarIntArray((decoder) => {
      const topicId = decoder.readUuid()
      const partitions = decoder.readUVarIntArray((decoder) => {
        const row = { partitionIndex: decoder.readInt32(), errorCode: decoder.readInt16() }
        decoder.readUVarIntString()
        if (frame.apiKey === 78) {
          row.acknowledgeErrorCode = decoder.readInt16()
          decoder.readUVarIntString()
        }
        decoder.readInt32()
        decoder.readInt32()
        decoder.readTaggedFields()
        if (frame.apiKey === 78) {
          decoder.readUVarIntBytes()
          row.acquiredRecords = decoder.readUVarIntArray((decoder) => {
            const range = {
              firstOffset: decoder.readInt64().toString(),
              lastOffset: decoder.readInt64().toString(),
              deliveryCount: decoder.readInt16(),
            }
            decoder.readTaggedFields()
            return range
          })
        }
        decoder.readTaggedFields()
        return row
      })
      decoder.readTaggedFields()
      return { topicId, partitions }
    })
    return { errorCode, responses: topics }
  }

  /**
   * Decodes a socket-matched heartbeat answer without using the Broker completion as a receipt.
   * @param {object} frame Original heartbeat request and response bytes.
   * @returns {object|null} Raw heartbeat fields, or no matched response.
   * @throws {Error} On malformed/truncated response fields.
   */
  static heartbeatResponse(frame) {
    const Decoder = require('../../src/protocol/decoder')
    const bytes = Buffer.from(frame.responseBytes, 'base64')
    const decoder = new Decoder(bytes)
    if (decoder.readInt32() !== bytes.length - 4 || decoder.readInt32() !== frame.correlationId)
      return null
    decoder.readTaggedFields()
    decoder.readInt32()
    const errorCode = decoder.readInt16()
    decoder.readUVarIntString()
    const memberId = decoder.readUVarIntString()
    const memberEpoch = decoder.readInt32()
    const heartbeatIntervalMs = decoder.readInt32()
    const present = decoder.readInt8()
    if (present !== -1 && present !== 1) return null
    if (present === 1) {
      decoder.readUVarIntArray((entry) => {
        entry.readUuid()
        entry.readUVarIntArray((entry) => entry.readInt32())
        entry.readTaggedFields()
      })
      decoder.readTaggedFields()
    }
    decoder.readTaggedFields()
    return decoder.canReadBytes(1)
      ? null
      : { errorCode, memberId, memberEpoch, heartbeatIntervalMs }
  }

  /**
   * Proves a member's departure from its invoked epoch -1 request and matched successful answer.
   * @param {object} provenance Original heartbeat call-through and wire archive.
   * @param {string} groupId Original group.
   * @param {string} memberId Original member, never inferred from disconnect completion.
   * @returns {object|null} The successful leave frame, or no departure proof.
   */
  static departure(provenance, groupId, memberId) {
    if (!groupId || !memberId) return null
    for (const beat of provenance.heartbeats ?? []) {
      if (
        beat.groupId !== groupId ||
        beat.memberId !== memberId ||
        beat.request?.groupId !== groupId ||
        beat.request?.memberId !== memberId ||
        beat.request?.memberEpoch !== -1
      )
        continue
      for (const frame of beat.frames ?? []) {
        if (
          frame.apiKey !== 76 ||
          frame.apiVersion !== 1 ||
          !Number.isInteger(frame.socket) ||
          frame.endpoint !== beat.endpoint ||
          !Number.isInteger(frame.correlationId) ||
          ![beat.at, frame.at, frame.answeredAt].every(Number.isFinite) ||
          frame.at < beat.at ||
          frame.answeredAt < frame.at ||
          frame.lostAt !== undefined ||
          !frame.responseBytes
        )
          continue
        try {
          const { readRequestHeader } = require('../../testHelpers/fakeSocket')
          const wire = require('../../testHelpers/wire/shareGroupHeartbeatV1')
          const bytes = Buffer.from(frame.bytes, 'base64')
          const header = readRequestHeader(bytes)
          const request = wire.readRequest(bytes.subarray(header.byteLength))
          const response = ArchivedCycle.heartbeatResponse(frame)
          if (
            bytes.readInt32BE(0) === bytes.length - 4 &&
            header.apiKey === 76 &&
            header.apiVersion === 1 &&
            header.correlationId === frame.correlationId &&
            JSON.stringify(request) === JSON.stringify(frame.request) &&
            request.groupId === groupId &&
            request.memberId === memberId &&
            request.memberEpoch === -1 &&
            response?.errorCode === 0 &&
            response.memberEpoch === -1 &&
            (response.memberId === null || response.memberId === memberId)
          )
            return frame
        } catch {}
      }
    }
    return null
  }

  /**
   * Recomputes terminality from raw named/all listings and independent ends.
   * @param {object} plan Fixed cumulative end.
   * @param {object} position Archived primitive observation.
   * @param {Function} fail Records a violation.
   * @returns {void}
   */
  static position(plan, position, fail) {
    const { terminalSharePosition } = require('../../testHelpers/shareLiveDiagnostics')
    if (
      !position?.target ||
      position.target.partitions.length !== 3 ||
      position.target.partitions.some(({ floor, end }) => floor !== '0' || end !== plan.end) ||
      !terminalSharePosition(position.target, position).settled
    )
      fail('the raw terminal listings or independent ends are incomplete or disagree')
  }

  /**
   * Reconstructs verifier activity from assignment reads and actual request chronology.
   * @param {object} verifier Original member observations.
   * @param {number} windowMs Required unchanged window.
   * @param {Function} fail Records a violation.
   * @returns {void}
   */
  static verifier(verifier = {}, windowMs, fail) {
    const start = verifier.events?.find(({ kind }) => kind === 'window-start')
    const end = verifier.events?.findLast(({ kind }) => kind === 'window-end')
    const owns = (assignment) =>
      assignment?.length === 1 &&
      assignment[0].topic === verifier.topic &&
      [...assignment[0].partitions].sort().join() === '0,1,2'
    const times = [verifier.openedAt, start?.at, end?.at, verifier.closedAt]
    const ordered =
      times.every(Number.isFinite) &&
      times.every((at, i) => !i || at >= times[i - 1]) &&
      Number.isFinite(start?.wallAt) &&
      Number.isFinite(end?.wallAt) &&
      end.wallAt >= start.wallAt &&
      verifier.events.every(
        ({ at }, i, events) =>
          Number.isFinite(at) &&
          at >= verifier.openedAt &&
          at <= verifier.closedAt &&
          (!i || at >= events[i - 1].at)
      )
    const joined = verifier.events?.filter(({ kind }) => kind === 'GROUP_JOIN') ?? []
    const covered = new Set()
    const sessions = new Map()
    const activity = []
    for (const exchange of verifier.provenance?.exchanges ?? []) {
      const frame = exchange.frames?.[0]
      if (!frame || exchange.apiKey !== 78 || exchange.memberId !== verifier.memberId) continue
      const key = `${frame.socket}:${exchange.open}:${exchange.pool}:${exchange.generation}`
      const epoch = frame.request.shareSessionEpoch
      const previous = sessions.get(key)
      const response = ArchivedCycle.response(exchange)
      const detail = {
        exchange: exchange.id,
        socket: frame.socket,
        open: exchange.open,
        pool: exchange.pool,
        generation: exchange.generation,
        epoch,
        at: frame.at,
        answeredAt: frame.answeredAt,
        partitions: [],
      }
      activity.push(detail)
      const valid =
        ordered &&
        response &&
        frame.request.isRenewAck === false &&
        frame.request.maxRecords > 0 &&
        frame.request.maxBytes > 0 &&
        exchange.groupId === verifier.groupId &&
        Number.isFinite(frame.at) &&
        Number.isFinite(frame.answeredAt) &&
        frame.at >= verifier.openedAt &&
        frame.answeredAt >= frame.at &&
        frame.answeredAt <= verifier.closedAt &&
        Number.isInteger(frame.socket) &&
        Number.isInteger(exchange.open) &&
        Number.isInteger(exchange.pool) &&
        Number.isInteger(exchange.generation) &&
        (epoch === 0 ||
          (previous &&
            epoch === (previous.epoch === 2147483647 ? 1 : previous.epoch + 1) &&
            frame.at >= previous.answeredAt))
      if (!valid) {
        sessions.delete(key)
        detail.reason = 'invalid wire ownership, response, session epoch or timestamps'
        continue
      }
      const partitions = epoch === 0 ? new Set() : previous.partitions
      const parts = (partition) =>
        response.responses
          .filter(({ topicId }) => topicId === verifier.topicId)
          .flatMap(({ partitions }) =>
            partitions.filter(({ partitionIndex }) => partitionIndex === partition)
          )
      for (const topic of frame.request.topics ?? []) {
        if (topic.topicId !== verifier.topicId) continue
        for (const { partitionIndex } of topic.partitions) {
          const rows = parts(partitionIndex)
          if (rows.length === 1 && rows[0].errorCode === 0) partitions.add(partitionIndex)
        }
      }
      for (const topic of frame.request.forgottenTopicsData ?? [])
        if (topic.topicId === verifier.topicId)
          for (const partition of topic.partitions) partitions.delete(partition)
      sessions.set(key, { partitions, epoch, answeredAt: frame.answeredAt })
      detail.partitions = [...partitions]
      const request = verifier.events.find(
        ({ kind, payload, at }) =>
          kind === 'REQUEST' &&
          payload.apiName === 'ShareFetch' &&
          payload.correlationId === frame.correlationId &&
          payload.broker === frame.endpoint &&
          payload.sentAt >= start.wallAt &&
          payload.sentAt <= end.wallAt &&
          at >= frame.answeredAt &&
          at <= end.at
      )
      if (frame.at < start.at || frame.answeredAt > end.at || !request) {
        detail.reason = 'request write, response or public completion outside observation window'
        continue
      }
      detail.requestAt = request.at
      // An empty incremental response still fetches the previously established session model.
      for (const partition of partitions) {
        const rows = parts(partition)
        if (
          rows.length === 0 ||
          (rows.length === 1 && rows[0].errorCode === 0 && rows[0].acquiredRecords?.length === 0)
        )
          covered.add(partition)
      }
    }
    const checks = {
      'ordered timestamps': ordered,
      'measured duration': ordered && end.at - start.at >= windowMs,
      'full endpoint assignment': owns(start?.payload) && owns(end?.payload),
      'all-partition in-window wire activity': [...covered].sort().join() === '0,1,2',
      'same-group member join':
        joined.length === 1 &&
        joined[0].payload.memberId === verifier.memberId &&
        joined[0].payload.groupId === verifier.groupId &&
        joined[0].at <= start?.at,
      'joined leave': verifier.events?.some(
        ({ kind, at }) => kind === 'DISCONNECT' && at >= end?.at && at <= verifier.closedAt
      ),
      'continuous assignment':
        start &&
        end &&
        !verifier.events
          .slice(verifier.events.indexOf(start), verifier.events.indexOf(end))
          .some(({ kind, payload }) => kind === 'ASSIGNMENT_CHANGED' && !owns(payload.assignment)),
      'declared duration': verifier.windowMs === windowMs,
      'member and topic identity': Boolean(verifier.memberId && verifier.topicId),
      'active empty verifier': verifier.active === true && verifier.delivered?.length === 0,
    }
    const failed = Object.keys(checks).filter((name) => !checks[name])
    if (failed.length)
      fail(
        'the same-group verifier activity or measured observation window is incomplete: ' +
          `${failed.join(', ')}; ${JSON.stringify({
            openedAt: verifier.openedAt,
            window: [start?.at, end?.at],
            wallWindow: [start?.wallAt, end?.wallAt],
            closedAt: verifier.closedAt,
            coveredPartitions: [...covered].sort(),
            missingPartitions: [0, 1, 2].filter((partition) => !covered.has(partition)),
            activity,
          })}`
      )
  }

  /**
   * Binds original reads, departures and the joined verifier to one enclosing observation.
   * @param {object} observation Original cycle or final proof.
   * @param {Function} fail Records a violation.
   * @returns {void}
   */
  static sequence({ timing = {}, positions = [], verifier = {}, members = {} }, fail) {
    const [before, after] = positions
    const departures =
      timing.kind === 'final'
        ? [members.bRemovedAt, members.aLeftAt, timing.startedAt]
        : [timing.startedAt, members.bRemovedAt, members.aLeftAt]
    const times = [
      ...departures,
      before?.startedAt,
      before?.completedAt,
      members.emptyAt,
      verifier.openedAt,
      verifier.closedAt,
      after?.startedAt,
      after?.completedAt,
      timing.completedAt,
      timing.due,
    ]
    if (
      !times.every(Number.isFinite) ||
      times.some((at, i) => i && at < times[i - 1]) ||
      !Number.isInteger(timing.cycle) ||
      positions.length !== 2 ||
      [before, after, verifier].some(
        (part) => part?.cycle !== timing.cycle || part?.kind !== timing.kind
      )
    )
      fail(
        'the terminal reads, departures and verifier are not bound to their original cycle window'
      )
  }

  /**
   * The terminal position oracle: three distinct partition rows, each answered without error,
   * with zero lag, a canonical start offset inside the log and the independently read end.
   * @param {ReturnType<typeof ArchivedCycle.plan>} plan the cycle
   * @param {object[]} rows the terminal partition rows
   * @param {(message: string) => void} fail
   * @returns {void}
   */
  static broker(plan, rows, fail) {
    const partitions = rows.map(({ partition }) => partition).sort()
    if (partitions.join() !== '0,1,2') fail('three distinct terminal partition rows are required')
    for (const row of rows) {
      const decimal = (value) => typeof value === 'string' && /^(0|[1-9][0-9]*)$/.test(value)
      if (
        row.errorCode !== 0 ||
        row.lag !== '0' ||
        row.logEnd !== plan.end ||
        !decimal(row.startOffset) ||
        BigInt(row.startOffset) > BigInt(plan.end)
      ) {
        fail(`partition ${row.partition} is not at its terminal position ${plan.end}`)
      }
    }
  }

  /**
   * Rejects writes after the reset socket's observed close, including attempts without answers.
   * The close's last frame orders same-millisecond events; older archives retain clock evidence.
   * @param {object} observation Original reset, close steps and request frames.
   * @param {(message: string) => void} fail Receives a retirement violation.
   * @returns {void}
   */
  static retirement(observation, fail) {
    const socket = observation.resetTarget?.socket ?? observation.reset?.oldSocket
    if (socket === undefined) return
    const steps = observation.steps ?? []
    const closed = steps.find((s) => s.step === 'socket-close' && s.socket === socket)
    const provenance = observation.provenance ?? {}
    const requests = [...(provenance.exchanges ?? []), ...(provenance.heartbeats ?? [])]
    if (
      steps.some((s) => s.step === 'retired-socket-write' && s.socket === socket) ||
      (closed &&
        requests.some((request) =>
          request.frames?.some(
            (frame) =>
              frame.socket === socket &&
              (frame.at > closed.at ||
                (Number.isSafeInteger(closed.lastFrame) && frame.id > closed.lastFrame))
          )
        ))
    )
      fail(`Retired reset socket ${socket} was written after close`)
  }

  /**
   * @param {ReturnType<typeof ArchivedCycle.plan>} plan the cycle
   * @param {object} observation the cycle's observations
   * @param {(message: string) => void} fail
   * @returns {void}
   */
  static membership(plan, observation, fail) {
    ArchivedCycle.retirement(observation, fail)
    const { members = {}, reset = null, verifier = {}, provenance = {} } = observation
    for (const label of ['A', 'B']) {
      const member = observation.memberObservations?.find((member) => member.member === label)
      const leave = ArchivedCycle.departure(provenance, verifier.groupId, member?.memberId)
      const removedAt = label === 'A' ? members.aLeftAt : members.bRemovedAt
      if (
        !leave ||
        !Number.isFinite(removedAt) ||
        leave.answeredAt > removedAt ||
        leave.at < observation.timing?.startedAt
      )
        fail(
          `member ${label} (${member?.memberId ?? 'unknown'}) lacks original successful leave evidence`
        )
    }
    if (!(members.aJoined && members.aLeft && members.bAdded && members.bRemoved)) {
      fail('the member joins, leaves and the additional member add and remove are incomplete')
    }
    if (plan.reset) {
      const reopened =
        reset?.apiKey === 78 &&
        Number.isInteger(reset.oldEpoch) &&
        reset.oldEpoch > 0 &&
        reset.nextEpoch === 0 &&
        Boolean(reset.reason) &&
        Boolean(reset.oldSocket) &&
        Boolean(reset.nextSocket) &&
        reset.oldSocket !== reset.nextSocket
      if (!reopened) fail('no original session reset and new epoch-zero open was observed')
    } else if (reset !== null) {
      fail('a reset was observed outside a reset cycle')
    }
    if (
      verifier.active !== true ||
      !Array.isArray(verifier.delivered) ||
      verifier.delivered.length > 0
    ) {
      fail('the same-group verifier was inactive or received a record')
    }
    if (observation.clientsDisconnected !== true) fail('a cycle client was still connected')
  }
}

module.exports = { ArchivedCycle }
