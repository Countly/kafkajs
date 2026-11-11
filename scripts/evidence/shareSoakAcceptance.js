const fs = require('node:fs')
const path = require('node:path')
const { isDeepStrictEqual: same } = require('node:util')
const { ArchivedCycle } = require('./shareSoakCycle')
const { verifySharePriming, verifySharePeriodic } = require('./sharePriming')
const { verifyProtocolAnswers } = require('./shareSoakProtocol')
const { verifyShareQuietHeap } = require('./shareQuietHeap')
const { problemsOf } = require('./verify')
const { PROFILES, childArgs, digest } = require('../ci/shareSoakContract')
const { cleanupTimestamp, validateJestReport } = require('../ci/naturalExit')
const { inventory, sourceIdentity, admit } = require('../ci/shareSoakJob')

/** @param {string} root Archive directory. @param {string} name Fixed required file. @returns {object} Parsed JSON. @throws {Error} On missing/invalid bytes. */
const json = (root, name) => JSON.parse(fs.readFileSync(path.join(root, name), 'utf8'))

/**
 * Reconstructs the original cleanup population without discounting generic timers or sockets.
 * @param {object} point Original resource snapshot.
 * @returns {object} Strict comparable source counts.
 * @throws {Error} On missing, invalid or inconsistent readings.
 */
const cleanupPopulation = (point) => {
  const count = (n) => Number.isSafeInteger(n) && n >= 0
  const counts = (value) =>
    value && typeof value === 'object' && !Array.isArray(value) && Object.values(value).every(count)
  if (
    !point ||
    !counts(point.listeners) ||
    !counts(point.activeResources) ||
    !count(point.queueDepth) ||
    !count(point.ledgerSize) ||
    !['created', 'destroyed', 'open'].every((k) => count(point.sockets?.[k])) ||
    point.sockets.created - point.sockets.destroyed !== point.sockets.open
  )
    throw new Error('Invalid original cleanup population')
  return {
    listeners: point.listeners,
    activeResources: point.activeResources,
    sockets: point.sockets.open,
    queueDepth: point.queueDepth,
    ledgerSize: point.ledgerSize,
  }
}

/**
 * Checks original configuration values and each broker's effective timing/fallback provenance.
 * @param {object} config Original setup readback.
 * @returns {string[]} Configuration failures.
 */
const configurationProblems = (config) => {
  const problems = []
  const reads = config?.reads ?? []
  const effective = config?.effective ?? []
  const entries = (resource) => resource?.configEntries ?? []
  const value = (resource, key) => entries(resource).find((entry) => entry.configName === key)
  const group = reads.find((r) => r.query?.name === config.groupId)?.resource
  if (
    reads.length !== 4 ||
    effective.length !== 3 ||
    new Set(effective.map((b) => b.nodeId)).size !== 3 ||
    reads.some(
      (r) =>
        r.resource?.errorCode !== 0 ||
        r.completedAt < config.startedAt ||
        r.completedAt > config.completedAt
    ) ||
    value(group, 'share.auto.offset.reset')?.configValue !== 'earliest' ||
    value(group, 'share.record.lock.duration.ms')?.configValue !== '30000'
  )
    problems.push('Original group configuration missing or changed')
  const topic = config?.topicMetadata?.topics?.find((t) => t.name === config.topic)
  const topicConfig = config?.topicConfig?.resources?.find((r) => r.resourceName === config.topic)
  if (
    !topic ||
    topic.topicId !== config.topicId ||
    topic.partitions?.length !== 3 ||
    topic.partitions.some((p) => p.replicas?.length !== 3 || new Set(p.replicas).size !== 3) ||
    topicConfig?.errorCode !== 0 ||
    value(topicConfig, 'min.insync.replicas')?.configValue !== '2' ||
    value(topicConfig, 'retention.ms')?.configValue !== '-1' ||
    value(topicConfig, 'cleanup.policy')?.configValue !== 'delete'
  )
    problems.push('Effective topic replication/retention configuration missing or changed')
  for (const broker of effective) {
    const raw = reads.find((r) => r.query?.name === String(broker.nodeId))?.resource
    for (const key of ['heartbeat.interval.ms', 'session.timeout.ms', 'assignment.interval.ms']) {
      const local = value(group, `share.${key}`)
      const fallback = local?.configValue == null || !Number.isInteger(local.configSource)
      const original = fallback ? value(raw, `group.share.${key}`) : local
      const observed = broker.timings?.[key]
      const min = Number(value(raw, `group.share.min.${key}`)?.configValue)
      const max = Number(value(raw, `group.share.max.${key}`)?.configValue)
      if (
        !original ||
        !observed ||
        !Number.isInteger(original.configSource) ||
        !Object.entries(original).every(([k, v]) => same(observed[k], v)) ||
        observed.fallback !== fallback ||
        observed.resourceName !== (fallback ? String(broker.nodeId) : config.groupId) ||
        !Number.isFinite(min) ||
        !Number.isFinite(max) ||
        Number(original.configValue) < min ||
        Number(original.configValue) > max
      )
        problems.push(`Unreconstructible effective timing: ${broker.nodeId}/${key}`)
    }
  }
  return problems
}

/**
 * Reconstructs all absolute slot, phase, work and cumulative stage allocations.
 * Historical lifecycleMs is deliberately absent from the accepted budget.
 * @param {object} done Original scope completion with uncapped timestamps.
 * @param {boolean} prime Whether this is the fixed short-shaped priming scope.
 * @returns {string[]} Deadline and allocation failures.
 */
const scheduleProblems = (done, prime) => {
  const problems = []
  const profile = prime || done.selector === 'SHARE_SOAK_E2E' ? PROFILES.short : PROFILES.full
  const timing = done.timing ?? {}
  const phases = timing.phases ?? []
  const stages = timing.stages ?? []
  const slots = timing.slots ?? []
  const workMs = prime ? 1010000 : profile.totalMs - 10000
  const budgets = {
    priming: prime ? 0 : 1020000,
    setup: 15000,
    transition: 5000,
    profile: profile.profileMs + 10000,
    cleanup: 20000,
  }
  if (
    !Number.isFinite(timing.startedAt) ||
    timing.workDue !== timing.startedAt + workMs ||
    timing.observedAt > timing.workDue ||
    phases.length !== 3 ||
    slots.length !== 30 ||
    stages.length < 5
  )
    problems.push('Original owner scope deadline or mandatory schedule missing')
  for (const [i, phase] of phases.entries())
    if (
      phase.phase !== i ||
      ![phase.startedAt, phase.due, phase.completedAt].every(Number.isFinite) ||
      phase.due !== phase.startedAt + profile.slotMs * 10 ||
      phase.completedAt < phase.due ||
      phase.completedAt > phase.due + 10000 ||
      (i && phase.startedAt < phases[i - 1].completedAt)
    )
      problems.push('Original phase deadline/order failure')
  if (phases.reduce((sum, p) => sum + p.completedAt - p.startedAt, 0) !== done.elapsedProfileMs)
    problems.push('Uncapped phase duration differs')
  for (const [i, slot] of slots.entries()) {
    const phase = phases[Math.floor(i / 10)]
    if (
      !phase ||
      slot.cycle !== i ||
      ![slot.startedAt, slot.due, slot.completedAt].every(Number.isFinite) ||
      slot.due !== phase.startedAt + ((i % 10) + 1) * profile.slotMs ||
      slot.startedAt < phase.startedAt ||
      slot.startedAt >= slot.due ||
      slot.completedAt < slot.due ||
      slot.completedAt > phase.completedAt ||
      (i && slot.startedAt < slots[i - 1].completedAt)
    )
      problems.push('Original slot deadline/order failure')
  }
  const spent = Object.fromEntries(Object.keys(budgets).map((k) => [k, 0]))
  for (const stage of stages) {
    if (
      !(stage.name in budgets) ||
      ![stage.startedAt, stage.due, stage.completedAt].every(Number.isFinite) ||
      stage.startedAt < timing.startedAt ||
      stage.completedAt < stage.startedAt ||
      stage.completedAt > stage.due ||
      stage.due > timing.workDue
    ) {
      problems.push('Original stage deadline/order failure')
      continue
    }
    spent[stage.name] += stage.completedAt - stage.startedAt
  }
  for (const [name, used] of Object.entries(spent))
    if (used > budgets[name]) problems.push(`Original cumulative ${name} allocation exceeded`)
  return problems
}

/**
 * Requires primitive producer acknowledgements and decoded raw records before reading summaries.
 * @param {object[]} rows One scope's ordered original observations.
 * @param {object} completion Scope completion row.
 * @returns {string[]} Independent cycle, final, schedule and cleanup failures.
 */
const scopeProblems = (rows, completion, contract) => {
  const problems = []
  const fail = (message) => problems.push(message)
  const original = rows.filter((r) => r.event === 'soak-original-cycle')
  const summaries = rows.filter((r) => r.event === 'soak-cycle')
  if (original.length !== 30 || summaries.length !== 30)
    return ['Thirty original cycles and summaries are required']
  problems.push(...scheduleProblems(completion, completion.scope === 'priming'))
  const schedules = rows.filter((r) => r.event === 'soak-schedule')
  const schedule = schedules[0]
  if (schedules.length !== 1 || !same(schedule.schedule, completion.schedule))
    fail('Missing or changed original schedule declaration')
  if (contract) {
    const expected = new Map(contract.source.files.map((entry) => [entry.path, entry.sha256]))
    if (
      !same(schedule?.config?.compose, [
        { file: 'docker-compose.yml', sha256: expected.get('docker-compose.yml') },
      ]) ||
      schedule?.config?.source?.sha256 !== expected.get('scripts/soak/soak.js') ||
      !Array.isArray(schedule?.config?.sources) ||
      schedule.config.sources.length < 10 ||
      schedule.config.sources.some((entry) => entry.sha256 !== expected.get(entry.file))
    )
      fail('Original schedule source/fixture identity differs')
  }
  const resets = rows.filter((r) => r.event === 'soak-reset')
  if (
    resets.length !== 10 ||
    resets.some(
      (r, i) =>
        r.cycle !== i + 10 ||
        !Object.entries(original[i + 10]?.observation?.reset ?? {}).every(([key, value]) =>
          same(r[key], value)
        )
    )
  )
    fail('Missing, reordered or changed original reset rows')
  const prefix = []
  for (const [i, row] of original.entries()) {
    const observation = row.observation
    const plan = ArchivedCycle.plan(i)
    if (row.cycle !== i || !observation) {
      fail(`Original cycle ${i} missing`)
      continue
    }
    try {
      problems.push(...ArchivedCycle.verify(plan, observation))
      const slot = completion.timing?.slots?.[i]
      if (
        !slot ||
        observation.timing.startedAt < slot.startedAt ||
        observation.timing.due !== slot.due ||
        observation.timing.completedAt > slot.due
      )
        fail(`Cycle ${i} is outside original slot`)
      const acknowledged = []
      for (const partition of [0, 1, 2]) {
        const send = rows.filter(
          (r) => r.event === 'soak-producer' && r.cycle === i && r.partition === partition
        )
        if (
          send.length !== 1 ||
          send[0].topic !== completion.final?.verifier?.topic ||
          send[0].sent?.length !== 1 ||
          send[0].sent[0].errorCode !== 0 ||
          send[0].sent[0].partition !== partition ||
          send[0].sent[0].baseOffset !== String(i * 20) ||
          send[0].messages?.length !== 20
        )
          fail(`Cycle ${i} partition ${partition} lacks original producer acknowledgement`)
        else
          send[0].messages.forEach((message, j) =>
            acknowledged.push(
              JSON.stringify([
                completion.final.verifier.topic,
                partition,
                String(i * 20 + j),
                message.key,
                message.value,
              ])
            )
          )
      }
      if (!same([...acknowledged].sort(), observation.produced.map((p) => p.identity).sort()))
        fail(`Cycle ${i} producer identities differ`)
      prefix.push(...observation.produced.map((p) => p.identity))
      if (!same([...prefix].sort(), [...observation.prefix].sort()))
        fail(`Cycle ${i} prefix mismatch`)
      const steps = observation.steps ?? []
      const at = (step) => steps.find((s) => s.step === step)?.at
      const sequence = [
        'cycle-start',
        'a-membership-start',
        'a-membership-end',
        'member-add-start',
        'member-add-end',
        'member-remove-start',
        'member-remove-end',
        'produce-start',
        'produce-end',
      ].map(at)
      if (sequence.some((t, j) => !Number.isFinite(t) || (j && t < sequence[j - 1])))
        fail(`Cycle ${i} produced before membership barriers`)
      const members = observation.memberObservations ?? []
      const b = members.find((m) => m.member === 'B')
      if (!b || b.handler?.length !== 0 || observation.snapshot?.workloadJoined !== true)
        fail(`Cycle ${i} workload members not joined or B acquired workload`)
      const bId = b?.memberId
      if (
        observation.provenance?.exchanges?.some(
          (exchange) =>
            exchange.memberId === bId &&
            ArchivedCycle.response(exchange)?.responses?.some((topic) =>
              topic.partitions.some((part) => part.acquiredRecords?.length)
            )
        )
      )
        fail(`Cycle ${i} B acquired workload on the original transport`)
      const barriers = rows.filter((r) => r.event === 'soak-membership' && r.cycle === i)
      if (
        !same(
          barriers.map((r) => r.members.length),
          [1, 2, 1]
        )
      )
        fail(`Cycle ${i} original assignment barriers missing`)
      const aId = members.find((m) => m.member === 'A')?.memberId
      const expectedMembers = [[aId], [aId, bId].sort(), [aId]]
      if (
        !same(
          barriers.map((barrier) => barrier.members.map((m) => m.memberId).sort()),
          expectedMembers
        )
      )
        fail(`Cycle ${i} original A-only/A+B/A-only ownership differs`)
      for (const barrier of barriers) {
        const ids = barrier.members.map((m) => m.memberId).sort()
        if (
          new Set(
            barrier.members.flatMap((member) =>
              member.assignment.flatMap((topic) => topic.partitions)
            )
          ).size !== 3
        )
          fail(`Cycle ${i} incomplete original assignment coverage`)
        const actual = barrier.described.map((m) => m.memberId).sort()
        if (
          !same(ids, actual) ||
          barrier.members.some(
            (m) =>
              !same(
                m.assignment,
                barrier.described.find((b) => b.memberId === m.memberId)?.assignment
              )
          )
        )
          fail(`Cycle ${i} assignment mismatch`)
      }
      if (
        !observation.provenance?.groupDescriptions?.some(
          (r) =>
            r.group?.members?.length === 0 &&
            r.at <= observation.members.emptyAt &&
            r.at >= observation.members.aLeftAt
        )
      )
        fail(`Cycle ${i} missing original empty barrier`)
      if (plan.reset) {
        const target = observation.resetTarget
        const reset = observation.reset
        const writes = steps.filter((s) => s.step === 'post-reset-api78')
        const reopen = observation.provenance.exchanges.find(
          (e) =>
            e.frames?.[0]?.socket === reset.nextSocket &&
            e.frames[0].request.shareSessionEpoch === 0
        )
        const source = observation.provenance.exchanges.find(
          (e) =>
            e.id ===
            observation.provenance.acquisitions.find((a) => a.id === target?.acquisition)?.exchange
        )
        if (
          !target ||
          source?.frames?.[0]?.socket !== target.socket ||
          reset.oldSocket !== target.socket ||
          !reopen ||
          !ArchivedCycle.response(reopen) ||
          reopen.open === source.open ||
          (reopen.pool === source.pool && reopen.generation === source.generation) ||
          new Set(writes.map((s) => s.nodeId)).size !== 3 ||
          !steps.some((s) => s.step === 'socket-close' && s.socket === target.socket)
        )
          fail(`Cycle ${i} reset transport/generation/three-node proof missing`)
      }
      const raw = rows.filter(
        (r) => r.event === 'soak-raw-log' && r.cycle === i && r.kind === 'cycle'
      )
      if (
        raw.length !== 1 ||
        !same(
          raw[0].records
            ?.map((r) => JSON.stringify([r.topic, r.partition, r.offset, r.key, r.value]))
            .sort(),
          [...prefix].sort()
        ) ||
        !same([...raw[0].identities].sort(), [...prefix].sort()) ||
        !same(raw[0].ends, observation.logs)
      )
        fail(`Cycle ${i} raw records/end reads differ`)
      const summary = summaries[i]
      if (
        summary.cycle !== i ||
        !same(summary.handler, observation.handler) ||
        !same(
          summary.oracles?.log?.expected,
          observation.produced.map((p) => p.identity)
        ) ||
        !same(summary.oracles?.accepted, observation.accepted) ||
        !same(summary.positions, observation.positions)
      )
        fail(`Cycle ${i} summary differs from originals`)
    } catch (error) {
      fail(`Cycle ${i}: ${error.message}`)
    }
  }
  const finals = rows.filter((r) => r.event === 'soak-original-final')
  const final = finals[0]?.observation ?? {}
  if (finals.length !== 1 || !same(final, completion.final))
    fail('Missing or changed original final observation')
  const finalRaw = rows.filter((r) => r.event === 'soak-raw-log' && r.kind === 'final')
  if (
    finalRaw.length !== 1 ||
    !same(
      finalRaw[0].records
        ?.map((r) => JSON.stringify([r.topic, r.partition, r.offset, r.key, r.value]))
        .sort(),
      [...prefix].sort()
    )
  )
    fail('Missing original final raw corpus')
  const last = ArchivedCycle.plan(29)
  try {
    ArchivedCycle.sequence(final, fail)
    ArchivedCycle.broker(last, final.broker ?? [], fail)
    ArchivedCycle.verifier(final.verifier, 5000, fail)
    for (const position of final.positions ?? []) ArchivedCycle.position(last, position, fail)
    ArchivedCycle.log(
      last,
      new Map(original.at(-1)?.observation?.produced?.map((p) => [p.identity, {}])),
      final,
      fail
    )
    if (final.positions?.length !== 2 || !same([...final.prefix].sort(), [...prefix].sort()))
      fail('Final terminal/raw prefix missing')
  } catch (error) {
    fail(`Final proof: ${error.message}`)
  }
  const config = rows.filter((r) => r.event === 'soak-configuration')
  if (config.length !== 1) fail('Original configuration required')
  else problems.push(...configurationProblems(config[0].config))
  const effective = config[0]?.config?.effective ?? []
  const replies = new Map(
    rows
      .filter((r) => r.event === 'soak-wire' && r.kind === 'response')
      .map((r) => [`${r.socket}:${r.frame}`, r])
  )
  let successfulHeartbeats = 0
  for (const request of rows.filter(
    (r) => r.event === 'soak-wire' && r.kind === 'request' && r.apiKey === 76
  )) {
    const reply = replies.get(`${request.socket}:${request.frame}`)
    if (!reply) continue
    try {
      const answer = ArchivedCycle.heartbeatResponse({ ...request, responseBytes: reply.bytes })
      if (answer?.errorCode !== 0 || answer.memberEpoch < 0) continue
      const broker = effective.find((b) => `${b.host}:${b.port}` === request.endpoint)
      if (
        !broker ||
        answer.heartbeatIntervalMs !==
          Number(broker.timings?.['heartbeat.interval.ms']?.configValue)
      )
        fail('Original successful heartbeat interval differs from effective configuration')
      successfulHeartbeats++
    } catch (error) {
      fail(`Original heartbeat answer: ${error.message}`)
    }
  }
  if (!successfulHeartbeats) fail('Original successful heartbeat interval observations missing')
  const cleanup = rows.filter((r) => r.event === 'soak-cleanup')
  for (const step of ['config-reset', 'group-deletion', 'topic-deletion', 'cleanup-resources'])
    if (!cleanup.some((r) => r.step === step)) fail(`Missing original cleanup ${step}`)
  const resources = cleanup.filter((r) => r.step === 'cleanup-resources').at(-1)
  if (
    !resources ||
    !same(cleanupPopulation(resources.baseline), cleanupPopulation(resources.after))
  )
    fail('Original cleanup populations differ')
  const topics = cleanup.filter((r) => r.step === 'topic-deletion')
  if (new Set(topics.filter((r) => r.present?.length === 0).map((r) => r.broker)).size !== 3)
    fail('Topic absence on every broker missing')
  if (
    cleanup.some(
      (r) =>
        r.step === 'group-deletion' &&
        r.unrelated?.some((id) => !r.listed?.some((g) => g.groupId === id))
    )
  )
    fail('Group deletion or unrelated-group preservation failed')
  if (cleanup.findLast((r) => r.step === 'group-deletion')?.remaining?.length !== 0)
    fail('Owned group is still listed')
  const reset = cleanup.findLast((r) => r.step === 'config-reset')
  if (
    !reset ||
    !Array.isArray(reset.resources) ||
    reset.resources.length !== 1 ||
    !Array.isArray(reset.read?.resources) ||
    reset.read.resources.length !== 1 ||
    reset.group !== config[0]?.config?.groupId ||
    !same(reset.names, ['share.auto.offset.reset', 'share.record.lock.duration.ms']) ||
    reset.resources?.some((r) => r.errorCode !== 0 || r.resourceName !== reset.group) ||
    reset.read?.resources?.some((r) => r.configEntries.some((e) => e.configSource === 8))
  )
    fail('Original config reset readback failed')
  return problems
}

/**
 * Validates a fixed two-scope recording independently of the child judge and reported status.
 * @param {object[]} rows JSON-round-tripped original evidence, in order.
 * @param {object} contract Approved source/run/profile contract.
 * @returns {string[]} All mandatory predicate failures.
 */
const validateRows = (rows, contract) => {
  const problems = []
  const selected = PROFILES[contract.profile]
  if (!selected) return ['Invalid fixed profile']
  for (const row of rows) {
    if (
      !same(row.ci, { runId: contract.runId, attempt: contract.attempt, profile: contract.profile })
    )
      problems.push('Cross-job or cross-run primitive identity')
    problems.push(
      ...problemsOf(row, {
        tree: contract.source.commit,
        node: contract.node,
        brokerVersion: '4.3.1',
      })
    )
    if (
      row.schema !== 'share-soak-scopes-v1' ||
      !['priming', 'scored'].includes(row.scope) ||
      row.profile !== 'tv2'
    )
      problems.push('Missing mandatory scope/schema/fixture')
    const expected = row.scope === 'priming' ? PROFILES.short : selected
    if (
      row.selector !== expected.selector ||
      row.soakProfile !== expected.profile ||
      row.stem !== (row.scope === 'priming' ? 'share-soak-prime' : expected.stem)
    )
      problems.push('Contradictory selector/profile/stem')
  }
  problems.push(...verifySharePriming(rows))
  for (const scope of ['priming', 'scored']) {
    const original = rows.filter((r) => r.scope === scope)
    const completions = original.filter(
      (r) => r.event === (scope === 'priming' ? 'priming-complete' : 'case-complete')
    )
    if (completions.length !== 1) {
      problems.push(`Missing unique ${scope} completion`)
      continue
    }
    problems.push(...scopeProblems(original, completions[0], contract).map((p) => `${scope}: ${p}`))
    problems.push(...verifySharePeriodic(original, scope === 'priming'))
    if (scope === 'scored') {
      const quiet = original.filter(
        (r) =>
          r.event === 'soak-resources' &&
          ['baseline', 'phase-end', 'teardown', 'resource-verdict'].includes(r.kind)
      )
      problems.push(...verifyShareQuietHeap(quiet).problems)
      const ends = quiet.filter((r) => r.kind === 'phase-end' || r.kind === 'teardown')
      if (
        ends.length !== 4 ||
        ends.some((r) => r.snapshot?.heapUsed - ends[0].snapshot?.heapUsed > 2097152)
      )
        problems.push('Quiet heap endpoint screen')
    }
  }
  return problems
}

/**
 * Checks original files, parent, CI ownership and deterministic seals without broker access.
 * The approved contract comes from the matching frozen checkout, never from a child checksum.
 * @param {object} contract Frozen matching source/command contract.
 * @param {string} root Archive root.
 * @param {object} [options] Pre-teardown or final replay selection and platform provenance.
 * @returns {object} Reproducible facts and problems, with no verifier-local producer stamps.
 */
const validateArchive = async (contract, root, { sealed = true, provenance } = {}) => {
  const problems = []
  let rows = []
  let contractSha256 = digest(JSON.stringify(contract))
  const profile = PROFILES[contract.profile]
  try {
    if (
      contract.schema !== 'share-soak-ci-v1' ||
      !profile ||
      !same(contract.selected, profile) ||
      !same(contract.argv, childArgs(contract.profile)) ||
      !/^v24\./.test(contract.node) ||
      !same(contract.tuple, {
        kafka: '4.3.1',
        node: 24,
        lane: 'docker',
        fixture: 'docker-compose.yml',
        profile: 'tv2',
      })
    )
      throw new Error('Invalid fixed CI contract')
    if (
      contract.validatorRevision !==
      contract.source.files?.find(
        (entry) => entry.path === 'scripts/evidence/shareSoakAcceptance.js'
      )?.sha256
    )
      throw new Error('Unbound validator revision')
    if (!same(sourceIdentity(), contract.source)) throw new Error('Frozen source identity differs')
    const text = fs.readFileSync(path.join(root, `${profile.stem}.jsonl`), 'utf8')
    if (!text.endsWith('\n') || !text.trim()) throw new Error('Truncated or empty JSONL')
    rows = text.trimEnd().split('\n').map(JSON.parse)
    problems.push(...validateRows(rows, contract))
    for (const scope of ['priming', 'scored']) {
      const wire = await verifyProtocolAnswers(rows.filter((r) => r.scope === scope))
      problems.push(...wire.problems.map((p) => `${scope}: ${p}`))
      const own = rows.find((r) => r.scope === scope && r.event === 'soak-configuration')?.config
        ?.groupId
      const listings = wire.groupListings
      if (!listings.length) problems.push(`${scope}: missing original group listings`)
      else {
        const groupsAt = (id) =>
          new Set(
            listings
              .filter((r) => r.operationId === id)
              .flatMap((r) => (r.groups ?? []).map((g) => g.groupId))
          )
        const before = groupsAt(listings[0].operationId)
        const after = groupsAt(listings.at(-1).operationId)
        const unrelated = [...before].filter((group) => group !== own).sort()
        if (
          after.has(own) ||
          unrelated.some((group) => !after.has(group)) ||
          rows
            .filter(
              (r) => r.scope === scope && r.event === 'soak-cleanup' && r.step === 'group-deletion'
            )
            .some((r) => !same([...(r.unrelated ?? [])].sort(), unrelated))
        )
          problems.push(`${scope}: original unrelated-group or deletion proof differs`)
      }
    }
    for (const name of ['stdout.log', 'stderr.log', 'contract.json'])
      if (!fs.statSync(path.join(root, name)).isFile())
        throw new Error(`Missing mandatory archive file: ${name}`)
    contractSha256 = digest(fs.readFileSync(path.join(root, 'contract.json')))
    if (!same(json(root, 'contract.json'), contract))
      throw new Error('Archive contract differs from frozen supplied contract')
    const exit = json(root, `${profile.stem}-exit.json`)
    const cleanupAt = cleanupTimestamp(profile.lane, text, exit)
    if (
      exit.accepted !== true ||
      exit.status !== 0 ||
      exit.signal !== null ||
      exit.watchdogTerminated !== false ||
      exit.error ||
      exit.executable !== contract.executable ||
      !same(exit.argv, contract.argv) ||
      !same(exit.environment, contract.environment) ||
      ![exit.startedAt, exit.exitedAt, cleanupAt].every(Number.isSafeInteger) ||
      exit.startedAt < 0 ||
      exit.exitedAt < exit.startedAt ||
      exit.parentPid <= 0 ||
      exit.childPid <= 0 ||
      exit.exitedAt - exit.startedAt > profile.totalMs ||
      exit.exitedAt - cleanupAt > 10000 ||
      exit.totalMs !== profile.totalMs ||
      exit.exitMs !== 10000
    )
      problems.push('Invalid actual fixed-parent exit')
    if (contract.profile === 'short') validateJestReport(json(root, 'share-soak-jest.json'))
    const lifecycle = json(root, 'lifecycle.json')
    const admission = json(root, 'admission.json')
    admit(lifecycle, admission.at)
    if (
      admission.origin !== lifecycle.origin ||
      admission.deadline !== lifecycle.deadline ||
      admission.requiredMs !== profile.parentMs + 600000 ||
      admission.remainingMs !== lifecycle.deadline - admission.at ||
      lifecycle.runId !== contract.runId ||
      lifecycle.attempt !== contract.attempt ||
      lifecycle.profile !== contract.profile
    )
      problems.push('Foreign or clipped lifecycle admission')
    if (
      !lifecycle.ownership ||
      lifecycle.ownership.containers.length !== 3 ||
      lifecycle.errors.length ||
      !lifecycle.stages.some((r) => r.name === 'parent' && r.status === 'passed')
    )
      problems.push('Missing ownership or failed lifecycle')
    const expectedStages = sealed
      ? ['startup', 'parent', 'verify', 'logs', 'teardown', 'seal']
      : ['startup', 'parent']
    if (
      !same(
        lifecycle.stages.map((stage) => stage.name),
        expectedStages
      ) ||
      !Number.isSafeInteger(lifecycle.wallAt) ||
      lifecycle.stages.some(
        (stage, index, stages) =>
          stage.status !== 'passed' ||
          ![stage.startedAt, stage.completedAt, stage.cap].every(Number.isSafeInteger) ||
          stage.startedAt < lifecycle.origin ||
          stage.completedAt < stage.startedAt ||
          stage.completedAt - stage.startedAt > stage.cap ||
          stage.completedAt > lifecycle.deadline ||
          (index && stage.startedAt < stages[index - 1].completedAt)
      )
    )
      problems.push('Missing, reordered, failed or late original CI stage')
    const parentStage = lifecycle.stages.find((stage) => stage.name === 'parent')
    if (
      !parentStage ||
      parentStage.cap !== profile.parentMs ||
      admission.at < parentStage.startedAt ||
      exit.startedAt < admission.at ||
      exit.exitedAt > parentStage.completedAt
    )
      problems.push('Parent observation is outside original admitted lifecycle stage')
    for (const done of rows.filter((row) =>
      ['priming-complete', 'case-complete'].includes(row.event)
    )) {
      const clock = done.cleanup?.clock
      const origin = clock?.completedAt - (clock?.performanceAt - done.timing?.startedAt)
      if (
        !Number.isFinite(origin) ||
        origin < exit.startedAt ||
        origin > clock.completedAt ||
        clock.performanceAt > done.timing?.observedAt
      )
        problems.push('Uncorrelated original owner/parent/cleanup clocks')
    }
    const startup = json(root, 'broker-startup.json')
    const broker = json(root, 'broker-facts.json')
    if (
      !same(startup.containers?.map((c) => c.Id).sort(), broker.containers?.map((c) => c.Id).sort())
    )
      problems.push('Startup container identities differ from validation originals')
    if (
      broker.containers?.length !== 3 ||
      !same(
        broker.containers.map((c) => c.Id).sort(),
        [...lifecycle.ownership.containers].sort()
      ) ||
      broker.versions?.some((v) => !/^4\.3\.1(?:\s|$)/.test(v.version)) ||
      broker.containers.some(
        (c) =>
          c.Config.Image !== 'apache/kafka:4.3.1' ||
          c.Config.Labels['com.docker.compose.project'] !== lifecycle.project
      )
    )
      problems.push('Invalid original broker provenance')
    if (sealed) {
      for (const [key, value] of Object.entries({
        GITHUB_REPOSITORY: contract.repository,
        GITHUB_RUN_ID: contract.runId,
        GITHUB_RUN_ATTEMPT: contract.attempt,
        GITHUB_SHA: contract.source.commit,
        GITHUB_WORKFLOW_REF: contract.workflowRef,
        GITHUB_WORKFLOW_SHA: contract.workflowSha,
      }))
        if (!value || process.env[key] !== value)
          problems.push(`Archive differs from platform context: ${key}`)
      if (
        contract.workflow !== '.github/workflows/memory-gates.yml' ||
        !(
          contract.event === 'workflow_dispatch' ||
          (contract.event === 'pull_request' && contract.headRef?.startsWith('p9m/'))
        )
      )
        problems.push('Ineligible workflow/profile contract')
      const seal = json(root, 'seal.json')
      if (
        seal.complete !== true ||
        !same(seal.files, inventory(root)) ||
        seal.contractSha256 !== digest(fs.readFileSync(path.join(root, 'contract.json'))) ||
        !same(seal.excluded, ['seal.json']) ||
        seal.root !== '.'
      )
        problems.push('Changed/incomplete artifact seal')
      const teardown = json(root, 'teardown.json')
      if (teardown.project !== lifecycle.project || Object.values(teardown.remaining).some(Boolean))
        problems.push('Missing owned teardown absence')
      const earlier = json(root, 'validation.json')
      if (earlier.problems?.length || earlier.semanticSha256 !== digest(JSON.stringify(rows)))
        problems.push('Archive semantic replay differs from pre-teardown validation')
      if (
        !provenance ||
        provenance.repository !== contract.repository ||
        provenance.head !== contract.head ||
        provenance.base !== contract.base ||
        provenance.workflowRef !== contract.workflowRef ||
        provenance.workflowSha !== contract.workflowSha ||
        String(provenance.runId) !== contract.runId ||
        String(provenance.attempt) !== contract.attempt ||
        provenance.job !== contract.job ||
        provenance.artifact !== contract.artifact ||
        provenance.conclusion !== 'success' ||
        provenance.artifactExpired !== false ||
        !/^sha256:[a-f0-9]{64}$/.test(provenance.artifactDigest) ||
        provenance.downloadDigest !== provenance.artifactDigest ||
        provenance.testedSha !== contract.source.commit
      )
        problems.push('Missing or mismatched GitHub producer/artifact provenance')
    }
  } catch (error) {
    problems.push(error.message)
  }
  return {
    schema: 'share-soak-validation-v1',
    profile: contract.profile,
    semanticSha256: digest(JSON.stringify(rows)),
    contractSha256,
    problems,
  }
}

/** @param {string[]} argv Fixed --contract/--archive/--provenance options. @returns {number} Exit status. */
const main = async (argv) => {
  try {
    if (process.versions.node.split('.')[0] !== '24')
      throw new Error('Expected Node24 archive verifier')
    if (
      argv.length !== 6 ||
      argv[0] !== '--contract' ||
      argv[2] !== '--archive' ||
      argv[4] !== '--provenance'
    )
      throw new Error('Usage: --contract <file> --archive <directory> --provenance <file>')
    const result = await validateArchive(JSON.parse(fs.readFileSync(argv[1], 'utf8')), argv[3], {
      provenance: JSON.parse(fs.readFileSync(argv[5], 'utf8')),
    })
    console.log(JSON.stringify(result, null, 2))
    return result.problems.length ? 1 : 0
  } catch (error) {
    console.error(error)
    return 1
  }
}
module.exports = {
  validateArchive,
  validateRows,
  scopeProblems,
  configurationProblems,
  cleanupPopulation,
}

if (require.main === module)
  main(process.argv.slice(2)).then((status) => {
    process.exitCode = status
  })
