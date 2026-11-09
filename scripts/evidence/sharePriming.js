const { createHash } = require('node:crypto')
const { isDeepStrictEqual } = require('node:util')
const { readRequestHeader } = require('../../testHelpers/fakeSocket')
const { produceCodecs } = require('../soak/shareProtocol')
const { median, slope } = require('../soak/bounds')
const { verifyShareQuietHeap } = require('./shareQuietHeap')
const { ShareTimerObserver } = require('../soak/shareTimerObserver')
const { verifySharePrimingLink } = require('./sharePrimingLink')

const PHASES = ['share-steady', 'share-reset', 'share-members']
const REQUIRED_APIS = [0, 1, 2, 3, 10, 16, 18, 19, 20, 22, 32, 42, 44, 60, 76, 77, 78, 79, 90]
const REQUIRED_OPERATIONS = [
  'send',
  'readLog',
  'createTopics',
  'describeCluster',
  'describeConfigs',
  'incrementalAlterConfigs:0',
  'incrementalAlterConfigs:1',
  'listGroups',
  'describeShareGroups',
  'listShareGroupOffsets:named',
  'listShareGroupOffsets:all',
  'deleteGroups',
  'deleteTopics',
  'fetchTopicMetadata',
  'listTopics',
]

const OPERATION_APIS = {
  send: 0,
  readLog: 1,
  createTopics: 19,
  describeCluster: 60,
  describeConfigs: 32,
  'incrementalAlterConfigs:0': 44,
  'incrementalAlterConfigs:1': 44,
  listGroups: 16,
  describeShareGroups: 77,
  'listShareGroupOffsets:named': 90,
  'listShareGroupOffsets:all': 90,
  deleteGroups: 42,
  deleteTopics: 20,
  fetchTopicMetadata: 3,
  listTopics: 3,
}

/**
 * Reconstructs negotiated coverage from matched original bytes and public-call observations.
 * A response is an answered attempt, never a claim that negotiation succeeded. All attempts,
 * including ApiVersions fallbacks, remain in their original order on disk.
 * @param {object[]} rows One scope's original rows.
 * @returns {{ problems: string[], inventory: object[], sha256: string }} Observed facts only.
 */
const verifyShareProtocol = (rows) => {
  const problems = []
  const requests = rows.filter((r) => r.event === 'soak-wire' && r.kind === 'request')
  const replies = rows.filter((r) => r.event === 'soak-wire' && r.kind === 'response')
  const operations = rows.filter((r) => r.event === 'soak-wire' && r.kind === 'operation')
  const inventory = new Map()
  const responseFrames = new Map()
  for (const reply of replies) {
    const key = `${reply.socket}:${reply.frame}`
    if (!responseFrames.has(key)) responseFrames.set(key, [])
    responseFrames.get(key).push(reply)
  }
  const frames = new Set()
  for (const request of requests) {
    try {
      const bytes = Buffer.from(request.bytes, 'base64')
      const header = readRequestHeader(bytes)
      const key = `${request.socket}:${request.frame}`
      if (
        frames.has(key) ||
        bytes.readInt32BE(0) !== bytes.length - 4 ||
        !['apiKey', 'apiVersion', 'correlationId'].every((k) => header[k] === request[k]) ||
        request.generation !== request.socket ||
        !Number.isSafeInteger(request.socket) ||
        request.socket <= 0 ||
        !Number.isSafeInteger(request.frame) ||
        request.frame <= 0 ||
        !Number.isSafeInteger(request.correlationId) ||
        request.correlationId < 0 ||
        !Number.isFinite(request.at) ||
        !request.endpoint
      )
        throw new Error('request framing or identity')
      frames.add(key)
      const matched = responseFrames.get(key) ?? []
      if (matched.length > 1) throw new Error('duplicate response')
      for (const reply of matched) {
        const response = Buffer.from(reply.bytes, 'base64')
        if (
          response.length < 8 ||
          response.readInt32BE(0) !== response.length - 4 ||
          response.readInt32BE(4) !== header.correlationId ||
          reply.correlationId !== header.correlationId ||
          reply.endpoint !== request.endpoint ||
          reply.generation !== request.generation ||
          reply.at < request.at
        )
          throw new Error('response framing or identity')
      }
      if (header.apiKey === 0) {
        const codecs = produceCodecs(bytes)
        if (codecs.some((codec) => codec !== 0) || !isDeepStrictEqual(codecs, request.codecs))
          throw new Error('Produce codec must be None')
      }
      if (
        [76, 78, 79].includes(header.apiKey) &&
        header.apiVersion !== (header.apiKey === 76 ? 1 : 2)
      )
        throw new Error('share negotiated version')
      if (!matched.length) continue
      const responseErrorCode =
        header.apiKey === 18 ? Buffer.from(matched[0].bytes, 'base64').readInt16BE(8) : null
      const entry = {
        attempt:
          responseErrorCode === null
            ? 'answered'
            : responseErrorCode === 0
              ? 'negotiated'
              : 'rejected',
        responseErrorCode,
        operation: request.operation,
        role: request.role,
        endpoint: request.endpoint,
        apiKey: header.apiKey,
        apiVersion: header.apiVersion,
      }
      inventory.set(JSON.stringify(entry), entry)
    } catch (error) {
      problems.push(`protocol: ${error.message}`)
    }
  }
  for (const reply of replies)
    if (!frames.has(`${reply.socket}:${reply.frame}`)) problems.push('protocol: orphan response')
  const observed = [...inventory.values()].sort((a, b) =>
    JSON.stringify(a).localeCompare(JSON.stringify(b))
  )
  for (const key of REQUIRED_APIS)
    if (!observed.some((r) => r.apiKey === key && (key !== 18 || r.attempt === 'negotiated')))
      problems.push(`protocol: missing API ${key}`)
  for (const operation of REQUIRED_OPERATIONS)
    if (
      !operations.some(
        (r) =>
          r.name === operation &&
          r.status === 'passed' &&
          requests.some(
            (q) =>
              q.operationId === r.id &&
              q.apiKey === OPERATION_APIS[operation] &&
              responseFrames.has(`${q.socket}:${q.frame}`)
          )
      )
    )
      problems.push(`protocol: missing public operation ${operation}`)
  if (!observed.some((r) => r.role === 'raw' && r.apiKey === 1))
    problems.push('protocol: missing raw Fetch')
  return {
    problems,
    inventory: observed,
    sha256: createHash('sha256').update(JSON.stringify(observed)).digest('hex'),
  }
}

/**
 * Projects authenticated operation/client roles and negotiated implementations across placements.
 * Literal endpoints remain in each manifest; independent groups may select different brokers.
 * @param {object[]} inventory Validated original manifest entries.
 * @returns {string[]} Distinct corresponding implementations in a stable order.
 */
const correspondingImplementations = (inventory) =>
  [
    ...new Set(inventory.map(({ endpoint, ...implementation }) => JSON.stringify(implementation))),
  ].sort()

/**
 * Binds actual cycle/verifier work to its original slot, cleanup and absolute scope cutoffs.
 * Padded idle-wake slot summaries are deliberately separate from actual work completion.
 * @param {object} done Original scope completion.
 * @param {object[]} cycles Original ordered cycle rows.
 * @param {number} startedAt Original process entry.
 * @param {number} workDue Immutable absolute scope work cutoff.
 * @param {number} childDue Immutable absolute child cutoff.
 * @returns {string[]} Invalid original work observations.
 */
const verifyShareWorkTiming = (done, cycles, startedAt, workDue, childDue) => {
  const problems = []
  const observedAt = done.timing?.observedAt
  if (
    !Number.isFinite(observedAt) ||
    observedAt < startedAt ||
    observedAt > Math.min(workDue, childDue)
  )
    problems.push('original scope completion exceeds its absolute bound')
  const check = (work, kind, cycle, floor, due, label) => {
    const timing = work?.timing
    const verifier = work?.verifier
    if (
      ![
        floor,
        due,
        timing?.startedAt,
        timing?.completedAt,
        verifier?.openedAt,
        verifier?.closedAt,
      ].every(Number.isFinite) ||
      timing.kind !== kind ||
      timing.cycle !== cycle ||
      timing.due !== due ||
      timing.startedAt < Math.max(startedAt, floor) ||
      timing.completedAt < timing.startedAt ||
      timing.completedAt > Math.min(due, workDue, childDue, observedAt) ||
      verifier.kind !== kind ||
      verifier.cycle !== cycle ||
      verifier.openedAt < timing.startedAt ||
      verifier.closedAt < verifier.openedAt ||
      verifier.closedAt > timing.completedAt ||
      verifier.events?.some(
        (r) => !Number.isFinite(r.at) || r.at < verifier.openedAt || r.at > verifier.closedAt
      )
    )
      problems.push(`original ${label} work exceeds its unchanged deadline or scope`)
  }
  for (const [i, work] of cycles.entries()) {
    const slot = done.timing?.slots?.[i]
    check(work, 'cycle', i, slot?.startedAt, slot?.due, `cycle ${i}`)
  }
  const cleanup = done.timing?.stages?.find((s) => s.name === 'cleanup')
  check(done.final, 'final', 29, done.timing?.phases?.at(-1)?.completedAt, cleanup?.due, 'final')
  return problems
}

/**
 * Independently fits every eligible periodic reading; a forged passing summary cannot mask growth.
 * Priming measurements are returned without applying growth limits, but invalid sources fail.
 * @param {object[]} rows Original scope rows, including full periodic observations.
 * @param {boolean} [priming=false] Fixed schema scope, never selected by a resource verdict.
 * @returns {string[]} Invalid sources, summary mismatch or failed scored growth predicates.
 */
const verifySharePeriodic = (rows, priming = false) => {
  const problems = []
  const phases = []
  for (const phase of PHASES) {
    const samples = rows.filter(
      (r) =>
        r.event === 'soak-resources' &&
        r.kind === 'sample' &&
        r.phase === phase &&
        r.phaseMs >= 10000
    )
    const heaps = samples.map((r) => r.snapshot?.heapUsed)
    if (
      samples.length < 6 ||
      !heaps.every((h) => Number.isSafeInteger(h) && h >= 0) ||
      samples.some(
        (r, i) =>
          !Number.isFinite(r.phaseMs) ||
          !Number.isFinite(r.atMs) ||
          (i && (r.phaseMs <= samples[i - 1].phaseMs || r.atMs <= samples[i - 1].atMs)) ||
          !['shareRecords', 'shareRanges', 'shareSessions'].every(
            (k) => Number.isSafeInteger(r.snapshot?.[k]) && r.snapshot[k] >= 0
          ) ||
          r.snapshot.shareRecords > 300
      )
    ) {
      problems.push(`${phase}: invalid periodic sources`)
      continue
    }
    const endpointBytes = median(heaps.slice(-3)) - median(heaps.slice(0, 3))
    const slopeBytesPerMinute = slope(samples.map((r) => [r.phaseMs / 60000, r.snapshot.heapUsed]))
    phases.push({ phase, heapSamples: heaps.length, endpointBytes, slopeBytesPerMinute })
    if (!priming && (endpointBytes > 2097152 || slopeBytesPerMinute > 262144))
      problems.push(`${phase}: retained-heap screen`)
  }
  const verdict = rows.find((r) => r.event === 'soak-resources' && r.kind === 'resource-verdict')
  const measured = priming ? verdict?.measurementOnly?.measured : verdict?.measured
  if (!isDeepStrictEqual(phases, measured?.phases))
    problems.push('periodic verdict differs from original observations')
  return problems
}

const equalCounts = (a, b) =>
  a &&
  b &&
  [...new Set([...Object.keys(a), ...Object.keys(b)])].every(
    (k) =>
      Number.isSafeInteger(a[k] ?? 0) &&
      (a[k] ?? 0) >= 0 &&
      Number.isSafeInteger(b[k] ?? 0) &&
      (a[k] ?? 0) === (b[k] ?? 0)
  )

/**
 * Checks original quiet inventories and elapsed heap-source chronology in either schema scope.
 * The reporter helper recognizes only individually observed Jest status debounce allocations.
 * @param {object[]} rows Original scope archive.
 * @returns {string[]} Source, inventory or failed structural verdict problems.
 */
const verifyShareSources = (rows) => {
  const problems = []
  const quiet = rows.filter(
    (r) => r.event === 'soak-resources' && ['baseline', 'phase-end', 'teardown'].includes(r.kind)
  )
  const baseline = quiet[0]?.snapshot
  if (
    quiet.length !== 5 ||
    quiet.map((r) => r.kind).join(',') !== 'baseline,phase-end,phase-end,phase-end,teardown'
  )
    problems.push('invalid quiet observation order')
  for (const [i, { snapshot: point }] of quiet.entries()) {
    if (
      !Number.isSafeInteger(point?.heapUsed) ||
      point.heapUsed < 0 ||
      !Number.isFinite(point?.heapAtMs) ||
      point.heapAtMs < 0 ||
      (i && point.heapAtMs <= quiet[i - 1].snapshot?.heapAtMs) ||
      !['queueDepth', 'shareSessions', 'shareRecords', 'shareRanges'].every(
        (k) => point?.[k] === 0
      ) ||
      point?.sockets?.open !== 0 ||
      !equalCounts(point?.namedListeners, baseline?.namedListeners)
    )
      problems.push('quiet source or resource failure')
    for (const [when, resource] of [
      ['beforeCollection', 'activeResourcesBeforeCollection'],
      ['afterCollection', 'activeResources'],
    ]) {
      const owners = point?.timerOwners?.[when]
      const before = baseline?.timerOwners?.afterCollection
      if (
        !owners ||
        !Array.isArray(owners.rows) ||
        owners.dropped !== 0 ||
        !equalCounts(owners.unattributed, baseline?.timerOwners?.[when]?.unattributed) ||
        !equalCounts(
          ShareTimerObserver.comparableResources(point?.[resource], owners),
          ShareTimerObserver.comparableResources(baseline?.activeResources, before)
        )
      )
        problems.push('quiet inventory, attribution or observer overflow failure')
    }
  }
  const verdict = rows.find((r) => r.event === 'soak-resources' && r.kind === 'resource-verdict')
  if (verdict?.ok !== true || !Array.isArray(verdict?.violations) || verdict.violations.length)
    problems.push('structural resource verdict is absent or failed')
  return problems
}

/**
 * Reconstructs the fixed two-scope boundary, identities and original absolute schedule.
 * The caller authenticates source/tree/command stamps before using this independent checker.
 * @param {object[]} rows Original acceptance-process archive, in serialization order.
 * @returns {string[]} Broken scope, isolation, linkage, negotiation or timing requirements.
 */
const verifySharePriming = (rows) => {
  const problems = verifySharePrimingLink(rows)
  const starts = rows.filter((r) => r.event === 'priming-start')
  const primes = rows.filter((r) => r.event === 'priming-complete')
  const boundaries = rows.filter((r) => r.event === 'priming-boundary')
  const scores = rows.filter((r) => r.event === 'case-complete')
  if (starts.length !== 1 || primes.length !== 1 || boundaries.length !== 1 || scores.length !== 1)
    return ['exactly one ordered priming pass, boundary and scored completion are required']
  const [start] = starts
  const [prime] = primes
  const [boundary] = boundaries
  const [score] = scores
  const same = (a, b) => isDeepStrictEqual(a, b)
  const times = [
    start.startedAt,
    prime.timing?.startedAt,
    prime.timing?.observedAt,
    boundary.priming?.boundaryAt,
    score.timing?.observedAt,
  ]
  if (
    !(
      rows.indexOf(start) < rows.indexOf(prime) &&
      rows.indexOf(prime) < rows.indexOf(boundary) &&
      rows.indexOf(boundary) < rows.indexOf(score)
    ) ||
    start.passes !== 1 ||
    start.cycles !== 30 ||
    start.resets !== 10 ||
    start.totalMs !== 1020000 ||
    times.some((t) => !Number.isFinite(t)) ||
    times[0] !== times[1] ||
    times.slice(2).some((t, i) => t < times[i + 1]) ||
    score.timing?.startedAt !== start.startedAt ||
    score.priming?.scenario !== prime.scenario ||
    !same(score.priming, boundary.priming) ||
    prime.scenario === score.scenario ||
    prime.status !== 'passed' ||
    prime.tainted !== false ||
    score.status !== 'passed' ||
    score.tainted !== false ||
    boundary.priming?.status !== 'passed' ||
    boundary.priming?.cycles !== 30 ||
    boundary.priming?.resets !== 10 ||
    boundary.priming?.verifiers !== 31 ||
    boundary.priming?.passes !== 1 ||
    boundary.priming?.boundaryAt > start.startedAt + 1020000 ||
    prime.timing?.workDue !== start.startedAt + 1010000 ||
    rows.some((r) => r.event === 'case-failed')
  )
    problems.push('invalid priming chronology or completion link')
  if (rows.some((r) => ![prime.scenario, score.scenario].includes(r.scenario)))
    problems.push('unexpected additional acceptance scope')
  const references = rows.filter((r) => r.event === 'priming-boundary-reference')
  const reference = references[0]?.snapshot
  const observed = boundary.snapshot
  if (
    references.length !== 1 ||
    rows.indexOf(references[0]) >= rows.indexOf(prime) ||
    !Number.isSafeInteger(observed?.heapUsed) ||
    observed.heapUsed < 0 ||
    !Number.isFinite(observed?.heapAtMs) ||
    observed.heapAtMs < prime.timing?.observedAt - start.startedAt ||
    observed.heapAtMs > boundary.priming?.boundaryAt - start.startedAt ||
    !['queueDepth', 'shareSessions', 'shareRecords', 'shareRanges'].every(
      (k) => observed?.[k] === 0
    ) ||
    observed?.sockets?.open !== 0 ||
    !equalCounts(observed?.namedListeners, reference?.namedListeners)
  )
    problems.push('invalid priming boundary resource observation')
  for (const [when, resource] of [
    ['beforeCollection', 'activeResourcesBeforeCollection'],
    ['afterCollection', 'activeResources'],
  ]) {
    const owners = observed?.timerOwners?.[when]
    const before = reference?.timerOwners?.[when]
    if (
      !owners ||
      !before ||
      owners.dropped !== 0 ||
      !equalCounts(owners.unattributed, before.unattributed) ||
      !equalCounts(
        ShareTimerObserver.comparableResources(observed?.[resource], owners),
        ShareTimerObserver.comparableResources(reference?.[resource], before)
      )
    )
      problems.push('priming boundary inventory, attribution or observer overflow failure')
  }
  const inventories = []
  const configurations = []
  const identities = []
  const members = []
  for (const [done, scope] of [
    [prime, 'priming'],
    [score, 'scored'],
  ]) {
    const original = rows.filter((r) => r.scenario === done.scenario)
    const cycles = original.filter((r) => r.event === 'soak-cycle')
    const config = original.filter((r) => r.event === 'soak-configuration')
    configurations.push(config[0]?.config)
    const slotMs = scope === 'priming' || score.selector === 'SHARE_SOAK_E2E' ? 32000 : 64000
    const profileMs = slotMs * 30
    const budget = scope === 'priming' ? 1010000 : slotMs === 32000 ? 2030000 : 2990000
    if (
      original.some((r) => r.scope !== scope) ||
      config.length !== 1 ||
      cycles.length !== 30 ||
      cycles.some(
        (r, i) =>
          r.cycle !== i ||
          r.status !== 'passed' ||
          r.phase !== PHASES[Math.floor(i / 10)] ||
          r.resetObserved !== (i >= 10 && i < 20) ||
          r.verifier?.windowMs !== 5000 ||
          !r.cleanup?.clientsDisconnected ||
          !r.cleanup?.verifierEmpty
      ) ||
      done.profileMs !== profileMs ||
      done.elapsedProfileMs < profileMs ||
      done.elapsedProfileMs > profileMs + 10000 ||
      done.timing?.workDue !== start.startedAt + budget ||
      done.cleanup?.groupsAbsent !== true ||
      done.cleanup?.topicsAbsent !== true ||
      done.cleanup?.clock?.origin !== 'hrtime' ||
      !Number.isSafeInteger(done.cleanup?.completedAt) ||
      done.timing?.stages?.some((s) => s.completedAt > s.due) ||
      done.timing?.slots?.length !== 30 ||
      done.timing?.phases?.length !== 3 ||
      done.timing?.slots?.some((s, i) => {
        const phase = done.timing.phases[Math.floor(i / 10)]
        return (
          s.cycle !== i ||
          s.due !== phase.startedAt + ((i % 10) + 1) * slotMs ||
          s.startedAt >= s.due ||
          s.completedAt < s.due ||
          s.completedAt > phase.completedAt
        )
      }) ||
      done.final?.verifier?.windowMs !== 5000
    )
      problems.push(`${scope}: invalid fixed schedule, workload or cleanup`)
    problems.push(
      ...verifyShareWorkTiming(
        done,
        cycles,
        start.startedAt,
        start.startedAt + budget,
        start.startedAt + (score.selector === 'SHARE_SOAK_E2E' ? 2040000 : 3000000)
      ).map((p) => `${scope}: ${p}`)
    )
    const corpus = cycles.flatMap((r) => r.oracles?.log?.expected ?? [])
    identities.push(corpus)
    members.push([...cycles.map((r) => r.verifier?.memberId), done.final?.verifier?.memberId])
    if (
      new Set(corpus).size !== 1800 ||
      new Set(members.at(-1)).size !== 31 ||
      corpus.some((id) => {
        try {
          return JSON.parse(id)[0] !== config[0]?.config?.topic
        } catch {
          return true
        }
      })
    )
      problems.push(`${scope}: invalid corpus or verifier identities`)
    const protocol = verifyShareProtocol(original)
    problems.push(...protocol.problems.map((p) => `${scope}: ${p}`))
    inventories.push(protocol.inventory)
    const manifest = original.filter((r) => r.event === 'soak-protocol-manifest')
    if (
      manifest.length !== 1 ||
      manifest[0].sha256 !== protocol.sha256 ||
      !same(manifest[0].inventory, protocol.inventory)
    )
      problems.push(`${scope}: invalid observed protocol manifest`)
    problems.push(...verifySharePeriodic(original, scope === 'priming'))
    problems.push(...verifyShareSources(original))
    if (scope === 'scored') {
      const quiet = original.filter(
        (r) =>
          r.event === 'soak-resources' &&
          ['baseline', 'phase-end', 'teardown', 'resource-verdict'].includes(r.kind)
      )
      problems.push(...verifyShareQuietHeap(quiet).problems)
    }
    if (scope === 'scored') {
      const setup = done.timing?.stages?.find((s) => s.name === 'setup')
      const baseline = original.find((r) => r.kind === 'baseline')
      if (
        !setup ||
        setup.startedAt < boundary.priming?.boundaryAt ||
        baseline?.snapshot?.heapAtMs < setup.completedAt - start.startedAt
      )
        problems.push('scored setup/baseline preceded priming boundary')
    }
  }
  const sockets = [prime, score].map(
    (done) =>
      new Set(
        rows
          .filter(
            (r) => r.scenario === done.scenario && r.event === 'soak-wire' && r.kind === 'request'
          )
          .map((r) => r.socket)
      )
  )
  if ([...sockets[0]].some((id) => sockets[1].has(id)))
    problems.push('priming/scored socket collision')
  if (!same(...inventories.map(correspondingImplementations)))
    problems.push('priming/scored negotiated implementations differ')
  for (const field of ['topic', 'groupId', 'topicId'])
    if (!configurations[0]?.[field] || configurations[0][field] === configurations[1]?.[field])
      problems.push(`priming/scored ${field} collision`)
  if (
    !configurations[0]?.topic?.startsWith('soak-share-prime-') ||
    !configurations[0]?.groupId?.startsWith('soak-share-prime-')
  )
    problems.push('missing explicit priming resource names')
  if (new Set(identities.flat()).size !== 3600 || new Set(members.flat()).size !== 62)
    problems.push('priming/scored corpus or member collision')
  return problems
}

module.exports = {
  verifyShareProtocol,
  verifySharePeriodic,
  verifySharePriming,
  verifyShareWorkTiming,
}
