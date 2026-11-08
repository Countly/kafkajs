'use strict'

const { isDeepStrictEqual } = require('node:util')

const CAPTURES = ['baseline', 'share-steady', 'share-reset', 'share-members', 'repeat-010']

/**
 * Checks diagnostic completeness independently of the resource-screen verdict. This is not an
 * acceptance validator: it requires the original proofs and replays their unchanged cycle oracle.
 * @param {object[]} rows Original JSONL, in archival order.
 * @param {object[]} captures Original manifest, in capture order.
 * @param {boolean} [completed=true] Whether the final diagnostic completion row is required.
 * @returns {{complete:boolean, violations:string[]}} Missing or invalid evidence remains incomplete.
 */
const evidenceStatus = (rows, captures, completed = true) => {
  const { ShareCycleOracle } = require('../soak/soak')
  const violations = []
  const fail = (message) => violations.push(message)
  const events = (event) => rows.filter((row) => row.event === event)
  if (
    rows.some(
      (row) =>
        row.diagnostic !== true ||
        row.acceptanceEligible !== false ||
        row.selector !== 'DIAG_SHARE_HEAP'
    )
  )
    fail('Every row must retain its diagnostic identity')
  if (
    events('case-complete').length ||
    events('natural-exit').length ||
    events('case-failed').length ||
    events('diag-failed').length ||
    events('diag-cleanup-failed').length
  )
    fail('Acceptance or failure rows cannot establish diagnostic completion')
  const source = events('diag-source')
  const sourceFiles = [
    'scripts/diag/shareHeap.js',
    'scripts/diag/evidence.js',
    'scripts/soak/soak.js',
  ]
  if (
    source.length !== 1 ||
    source[0].harness !== 'standalone-short-share' ||
    !isDeepStrictEqual(
      source[0].sources?.map(({ file }) => file),
      sourceFiles
    ) ||
    source[0].sources?.some(({ sha256 }) => !/^[a-f0-9]{64}$/.test(sha256))
  )
    fail('Diagnostic source and harness identity is incomplete')
  if (
    !rows[0]?.tree ||
    rows[0].tree === 'unfrozen' ||
    rows.some(
      (row) =>
        row.tree !== rows[0].tree || !/^v24\./.test(row.node) || row.brokerVersion !== '4.3.1'
    )
  )
    fail('Diagnostic evidence must retain one source tree and the Node24/Kafka4.3.1 tuple')
  const envelope = events('diag-envelope')
  if (envelope.length !== 1 || envelope[0].extraCycles !== 10 || envelope[0].totalMs !== 1955000)
    fail('Missing fixed diagnostic envelope')
  if (
    !isDeepStrictEqual(
      captures.map(({ label }) => label),
      CAPTURES
    )
  )
    fail('Exactly five ordered captures are required')
  let previous = -Infinity
  for (const capture of captures) {
    if (
      capture.diagnostic !== true ||
      capture.acceptanceEligible !== false ||
      ![
        capture.ownerAtMs,
        capture.completedOwnerAtMs,
        capture.snapshotMs,
        capture.heapUsed,
        capture.sizeBytes,
      ].every(Number.isFinite) ||
      capture.ownerAtMs < previous ||
      capture.completedOwnerAtMs < capture.ownerAtMs ||
      capture.snapshotMs < 0 ||
      capture.snapshotMs > 120000 ||
      capture.heapUsed <= 0 ||
      capture.sizeBytes <= 0
    )
      fail(`Invalid capture: ${capture.label}`)
    previous = capture.completedOwnerAtMs
  }
  if (
    !isDeepStrictEqual(
      events('diag-heap-snapshot').map(({ label }) => label),
      CAPTURES
    )
  )
    fail('JSONL capture order does not match the required manifest')
  for (const [i, capture] of captures.entries()) {
    const row = events('diag-heap-snapshot')[i]
    if (!row || Object.keys(capture).some((key) => !isDeepStrictEqual(row[key], capture[key])))
      fail(`Manifest and JSONL capture facts differ: ${capture.label}`)
  }
  const original = events('soak-cycle')
  const extra = events('diag-extra-cycle')
  if (
    original.length !== 30 ||
    original.some((row, i) => row.cycle !== i || row.status !== 'passed')
  )
    fail('Original cycles 0–29 must be complete, passed and ordered')
  if (
    extra.length !== 10 ||
    extra.some((row, i) => row.plan?.cycle !== 30 + i || row.extra !== i + 1)
  )
    fail('Extra cycles 30–39 must be complete, distinct and ordered')
  if (
    events('soak-reset').length !== 10 ||
    events('soak-reset').some((row, i) => row.cycle !== 10 + i)
  )
    fail('All ten original resets are required')
  const timing = events('diag-original-timing')[0]?.timing
  if (
    events('diag-original-timing').length !== 1 ||
    timing?.slots?.length !== 30 ||
    timing?.phases?.length !== 3
  )
    fail('Original schedule timing is incomplete')
  for (const [index, phase] of (timing?.phases ?? []).entries()) {
    if (
      phase.phase !== index ||
      ![phase.startedAt, phase.due, phase.completedAt].every(Number.isFinite) ||
      phase.due - phase.startedAt !== 320000 ||
      phase.completedAt < phase.due ||
      (index && phase.startedAt < timing.phases[index - 1].completedAt)
    )
      fail('Original phase timing differs from the fixed short schedule')
  }
  const verifiers = new Set()
  const prefix = []
  for (const [index, row] of [...original, ...extra].entries()) {
    const plan = ShareCycleOracle.plan(index)
    if (index >= 30) Object.assign(plan, { phase: 'share-steady', reset: false })
    try {
      const observation =
        index >= 30
          ? row.observation
          : {
              produced: row.oracles.log.expected.map((identity) => ({
                identity,
                partition: JSON.parse(identity)[1],
              })),
              log: row.oracles.log.actual,
              logs: row.logs,
              handler: row.handler,
              accepted: row.oracles.accepted,
              broker: row.oracles.broker,
              positions: row.positions,
              timing: row.timing,
              held: row.held,
              reset: row.reset,
              verifier: row.verifier,
              provenance: row.provenance,
              memberObservations: row.memberObservations,
              prefix: row.prefix,
              acknowledgedPrefix: row.acknowledgedPrefix,
              members: row.members,
              clientsDisconnected: row.cleanup.clientsDisconnected,
            }
      if (row.violations?.length !== 0) fail(`Cycle ${index} has oracle violations`)
      if (index >= 30 && !isDeepStrictEqual(row.plan, { ...plan, first: String(plan.first) }))
        fail(`Cycle ${index} archived plan differs from its fixed corpus`)
      const slot = index >= 30 ? row.slot : events('diag-original-timing')[0]?.timing?.slots[index]
      if (
        !slot ||
        ![slot.startedAt, slot.due, slot.completedAt].every(Number.isFinite) ||
        slot.startedAt >= slot.due ||
        slot.completedAt < slot.startedAt ||
        (index >= 30
          ? slot.completedAt > slot.due ||
            slot.startedAt < slot.windowStart ||
            slot.due - slot.windowStart !== 32000
          : slot.completedAt < slot.due) ||
        observation.timing?.due !== slot.due ||
        observation.timing?.startedAt < slot.startedAt ||
        observation.timing?.completedAt > slot.due
      )
        fail(`Cycle ${index} is outside its original 32-second slot`)
      if (index < 30) {
        const phase = timing?.phases?.[Math.floor(index / 10)]
        if (
          !phase ||
          slot?.due !== phase.startedAt + ((index % 10) + 1) * 32000 ||
          slot?.startedAt < slot?.due - 32000 ||
          slot?.completedAt > phase.completedAt
        )
          fail(`Cycle ${index} does not occupy its original fixed slot`)
      } else if (index > 30 && slot?.windowStart !== extra[index - 31]?.slot?.due)
        fail(`Cycle ${index} repetition slots are not consecutive`)
      for (const violation of ShareCycleOracle.judge(plan, observation)) fail(violation)
      prefix.push(...observation.produced.map(({ identity }) => identity))
      if (!isDeepStrictEqual([...prefix].sort(), [...observation.prefix].sort()))
        fail(`Cycle ${index} cumulative corpus differs`)
      verifiers.add(observation.verifier.memberId)
    } catch (error) {
      fail(`Cycle ${index} proof is incomplete: ${error.message}`)
    }
  }
  if (new Set(prefix).size !== 2400 || verifiers.size !== 40)
    fail('2400 distinct identities and forty distinct joined cycle verifiers are required')
  const final = events('diag-original-final')
  if (final.length !== 1) fail('Separate original-final proof is required')
  else {
    try {
      const last = ShareCycleOracle.plan(29)
      const proof = final[0].final
      if (
        proof.timing?.kind !== 'final' ||
        proof.timing?.cycle !== 29 ||
        proof.timing?.due - proof.timing?.startedAt > 20000 ||
        proof.timing?.startedAt < timing?.phases?.at(-1)?.completedAt
      )
        fail('Original-final proof is outside its separate final window')
      ShareCycleOracle.sequence(proof, fail)
      ShareCycleOracle.broker(last, proof.broker, fail)
      ShareCycleOracle.verifier(proof.verifier, 5000, fail)
      if (proof.positions?.length !== 2 || verifiers.has(proof.verifier.memberId))
        fail('Original-final verifier and terminal observations must be separate')
      for (const position of proof.positions ?? []) ShareCycleOracle.position(last, position, fail)
      ShareCycleOracle.log(
        last,
        new Map(original.at(-1).oracles.log.expected.map((identity) => [identity, {}])),
        proof,
        fail
      )
      const originalPrefix = original.flatMap((row) => row.oracles.log.expected)
      if (!isDeepStrictEqual([...proof.prefix].sort(), originalPrefix.sort()))
        fail('Original-final corpus must remain 1800 identities')
    } catch (error) {
      fail(`Original-final proof is incomplete: ${error.message}`)
    }
  }
  const resourceRows = rows.filter(({ event }) => event === 'soak-resources')
  for (const kind of ['baseline', 'teardown', 'resource-verdict'])
    if (resourceRows.filter((row) => row.kind === kind).length !== 1)
      fail(`Missing resource ${kind}`)
  if (
    !isDeepStrictEqual(
      resourceRows.filter((row) => row.kind === 'phase-end').map(({ phase }) => phase),
      CAPTURES.slice(1, 4)
    )
  )
    fail('Original disconnected phase ends are incomplete')
  if (
    rows.filter(
      ({ event, kind, phase }) =>
        event === 'diag-repetition-resources' && kind === 'phase-end' && phase === 'diag-repeat-10'
    ).length !== 1
  )
    fail('Repetition phase end is required')
  for (const phase of CAPTURES.slice(1, 4))
    if (resourceRows.filter((row) => row.kind === 'sample' && row.phase === phase).length < 6)
      fail(`Original periodic evidence is incomplete: ${phase}`)
  if (
    rows.filter(
      (row) =>
        row.event === 'diag-repetition-resources' &&
        row.kind === 'sample' &&
        row.phase === 'diag-repeat-10'
    ).length < 6
  )
    fail('Repetition periodic evidence is incomplete')
  const cleanup = events('diag-cleanup')
  if (
    cleanup.length !== 1 ||
    cleanup[0].status !== 'passed' ||
    !Number.isFinite(cleanup[0].completedAt) ||
    !Number.isFinite(cleanup[0].spentMs) ||
    cleanup[0].spentMs > 20000 ||
    cleanup[0].budgetMs !== 20000
  )
    fail('Successful strict cleanup is required')
  if (
    completed &&
    (events('diag-complete').length !== 1 ||
      events('diag-complete')[0].extraCycles !== 10 ||
      rows.at(-1)?.event !== 'diag-complete')
  )
    fail('Final diagnostic completion row is required')
  if (cleanup.length === 1) {
    try {
      const project = (snapshot) => {
        if (
          !snapshot ||
          !['listeners', 'activeResources'].every(
            (key) =>
              snapshot[key] &&
              typeof snapshot[key] === 'object' &&
              Object.values(snapshot[key]).every(
                (value) => Number.isSafeInteger(value) && value >= 0
              )
          ) ||
          !['queueDepth', 'ledgerSize'].every(
            (key) => Number.isSafeInteger(snapshot[key]) && snapshot[key] >= 0
          ) ||
          !['created', 'destroyed', 'open'].every(
            (key) => Number.isSafeInteger(snapshot.sockets?.[key]) && snapshot.sockets[key] >= 0
          ) ||
          snapshot.sockets.created - snapshot.sockets.destroyed !== snapshot.sockets.open
        )
          throw new Error('Missing or invalid original resource inventory')
        return {
          listeners: snapshot.listeners,
          activeResources: snapshot.activeResources,
          sockets: snapshot.sockets.open,
          queueDepth: snapshot.queueDepth,
          ledgerSize: snapshot.ledgerSize,
        }
      }
      if (!isDeepStrictEqual(project(cleanup[0].baseline), project(cleanup[0].after)))
        fail('Strict cleanup raw resources do not match their original baseline')
    } catch (error) {
      fail(`Strict cleanup measurements are incomplete: ${error.message}`)
    }
  }
  const order = ['diag-original-final', 'diag-extra-cycle', 'diag-cleanup', 'diag-complete'].map(
    (event) => rows.findIndex((row) => row.event === event)
  )
  if (
    order
      .slice(0, completed ? 4 : 3)
      .some((value, i, list) => value < 0 || (i > 0 && value <= list[i - 1]))
  )
    fail('Diagnostic proof and cleanup order is invalid')
  const captureIndices = events('diag-heap-snapshot').map((capture) => rows.indexOf(capture))
  const rowIndex = (event, cycle) =>
    rows.findIndex((row) => row.event === event && (row.cycle ?? row.plan?.cycle) === cycle)
  const boundaries = [
    [-1, rowIndex('soak-cycle', 0)],
    [rowIndex('soak-cycle', 9), rowIndex('soak-cycle', 10)],
    [rowIndex('soak-cycle', 19), rowIndex('soak-cycle', 20)],
    [rowIndex('soak-cycle', 29), rows.findIndex((row) => row.event === 'diag-original-final')],
    [rowIndex('diag-extra-cycle', 39), rows.findIndex((row) => row.event === 'diag-cleanup')],
  ]
  if (
    captureIndices.length !== 5 ||
    boundaries.some(
      ([before, after], i) => captureIndices[i] <= before || captureIndices[i] >= after
    )
  )
    fail('Captures must surround the original phases and the fixed repetition block')
  return { complete: violations.length === 0, violations }
}

module.exports = { CAPTURES, evidenceStatus }
