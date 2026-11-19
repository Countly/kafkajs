'use strict'

/**
 * Schema 3 of the final soak artifact: the matrix, the run config, the header and the
 * newline-delimited record stream. It is the only code the driver and the verifier share.
 *
 * Validity is structural: exact field types, discriminators, roles, arms, references and
 * per-source chronology. Whether a run passes a row is computed by the verifier alone; a failed run
 * stays representable here. Every exported check returns problem strings and never throws on
 * parsed JSON: types are guarded before any lookup, comparison or regular expression, and
 * diagnostics name a field path and expected type, never the offending value.
 *
 * Times (`at`, `t`, `Ms` fields) are milliseconds of the Linux monotonic clock relative to
 * `header.clock.originMs`. Offsets are canonical decimal int64 strings compared as BigInt.
 */
const crypto = require('node:crypto')

const SCHEMA_VERSION = 3

/** The eight row ids, in matrix order. */
const ROWS = Object.freeze([
  'rs-p10',
  'rs-t11',
  'rs-g07',
  'rs-l14',
  'rs-l15',
  'rs-l16',
  'rs-l22',
  'rs-l24',
])

/** The twelve judged client kinds, one standing subject process each. */
const CLIENT_KINDS = Object.freeze([
  'classic-eager-dynamic',
  'classic-eager-static',
  'classic-cooperative-dynamic',
  'classic-cooperative-static',
  'consumer-dynamic',
  'consumer-static',
  'share',
  'producer-idempotent',
  'producer-transactional',
  'admin',
  'telemetry-producer',
  'telemetry-consumer',
])

const SHAPES = Object.freeze([
  'traffic',
  'idle',
  'regex-churn',
  'topic-recreation',
  'pause-resume',
  'membership-churn',
  'broker-outage',
])

const MATRIX_SECTIONS = Object.freeze([
  'schema',
  'acceptance',
  'host',
  'traffic',
  'clientKinds',
  'topology',
  'schedule',
  'sampling',
  'controls',
  'recordSections',
  'observations',
  'bounds',
  'predicates',
  'rows',
])

/** The shared row predicates the matrix defines once and rows reference by name. */
const PREDICATES = Object.freeze([
  'Complete',
  'Delivery',
  'AuthorityOK',
  'ResourcesOK',
  'HeapOK',
  'TablesOK',
])
const OTHER_TABLES = Object.freeze([
  'topic-configurations',
  'preferred-replicas',
  'grown-topic-names',
  'queued-seeks',
  'cluster-targets',
  'acquired',
  'halted',
])
/** Every backing table TablesOK maps to its source and permitted live owner. */
const TABLES = Object.freeze([
  'subscription',
  'pause-state',
  'assignment',
  'membership-state',
  'operation-cell',
  'reset-debt',
  ...OTHER_TABLES,
  'share-state',
])
const ARCHIVE = Object.freeze([
  'scripts/soak/finalSoakMatrix.json',
  'scripts/soak/finalSoakSchema.js',
  'scripts/soak/finalSoak.js',
  'scripts/soak/finalSoakScheduler.js',
  'scripts/soak/finalSoakCollector.js',
  'scripts/soak/finalSoakObserver.js',
  'scripts/soak/finalSoakWorkers.js',
])
/** The seed's fixed start, clear and sensitivity rule in each mode. */
const SEED_TIMING = Object.freeze({
  acceptance: {
    starts: 'after-first-quiet-triplet',
    clears: 'after-last-quiet-triplet-before-disconnect',
    sensitivity: 'required',
  },
  smoke: {
    starts: 'after-baseline-before-traffic',
    clears: 'after-only-quiet-triplet-before-disconnect',
    sensitivity: 'report-only',
  },
})

const COMPANIONS = Object.freeze(['natural-exit', 'consume-retention', 'prior-checkpoints'])
const CONTROL = 'interning-seeded'
const MODES = Object.freeze(['smoke', 'acceptance'])
const PROCESS_ROLES = Object.freeze(['subject', 'seed', 'reader', 'collector', 'decoder'])
const SAMPLE_ROLES = Object.freeze(['baseline', 'periodic', 'quiet', 'closed', 'final'])
const REQUIRED_FLAGS = Object.freeze(['--expose-gc', '--no-flush-bytecode'])
const EXPLICIT_RESOURCES = Object.freeze(['TCPSocketWrap', 'Timeout'])
const OUTER = 'outer'
const MAX_OFFSET = 2n ** 63n - 1n
const MAX_PROBLEMS = 1000
/** Records the stream validator holds at once for a client whose construction has not arrived. */
const MAX_HELD = 100000
/**
 * Entries the stream validator keeps of any one kind of live state (constructed clients, open and
 * closed executions, open inventory cuts, open counting records and their open items); a normal day
 * keeps far fewer, and a stream that needs more fails.
 */
const MAX_OPEN = 100000
/** Each item record kind and the counting record kind it cites by `cause`. */
const ITEM_KINDS = Object.freeze({ 'wave-key': 'wave', 'partition-state': 'broker-state' })

/** The arm and control id each process role runs with. */
const ROLE_ARMS = Object.freeze({
  subject: ['unseeded', null],
  seed: [CONTROL, CONTROL],
  reader: ['observer', null],
  collector: ['observer', null],
  decoder: ['observer', null],
})

const isObject = (v) => v !== null && typeof v === 'object' && !Array.isArray(v)
const isString = (v) => typeof v === 'string'
const own = (object, key) => isObject(object) && isString(key) && Object.hasOwn(object, key)

/** @param {string} key @returns {string} a field name safe to print */
const nameOf = (key) => (/^[\w.-]{1,64}$/.test(key) ? key : JSON.stringify(key.slice(0, 32)))

/**
 * @typedef {{ label: string, test: (v: unknown) => boolean }} Type
 * @param {string} label what a valid value is, for diagnostics
 * @param {(v: unknown) => boolean} test
 * @returns {Type}
 */
const type = (label, test) => ({ label, test })

const Id = type('an Id', (v) => isString(v) && v.length > 0 && Buffer.byteLength(v) <= 128)
const Text = type('text', (v) => isString(v) && v.length > 0)
const Topic = type(
  'a topic name',
  (v) => isString(v) && v.length > 0 && Buffer.byteLength(v) <= 249
)
const Uuid = type('a topic id', (v) => isString(v) && /^[A-Za-z0-9_-]{22}$/.test(v))
const Count = type('a Count', (v) => Number.isSafeInteger(v) && v >= 0)
const Int = type('an integer', (v) => Number.isSafeInteger(v))
const Ms = type('Ms', (v) => typeof v === 'number' && Number.isFinite(v) && v >= 0)
const Bool = type('a boolean', (v) => typeof v === 'boolean')
const Sha = type('a sha256', (v) => isString(v) && /^[0-9a-f]{64}$/.test(v))
const CommitSha = type('a commit sha', (v) => isString(v) && /^[0-9a-f]{40}$/.test(v))
const Offset = type(
  'an Offset',
  (v) => isString(v) && /^(0|[1-9][0-9]{0,18})$/.test(v) && BigInt(v) <= MAX_OFFSET
)

/** @param {...unknown} values @returns {Type} membership by SameValueZero, never coercing */
const oneOf = (...values) => type(`one of ${values.join('|')}`, (v) => values.includes(v))
/** @param {Type} t @returns {Type} */
const nullable = (t) => type(`${t.label} or null`, (v) => v === null || t.test(v))
/** @param {Type} t @returns {Type} */
const listOf = (t) => type(`a list of ${t.label}`, (v) => Array.isArray(v) && v.every(t.test))

/**
 * Checks an object has exactly the given fields, each of its type.
 * @param {unknown} value
 * @param {Record<string, Type>} fields
 * @param {string} path printed before each problem
 * @param {boolean} [open=false] whether fields beyond `fields` are allowed
 * @returns {string[]}
 */
const fieldProblems = (value, fields, path, open = false) => {
  if (!isObject(value)) return [`${path} is not an object`]
  const problems = []
  if (!open) {
    for (const key of Object.keys(value)) {
      if (!Object.hasOwn(fields, key)) problems.push(`${path} has unknown field ${nameOf(key)}`)
    }
  }
  for (const [key, { label, test }] of Object.entries(fields)) {
    if (!Object.hasOwn(value, key)) problems.push(`${path}.${key} is missing`)
    else if (!test(value[key])) problems.push(`${path}.${key} is not ${label}`)
  }
  return problems
}

/** @param {string} label @param {Record<string, Type>} fields @returns {Type} an exact object */
const shape = (label, fields) => type(label, (v) => fieldProblems(v, fields, '').length === 0)

const Key = shape('a Key', { topic: Topic, topicId: Uuid, partition: Count })
const Authority = shape('an Authority', {
  client: Id,
  run: Id,
  member: Id,
  epoch: Int,
  token: Count,
  assignmentEpoch: Count,
  acquiredAt: Count,
  recreatedAt: Count,
})
const Gc = shape('a Gc', {
  id: Id,
  firstBeginAt: Ms,
  firstEndAt: Ms,
  secondBeginAt: Ms,
  secondEndAt: Ms,
  forcedCollections: oneOf(2),
  cumulativeForced: Count,
})
const TopicRef = shape('a topic reference', { topic: Topic, topicId: Uuid })
const LooseTopicRef = shape('a topic reference', { topic: Topic, topicId: nullable(Uuid) })
const TableKey = shape('a TableKey', {
  topic: Topic,
  topicId: nullable(Uuid),
  partition: nullable(Count),
})
const ARM = oneOf('unseeded', CONTROL, 'observer')
const CONTROL_ID = nullable(oneOf(CONTROL))
const RESULT = nullable(oneOf('ok', 'error'))
const PROTOCOL = oneOf('eager', 'cooperative', 'consumer')

const ENVELOPE = Object.freeze({
  kind: Id,
  recordId: Id,
  source: Id,
  seq: Count,
  at: Ms,
  t: Ms,
  execution: nullable(Id),
})

const CALL = {
  client: Id,
  phase: oneOf('begin', 'end'),
  call: Id,
  result: RESULT,
  errorType: nullable(Id),
}
const SHAPE_FIELDS = { shape: oneOf(...SHAPES), participants: listOf(Id), operationCount: Count }
const ROUTE = {
  group: Id,
  topic: Topic,
  topicId: Uuid,
  partition: Count,
  from: Offset,
  to: nullable(Offset),
  topicRecord: Id,
  scan: nullable(Id),
}
const OPERATION = {
  operation: Id,
  op: oneOf(
    'subscribe',
    'unsubscribe',
    'pause',
    'resume',
    'restart',
    'broker-stop',
    'broker-start'
  ),
  client: nullable(Id),
  keys: listOf(Key),
  call: Id,
  result: RESULT,
  errorType: nullable(Id),
  causes: listOf(Id),
}
const SEED = { process: Id, client: Id, controlId: oneOf(CONTROL), mapEntries: Count, cause: Id }

/**
 * Every record kind: `fields`, or `by` (the discriminator field) and its `variants`. `open` admits
 * fields beyond those listed (the diagnostic note only). A `wave` or `broker-state` carries the
 * count of its keys or partitions; each one is its own `wave-key` or `partition-state` record of
 * the same source citing it by `cause`, so no record grows with the topology past its line cap.
 */
const RECORDS = Object.freeze({
  process: {
    by: 'event',
    variants: {
      start: {
        process: Id,
        pid: Count,
        parent: Id,
        arm: ARM,
        controlId: CONTROL_ID,
        execArgv: listOf(Id),
        nodeOptions: listOf(Id),
        node: Id,
        v8: Id,
        bootId: Id,
      },
      exit: { process: Id, code: nullable(Int), signal: nullable(Id), spawnRecord: Id },
    },
  },
  client: {
    by: 'event',
    variants: {
      construct: {
        client: Id,
        slot: Id,
        clientKind: oneOf(...CLIENT_KINDS),
        process: Id,
        arm: ARM,
        controlId: CONTROL_ID,
        purpose: oneOf('subject', 'churn', 'observer', 'control'),
        group: nullable(Id),
        groupInstanceId: nullable(Id),
        standing: Bool,
        telemetryEnabled: Bool,
        configSha256: Sha,
      },
      connect: CALL,
      run: CALL,
      stop: CALL,
      disconnect: CALL,
    },
  },
  shape: { by: 'phase', variants: { start: SHAPE_FIELDS, end: SHAPE_FIELDS } },
  topic: {
    fields: {
      client: Id,
      call: Id,
      op: oneOf('create', 'delete', 'metadata'),
      topic: Topic,
      topicId: nullable(Uuid),
      partitions: listOf(Count),
      errorCode: Int,
    },
  },
  route: { by: 'event', variants: { start: ROUTE, seal: ROUTE } },
  operation: { by: 'phase', variants: { begin: OPERATION, end: OPERATION } },
  subscription: {
    fields: {
      client: Id,
      revision: Count,
      run: Id,
      names: listOf(TopicRef),
      mode: oneOf('literal', 'client-pattern', 'broker-regex'),
      pattern: nullable(Id),
      cause: Id,
    },
  },
  boundary: {
    fields: {
      client: Id,
      group: Id,
      run: Id,
      member: Id,
      epoch: Int,
      protocol: PROTOCOL,
      event: oneOf('acquire', 'revoke-begin', 'revoke-end', 'lost', 'position'),
      key: Key,
      authority: Authority,
      position: nullable(Offset),
      cause: Id,
    },
  },
  round: {
    fields: {
      client: Id,
      group: Id,
      run: Id,
      round: Id,
      protocol: PROTOCOL,
      epoch: Int,
      assigned: listOf(Key),
      revoked: listOf(Key),
      causes: listOf(Id),
    },
  },
  seed: { by: 'event', variants: { start: SEED, clear: SEED } },
  'broker-state': {
    fields: {
      node: Count,
      pid: Count,
      container: Id,
      event: oneOf('down', 'ready', 'isr'),
      members: listOf(Count),
      partitions: Count,
      cause: Id,
      downMs: Ms,
      processAt: nullable(Ms),
      processTime: nullable(Id),
    },
  },
  'partition-state': { fields: { cause: Id, key: Key, leader: Int, isr: listOf(Count) } },
  wave: {
    fields: {
      client: Id,
      event: oneOf('transfer', 'return'),
      member: Id,
      paused: Count,
      held: Count,
      observed: Bool,
    },
  },
  'wave-key': { fields: { cause: Id, key: Key, held: Bool } },
  identity: {
    by: 'stage',
    variants: {
      offered: {
        client: Id,
        id: Id,
        key: Key,
        payloadSha256: Sha,
        bytes: Count,
        transaction: nullable(Id),
      },
      ack: {
        client: Id,
        id: Id,
        outcome: oneOf('acknowledged', 'unknown', 'rejected'),
        key: Key,
        offset: nullable(Offset),
        errorType: nullable(Id),
      },
      transaction: {
        client: Id,
        transaction: Id,
        decision: oneOf('commit', 'abort'),
        outcome: oneOf('committed', 'aborted', 'unknown'),
        call: Id,
        errorType: nullable(Id),
      },
      log: {
        reader: Id,
        scan: Id,
        id: Id,
        key: Key,
        offset: Offset,
        payloadSha256: Sha,
        transaction: nullable(Id),
      },
      'handler-enter': {
        client: Id,
        group: Id,
        handler: Id,
        id: Id,
        key: Key,
        offset: Offset,
        payloadSha256: Sha,
        authority: nullable(Authority),
        acquisition: nullable(Id),
        deliveryCount: nullable(Count),
      },
      'handler-end': {
        handler: Id,
        outcome: oneOf('processed', 'retry', 'stale'),
        errorType: nullable(Id),
      },
      commit: {
        client: Id,
        group: Id,
        commit: Id,
        key: Key,
        next: Offset,
        authority: Authority,
        phase: oneOf('begin', 'write', 'end', 'publish'),
        outcome: nullable(oneOf('ok', 'refused', 'unknown')),
        errorType: nullable(Id),
      },
      'broker-commit': {
        client: Id,
        group: Id,
        commit: nullable(Id),
        key: Key,
        next: nullable(Offset),
        errorCode: Int,
      },
      'share-outcome': {
        client: Id,
        group: Id,
        key: Key,
        from: Offset,
        to: Offset,
        type: oneOf('accept', 'release', 'reject', 'renew'),
        reason: nullable(Id),
        errorType: nullable(Id),
      },
    },
  },
  scan: {
    fields: {
      scan: Id,
      key: Key,
      from: Offset,
      to: Offset,
      lso: Offset,
      highWatermark: Offset,
      rawFile: Id,
      rawSha256: Sha,
      records: Count,
    },
  },
  replay: {
    fields: {
      replay: Id,
      group: Id,
      key: Key,
      from: Offset,
      to: Offset,
      reason: oneOf('handler-retry', 'rebalance', 'outage'),
      oldAuthority: Authority,
      newAuthority: Authority,
      cause: Id,
    },
  },
  'share-acquire': {
    fields: {
      acquisition: Id,
      client: Id,
      group: Id,
      key: Key,
      from: Offset,
      to: Offset,
      deliveryCount: Count,
      receivedAt: Ms,
      lockMs: Count,
      wireFile: Id,
      wireSha256: Sha,
    },
  },
  'share-settle': {
    fields: {
      acquisition: Id,
      client: Id,
      group: Id,
      key: Key,
      offset: Offset,
      type: oneOf('accept', 'release', 'reject'),
      phase: oneOf('request', 'response'),
      errorCode: nullable(Int),
      wireFile: Id,
      wireSha256: Sha,
    },
  },
  telemetry: {
    fields: {
      client: Id,
      instanceId: Id,
      subscriptionId: Id,
      broker: Count,
      metrics: Count,
      terminating: Bool,
      rawFile: Id,
      rawSha256: Sha,
      decodedFile: Id,
      decodedSha256: Sha,
    },
  },
  sample: {
    fields: {
      process: Id,
      role: oneOf(...SAMPLE_ROLES),
      slot: nullable(Count),
      boundary: nullable(Id),
      reading: nullable(Count),
      admittedAt: Ms,
      completedAt: Ms,
      rss: Count,
      heapUsed: Count,
      external: Count,
      activeResources: listOf(shape('a resource count', { type: Id, count: Count })),
      gc: nullable(Gc),
      inventory: Id,
      clients: listOf(Id),
    },
  },
  inventory: {
    fields: {
      inventory: Id,
      process: Id,
      client: nullable(Id),
      section: type('a known section', (v) => own(SECTIONS, v)),
      chunk: Count,
      chunks: Count,
      entries: listOf(type('an object', isObject)),
    },
  },
  ticket: {
    fields: {
      ticket: Id,
      ledger: Id,
      client: Id,
      ownerRun: nullable(Id),
      phase: oneOf('begin', 'end'),
      keys: listOf(Key),
      token: nullable(Count),
    },
  },
  diagnostic: { fields: { message: Text }, open: true },
})

/** The record kinds, in schema order. */
const RECORD_KINDS = Object.freeze(Object.keys(RECORDS))

/**
 * Inventory sections: `entry` fields and `key`, the identity that must be unique across a section's
 * chunks in one cut.
 */
const SECTIONS = Object.freeze({
  queues: {
    entry: { queue: Id, client: Id, connection: Id, scope: Id, pending: Count, inflight: Count },
    key: (e) => e.queue,
  },
  requests: {
    entry: {
      request: Id,
      queue: Id,
      apiKey: Count,
      phase: oneOf('pending', 'inflight'),
      scope: Id,
      beganAt: Ms,
      deadlineAt: nullable(Ms),
    },
    key: (e) => e.request,
  },
  sockets: {
    entry: { socket: Id, client: Id, scope: Id, node: Id, connection: Id, destroyed: Bool },
    key: (e) => e.socket,
  },
  listeners: {
    entry: {
      emitter: Id,
      client: nullable(Id),
      scope: Id,
      event: nullable(Id),
      registrations: listOf(shape('a registration', { listener: Id, scope: Id })),
    },
    key: (e) => `${e.emitter}\0${e.event}`,
  },
  resources: {
    entry: { resource: Id, type: Id, scope: Id, client: nullable(Id) },
    key: (e) => e.resource,
  },
  scopes: {
    entry: {
      scope: Id,
      owner: Id,
      type: oneOf('run', 'request', 'worker', 'harness'),
      live: Bool,
      retiredAt: nullable(Ms),
    },
    key: (e) => e.scope,
  },
  subscription: {
    entry: {
      client: Id,
      run: Id,
      revision: Count,
      accepted: listOf(TopicRef),
      desired: listOf(LooseTopicRef),
      observed: listOf(LooseTopicRef),
    },
    key: (e) => `${e.client}\0${e.run}`,
  },
  'pause-state': {
    entry: {
      client: Id,
      run: Id,
      stateId: Id,
      topic: Topic,
      originTopicId: Uuid,
      subscriptionLife: Id,
      pauseAll: Bool,
      paused: listOf(Count),
      resumed: listOf(Count),
      lastOperation: Id,
    },
    key: (e) => e.stateId,
  },
  assignment: {
    entry: { client: Id, key: Key, authority: Authority },
    key: (e) => `${e.client}\0${e.key.topicId}\0${e.key.partition}`,
  },
  'membership-state': {
    entry: { client: Id, run: Id, token: Count, current: Bool, holders: Count, refused: Bool },
    key: (e) => `${e.client}\0${e.run}\0${e.token}`,
  },
  'operation-cell': {
    entry: { ledger: Id, topic: Topic, partition: Count, open: Count, tickets: listOf(Id) },
    key: (e) => `${e.ledger}\0${e.topic}\0${e.partition}`,
  },
  'reset-debt': {
    entry: { client: Id, topic: Topic, topicId: Uuid, discharged: listOf(Count) },
    key: (e) => `${e.client}\0${e.topicId}`,
  },
  'other-table': {
    entry: {
      client: Id,
      table: oneOf(...OTHER_TABLES),
      keys: listOf(TableKey),
    },
    key: (e) => `${e.client}\0${e.table}`,
  },
  'share-state': {
    entry: {
      client: Id,
      run: nullable(Id),
      sessions: listOf(shape('a session', { node: Count, session: Int })),
      records: listOf(
        shape('a held record', {
          acquisition: Id,
          key: Key,
          offset: Offset,
          state: oneOf('held', 'settling'),
        })
      ),
      ranges: listOf(
        shape('a held range', { acquisition: Id, key: Key, from: Offset, to: Offset })
      ),
      cohorts: listOf(Id),
    },
    key: (e) => e.client,
  },
})

/** Which sections a cut holds for the process (client null) and for each client inventory class. */
const SECTION_SETS = Object.freeze({
  process: ['queues', 'requests', 'sockets', 'listeners', 'resources', 'scopes'],
  ordinary: [
    'subscription',
    'pause-state',
    'assignment',
    'membership-state',
    'operation-cell',
    'reset-debt',
    'other-table',
  ],
  share: ['share-state'],
  none: [],
})

const big = (offset) => BigInt(offset)
/** @param {boolean} ok @param {string} problem @returns {string[]} */
const unless = (ok, problem) => (ok ? [] : [problem])
/** @param {unknown} a @param {unknown} b @returns {boolean} whether both are null or neither */
const sameNullness = (a, b) => (a === null) === (b === null)

/** @param {{ phase: string, result: unknown, errorType: unknown }} r @returns {string[]} */
const callRules = (r) => [
  ...unless(
    r.phase === 'begin' ? r.result === null : r.result !== null,
    'result is set only at the end of a call'
  ),
  ...unless(
    sameNullness(r.errorType, r.result === 'error' ? 1 : null),
    'errorType is set only for an error result'
  ),
]

/** Cross-field rules of a typed record, keyed by kind or kind/variant. */
const RULES = Object.freeze({
  'process/start': (r) => [
    ...unless((r.arm === CONTROL) === (r.controlId === CONTROL), 'arm and controlId disagree'),
    ...REQUIRED_FLAGS.flatMap((flag) =>
      unless(
        r.execArgv.includes(flag) || r.nodeOptions.includes(flag),
        `node flag ${flag} is absent`
      )
    ),
  ],
  'client/construct': (r) =>
    unless((r.arm === CONTROL) === (r.controlId === CONTROL), 'arm and controlId disagree'),
  'client/connect': callRules,
  'client/run': callRules,
  'client/stop': callRules,
  'client/disconnect': callRules,
  'shape/start': (r) => [
    ...unless(r.execution !== null, 'a shape needs an execution id'),
    ...unless(r.operationCount === 0, 'operationCount is 0 at start'),
  ],
  'shape/end': (r) => unless(r.execution !== null, 'a shape needs an execution id'),
  'route/start': (r) =>
    unless(r.to === null && r.scan === null, 'a route start has no end or scan'),
  'route/seal': (r) =>
    r.to === null || r.scan === null
      ? ['a route seal needs its end offset and scan']
      : unless(big(r.to) >= big(r.from), 'a route ends before it starts'),
  'operation/begin': (r) => [
    ...callRules({ phase: 'begin', result: r.result, errorType: r.errorType }),
    ...unless(
      sameNullness(r.client, r.op.startsWith('broker-') ? null : 1),
      'only broker operations have no client'
    ),
  ],
  'operation/end': (r) => [
    ...callRules({ phase: 'end', result: r.result, errorType: r.errorType }),
    ...unless(
      sameNullness(r.client, r.op.startsWith('broker-') ? null : 1),
      'only broker operations have no client'
    ),
  ],
  subscription: (r) =>
    unless(
      sameNullness(r.pattern, r.mode === 'literal' ? null : 1),
      'pattern is set only for a pattern mode'
    ),
  'identity/ack': (r) =>
    unless(
      r.offset === null || r.outcome === 'acknowledged',
      'only an acknowledged send has an offset'
    ),
  'identity/transaction': (r) =>
    unless(
      r.outcome === 'unknown' || r.outcome === (r.decision === 'commit' ? 'committed' : 'aborted'),
      'the transaction outcome contradicts its decision'
    ),
  'identity/handler-enter': (r) => [
    ...unless(
      !sameNullness(r.authority, r.acquisition),
      'a handler carries an authority or a share acquisition'
    ),
    ...unless(
      sameNullness(r.acquisition, r.deliveryCount),
      'acquisition and deliveryCount go together'
    ),
  ],
  'identity/commit': (r) =>
    unless(
      sameNullness(r.outcome, r.phase === 'end' ? 1 : null),
      'only a commit end has an outcome'
    ),
  scan: (r) =>
    unless(
      big(r.from) <= big(r.to) && big(r.to) <= big(r.lso) && big(r.lso) <= big(r.highWatermark),
      'a scan needs from <= to <= lso <= highWatermark'
    ),
  replay: (r) => unless(big(r.from) < big(r.to), 'a replay range is empty'),
  'share-acquire': (r) => unless(big(r.from) <= big(r.to), 'an acquisition ends before it starts'),
  'share-settle': (r) =>
    unless(
      sameNullness(r.errorCode, r.phase === 'request' ? null : 1),
      'only a response has an errorCode'
    ),
  sample: (r) => sampleRules(r),
  inventory: (r) => [
    ...unless(r.chunks >= 1 && r.chunk < r.chunks, 'chunk is not below chunks'),
    ...r.entries.flatMap((entry, i) => entryProblems(r.section, entry, `entries[${i}]`)),
  ],
  ticket: (r) => unless(sameNullness(r.ownerRun, r.token), 'ownerRun and token go together'),
  wave: (r) => unless(r.held <= r.paused, 'a wave holds no more keys than it paused'),
})

/** @param {object} r a typed sample @returns {string[]} */
const sampleRules = (r) => {
  const problems = []
  const want = {
    baseline: [null, null, null],
    periodic: ['slot', null, null],
    quiet: [null, 'boundary', 'reading'],
    closed: [null, 'boundary', null],
    final: [null, null, null],
  }[r.role]
  for (const [i, field] of ['slot', 'boundary', 'reading'].entries()) {
    if ((r[field] === null) !== (want[i] === null)) {
      problems.push(`${field} is ${want[i] === null ? 'null' : 'required'} for a ${r.role} sample`)
    }
  }
  if (r.role === 'quiet' && r.reading !== null && r.reading > 2)
    problems.push('reading is 0, 1 or 2')
  if (r.admittedAt > r.completedAt) problems.push('a sample completes before it is admitted')
  const types = r.activeResources.map((resource) => resource.type)
  if (new Set(types).size !== types.length) problems.push('activeResources repeats a type')
  for (const name of EXPLICIT_RESOURCES) {
    if (!types.includes(name)) problems.push(`activeResources omits ${name}`)
  }
  const gc = r.gc
  if (
    gc !== null &&
    !(
      r.admittedAt <= gc.firstBeginAt &&
      gc.firstBeginAt <= gc.firstEndAt &&
      gc.firstEndAt + 100 <= gc.secondBeginAt &&
      gc.secondBeginAt <= gc.secondEndAt &&
      gc.secondEndAt <= r.completedAt
    )
  ) {
    problems.push('gc collections are not two ordered calls 100 ms apart inside the sample')
  }
  return problems
}

/** Cross-field rules of an inventory entry, by section. */
const ENTRY_RULES = Object.freeze({
  listeners: (e) =>
    unless(
      (e.event === null) === (e.registrations.length === 0),
      'only an empty emitter has a null event'
    ),
  scopes: (e) => unless(e.live === (e.retiredAt === null), 'a live scope has no retiredAt'),
  'pause-state': (e) =>
    unless(
      e.subscriptionLife.endsWith(`/${e.originTopicId}`),
      'subscriptionLife does not end in its origin topic id'
    ),
  'operation-cell': (e) => unless(e.open === e.tickets.length, 'open differs from its tickets'),
})

/**
 * @param {string} section a known section
 * @param {unknown} entry
 * @param {string} path
 * @returns {string[]}
 */
const entryProblems = (section, entry, path) => {
  const problems = fieldProblems(entry, SECTIONS[section].entry, path)
  if (problems.length > 0 || !ENTRY_RULES[section]) return problems
  return ENTRY_RULES[section](entry).map((p) => `${path}: ${p}`)
}

/**
 * Checks one parsed record's kind, variant, exact fields and cross-field rules. References and
 * chronology need the whole stream; see `StreamValidator`.
 * @param {unknown} record
 * @returns {string[]} problems, empty when the record is well formed
 */
const validateRecord = (record) => {
  if (!isObject(record)) return ['record is not an object']
  const { kind } = record
  if (!own(RECORDS, kind)) return ['record.kind is not a known record kind']
  const spec = RECORDS[kind]
  let fields = spec.fields
  let rule = kind
  if (spec.by) {
    const variant = record[spec.by]
    if (!own(spec.variants, variant)) return [`record.${spec.by} is not a known ${kind} variant`]
    fields = { [spec.by]: oneOf(variant), ...spec.variants[variant] }
    rule = `${kind}/${variant}`
  }
  const problems = fieldProblems(record, { ...ENVELOPE, ...fields }, kind, spec.open)
  if (problems.length > 0 || !RULES[rule]) return problems
  return RULES[rule](record).map((p) => `${rule}: ${p}`)
}

const FORBIDDEN_TEXT = Object.freeze([
  [/\bD\d{2,3}\b/, 'a decision id'],
  [/\b[A-Z]{1,2}\d{0,2}-[A-Z]?\d{2}\b/, 'an uppercase registry or task label'],
  [/\b\d[A-Z]-\d{2}\b/, 'an uppercase registry or task label'],
  [
    /super[p]owers|upgrade[-]ref|\/var\/tmp|\/home\/|EXECUTION[.]md|CLAUDE[.]md/,
    'a local-only path',
  ],
])
const SOURCE = /^(symbol|code): [\w./-]+#[\w.]+$|^(rule|design): \S/

/** @param {unknown} value @param {string} path @returns {string[]} forbidden text anywhere inside */
const textProblems = (value, path) => {
  if (isString(value)) {
    return FORBIDDEN_TEXT.flatMap(([pattern, what]) =>
      pattern.test(value) ? [`${path} names ${what}`] : []
    )
  }
  if (Array.isArray(value)) return value.flatMap((v, i) => textProblems(v, `${path}[${i}]`))
  if (isObject(value)) {
    return Object.keys(value).flatMap((k) => textProblems(value[k], `${path}.${nameOf(k)}`))
  }
  return []
}

const Source = type(
  'a source (symbol:, code:, rule: or design:)',
  (v) => isString(v) && SOURCE.test(v)
)
const Setting = shape('a setting', {
  value: type('a finite number', Number.isFinite),
  unit: Id,
  source: Source,
})

/** @param {unknown} list @param {readonly unknown[]} allowed @returns {boolean} a non-empty subset */
const subsetOf = (list, allowed) =>
  Array.isArray(list) && list.length > 0 && list.every((v) => allowed.includes(v))
/** @param {unknown} list @param {readonly unknown[]} expected @returns {boolean} equal, in order */
const sameList = (list, expected) =>
  Array.isArray(list) && list.length === expected.length && list.every((v, i) => v === expected[i])

const Any = type('present', () => true)
const Positive = type('a positive Count', (v) => Count.test(v) && v > 0)
/** @param {string} label @param {(v: unknown) => boolean} test @returns {Type} */
const nonEmptyListOf = (label, test) =>
  type(label, (v) => Array.isArray(v) && v.length > 0 && v.every(test))
/** @param {readonly string[]} names @param {Type} t @returns {Record<string, Type>} */
const each = (names, t) => Object.fromEntries(names.map((n) => [n, t]))

/**
 * Checks an object-valued field of `parent` exactly, unless the parent's own check already reports
 * the field missing.
 * @param {unknown} parent
 * @param {string} key
 * @param {Record<string, Type>} fields
 * @param {string} path the parent's path
 * @returns {string[]}
 */
const nested = (parent, key, fields, path) =>
  own(parent, key) ? fieldProblems(parent[key], fields, `${path}.${key}`) : []

/**
 * Resolves a dotted reference from the matrix root through own keys only.
 * @param {object} matrix
 * @param {unknown} ref
 * @returns {unknown} the named value, or undefined
 */
const lookup = (matrix, ref) =>
  isString(ref)
    ? ref.split('.').reduce((v, k) => (own(v, k) ? v[k] : undefined), matrix)
    : undefined

const Bound = type('a bound', (v) => {
  if (!isObject(v)) return false
  const reportOnly = Object.hasOwn(v, 'reportOnly')
  const fields = {
    name: Text,
    value: reportOnly ? oneOf(null) : type('', Number.isFinite),
    unit: Id,
    source: Source,
    ...(reportOnly ? { reportOnly: oneOf(true) } : {}),
  }
  return fieldProblems(v, fields, '').length === 0
})
/**
 * Checks one row. Its bounds reference sourced bounds or settings elsewhere in the matrix, its
 * minima are products of positive counts and references to counts, so each constant is stored once,
 * and `uses` lists exactly the shared predicates its predicate text names.
 * @param {unknown} row
 * @param {number} i
 * @param {object} matrix
 * @returns {string[]}
 */
const rowProblems = (row, i, matrix) => {
  const path = `rows[${i}]`
  const kinds = isObject(matrix.clientKinds) ? Object.keys(matrix.clientKinds) : []
  const sourced = (ref) => {
    const v = lookup(matrix, ref)
    return Bound.test(v) || Setting.test(v)
  }
  const factor = (f) => {
    const v = isString(f) ? lookup(matrix, f) : f
    return Positive.test(Bound.test(v) || Setting.test(v) ? v.value : v)
  }
  const distinct = (v) => Array.isArray(v) && new Set(v).size === v.length
  const Minimum = shape('a minimum', {
    name: Text,
    of: nonEmptyListOf('factors', factor),
  })
  const fields = {
    id: oneOf(ROWS[i]),
    obligation: Text,
    scope: shape('a scope', {
      kinds: type('a non-empty list of client kinds', (v) => subsetOf(v, kinds)),
      shapes: type('a non-empty list of shapes', (v) => subsetOf(v, SHAPES)),
      judged: Text,
    }),
    evidence: type('a non-empty list of record kinds', (v) => subsetOf(v, RECORD_KINDS)),
    uses: type(
      'a list of distinct predicate names',
      (v) => distinct(v) && v.every((n) => PREDICATES.includes(n))
    ),
    predicate: Text,
    bounds: type(
      'a non-empty list of distinct references to sourced bounds or settings',
      (v) => distinct(v) && v.length > 0 && v.every(sourced)
    ),
    minima: nonEmptyListOf('a non-empty list of derived minima', Minimum.test),
    fail: Text,
    pass: Text,
    closure: type(
      'a list of companions',
      (v) => Array.isArray(v) && v.every((c) => COMPANIONS.includes(c))
    ),
  }
  const problems = fieldProblems(row, fields, path)
  if (problems.length > 0) return problems
  const named = PREDICATES.filter((n) => new RegExp(`\\b${n}\\b`).test(row.predicate))
  return unless(
    named.length === row.uses.length && named.every((n) => row.uses.includes(n)),
    `${path}.uses differs from the predicates its text names`
  )
}

const KIND_FIELDS = Object.freeze({
  role: oneOf('consumer', 'share', 'producer', 'admin'),
  protocol: nullable(oneOf('eager', 'cooperative', 'consumer', 'share')),
  static: Bool,
  grouped: Bool,
  telemetry: Bool,
  churn: Bool,
  inventory: oneOf('ordinary', 'share', 'none'),
})

const Window = type(
  'a window',
  (v) => fieldProblems(v, { from: Ms, to: Ms }, '').length === 0 && v.from < v.to
)
const QuietTimes = type(
  'three quiet times',
  (v) => Array.isArray(v) && v.length === 3 && v.every(Ms.test)
)
const Minutes = nonEmptyListOf('a non-empty list of minutes', (m) => Count.test(m) && m < 60)

const ACCEPTANCE_FIELDS = Object.freeze({
  profile: Id,
  modes: Any,
  archive: type(
    'a list of relative paths holding the archived matrix, schema and driver modules',
    (v) => Array.isArray(v) && v.every(isRelativePath) && ARCHIVE.every((f) => v.includes(f))
  ),
  companions: type('the companion list', (v) => sameList(v, COMPANIONS)),
  cli: type(
    'a list holding every required verifier argument',
    (v) =>
      Array.isArray(v) &&
      v.every(Text.test) &&
      ['--expect-sha', '--expect-profile', '--matrix', '--companion rs-l14=<run url>'].every((f) =>
        v.includes(f)
      )
  ),
  verdicts: shape('the day and closure vocabulary', {
    day: type('', (v) => sameList(v, ['PASS', 'FAIL', 'INCONCLUSIVE'])),
    closure: type('', (v) => sameList(v, ['COMPLETE', 'OPEN'])),
  }),
  limitations: nonEmptyListOf('a non-empty list of text', Text.test),
})
const MODE_FIELDS = each(['requestedMs', 'teardownAllowanceMs'], Setting)
const HOST_FIELDS = Object.freeze({
  platform: oneOf('linux'),
  ...each(
    [
      'minMemAvailableBytes',
      'minFreeDiskBytes',
      'maxLoadAverage1m',
      'abortMemAvailableBytes',
      'maxStreamBytes',
      'maxEventLineBytes',
      'maxInventoryLineBytes',
      'maxTelemetryBytes',
      'brokerReporterFileBytes',
    ],
    Setting
  ),
})
const TRAFFIC_FIELDS = Object.freeze({
  ...each(
    [
      'valueBytes',
      'maxOffers',
      'mainRate',
      'telemetryProducerRate',
      'transactionRecords',
      'transactionEveryMs',
      'abortEvery',
      'commitEvery',
      'metadataMaxAgeMs',
      'telemetryPushMaxMs',
    ],
    Setting
  ),
  floors: Any,
})
const FLOORS = Object.freeze([
  'confirmedCommits',
  'confirmedAborts',
  'visiblePerProducerKind',
  'mainPerGroup',
  'transactionsPerGroup',
  'auxiliaryPerIncarnation',
  'distinctOffsets',
])
const SAMPLING_FIELDS = Object.freeze({
  ...each(
    [
      'periodMs',
      'jitterMs',
      'periodicSlots',
      'warmupMs',
      'quietReadings',
      'gcCollections',
      'gcGapMs',
    ],
    Setting
  ),
  roles: type('the five sample roles', (v) => sameList(v, SAMPLE_ROLES)),
  explicitResources: type('TCPSocketWrap and Timeout', (v) => sameList(v, EXPLICIT_RESOURCES)),
  flags: type('the required node flags', (v) => sameList(v, REQUIRED_FLAGS)),
})
const SCHEDULE_FIELDS = Object.freeze({
  hourMs: Positive,
  shapes: type('the seven shapes', (v) => sameList(v, SHAPES)),
  activeMs: Window,
  idleMs: Window,
  auxiliaryDrainMs: Window,
  quietAtMs: QuietTimes,
  quietDeadlineMs: Positive,
  executions: type('a list', Array.isArray),
  totals: Any,
  smoke: Any,
})
const TOTALS = each(
  [
    'idleWindows',
    'regexExecutions',
    'recreations',
    'membershipExecutions',
    'pauseSequencesPerOrdinaryKind',
    'outages',
    'staticRestartsPerStaticKind',
  ],
  Positive
)
const SMOKE_FIELDS = Object.freeze({
  activeMs: Window,
  idleMs: Window,
  quietAtMs: QuietTimes,
  ...each(['membershipAtMs', 'regexAtMs', 'recreationAtMs', 'outageAtMs', 'recoveredByMs'], Ms),
  abortAtTransaction: Positive,
  periodicSlots: Positive,
  floors: Any,
})
const SMOKE_FLOORS = each(
  ['mainPerGroup', 'visiblePerProducerKind', 'confirmedCommits', 'confirmedAborts'],
  Positive
)
/** The fields of each scheduled auxiliary execution, in schedule order. */
const EXECUTIONS = Object.freeze({
  'membership-churn': {
    minutes: Minutes,
    maxMs: Positive,
    waves: Positive,
    waveMaxMs: Positive,
    pausesPerWave: Positive,
  },
  'regex-churn': { minutes: Minutes, maxMs: Positive },
  'topic-recreation': { minutes: Minutes, maxMs: Positive },
  'broker-outage': { minutes: Minutes, evenHoursOnly: Bool, recoveredByMinute: Positive },
})

/** @param {object} schedule @returns {string[]} the exact day schedule, totals and smoke profile */
const scheduleProblems = (schedule) => {
  const path = 'matrix.schedule'
  const problems = [
    ...fieldProblems(schedule, SCHEDULE_FIELDS, path),
    ...nested(schedule, 'totals', TOTALS, path),
    ...nested(schedule, 'smoke', SMOKE_FIELDS, path),
    ...nested(schedule.smoke, 'floors', SMOKE_FLOORS, `${path}.smoke`),
  ]
  const executions = schedule.executions
  if (!Array.isArray(executions)) return problems
  const shapes = Object.keys(EXECUTIONS)
  problems.push(
    ...unless(
      sameList(
        executions.map((e) => (isObject(e) ? e.shape : null)),
        shapes
      ),
      `${path}.executions is not one execution of each auxiliary shape`
    )
  )
  executions.forEach((e, i) => {
    const known = own(e, 'shape') && own(EXECUTIONS, e.shape)
    const fields = { shape: oneOf(...shapes), ...(known ? EXECUTIONS[e.shape] : {}) }
    problems.push(...fieldProblems(e, fields, `${path}.executions[${i}]`, !known))
  })
  return problems
}

const RuleList = nonEmptyListOf('a non-empty list of rules', Text.test)
const TableRule = shape('a table rule', { source: Source, owner: Text })

/**
 * Checks the shared predicate definitions: exactly the six names, each with its rules; Complete
 * also names every kind's recovery witness and TablesOK every backing table's source and owner.
 * @param {object} predicates
 * @returns {string[]}
 */
const predicateProblems = (predicates) => {
  const path = 'matrix.predicates'
  const extra = { Complete: { recovery: Any }, TablesOK: { tables: Any } }
  return [
    ...fieldProblems(predicates, each(PREDICATES, Any), path),
    ...PREDICATES.flatMap((n) => nested(predicates, n, { rules: RuleList, ...extra[n] }, path)),
    ...nested(predicates.Complete, 'recovery', each(CLIENT_KINDS, Text), `${path}.Complete`),
    ...nested(predicates.TablesOK, 'tables', each(TABLES, TableRule), `${path}.TablesOK`),
  ]
}

/**
 * Checks the matrix: version 3, every mandatory section with its container type, the exact nested
 * acceptance, host, traffic, schedule (day, executions, totals, smoke) and sampling settings, the
 * twelve kinds, the single isolated seed control with its per-mode timing, exact section and record
 * vocabularies, the sourced bound catalogue, the six shared predicate definitions, and the eight
 * ordered rows with scope, evidence, predicate uses, bound references, derived minima and closure.
 * Text anywhere may not name decision ids, uppercase registry labels or local-only paths.
 * @param {unknown} matrix parsed matrix JSON
 * @returns {string[]}
 */
const validateMatrix = (matrix) => {
  if (!isObject(matrix)) return ['matrix is not an object']
  const problems = textProblems(matrix, 'matrix')
  for (const name of MATRIX_SECTIONS) {
    if (!Object.hasOwn(matrix, name)) problems.push(`matrix.${name} is missing`)
  }
  if (problems.some((p) => p.endsWith('is missing'))) return problems
  if (matrix.schema !== SCHEMA_VERSION) problems.push(`matrix.schema is not ${SCHEMA_VERSION}`)
  for (const name of MATRIX_SECTIONS.slice(1, -1)) {
    if (!isObject(matrix[name])) problems.push(`matrix.${name} is not an object`)
  }
  if (!Array.isArray(matrix.rows)) problems.push('matrix.rows is not a list')
  if (problems.length > 0) return problems

  const { acceptance, clientKinds, sampling, schedule } = matrix
  problems.push(
    ...fieldProblems(acceptance, ACCEPTANCE_FIELDS, 'matrix.acceptance'),
    ...nested(acceptance, 'modes', each(MODES, Any), 'matrix.acceptance'),
    ...MODES.flatMap((mode) =>
      nested(acceptance.modes, mode, MODE_FIELDS, 'matrix.acceptance.modes')
    ),
    ...fieldProblems(matrix.host, HOST_FIELDS, 'matrix.host'),
    ...fieldProblems(matrix.traffic, TRAFFIC_FIELDS, 'matrix.traffic'),
    ...nested(matrix.traffic, 'floors', each(FLOORS, Setting), 'matrix.traffic'),
    ...unless(
      sameList(Object.keys(clientKinds), CLIENT_KINDS),
      'matrix.clientKinds is not the twelve kinds in order'
    ),
    ...CLIENT_KINDS.flatMap((k) =>
      own(clientKinds, k)
        ? fieldProblems(clientKinds[k], KIND_FIELDS, `matrix.clientKinds.${k}`)
        : []
    ),
    ...topologyProblems(matrix.topology, clientKinds),
    ...scheduleProblems(schedule),
    ...fieldProblems(sampling, SAMPLING_FIELDS, 'matrix.sampling'),
    ...controlProblems(matrix.controls, clientKinds),
    ...unless(
      sameList(matrix.recordSections.process, SECTION_SETS.process) &&
        sameList(matrix.recordSections.ordinary, SECTION_SETS.ordinary) &&
        sameList(matrix.recordSections.share, SECTION_SETS.share) &&
        Object.keys(matrix.recordSections).length === 3,
      'matrix.recordSections is not the process, ordinary and share section lists'
    ),
    ...unless(
      sameList(Object.keys(matrix.observations), RECORD_KINDS) &&
        Object.values(matrix.observations).every(Text.test),
      'matrix.observations does not describe exactly the record kinds'
    ),
    ...Object.keys(matrix.bounds).flatMap((k) =>
      unless(Bound.test(matrix.bounds[k]), `matrix.bounds.${nameOf(k)} is not a bound`)
    ),
    ...predicateProblems(matrix.predicates)
  )
  if (problems.length === 0) {
    const period = sampling.periodMs.value
    if (acceptance.modes.acceptance.requestedMs.value !== 86400000)
      problems.push('matrix.acceptance.modes.acceptance.requestedMs is not one day')
    if (sampling.periodicSlots.value !== acceptance.modes.acceptance.requestedMs.value / period + 1)
      problems.push('matrix.sampling.periodicSlots does not cover the day at the sample period')
    if (schedule.smoke.periodicSlots !== acceptance.modes.smoke.requestedMs.value / period + 1)
      problems.push(
        'matrix.schedule.smoke.periodicSlots does not cover the smoke at the sample period'
      )
    if (sampling.gcCollections.value !== 2) problems.push('matrix.sampling.gcCollections is not 2')
    if (
      (schedule.smoke.abortAtTransaction - 1) * matrix.traffic.transactionEveryMs.value >=
      schedule.smoke.outageAtMs
    )
      problems.push(
        'matrix.schedule.smoke.abortAtTransaction does not abort before the first outage'
      )
  }
  if (matrix.rows.length !== ROWS.length)
    problems.push(`matrix.rows does not hold the ${ROWS.length} rows`)
  matrix.rows
    .slice(0, ROWS.length)
    .forEach((row, i) => problems.push(...rowProblems(row, i, matrix)))
  return problems
}

/** @param {object} topology @param {object} kinds @returns {string[]} */
const topologyProblems = (topology, kinds) => {
  const names = Object.keys(kinds)
  const counts = topology.processes
  const problems = [
    ...unless(
      topology.replicationFactor === 3 && topology.minInsyncReplicas === 2,
      'matrix.topology needs replication factor 3 and min.insync.replicas 2'
    ),
    ...unless(
      isObject(counts) &&
        sameList(Object.keys(counts), PROCESS_ROLES) &&
        counts.subject === CLIENT_KINDS.length &&
        counts.seed === 1 &&
        counts.reader === 1 &&
        counts.collector === 1 &&
        counts.decoder === 1,
      'matrix.topology.processes is not 12 subjects, one seed and three observers'
    ),
    ...unless(Text.test(topology.routeRule), 'matrix.topology.routeRule is missing'),
  ]
  if (!Array.isArray(topology.topics) || topology.topics.length === 0) {
    return [...problems, 'matrix.topology.topics is not a list']
  }
  const roles = new Set()
  topology.topics.forEach((t, i) => {
    const path = `matrix.topology.topics[${i}]`
    const fields = {
      role: oneOf('main', 'transactions', 'regex', 'recreate'),
      partitions: type('a positive count', (v) => Count.test(v) && v > 0),
      recreated: Bool,
      producers: type('client kinds', (v) => subsetOf(v, names)),
      recipients: type('client kinds', (v) => subsetOf(v, names)),
      seed: Bool,
    }
    const found = fieldProblems(t, fields, path)
    problems.push(...found)
    if (found.length === 0) {
      if (roles.has(t.role)) problems.push(`${path}.role repeats`)
      roles.add(t.role)
    }
  })
  if (roles.size !== 4)
    problems.push('matrix.topology.topics does not cover main, transactions, regex and recreate')
  return problems
}

/**
 * Checks the single isolated seed control, including its fixed per-mode timing: the day starts the
 * map after the first quiet triplet and requires sensitivity; the smoke starts it after the
 * baseline before traffic and only reports growth.
 * @param {object} controls
 * @param {object} kinds
 * @returns {string[]}
 */
const controlProblems = (controls, kinds) => {
  if (!sameList(Object.keys(controls), [CONTROL]))
    return [`matrix.controls is not exactly ${CONTROL}`]
  const path = `matrix.controls.${CONTROL}`
  const control = controls[CONTROL]
  const fields = {
    arm: oneOf(CONTROL),
    clientKind: type(
      'an ordinary consumer kind',
      (v) => own(kinds, v) && isObject(kinds[v]) && kinds[v].inventory === 'ordinary'
    ),
    isolated: oneOf(true),
    retains: Text,
    pattern: Source,
    timing: Any,
    breachBytes: Setting,
    exemption: Text,
  }
  const exactly = (rule) => Object.fromEntries(Object.entries(rule).map(([k, v]) => [k, oneOf(v)]))
  return [
    ...fieldProblems(control, fields, path),
    ...nested(control, 'timing', each(MODES, Any), path),
    ...(own(control, 'timing')
      ? MODES.flatMap((m) => nested(control.timing, m, exactly(SEED_TIMING[m]), `${path}.timing`))
      : []),
  ]
}

/** @param {unknown} v @returns {boolean} a relative path that cannot escape its root */
function isRelativePath(v) {
  return (
    Id.test(v) &&
    !v.startsWith('/') &&
    !v.includes('\\') &&
    v.split('/').every((part) => part !== '' && part !== '.' && part !== '..')
  )
}

const ProcessSlot = shape('a process slot', {
  slot: type('a process slot id', (v) => isString(v) && /^p\d{2}$/.test(v)),
  role: oneOf(...PROCESS_ROLES),
  arm: ARM,
  controlId: CONTROL_ID,
})
const ClientSlot = shape('a client slot', {
  slot: type('a client slot id', (v) => isString(v) && /^c\d{2}$/.test(v)),
  process: Id,
  clientKind: oneOf(...CLIENT_KINDS),
  purpose: oneOf('subject', 'churn', 'observer', 'control'),
  arm: ARM,
  controlId: CONTROL_ID,
  group: nullable(Id),
  groupInstanceId: nullable(Id),
  standing: Bool,
  telemetryEnabled: Bool,
})

/**
 * Checks the run config's binding of every process and client slot: 16 persistent processes with
 * their arms; one standing subject of each kind alone in its own process (churn extras of the same
 * churn kind may share it); exactly one isolated seed process holding exactly one control client of
 * the seeded kind; observers only in observer processes; group, static id and telemetry per kind.
 * Other config sections belong to the driver profile and are compared by the verifier.
 * @param {unknown} config parsed config JSON
 * @param {object} matrix a valid matrix
 * @returns {string[]}
 */
const validateConfig = (config, matrix) => {
  if (!isObject(config)) return ['config is not an object']
  const runIdOk = isString(config.runId) && /^[0-9a-f]{12}$/.test(config.runId)
  const problems = [
    ...unless(config.schema === SCHEMA_VERSION, `config.schema is not ${SCHEMA_VERSION}`),
    ...unless(MODES.includes(config.mode), 'config.mode is not smoke or acceptance'),
    ...unless(runIdOk, 'config.runId is not 12 hex digits'),
  ]
  if (!Array.isArray(config.processes) || !config.processes.every(ProcessSlot.test)) {
    return [...problems, 'config.processes is not a list of process slots']
  }
  if (!Array.isArray(config.clients) || !config.clients.every(ClientSlot.test)) {
    return [...problems, 'config.clients is not a list of client slots']
  }
  const processes = new Map(config.processes.map((p) => [p.slot, p]))
  if (processes.size !== config.processes.length) problems.push('config.processes repeats a slot')
  for (const role of PROCESS_ROLES) {
    const n = config.processes.filter((p) => p.role === role).length
    if (n !== matrix.topology.processes[role])
      problems.push(`config.processes has ${n} ${role} processes`)
  }
  for (const p of config.processes) {
    const [arm, controlId] = ROLE_ARMS[p.role]
    if (p.arm !== arm || p.controlId !== controlId)
      problems.push(`config process ${p.slot} has the wrong arm for a ${p.role}`)
  }
  const slots = new Set(config.clients.map((c) => c.slot))
  if (slots.size !== config.clients.length) problems.push('config.clients repeats a slot')
  const subjectOf = new Map()
  const statics = new Set()
  for (const c of config.clients) {
    const where = `config client ${c.slot}`
    const proc = processes.get(c.process)
    if (!proc) {
      problems.push(`${where} names no config process`)
      continue
    }
    const kind = matrix.clientKinds[c.clientKind]
    if (c.arm !== proc.arm || c.controlId !== proc.controlId)
      problems.push(`${where} arm differs from its process`)
    const placed = {
      subject: proc.role === 'subject' && c.standing,
      churn: proc.role === 'subject' && kind.churn && !c.standing,
      control:
        proc.role === 'seed' && c.clientKind === matrix.controls[CONTROL].clientKind && c.standing,
      observer: !['subject', 'seed'].includes(proc.role),
    }[c.purpose]
    if (!placed) problems.push(`${where} is a ${c.purpose} client in a ${proc.role} process`)
    if (c.purpose === 'subject') {
      if (subjectOf.has(c.clientKind)) problems.push(`config has two ${c.clientKind} subjects`)
      subjectOf.set(c.clientKind, c)
    }
    if (c.purpose !== 'observer') {
      if (c.telemetryEnabled !== (kind.telemetry && c.purpose !== 'control'))
        problems.push(`${where} telemetry does not match its kind`)
      if (sameNullness(c.groupInstanceId, kind.static ? null : 1))
        problems.push(`${where} static identity does not match its kind`)
      if (!kind.grouped && c.group !== null)
        problems.push(`${where} has a group its kind does not use`)
    }
    if (c.groupInstanceId !== null) {
      if (statics.has(c.groupInstanceId)) problems.push(`${where} repeats a static identity`)
      statics.add(c.groupInstanceId)
    }
  }
  for (const kind of CLIENT_KINDS) {
    if (!subjectOf.has(kind)) problems.push(`config has no ${kind} subject`)
  }
  for (const p of config.processes) {
    const hosted = config.clients.filter((c) => c.process === p.slot)
    const subjects = hosted.filter((c) => c.purpose === 'subject')
    if (
      p.role === 'subject' &&
      (subjects.length !== 1 || hosted.some((c) => c.clientKind !== subjects[0].clientKind))
    ) {
      problems.push(`config process ${p.slot} does not host exactly one subject kind`)
    }
    if (p.role === 'seed' && (hosted.length !== 1 || hosted[0].purpose !== 'control')) {
      problems.push(`config seed process ${p.slot} does not host exactly one control client`)
    }
  }
  for (const c of config.clients) {
    if (c.purpose === 'observer' || !matrix.clientKinds[c.clientKind].grouped) continue
    const owner = c.purpose === 'churn' ? subjectOf.get(c.clientKind) : c
    if (runIdOk && owner && c.group !== `g-${config.runId}-${owner.slot}`)
      problems.push(`config client ${c.slot} group is not its kind's run-scoped group`)
  }
  return problems
}

const Manifest = shape('a manifest entry', {
  file: type('a relative path', isRelativePath),
  bytes: Count,
  sha256: Sha,
})
const Companion = shape('a companion', {
  name: oneOf(...COMPANIONS),
  treeSha: CommitSha,
  file: type('a relative path', isRelativePath),
  sha256: Sha,
  runUrl: nullable(Id),
})
const HEADER_FIELDS = Object.freeze({
  schema: oneOf(SCHEMA_VERSION),
  mode: oneOf(...MODES),
  runId: type('a run id', (v) => isString(v) && /^[0-9a-f]{12}$/.test(v)),
  treeSha: CommitSha,
  node: Id,
  v8: Id,
  platform: oneOf('linux'),
  files: listOf(Manifest),
  clock: shape('a clock', { kind: oneOf('linux-monotonic'), originMs: Ms, bootId: Id }),
  window: shape('a window', {
    runStartAt: Ms,
    runEndAt: Ms,
    observedEndAt: Ms,
    requestedMs: Count,
  }),
  host: shape('a host census', {
    cpus: Count,
    totalMemoryBytes: Count,
    availableMemoryBytes: Count,
    freeDiskBytes: Count,
    loadAverage1m: Ms,
    docker: Id,
    measuredAt: Ms,
    pausedWork: Bool,
  }),
  broker: shape('a broker provenance', {
    provenanceKind: oneOf('image', 'supplied'),
    provenanceFile: type('a relative path', isRelativePath),
    capabilitiesFile: type('a relative path', isRelativePath),
    composeFile: type('a relative path', isRelativePath),
  }),
  exit: shape('a parent-observed exit', {
    supervisorProcess: Id,
    code: nullable(Int),
    signal: nullable(Id),
    observedAt: Ms,
  }),
  stream: shape('a stream digest', {
    file: type('a relative path', isRelativePath),
    bytes: Count,
    records: Count,
    sha256: Sha,
  }),
  companions: listOf(Companion),
})

/**
 * Checks the header: exact typed fields, a manifest of unique relative paths that binds the config,
 * the stream, every archived repository input, the broker files and each companion, and the mode's
 * window: requested length equal to the mode's, the run window equal to it, and the last child and
 * supervisor exits observed after the run end within the mode's teardown allowance. A relabelled
 * smoke therefore cannot pass as acceptance.
 * @param {unknown} header parsed header JSON
 * @param {object} matrix a valid matrix
 * @returns {string[]}
 */
const validateHeader = (header, matrix) => {
  const problems = fieldProblems(header, HEADER_FIELDS, 'header')
  if (problems.length > 0) return problems
  const files = new Map(header.files.map((f) => [f.file, f]))
  if (files.size !== header.files.length) problems.push('header.files repeats a path')
  const bound = (file, what) => unless(files.has(file), `header.files does not bind ${what}`)
  problems.push(
    ...bound('config.json', 'config.json'),
    ...bound(header.stream.file, 'the stream'),
    ...matrix.acceptance.archive.flatMap((f) => bound(`archive/${f}`, `archived ${f}`)),
    ...bound(header.broker.provenanceFile, 'the broker provenance'),
    ...bound(header.broker.capabilitiesFile, 'the broker capabilities'),
    ...bound(header.broker.composeFile, 'the compose file')
  )
  const stream = files.get(header.stream.file)
  if (stream && (stream.sha256 !== header.stream.sha256 || stream.bytes !== header.stream.bytes)) {
    problems.push('header.stream differs from its manifest entry')
  }
  const names = new Set()
  for (const c of header.companions) {
    if (names.has(c.name)) problems.push(`header.companions repeats ${c.name}`)
    names.add(c.name)
    if (files.get(c.file)?.sha256 !== c.sha256)
      problems.push(`header companion ${c.name} is not bound by the manifest`)
  }
  const mode = matrix.acceptance.modes[header.mode]
  const w = header.window
  const teardown = mode.teardownAllowanceMs.value
  problems.push(
    ...unless(
      w.requestedMs === mode.requestedMs.value,
      `header.window.requestedMs is not the ${header.mode} length`
    ),
    ...unless(
      w.runEndAt - w.runStartAt === w.requestedMs,
      'header.window run length differs from requestedMs'
    ),
    ...unless(
      w.runEndAt <= w.observedEndAt &&
        w.observedEndAt <= header.exit.observedAt &&
        header.exit.observedAt <= w.runEndAt + teardown,
      'header exits are not observed after the run end within the teardown allowance'
    )
  )
  return problems
}

const NodeId = { node: Count }
const CAPABILITIES = {
  nodes: listOf(
    shape('a broker node', {
      ...NodeId,
      apiRanges: listOf(
        shape('an api range', { apiKey: Count, minVersion: Count, maxVersion: Count })
      ),
      supportedFeatures: listOf(
        shape('a feature', { name: Id, minVersion: Count, maxVersion: Count })
      ),
      finalizedFeatures: listOf(
        shape('a finalized feature', { name: Id, minVersionLevel: Count, maxVersionLevel: Count })
      ),
    })
  ),
}
const PROVENANCE = {
  image: {
    nodes: listOf(
      shape('an image node', {
        ...NodeId,
        image: Id,
        digest: type('an image digest', (v) => isString(v) && /^sha256:[0-9a-f]{64}$/.test(v)),
        container: Id,
        version: Id,
        inspectFile: type('a relative path', isRelativePath),
        inspectSha256: Sha,
      })
    ),
  },
  supplied: {
    nodes: listOf(
      shape('a supplied node', {
        ...NodeId,
        version: Id,
        evidenceFile: type('a relative path', isRelativePath),
        evidenceSha256: Sha,
      })
    ),
  },
}

/** @param {unknown[]} list @param {(v: any) => string} keyOf @returns {boolean} */
const unique = (list, keyOf) => new Set(list.map(keyOf)).size === list.length

/**
 * Checks the broker capabilities file: non-empty unique nodes, each with non-empty unique API
 * ranges and unique feature names, every high at or above its low. Wire capabilities are not
 * release provenance; see `validateProvenance`.
 * @param {unknown} json
 * @returns {string[]}
 */
const validateCapabilities = (json) => {
  const problems = fieldProblems(json, CAPABILITIES, 'capabilities')
  if (problems.length > 0) return problems
  if (json.nodes.length === 0 || !unique(json.nodes, (n) => n.node))
    return ['capabilities.nodes is empty or repeats a node']
  for (const n of json.nodes) {
    const ok =
      n.apiRanges.length > 0 &&
      unique(n.apiRanges, (r) => r.apiKey) &&
      unique(n.supportedFeatures, (f) => f.name) &&
      unique(n.finalizedFeatures, (f) => f.name) &&
      n.apiRanges.every((r) => r.maxVersion >= r.minVersion) &&
      n.supportedFeatures.every((f) => f.maxVersion >= f.minVersion) &&
      n.finalizedFeatures.every((f) => f.maxVersionLevel >= f.minVersionLevel)
    if (!ok) problems.push(`capabilities node ${n.node} has empty, repeated or inverted ranges`)
  }
  return problems
}

/**
 * Checks the broker provenance file of the header's kind: per node the image, digest, container,
 * version and archived inspect bytes, or for a supplied cluster the hashed build evidence.
 * @param {unknown} json
 * @param {unknown} kind `image` or `supplied`
 * @returns {string[]}
 */
const validateProvenance = (json, kind) => {
  if (!own(PROVENANCE, kind)) return ['provenance kind is not image or supplied']
  const problems = fieldProblems(json, PROVENANCE[kind], 'provenance')
  if (problems.length > 0) return problems
  return unless(
    json.nodes.length > 0 && unique(json.nodes, (n) => n.node),
    'provenance.nodes is empty or repeats a node'
  )
}

/**
 * Validates a record stream line by line with bounded state: per-source counters, live processes
 * and clients, open executions and inventory cuts, and the stream digest. It never holds the
 * stream. Checks: config and header agree on run id and mode; exact record types; `recordId` = `<source>.<seq>` with gap-free per-source
 * `seq` from 0; `at <= t`, `t` non-decreasing, per-source `at` non-decreasing, `t` within the
 * observed window; processes on the header's boot and Node; no record of a process later than its
 * exit; clients constructed in a started process as their config slot says, never reusing an id;
 * seed records only from the isolated seed's control client; telemetry only from telemetry-enabled
 * clients; executions opened once and closed with their operation count; every `wave-key` and
 * `partition-state` record its `wave` or `broker-state` counts; inventory cuts complete in every
 * chunk and required section before the sample that cites them; gc witnesses with two new forced
 * collections per reading.
 *
 * The merged stream keeps each source's order, not the order between sources. Facts about another
 * process are therefore judged independently of stream position: a process's exit is compared with
 * the event times of its records, and a record naming a client whose construction (by another
 * process) has not arrived yet is held, bounded, and judged when it arrives.
 *
 * Every kind of live state is capped at `maxOpen` entries; a stream that needs more fails once per
 * kind and the excess is not tracked (its later references then fail too), and the problems found
 * at the end of the stream count against the same problem cap as the others.
 */
class StreamValidator {
  /**
   * @param {object} options
   * @param {object} options.matrix a valid matrix
   * @param {object} options.config a valid config
   * @param {object} options.header a valid header
   * @param {number} [options.maxHeld] records held at once for clients not yet constructed
   * @param {number} [options.maxOpen] entries kept of any one kind of live state
   */
  constructor({ matrix, config, header, maxHeld = MAX_HELD, maxOpen = MAX_OPEN }) {
    this.matrix = matrix
    this.header = header
    this.slots = new Map(config.clients.map((c) => [c.slot, c]))
    this.allowedSources = new Set([...config.processes.map((p) => p.slot), OUTER])
    this.seedProcess = config.processes.find((p) => p.role === 'seed').slot
    this.processes = new Map()
    this.sources = new Map()
    this.clients = new Map()
    this.exits = new Map()
    this.held = new Map()
    this.heldCount = 0
    this.maxHeld = maxHeld
    this.maxOpen = maxOpen
    this.full = new Set()
    this.openItems = 0
    this.summaries = new Map()
    this.executions = new Map()
    this.closedExecutions = new Set()
    this.cuts = new Map()
    this.gc = new Map()
    this.hash = crypto.createHash('sha256')
    this.bytes = 0
    this.lines = 0
    this.lastT = 0
    this.problems = []
    this.omitted = 0
    if (config.runId !== header.runId || config.mode !== header.mode)
      this.problems.push('config and header disagree on run id or mode')
  }

  /**
   * @param {number} size how many entries of one kind of state are kept
   * @param {string} what the kind, as the problem names it
   * @returns {boolean} whether one more fits; the first refusal of a kind fails the stream
   */
  room(size, what) {
    if (size < this.maxOpen) return true
    if (!this.full.has(what)) this.fail(`more than ${this.maxOpen} ${what}`)
    this.full.add(what)
    return false
  }

  /** @param {string} problem @param {number} [line] the line it is about, the current by default */
  fail(problem, line = this.lines) {
    if (this.problems.length < MAX_PROBLEMS) this.problems.push(`line ${line}: ${problem}`)
    else this.omitted++
  }

  /**
   * Adds one physical line, without its newline.
   * @param {unknown} line
   */
  push(line) {
    this.lines++
    if (!isString(line)) return this.fail('is not a string')
    const bytes = Buffer.byteLength(line)
    this.hash.update(line)
    this.hash.update('\n')
    this.bytes += bytes + 1
    if (line.length === 0) return this.fail('is blank')
    let record
    try {
      record = JSON.parse(line)
    } catch {
      return this.fail('is not JSON')
    }
    const limit =
      isObject(record) && record.kind === 'inventory'
        ? 'maxInventoryLineBytes'
        : 'maxEventLineBytes'
    if (bytes > this.matrix.host[limit].value)
      this.fail(`exceeds ${this.matrix.host[limit].value} bytes`)
    const problems = validateRecord(record)
    if (problems.length > 0) return problems.forEach((p) => this.fail(p))
    this.envelope(record)
    this.references(record)
  }

  /** @param {object} r a typed record */
  envelope(r) {
    if (!this.allowedSources.has(r.source))
      return this.fail('source is not a config process or the outer observer')
    const last = this.sources.get(r.source) ?? { seq: -1, at: 0 }
    if (r.seq !== last.seq + 1) this.fail('seq is not the next of its source')
    if (r.recordId !== `${r.source}.${r.seq}`) this.fail('recordId is not <source>.<seq>')
    if (r.at > r.t) this.fail('at is after t')
    if (r.t < this.lastT) this.fail('t decreases')
    if (r.at < last.at) this.fail('at decreases within its source')
    if (r.t > this.header.window.observedEndAt) this.fail('t is after the observed end')
    if (r.at > (this.exits.get(r.source) ?? Infinity)) this.fail('its source process has exited')
    this.sources.set(r.source, { seq: r.seq, at: Math.max(r.at, last.at) })
    this.lastT = Math.max(this.lastT, r.t)
  }

  /**
   * The config slot of a constructed client. A record naming one not constructed yet is held,
   * reduced to what `slotChecks` reads, and judged when the construction arrives; beyond
   * `maxHeld` held records it is not held and the stream fails once.
   * @param {string} client @param {object} r the record naming it
   * @returns {object|undefined} the slot, undefined while the client is not constructed
   */
  client(client, r) {
    const slot = this.clients.get(client)
    if (slot) return slot
    if (this.heldCount >= this.maxHeld) {
      if (!this.heldFull)
        this.fail(`more than ${this.maxHeld} records wait for a client not yet constructed`)
      this.heldFull = true
      return undefined
    }
    const { kind, source, process, acquisition, group } = r
    const fact = { line: this.lines, kind, source, process, acquisition, group }
    fact.variant = RECORDS[kind].by ? r[RECORDS[kind].by] : null
    this.heldCount++
    if (this.held.has(client)) this.held.get(client).push(fact)
    else this.held.set(client, [fact])
    return undefined
  }

  /**
   * The checks of a record that need its client's config slot.
   * @param {{ kind: string, variant: ?string, process?: string, acquisition?: ?string,
   *   group?: string }} r the record, or the fact held of it
   * @param {object} slot @param {number} [line] the record's line
   */
  slotChecks(r, slot, line = this.lines) {
    if (r.kind === 'telemetry' && !slot.telemetryEnabled)
      this.fail('telemetry from a client without telemetry', line)
    if (
      r.kind === 'seed' &&
      (r.process !== this.seedProcess || slot.purpose !== 'control' || slot.process !== r.process)
    ) {
      this.fail('seed record is not from the isolated seed process and its control client', line)
    }
    if (r.kind === 'sample' && slot.process !== r.process)
      this.fail('sample lists a client of another process', line)
    if (r.kind === 'identity' && r.variant === 'handler-enter') {
      const share = this.matrix.clientKinds[slot.clientKind].role === 'share'
      if (share === (r.acquisition === null))
        this.fail('handler authority does not match the client kind', line)
      if (r.group !== slot.group) this.fail('handler group is not its client group', line)
    }
  }

  /**
   * @param {string} process @param {number} at an event time
   * @returns {boolean} whether the process had started and not yet exited at `at`
   */
  liveAt(process, at) {
    return this.processes.has(process) && !(at > (this.exits.get(process) ?? Infinity))
  }

  /** @param {object} r a typed, chronologically checked record */
  references(r) {
    const variant = RECORDS[r.kind].by ? r[RECORDS[r.kind].by] : null
    if (r.kind === 'process')
      return variant === 'start' ? this.processStart(r) : this.processExit(r)
    if (r.kind === 'client' && variant === 'construct') return this.construct(r)
    if (r.kind === 'shape') return this.shape(r, variant)
    if (r.kind === 'sample') return this.sample(r)
    if (r.kind === 'inventory') return this.inventory(r)
    if (own(ITEM_KINDS, r.kind)) return this.item(r)
    const left = r.kind === 'wave' ? r.paused : r.kind === 'broker-state' ? r.partitions : 0
    if (left > 0 && this.room(this.summaries.size, 'open wave or broker-state records')) {
      if (this.room(this.openItems + left - 1, 'open wave-key or partition-state items')) {
        this.openItems += left
        this.summaries.set(r.recordId, {
          kind: r.kind,
          source: r.source,
          execution: r.execution,
          left,
          keys: new Set(),
          held: r.kind === 'wave' ? r.held : null,
          line: this.lines,
        })
      }
    }
    if (r.kind === 'wave') this.client(r.member, r)
    if (r.kind === 'identity' && variant === 'log') this.client(r.reader, r)
    if (r.kind === 'operation' && r.execution !== null) {
      const open = this.executions.get(r.execution)
      if (!open) this.fail('operation names no open execution')
      else if (variant === 'end') open.operations++
    }
    const slot = isString(r.client) ? this.client(r.client, r) : undefined
    if (slot) this.slotChecks({ ...r, variant }, slot)
  }

  /**
   * One of a counting record's items: it must cite an open one of its source and execution and
   * name a key no other item of it named. When the last item arrives, a wave's `held` count must
   * equal its items held.
   * @param {object} r a `wave-key` or `partition-state` record
   */
  item(r) {
    const kind = ITEM_KINDS[r.kind]
    const open = this.summaries.get(r.cause)
    if (!open || open.kind !== kind || open.source !== r.source)
      return this.fail(`cites no open ${kind} of its source`)
    if (open.execution !== r.execution) return this.fail(`is of another execution than its ${kind}`)
    const key = JSON.stringify([r.key.topicId, r.key.partition])
    if (open.keys.has(key)) return this.fail(`repeats a key of its ${kind}`)
    open.keys.add(key)
    if (r.kind === 'wave-key' && r.held) open.held--
    this.openItems--
    if (--open.left > 0) return
    this.summaries.delete(r.cause)
    if (open.held !== null && open.held !== 0)
      this.fail(`${kind} held count differs from its ${r.kind} records`)
  }

  /** @param {object} r */
  processStart(r) {
    if (r.source !== r.process) this.fail('a process start comes from the process itself')
    if (!this.allowedSources.has(r.process) || r.process === OUTER)
      return this.fail('process is not a config process')
    if (this.processes.has(r.process)) this.fail('process starts twice')
    if (r.bootId !== this.header.clock.bootId || r.node !== this.header.node) {
      this.fail('process bootId or node differs from the header')
    }
    this.processes.set(r.process, 'live')
  }

  /** @param {object} r */
  processExit(r) {
    if (this.processes.get(r.process) !== 'live') this.fail('exit of a process that is not live')
    if (r.source === r.process) this.fail('a process cannot observe its own exit')
    if ((this.sources.get(r.process)?.at ?? 0) > r.at)
      this.fail('the process has a record after its exit')
    this.processes.set(r.process, 'exited')
    this.exits.set(r.process, r.at)
  }

  /** @param {object} r */
  construct(r) {
    const slot = this.slots.get(r.slot)
    if (!slot) return this.fail('client slot is not in the config')
    if (!new RegExp(`^${r.slot}\\.(0|[1-9][0-9]*)$`).test(r.client))
      this.fail('client id is not <slot>.<incarnation>')
    if (this.clients.has(r.client)) this.fail('client id is reused')
    else if (!this.room(this.clients.size, 'constructed clients')) return
    if (!this.liveAt(r.process, r.at)) this.fail('client is constructed outside a live process')
    for (const field of [
      'clientKind',
      'process',
      'arm',
      'controlId',
      'purpose',
      'group',
      'groupInstanceId',
      'standing',
      'telemetryEnabled',
    ]) {
      if (r[field] !== slot[field]) this.fail(`client ${field} differs from its config slot`)
    }
    this.clients.set(r.client, slot)
    for (const fact of this.held.get(r.client) ?? []) {
      if (fact.source === slot.process)
        this.fail('names a client before its construction', fact.line)
      else this.slotChecks(fact, slot, fact.line)
      this.heldCount--
    }
    this.held.delete(r.client)
  }

  /** @param {object} r @param {string} phase */
  shape(r, phase) {
    if (phase === 'start') {
      if (this.executions.has(r.execution) || this.closedExecutions.has(r.execution))
        return this.fail('execution id is reused')
      if (!this.room(this.executions.size, 'open executions')) return
      return this.executions.set(r.execution, { shape: r.shape, operations: 0 })
    }
    const open = this.executions.get(r.execution)
    if (!open || open.shape !== r.shape) return this.fail('shape end has no matching start')
    if (open.operations !== r.operationCount)
      this.fail('operationCount differs from the ended operations')
    this.executions.delete(r.execution)
    if (this.room(this.closedExecutions.size, 'closed executions'))
      this.closedExecutions.add(r.execution)
  }

  /** @param {object} r */
  inventory(r) {
    if (!this.liveAt(r.process, r.at)) this.fail('inventory of a process that is not live')
    const slot = r.client === null ? null : this.client(r.client, r)
    const sections =
      r.client === null
        ? SECTION_SETS.process
        : slot
          ? SECTION_SETS[this.matrix.clientKinds[slot.clientKind].inventory]
          : []
    if (slot !== undefined && !sections.includes(r.section))
      this.fail('section does not apply to this owner')
    let cut = this.cuts.get(r.inventory)
    if (!cut) {
      if (!this.room(this.cuts.size, 'open inventory cuts')) return
      cut = { process: r.process, at: r.at, groups: new Map() }
      this.cuts.set(r.inventory, cut)
    }
    if (cut.process !== r.process || cut.at !== r.at)
      this.fail('chunks of one cut differ in process or at')
    const name = `${r.client}\0${r.section}`
    let group = cut.groups.get(name)
    if (!group) {
      group = { chunks: r.chunks, seen: new Set(), keys: new Set() }
      cut.groups.set(name, group)
    }
    if (group.chunks !== r.chunks) this.fail('chunks of one section disagree on their count')
    if (group.seen.has(r.chunk)) this.fail('chunk repeats')
    group.seen.add(r.chunk)
    for (const entry of r.entries) {
      const key = SECTIONS[r.section].key(entry)
      if (group.keys.has(key)) this.fail(`${r.section} entry key repeats`)
      group.keys.add(key)
    }
  }

  /** @param {object} r */
  sample(r) {
    if (!this.liveAt(r.process, r.at)) this.fail('sample of a process that is not live')
    for (const client of r.clients) {
      const slot = this.client(client, r)
      if (slot) this.slotChecks(r, slot)
    }
    const cut = this.cuts.get(r.inventory)
    if (!cut || cut.process !== r.process) this.fail('sample cites no inventory cut of its process')
    else {
      const owners = [null, ...r.clients]
      for (const owner of owners) {
        const slot = owner === null ? null : this.clients.get(owner)
        if (slot === undefined) continue
        const sections =
          owner === null
            ? SECTION_SETS.process
            : SECTION_SETS[this.matrix.clientKinds[slot.clientKind].inventory]
        for (const section of sections) {
          const group = cut.groups.get(`${owner}\0${section}`)
          if (!group) this.fail(`inventory cut omits ${section}`)
          else if (group.seen.size !== group.chunks)
            this.fail(`inventory cut misses a ${section} chunk`)
        }
      }
      this.cuts.delete(r.inventory)
    }
    if (r.gc === null) {
      if (this.header.mode === 'acceptance') this.fail('an acceptance sample has no gc witness')
      return
    }
    const last = this.gc.get(r.process)
    if (last && r.gc.cumulativeForced !== last.cumulative + 2)
      this.fail('cumulativeForced does not rise by two new collections')
    if (!last && r.gc.cumulativeForced < 2)
      this.fail('cumulativeForced does not count this reading')
    if (last && r.gc.firstBeginAt <= last.endAt)
      this.fail('gc interval overlaps the previous reading')
    this.gc.set(r.process, { cumulative: r.gc.cumulativeForced, endAt: r.gc.secondEndAt })
  }

  /**
   * Ends the stream and compares it with the header's digest, byte and record counts.
   * @returns {string[]} every problem found, capped with a count of those omitted
   */
  finish() {
    for (const facts of this.held.values())
      for (const fact of facts) this.fail('names a client that was never constructed', fact.line)
    this.held.clear()
    for (const [recordId, open] of this.summaries) {
      const item = Object.keys(ITEM_KINDS).find((k) => ITEM_KINDS[k] === open.kind)
      this.fail(`${nameOf(recordId)} misses ${open.left} of its ${item} records`, open.line)
    }
    for (const [execution] of this.executions)
      this.fail(`execution ${nameOf(execution)} never ends`)
    for (const [inventory, cut] of this.cuts)
      if ([...cut.groups.values()].some((group) => group.seen.size !== group.chunks))
        this.fail(`inventory ${nameOf(inventory)} misses a chunk`)
    const problems = [...this.problems]
    if (this.omitted > 0) problems.push(`${this.omitted} more problems omitted`)
    const stream = this.header.stream
    if (stream.records !== this.lines)
      problems.push('the stream record count differs from the header')
    if (stream.bytes !== this.bytes) problems.push('the stream byte count differs from the header')
    if (stream.sha256 !== this.hash.digest('hex'))
      problems.push('the stream digest does not match the header')
    return problems
  }
}

module.exports = {
  SCHEMA_VERSION,
  ROWS,
  CLIENT_KINDS,
  SHAPES,
  RECORDS,
  RECORD_KINDS,
  SECTION_SETS,
  StreamValidator,
  validateCapabilities,
  validateConfig,
  validateHeader,
  validateMatrix,
  validateProvenance,
  validateRecord,
}
