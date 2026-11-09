const { createHash } = require('crypto')
const fs = require('fs')
const os = require('os')
const path = require('path')
const { JAVA_REQUESTED, javaSampleSummary } = require('../scripts/e2e/telemetry/semantics')

const root = path.resolve(__dirname, '..')
const digest = (bytes) => createHash('sha256').update(bytes).digest('hex')

/**
 * Builds the pinned Java four-resource metric payload used by the offline replay controls.
 * @param {boolean} [final=false] whether this is a terminating export
 * @param {boolean} [later=false] whether delta counters have already been reported
 * @returns {object} complete retained metric and resource trees
 */
const javaRow = (final = false, later = false) => ({
  clientId: 'java-producer',
  pushIntervalMs: 1000,
  parse: 'ok',
  truncated: false,
  terminating: final,
  tsMs: final ? 35 : later ? 20 : 10,
  resourceMetricsCount: 4,
  metricsTotal: 4,
  metricsKept: 4,
  pointsTotal: 4,
  resources: Array.from({ length: 4 }, () => ({ attrs: [], droppedAttributes: 0 })),
  metrics: [
    {
      name: 'org.apache.kafka.producer.connection.creation.total',
      kind: 'SUM',
      monotonic: true,
      temporality: 'AGGREGATION_TEMPORALITY_DELTA',
      points: [
        {
          attrs: [],
          valueKind: 'AS_DOUBLE',
          asDouble: later ? '0.0' : '1.0',
          startNs: '1',
          timeNs: '2',
        },
      ],
    },
    {
      name: 'org.apache.kafka.tf1.custom.gauge.double',
      kind: 'GAUGE',
      points: [
        {
          attrs: [],
          valueKind: 'AS_DOUBLE',
          asDouble: '42.5',
          asDoubleBits: '4045400000000000',
          timeNs: '2',
        },
      ],
    },
    {
      name: 'org.apache.kafka.tf1.custom.gauge.long',
      kind: 'GAUGE',
      points: [{ attrs: [], valueKind: 'AS_INT', asInt: '4242', timeNs: '2' }],
    },
    {
      name: 'org.apache.kafka.tf1.custom.sum.total',
      kind: 'SUM',
      monotonic: true,
      temporality: 'AGGREGATION_TEMPORALITY_DELTA',
      points: [
        {
          attrs: [],
          valueKind: 'AS_DOUBLE',
          asDouble: later ? '0.0' : '7.0',
          startNs: '1',
          timeNs: '2',
        },
      ],
    },
  ].map((m, res) => ({
    ...m,
    unit: '',
    res,
    pointsInMessage: 1,
    points: m.points.map((p) => ({ ...p, flags: 0 })),
  })),
})

/**
 * Builds all three Java roles, four JS Get confirmations and complete three-node sink files.
 * The enclosing scenario verifier separately owns the existing JS wire/decode joins.
 * @param {boolean} [final=false] whether an optional admin final is represented
 * @param {object} [options] archive identity and destination
 * @param {string} [options.stem=case] run stem used in all assigned client and resource IDs
 * @param {string} [options.directory] existing or new destination; otherwise owns a temporary directory
 * @returns {object} version-2 Java replay inputs and bounded cleanup
 * @throws {Error} if pinned fixture inputs or archive files cannot be read or written
 */
const createJavaInteropArchive = (final = false, options = {}) => {
  const { stem = 'case' } = options
  const directory = options.directory ?? fs.mkdtempSync(path.join(os.tmpdir(), 'java-checkpoint-'))
  fs.mkdirSync(directory, { recursive: true })
  const roles = ['producer', 'consumer', 'admin']
  const kinds = [...roles, 'share']
  const uuid = (n) => `00112233-4455-6677-8899-aabbccddee0${n}`
  const compact = (id) => Buffer.from(id.replaceAll('-', ''), 'hex').toString('base64url')
  const fixture = {
    clientSourceSha256: digest(
      fs.readFileSync(path.join(root, 'scripts/e2e/telemetry/JavaTelemetryClient.java'))
    ),
    runnerSha256: digest(fs.readFileSync(path.join(root, 'scripts/e2e/telemetry/run.sh'))),
  }
  const provenance = {
    clientVersion: '4.3.1',
    sourceSha256: fixture.clientSourceSha256,
    runnerSha256: fixture.runnerSha256,
    jdk: Object.fromEntries(roles.map((r) => [r, { jdk: '21-test', vendor: 'test' }])),
    cache: fs
      .readFileSync(path.join(root, 'scripts/e2e/telemetry/interop.sha256'), 'utf8')
      .trim()
      .split('\n')
      .map((line) => {
        const [sha256, name] = line.split(/\s+/)
        return { name, sha256 }
      }),
  }
  const results = {
    javaEvidenceVersion: 2,
    provenance,
    java: {},
    js: {},
    children: {},
    uuids: {
      java: Object.fromEntries(roles.map((r, i) => [r, uuid(i + 1)])),
      js: Object.fromEntries(kinds.map((r, i) => [r, uuid(i + 4)])),
    },
    controls: { producerRecords: 2, seedCommit: '1' },
    readback: {
      metrics: JAVA_REQUESTED,
      interval: 1000,
      ids: [...roles.map((r) => `${stem}-java-${r}`), ...kinds.map((r) => `${stem}-js-${r}`)],
    },
  }
  const rows = {}
  for (const [i, role] of roles.entries()) {
    const id = results.uuids.java[role]
    const list = [
      javaRow(),
      javaRow(false, true),
      ...(role !== 'admin' || final ? [javaRow(true, true)] : []),
    ]
    for (const [index, sample] of list.entries()) {
      Object.assign(sample, {
        v: 1,
        node: String(i + 1),
        seq: index + 1,
        dataBytes: 500,
        dataSha256: digest(`${role}-${index}`),
        clientId: `${stem}-java-${role}`,
        clientInstanceId: id,
        tsMs: [100, 250, role === 'admin' ? 320 : 305][index],
      })
      sample.metrics[0].name = `org.apache.kafka.${role === 'admin' ? 'admin.client' : role}.connection.creation.total`
      for (const metric of sample.metrics)
        metric.points[0].timeNs = String(BigInt([100, 250, 305][index]) * 1000000n)
      if (role === 'consumer')
        for (const resource of sample.resources)
          resource.attrs = [
            ['group_id', 'string', `${stem}-java-consumer-group`],
            ['group_member_id', 'string', 'member'],
          ]
    }
    rows[role] = list
    fs.writeFileSync(
      path.join(directory, `exports-${i + 1}.jsonl`),
      list.map((r) => JSON.stringify(r) + '\n').join('')
    )
    const markers = [
      { at: 10, fields: provenance.jdk[role] },
      { at: 50, fields: { role, clientInstanceId: compact(id), closed: false } },
      ...(role === 'consumer'
        ? [
            {
              at: 51,
              fields: {
                membership: true,
                fromEpochMs: 0,
                epochMs: 1,
                beforeMemberId: '',
                memberId: 'member',
              },
            },
          ]
        : []),
      ...(role === 'admin'
        ? [
            {
              at: 200,
              fields: {
                closeReady: true,
                role,
                clientId: `${stem}-java-${role}`,
                clientInstanceId: compact(id),
                checkpointId: 'q'.repeat(22),
                epochMs: 200,
                operation: {
                  api: 'listTopics',
                  success: true,
                  startedEpochMs: 190,
                  completedEpochMs: 195,
                },
              },
            },
            {
              at: 270,
              fields: {
                closeReleased: true,
                role,
                clientId: `${stem}-java-${role}`,
                clientInstanceId: compact(id),
                checkpointId: 'q'.repeat(22),
                epochMs: 270,
                node: '3',
                seq: 2,
                dataSha256: list[1].dataSha256,
              },
            },
          ]
        : []),
      { at: 300, fields: { role, closing: true, epochMs: 300, requestedCloseMs: 3000 } },
      { at: 310, fields: { role, closed: true, epochMs: 310, closeElapsedNs: '10000000' } },
    ].map(({ at, fields }) => ({ at, line: JSON.stringify(fields) }))
    const child = (results.children[role] = {
      code: 0,
      signal: null,
      exitAt: 320,
      overflow: false,
      pipesDrained: true,
      supervisorFailure: false,
      liveDescendants: [],
      markers,
      stdout: markers.map((m) => m.line + '\n').join(''),
      stderr: '',
    })
    results.java[role] = {
      uuid: id,
      assignedId: `${stem}-java-${role}`,
      closePolicy: role === 'admin' ? 'preclose-checkpoint-v1' : 'required-final-v1',
      timing: javaSampleSummary(list),
      normalExports: 2,
      finalExports: list.length - 2,
      exports: list.map(({ node, seq, terminating, dataSha256 }) => ({
        node,
        seq,
        terminating,
        dataSha256,
      })),
      closing: JSON.parse(markers.at(-2).line),
      closed: JSON.parse(markers.at(-1).line),
      exitCode: 0,
      exitSignal: null,
      closeToExitMs: 20,
      liveDescendants: [],
      markers: child.markers,
      stdout: child.stdout,
      stderr: '',
    }
  }
  const capture = (at, prefix = false) => ({
    at,
    cursors: Object.fromEntries(roles.map((r, i) => [String(i + 1), prefix ? 2 : rows[r].length])),
    nodes: Object.fromEntries(
      roles.map((r, i) => {
        const bytes = Buffer.from(
          (prefix ? rows[r].slice(0, 2) : rows[r]).map((r) => JSON.stringify(r) + '\n').join('')
        )
        return [String(i + 1), { bytes: bytes.length, sha256: digest(bytes) }]
      })
    ),
  })
  const admin = results.java.admin
  admin.checkpoint = {
    proof: capture(260, true),
    command: {
      raw: `CLOSE ${'q'.repeat(22)} ${compact(admin.uuid)} 3 2 ${rows.admin[1].dataSha256}\n`,
      sentAt: 261,
      completedAt: 262,
    },
  }
  admin.cessation = {
    drain: Array.from({ length: 11 }, (_, i) => capture(320 + i * 200)),
    quiet: Array.from({ length: 11 }, (_, i) => capture(2320 + i * 200)),
  }
  const entries = {
    metrics: JAVA_REQUESTED,
    'interval.ms': '1000',
    match: `client_id=^(${results.readback.ids.join('|')})$`,
  }
  results.subscription = {
    name: stem,
    entries,
    createdAt: 0,
    readback: {
      resourceType: 16,
      resourceName: stem,
      errorCode: 0,
      configEntries: Object.entries(entries).map(([configName, configValue]) => ({
        configName,
        configValue,
      })),
    },
    deletion: {
      startedAt: 4400,
      response: { resources: [{ errorCode: 0 }] },
      listing: { resources: [] },
    },
  }
  results.subscriptionOperations = [0, 1].map((index) => ({
    request: {
      timeout: 3000,
      resources: [
        {
          type: 16,
          name: stem,
          configEntries: Object.entries(entries).map(([name, value]) => ({
            name,
            operation: index,
            ...(index === 0 ? { value } : {}),
          })),
        },
      ],
    },
    response: { resources: [{ resourceType: 16, resourceName: stem, errorCode: 0 }] },
    startedAt: index ? 4400 : 0,
    completedAt: index ? 4401 : 0,
  }))
  for (const kind of kinds) results.js[kind] = { uuid: results.uuids.js[kind] }
  const transcripts = Object.fromEntries(
    kinds.map((kind) => [
      `js-${kind}`,
      {
        records: [
          {
            apiKey: 71,
            response: {
              errorCode: 0,
              requestedMetrics: JAVA_REQUESTED.split(','),
              pushIntervalMs: 1000,
              deltaTemporality: true,
            },
          },
        ],
      },
    ])
  )
  const record = {
    fixture,
    results,
    run: { stem },
    sidecars: { scenario: 'telemetry-java' },
  }
  return {
    directory,
    record,
    transcripts,
    rows,
    capture,
    close: () => fs.rmSync(directory, { recursive: true, force: true }),
  }
}

module.exports = { javaRow, createJavaInteropArchive }
