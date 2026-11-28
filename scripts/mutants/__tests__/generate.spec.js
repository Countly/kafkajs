const fs = require('fs')
const os = require('os')
const path = require('path')

const { generate, AREAS } = require('../generate')
const { MutantRunner } = require('../runner')

const REPO = path.resolve(__dirname, '..', '..', '..')

/**
 * @param {{ spec: string, witnesses?: string[] }} record
 * @returns {string[]} every witness spec of a record, the primary one first
 */
const witnessesOf = (record) => record.witnesses || [record.spec]

describe('scripts/mutants/generate.js', () => {
  let root

  beforeAll(() => {
    root = fs.mkdtempSync(path.join(os.tmpdir(), 'mutants-generate-'))
    const files = {
      'src/a/lib.js': [
        'function f(a, b) {',
        '  if (a < b) return true',
        "  log('x < y', a) // a call",
        '  return 0',
        '}',
        '',
      ].join('\n'),
      'src/b/two.js': [
        'class Box {',
        '  constructor(n) {',
        '    this.n = n',
        '  }',
        '',
        '  first(a, b) {',
        '    if (a < b) return 1',
        '    return 0',
        '  }',
        '',
        '  second(items) {',
        '    items.forEach((x) => {',
        '      log(x)',
        '    })',
        '  }',
        '}',
        '',
        'const third = (x) => {',
        '  const inner = (y) => {',
        '    if (y > 1) return y',
        '    return 0',
        '  }',
        '  return inner(x)',
        '}',
        '',
        'const LIMIT = 3',
        '',
        'const fourth = ({',
        '  a,',
        '  b = () => 1,',
        '}) => {',
        '  if (a > 2) return a',
        '  return b()',
        '}',
        '',
      ].join('\n'),
      'src/a/lib.spec.js': "test('f', () => {})\n",
      'src/a/other.spec.js': "test('g', () => {})\n",
      'src/a/__tests__/skipped.js': 'if (a < b) x()\n',
    }
    for (const [file, text] of Object.entries(files)) {
      fs.mkdirSync(path.dirname(path.join(root, file)), { recursive: true })
      fs.writeFileSync(path.join(root, file), text)
    }
  })

  afterAll(() => fs.rmSync(root, { recursive: true, force: true }))

  test('generation is deterministic for a fixed tree', () => {
    const areas = [
      { name: 'alpha', targets: ['src/a/'], specs: ['src/a/other.spec.js', 'src/a/lib.spec.js'] },
    ]
    const first = generate(root, areas)
    expect(generate(root, areas)).toEqual(first)
    const base = {
      area: 'alpha',
      target: 'src/a/lib.js',
      spec: 'src/a/lib.spec.js',
      title: '',
      commandClass: 'unit',
    }
    const at = (line, operator, original, mutated) => ({
      id: `alpha/src/a/lib.js:${line}:${operator}`,
      ...base,
      line,
      operator,
      original,
      mutated,
    })
    expect(first).toEqual([
      at(1, 'early-return', 'function f(a, b) {', 'function f(a, b) { return'),
      at(2, 'boundary', '  if (a < b) return true', '  if (a <= b) return true'),
      at(2, 'negated-condition', '  if (a < b) return true', '  if (!(a < b)) return true'),
      at(2, 'constant', '  if (a < b) return true', '  if (a < b) return false'),
      at(3, 'removed-call', "  log('x < y', a) // a call", ''),
      at(4, 'constant', '  return 0', '  return 1'),
    ])
    expect(() => MutantRunner.validate(first)).not.toThrow()
  })

  test('every generated mutant names an area, a target line and a witness spec', () => {
    const records = generate(REPO, AREAS)
    const names = AREAS.map((a) => a.name)
    expect(names).toEqual([
      'transactions',
      'offsets',
      'group-timing',
      'share',
      'protocol',
      'producer-send',
      'network',
      'fetch-loop',
      'compression',
      'aged-metadata',
      'cluster',
      'admin',
    ])
    for (const name of names) expect(records.some((r) => r.area === name)).toBe(true)
    expect(new Set(records.map((r) => r.id)).size).toBe(records.length)
    const lines = new Map()
    for (const r of records) {
      expect(witnessesOf(r)[0]).toBe(r.spec)
      expect(new Set(witnessesOf(r)).size).toBe(witnessesOf(r).length)
      for (const spec of witnessesOf(r)) {
        expect(spec).not.toMatch(/Live\.spec\.js$/)
        expect(fs.existsSync(path.join(REPO, spec))).toBe(true)
      }
      if (!lines.has(r.target))
        lines.set(r.target, fs.readFileSync(path.join(REPO, r.target), 'utf8').split('\n'))
      expect(lines.get(r.target)[r.line - 1]).toBe(r.original)
      expect(r.mutated).not.toBe(r.original)
    }
    expect(() => MutantRunner.validate(records)).not.toThrow()
  })

  test('the flexible SyncGroup codecs are witnessed by the grouped codec specs', () => {
    const records = generate(REPO, AREAS)
    const specsOf = (target) => [
      ...new Set(records.filter((r) => r.target === target).map((r) => r.spec)),
    ]
    const dir = 'src/protocol/requests'
    expect(specsOf(`${dir}/syncGroup/v4/request.js`)).toEqual([`${dir}/groupRequests.spec.js`])
    expect(specsOf(`${dir}/syncGroup/v5/request.js`)).toEqual([`${dir}/groupRequests.spec.js`])
    expect(specsOf(`${dir}/syncGroup/v4/response.js`)).toEqual([`${dir}/groupResponses.spec.js`])
    expect(specsOf(`${dir}/syncGroup/v5/response.js`)).toEqual([`${dir}/groupResponses.spec.js`])
    expect(specsOf(`${dir}/syncGroup/index.js`)).toEqual([`${dir}/syncGroup/index.spec.js`])
  })
  test('every witness spec an area names runs in the broker-free unit lane', () => {
    const script = require('../../../package.json').scripts['test:unit']
    const selected = script.slice(script.indexOf('--testPathPatterns'))
    const patterns = [...selected.matchAll(/'([^']+)'/g)].map(([, p]) => new RegExp(p))
    const named = AREAS.flatMap(({ specs, witnesses = {} }) => [
      ...specs,
      ...Object.values(witnesses).flatMap((entry) =>
        typeof entry === 'object' && !Array.isArray(entry) ? Object.values(entry).flat() : entry
      ),
    ])
    expect(named.length).toBeGreaterThan(100)
    expect([...new Set(named)].filter((spec) => !patterns.some((p) => p.test(spec)))).toEqual([])
  })

  test('aged-metadata mutates only the metadata-age and disconnect-fence methods of the cluster and pool', () => {
    const area = AREAS.find((a) => a.name === 'aged-metadata')
    const records = generate(REPO, AREAS).filter((r) => r.area === 'aged-metadata')
    const lines = new Map()
    const methods = new Set()
    for (const r of records) {
      if (!lines.has(r.target))
        lines.set(r.target, fs.readFileSync(path.join(REPO, r.target), 'utf8').split('\n'))
      const text = lines.get(r.target)
      let at = r.line - 1
      while (at >= 0 && !/^ {2}(?:async )?[\w#]+\(/.test(text[at])) at--
      const method = /^ {2}(?:async )?([\w#]+)\(/.exec(text[at])[1]
      expect(Object.keys(area.witnesses[r.target])).toContain(method)
      methods.add(`${r.target}#${method}`)
    }
    expect([...methods].sort()).toEqual([
      'src/cluster/brokerPool.js#disconnect',
      'src/cluster/brokerPool.js#metadataState',
      'src/cluster/brokerPool.js#refreshMetadata',
      'src/cluster/brokerPool.js#refreshMetadataIfNecessary',
      'src/cluster/brokerPool.js#refreshMetadataUnshared',
      'src/cluster/index.js#refreshMetadataIfNecessary',
      'src/cluster/index.js#serveFromCache',
    ])
  })

  describe('method-specific witnesses', () => {
    const lib = 'src/a/lib.spec.js'
    const other = 'src/a/other.spec.js'
    const recordOf = (records, original, operator) =>
      records.find((r) => r.original === original && r.operator === operator)

    test('a method with a rule is witnessed by its specs, then by the target default; other methods keep the default only', () => {
      const area = {
        name: 'beta',
        targets: ['src/b/two.js'],
        specs: [lib],
        witnesses: {
          'src/b/two.js': { first: [other], third: [other], fourth: [other], LIMIT: [other] },
        },
      }
      const records = generate(root, [area])
      const first = recordOf(records, '    if (a < b) return 1', 'boundary')
      expect(first).toMatchObject({ spec: other, witnesses: [other, lib] })
      const head = recordOf(records, '  first(a, b) {', 'early-return')
      expect(head).toMatchObject({ spec: other, witnesses: [other, lib] })
      const nested = recordOf(records, '    if (y > 1) return y', 'boundary')
      expect(nested).toMatchObject({ spec: other, witnesses: [other, lib] })
      const multiline = recordOf(records, '  if (a > 2) return a', 'boundary')
      expect(multiline).toMatchObject({ spec: other, witnesses: [other, lib] })
      const constant = recordOf(records, 'const LIMIT = 3', 'constant')
      expect(constant).toMatchObject({ spec: other, witnesses: [other, lib] })
      const ctor = recordOf(records, '  constructor(n) {', 'early-return')
      expect(ctor).toMatchObject({ spec: lib })
      expect(ctor).not.toHaveProperty('witnesses')
      const closure = recordOf(records, '      log(x)', 'removed-call')
      expect(closure).toMatchObject({ spec: lib })
      expect(closure).not.toHaveProperty('witnesses')
      expect(() => MutantRunner.validate(records)).not.toThrow()
    })

    test('a rule for a whole file replaces the default and names every mutant of the file', () => {
      const area = {
        name: 'beta',
        targets: ['src/a/lib.js'],
        specs: [lib],
        witnesses: { 'src/a/lib.js': [other, lib] },
      }
      const records = generate(root, [area])
      expect(records.length).toBeGreaterThan(0)
      for (const r of records) expect(r).toMatchObject({ spec: other, witnesses: [other, lib] })
    })

    test('a rule that already names the default does not repeat it', () => {
      const area = {
        name: 'beta',
        targets: ['src/b/two.js'],
        specs: [lib],
        witnesses: { 'src/b/two.js': { first: [lib, other] } },
      }
      const first = recordOf(generate(root, [area]), '    if (a < b) return 1', 'boundary')
      expect(first).toMatchObject({ spec: lib, witnesses: [lib, other] })
    })

    test('a witness spec that does not exist fails generation and names the spec and the area', () => {
      const missing = 'src/a/missing.spec.js'
      const ruled = {
        name: 'gamma',
        targets: ['src/b/two.js'],
        specs: [lib],
        witnesses: { 'src/b/two.js': { first: [missing] } },
      }
      const listed = {
        name: 'delta',
        targets: ['src/b/two.js'],
        specs: [lib, 'src/a/gone.spec.js'],
      }
      const whole = {
        name: 'epsilon',
        targets: ['src/a/lib.js'],
        specs: [lib],
        witnesses: { 'src/a/lib.js': missing },
      }
      expect(() => generate(root, [ruled])).toThrow(
        'mutants: the witness spec src/a/missing.spec.js of area gamma does not exist'
      )
      expect(() => generate(root, [listed])).toThrow(
        'mutants: the witness spec src/a/gone.spec.js of area delta does not exist'
      )
      expect(() => generate(root, [whole])).toThrow(
        'mutants: the witness spec src/a/missing.spec.js of area epsilon does not exist'
      )
    })

    test('a method rule that matches no method fails generation and names the key and the target', () => {
      const rule = (witnesses) => ({
        name: 'zeta',
        targets: ['src/b/two.js'],
        specs: [lib],
        witnesses: { 'src/b/two.js': witnesses },
      })
      expect(() => generate(root, [rule({ first: [other], renamed: [other] })])).toThrow(
        'mutants: the witness rule renamed of src/b/two.js matches no method'
      )
      expect(() => generate(root, [rule({ First: [other] })])).toThrow(
        'mutants: the witness rule First of src/b/two.js matches no method'
      )
      const valid = generate(root, [rule({ first: [other], third: [other] })])
      expect(recordOf(valid, '    if (a < b) return 1', 'boundary')).toMatchObject({
        spec: other,
        witnesses: [other, lib],
      })
    })
    test('a methodsOnly area mutates only the methods its rules name and keeps its unruled targets whole', () => {
      const area = {
        name: 'theta',
        targets: ['src/a/lib.js', 'src/b/two.js'],
        specs: [lib],
        methodsOnly: true,
        witnesses: { 'src/b/two.js': { first: [other], third: [other] } },
      }
      const records = generate(root, [area])
      const ruled = records.filter((r) => r.target === 'src/b/two.js')
      expect(ruled.map((r) => [r.line, r.operator])).toEqual([
        [6, 'early-return'],
        [7, 'boundary'],
        [7, 'negated-condition'],
        [7, 'constant'],
        [8, 'constant'],
        [18, 'early-return'],
        [19, 'early-return'],
        [20, 'boundary'],
        [20, 'negated-condition'],
        [20, 'constant'],
        [21, 'constant'],
      ])
      for (const r of ruled) expect(r).toMatchObject({ spec: other, witnesses: [other, lib] })
      const whole = generate(root, [{ ...area, methodsOnly: undefined }])
      expect(records.filter((r) => r.target === 'src/a/lib.js')).toEqual(
        whole.filter((r) => r.target === 'src/a/lib.js')
      )
      expect(whole.filter((r) => r.target === 'src/b/two.js').length).toBeGreaterThan(ruled.length)
      expect(() =>
        generate(root, [{ ...area, witnesses: { 'src/b/two.js': { gone: [other] } } }])
      ).toThrow('mutants: the witness rule gone of src/b/two.js matches no method')
    })

    test('a rule for a file the area does not generate fails generation and names the file', () => {
      const area = (witnesses) => ({ name: 'eta', targets: ['src/b/'], specs: [lib], witnesses })
      expect(() => generate(root, [area({ 'src/b/moved.js': { first: [other] } })])).toThrow(
        'mutants: the witness rule for src/b/moved.js of area eta names no generated target'
      )
      expect(() => generate(root, [area({ 'src/b/gone.js': [other] })])).toThrow(
        'mutants: the witness rule for src/b/gone.js of area eta names no generated target'
      )
      const valid = generate(root, [area({ 'src/b/two.js': { first: [other] } })])
      expect(recordOf(valid, '    if (a < b) return 1', 'boundary')).toMatchObject({
        spec: other,
        witnesses: [other, lib],
      })
    })
  })

  describe('the guard specs of the survivors a default witness missed', () => {
    const OFFSETS = 'src/consumer/offsetManager/__tests__'
    const SHARE = 'src/consumer/share/__tests__'
    const PRODUCER = 'src/producer'
    const PROTOCOL = 'src/protocol'
    const REQUESTS = 'src/protocol/requests'
    const SCHEMA_SPEC = 'scripts/protocol/__tests__/schema.spec.js'
    const records = generate(REPO, AREAS)
    /** @type {Map<string, string[]>} */
    const files = new Map()
    const linesOf = (target) => {
      if (!files.has(target))
        files.set(target, fs.readFileSync(path.join(REPO, target), 'utf8').split('\n'))
      return files.get(target)
    }

    /**
     * @param {string} target the source file
     * @param {string} head the start of the line that opens the method holding the mutated line
     * @param {string} original the mutated line, exactly
     * @param {string} operator
     * @returns {object|undefined} the generated record of that line
     */
    const recordAt = (target, head, original, operator) => {
      const lines = linesOf(target)
      const from = lines.findIndex((l) => l.trim().startsWith(head))
      if (from < 0) throw new Error(`no line of ${target} starts with ${head}`)
      const at = lines.findIndex((l, i) => i >= from && l === original)
      return records.find(
        (r) => r.target === target && r.line === at + 1 && r.operator === operator
      )
    }

    // [label, target, method head, mutated line, operator, witnesses: the rule's specs in order, then the default unless listed]
    const GUARDED = [
      [
        'committed offset revisions: the shared cache entry',
        'src/cluster/committedOffsetRevisions.js',
        'static forCache(',
        '      caches.set(cache, revisions)',
        'removed-call',
        [
          `${OFFSETS}/privateResetPositions.spec.js`,
          'src/cluster/__tests__/committedOffsetRevisions.spec.js',
        ],
      ],
      [
        'offset manager: the duration reset gate',
        'src/consumer/offsetManager/index.js',
        'async resetByDuration(',
        '      if (!current()) return false',
        'negated-condition',
        [`${OFFSETS}/resetSchedules.spec.js`, `${OFFSETS}/commitOffsets.spec.js`],
      ],
      [
        'offset manager: the seek publication fence',
        'src/consumer/offsetManager/index.js',
        'async commitAndClear(',
        '        this.operations.apply(ticket, topic, partition)',
        'removed-call',
        [`${OFFSETS}/resetSchedules.spec.js`, `${OFFSETS}/commitOffsets.spec.js`],
      ],
      [
        'offset manager: a failed resolution ends its ticket',
        'src/consumer/offsetManager/index.js',
        'async resolveOffsets(',
        '      this.operations.end(ticket)',
        'removed-call',
        [
          `${OFFSETS}/localOffsets.spec.js`,
          `${OFFSETS}/resetSchedules.spec.js`,
          `${OFFSETS}/commitOffsets.spec.js`,
        ],
      ],
      [
        'offset manager: applying a resolution',
        'src/consumer/offsetManager/index.js',
        'applyResolution(',
        '  applyResolution(ticket, offsets, epochSeeds, requestedIds, derived = new Set()) {',
        'early-return',
        [
          `${OFFSETS}/initializeConsumerOffsets.spec.js`,
          `${OFFSETS}/resetSchedules.spec.js`,
          `${OFFSETS}/commitOffsets.spec.js`,
        ],
      ],
      [
        'offset manager: seeding consumed epochs from an epoch of zero',
        'src/consumer/offsetManager/index.js',
        'seedConsumedEpochs(',
        '      if (epoch < 0 && epochs[partition] >= 0) {',
        'boundary',
        [`${OFFSETS}/resolveOffsetsEpoch.spec.js`, `${OFFSETS}/commitOffsets.spec.js`],
      ],
      [
        'offset operations: the last idle notification',
        'src/consumer/offsetManager/offsetOperations.js',
        'notifyIdle(',
        '    if (this.isIdle()) this.onIdle?.()',
        'negated-condition',
        ['src/cluster/__tests__/groupRegistry.spec.js', `${OFFSETS}/offsetOperations.spec.js`],
      ],
      [
        'reset obligations: clearing the entries',
        'src/consumer/offsetManager/resetObligations.js',
        'clear(',
        '    this.entries.clear()',
        'removed-call',
        [
          'src/cluster/__tests__/groupRegistry.spec.js',
          `${OFFSETS}/resetSchedules.spec.js`,
          `${OFFSETS}/resetObligations.spec.js`,
        ],
      ],
      [
        'decoder: the int32 length check',
        'src/protocol/decoder.js',
        'readInt32(',
        "    this.ensureReadable(INT32_SIZE, 'readInt32')",
        'removed-call',
        [`${PROTOCOL}/decoder.fuzz.spec.js`, `${PROTOCOL}/decoder.spec.js`],
      ],
      [
        'decoder: the uuid length check',
        'src/protocol/decoder.js',
        'readUuid(',
        "    this.ensureReadable(UUID_SIZE, 'readUuid')",
        'removed-call',
        [`${PROTOCOL}/uuid.spec.js`, `${PROTOCOL}/decoder.spec.js`],
      ],
      [
        'decoder: the array payload check',
        'src/protocol/decoder.js',
        'readArray(',
        "    this.ensureReadable(length, 'readArray')",
        'removed-call',
        [`${PROTOCOL}/decoderCountBounds.spec.js`, `${PROTOCOL}/decoder.spec.js`],
      ],
      [
        'decoder: the array count cap',
        'src/protocol/decoder.js',
        'assertCount(',
        '    if (count > MAX_ARRAY_COUNT) {',
        'boundary',
        [`${PROTOCOL}/decoderCountBounds.spec.js`, `${PROTOCOL}/decoder.spec.js`],
      ],
      [
        'decoder: the unsigned varint continuation guard',
        'src/protocol/decoder.js',
        'readUVarInt(',
        '      if (i > 28) {',
        'negated-condition',
        [`${PROTOCOL}/encoder.spec.js`, `${PROTOCOL}/decoder.spec.js`],
      ],
      [
        'decoder: the tagged field count cap',
        'src/protocol/decoder.js',
        'readTaggedFields(',
        '    if (numberOfTaggedFields > MAX_TAGGED_FIELDS) {',
        'negated-condition',
        [`${PROTOCOL}/taggedFields.spec.js`, `${PROTOCOL}/decoder.spec.js`],
      ],
      [
        'decoder: the bounded header array loop',
        'src/protocol/decoder.js',
        'readVarIntArrayBytesBounded(',
        '    for (let i = 0; i < length; i++) {',
        'boundary',
        [`${PROTOCOL}/decoderHeaderArray.spec.js`, `${PROTOCOL}/decoder.spec.js`],
      ],
      [
        'encoder: writing an int8',
        'src/protocol/encoder.js',
        'writeInt8(',
        '  writeInt8(value) {',
        'early-return',
        [`${PROTOCOL}/compactPrimitives.spec.js`, `${PROTOCOL}/encoder.spec.js`],
      ],
      [
        'encoder: the null marker of a varint-length byte string',
        'src/protocol/encoder.js',
        'writeVarIntBytes(',
        '      this.writeVarInt(-1)',
        'removed-call',
        [
          `${PROTOCOL}/recordBatch/record/v0/decoderEquality.spec.js`,
          `${PROTOCOL}/requests/produce/produceFraming.spec.js`,
          `${PROTOCOL}/encoder.spec.js`,
        ],
      ],
      [
        'encoder: the length of a compact byte string',
        'src/protocol/encoder.js',
        'writeUVarIntBytes(',
        '      this.writeUVarInt(value.length + 1)',
        'removed-call',
        [`${PROTOCOL}/compactPrimitives.spec.js`, `${PROTOCOL}/encoder.spec.js`],
      ],
      [
        'encoder: a nullable array that has elements',
        'src/protocol/encoder.js',
        'writeNullableArray(',
        '    this.writeArray(array, type, length)',
        'removed-call',
        [`${PROTOCOL}/requests/metadata/v1/request.spec.js`, `${PROTOCOL}/encoder.spec.js`],
      ],
      [
        'encoder: the uuid string check',
        'src/protocol/encoder.js',
        'writeUuid(',
        '    } else if (UUID_STRING.test(value)) {',
        'negated-condition',
        [`${PROTOCOL}/uuid.spec.js`, `${PROTOCOL}/encoder.spec.js`],
      ],
      [
        'share run: processing an advancing session answer',
        'src/consumer/share/shareRun.js',
        'afterFetch(',
        '    if (result.reset || result.closed || node.session.epoch === epoch) return',
        'negated-condition',
        [`${SHARE}/shareConsumerSubscription.spec.js`, `${SHARE}/shareConsumerClose.spec.js`],
      ],
      [
        'share run: delivering a batch to the batch handler',
        'src/consumer/share/shareRun.js',
        'async dispatchBatch(',
        '    await this.invoke(node, cohort, call, call.batchPayload(batch))',
        'removed-call',
        [`${SHARE}/shareConsumerModes.spec.js`, `${SHARE}/shareConsumerClose.spec.js`],
      ],
      [
        'share run: releasing the records a halted acquisition keeps',
        'src/consumer/share/shareRun.js',
        'releaseFrom(',
        '  releaseFrom(node, cohort, from) {',
        'early-return',
        [`${SHARE}/shareConsumerModes.spec.js`, `${SHARE}/shareConsumerClose.spec.js`],
      ],
      [
        'share run: freezing handler authority when a crash cuts a stop short',
        'src/consumer/share/shareRun.js',
        'escalate(',
        '  escalate(episode) {',
        'early-return',
        [`${SHARE}/shareConsumerShutdown.spec.js`, `${SHARE}/shareConsumerClose.spec.js`],
      ],
      [
        'share run: retiring the operation of a held initial dial',
        'src/consumer/share/shareRun.js',
        'freeze(',
        "      node.operation.retire(new KafkaJSNonRetriableError('Share run closed'))",
        'removed-call',
        [
          `${SHARE}/shareConsumerDialRelease.spec.js`,
          `${SHARE}/shareConsumerEvents.spec.js`,
          `${SHARE}/shareConsumerClose.spec.js`,
        ],
      ],
      [
        'share runner: a failed connection is not remembered',
        'src/consumer/share/shareRunner.js',
        'connect(',
        '      const attempt = this.connectOnce().finally(() => {',
        'early-return',
        [`${SHARE}/shareConsumerConnect.spec.js`, `${SHARE}/shareConsumerClose.spec.js`],
      ],
      [
        'share runner: the telemetry reader of the current run',
        'src/consumer/share/shareRunner.js',
        'readTelemetryResources(',
        '  readTelemetryResources() {',
        'early-return',
        ['src/telemetry/__tests__/factories.spec.js', `${SHARE}/shareConsumerClose.spec.js`],
      ],
      [
        'share runner: the telemetry reader of a retired run',
        'src/consumer/share/shareRunner.js',
        'readTelemetryResources(',
        '    if (!run || run.retired || membership.closed) return null',
        'negated-condition',
        ['src/telemetry/__tests__/factories.spec.js', `${SHARE}/shareConsumerClose.spec.js`],
      ],
      [
        'accumulator: a half of a split send settles once',
        'src/producer/state/accumulator.js',
        'const half = (first) => {',
        '        if (done) return',
        'negated-condition',
        [
          `${PRODUCER}/state/__tests__/subdivision.spec.js`,
          `${PRODUCER}/state/__tests__/accumulator.spec.js`,
        ],
      ],
      [
        'record size: the bytes of a record list',
        'src/producer/state/recordSize.js',
        'const recordsBytes = (',
        'const recordsBytes = (records) => {',
        'early-return',
        [
          `${PROTOCOL}/requests/produce/produceFraming.spec.js`,
          `${PRODUCER}/state/__tests__/recordSize.spec.js`,
        ],
      ],
      [
        'transaction offsets: a topic without partitions starts nothing',
        'src/producer/eosManager/index.js',
        'const hasPartitions = ',
        'const hasPartitions = (topics) => topics.some(({ partitions }) => partitions.length > 0)',
        'boundary',
        [
          `${PRODUCER}/__tests__/sendOffsets.spec.js`,
          `${PRODUCER}/__tests__/sendOffsetsPublication.spec.js`,
          `${PRODUCER}/eosManager/index.spec.js`,
        ],
      ],
      [
        'partition sequencer: restoring the first unsent batch',
        'src/producer/state/partitionSequencer.js',
        'restoreUnsent(',
        '    if (position >= 0) {',
        'boundary',
        [
          `${PRODUCER}/__tests__/splitSend.spec.js`,
          `${PRODUCER}/state/__tests__/partitionSequencer.spec.js`,
        ],
      ],

      [
        'group timeouts: the lower bound of the session timeout',
        'src/consumer/groupTimeouts.js',
        'const resolveGroupTimeouts = ({',
        '  if (!Number.isInteger(sessionTimeout) || sessionTimeout < 1 || sessionTimeout > 2147483647) {',
        'constant',
        [
          'src/consumer/__tests__/consumerDefaults.spec.js',
          'src/consumer/__tests__/groupTimeouts.spec.js',
        ],
      ],
      [
        'sync group v0 request: its api version',
        `${REQUESTS}/syncGroup/v0/request.js`,
        'module.exports',
        '  apiVersion: 0,',
        'constant',
        [`${REQUESTS}/index.spec.js`, `${REQUESTS}/syncGroup/v0/request.spec.js`],
      ],
      [
        'sync group v1 request: its api version',
        `${REQUESTS}/syncGroup/v1/request.js`,
        'module.exports',
        '  Object.assign(requestV0({ groupId, generationId, memberId, groupAssignment }), { apiVersion: 1 })',
        'constant',
        [`${REQUESTS}/index.spec.js`, `${REQUESTS}/syncGroup/v1/request.spec.js`],
      ],
      [
        'sync group v2 request: its api version',
        `${REQUESTS}/syncGroup/v2/request.js`,
        'module.exports',
        '  Object.assign(requestV1({ groupId, generationId, memberId, groupAssignment }), { apiVersion: 2 })',
        'constant',
        [`${REQUESTS}/index.spec.js`, `${REQUESTS}/syncGroup/v2/request.spec.js`],
      ],
      [
        'sync group v3 request: its api version',
        `${REQUESTS}/syncGroup/v3/request.js`,
        'module.exports',
        '  apiVersion: 3,',
        'constant',
        [`${REQUESTS}/index.spec.js`, `${REQUESTS}/syncGroup/v3/request.spec.js`],
      ],
      [
        'sync group v4 request: its api version',
        `${REQUESTS}/syncGroup/v4/request.js`,
        'module.exports',
        '  apiVersion: 4,',
        'constant',
        [`${REQUESTS}/groupRequests.spec.js`, `${REQUESTS}/index.spec.js`, SCHEMA_SPEC],
      ],
      [
        'sync group v4 request: the version of its schema',
        `${REQUESTS}/syncGroup/v4/request.js`,
        'module.exports.schema',
        '  version: 4,',
        'constant',
        [`${REQUESTS}/groupRequests.spec.js`, `${REQUESTS}/index.spec.js`, SCHEMA_SPEC],
      ],
      [
        'sync group v4 response: the version of its schema',
        `${REQUESTS}/syncGroup/v4/response.js`,
        'module.exports.schema',
        '  version: 4,',
        'constant',
        [`${REQUESTS}/groupResponses.spec.js`, SCHEMA_SPEC],
      ],
      [
        'sync group v5 request: the version of its schema',
        `${REQUESTS}/syncGroup/v5/request.js`,
        'module.exports.schema',
        '  version: 5,',
        'constant',
        [`${REQUESTS}/groupRequests.spec.js`, `${REQUESTS}/index.spec.js`, SCHEMA_SPEC],
      ],
      [
        'sync group v5 request: a nullable protocol type in its schema',
        `${REQUESTS}/syncGroup/v5/request.js`,
        'module.exports.schema',
        "          { name: 'ProtocolType', type: 'string', versions: '5+', nullable: true },",
        'constant',
        [`${REQUESTS}/groupRequests.spec.js`, `${REQUESTS}/index.spec.js`, SCHEMA_SPEC],
      ],
      [
        'sync group v5 response: the version of its schema',
        `${REQUESTS}/syncGroup/v5/response.js`,
        'module.exports.schema',
        '  version: 5,',
        'constant',
        [`${REQUESTS}/groupResponses.spec.js`, SCHEMA_SPEC],
      ],
      [
        'offset manager: a commit is manual unless a caller says otherwise',
        'src/consumer/offsetManager/index.js',
        'async commitWithOwnership(',
        '  async commitWithOwnership(offsets, { partitions = null, auto = false, guard, fence } = {}) {',
        'constant',
        [`src/consumer/__tests__/lostMeansNoCommit.spec.js`, `${OFFSETS}/commitOffsets.spec.js`],
      ],
      [
        'offset operations: ending a ticket releases its open slot',
        'src/consumer/offsetManager/offsetOperations.js',
        'end(',
        '    this.openTickets -= 1',
        'constant',
        ['src/cluster/__tests__/groupRegistry.spec.js', `${OFFSETS}/offsetOperations.spec.js`],
      ],
      [
        'decoder: the high byte of an int64',
        'src/protocol/decoder.js',
        'readInt64(',
        '      (first << 24) + // Overflow',
        'constant',
        [`${PROTOCOL}/decoder.spec.js`, 'src/__tests__/equivalentApis.spec.js'],
      ],
      [
        'decoder: an empty compact byte string',
        'src/protocol/decoder.js',
        'readUVarIntBytes(',
        '    if (byteLength === 0) {',
        'constant',
        [
          `${PROTOCOL}/compactPrimitives.spec.js`,
          `${PROTOCOL}/encoder.spec.js`,
          `${PROTOCOL}/decoder.spec.js`,
        ],
      ],
      [
        'record size: the size of an empty record',
        'src/producer/state/recordSize.js',
        'const EMPTY_RECORD',
        'const EMPTY_RECORD = 24',
        'constant',
        [
          `${REQUESTS}/produce/produceFraming.spec.js`,
          `${PRODUCER}/state/__tests__/recordSize.spec.js`,
        ],
      ],
      [
        'share run: retiring the coordinator lookups in flight',
        'src/consumer/share/shareRun.js',
        'retireLookups(',
        '    if (this.lookups.size === 0) return',
        'constant',
        [`${SHARE}/shareConsumerCoordinatorClose.spec.js`, `${SHARE}/shareConsumerClose.spec.js`],
      ],
      [
        'admin deadline: a closed lifecycle expires the invocation',
        'src/admin/deadline.js',
        'get expired(',
        '  get expired() {',
        'early-return',
        ['src/admin/__tests__/deadlineScopes.spec.js', 'src/admin/__tests__/apiDeadline.spec.js'],
      ],
      [
        'admin deadline: the consumer scope checks its operation',
        'src/admin/deadline.js',
        'class ConsumerDescriptionScope',
        '    this.operation.check()',
        'removed-call',
        ['src/admin/__tests__/deadlineScopes.spec.js', 'src/admin/__tests__/apiDeadline.spec.js'],
      ],
      [
        'share runner: the budget a disconnect hands to the telemetry close',
        'src/consumer/share/shareRunner.js',
        'async disconnect(',
        '            deadline === Infinity ? Infinity : Math.max(0, deadline - performance.now()),',
        'constant',
        ['src/telemetry/__tests__/close.spec.js', `${SHARE}/shareConsumerClose.spec.js`],
      ],
      [
        'share runner: the default restart verdict of a crash',
        'src/consumer/share/shareRunner.js',
        'async superviseCrash(',
        "      error.name === 'KafkaJSNumberOfRetriesExceeded' || originalCause(error).retriable === true",
        'constant',
        [
          `${SHARE}/shareConsumerConnect.spec.js`,
          `${SHARE}/shareConsumerModes.spec.js`,
          `${SHARE}/shareConsumerClose.spec.js`,
        ],
      ],
      [
        'share runner: a crash that must not restart never restarts',
        'src/consumer/share/shareRunner.js',
        'async superviseCrash(',
        '    if (this.mustNotRestart(error)) restart = false',
        'constant',
        [
          `${SHARE}/shareConsumerConnect.spec.js`,
          `${SHARE}/shareConsumerModes.spec.js`,
          `${SHARE}/shareConsumerClose.spec.js`,
        ],
      ],
      [
        'committed offset revisions: peeking never creates a source',
        'src/cluster/committedOffsetRevisions.js',
        'static peek(',
        '  static peek(cache) {',
        'early-return',
        [
          'src/cluster/__tests__/committedOffsetRetirement.spec.js',
          'src/cluster/__tests__/committedOffsetRevisions.spec.js',
        ],
      ],
      [
        'offset operations: binding the cache',
        'src/consumer/offsetManager/offsetOperations.js',
        'bind(',
        '  bind(cache) {',
        'early-return',
        ['src/cluster/__tests__/groupRegistry.spec.js', `${OFFSETS}/offsetOperations.spec.js`],
      ],
      [
        'reset obligations: a subscription change retires unowned topics',
        'src/consumer/offsetManager/resetObligations.js',
        'claimSubscription(',
        '    this.retireUnowned()',
        'removed-call',
        [
          'src/cluster/__tests__/committedOffsetRetirement.spec.js',
          `${OFFSETS}/resetObligations.spec.js`,
        ],
      ],
      [
        'reset obligations: retaining a subscription drops other debts',
        'src/consumer/offsetManager/resetObligations.js',
        'retain(',
        '  retain(topics) {',
        'early-return',
        [
          'src/cluster/__tests__/committedOffsetRetirement.spec.js',
          `${OFFSETS}/resetObligations.spec.js`,
        ],
      ],
      [
        'offset manager: a log truncation repositions the partition',
        'src/consumer/offsetManager/index.js',
        'async reposition(',
        '  async reposition({ topic, partition, offset }, preparation) {',
        'early-return',
        ['src/consumer/__tests__/logTruncation.spec.js', `${OFFSETS}/commitOffsets.spec.js`],
      ],
      [
        'offset manager: a seek of an assigned partition is applied',
        'src/consumer/offsetManager/index.js',
        'async seek(',
        '    if (!this.memberAssignment[topic] || !this.memberAssignment[topic].includes(partition)) {',
        'negated-condition',
        [`${OFFSETS}/groupInstanceId.spec.js`, `${OFFSETS}/commitOffsets.spec.js`],
      ],
      [
        'offset manager: a held reset refusal is handed out once',
        'src/consumer/offsetManager/index.js',
        'holdResetRefusals(',
        '      partitions.set(partition, { refusal, taken: false, topicId: requestedIds.get(topic) })',
        'constant',
        ['src/consumer/__tests__/offsetResetHalt.spec.js', `${OFFSETS}/commitOffsets.spec.js`],
      ],
      [
        'offset manager: one pending topic is resolved',
        'src/consumer/offsetManager/index.js',
        'async resolveOffsets(',
        '    if (pendingPartitions.length === 0) {',
        'constant',
        [
          `${OFFSETS}/localOffsets.spec.js`,
          `${OFFSETS}/resetSchedules.spec.js`,
          `${OFFSETS}/commitOffsets.spec.js`,
        ],
      ],
      [
        'transactions: sent offsets are staged for the commit',
        'src/producer/eosManager/index.js',
        'const createEosManager',
        '          stateManager.offsets.stage(snapshot)',
        'removed-call',
        [
          'src/producer/eosManager/index.spec.js',
          'src/producer/__tests__/sendOffsetsPublication.spec.js',
        ],
      ],
      [
        'sync group: the version table',
        'src/protocol/requests/syncGroup/index.js',
        'const versions',
        '  0: ({ groupId, generationId, memberId, groupAssignment, sessionTimeout, rebalanceTimeout }) => {',
        'constant',
        [
          'src/protocol/requests/syncGroup/index.spec.js',
          'scripts/compat/__tests__/forkVersions.spec.js',
        ],
      ],
      [
        'share run: a partition move resets the old session',
        'src/consumer/share/shareRun.js',
        'onMove(',
        '  onMove(node, move) {',
        'early-return',
        [`${SHARE}/shareSessionFaults.spec.js`, `${SHARE}/shareConsumerClose.spec.js`],
      ],
      [
        'share run: the close coordinator needs a positive member epoch',
        'src/consumer/share/shareRun.js',
        'captureCloseCoordinator(',
        '      membership.memberEpoch <= 0 ||',
        'constant',
        [`${SHARE}/shareConsumerCoordinatorClose.spec.js`, `${SHARE}/shareConsumerClose.spec.js`],
      ],
      [
        'share run: a written leave is recorded',
        'src/consumer/share/shareRun.js',
        'async leave(',
        '      else if (frameLength !== undefined) episode.leaveWritten = true',
        'constant',
        [`${SHARE}/shareConsumerCoordinatorClose.spec.js`, `${SHARE}/shareConsumerClose.spec.js`],
      ],
      [
        'share runner: a disconnect timeout of 0 is accepted',
        'src/consumer/share/shareRunner.js',
        'const parseDisconnectTimeout',
        '  if (timeout !== undefined && (!Number.isInteger(timeout) || timeout < 0 || timeout > MAX_INT)) {',
        'boundary',
        [`${SHARE}/shareConsumerOptions.spec.js`, `${SHARE}/shareConsumerClose.spec.js`],
      ],
      [
        'aged metadata: an aged cache is served while the refresh runs',
        'src/cluster/index.js',
        'serveFromCache(',
        "    if (cached !== 'aged') return cached === 'fresh'",
        'negated-condition',
        [`${PRODUCER}/__tests__/agedMetadata.spec.js`],
      ],
      [
        'aged metadata: a disconnect clears what the pool installed',
        'src/cluster/brokerPool.js',
        'async disconnect(',
        '    this.authenticationBlackout.clear()',
        'removed-call',
        [
          `${PRODUCER}/__tests__/agedMetadata.spec.js`,
          'src/cluster/__tests__/disconnectDrains.spec.js',
        ],
      ],
      [
        'aged metadata: a refresh begun before a disconnect installs nothing',
        'src/cluster/brokerPool.js',
        'async refreshMetadataUnshared(',
        '        if (!applicable()) return',
        'negated-condition',
        [
          `${PRODUCER}/__tests__/agedMetadata.spec.js`,
          'src/cluster/__tests__/metadataCacheWiring.spec.js',
          'src/cluster/__tests__/obsoleteDrain.spec.js',
          'src/cluster/__tests__/consumerTopicIsolation.spec.js',
        ],
      ],
      [
        'network: the frame buffer refuses a non-Kafka reply',
        'src/network/frameBuffer.js',
        'const looksLike',
        '  if (prefix.readInt32BE(0) === HTTP_AS_SIZE) {',
        'negated-condition',
        [
          'src/network/__tests__/frameValidation.spec.js',
          'src/network/__tests__/flexibleFraming.spec.js',
        ],
      ],
      [
        'producer send: a request keyed by topic id only when every id is known',
        'src/producer/sendMessages.js',
        'const topicIdsOf',
        '    if (topicId === undefined) {',
        'negated-condition',
        [
          `${PRODUCER}/sendMessages.spec.js`,
          `${PRODUCER}/__tests__/drainLoop.spec.js`,
          `${PRODUCER}/__tests__/splitSend.spec.js`,
          `${PRODUCER}/__tests__/leaderHint.spec.js`,
        ],
      ],
      [
        'fetch loop: the fetcher set is rebuilt when its nodes differ',
        'src/consumer/fetchManager.js',
        'const differs',
        '  if (current.length !== wanted.size) return true',
        'negated-condition',
        [
          'src/consumer/fetchManager.spec.js',
          'src/consumer/__tests__/fetchManagerReconcile.spec.js',
          'src/consumer/__tests__/fetchManagerStop.spec.js',
          'src/consumer/__tests__/fetchManagerErrorIsolation.spec.js',
        ],
      ],
    ]

    test.each(GUARDED)(
      '%s is witnessed by the specs of its rule in listed order, then the default unless listed',
      (_label, target, head, original, operator, specs) => {
        const record = recordAt(target, head, original, operator)
        expect(record).toMatchObject({ target, operator, original })
        expect(witnessesOf(record)).toEqual(specs)
        expect(record.spec).toBe(specs[0])
      }
    )

    test('a mutant outside every rule keeps the area default and carries no witness list', () => {
      const rows = [
        [
          'src/protocol/encoder.js',
          'static sizeOfVarIntBytes(',
          '    if (size < 0) {',
          'boundary',
          'src/protocol/encoder.spec.js',
        ],
        [
          'src/consumer/share/shareRun.js',
          'finalizeActive(',
          '      if (active.next < active.records.length) release(active.cohort, active.next)',
          'boundary',
          `${SHARE}/shareConsumerClose.spec.js`,
        ],
        [
          'src/consumer/offsetManager/offsetOperations.js',
          'isSuperseded(',
          '    if (state.barrierAt > ticket.began || ownerBarrierAt > ticket.began) {',
          'boundary',
          `${OFFSETS}/offsetOperations.spec.js`,
        ],
        [
          'src/admin/deadline.js',
          'wait(',
          '        unown()',
          'removed-call',
          'src/admin/__tests__/apiDeadline.spec.js',
        ],
        [
          'src/admin/deadline.js',
          'async join(',
          '      this.running.catch(() => {})',
          'removed-call',
          'src/admin/__tests__/apiDeadline.spec.js',
        ],
        [
          'src/admin/deadline.js',
          'const shareMetadata',
          '          if (entries.get(key) === episode) entries.delete(key)',
          'negated-condition',
          'src/admin/__tests__/apiDeadline.spec.js',
        ],
        [
          'src/admin/deadline.js',
          'const waitForOf',
          '          unown()',
          'removed-call',
          'src/admin/__tests__/apiDeadline.spec.js',
        ],
        [
          'src/cluster/bootstrapResolution.js',
          'class Round',
          '    this.promise.catch(() => {})',
          'removed-call',
          'src/cluster/__tests__/bootstrapResolution.spec.js',
        ],
        [
          'src/cluster/bootstrapResolution.js',
          'attempt(',
          '      settle(error)',
          'removed-call',
          'src/cluster/__tests__/bootstrapResolution.spec.js',
        ],
        [
          'src/cluster/bootstrapResolution.js',
          'settle(',
          '    const resolved = !error && addresses?.length > 0',
          'boundary',
          'src/cluster/__tests__/bootstrapResolution.spec.js',
        ],
        [
          'src/cluster/bootstrapResolution.js',
          'expire(',
          "    const where = hosts.length > 0 ? hosts.join(', ') : 'the bootstrap list'",
          'boundary',
          'src/cluster/__tests__/bootstrapResolution.spec.js',
        ],
        [
          'src/producer/state/joinOrder.js',
          'enter(send)',
          "    if (typeof lanes === 'string') {",
          'negated-condition',
          'src/producer/state/__tests__/joinOrder.spec.js',
        ],
        [
          'src/protocol/message/compression/lz4.js',
          'const unavailable',
          'const unavailable = () => {',
          'early-return',
          'src/protocol/message/compression/lz4.spec.js',
        ],
        [
          'src/consumer/batch.js',
          'leaderEpochAt(offset)',
          '  leaderEpochAt(offset) {',
          'early-return',
          'src/consumer/__tests__/batch.spec.js',
        ],
        [
          'src/producer/state/transactionOffsets.js',
          'enlist(',
          '    if (held === undefined || acquireClock < held) this.inputs.set(acquisition, acquireClock)',
          'boundary',
          'src/producer/state/__tests__/transactionOffsets.spec.js',
        ],
      ]
      for (const [target, head, original, operator, spec] of rows) {
        const record = recordAt(target, head, original, operator)
        expect(record).toMatchObject({ target, operator, spec })
        expect(record).not.toHaveProperty('witnesses')
      }
    })

    test('no mutant has more than four witnesses, so a survivor costs at most four spec runs', () => {
      const counts = records.map((r) => witnessesOf(r).length)
      expect(Math.max(...counts)).toBeLessThanOrEqual(4)
      expect(counts.filter((n) => n > 1).length).toBeGreaterThan(32)
    })
  })
})
