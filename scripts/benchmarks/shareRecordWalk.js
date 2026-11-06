const os = require('os')
const assert = require('assert/strict')
const { performance } = require('perf_hooks')
const { ShareRecordWalk } = require('../../src/consumer/share/shareRecordWalk')
const { ShareAckLedger } = require('../../src/consumer/share/shareAckLedger')

/**
 * Parses the internal-API walk benchmark CLI; no options have implicit values.
 * @param {string[]} args Alternating --offsets/--control flags and values, each once.
 * @returns {{offsets:number,control:number}} Validated fixed workload and untimed/timed selection.
 * @throws {Error} For missing, duplicate, unknown or invalid options.
 */
const parseArgs = (args) => {
  const options = {}
  for (let i = 0; i < args.length; i += 2) {
    const key = args[i]
    const value = args[i + 1]
    if (
      !['--offsets', '--control'].includes(key) ||
      Object.hasOwn(options, key) ||
      value === undefined
    )
      throw new Error('Expected --offsets 100000|1000000 --control 0|1')
    options[key] = value
  }
  if (
    !['100000', '1000000'].includes(options['--offsets']) ||
    !['0', '1'].includes(options['--control'])
  )
    throw new Error('Invalid offsets or control value')
  return { offsets: Number(options['--offsets']), control: Number(options['--control']) }
}

/**
 * Generates 500-wide acquired ranges separated by three unacquired offsets; half are absent records.
 * @param {number} count Number of acquired offsets, divisible by 500.
 * @returns {object} Input and independently calculated delivered identities and counts.
 * @throws {Error} For an unsupported generator size.
 */
const generate = (count) => {
  assert(count > 0 && count % 500 === 0)
  const acquiredRecords = []
  const messages = []
  const expected = new Set()
  for (let range = 0; range < count / 500; range++) {
    const first = range * 503
    acquiredRecords.push({
      firstOffset: String(first),
      lastOffset: String(first + 499),
      deliveryCount: 1,
    })
    for (let i = 0; i < 250; i++) {
      const offset = String(first + i)
      messages.push({ offset })
      expected.add(offset)
    }
  }
  return {
    acquiredRecords,
    messages,
    expected,
    acquired: count,
    delivered: count / 2,
    gaps: count / 2,
  }
}

/**
 * Deliberately expands ownership per offset as the same-tree old-algorithm control, never production.
 * @param {object} input Generated complete response.
 * @returns {object} Same semantic outputs plus retained expanded ownership for the control.
 */
const expansionControl = (input) => {
  const expanded = new Map()
  const records = new Map(input.messages.map((record) => [record.offset, record]))
  const deliver = []
  const gaps = []
  for (const range of input.acquiredRecords)
    for (let n = BigInt(range.firstOffset); n <= BigInt(range.lastOffset); n++) {
      const offset = String(n)
      const record = records.get(offset)
      expanded.set(offset, { deliveryCount: range.deliveryCount, record })
      if (record) deliver.push({ record, deliveryCount: range.deliveryCount })
      else gaps.push({ firstOffset: offset, lastOffset: offset })
    }
  return { deliver, gaps, release: [], reject: [], expanded }
}

/**
 * Independently checks count and complete delivery identities, and exact missing-offset gaps.
 * @param {object} output Arm result; arm-reported counters are never evidence.
 * @param {object} input Generator's independent expected identities/counts.
 * @returns {void} Rejects skipped, omitted, duplicate or fabricated work before baseline admission.
 * @throws {AssertionError} For any semantic mismatch.
 */
const verify = (output, input) => {
  const actual = new Set()
  for (const { record } of output.deliver) {
    assert(input.expected.has(record.offset))
    assert(!actual.has(record.offset))
    actual.add(record.offset)
  }
  assert.equal(actual.size, input.delivered)
  assert.deepEqual(actual, input.expected)
  let gapCount = 0n
  let prior = -1n
  for (const range of output.gaps) {
    const first = BigInt(range.firstOffset)
    const last = BigInt(range.lastOffset)
    assert(first > prior && last >= first)
    const low = Number(first)
    const high = Number(last)
    assert(
      low >= 0 && high <= Number(input.acquiredRecords[input.acquiredRecords.length - 1].lastOffset)
    )
    assert(Math.floor(low / 503) === Math.floor(high / 503))
    assert(low % 503 >= 250 && high % 503 <= 499)
    gapCount += last - first + 1n
    prior = last
  }
  assert.equal(gapCount, BigInt(input.gaps))
  assert.equal(output.reject.length, 0)
  assert.equal(output.release.length, 0)
}

/**
 * Runs untimed positive and work-skipping controls, including fabricated counters ignored by verify.
 * @param {object} input Fixed generated corpus.
 * @returns {object} Explicit admitted arm/control counts; a false control fails the process.
 * @throws {AssertionError} When an arm is wrong or any corrupt output is admitted.
 */
const controls = (input) => {
  for (const arm of [ShareRecordWalk.walk, expansionControl]) verify(arm(input), input)
  const baseline = ShareRecordWalk.walk(input)
  assert.throws(() => verify({ ...baseline, deliver: [] }, input))
  assert.throws(() => verify({ ...baseline, deliver: baseline.deliver.slice(1) }, input))
  assert.throws(() =>
    verify({ ...baseline, deliver: baseline.deliver.concat(baseline.deliver[0]) }, input)
  )
  assert.throws(() =>
    verify(
      {
        ...baseline,
        deliver: baseline.deliver.slice(1),
        delivered: input.delivered,
        gapCount: input.gaps,
      },
      input
    )
  )
  return { validArms: 2, rejectedControls: 4 }
}

/** @param {number[]} values Observations. @returns {number} Median without choosing favorable samples. */
const median = (values) => [...values].sort((a, b) => a - b)[Math.floor(values.length / 2)]

/**
 * Measures complete retained walk output, including delivered wrappers and control ownership.
 * Also reports ledger construction/encoding/settlement separately, outside the gated walk interval.
 * @param {object} input Fixed generated corpus.
 * @param {Function} arm Walk or expansion control.
 * @returns {object} ns/acquired offset, retained heap/array buffers and ledger cost/size.
 * @throws {Error} For missing explicit GC or incorrect arm semantics.
 */
const observe = (input, arm) => {
  if (!global.gc) throw new Error('Run the internal-API benchmark with node --expose-gc')
  global.gc()
  const before = process.memoryUsage()
  const started = performance.now()
  const output = arm(input)
  const elapsed = performance.now() - started
  verify(output, input)
  global.gc()
  const after = process.memoryUsage()
  const token = Symbol('acquisition')
  const ledgerStart = performance.now()
  const ledger = new ShareAckLedger({
    topic: 'walk',
    topicId: '11111111-1111-4111-8111-111111111111',
    partition: 0,
    token,
    sentAt: 0,
    lockDuration: 30000,
    walk: output,
  })
  const { ticket } = ledger.prepare({ token })
  assert(ticket)
  ledger.settle(ticket, { token })
  ledger.invalidate('not-sent')
  const ledgerMs = performance.now() - ledgerStart
  assert.equal(ledger.size, 0)
  return {
    nsPerOffset: (elapsed * 1e6) / input.acquired,
    retainedHeap: Math.max(0, after.heapUsed - before.heapUsed),
    retainedBuffers: Math.max(0, after.arrayBuffers - before.arrayBuffers),
    ledgerMs,
  }
}

/**
 * Runs one discarded warmup per arm then seven measured observations in balanced interleaving.
 * @param {object} input Fixed corpus.
 * @returns {object} All raw observations and per-arm medians; no baseline is silently persisted.
 */
const session = (input) => {
  const arms = { walk: ShareRecordWalk.walk, control: expansionControl }
  const observations = { walk: [], control: [] }
  for (const arm of Object.values(arms)) observe(input, arm)
  for (let round = 0; round < 7; round++)
    for (const name of round % 2 ? ['control', 'walk'] : ['walk', 'control'])
      observations[name].push(observe(input, arms[name]))
  const medians = {}
  for (const [name, rows] of Object.entries(observations))
    medians[name] = Object.fromEntries(
      ['nsPerOffset', 'retainedHeap', 'retainedBuffers', 'ledgerMs'].map((key) => [
        key,
        median(rows.map((row) => row[key])),
      ])
    )
  return { observations, medians }
}

/**
 * Executes controls before two identical-tree sessions; applies fixed walk, memory and A/A gates.
 * @param {string[]} args CLI arguments.
 * @returns {object} Serializable evidence with host load, all observations and acceptance status.
 * @throws {Error} For invalid CLI, failed controls, void A/A or failed fixed gates.
 */
const run = (args) => {
  const { offsets, control } = parseArgs(args)
  const input = generate(offsets)
  const host = {
    node: process.version,
    hostname: os.hostname(),
    cpus: os.cpus().length,
    loadBefore: os.loadavg(),
    started: new Date().toISOString(),
  }
  const correctness = controls(input)
  if (control) return { internalAPI: true, host, offsets, correctness, timed: false }
  const first = session(input)
  const second = session(input)
  const result = {
    internalAPI: true,
    host: { ...host, loadAfter: os.loadavg() },
    offsets,
    correctness,
    first,
    second,
    timed: true,
  }
  for (const name of ['walk', 'control'])
    for (const key of ['nsPerOffset', 'retainedHeap']) {
      const a = first.medians[name][key]
      const b = second.medians[name][key]
      if (!a || !b || Math.abs(a - b) / Math.min(a, b) > 0.03) {
        result.status = 'void'
        console.log(JSON.stringify(result))
        throw new Error('A/A exceeds three percent or has no measurable denominator')
      }
    }
  const failed = []
  for (const data of [first, second]) {
    if (data.medians.walk.nsPerOffset > 1.5 * data.medians.control.nsPerOffset)
      failed.push('walk time exceeds control bound')
    if (data.medians.walk.retainedHeap > 0.15 * data.medians.control.retainedHeap)
      failed.push('walk heap exceeds control ratio')
    if (offsets === 1000000 && data.medians.walk.retainedHeap > 10 * 1024 * 1024)
      failed.push('walk heap exceeds ten MiB')
  }
  return {
    ...result,
    failed,
    status: failed.length
      ? 'failed'
      : 'candidate; compare both sizes and first accepted baseline ceilings',
  }
}

if (require.main === module) {
  try {
    const result = run(process.argv.slice(2))
    console.log(JSON.stringify(result))
    if (result.status === 'failed') process.exitCode = 1
  } catch (error) {
    console.error(error.message)
    process.exitCode = 1
  }
}
module.exports = { parseArgs, generate, expansionControl, verify, controls, run }
