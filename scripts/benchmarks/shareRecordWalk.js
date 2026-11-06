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
 * Delivery counts alternate between the 1 and 32767 boundaries so a wrapped, truncated or
 * misaligned count cannot pass.
 * @param {number} count Number of acquired offsets, divisible by 500.
 * @returns {object} Input plus independently calculated expected record identity and count per delivered offset.
 * @throws {Error} For an unsupported generator size.
 */
const generate = (count) => {
  assert(count > 0 && count % 500 === 0)
  const acquiredRecords = []
  const messages = []
  const expected = new Map()
  for (let range = 0; range < count / 500; range++) {
    const first = range * 503
    const deliveryCount = range % 2 ? 32767 : 1
    acquiredRecords.push({
      firstOffset: String(first),
      lastOffset: String(first + 499),
      deliveryCount,
    })
    for (let i = 0; i < 250; i++) {
      const record = { offset: String(first + i) }
      messages.push(record)
      expected.set(record.offset, { record, deliveryCount })
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
 * Its deliveries use the same compact contract as the walk; only the per-offset ownership is extra.
 * @param {object} input Generated complete response.
 * @returns {object} Same semantic outputs plus retained expanded ownership for the control.
 */
const expansionControl = (input) => {
  const expanded = new Map()
  const records = new Map(input.messages.map((record) => [record.offset, record]))
  const delivered = []
  const counts = []
  const gaps = []
  for (const range of input.acquiredRecords)
    for (let n = BigInt(range.firstOffset); n <= BigInt(range.lastOffset); n++) {
      const offset = String(n)
      const record = records.get(offset)
      expanded.set(offset, { deliveryCount: range.deliveryCount, record })
      if (record) {
        delivered.push(record)
        counts.push(range.deliveryCount)
      } else gaps.push({ firstOffset: offset, lastOffset: offset })
    }
  const deliver = { records: delivered, deliveryCounts: Uint16Array.from(counts) }
  return { deliver, gaps, release: [], reject: [], expanded }
}

/**
 * Independently checks the compact delivery arrays (shape, equal length, exact backing buffer,
 * original record identity and the count paired with each record), and exact missing-offset gaps.
 * @param {object} output Arm result; arm-reported counters are never evidence.
 * @param {object} input Generator's independent expected identities/counts.
 * @returns {void} Rejects skipped, omitted, duplicate, misaligned or fabricated work before baseline admission.
 * @throws {AssertionError} For any semantic mismatch.
 */
const verify = (output, input) => {
  const { records, deliveryCounts } = output.deliver
  assert(Array.isArray(records), 'delivered records must be an array')
  assert(deliveryCounts instanceof Uint16Array, 'delivery counts must be a Uint16Array')
  assert.equal(deliveryCounts.length, records.length, 'delivery arrays are misaligned')
  assert(
    deliveryCounts.byteOffset === 0 &&
      deliveryCounts.buffer.byteLength === deliveryCounts.byteLength,
    'delivery counts retain unused capacity'
  )
  const actual = new Set()
  records.forEach((record, i) => {
    const expected = input.expected.get(record.offset)
    assert(expected, 'delivered offset was not generated')
    assert.equal(record, expected.record, 'delivered record is not the original')
    assert.equal(
      deliveryCounts[i],
      expected.deliveryCount,
      'delivery count does not match its record'
    )
    assert(!actual.has(record.offset), 'delivered offset repeated')
    actual.add(record.offset)
  })
  assert.equal(actual.size, input.delivered, 'delivered offsets differ from the generated corpus')
  let gapCount = 0n
  let prior = -1n
  for (const range of output.gaps) {
    const first = BigInt(range.firstOffset)
    const last = BigInt(range.lastOffset)
    assert(first > prior && last >= first, 'gaps must ascend without overlap')
    const low = Number(first)
    const high = Number(last)
    assert(
      low >= 0 &&
        high <= Number(input.acquiredRecords[input.acquiredRecords.length - 1].lastOffset),
      'gap lies outside the acquired span'
    )
    assert(Math.floor(low / 503) === Math.floor(high / 503), 'gap crosses an unacquired offset')
    assert(low % 503 >= 250 && high % 503 <= 499, 'gap covers a delivered offset')
    gapCount += last - first + 1n
    prior = last
  }
  assert.equal(gapCount, BigInt(input.gaps))
  assert.equal(output.reject.length, 0)
  assert.equal(output.release.length, 0)
}

const OUTPUT_SIZE = 'delivered offsets differ from the generated corpus'
const MISALIGNED = 'delivery arrays are misaligned'
const WRONG_COUNT = 'delivery count does not match its record'
const withoutFirst = ({ records, deliveryCounts }) => ({
  records: records.slice(1),
  deliveryCounts: deliveryCounts.slice(1),
})
const replaced = (deliver, edit) => {
  const records = deliver.records.slice()
  const deliveryCounts = deliver.deliveryCounts.slice()
  edit(records, deliveryCounts)
  return { records, deliveryCounts }
}

/**
 * Corrupt walk outputs that `verify` must reject, each for its stated reason. Every corruption
 * keeps the rest of the baseline intact, so the named defect is the only possible cause.
 * @type {{label:string,message:string,corrupt:(baseline:object,input:object)=>object}[]}
 */
const DELIVERY_CORRUPTIONS = [
  {
    label: 'all deliveries dropped',
    message: OUTPUT_SIZE,
    corrupt: (baseline) => ({
      ...baseline,
      deliver: { records: [], deliveryCounts: new Uint16Array(0) },
    }),
  },
  {
    label: 'the first delivery dropped from both arrays',
    message: OUTPUT_SIZE,
    corrupt: (baseline) => ({ ...baseline, deliver: withoutFirst(baseline.deliver) }),
  },
  {
    label: 'the first delivery repeated at the end',
    message: 'delivered offset repeated',
    corrupt: (baseline) => {
      const { records, deliveryCounts } = baseline.deliver
      const counts = new Uint16Array(deliveryCounts.length + 1)
      counts.set(deliveryCounts)
      counts[deliveryCounts.length] = deliveryCounts[0]
      return {
        ...baseline,
        deliver: { records: records.concat(records[0]), deliveryCounts: counts },
      }
    },
  },
  {
    label: 'a dropped delivery with fabricated totals',
    message: OUTPUT_SIZE,
    corrupt: (baseline, input) => ({
      ...baseline,
      deliver: withoutFirst(baseline.deliver),
      delivered: input.delivered,
      gapCount: input.gaps,
    }),
  },
  {
    label: 'a record dropped while its count is kept',
    message: MISALIGNED,
    corrupt: (baseline) => ({
      ...baseline,
      deliver: { ...baseline.deliver, records: baseline.deliver.records.slice(1) },
    }),
  },
  {
    label: 'a count dropped while its record is kept',
    message: MISALIGNED,
    corrupt: (baseline) => ({
      ...baseline,
      deliver: { ...baseline.deliver, deliveryCounts: baseline.deliver.deliveryCounts.slice(1) },
    }),
  },
  {
    label: 'one count replaced by a wrong value at equal length',
    message: WRONG_COUNT,
    corrupt: (baseline) => ({
      ...baseline,
      deliver: replaced(baseline.deliver, (_records, counts) => {
        counts[0] = counts[0] + 1
      }),
    }),
  },
  {
    label: 'the first and last counts swapped at equal length',
    message: WRONG_COUNT,
    corrupt: (baseline) => ({
      ...baseline,
      deliver: replaced(baseline.deliver, (_records, counts) => {
        const last = counts.length - 1
        const first = counts[0]
        counts[0] = counts[last]
        counts[last] = first
      }),
    }),
  },
  {
    label: 'counts held in a plain array',
    message: 'delivery counts must be a Uint16Array',
    corrupt: (baseline) => ({
      ...baseline,
      deliver: { ...baseline.deliver, deliveryCounts: Array.from(baseline.deliver.deliveryCounts) },
    }),
  },
  {
    label: 'counts viewed over a larger retained buffer',
    message: 'delivery counts retain unused capacity',
    corrupt: (baseline) => {
      const { deliveryCounts } = baseline.deliver
      const backing = new Uint16Array(deliveryCounts.length + 8)
      backing.set(deliveryCounts)
      return {
        ...baseline,
        deliver: {
          ...baseline.deliver,
          deliveryCounts: backing.subarray(0, deliveryCounts.length),
        },
      }
    },
  },
  {
    label: 'one wrapper object per delivery',
    message: 'delivered records must be an array',
    corrupt: (baseline) => ({
      ...baseline,
      deliver: baseline.deliver.records.map((record, i) => ({
        record,
        deliveryCount: baseline.deliver.deliveryCounts[i],
      })),
    }),
  },
  {
    label: 'the first record replaced by a clone',
    message: 'delivered record is not the original',
    corrupt: (baseline) => ({
      ...baseline,
      deliver: replaced(baseline.deliver, (records) => {
        records[0] = { ...records[0] }
      }),
    }),
  },
  {
    label: 'the first record replaced by one at an absent offset',
    message: 'delivered offset was not generated',
    corrupt: (baseline) => ({
      ...baseline,
      deliver: replaced(baseline.deliver, (records) => {
        records[0] = { offset: '250' }
      }),
    }),
  },
]

/**
 * Runs untimed positive and corrupt-output controls, including fabricated counters ignored by verify.
 * @param {object} input Fixed generated corpus.
 * @returns {object} Explicit admitted arm/control counts; a control that is not rejected for its stated reason fails the process.
 * @throws {AssertionError} When an arm is wrong or any corrupt output is admitted.
 */
const controls = (input) => {
  for (const arm of [ShareRecordWalk.walk, expansionControl]) verify(arm(input), input)
  const baseline = ShareRecordWalk.walk(input)
  for (const { message, corrupt } of DELIVERY_CORRUPTIONS)
    assert.throws(() => verify(corrupt(baseline, input), input), {
      name: 'AssertionError',
      message,
    })
  return { validArms: 2, rejectedControls: DELIVERY_CORRUPTIONS.length }
}

/** @param {number[]} values Observations. @returns {number} Median without choosing favorable samples. */
const median = (values) => [...values].sort((a, b) => a - b)[Math.floor(values.length / 2)]

/**
 * Measures the complete retained arm output: every array, range and backing buffer plus the
 * control's ownership. The fixture and the whole output stay referenced through the after-GC
 * snapshot. Heap H and array-buffer bytes B are nonnegative deltas taken separately; T = H + B
 * (`external` is not added again because it already includes array buffers). Ledger
 * construction/encoding/settlement is timed separately, outside the gated walk interval.
 * @param {object} input Fixed generated corpus.
 * @param {Function} arm Walk or expansion control.
 * @returns {object} ns/acquired offset, retained H/B/T, delivery count, bytes per delivery and ledger cost.
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
  const retainedHeap = Math.max(0, after.heapUsed - before.heapUsed)
  const retainedBuffers = Math.max(0, after.arrayBuffers - before.arrayBuffers)
  const retainedTotal = retainedHeap + retainedBuffers
  const deliveries = output.deliver.records.length
  return {
    nsPerOffset: (elapsed * 1e6) / input.acquired,
    retainedHeap,
    retainedBuffers,
    retainedTotal,
    deliveries,
    bytesPerDelivery: retainedTotal / deliveries,
    ledgerMs,
  }
}

const OBSERVED = [
  'nsPerOffset',
  'retainedHeap',
  'retainedBuffers',
  'retainedTotal',
  'deliveries',
  'bytesPerDelivery',
  'ledgerMs',
]

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
      OBSERVED.map((key) => [key, median(rows.map((row) => row[key]))])
    )
  return { observations, medians }
}

/**
 * Applies the fixed gates to each session's medians: walk time at most 1.5x control, and heap H
 * and total T = H + B each at most 0.15x control and, at 1M offsets, at most 10 MiB. T is gated
 * separately so moving counts off-heap cannot satisfy the heap gate by exclusion.
 * @param {{medians:{walk:object,control:object}}[]} sessions Valid sessions.
 * @param {number} offsets Acquired offsets in the corpus.
 * @returns {string[]} One reason per missed gate per session; empty when every gate holds.
 */
const failedGates = (sessions, offsets) => {
  const limit = 10 * 1024 * 1024
  const failed = []
  for (const { medians } of sessions) {
    const { walk, control } = medians
    if (walk.nsPerOffset > 1.5 * control.nsPerOffset) failed.push('walk time exceeds control bound')
    if (walk.retainedHeap > 0.15 * control.retainedHeap)
      failed.push('walk heap exceeds control ratio')
    if (offsets === 1000000 && walk.retainedHeap > limit) failed.push('walk heap exceeds ten MiB')
    if (walk.retainedTotal > 0.15 * control.retainedTotal)
      failed.push('walk total exceeds control ratio')
    if (offsets === 1000000 && walk.retainedTotal > limit) failed.push('walk total exceeds ten MiB')
  }
  return failed
}

/**
 * Executes controls before two identical-tree sessions; applies fixed walk, heap, total-memory and
 * A/A gates (A/A covers time, heap and total).
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
    cpuModel: os.cpus()[0]?.model,
    v8: process.versions.v8,
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
    for (const key of ['nsPerOffset', 'retainedHeap', 'retainedTotal']) {
      const a = first.medians[name][key]
      const b = second.medians[name][key]
      if (!a || !b || Math.abs(a - b) / Math.min(a, b) > 0.03) {
        result.status = 'void'
        console.log(JSON.stringify(result))
        throw new Error('A/A exceeds three percent or has no measurable denominator')
      }
    }
  const failed = failedGates([first, second], offsets)
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
module.exports = {
  parseArgs,
  generate,
  expansionControl,
  verify,
  controls,
  DELIVERY_CORRUPTIONS,
  observe,
  failedGates,
  run,
}
