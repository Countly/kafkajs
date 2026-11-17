/**
 * Report adapter of the `shareThroughput.js --session` rows: one session per report arm, which runs
 * every arm of its session eight times in each of two identical-tree repetitions and prints one
 * session record. Each arm shows the median and range of its seven retained observations of the
 * first repetition, and its `~AA` copy those of the second. A record that is incomplete or
 * contradicts itself is an `error`, a failed control makes the row `invalid`, a void A/A
 * comparison or an observation that formed no measurement window `inconclusive` (one row without
 * numbers when no observation formed one), and a session outside its own workflow job `omitted`.
 *
 * `node scripts/benchmarks/adapters/shareThroughput.js --compare <pre> <post>` compares the
 * `eachMessage` sessions of two runs (`bench-raw/shareThroughput-eachMessage/fork.json` of each)
 * and prints the per-record cost the change added and the smallest cost the runs can resolve.
 */
const assert = require('node:assert/strict')
const fs = require('node:fs')
const { lastJsonLine, tail } = require('../report')
const {
  LIMITS,
  SESSIONS,
  SESSION_EXIT,
  SESSION_BUDGET,
  evaluateSession,
  quantile,
} = require('../shareThroughput')

const HEX = /^[0-9a-f]{64}$/
const TOPIC = /^share-bench-[a-f0-9-]+$/
const SPREAD = 'A/A spread'

/**
 * @param {import('../report').ManifestEntry} entry
 * @returns {string} the session named after `--session`
 * @throws {Error} when the command names no known session
 */
const sessionOf = ({ command }) => {
  const name = command.includes('--session') ? command[command.indexOf('--session') + 1] : ''
  if (!Object.hasOwn(SESSIONS, name)) throw new Error('the command names no --session')
  return name
}

/**
 * Reads one session record. Exit 3 is a finished void or inconclusive session, read like exit 0.
 * @param {{status:number|null,signal?:string,error?:Error,stdout?:string,stderr?:string}} result
 * @returns {object} the session record
 * @throws {Error} for a spawn error, a signal, no session record, a session error, or an exit that
 *   disagrees with the session's status
 */
const read = (result) => {
  if (result.error) throw result.error
  if (result.status === null) throw new Error(`killed by ${result.signal}`)
  let record
  try {
    record = lastJsonLine(result.stdout ?? '')
  } catch {
    throw new Error(`exit ${result.status}: ${tail(result.stderr) || 'no session record'}`)
  }
  if (record.schema !== 1 || !Object.hasOwn(SESSION_EXIT, record.status)) {
    throw new Error(`exit ${result.status}: the last JSON line is not a session record`)
  }
  if (record.status === 'error') throw new Error(`session error: ${record.reason}`)
  if (SESSION_EXIT[record.status] !== result.status) {
    throw new Error(`exit ${result.status} disagrees with the ${record.status} session`)
  }
  return record
}

/**
 * @param {object[]} rows observations
 * @param {(row: object) => boolean} test
 * @returns {boolean} whether every observation passes
 */
const every = (rows, test) => rows.every(test)

/**
 * The controls of one arm over both repetitions.
 * @param {string} label the session arm
 * @param {object[]} rows its observations, both repetitions
 * @param {object} spec its driver flags
 * @param {object} corpus the session's corpus identity
 * @returns {{arm:string,name:string,passed:boolean,detail?:string}[]}
 */
const armControls = (label, rows, spec, corpus) => {
  const valid = rows.filter((row) => row.status === 'valid')
  const [first] = rows
  const share = spec.arm === 'share'
  const perMessage = spec.handler === 'eachMessage'
  const handlerPath = every(
    rows,
    ({ arm, handler, delivered }) =>
      arm === (share ? spec.acquireMode : 'ordinary') &&
      handler === spec.handler &&
      (perMessage
        ? delivered?.batches === delivered?.messages
        : delivered?.batches < delivered?.messages)
  )
  const traces = new Set(rows.map((row) => row.trace))
  const controls = [
    {
      name: 'driver controls held',
      passed:
        every(rows, (row) => ['valid', 'inconclusive'].includes(row.status)) &&
        every(valid, (row) => row.controls === true),
    },
    {
      name: 'delivered = corpus',
      passed: every(rows, (row) => row.delivered?.messages === corpus.messages),
      detail: `${corpus.messages} records in every observation`,
    },
    {
      name: 'handler path',
      passed: handlerPath,
      detail: perMessage
        ? `eachMessage: one call per record, ${first?.delivered?.batches} calls`
        : `eachBatch: ${first?.delivered?.batches} calls for ${first?.delivered?.messages} records`,
    },
    {
      name: 'trace archived',
      passed:
        every(rows, (row) => typeof row.trace === 'string' && HEX.test(row.traceHash ?? '')) &&
        traces.size === rows.length,
      detail: `${traces.size} distinct traces`,
    },
  ]
  if (share) {
    controls.push({
      name: 'measured window',
      passed: every(valid, (row) => row.records === LIMITS.measured),
      detail: `${LIMITS.measured} measured records`,
    })
  }
  return controls.map((control) => ({ arm: label, ...control }))
}

/**
 * The session-wide controls: guard, share read-back, corpus identity and the frame control.
 * @param {object} record the session record
 * @param {string} name the session
 * @returns {{arm:string,name:string,passed:boolean,detail?:string}[]}
 */
const sessionControls = (record, name) => {
  const { share, guard, corpus, frameControl } = record
  const controls = [
    {
      name: 'guard manifest',
      passed: HEX.test(guard?.sha256 ?? ''),
      detail: `${guard?.manifest}`,
    },
    {
      name: 'share enabled and read back',
      passed:
        share?.feature === 'share.version' && Number.isInteger(share.level) && share.level >= 1,
      detail: `share.version level ${share?.level}`,
    },
    {
      name: 'corpus identity',
      passed:
        TOPIC.test(corpus?.topic ?? '') &&
        corpus.messages === LIMITS.messages &&
        corpus.size === LIMITS.size &&
        corpus.partitions === LIMITS.partitions &&
        HEX.test(corpus.identities ?? ''),
      detail: `${corpus?.topic}: ${corpus?.messages} records of ${corpus?.size} bytes on ${corpus?.partitions} partitions, identities sha256 ${corpus?.identities}`,
    },
  ]
  if (SESSIONS[name].frameControl) {
    controls.push({
      name: 'frame positive control',
      passed: frameControl?.status === 'valid' && frameControl.fetchPer1000 >= LIMITS.frameFloor,
      detail: `fetchPer1000 ${frameControl?.fetchPer1000}, at least ${LIMITS.frameFloor}`,
    })
  }
  return controls.map((control) => ({ arm: 'session', ...control }))
}

/**
 * Checks that a session record is the requested session and agrees with itself.
 * @param {object} record
 * @param {string} name
 * @param {string[]} labels
 * @returns {string|undefined} why the record cannot be shown, if it cannot
 */
const malformed = (record, name, labels) => {
  if (record.session !== name) return `the record is session ${record.session}, expected ${name}`
  const found = Object.keys(record.arms ?? {}).sort()
  if (found.join() !== labels.join()) {
    return `the session has arms ${found.join(', ')}, expected ${labels.join(', ')}`
  }
  for (const label of labels) {
    for (const rep of ['a', 'aprime']) {
      const rows = record.arms[label][rep]
      if (!Array.isArray(rows) || rows.length !== LIMITS.observations) {
        return `${label} ${rep} has ${rows?.length} observations, expected ${LIMITS.observations}`
      }
    }
  }
  if (record.status === 'inconclusive') return undefined
  let evaluation
  try {
    evaluation = evaluateSession({ arms: record.arms }, null, labels)
  } catch (error) {
    return `the observations cannot be evaluated: ${error.message}`
  }
  if (
    evaluation.status !== record.status ||
    JSON.stringify(evaluation) !== JSON.stringify(record.evaluation)
  ) {
    return 'the session evaluation disagrees with its observations'
  }
  return undefined
}

/**
 * @param {object[]} rows one repetition of an arm
 * @param {string} key metric
 * @returns {{median:number,min:number,max:number,runs:number}|null} the stats of the retained valid
 *   observations, or null without one
 */
const statsOf = (rows, key) => {
  const values = rows
    .slice(1)
    .filter((row) => row.status === 'valid')
    .map((row) => row[key])
  if (values.length === 0 || !values.every(Number.isFinite)) return null
  return {
    median: quantile(values, 0.5),
    min: Math.min(...values),
    max: Math.max(...values),
    runs: values.length,
  }
}

/**
 * @param {{a:object[],aprime:object[]}} arm one arm of a session record
 * @returns {{ns:number,spread:number}|null} the median nanoseconds per record of the first
 *   repetition and how far the two repetitions' throughput medians differ (a fraction), or null
 *   without valid observations in both
 */
const perRecord = (arm) => {
  const a = statsOf(arm.a, 'recordsPerSecond')
  const b = statsOf(arm.aprime, 'recordsPerSecond')
  return a && b ? { ns: 1e9 / a.median, spread: Math.abs(a.median / b.median - 1) } : null
}

/**
 * The resolution of one eachMessage session: a per-message cost below its A/A spread of the
 * per-record time cannot be told from noise.
 * @param {object} record an eachMessage session record
 * @returns {string|undefined} the note, without valid observations in both arms none
 */
const resolutionNote = (record) => {
  const message = perRecord(record.arms.eachMessage)
  const control = perRecord(record.arms.record_limit)
  if (!message || !control) return undefined
  const spread = Math.max(message.spread, control.spread)
  return `eachMessage ${message.ns.toFixed(2)} ns per record, A/A spread ${(spread * 100).toFixed(2)} %: the smallest per-message cost this session resolves is ${(spread * message.ns).toFixed(2)} ns`
}

/**
 * Compares the eachMessage sessions of a run before (PRE) and after (POST) a change. The runs are
 * comparable only with the same driver digest and Node. The PRE per-record time is first scaled
 * by how far the eachBatch control moved between the runs, so a slower runner is not read as a
 * cost. The noise is the largest A/A spread of the four arms; a difference within it of the
 * per-record time is not resolved.
 * @param {object} pre PRE eachMessage session record
 * @param {object} post POST eachMessage session record
 * @returns {{comparable:boolean,perRecordNs:{pre:number,post:number},deltaNs:number,
 *   controlDrift:number,adjustedDeltaNs:number,noise:number,resolutionNs:number,
 *   resolved:boolean}}
 * @throws {Error} unless both records have valid observations in both arms
 */
const compareEachMessage = (pre, post) => {
  const [message, after, control, controlAfter] = [
    pre.arms?.eachMessage,
    post.arms?.eachMessage,
    pre.arms?.record_limit,
    post.arms?.record_limit,
  ].map((arm) => arm && perRecord(arm))
  assert(message && after && control && controlAfter, 'both runs need valid eachMessage sessions')
  const noise = Math.max(message.spread, after.spread, control.spread, controlAfter.spread)
  const controlDrift = controlAfter.ns / control.ns - 1
  const adjustedDeltaNs = after.ns - message.ns * (1 + controlDrift)
  const resolutionNs = noise * message.ns
  const comparable = pre.driver?.sha256 === post.driver?.sha256 && pre.node === post.node
  return {
    comparable,
    perRecordNs: { pre: message.ns, post: after.ns },
    deltaNs: after.ns - message.ns,
    controlDrift,
    adjustedDeltaNs,
    noise,
    resolutionNs,
    resolved: comparable && Math.abs(adjustedDeltaNs) > resolutionNs,
  }
}

/**
 * Turns one session record into the row's lines and details.
 * @param {import('../report').ManifestEntry} entry
 * @param {string} arm the report's arm, `fork`
 * @param {import('../report').Outcome} outcome
 * @returns {{ rows: import('../report').Row[], detail?: import('../report').Detail }}
 */
const rows = (entry, arm, { outputs, error }) => {
  const one = (status, reason, runs = outputs.length) => ({
    rows: [{ benchmark: entry.name, arm: '-', metric: '-', unit: '-', runs, status, reason }],
  })
  if (error) return one('error', error)
  if (outputs[0].status === 'omitted') return one('omitted', outputs[0].reason)
  let name
  try {
    name = sessionOf(entry)
  } catch (failure) {
    return one('error', failure.message)
  }
  const [record] = outputs
  const { arms } = SESSIONS[name]
  const labels = Object.keys(arms).sort()
  const problem = malformed(record, name, labels)
  if (problem) return one('error', problem)

  const voided = record.evaluation?.status === 'void' ? record.evaluation : null
  const controls = [
    ...sessionControls(record, name),
    ...labels.flatMap((label) => [
      ...armControls(
        label,
        [...record.arms[label].a, ...record.arms[label].aprime],
        arms[label],
        record.corpus
      ),
      {
        arm: label,
        name: SPREAD,
        passed: voided?.arm !== label,
        detail:
          voided?.arm === label
            ? `${voided.metric} ${(voided.delta * 100).toFixed(2)} % between repetitions`
            : `within ${LIMITS.aa * 100} %`,
      },
    ]),
  ]
  const failed = controls.filter(({ name: control, passed }) => !passed && control !== SPREAD)
  const notes = [
    `driver sha256 ${record.driver?.sha256}`,
    `node ${record.node}`,
    `guard ${record.guard?.manifest}`,
    'every observation, its trace and its child output are in the raw evidence',
    ...(name === 'eachMessage' ? [resolutionNote(record)].filter(Boolean) : []),
  ]
  let status = 'valid'
  let reason
  if (failed.length) {
    status = 'invalid'
    reason = `failed controls: ${failed.map(({ arm: label, name: control }) => `${label} ${control}`).join('; ')}`
  } else if (voided) {
    status = 'inconclusive'
    reason = `the A/A control is void: ${voided.arm} ${voided.metric} differs by ${(voided.delta * 100).toFixed(2)} % between repetitions`
  } else if (record.status === 'inconclusive') {
    status = 'inconclusive'
    reason = record.reason
  }
  const detail = { status, ...(reason && { reason }), controls, notes }
  if (status === 'invalid') return { ...one(status, reason, 1), detail }
  const lines = labels.flatMap((label) =>
    [
      [label, record.arms[label].a],
      [`${label}~AA`, record.arms[label].aprime],
    ].flatMap(([shown, series]) =>
      entry.metrics.flatMap(({ key, unit }) => {
        const stats = statsOf(series, key)
        return stats
          ? [{ benchmark: entry.name, arm: shown, metric: key, unit, status, ...stats }]
          : []
      })
    )
  )
  if (lines.length === 0 && status === 'inconclusive') return { ...one(status, reason), detail }
  if (lines.length === 0) return { ...one('error', `no valid observation: ${reason}`), detail }
  return { rows: lines, detail }
}

/**
 * @param {string} file a run's `fork.json` evidence, or a session record
 * @returns {object} the session record
 */
const sessionFrom = (file) => {
  const value = JSON.parse(fs.readFileSync(file, 'utf8'))
  return Array.isArray(value.runs) ? lastJsonLine(value.runs[0].stdout) : value
}

/**
 * One session per report arm, which holds every observation of the session; the row's limit is the
 * session budget plus ten minutes for its admission and its record.
 */
module.exports = {
  runs: 1,
  timeoutMs: SESSION_BUDGET.totalMs + 10 * 60000,
  read,
  rows,
  compareEachMessage,
}

if (require.main === module) {
  const [flag, pre, post] = process.argv.slice(2)
  if (flag !== '--compare' || !pre || !post) {
    console.error('usage: shareThroughput.js --compare <pre fork.json> <post fork.json>')
    process.exitCode = 2
  } else {
    console.log(JSON.stringify(compareEachMessage(sessionFrom(pre), sessionFrom(post)), null, 2))
  }
}
