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
 * `eachMessage` sessions of two runs (`bench-raw/shareThroughput-eachMessage/fork.json` of each):
 * it refuses unless both are complete accepted sessions of the same driver and Node, and prints the
 * observed per-record difference with its A/A spread and scatter. Those are repeatability figures,
 * not a detection limit, so the per-message context-entry cost stays owed to an invocation-path
 * measurement.
 */
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
const NODE = /^v\d+\.\d+\.\d+$/
const OWED = 'the per-message context-entry cost is owed to an invocation-path measurement'
/** The eachMessage row as the comparison checks a record against it. */
const EACH_MESSAGE = Object.freeze({
  name: 'shareThroughput-eachMessage',
  command: ['--session', 'eachMessage'],
  metrics: [{ key: 'recordsPerSecond', unit: 'records/s' }],
})

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
 * @returns {{ns:number,spread:number,scatter:number}|null} the median nanoseconds per record of the
 *   first repetition, how far the two repetitions' throughput medians differ and the larger range
 *   of their retained observations (both fractions of a median), or null without valid
 *   observations in both
 */
const perRecord = (arm) => {
  const a = statsOf(arm.a, 'recordsPerSecond')
  const b = statsOf(arm.aprime, 'recordsPerSecond')
  if (!a || !b) return null
  return {
    ns: 1e9 / a.median,
    spread: Math.abs(a.median / b.median - 1),
    scatter: Math.max(...[a, b].map(({ min, max, median }) => (max - min) / median)),
  }
}

/** @param {number} fraction @returns {string} the fraction in percent, two decimals */
const percent = (fraction) => (fraction * 100).toFixed(2)

/**
 * The repeatability of one eachMessage session. Its A/A spread and scatter describe these runs;
 * they are no detection limit, so the per-message cost stays owed.
 * @param {object} record an eachMessage session record
 * @returns {string|undefined} the note, without valid observations in both arms none
 */
const repeatabilityNote = (record) => {
  const message = perRecord(record.arms.eachMessage)
  const control = perRecord(record.arms.record_limit)
  if (!message || !control) return undefined
  const spread = percent(Math.max(message.spread, control.spread))
  const scatter = percent(Math.max(message.scatter, control.scatter))
  return `eachMessage ${message.ns.toFixed(2)} ns per record; observed A/A spread ${spread} % and retained scatter ${scatter} % (repeatability, not a detection limit); ${OWED}`
}

/**
 * Runs `read` and prefixes its failure with the side of the comparison.
 * @template T
 * @param {string} side `PRE` or `POST`
 * @param {() => T} read
 * @returns {T}
 * @throws {Error} `read`'s failure, prefixed
 */
const sided = (side, read) => {
  try {
    return read()
  } catch (error) {
    throw new Error(`${side}: ${error.message}`)
  }
}

/**
 * Checks that a record can enter a comparison: an accepted eachMessage session with its driver
 * digest and Node, whose row is valid (every control of the adapter passes) and whose every
 * observation has a positive finite throughput.
 * @param {object} record an eachMessage session record
 * @returns {void}
 * @throws {Error} naming the first thing that is missing or wrong
 */
const checkComparable = (record) => {
  if (record?.schema !== 1 || record.session !== 'eachMessage') {
    throw new Error('not an eachMessage session record')
  }
  if (record.status !== 'accepted') {
    throw new Error(`the session is ${record.status}${record.reason ? `: ${record.reason}` : ''}`)
  }
  if (!HEX.test(record.driver?.sha256 ?? '')) throw new Error('no driver sha256')
  if (!NODE.test(record.node ?? '')) throw new Error('no Node version')
  const shown = rows(EACH_MESSAGE, 'fork', { outputs: [record] })
  const { status, reason } = shown.detail ?? shown.rows[0]
  if (status !== 'valid') throw new Error(reason)
  for (const [label, arm] of Object.entries(record.arms))
    for (const rep of ['a', 'aprime'])
      arm[rep].forEach(({ recordsPerSecond }, index) => {
        if (!(Number.isFinite(recordsPerSecond) && recordsPerSecond > 0)) {
          throw new Error(`${label} ${rep} ${index} has no positive throughput`)
        }
      })
}

/**
 * Compares the eachMessage sessions of a run before (PRE) and after (POST) a change. The PRE
 * per-record time is first scaled by how far the eachBatch control moved between the runs, so a
 * slower runner is not read as a cost. The result is always `inconclusive`: the A/A spread (the
 * largest difference of two repetitions' medians) and the scatter (the largest range of retained
 * observations) describe these runs, and neither is a detection limit for the per-message cost.
 * @param {object} pre PRE eachMessage session record
 * @param {object} post POST eachMessage session record
 * @returns {{status:'inconclusive',reason:string,perRecordNs:{pre:number,post:number},
 *   deltaNs:number,controlDrift:number,adjustedDeltaNs:number,aaSpread:number,scatter:number}}
 * @throws {Error} unless both records pass `checkComparable` and share the driver digest and Node
 */
const compareEachMessage = (pre, post) => {
  sided('PRE', () => checkComparable(pre))
  sided('POST', () => checkComparable(post))
  if (pre.driver.sha256 !== post.driver.sha256) {
    throw new Error(
      `the runs used different drivers: ${pre.driver.sha256} and ${post.driver.sha256}`
    )
  }
  if (pre.node !== post.node) {
    throw new Error(`the runs used different Node versions: ${pre.node} and ${post.node}`)
  }
  const arms = [
    pre.arms.eachMessage,
    post.arms.eachMessage,
    pre.arms.record_limit,
    post.arms.record_limit,
  ].map(perRecord)
  const [message, after, control, controlAfter] = arms
  const controlDrift = controlAfter.ns / control.ns - 1
  return {
    status: 'inconclusive',
    reason: `${OWED}; these runs show the observed difference, A/A spread and scatter, not a detection limit`,
    perRecordNs: { pre: message.ns, post: after.ns },
    deltaNs: after.ns - message.ns,
    controlDrift,
    adjustedDeltaNs: after.ns - message.ns * (1 + controlDrift),
    aaSpread: Math.max(...arms.map(({ spread }) => spread)),
    scatter: Math.max(...arms.map(({ scatter }) => scatter)),
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
    ...(name === 'eachMessage' ? [repeatabilityNote(record)].filter(Boolean) : []),
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
 * @returns {object} the session record; from evidence, read like the report reads its one run
 * @throws {Error} for evidence without exactly one run, or a run `read` refuses
 */
const sessionFrom = (file) => {
  const value = JSON.parse(fs.readFileSync(file, 'utf8'))
  if (!Array.isArray(value.runs)) return value
  if (value.runs.length !== 1) throw new Error(`${value.runs.length} runs, expected one`)
  const [run] = value.runs
  return read({ ...run, error: run.error ? new Error(run.error) : undefined })
}

/**
 * `compareEachMessage` over two files.
 * @param {string} preFile PRE `fork.json` or session record
 * @param {string} postFile POST `fork.json` or session record
 * @returns {ReturnType<typeof compareEachMessage>}
 * @throws {Error} for a file `sessionFrom` refuses or records the comparison refuses
 */
const compareFiles = (preFile, postFile) =>
  compareEachMessage(
    sided('PRE', () => sessionFrom(preFile)),
    sided('POST', () => sessionFrom(postFile))
  )

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
  compareFiles,
}

if (require.main === module) {
  const [flag, pre, post] = process.argv.slice(2)
  if (flag !== '--compare' || !pre || !post) {
    console.error('usage: shareThroughput.js --compare <pre fork.json> <post fork.json>')
    process.exitCode = 2
  } else {
    try {
      console.log(JSON.stringify(compareFiles(pre, post), null, 2))
    } catch (error) {
      console.error(`refused: ${error.message}`)
      process.exitCode = 1
    }
  }
}
