const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { summarise, render, readRecords, pooledSd, main } = require('../longWindowReport')

const KIB = 1024
const KINDS = ['producer', 'consumer', 'admin', 'shareConsumer']

/** @returns {object} one worker report as the diagnostic driver writes it */
const workerOf = (kind, arm, slopeKiB, { tripped = false, complete = true } = {}) => ({
  kind,
  arm,
  complete,
  mid: { data: { slopeBytesPerMinute: slopeKiB * KIB, endpointBytes: 0 } },
  masked: slopeKiB < -32,
  firstUse: { tripped },
  wouldBe: {
    s1: arm === 'seeded' ? { pass: slopeKiB >= 96 } : null,
    s2: arm === 'unseeded' ? { pass: slopeKiB <= 64 } : null,
  },
})

const recordOf = ({ node, kafka = '4.3.1', arm, replicate = 1, workers }) => ({
  diagnostic: true,
  acceptanceEligible: false,
  arm,
  replicate,
  node: `v${node}.1.0`,
  kafka,
  tree: 'a'.repeat(40),
  sha: 'a'.repeat(40),
  workers,
})

/**
 * @param {number} node major version
 * @param {function(string, number, number):number} unseeded slope in KiB by kafka index, kind index
 *   and replicate
 * @param {number} seeded slope of every seeded worker, KiB/min
 * @param {number} [replicates] replicates per tuple
 * @returns {object[]} the records of one Node's matrix
 */
const matrix = (node, unseeded, seeded, replicates = 3) => {
  const records = []
  for (const [k, kafka] of ['4.2.2', '4.3.1'].entries())
    for (let replicate = 1; replicate <= replicates; replicate++) {
      records.push(
        recordOf({
          node,
          kafka,
          arm: 'unseeded',
          replicate,
          workers: KINDS.map((kind, i) => workerOf(kind, 'unseeded', unseeded(k, i, replicate))),
        }),
        recordOf({
          node,
          kafka,
          arm: 'seeded',
          replicate,
          workers: KINDS.map((kind) => workerOf(kind, 'seeded', seeded)),
        })
      )
    }
  records.push(
    recordOf({
      node,
      arm: 'warmup-seed',
      workers: KINDS.map((kind) => workerOf(kind, 'warmup-seed', 0, { tripped: true })),
    })
  )
  return records
}

describe('long-window report', () => {
  test('counts S1 over seeded workers, S2 over unseeded workers and first-use trips per Node', () => {
    const { nodes } = summarise(matrix(22, (k, i, r) => 10 + (r - 2), 120))
    expect(nodes['22']).toMatchObject({
      s1: { pass: 24, total: 24 },
      s2: { pass: 24, total: 24 },
      masked: 0,
      firstUse: { tripped: 4, total: 4 },
      incomplete: 0,
    })
  })

  test('passes S3 when the seeded mean less three pooled SDs reaches 96 and the unseeded mean plus three stays within 64', () => {
    const { nodes } = summarise(matrix(22, (k, i, r) => 10 + (r - 2), 120))
    expect(nodes['22'].s3).toMatchObject({
      status: 'pass',
      seededMean: 120,
      unseededMean: 10,
      sd: 1,
      seededBound: 117,
      unseededBound: 13,
    })
  })

  test('fails S3 and counts the S2 misses of a Node whose unseeded replicates scatter', () => {
    const { nodes } = summarise(matrix(24, (k, i, r) => 10 + (r - 1) * 30, 120))
    expect(nodes['24'].s2).toMatchObject({ status: 'fail', pass: 16, total: 24 })
    expect(nodes['24'].s3).toMatchObject({
      status: 'fail',
      sd: 30,
      unseededMean: 40,
      unseededBound: 130,
      seededBound: 30,
    })
  })

  test('reports S3 as undecidable with one replicate per tuple', () => {
    const { nodes } = summarise(matrix(26, () => 10, 120, 1))
    expect(nodes['26'].s3).toMatchObject({ status: 'insufficient', sd: null })
    expect(nodes['26'].s1).toMatchObject({ status: 'insufficient', pass: 8, total: 24 })
  })

  test('pools the within-tuple deviation over the degrees of freedom of every tuple', () => {
    expect(
      pooledSd([
        [10, 12, 14],
        [100, 100, 100],
      ])
    ).toBeCloseTo(Math.SQRT2, 10)
    expect(pooledSd([[1], [2]])).toBeNull()
    expect(pooledSd([[5, 5]])).toBe(0)
  })

  test('counts masked and incomplete workers without letting an incomplete one into a table', () => {
    const records = matrix(22, (k, i) => (k === 0 && i === 0 ? -40 : 10), 120)
    records[0].workers[1] = workerOf('consumer', 'unseeded', 999, { complete: false })
    const { nodes } = summarise(records)
    expect(nodes['22'].incomplete).toBe(1)
    expect(nodes['22'].masked).toBe(3)
    expect(nodes['22'].s2).toMatchObject({ status: 'insufficient', pass: 23, total: 24 })
    expect(nodes['22'].s3.status).toBe('insufficient')
    expect(nodes['22'].s3.unseededMean).toBeLessThan(50)
  })

  test('rejects duplicate downloads and duplicate worker identities instead of pooling them', () => {
    const records = matrix(22, () => 10, 120)
    expect(() => summarise([...records, JSON.parse(JSON.stringify(records[0]))])).toThrow(
      /duplicate.*identity/i
    )
    records[0].workers.push({ ...records[0].workers[0] })
    expect(() => summarise(records)).toThrow(/duplicate.*identity/i)
  })

  test('requires every Kafka, arm, kind and replicate observation before S1, S2 or S3 can pass', () => {
    const missingJob = matrix(22, () => 10, 120).filter(
      (record) => !(record.kafka === '4.3.1' && record.arm === 'seeded' && record.replicate === 3)
    )
    const job = summarise(missingJob).nodes['22']
    expect(job.s1).toMatchObject({ status: 'insufficient', pass: 20, total: 24 })
    expect(job.s3.status).toBe('insufficient')
    const missingWorker = matrix(22, () => 10, 120)
    missingWorker[0].workers.pop()
    const worker = summarise(missingWorker).nodes['22']
    expect(worker.s2).toMatchObject({ status: 'insufficient', pass: 23, total: 24 })
    expect(worker.missing).toBe(1)
    expect(worker.s3.status).toBe('insufficient')
    const twoReplicates = summarise(matrix(22, () => 10, 120, 2)).nodes['22']
    expect(twoReplicates.s3.status).toBe('insufficient')
    expect(twoReplicates.s2).toMatchObject({ status: 'insufficient', pass: 16, total: 24 })
    const oneTuple = summarise(matrix(22, () => 10, 120).slice(0, 6)).nodes['22']
    expect(oneTuple.s3.status).toBe('insufficient')
  })

  test('keeps incomplete seeded workers in the expected denominator and makes S3 insufficient', () => {
    const records = matrix(22, () => 10, 120)
    records[1].workers[0] = { kind: 'producer', arm: 'seeded', complete: false }
    const node = summarise(records).nodes['22']
    expect(node.s1).toMatchObject({ status: 'insufficient', pass: 23, total: 24 })
    expect(node.incomplete).toBe(1)
    expect(node.s3.status).toBe('insufficient')
    expect(render({ nodes: { 22: node } })).toMatch(/23\/24 insufficient/)
  })

  test('reports entirely missing Nodes and warm-up controls as insufficient', () => {
    const records = matrix(22, () => 10, 120).filter((record) => record.arm !== 'warmup-seed')
    const { nodes } = summarise(records)
    expect(nodes['22'].firstUse).toMatchObject({ status: 'insufficient', tripped: 0, total: 4 })
    for (const major of ['24', '26']) {
      expect(nodes[major].s1).toMatchObject({ status: 'insufficient', pass: 0, total: 24 })
      expect(nodes[major].s2).toMatchObject({ status: 'insufficient', pass: 0, total: 24 })
      expect(nodes[major].s3.status).toBe('insufficient')
    }
  })

  test('refuses to pool different commits or Node runtimes in one collection', () => {
    const commits = matrix(22, () => 10, 120)
    commits[0].sha = 'b'.repeat(40)
    expect(() => summarise(commits)).toThrow(/commit/)
    const runtimes = matrix(22, () => 10, 120)
    runtimes[0].node = 'v22.2.0'
    expect(() => summarise(runtimes)).toThrow(/runtime/)
  })

  test('prints one line per Node with S1, S2, masking, first-use trips and S3', () => {
    const text = render(
      summarise([
        ...matrix(22, (k, i, r) => 10 + (r - 2), 120),
        ...matrix(24, (k, i, r) => 10 + (r - 1) * 30, 120),
        ...matrix(26, () => 10, 120, 1),
      ])
    )
    expect(text).toMatch(/diagnostic/)
    expect(text).toMatch(/^22 +24\/24 pass +24\/24 pass +0 +4\/4 pass +pass/m)
    expect(text).toMatch(/^24 +24\/24 pass +16\/24 fail +0 +4\/4 pass +fail/m)
    expect(text).toMatch(/^26 +8\/24 insufficient +8\/24 insufficient +0 +4\/4 pass +insufficient/m)
  })

  describe('over downloaded artifacts', () => {
    let directory
    beforeEach(() => {
      directory = fs.mkdtempSync(path.join(os.tmpdir(), 'long-window-report-'))
    })
    afterEach(() => {
      fs.rmSync(directory, { recursive: true, force: true })
    })

    const place = (name, record) => {
      const folder = path.join(directory, name)
      fs.mkdirSync(folder, { recursive: true })
      fs.writeFileSync(path.join(folder, 'long-window.json'), JSON.stringify(record))
    }

    test('reads every long-window record beneath the directory and nothing else', () => {
      const records = matrix(22, () => 10, 120, 1)
      records.forEach((record, i) => place(`artifact-${i}`, record))
      fs.writeFileSync(path.join(directory, 'artifact-0', 'identity.json'), '{}')
      expect(readRecords(directory)).toHaveLength(records.length)
      expect(() => readRecords(path.join(directory, 'missing'))).toThrow()
    })

    test('refuses a record that is not diagnostic or that claims acceptance', () => {
      place('a', { ...recordOf({ node: 22, arm: 'seeded', workers: [] }), diagnostic: false })
      expect(() => readRecords(directory)).toThrow('not a diagnostic long-window record')
    })

    test('prints the table of a directory through main', () => {
      matrix(22, (k, i, r) => 10 + (r - 2), 120).forEach((record, i) => place(`a-${i}`, record))
      const lines = []
      expect(main([directory], (line) => lines.push(line))).toBe(0)
      expect(lines.join('\n')).toMatch(/^22 +24\/24/m)
      expect(() => main([], () => {})).toThrow('usage')
    })
  })
})
