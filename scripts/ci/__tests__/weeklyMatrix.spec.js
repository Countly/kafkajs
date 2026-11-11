const fs = require('node:fs')
const path = require('node:path')
const yaml = require('js-yaml')

const WORKFLOW = path.join(__dirname, '..', '..', '..', '.github', 'workflows', 'weekly.yml')
const KAFKA_LINES = ['4.0.2', '4.2.2', '4.3.2', '4.4.0']
const NODE_LINES = [22, 24, 26]
const GROUPS = ['broker', 'admin', 'producer', 'consumer', 'others', 'oauthbearer']

/**
 * @param {string} body expression text
 * @returns {string} the workflow expression `${{ body }}`
 */
const expr = (body) => `\${{ ${body} }}`

const source = fs.readFileSync(WORKFLOW, 'utf8')
const workflow = yaml.load(source)
const job = workflow.jobs.integration

/**
 * Expands a strategy matrix the way GitHub does: the product of its axes, minus every
 * combination that an `exclude` entry matches on all of that entry's keys.
 * @param {Object<string, any>} matrix a `strategy.matrix` object without `include`
 * @returns {Object<string, string>[]} one object per job, values as strings
 * @throws {Error} when the matrix has an `include` list, which this helper does not model
 */
const expand = (matrix) => {
  if (matrix.include !== undefined) throw new Error('include is not modelled')
  const { exclude = [], ...axes } = matrix
  const products = Object.entries(axes).reduce(
    (cells, [axis, values]) =>
      cells.flatMap((cell) => values.map((value) => ({ ...cell, [axis]: String(value) }))),
    [{}]
  )
  const matches = (cell, rule) =>
    Object.entries(rule).every(([axis, value]) => cell[axis] === String(value))
  return products.filter((cell) => !exclude.some((rule) => matches(cell, rule)))
}

/**
 * @param {Object<string, string>[]} jobs expanded matrix
 * @param {string} kafka Kafka line
 * @returns {string[]} the Node versions that run that Kafka line, without repeats
 */
const nodesOf = (jobs, kafka) => [
  ...new Set(jobs.filter((cell) => cell.kafka === kafka).map((cell) => cell.node)),
]

describe('weekly matrix', () => {
  const jobs = expand(job.strategy.matrix)

  test('covers Kafka 4.0.2, 4.2.2, 4.3.2 and 4.4.0 exactly', () => {
    expect(job.strategy.matrix.kafka).toEqual(KAFKA_LINES)
    expect([...new Set(jobs.map((cell) => cell.kafka))].sort()).toEqual(KAFKA_LINES)
  })

  test('runs every Kafka line on at least two Node versions and 4.4.0 on all three', () => {
    for (const kafka of KAFKA_LINES) {
      expect(nodesOf(jobs, kafka).length).toBeGreaterThanOrEqual(2)
    }
    expect(nodesOf(jobs, '4.4.0').sort()).toEqual(NODE_LINES.map(String))
    expect([...new Set(jobs.map((cell) => cell.node))].sort()).toEqual(NODE_LINES.map(String))
  })

  test('expands to nine Kafka-by-Node cells and 54 jobs', () => {
    const cells = new Set(jobs.map((cell) => `${cell.kafka}/${cell.node}`))

    expect(cells.size).toBe(9)
    expect(jobs).toHaveLength(54)
  })

  test('has no dispatch input and KAFKA_VERSION comes only from matrix.kafka', () => {
    expect(Object.keys(workflow.on)).toEqual(['schedule', 'workflow_dispatch'])
    expect(workflow.on.workflow_dispatch ?? {}).not.toHaveProperty('inputs')
    expect(job.env.KAFKA_VERSION).toBe(expr('matrix.kafka'))
    expect(source).not.toMatch(/github\.event\.inputs|inputs\.kafka_version/)
  })

  test('keeps the Monday 03:00 cron and the six groups', () => {
    expect(workflow.on.schedule).toEqual([{ cron: '0 3 * * 1' }])
    expect(job.strategy.matrix.group).toEqual(GROUPS)
    expect(job.env.COMPOSE_FILE).toBe(
      expr(
        "matrix.group == 'oauthbearer' && 'docker-compose.oauthbearer.yml' || 'docker-compose.yml'"
      )
    )
  })

  test('runs all six groups on Kafka 4.0.2', () => {
    const floor = jobs.filter((cell) => cell.kafka === '4.0.2')

    expect(job.strategy.matrix.exclude.filter((rule) => 'group' in rule)).toEqual([])
    expect([...new Set(floor.map((cell) => cell.group))]).toEqual(GROUPS)
    expect(floor).toHaveLength(GROUPS.length * nodesOf(jobs, '4.0.2').length)
  })

  test('keeps max-parallel at 6', () => {
    expect(job.strategy['max-parallel']).toBe(6)
    expect(job.strategy['fail-fast']).toBe(false)
  })

  test('sets timeout-minutes', () => {
    expect(Number.isInteger(job['timeout-minutes'])).toBe(true)
    expect(job['timeout-minutes']).toBeGreaterThanOrEqual(1)
    expect(job['timeout-minutes']).toBeLessThanOrEqual(360)
  })

  test('drops a combination only when every key of an exclude entry matches', () => {
    const matrix = { node: [22, 24], kafka: ['a', 'b'], exclude: [{ node: 22, kafka: 'a' }] }

    expect(expand(matrix)).toEqual([
      { node: '22', kafka: 'b' },
      { node: '24', kafka: 'a' },
      { node: '24', kafka: 'b' },
    ])
    expect(() => expand({ ...matrix, include: [{ node: 26 }] })).toThrow(/include/)
  })
})
