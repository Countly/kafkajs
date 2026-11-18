const fs = require('fs')
const os = require('os')
const path = require('path')

const { generate, AREAS } = require('../generate')
const { MutantRunner } = require('../runner')

const REPO = path.resolve(__dirname, '..', '..', '..')

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
      'cluster',
      'admin',
    ])
    for (const name of names) expect(records.some((r) => r.area === name)).toBe(true)
    expect(new Set(records.map((r) => r.id)).size).toBe(records.length)
    const lines = new Map()
    for (const r of records) {
      const area = AREAS.find((a) => a.name === r.area)
      expect(area.specs).toContain(r.spec)
      expect(r.spec).not.toMatch(/Live\.spec\.js$/)
      expect(fs.existsSync(path.join(REPO, r.spec))).toBe(true)
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
})
