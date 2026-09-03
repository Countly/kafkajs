const { render, cell, row, range, gapCell } = require('../render')

const model = () => ({
  kafkaTag: '9.9.9',
  apis: [
    {
      kafkaName: 'Produce',
      apiKey: 0,
      validVersions: [3, 13],
      flexibleFrom: 9,
      forkVersions: [0, 1, 2, 3, 5],
      gap: 'behind:8',
      onFloor: false,
      belowFloor: [0, 1, 2],
      notes: 'A note with a | pipe and\na newline.',
    },
    {
      kafkaName: 'DeleteTopics',
      apiKey: 20,
      validVersions: [1, 6],
      flexibleFrom: 4,
      forkVersions: [1],
      gap: 'behind:5',
      onFloor: true,
      belowFloor: [],
      notes: '',
    },
    {
      kafkaName: 'SaslHandshake',
      apiKey: 17,
      validVersions: [0, 0],
      flexibleFrom: null,
      forkVersions: null,
      gap: 'missing',
      onFloor: false,
      belowFloor: [],
      notes: '',
    },
  ],
  waivers: [
    { api: 'Produce', versions: [0, 1, 2], kafkaMin: 3, reason: 'KIP-724', until: 'phase-2 exit' },
  ],
  crossCutting: [{ gap: 'A | gap', where: 'somewhere', impact: 'an impact', owner: 'phase-4' }],
  errors: {
    counts: { kafka: 134, fork: 89 },
    missingInFork: [{ code: 89, type: 'THROTTLING_QUOTA_EXCEEDED' }],
    unknownToKafka: [],
    mismatches: [],
    aliased: [
      { code: -1, forkType: 'UNKNOWN', kafkaType: 'UNKNOWN_SERVER_ERROR', reason: 'public API' },
    ],
    staleAliases: [],
  },
  untracked: [
    { kafkaName: 'LeaderAndIsr', apiKey: 4, apiKeysName: 'LeaderAndIsr', reason: 'ZooKeeper-era' },
  ],
})

describe('scripts/compat/render', () => {
  describe('cells', () => {
    it('escapes pipes so a note cannot break the table', () => {
      expect(cell('a | b')).toBe('a \\| b')
    })

    it('flattens newlines', () => {
      expect(cell('a\nb')).toBe('a b')
      expect(cell('a\r\nb')).toBe('a b')
    })

    it('renders null and undefined as an empty cell', () => {
      expect(cell(null)).toBe('')
      expect(cell(undefined)).toBe('')
    })

    it('builds a padded row', () => {
      expect(row(['a', 1, null])).toBe('| a | 1 |  |')
    })

    it('collapses a single-version range', () => {
      expect(range([0, 0])).toBe('0')
      expect(range([1, 6])).toBe('1-6')
      expect(range(null)).toBe('—')
    })

    it('appends the floor signals to the gap class', () => {
      expect(gapCell(model().apis[0])).toBe('behind:8 (below-floor 0-2)')
      expect(gapCell(model().apis[1])).toBe('behind:5 (on-floor)')
      expect(gapCell(model().apis[2])).toBe('missing')
    })
  })

  describe('render', () => {
    it('is deterministic: the same model renders byte-identically', () => {
      expect(render(model())).toBe(render(model()))
    })

    it('does not embed a date, a timestamp or an absolute path', () => {
      const output = render(model())
      expect(output).not.toMatch(/\d{4}-\d{2}-\d{2}T/)
      expect(output).not.toMatch(/\/Users\/|\/home\//)
      expect(output).not.toContain(new Date().getFullYear().toString() + '-')
    })

    it('references no git-ignored path', () => {
      const output = render(model())
      for (const ignored of ['upgrade-ref', 'docs/superpowers', 'CLAUDE.md', 'CODEX_brainstorm']) {
        expect(output).not.toContain(ignored)
      }
    })

    it('escapes pipes inside every generated table', () => {
      const output = render(model())
      expect(output).toContain('A note with a \\| pipe and a newline.')
      expect(output).toContain('A \\| gap')
    })

    it('keeps every summary row at seven columns', () => {
      const summary = render(model())
        .split('## Floor watch')[0]
        .split('\n')
        .filter((line) => line.startsWith('| '))
      for (const line of summary) {
        expect(line.replace(/\\\|/g, '').split('|').length - 2).toBe(7)
      }
    })

    it('renders every section', () => {
      const output = render(model())
      expect(output).toContain('## Summary')
      expect(output).toContain('## Floor watch')
      expect(output).toContain('## Below-floor waivers')
      expect(output).toContain('## Cross-cutting gaps')
      expect(output).toContain('## Error table')
      expect(output).toContain('## Untracked API keys')
    })

    it('names the floor-sitters and the waived versions', () => {
      const output = render(model())
      expect(output).toContain('| DeleteTopics | 20 | v1 | v1 |')
      expect(output).toContain('| Produce | 0-2 | v3 | KIP-724 | phase-2 exit |')
    })

    it('renders an em dash for a waiver whose API is not in the matrix', () => {
      const orphan = model()
      orphan.waivers = [
        { api: 'Ghost', versions: [0], kafkaMin: null, reason: 'stale', until: 'phase-2 exit' },
      ]
      const output = render(orphan)
      expect(output).toContain('| Ghost | 0 | — | stale | phase-2 exit |')
      expect(output).not.toContain('vnull')
    })

    it('says the waiver list is empty when it is', () => {
      const empty = model()
      empty.waivers = []
      expect(render(empty)).toContain('None — the waiver list is empty.')
    })

    it('ends with exactly one trailing newline', () => {
      expect(render(model()).endsWith('\n')).toBe(true)
      expect(render(model()).endsWith('\n\n')).toBe(false)
    })
  })
})
