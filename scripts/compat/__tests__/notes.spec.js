const notes = require('../notes.json')
const trackedApis = require('../tracked-apis.json')
const { errorCodes } = require('../../../src/protocol/error')

describe('scripts/compat/notes.json', () => {
  const trackedNames = trackedApis.tracked.map((api) => api.kafkaName)

  it('has no orphan API note', () => {
    const orphans = Object.keys(notes.apis).filter((name) => trackedNames.indexOf(name) === -1)
    expect(orphans).toEqual([])
  })

  it('gives every API note a body and a since date', () => {
    for (const [name, note] of Object.entries(notes.apis)) {
      expect(typeof note.note).toBe('string')
      expect(note.note.length).toBeGreaterThan(0)
      expect(note.since).toMatch(/^\d{4}-\d{2}-\d{2}$/)
      expect(name).not.toMatch(/Request$|Response$/)
    }
  })

  it('gives every cross-cutting row an owning phase', () => {
    expect(notes.crossCutting.length).toBeGreaterThan(0)
    for (const row of notes.crossCutting) {
      expect(row.gap.length).toBeGreaterThan(0)
      expect(row.where.length).toBeGreaterThan(0)
      expect(row.impact.length).toBeGreaterThan(0)
      expect(row.owner).toMatch(/^(phase-\d|backlog)$/)
    }
  })

  it('only aliases error types the fork actually defines', () => {
    const forkTypes = errorCodes.map((error) => error.type)
    for (const forkType of Object.keys(notes.errorAliases)) {
      expect(forkTypes).toContain(forkType)
      expect(notes.errorAliases[forkType].kafkaType).toMatch(/^[A-Z][A-Z0-9_]*$/)
      expect(notes.errorAliases[forkType].reason.length).toBeGreaterThan(0)
    }
  })

  it('references no git-ignored path', () => {
    const serialized = JSON.stringify(notes)
    for (const ignored of ['upgrade-ref', 'docs/superpowers', 'CLAUDE.md', 'CODEX_brainstorm']) {
      expect(serialized).not.toContain(ignored)
    }
  })
})
