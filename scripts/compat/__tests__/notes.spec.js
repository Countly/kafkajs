const fs = require('fs')
const path = require('path')
const notes = require('../notes.json')
const trackedApis = require('../tracked-apis.json')
const { errorCodes } = require('../../../src/protocol/error')

// A phase with a section in the changelog has started, so nothing may still be owed to it. Without
// any phase heading, `lastStartedPhase` is -Infinity and the check passes vacuously.
const changelog = fs.readFileSync(path.join(__dirname, '..', '..', '..', 'CHANGELOG.md'), 'utf8')
const lastStartedPhase = Math.max(
  ...[...changelog.matchAll(/^### Phase (\d+)/gm)].map((m) => +m[1])
)

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

  it('owes no cross-cutting row, and plans no API, to a phase that has started', () => {
    const named = [
      ...notes.crossCutting.map(({ owner }) => owner),
      ...trackedApis.tracked.map(({ planned }) => planned),
    ].flatMap((value) => /^phase-(\d)$/.exec(value)?.slice(1) ?? [])

    expect(named.filter((phase) => Number(phase) <= lastStartedPhase)).toEqual([])
  })

  it('names no phase, task or decision in any text it ships', () => {
    const texts = [
      ...Object.values(notes.apis).map(({ note }) => note),
      ...notes.crossCutting.flatMap(({ gap, where, impact }) => [gap, where, impact]),
    ]
    for (const text of texts) {
      expect(text).not.toMatch(/\b(?:phase|task|amendment)[- ]?\d|\bD\d{1,3}\b|\bP\d+-[A-Z]\d+|§/i)
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
