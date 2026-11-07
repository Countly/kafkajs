const { plainTextBrokers } = require('testHelpers')
const { SHARE_SOAK, buildShareSoak } = require('../../../../scripts/soak/soak')
const { caseTimeout, liveCase, liveRun } = require('../../../producer/__tests__/fixtures/bounded')
const { CleanupStack } = require('../../../producer/__tests__/fixtures/liveProbe')

// Gated: it needs the share-enabled base cluster (docker-compose.yml, three nodes) and runs through
// the guarded live entry with SHARE_SOAK_E2E=1, alone, with --expose-gc --detectOpenHandles
// --runInBand and no forced exit.
const enabled = process.env.SHARE_SOAK_E2E === '1' ? describe : describe.skip

const TITLE = 'preserves share identities and resources through the short soak'
const BUDGET_MS = SHARE_SOAK.budgets.short.totalMs
const CLEANUP_MS = SHARE_SOAK.budgets.short.cleanupMs

enabled('share consumer short soak', () => {
  /**
   * The whole short soak, through the real owner the full profile also uses: thirty join and leave
   * cycles of a share member and a transient second one, ten forced session resets, and the
   * independent log, broker-position and handler oracles of every cycle, with the resource screen
   * of each phase. The owner registers its cleanup before it creates anything and proves the fixed
   * topic and group absent and every measured owner back at its baseline before it reports.
   */
  test(
    TITLE,
    liveCase(BUDGET_MS, async () => {
      if (typeof globalThis.gc !== 'function') {
        throw new Error(
          'The short soak reads the heap after a collection: run node with --expose-gc'
        )
      }
      const owner = buildShareSoak({ brokers: plainTextBrokers(), short: true, run: liveRun })
      const stack = new CleanupStack()
      stack.add('close the share soak', () => owner.close())

      let verdict
      await stack.run(
        async () => {
          verdict = await owner.run()
        },
        { budgetMs: CLEANUP_MS }
      )

      expect(verdict).toMatchObject({ ok: true, violations: [] })
      expect(owner.counts).toEqual({
        cycles: SHARE_SOAK.cycles,
        resets: SHARE_SOAK.resets,
        memberAdds: SHARE_SOAK.cycles,
        memberRemoves: SHARE_SOAK.cycles,
      })
      expect(owner.elapsedProfileMs).toBe(SHARE_SOAK.shortMs)
      expect(owner.timing.slots).toHaveLength(SHARE_SOAK.cycles)
      expect(owner.timing.slots.every(({ due, completedAt }) => completedAt <= due)).toBe(true)
      expect(owner.final.verifier.windowMs).toBe(5000)
      const cycles = owner.evidence.read().filter(({ event }) => event === 'soak-cycle')
      const verifiers = [...cycles.map(({ verifier }) => verifier), owner.final.verifier]
      expect(new Set(verifiers.map(({ memberId }) => memberId)).size).toBe(31)
      expect(
        verifiers.every(
          ({ openedAt, closedAt, provenance }) =>
            Number.isFinite(openedAt) &&
            Number.isFinite(closedAt) &&
            closedAt >= openedAt &&
            provenance.exchanges.some(({ apiKey }) => apiKey === 78)
        )
      ).toBe(true)
      expect(owner.final.timing.startedAt).toBeGreaterThanOrEqual(
        owner.timing.phases.at(-1).completedAt
      )
    }),
    caseTimeout(BUDGET_MS)
  )
})
