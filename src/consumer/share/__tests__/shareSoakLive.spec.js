const { plainTextBrokers } = require('testHelpers')
const { SHARE_SOAK, ShareCycleOracle, buildShareSoak } = require('../../../../scripts/soak/soak')
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
      expect(SHARE_SOAK).toMatchObject({
        shortMs: 960000,
        fullMs: 1920000,
        slotMs: { short: 32000, full: 64000 },
        quietMs: { cycle: 5000, final: 5000 },
        cycles: 30,
        resets: 10,
        budgets: {
          short: { profileMs: 960000, totalMs: 1020000, watchdogMs: 1020000, lifecycleMs: 1440000 },
          full: { profileMs: 1920000, totalMs: 1980000, watchdogMs: 1980000, lifecycleMs: 2400000 },
        },
      })
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
      expect(owner.elapsedProfileMs).toBeGreaterThanOrEqual(SHARE_SOAK.shortMs)
      expect(owner.elapsedProfileMs).toBeLessThanOrEqual(SHARE_SOAK.shortMs + owner.profileMarginMs)
      expect(owner.timing.slots).toHaveLength(SHARE_SOAK.cycles)
      expect(owner.timing.slots.every(({ due, completedAt }) => completedAt >= due)).toBe(true)
      for (const [index, slot] of owner.timing.slots.entries()) {
        const phase = owner.timing.phases[Math.floor(index / 10)]
        expect(phase.due - phase.startedAt).toBe(320000)
        expect(slot.due).toBe(phase.startedAt + ((index % 10) + 1) * 32000)
        expect(slot.completedAt).toBeGreaterThanOrEqual(slot.due)
      }
      expect(owner.final.verifier.windowMs).toBe(5000)
      const cycles = owner.evidence.read().filter(({ event }) => event === 'soak-cycle')
      const steps = owner.evidence.read().filter(({ event }) => event === 'soak-cycle-step')
      for (const cycle of cycles) {
        expect(cycle.verifier.windowMs).toBe(5000)
        expect(steps.filter((row) => row.cycle === cycle.cycle)).toEqual(
          expect.arrayContaining(
            cycle.steps.map(({ at, ...step }) =>
              expect.objectContaining({ ...step, monotonicAt: at })
            )
          )
        )
        expect(cycle.steps.map(({ step }) => step)).toEqual(
          expect.arrayContaining([
            'produce-start',
            'produce-end',
            'member-joined',
            'first-delivery',
            'all-delivered',
            'all-acks',
            'member-add-end',
            'member-remove-end',
            'disconnect-start',
            'disconnect-end',
            'group-empty-end',
            'verifier-start',
            'verifier-end',
            'cycle-end',
          ])
        )
        for (const member of cycle.memberObservations) {
          const frame = ShareCycleOracle.departure(
            cycle.provenance,
            cycle.verifier.groupId,
            member.memberId
          )
          expect(frame).not.toBeNull()
          const written = cycle.steps.find(
            ({ step, memberId, socket, correlationId }) =>
              step === 'leave-write' &&
              memberId === member.memberId &&
              socket === frame.socket &&
              correlationId === frame.correlationId
          )
          const answered = cycle.steps.find(
            ({ step, memberId, socket, correlationId }) =>
              step === 'leave-answer' &&
              memberId === member.memberId &&
              socket === written?.socket &&
              correlationId === written?.correlationId
          )
          expect(written).toMatchObject({
            member: member.member,
            groupId: cycle.verifier.groupId,
            memberEpoch: -1,
            socket: expect.any(Number),
            endpoint: expect.any(String),
            correlationId: expect.any(Number),
            invokedAt: expect.any(Number),
            writtenAt: expect.any(Number),
          })
          expect(answered).toMatchObject({ errorCode: 0, answeredAt: expect.any(Number) })
          expect(answered.answeredAt).toBeGreaterThanOrEqual(written.writtenAt)
        }
        const descriptions = cycle.provenance.groupDescriptions
        expect(descriptions.length).toBeGreaterThan(0)
        expect(descriptions.at(-1).group).toMatchObject({
          groupId: cycle.verifier.groupId,
          errorCode: 0,
          state: 'Empty',
          members: [],
        })
        expect(cycle.steps.filter(({ step }) => step === 'cleanup-members')).toHaveLength(
          descriptions.length
        )
        expect(cycle.snapshot.workloadJoined).toBe(true)
        expect(cycle.memberObservations).toHaveLength(2)
        expect(
          cycle.memberObservations.every(({ disconnected, finalized }) => disconnected && finalized)
        ).toBe(true)
        if (cycle.reset) {
          const { resetTarget: target, reset, provenance } = cycle
          expect(target).toMatchObject({
            socket: reset.oldSocket,
            nodeId: reset.nodeId,
            epoch: reset.oldEpoch,
            acquisition: expect.any(Number),
            dispatch: expect.any(Number),
            held: cycle.held,
          })
          expect(provenance.acquisitions.some(({ id }) => id === target.acquisition)).toBe(true)
          expect(cycle.steps.map(({ step }) => step)).toEqual(
            expect.arrayContaining([
              'reset-target',
              'socket-close',
              'reconnect-start',
              'reconnect-end',
              'post-reset-api78',
              'SESSION_RESET',
              'held-redelivery',
            ])
          )
          const closed = cycle.steps.find(({ step }) => step === 'socket-close')
          expect(closed).toMatchObject({ socket: target.socket, endpoint: target.endpoint })
          const writes = cycle.steps.filter(({ step }) => step === 'post-reset-api78')
          expect(new Set(writes.map(({ nodeId }) => nodeId)).size).toBe(3)
          expect(writes).toHaveLength(3)
          for (const written of writes) {
            const exchange = provenance.exchanges.find(({ id }) => id === written.exchange)
            const frame = exchange.frames.find(({ id }) => id === written.frame)
            expect(written).toMatchObject({
              at: frame.at,
              epoch: frame.request.shareSessionEpoch,
              socket: frame.socket,
              endpoint: frame.endpoint,
              attempted: true,
            })
          }
          expect(writes.find(({ nodeId }) => nodeId === target.nodeId)).toMatchObject({
            epoch: 0,
            socket: reset.nextSocket,
          })
          const redeliveries = cycle.steps.filter(({ step }) => step === 'held-redelivery')
          expect(new Set(redeliveries.map(({ identity }) => identity))).toEqual(new Set(cycle.held))
          expect(redeliveries.every(({ deliveryCount }) => deliveryCount >= 1)).toBe(true)
        }
        expect(
          cycle.steps.every(
            ({ at, elapsedMs, due, remainingMs }) =>
              Number.isFinite(at) &&
              elapsedMs === at - cycle.timing.startedAt &&
              due === cycle.timing.due &&
              remainingMs === due - at
          )
        ).toBe(true)
      }
      const verifiers = [...cycles.map(({ verifier }) => verifier), owner.final.verifier]
      const configuration = owner.evidence
        .read()
        .find(({ event }) => event === 'soak-configuration')
      expect(configuration.config.groupId).toBe(owner.driver.group)
      expect(configuration.config.effective).toHaveLength(3)
      for (const verifier of verifiers) {
        const beats = verifier.provenance.heartbeats.filter(
          ({ memberId, response }) => memberId === verifier.memberId && response?.errorCode === 0
        )
        expect(beats.length).toBeGreaterThan(0)
        expect(
          beats.every(
            ({ frames, response, at, answeredAt }) =>
              response.heartbeatIntervalMs > 0 &&
              answeredAt >= at &&
              frames.some((frame) => frame.apiKey === 76 && typeof frame.responseBytes === 'string')
          )
        ).toBe(true)
      }
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
