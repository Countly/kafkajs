const rows = require('./fixtures/classicCoordinatorFaults.json')
const { rebalanceAction, REBALANCE_PATHS } = require('../rebalanceErrorContract')
const { createErrorFromCode } = require('../../protocol/error')
const { ClassicFaultEpisode, FINDINGS } = require('./fixtures/classicFaultEpisode')

it('declares the complete independent five-request fault grid and stale answers', () => {
  expect(rows).toHaveLength(42)
  for (const api of ['JoinGroup', 'SyncGroup', 'Heartbeat', 'OffsetCommit', 'LeaveGroup']) {
    expect(
      rows
        .filter((row) => row.api === api && row.step === 'first-current-answer')
        .map(({ fault }) => fault)
    ).toEqual([16, 15, 14, 25, 22, 27, 'disconnect', 'timeout'])
  }
})

it.each([
  [16, 'retry-coordinator', false],
  [15, 'retry-coordinator', false],
  [14, null, false],
  [25, 'lost', true],
  [22, 'lost', false],
  [27, 'revoked', false],
])(
  'classifies code %i without deriving expected actions from the implementation',
  (code, action, clearsMemberId) => {
    expect(rebalanceAction(createErrorFromCode(code), REBALANCE_PATHS.HEARTBEAT)).toEqual({
      action,
      clearsMemberId,
      fatal: false,
    })
  }
)

const API_NAMES = ['JoinGroup', 'SyncGroup', 'Heartbeat', 'OffsetCommit', 'LeaveGroup']
const EPISODE_TIMEOUT_MS = 25000
const runs = new Map()

/**
 * @param {object} row a fixture row, or a control derived from one
 * @returns {string} the row's test title
 */
const titleOf = (row) => `${row.api} ${row.fault} ${row.step}: ${row.expectedState}`

/**
 * Plays one row through the real consumer on scripted sockets, once however many tests ask: the
 * ordinary test and any expected-failing finding of the same row share the run, so the order the
 * tests execute in does not matter.
 * @param {object} row a fixture row, or a control derived from one
 * @returns {Promise<{episode: ClassicFaultEpisode, error: Error|undefined}>} the closed episode and
 *   the first failure of its setup, fault injection, verification or cleanup; never rejects
 */
const ran = (row) => {
  const title = titleOf(row)
  if (!runs.has(title)) {
    runs.set(
      title,
      (async () => {
        const episode = new ClassicFaultEpisode(row)
        let error
        try {
          await episode.open()
          await episode.execute()
          await episode.recover()
          episode.verify()
        } catch (failure) {
          error = failure
        }
        try {
          await episode.close()
        } catch (failure) {
          error ??= failure
        }
        return { episode, error }
      })()
    )
  }
  return runs.get(title)
}

/**
 * @param {object} row a fixture row, or a control derived from one
 * @returns {Promise<void>}
 * @throws {Error} the first failure of the row's episode
 */
const play = async (row) => {
  const { error } = await ran(row)
  if (error) throw error
}

const healthy = (api) => ({
  api,
  fault: 'none',
  step: 'healthy',
  expectedState: 'healthy',
  expectedWire: { afterLossOffsetCommits: 0, duplicateAssignments: 0 },
  maxRounds: 0,
  config: rows[0].config,
})

/** Join, sync and leave have no fixture row for a late answer of a replaced membership. */
const lateAnswers = ['JoinGroup', 'SyncGroup', 'LeaveGroup'].map((api) => ({
  ...rows.find((row) => row.step === 'old-generation-answer'),
  api,
  fault: 'late',
}))

describe('healthy episodes bracket the injected faults', () => {
  it.each(API_NAMES.map((api) => [api, healthy(api)]))(
    'a %s episode with no fault settles into a stable group',
    (_api, row) => play(row),
    EPISODE_TIMEOUT_MS
  )
})

describe('classic coordinator faults over the real wire', () => {
  it.each([...rows, ...lateAnswers].map((row) => [titleOf(row), row]))(
    '%s',
    (_title, row) => play(row),
    EPISODE_TIMEOUT_MS
  )

  describe.each(rows.filter((row) => FINDINGS[`${row.api}:${row.fault}`]))(
    'finding for $api $fault',
    (row) => {
      const { title, check } = FINDINGS[`${row.api}:${row.fault}`]
      it.failing(title, async () => check((await ran(row)).episode))
    }
  )
})
