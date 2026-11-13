const fs = require('node:fs')
const path = require('node:path')
const vm = require('node:vm')
const Encoder = require('../../../src/protocol/encoder')
const { ShareCycleOracle } = require('../soak')
const wire = require('../../../testHelpers/wire/shareGroupHeartbeatV1')
const { heartbeatBytes, leaveFacts, verifierFacts } = require('./fixtures/shareCycleFacts')

const source = fs.readFileSync(
  path.join(__dirname, '../../../src/consumer/share/__tests__/shareSoakLive.spec.js'),
  'utf8'
)
const start = source.indexOf('      const verifiers =')
const end = source.indexOf('      expect(owner.final.timing.startedAt)', start)
if (start < 0 || end < 0) throw new Error('Missing live verifier assertions')
const assertions = source.slice(start, end)

/**
 * Encodes matching original heartbeat fields and frames after a fixture mutation.
 * @param {object} beat Original heartbeat call and answer.
 * @returns {void}
 */
const seal = (beat) => {
  const frame = beat.frames[0]
  frame.request = { ...beat.request, rackId: null, subscribedTopicNames: null }
  frame.bytes = heartbeatBytes(beat.request, frame.correlationId).toString('base64')
  const body = new Encoder()
    .writeInt32(frame.correlationId)
    .writeTaggedFields([])
    .writeBuffer(wire.buildResponse(beat.response))
  frame.responseBytes = new Encoder()
    .writeInt32(body.size())
    .writeEncoder(body)
    .buffer.toString('base64')
}

/**
 * Models the recorded active interval 5000 and successful epoch -1 leave interval 0.
 * @returns {object} Thirty cycle verifiers, the final verifier and their configuration.
 */
const fixture = () => {
  const verifiers = Array.from({ length: 31 }, (_, index) => {
    const verifier = verifierFacts(index)
    verifier.provenance.heartbeats = [0, 7, -1].map((epoch, n) => {
      const beat = leaveFacts(verifier.memberId, verifier.openedAt + n * 100, index * 3 + n + 1)
      beat.request.memberEpoch = epoch
      beat.response.memberEpoch = epoch === -1 ? -1 : 7
      beat.response.heartbeatIntervalMs = epoch === -1 ? 0 : 5000
      seal(beat)
      return beat
    })
    return verifier
  })
  const configuration = {
    event: 'soak-configuration',
    scope: 'scored',
    config: { groupId: 'soak-group', effective: [{}, {}, {}] },
  }
  return {
    cycles: verifiers.slice(0, 30).map((verifier) => ({ verifier })),
    owner: {
      final: { verifier: verifiers[30] },
      driver: { group: 'soak-group' },
      evidence: { read: () => [configuration] },
    },
  }
}

/**
 * Runs the actual post-run live assertions without loading its live entry or starting a workload.
 * @param {object} recording Broker-free original-frame fixture.
 * @returns {void}
 * @throws {Error} When an original live assertion rejects the recording.
 */
const verify = (recording) =>
  vm.runInNewContext(assertions, {
    ...recording,
    rows: recording.owner.evidence.read(),
    expect,
    Number,
    String,
    ShareCycleOracle,
  })

describe('share soak live verifier assertions', () => {
  test('proves active heartbeats and every original zero-interval leave separately', () => {
    expect(() => verify(fixture())).not.toThrow()
  })

  test.each([
    ['zero active interval', (v) => (v.provenance.heartbeats[0].response.heartbeatIntervalMs = 0)],
    ['only leave heartbeats', (v) => v.provenance.heartbeats.splice(0, 2)],
    ['missing departure', (v) => v.provenance.heartbeats.pop()],
    ['missing leave frame', (v) => (v.provenance.heartbeats[2].frames = [])],
    ['mismatched leave correlation', (v) => v.provenance.heartbeats[2].frames[0].correlationId++],
    [
      'rejected leave',
      (v) => {
        const beat = v.provenance.heartbeats[2]
        beat.response.errorCode = 15
        seal(beat)
      },
    ],
    [
      'non-leave response epoch',
      (v) => {
        const beat = v.provenance.heartbeats[2]
        beat.response.memberEpoch = 7
        seal(beat)
      },
    ],
    ['unordered heartbeat answer', (v) => (v.provenance.heartbeats[0].answeredAt = -1)],
    [
      'missing active response bytes',
      (v) => delete v.provenance.heartbeats[0].frames[0].responseBytes,
    ],
  ])('rejects %s after accepting the pristine recording', (_label, mutate) => {
    const recording = fixture()
    expect(() => verify(recording)).not.toThrow()
    mutate(recording.cycles[0].verifier)
    expect(() => verify(recording)).toThrow()
  })
})
