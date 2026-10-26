const assert = require('node:assert/strict')

/** Requires the closing public-send probe as part of each scenario acceptance result.
 * The scenario records the acquire observation synchronously, before any await. No field is
 * inferred from later counter recovery; fatal owners supply separately identified replacements.
 * @param {object} result per-producer stamped probe observation
 * @returns {void} when admission, acknowledgement, retirement and close all passed
 * @throws {AssertionError} absent probe or any incomplete invariant
 */
const assertProbe = (result) => {
  assert(result, 'Closing admission probe omitted')
  assert.equal(result.instanceMatchesWorkload, true, 'Probe used another Admission')
  assert.equal(result.calls, 1, 'Probe acquire count')
  assert.equal(result.bytes, result.capacity, 'Probe did not charge capacity')
  assert.equal(result.inEntry, true, 'Probe was not admitted in its calling turn')
  assert.equal(result.reservationImmediate, true, 'Probe returned a waiter')
  assert.equal(result.acknowledged, true, 'Probe was not acknowledged')
  assert.equal(result.available, result.capacity, 'Probe did not return capacity')
  for (const field of [
    'reserved',
    'waitingBytes',
    'debts',
    'pins',
    'registrations',
    'settlePromises',
  ]) {
    assert.equal(result[field], 0, `Probe retained ${field}`)
  }
}
module.exports = { assertProbe }
