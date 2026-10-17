const helpers = require('testHelpers')
const schedule = require('../schedule')
const lane = require('../lane')
const resourceProbe = require('../resourceProbe')
const evidence = require('../e2eEvidence')
const fakeSocket = require('../fakeSocket')

describe('testHelpers', () => {
  test.each([
    ['Gate', schedule.Gate],
    ['Schedule', schedule.Schedule],
    ['interleavings', schedule.interleavings],
    ['mulberry32', schedule.mulberry32],
    ['createResourceProbe', resourceProbe.createResourceProbe],
    ['lanes', lane.lanes],
    ['currentLane', lane.currentLane],
    ['useEvidenceRun', evidence.useEvidenceRun],
    ['evidenceLines', evidence.evidenceLines],
    ['wireVersionsOf', evidence.wireVersionsOf],
    ['socketCounts', fakeSocket.socketCounts],
  ])('exports %s, the same one its own module does', (name, expected) => {
    expect(helpers[name]).toBe(expected)
  })

  test('lists the lazily loaded helpers among its keys', () => {
    expect(Object.keys(helpers)).toEqual(
      expect.arrayContaining(['Gate', 'Schedule', 'interleavings', 'lanes', 'createResourceProbe'])
    )
  })
})
