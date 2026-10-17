const { LANES, currentLane, lanes } = require('../lane')

describe('LANES', () => {
  test('are the fake and the docker lane, and cannot be changed', () => {
    expect(LANES).toEqual(['fake', 'docker'])
    expect(Object.isFrozen(LANES)).toBe(true)
  })
})

describe('currentLane', () => {
  test.each([
    ['docker', { KAFKAJS_LANE: 'docker' }],
    ['fake', {}],
    ['fake', { KAFKAJS_LANE: 'fake' }],
    ['fake', { KAFKAJS_LANE: 'DOCKER' }],
    ['fake', { KAFKAJS_LANE: '' }],
  ])('is %s for %j', (expected, env) => {
    expect(currentLane(env)).toBe(expected)
  })

  test('reads process.env by default', () => {
    const before = process.env.KAFKAJS_LANE
    try {
      process.env.KAFKAJS_LANE = 'docker'
      expect(currentLane()).toBe('docker')
      delete process.env.KAFKAJS_LANE
      expect(currentLane()).toBe('fake')
    } finally {
      if (before === undefined) {
        delete process.env.KAFKAJS_LANE
      } else {
        process.env.KAFKAJS_LANE = before
      }
    }
  })
})

describe('lanes', () => {
  test.each([
    [['fake'], {}],
    [['docker'], { KAFKAJS_LANE: 'docker' }],
    [['fake'], { KAFKAJS_LANE: 'docker', KAFKAJS_SCENARIO_LANES: 'fake' }],
    [['docker'], { KAFKAJS_SCENARIO_LANES: 'docker' }],
    [['fake', 'docker'], { KAFKAJS_SCENARIO_LANES: 'fake,docker' }],
    [['fake', 'docker'], { KAFKAJS_SCENARIO_LANES: 'docker, fake' }],
    [['fake'], { KAFKAJS_SCENARIO_LANES: 'fake,fake' }],
    [['docker'], { KAFKAJS_LANE: 'docker', KAFKAJS_SCENARIO_LANES: '' }],
  ])('is %j for %j', (expected, env) => {
    expect(lanes(env)).toEqual(expected)
  })

  test.each(['chaos', 'fake,chaos', 'Docker'])('rejects the unknown lane in %p', (value) => {
    expect(() => lanes({ KAFKAJS_SCENARIO_LANES: value })).toThrow(
      /KAFKAJS_SCENARIO_LANES names \[.*\], expected fake and\/or docker/
    )
  })
})

describe('lanes with the process environment', () => {
  test('reads process.env by default', () => {
    const before = { ...process.env }
    try {
      delete process.env.KAFKAJS_SCENARIO_LANES
      process.env.KAFKAJS_LANE = 'docker'
      expect(lanes()).toEqual(['docker'])
      process.env.KAFKAJS_SCENARIO_LANES = 'fake,docker'
      expect(lanes()).toEqual(['fake', 'docker'])
    } finally {
      process.env = before
    }
  })
})

describe.each(lanes({ KAFKAJS_SCENARIO_LANES: 'fake,docker' }))(
  'describe.each over lanes() on the %s lane',
  (lane) => {
    test('is given the name of a lane', () => {
      expect(LANES).toContain(lane)
    })
  }
)
