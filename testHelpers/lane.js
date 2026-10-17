/**
 * The two places a scenario can run: `fake` is the unit lane (in process, over the scripted
 * cluster), `docker` is the compose cluster behind `npm run test:kafka`.
 * @type {readonly ['fake', 'docker']}
 */
const LANES = Object.freeze(['fake', 'docker'])

/**
 * The lane this process is in. `scripts/testWithKafka.sh` exports `KAFKAJS_LANE=docker`; a bare
 * `jest` run has no such variable and is the fake lane.
 * @param {NodeJS.ProcessEnv} [env]
 * @returns {'fake'|'docker'}
 */
const currentLane = (env = process.env) => (env.KAFKAJS_LANE === 'docker' ? 'docker' : 'fake')

/**
 * The lanes a scenario that holds without an injected fault runs on: `describe.each(lanes())`.
 * `KAFKAJS_SCENARIO_LANES` (for example `fake,docker`) overrides the current lane, so one run can
 * ask for both.
 * @param {NodeJS.ProcessEnv} [env]
 * @returns {Array<'fake'|'docker'>} one or both of the lanes, `fake` first
 * @throws {Error} when `KAFKAJS_SCENARIO_LANES` names a lane that does not exist, or none
 */
const lanes = (env = process.env) => {
  const requested = env.KAFKAJS_SCENARIO_LANES

  if (requested === undefined || requested === '') {
    return [currentLane(env)]
  }

  const names = requested.split(',').map((name) => name.trim())
  const unknown = names.filter((name) => !LANES.includes(name))
  if (unknown.length > 0) {
    throw new Error(
      `KAFKAJS_SCENARIO_LANES names ${JSON.stringify(unknown)}, expected ${LANES.join(' and/or ')}`
    )
  }

  return LANES.filter((lane) => names.includes(lane))
}

module.exports = { LANES, currentLane, lanes }
