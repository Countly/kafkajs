const leaderElection = require('./leaderElection')

/**
 * What is run inside the `kafka1` container to learn the broker's version: the tool prints the
 * build of the image it ships in, which is the build of the broker next to it. Proven on each
 * supported line by the live step that records `scripts/protocol/brokerRanges.json`.
 * @see https://github.com/apache/kafka/blob/4.0.2/tools/src/main/java/org/apache/kafka/tools/BrokerApiVersionsCommand.java
 * @type {readonly string[]}
 */
const BROKER_VERSION_ARGV = Object.freeze([
  '/opt/kafka/bin/kafka-broker-api-versions.sh',
  '--version',
])

/**
 * Reads the broker version off what `BROKER_VERSION_ARGV` printed. The first non-empty line must
 * start with `major.minor.patch`, followed by whitespace or the end of the line, so a snapshot
 * (`4.3.1-SNAPSHOT`), a four-part number or a warning printed ahead of the version is not mistaken
 * for a release.
 * @param {string|Buffer} output what the version command printed
 * @returns {string} the `major.minor.patch` that starts that line
 * @throws {Error} when that line does not start with such a version
 */
const parseBrokerVersion = (output) => {
  const line = String(output)
    .split('\n')
    .map((text) => text.trim())
    .find(Boolean)
  const match = line && /^(\d+\.\d+\.\d+)(?:\s|$)/.exec(line)
  if (!match) {
    throw new Error(
      `Cannot read a broker version from ${JSON.stringify(String(output).slice(0, 80))}`
    )
  }

  return match[1]
}

/**
 * Asks the running compose cluster's `kafka1` container for its Kafka version.
 * @returns {string} `major.minor.patch`
 * @throws {Error} when the container cannot be reached or prints no version
 */
const readBrokerVersion = () =>
  parseBrokerVersion(leaderElection.execInKafka1([...BROKER_VERSION_ARGV]))

module.exports = { BROKER_VERSION_ARGV, parseBrokerVersion, readBrokerVersion }
