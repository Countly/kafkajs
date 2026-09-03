#!/usr/bin/env node
// Waits for the 3 KRaft nodes, then warms the cluster (topics + a consumer-group describe).
const execa = require('execa')
const crypto = require('crypto')

const COMPOSE_FILE = process.env.COMPOSE_FILE || 'docker-compose.yml'
const BOOTSTRAP = 'kafka1:29092'
const BIN = '/opt/kafka/bin'
const secureRandom = (length = 10) => crypto.randomBytes(length).toString('hex')

const findContainerId = (node) => {
  const cmd = `docker ps --filter "status=running" --filter "label=custom.project=kafkajs" --filter "label=custom.service=${node}" --no-trunc -q`
  const containerId = execa.commandSync(cmd, { shell: true }).stdout.toString('utf-8').trim()
  if (!containerId)
    throw new Error(`container for ${node} is not running (COMPOSE_FILE=${COMPOSE_FILE})`)
  console.log(`${node}: ${containerId}`)
  return containerId
}

const kafkaCli = (containerId, args) =>
  execa
    .commandSync(`docker exec ${containerId} bash -c "${BIN}/${args} 2> /dev/null"`, {
      shell: true,
    })
    .stdout.toString('utf-8')

const waitForNode = (containerId) => {
  const deadline = Date.now() + 120000
  for (;;) {
    try {
      kafkaCli(containerId, `kafka-topics.sh --bootstrap-server ${BOOTSTRAP} --list`)
      console.log(`Kafka container ${containerId} is running`)
      return
    } catch (e) {
      if (Date.now() > deadline) throw e
      execa.commandSync('sleep 2', { shell: true })
    }
  }
}

const createTopic = (containerId, topicName) =>
  kafkaCli(
    containerId,
    `kafka-topics.sh --create --if-not-exists --topic ${topicName} --replication-factor 1 --partitions 2 --bootstrap-server ${BOOTSTRAP}`
  )

const consumerGroupDescribe = (containerId) =>
  kafkaCli(
    containerId,
    `kafka-consumer-groups.sh --bootstrap-server ${BOOTSTRAP} --group test-group-${secureRandom()} --describe`
  )

console.log('\nFinding container ids...')
const ids = ['kafka1', 'kafka2', 'kafka3'].map(findContainerId)

console.log('\nWaiting for nodes...')
ids.forEach(waitForNode)

console.log('\nAll nodes up:')
console.log(
  execa
    .commandSync(`docker compose -f ${COMPOSE_FILE} ps`, { shell: true })
    .stdout.toString('utf-8')
)

console.log('\nCreating default topics...')
createTopic(ids[0], 'test-topic-already-exists')

console.log('\nWarming up Kafka...')
for (let i = 0; i < 10; i++) createTopic(ids[0], `test-topic-${secureRandom()}`)
consumerGroupDescribe(ids[0])
