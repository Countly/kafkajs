/**
 * The Produce frames a real producer writes under `maxRequestSize`, with no broker: one producer
 * of a tree, over that tree's `ScriptedCluster`, whose sockets measure every Produce frame
 * written, length prefix included. Each arm sends a corpus to one topic:
 * - `corpus`: three sends of 52 records of 100 KiB, one partition each (5 MiB per partition);
 * - `fanout`: one send of one 1 KiB record to each of 3 000 partitions;
 * - `gzip`: the corpus, gzip-compressed;
 * - `incompressible`: the corpus of random bytes, gzip-compressed.
 * For every request the producer builds, the frame length it is about to write is compared with
 * the request-size plan of this script's tree (`recordSize.js`) for the same batches, less
 * `--undercount` bytes per batch: `modelOver` counts the frames above that plan, so an
 * undercounting plan is caught by the real frames. A tree with no plan of its own writes what it
 * always wrote, which is how the previous tree is measured.
 * Usage: node scripts/benchmarks/produceFrameSize.js [--arm corpus] [--max-request-size 1048576]
 *          [--undercount 0] [--tree <dir>]
 * Prints one JSON line: { tree, arm, maxRequestSize, sends, frames, framesPerSend,
 * largestFrameBytes, attempts, modelOver }. A bad argument exits 2. The run happens in a child
 * process whose `NODE_PATH` holds the tree, because the scripted cluster's fixtures require
 * `testHelpers` by name, as the test runner resolves it.
 */
const crypto = require('crypto')
const path = require('path')
const { spawnSync } = require('child_process')
const {
  encodedBound,
  framing,
  topicFraming,
  recordsBytes,
  BATCH_OVERHEAD,
  PARTITION_FRAMING,
} = require('../../src/producer/state/recordSize')

const TOPIC = 'frame-size'
const TOPIC_ID = '00000000-0000-4000-8000-00000000f5a1'
const CLIENT_ID = 'scripted-cluster'
const CORPUS = { partitions: 3, sends: 3, perSend: 52, valueBytes: 102400, codec: 'None' }
/** The corpus of each arm. */
const ARMS = {
  corpus: CORPUS,
  fanout: { partitions: 3000, sends: 1, perSend: 1, valueBytes: 1024, codec: 'None', fanout: true },
  gzip: { ...CORPUS, codec: 'GZIP' },
  incompressible: { ...CORPUS, codec: 'GZIP', random: true },
}
const DEFAULTS = {
  arm: 'corpus',
  maxRequestSize: 1048576,
  undercount: 0,
  tree: path.join(__dirname, '../..'),
}

/**
 * Reads the flags of `argv` over `DEFAULTS`.
 * @param {string[]} argv
 * @returns {typeof DEFAULTS}
 * @throws {Error} for an unknown flag or arm, or a number that is not an integer in range
 */
const parseArgs = (argv) => {
  const options = { ...DEFAULTS }
  for (let i = 0; i < argv.length; i += 2) {
    const name = argv[i].replace(/^--/, '').replace(/-(\w)/g, (_, c) => c.toUpperCase())
    if (!(name in DEFAULTS) || argv[i + 1] === undefined) {
      throw new Error(`unknown or incomplete flag ${argv[i]}`)
    }
    if (name === 'tree') {
      options.tree = path.resolve(argv[i + 1])
    } else if (name === 'arm') {
      if (!(argv[i + 1] in ARMS)) throw new Error(`--arm is one of ${Object.keys(ARMS).join(', ')}`)
      options.arm = argv[i + 1]
    } else {
      const value = Number(argv[i + 1])
      const least = name === 'undercount' ? 0 : 1
      if (!Number.isInteger(value) || value < least || value > 2147483647) {
        throw new Error(`${argv[i]} needs an integer from ${least} to 2147483647`)
      }
      options[name] = value
    }
  }
  return options
}

/**
 * The request-size plan of this script's tree for one Produce request, less `undercount` per batch.
 * @param {{ topicData: { topic: string, partitions: { messages: object[] }[] }[], compression?: number }} request
 * @param {number} undercount
 * @returns {number}
 */
const planOf = ({ topicData, compression }, undercount) => {
  let bytes = framing(CLIENT_ID)
  for (const { topic, partitions } of topicData) {
    bytes += topicFraming(topic)
    for (const { messages } of partitions) {
      bytes += PARTITION_FRAMING + BATCH_OVERHEAD - undercount
      bytes += encodedBound(compression, recordsBytes(messages))
    }
  }
  return bytes
}

/**
 * The scripted cluster of a tree, with one topic of `partitions` partitions led by its one node,
 * answering every Produce with success, and its sockets measuring each Produce frame written.
 * @param {string} tree
 * @param {number} partitions
 * @param {number[]} frames where the lengths go
 * @returns {object} the cluster
 */
const clusterOf = (tree, partitions, frames) => {
  const { createScriptedCluster } = require(path.join(tree, 'testHelpers/scriptedCluster'))
  const cluster = createScriptedCluster({ nodes: [{ id: 0, host: 'node0', port: 9092 }] })
  const factory = cluster.socketFactory
  cluster.socketFactory = (options) => {
    const socket = factory(options)
    const write = socket.write
    socket.write = (buffer, ...rest) => {
      if (Buffer.from(buffer).readInt16BE(4) === 0) frames.push(buffer.length)
      return write(buffer, ...rest)
    }
    return socket
  }
  const partition = (partitionIndex) => ({
    errorCode: 0,
    partitionIndex,
    leaderId: 0,
    leaderEpoch: 5,
    replicas: [0],
    isr: [0],
    offlineReplicas: [],
  })
  return cluster
    .answer('Metadata', () => ({
      brokers: [{ nodeId: 0, host: 'node0', port: 9092, rack: null }],
      controllerId: 0,
      topics: [
        {
          errorCode: 0,
          name: TOPIC,
          topicId: TOPIC_ID,
          isInternal: false,
          topicAuthorizedOperations: -2147483648,
          partitions: Array.from({ length: partitions }, (_, index) => partition(index)),
        },
      ],
    }))
    .answer('Produce', ({ topics }) => ({
      responses: topics.map(({ partitions: entries, ...key }) => ({
        ...key,
        partitions: entries.map(({ partition: index }) => ({
          partition: index,
          errorCode: 0,
          baseOffset: '0',
          logAppendTime: '-1',
          logStartOffset: '0',
          leader: null,
        })),
      })),
      nodeEndpoints: [],
    }))
}

/**
 * The records of one send of an arm.
 * @param {typeof CORPUS & { random?: boolean, fanout?: boolean }} shape
 * @param {number} send
 * @returns {object[]}
 */
const messagesOf = ({ partitions, perSend, valueBytes, random, fanout }, send) => {
  const value = () =>
    random ? crypto.randomBytes(valueBytes) : `${send}:`.padEnd(valueBytes, 'abcdefgh')
  return fanout
    ? Array.from({ length: partitions }, (_, partition) => ({ value: value(), partition }))
    : Array.from({ length: perSend }, () => ({ value: value(), partition: send % partitions }))
}

/**
 * Runs one arm in this process.
 * @param {typeof DEFAULTS} options
 * @returns {Promise<{ tree: string, arm: string, maxRequestSize: number, sends: number,
 *   frames: number, framesPerSend: number, largestFrameBytes: number, attempts: number,
 *   modelOver: number }>}
 */
const run = async ({ tree, arm, maxRequestSize, undercount }) => {
  const shape = ARMS[arm]
  const { Kafka, CompressionTypes, logLevel } = require(path.join(tree, 'index.js'))
  const Broker = require(path.join(tree, 'src/broker'))
  const frames = []
  let attempts = 0
  let modelOver = 0
  const produce = Broker.prototype.produce
  Broker.prototype.produce = function (request) {
    attempts += 1
    const plan = planOf(request, undercount)
    const { guard } = request
    const measured = (frameLength) => {
      if (frameLength !== undefined && frameLength > plan) modelOver += 1
      return guard ? guard(frameLength) : null
    }
    return produce.call(this, { ...request, guard: measured })
  }
  const cluster = clusterOf(tree, shape.partitions, frames)
  const producer = new Kafka({ ...cluster.kafkaOptions(), logLevel: logLevel.NOTHING }).producer({
    idempotent: true,
    linger: 0,
    maxRequestSize,
  })
  try {
    await producer.connect()
    for (let send = 0; send < shape.sends; send += 1) {
      await producer.send({
        topic: TOPIC,
        compression: CompressionTypes[shape.codec],
        messages: messagesOf(shape, send),
      })
    }
  } finally {
    await producer.disconnect()
    Broker.prototype.produce = produce
  }
  return {
    tree,
    arm,
    maxRequestSize,
    sends: shape.sends,
    frames: frames.length,
    framesPerSend: frames.length / shape.sends,
    largestFrameBytes: Math.max(0, ...frames),
    attempts,
    modelOver,
  }
}

module.exports = { parseArgs, run, planOf, ARMS }

if (require.main === module) {
  const argv = process.argv.slice(2)
  let options
  try {
    options = parseArgs(argv)
  } catch (e) {
    console.error(e.message)
    process.exit(2)
  }
  const paths = (process.env.NODE_PATH ?? '').split(path.delimiter).filter(Boolean)
  if (!paths.includes(options.tree)) {
    const child = spawnSync(process.execPath, [__filename, ...argv], {
      stdio: 'inherit',
      env: { ...process.env, NODE_PATH: [options.tree, ...paths].join(path.delimiter) },
    })
    process.exit(child.status ?? 1)
  }
  run(options).then(
    (result) => console.log(JSON.stringify(result)),
    (e) => {
      console.error(e)
      process.exit(1)
    }
  )
}
