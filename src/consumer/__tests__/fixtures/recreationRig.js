const fetchPair = require('../../../../testHelpers/wire/fetchV18')
const { TOPIC_ID } = require('./fetchHarness')
const { CloseRig, batchAt } = require('./closeRig')

/** @param {string} id a topic id @returns {Buffer} its 16 bytes */
const uuidBytes = (id) => Buffer.from(id.replaceAll('-', ''), 'hex')

const OLD_ID = TOPIC_ID
const NEW_ID = '11121314-1516-1718-191a-1b1c1d1e1f20'

/**
 * A scripted topic recreated while a consumer is subscribed. The old incarnation holds records
 * 5..7 and the broker holds a commit at 5 for the group; the new one holds records 0..2 and its
 * log starts at 0. The broker keeps what an OffsetCommit last sent it, so a later start reads the
 * broker's own answer.
 */
class RecreationRig {
  /**
   * @param {{enforce?: boolean, retry?: object, protocol?: 'classic'|'consumer',
   *   log?: Record<string, Buffer>}} [options] `log` replaces the raw batches served at a
   *   `partition|offset` fetch position
   * @returns {Promise<RecreationRig>}
   */
  static async open({ enforce = true, retry = {}, protocol, log: served = {} } = {}) {
    const log = {}
    for (const offset of [0, 1, 2, 5, 6, 7]) log[`0|${offset}`] = await batchAt(offset)
    Object.assign(log, served)
    const rig = await CloseRig.open({
      enforce,
      retry,
      protocol,
      last: -1,
      log,
      committed: { 0: '5' },
    })
    return new RecreationRig(rig)
  }

  /** @param {CloseRig} rig */
  constructor(rig) {
    this.rig = rig
    this.cluster = rig.cluster
    this.rejected = new Set()
    this.failWith = 0
    this.topicId = OLD_ID
    const metadata = rig.cluster.answerers.get('Metadata')
    rig.cluster.answer('Metadata', (fields, record) => {
      const answer = metadata(fields, record)
      answer.topics[0].topicId = this.topicId
      return answer
    })
    const fetch = rig.cluster.answerers.get('OffsetFetch')
    rig.cluster.answer('OffsetFetch', (fields, record) => {
      const answer = fetch(fields, record)
      for (const entry of answer.topics)
        for (const cell of entry.partitions) cell.offset = this.offset
      return answer
    })
    const commit = rig.cluster.answerers.get('OffsetCommit')
    rig.cluster.answer('OffsetCommit', (fields, record) => {
      const answer = commit(fields, record)
      if (this.failWith) {
        this.rejected.add(record.correlationId)
        for (const entry of answer.topics)
          for (const cell of entry.partitions) cell.errorCode = this.failWith
      }
      return answer
    })
    this.lookup = rig.lookup()
    // The wire pair writes the harness's topic id; a recreated topic's responses name its new one.
    const build = fetchPair.buildResponse
    this.fetchSpy = jest.spyOn(fetchPair, 'buildResponse').mockImplementation((fields) => {
      const body = Buffer.from(build.call(fetchPair, fields))
      const at = body.indexOf(uuidBytes(OLD_ID))
      if (at >= 0) uuidBytes(this.topicId).copy(body, at)
      return body
    })
  }

  /** @returns {string} what the broker holds: the last commit it received and did not refuse */
  get offset() {
    const applied = this.rig
      .frames('OffsetCommit')
      .filter(({ correlationId }) => !this.rejected.has(correlationId))
    return applied.length > 0
      ? applied[applied.length - 1].fields.topics[0].partitions[0].offset
      : '5'
  }

  /**
   * Starts a consumer and waits for the old incarnation's records.
   * @param {object} [options] `CloseRig#started` options
   * @returns {Promise<{ consumer: object, delivered: string[] }>}
   */
  start(options) {
    return this.rig.started({ delivered: 3, ...options })
  }

  /**
   * Runs a consumer again after `stop()`, as the same consumer, so its reset debt is kept.
   * @param {object} consumer a stopped consumer
   * @param {string[]} seen receives the delivered values
   * @returns {Promise<void>}
   */
  async rerun(consumer, seen) {
    await consumer.run({
      autoCommit: true,
      autoCommitInterval: 60000,
      eachMessage: async ({ message }) => {
        seen.push(message.value.toString())
      },
    })
  }

  /** @returns {Promise<void>} the topic is deleted and recreated: the cluster reads the new id */
  async recreate() {
    this.topicId = NEW_ID
    await this.lookup.cluster.refreshMetadata()
  }

  /** @param {string} api @returns {object[]} the decoded fields of every frame of that API */
  sent(api) {
    return this.rig.frames(api).map(({ fields }) => fields)
  }

  /** @returns {Promise<void>} */
  async dispose() {
    this.fetchSpy.mockRestore()
    await this.rig.dispose()
  }
}

module.exports = { RecreationRig, OLD_ID, NEW_ID }
