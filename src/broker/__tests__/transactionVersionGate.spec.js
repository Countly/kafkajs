const apiKeys = require('../../protocol/requests/apiKeys')
const { createGroupApiBroker } = require('./fixtures/groupApiHarness')

const END_TXN = '../../protocol/requests/endTxn/fixtures/java'
const TXN_OFFSET_COMMIT = '../../protocol/requests/txnOffsetCommit/fixtures/java'

/** @returns {{ hex: string, fields: object }} an Apache 4.3.1 EndTxn vector */
const endTxnVector = (direction, version, caseId) =>
  require(`${END_TXN}/endTxn${direction}-v${version}-${caseId}.json`)

/** @returns {{ hex: string, fields: object }} an Apache 4.3.1 TxnOffsetCommit vector */
const txnOffsetCommitVector = (direction, version, caseId) =>
  require(`${TXN_OFFSET_COMMIT}/txnOffsetCommit${direction}-v${version}-${caseId}.json`)

/** EndTxn's successful response body at each version. */
const endTxnResponse = (version) =>
  Buffer.from(
    endTxnVector('Response', version, version === 5 ? 'producerIdAndEpoch' : 'ok').hex,
    'hex'
  )

/** TxnOffsetCommit's successful response body at each version. */
const txnOffsetCommitResponse = (version) =>
  Buffer.from(txnOffsetCommitVector('Response', version, 'ok').hex, 'hex')

describe('Broker > the transaction-version gate on the EndTxn and TxnOffsetCommit codecs', () => {
  const harnesses = []

  /** A real Broker on a fake socket, offering `apiKey` up to `maxVersion`. */
  const offering = (apiKey, maxVersion, respond) => {
    const harness = createGroupApiBroker({
      versions: { [apiKey]: { minVersion: 0, maxVersion } },
      respond: ({ apiVersion }) => respond(apiVersion),
    })
    harnesses.push(harness)
    return harness
  }

  afterEach(async () => {
    await Promise.all(harnesses.splice(0).map(({ pool }) => pool.destroy()))
  })

  describe('endTxn', () => {
    test('under transaction version 1 a 4.x broker is asked at v4, byte for byte', async () => {
      const { broker, requests } = offering(apiKeys.EndTxn, 5, endTxnResponse)
      const { hex, fields } = endTxnVector('Request', 4, 'commit')

      await expect(broker.endTxn({ ...fields, transactionVersion: 1 })).resolves.toEqual({
        throttleTime: 0,
        clientSideThrottleTime: 25,
        errorCode: 0,
      })
      expect(requests[0]).toMatchObject({ apiKey: apiKeys.EndTxn, apiVersion: 4 })
      expect(requests[0].body.toString('hex')).toEqual(hex)
    })

    test('without a transaction version it is version 1: v4, and no producer id comes back', async () => {
      const { broker, requests } = offering(apiKeys.EndTxn, 5, endTxnResponse)
      const { fields } = endTxnVector('Request', 4, 'abort')

      const result = await broker.endTxn(fields)

      expect(requests[0].apiVersion).toEqual(4)
      expect(result).not.toHaveProperty('producerId')
    })

    test('under transaction version 2 it is asked at v5 and resolves the bumped pair', async () => {
      const { broker, requests } = offering(apiKeys.EndTxn, 5, endTxnResponse)
      const { hex, fields } = endTxnVector('Request', 5, 'commit')

      await expect(broker.endTxn({ ...fields, transactionVersion: 2 })).resolves.toEqual({
        throttleTime: 0,
        clientSideThrottleTime: 25,
        errorCode: 0,
        producerId: '1000',
        producerEpoch: 4,
      })
      expect(requests[0]).toMatchObject({ apiKey: apiKeys.EndTxn, apiVersion: 5 })
      expect(requests[0].body.toString('hex')).toEqual(hex)
    })

    test('under transaction version 2 a broker offering only v3 is asked at v3', async () => {
      const { broker, requests } = offering(apiKeys.EndTxn, 3, endTxnResponse)
      const { hex, fields } = endTxnVector('Request', 3, 'abort')

      await broker.endTxn({ ...fields, transactionVersion: 2 })

      expect(requests[0].apiVersion).toEqual(3)
      expect(requests[0].body.toString('hex')).toEqual(hex)
    })
  })

  describe('txnOffsetCommit', () => {
    test('under transaction version 1 a 4.x broker is asked at v4, group metadata included', async () => {
      const { broker, requests } = offering(apiKeys.TxnOffsetCommit, 5, txnOffsetCommitResponse)
      const { hex, fields } = txnOffsetCommitVector('Request', 4, 'groupMetadata')

      await expect(broker.txnOffsetCommit({ ...fields, transactionVersion: 1 })).resolves.toEqual({
        throttleTime: 0,
        clientSideThrottleTime: 25,
        topics: [{ topic: 'test-topic', partitions: [{ partition: 0, errorCode: 0 }] }],
      })
      expect(requests[0]).toMatchObject({ apiKey: apiKeys.TxnOffsetCommit, apiVersion: 4 })
      expect(requests[0].body.toString('hex')).toEqual(hex)
    })

    test('without a transaction version or group metadata it sends v4 with -1, "" and null', async () => {
      const { broker, requests } = offering(apiKeys.TxnOffsetCommit, 5, txnOffsetCommitResponse)
      const { hex, fields } = txnOffsetCommitVector('Request', 4, 'legacy')
      const { generationId, memberId, groupInstanceId, ...withoutMetadata } = fields

      await broker.txnOffsetCommit(withoutMetadata)

      expect(requests[0].apiVersion).toEqual(4)
      expect(requests[0].body.toString('hex')).toEqual(hex)
    })

    test('under transaction version 2 it is asked at v5', async () => {
      const { broker, requests } = offering(apiKeys.TxnOffsetCommit, 5, txnOffsetCommitResponse)
      const { hex, fields } = txnOffsetCommitVector('Request', 5, 'groupMetadata')

      await broker.txnOffsetCommit({ ...fields, transactionVersion: 2 })

      expect(requests[0]).toMatchObject({ apiKey: apiKeys.TxnOffsetCommit, apiVersion: 5 })
      expect(requests[0].body.toString('hex')).toEqual(hex)
    })

    test('a broker offering only v2 gets the committed leader epoch and no group metadata', async () => {
      const { broker, requests } = offering(apiKeys.TxnOffsetCommit, 2, txnOffsetCommitResponse)
      const { hex, fields } = txnOffsetCommitVector('Request', 2, 'leaderEpoch')

      await broker.txnOffsetCommit({
        ...fields,
        generationId: 5,
        memberId: 'test-member',
        groupInstanceId: 'test-instance',
        transactionVersion: 2,
      })

      expect(requests[0].apiVersion).toEqual(2)
      expect(requests[0].body.toString('hex')).toEqual(hex)
    })
  })
})
