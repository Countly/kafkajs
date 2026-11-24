---
id: transactions
title: Transactions
---

KafkaJS provides a simple interface to support Kafka transactions.

## <a name="transaction-messages"></a> Sending Messages within a Transaction

You initialize a transaction by making an async call to `producer.transaction()`. The returned transaction object has the methods `send` and `sendBatch` with an identical signature to the producer. When you are done you call `transaction.commit()` or `transaction.abort()` to end the transaction. A transactionally aware consumer will only read messages which were committed.

A producer created with a `transactionalId` is idempotent: every send waits for all in-sync replicas (`acks: -1`) and is retried in order with its own sequence numbers, so a retry can never write a record twice. A `transactionalId` makes idempotence explicit, so an `acks`, `maxInFlightRequests` (above 5) or `retry.retries: 0` that idempotence cannot honour throws instead of being worked around. There is no need for `maxInFlightRequests: 1`: a transactional producer keeps up to `maxInFlightRequests` (at most 5) requests in flight per broker, like any idempotent producer, and order is kept per partition.

```javascript
const client = new Kafka({
  clientId: 'transactional-client',
  brokers: ['kafka1:9092', 'kafka2:9092'],
})
const producer = client.producer({ transactionalId: 'my-transactional-producer' })
```

Within a transaction, you can produce one or more messages. If `transaction.abort` is called, all messages will be rolled back.

```javascript
const transaction = await producer.transaction()

try {
  await transaction.send({ topic, messages })
} catch (e) {
  await transaction.abort()
  throw e
}

try {
  await transaction.commit()
} catch (e) {
  if (!e.retriable) {
    await transaction.abort()
    throw e
  }
  // The commit's outcome is unknown and it may have landed: never abort it, commit again.
  await transaction.commit()
}
```

`commit()` and `abort()` first wait for every `send`, `sendBatch` and `sendOffsets` of the transaction to settle, so a send you did not await is never left out of a commit; `abort()` rejects the sends still waiting to go out before it waits for the rest. Once `commit()` or `abort()` has been called, the transaction accepts no new sends. A `commit()` called while another is under way joins it: one `EndTxn`, and the same outcome for both. An `abort()` called while a `commit()` is under way waits for it: it ends the transaction if the commit failed with an `abortable` error, and is refused if the commit ended the transaction, left it [pending](#errors) or left the producer unusable (it then rejects with the same `KafkaJSApplicationRecoverableError` and sends no abort).

### Choosing a `transactionalId`

The `transactionalId` allows Kafka to fence out zombie instances by rejecting writes from producers with the same `transactionalId`, allowing only writes from the most recently registered producer. To ensure EoS semantics in a stream processing application, it is important that the `transactionalId` is always the same for a given input topic and partition in the read-process-write cycle.

The simplest way to achieve this is to encode the topic and partition in the `transactionalId` itself such as the scheme: `"myapp-producer-" + topic + "-" + partition`.

[This article from Confluent](https://www.confluent.io/blog/transactions-apache-kafka/) goes into much greater detail on how transactions work and is a recommended read before deciding on a `transactionalId`.

### Transaction timeout

`transactionTimeout` defaults to 60000 ms, as in the Java `KafkaProducer`. The 10 s often quoted for exactly-once processing is a Kafka Streams override, not a producer default. A consume-transform-produce loop may still want a lower value: while a crashed instance's transaction stays open, the offsets it sent hold back the consumer that takes its partitions over, until the transaction times out.

A transaction left open longer than `transactionTimeout` is aborted by the transaction coordinator, which checks for timed-out transactions every 10 s by default (`transaction.abort.timed.out.transaction.cleanup.interval.ms`), and none of its records become visible. A `commit()` of it then rejects with `KafkaJSApplicationRecoverableError`. Its `abort()` resolves under transaction version 2, and the producer carries on; under version 1 the coordinator has fenced the producer's epoch, so `abort()` rejects with `KafkaJSApplicationRecoverableError` as well, and the producer has to be replaced. A commit that races that abort, or a pending one sent again after it, is refused with `INVALID_TXN_STATE` (48) or `PRODUCER_FENCED` (90), depending on the transaction version and on whether the broker has [KAFKA-20785](https://issues.apache.org/jira/browse/KAFKA-20785) (fixed in 4.3.2 and 4.5.0): both reject `commit()` with `KafkaJSApplicationRecoverableError`.

The coordinator also uses `transactionTimeout` as the timeout of its own writes to its transaction log, so a value of a few hundred milliseconds or less can make those writes time out, and an abort the coordinator owes can then stay pending until it retries the write ([KAFKA-16047](https://issues.apache.org/jira/browse/KAFKA-16047)). A producer that takes over a `transactionalId` whose previous owner left a transaction open waits for that abort: `producer.transaction()` retries `CONCURRENT_TRANSACTIONS` within the producer's `retry` budget - about 9 s on the defaults, where the Java client's `initTransactions()` waits up to `max.block.ms`, 60 s by default. Raise `retry.retries` to wait longer (and `retry.maxRetryTime` once the delays reach it); `retry.maxTotalTime` only shortens the wait.

## <a name="offsets"></a> Sending Offsets

To send offsets as part of a transaction, meaning they will be committed only if the transaction succeeds, use the `transaction.sendOffsets()` method. This is necessary whenever we want a transaction to produce messages derived from a consumer, in a "consume-transform-produce" loop.

Pass the consumer's group membership, from `consumer.groupMetadata()`, so the group coordinator can fence a member that was replaced in a rebalance (KIP-447):

```javascript
await transaction.sendOffsets({
  consumerGroupMetadata: consumer.groupMetadata(),
  topics,
})
```

`groupMetadata()` is a snapshot: read it again after every rebalance, for instance in each `eachBatch` call. The earlier form, `{ consumerGroupId, topics }`, still works and commits with no member to fence, as before; pass exactly one of the two.

`topics` has the following structure:

```javascript
[{
  topic: <String>,
  partitions: [{
    partition: <Number>,
    offset: <String>,
    leaderEpoch: <Number>, // optional: the leader epoch of the consumed record (KIP-320)
  }]
}]
```

If the offset commit is refused because the consumer is no longer the member it was — `ILLEGAL_GENERATION` or `UNKNOWN_MEMBER_ID` under the classic protocol, `STALE_MEMBER_EPOCH` under `groupProtocol: 'consumer'`, or `FENCED_INSTANCE_ID` — `sendOffsets` rejects and the transaction can only be aborted: abort it, let the consumer rejoin, and process the records again from the committed offsets. A `GROUP_ID_NOT_FOUND` from `TxnOffsetCommit` v6 (a group the coordinator does not know) is handled the same way.

Transactional offset commits use topic IDs when every requested topic has a known ID and the broker supports `TxnOffsetCommit` v6 (Kafka 4.4, KIP-1319). If any ID is unavailable, the whole request stays at v5 or below; transaction-version constraints may require an older version. Group metadata is preserved. Offsets become published only after a confirmed transaction commit. Group metadata mismatch errors require aborting the transaction. An unknown topic ID refreshes the producer's metadata, without creating a topic or adding one to the producer's targets, and uses the existing bounded retry.

`sendOffsets` reads each topic's ID from the producer's metadata when it is called, and every retry sends those same IDs. Before each attempt is written, the call is refused with `KafkaJSTransactionAbortableError` (`cause`: a `KafkaJSCommitFailedError` with `reason: 'stale-assignment'`) when the producer's metadata holds a different ID for a topic than it did at the call, an ID where it had none, or none where it had one. The producer then can no longer verify that the offsets land on the topic they were given for. A changed ID means the topic was recreated. A missing ID can also mean that the producer's metadata was cleared or the topic was dropped from it (a rebootstrap, idle pruning) while the broker still has the same topic, so a refusal does not prove a recreation. Either way, abort the transaction and process the records again. After an unknown topic ID, the refresh lets the retry see a recreation of a topic the producer sends to. The first attempt fixes how the request names topics: a retry of a request keyed by name stays keyed by name, and a request sent keyed by ID is never retried keyed by name (if the group coordinator moves to a broker that offers `TxnOffsetCommit` v5 at most, the call is refused the same way). Under transaction version 1, or when a topic had no known ID at the call, the request is keyed by name and only this client-side check applies: it compares what the producer's metadata knew at the call with what it knows before each write, so it cannot detect a recreation that metadata has not seen. The check starts when `sendOffsets` is called: it cannot tell whether offsets you computed earlier were read from the topic that has the name now. At v6, a topic with no partition is left out of the request. An answer that cannot be matched to the request rejects `sendOffsets` with an abortable error: the producer stays usable, and the caller aborts the transaction.

`sendOffsets` snapshots its topics, partitions, offsets, leader epochs and metadata when called. A topic or partition named more than once in one call is sent once, with the partition's last entry, as kafkajs 2.2.4 and the Java client commit it. Calls on one transaction run one at a time in call order, including retries. The last successful call for a partition selects its value, even when offsets move backward; it does not drop the batch an earlier call depended on (see [Seeking while a transaction is open](#seeking)).

Offsets enter the committed-offset cache shared by clients from one `Kafka` instance only after a confirmed transaction commit, once. A newer seek, reset, commit or topic recreation fences an older transaction publication per partition. An abort or fatal transaction state publishes nothing. Exactly-once applications must disable consumer auto-commit: before the transaction commits, a consumer can still attempt to commit the same offsets itself.

An unconfirmed commit keeps its offsets staged for a confirmed retry. A commit confirmed during a graceful `producer.disconnect()` publishes those offsets once, after the EndTxn response. A forced close forfeits all unconfirmed local publication and releases the offset tickets; a late answer or a later reconnect cannot publish those forfeited offsets. The broker may still have committed an unknown EndTxn: read broker offsets separately, for example with a fresh `admin.fetchOffsets`. An open or abortable transaction is aborted during graceful close; a transaction already committing is never aborted by close.

### <a name="seeking"></a> Seeking while a transaction is open

A `sendOffsets()` call made from an ordinary consumer handler, or from asynchronous work that keeps that handler's context, depends on the handler's batch when its offsets include the handler's group and partition, with either group argument. The transaction keeps that dependency until its commit `EndTxn` is written, also when a later call replaces the partition's offset. A seek of the partition, a topic recreation or the end of the batch's assignment supersedes the batch for good, also a seek to the same offset and one already applied. The assignment ends when the partition is revoked, and for every partition, kept ones included, in a rebalance with eager assigners (the default), after the group drops the member, and when the consumer stops. A manual `assign()` can change only after `stop()`, so any change of it ends every batch, also of partitions that stay assigned.

Before the commit `EndTxn` is written, a superseded batch makes `sendOffsets()` or `commit()` reject with `KafkaJSTransactionAbortableError`, whose `cause` is a `KafkaJSCommitFailedError` with `reason: 'stale-assignment'`. No partition is silently left out of the offsets: the whole transaction, its output records and the healthy offsets in it included, can only be aborted, and calling `sendOffsets()` again cannot repair it. A `sendOffsets()` that already resolved stays resolved, but `commit()` can still be refused. So a handler that seeks its own partition commits or aborts its transaction first, then seeks. Work started inside a handler, such as a timer, an interval or a worker it creates, keeps that handler's batch for its whole life: after a refusal, abort and process the records again from a handler of a batch fetched after the seek, not from that work, whose next transaction would be refused the same way.

The client cannot recall bytes already written. A `TxnOffsetCommit` written before the seek may be staged at the broker, but it is not committed while the commit `EndTxn` is refused. Once the commit `EndTxn` has been written the broker may already have committed: success stays success, an unknown outcome is still resolved by calling `commit()` again, and the offsets the broker committed reach the shared committed-offset cache as usual.

When several consumers feed one transaction, call `sendOffsets()` in each consumer's own handler: a seek by one consumer refuses the whole transaction only when that consumer's batch is among its inputs. `consumer.groupMetadata()` identifies a membership, not a batch, so passing it carries no batch. Offsets sent outside any handler, for another partition or for another group are not fenced; if a worker or queue loses the handler's context, settle or abort its transaction yourself before seeking. This check is the client's own: neither the Java client nor franz-go fences a seek in transactions.

## <a name="errors"></a> When something fails

Use admin transaction diagnostics to inspect a stuck transaction from outside the producer: `describeTransactions({ transactionalIds })`, `describeProducers({ topicPartitions })` and `listTransactions({ states: ['Ongoing'] })`. Administrative recovery is separate from a producer's own transaction object. `abortTransaction` writes an abort marker for the specified producer epoch and partition; physical records remain, while `read_committed` hides aborted records. `fenceProducers` starts a new producer epoch through the transaction coordinator. The equivalent of Java's `forceTerminateTransaction(id)` is:

```javascript
await admin.fenceProducers({ transactionalIds: [id] })
```

The existing producer becomes unusable when it observes its fencing failure; create a new producer. An admin `apiTimeout` does not prove an already-sent abort or fence failed remotely. Check the returned per-id errors and re-read diagnostics before repeating recovery.

Every error carries a `category` saying what to do about it:

| `category`               | Meaning                                                                    | What to do                                                            |
| ------------------------ | -------------------------------------------------------------------------- | --------------------------------------------------------------------- |
| `retriable`              | A transient condition the client already retried.                          | Retry the operation; a send that failed inside a transaction still needs `abort()`. After `commit()`, call `commit()` again. |
| `refreshRetriable`       | The client's view of the cluster was stale; it refreshed it and retried.   | As `retriable`.                                                       |
| `abortable`              | The transaction cannot be committed, but the producer is fine.             | `abort()`, then start a new transaction on the same producer.         |
| `applicationRecoverable` | The producer cannot continue — for instance another one took over its `transactionalId`. | Create a new producer.                                   |
| `invalidConfiguration`   | A record, a setting or an ACL is wrong.                                    | Fix it. In a transaction, also `abort()`.                             |

A send that fails inside a transaction ends that transaction's chance to commit, whatever its category — unless it was refused before any record was queued, for its own arguments, for a record that cannot be encoded, or by a custom partitioner that threw or returned an invalid partition: the transaction is missing that send's records. A send the producer's buffer refuses (see [Memory](Producing.md#memory)), or one with a record no request can carry (see [Request size](Producing.md#request-size)), is the exception to that exception: `KafkaJSRecordTooLargeError` with `reason: 'bufferMemory'` or `'maxRequestSize'`, `KafkaJSBufferExhaustedError` and a `KafkaJSDeliveryTimeout` that came while it waited for room end the transaction's chance to commit although none of its records was queued, as the Java client's `send()` does. The send still rejects with that error itself, so a `KafkaJSBufferExhaustedError` stays retriable for you; but the transaction can only be aborted: catch it, `abort()`, and start a new transaction. From then on `commit()`, `send()`, `sendBatch()` and `sendOffsets()` reject with the error that ended it — a `KafkaJSTransactionAbortableError` when the transaction can be aborted, a `KafkaJSApplicationRecoverableError` when the producer cannot continue (an `applicationRecoverable` error, or a missing cluster or transactional-id ACL) — and `producer.transaction()` rejects with it too, until you abort. `abort()` always ends the transaction; it rejects only when the producer cannot continue, and while a commit is pending (below), when it is refused. A retriable error that ran out of retries inside a send or `sendOffsets` reaches you as a `KafkaJSTransactionAbortableError`, with the retries-exceeded error as its `cause`. A batch dropped because the producer was disconnecting or disconnected ends the transaction's chance to commit the same way; a fault in the client's own send loop makes the producer unusable. An error the client raised itself rather than the broker — a `TypeError` while encoding an offset, say — makes the transaction abort-only too, as a `KafkaJSTransactionAbortableError` whose `cause` is that error, and leaves the producer usable. A batch that was sent and then gave up may have been written all the same, so its partition's sequence stays unresolved: `commit()` and `abort()` check every partition's sequence before `EndTxn`, so such a gap makes the transaction abort-only if nothing did already ("The client hasn't received acknowledgment for some previously sent messages and can no longer retry them. It is safe to abort the transaction and continue."), and under transaction version 1 the abort then bumps the producer's epoch.

A `commit()` whose `EndTxn` outcome is unknown — its retries ran out on a retriable answer, a request timed out, or a connection failed — leaves the commit pending and rejects with the last retriable error itself: `error.retriable` is `true` and its `category` is `retriable` or `refreshRetriable` (it is not a `KafkaJSNumberOfRetriesExceeded`, so code that caught that class from `commit()` in 2.2.4 does not match). The coordinator may have committed the transaction before the answers stopped arriving, so do not abort it: call `commit()` again. It sends the same `EndTxn`, and a coordinator that committed answers it with success (under transaction version 2 with the producer's next epoch, which the producer adopts); until then `abort()`, `send()`, `sendBatch()`, `sendOffsets()` and `producer.transaction()` throw a `KafkaJSNonRetriableError` saying the commit did not complete. If the coordinator aborted the transaction on its timeout before the commit landed, the commit sent again rejects with `KafkaJSApplicationRecoverableError` (see [Transaction timeout](#transaction-timeout)): replace the producer and process the input again. `producer.disconnect()` while a commit is pending sends it once more, through one more retry budget, and waits for a `commit()` still under way rather than sending its own; it then disconnects whatever the answer, logging a warning that names the transactional id when the commit did not succeed.

Use `await producer.disconnect({ timeout: 10000 })` to bound shutdown. The timeout includes delivery, transaction cleanup and connection draining. With no timeout, close waits naturally; with `enforceRequestTimeout: false` it can wait without a bound. A zero timeout starts no new initialization, commit or abort attempt. Concurrent calls share the first timeout, and calls after a completed close do nothing until an explicit `connect()`. A fresh connection permits ordinary sends; it does not resolve an unknown transaction, whose `commit()` must be retried explicitly, or whose producer must be replaced. A close that cuts short an abort - your `abort()`, or the one `disconnect()` runs for an open transaction - rejects that abort with `KafkaJSProducerClosedError` and still ends the transaction locally, leaving the producer `READY`; after `connect()`, the next `producer.transaction()` first sends that abort again (see [Transaction versions](#transaction-versions)).

These categories describe what this client does, on any Kafka 4.x broker. A send rejects with its own error — the broker's, such as a `KafkaJSProtocolError` with code 90 when another producer took over the `transactionalId` — and the wrapper classes come from `commit()`, `abort()` and every later call. So after a failed `commit()`, check `error.retriable` first and, while it is set, commit again a few times at most; otherwise abort: `abort()` rejects only when the producer cannot continue, with a `KafkaJSApplicationRecoverableError`, and that is when to replace the producer.

```javascript
const { KafkaJSApplicationRecoverableError } = require('@countly/kafkajs')

const COMMIT_ATTEMPTS = 3

const replaceProducer = async () => {
  await producer.disconnect({ timeout: 10000 }) // bounds shutdown; an unknown commit stays unknown
  producer = client.producer({ transactionalId })
  await producer.connect()
}

const transaction = await producer.transaction()
let unknownOutcome = null
try {
  await transaction.send({ topic, messages })
  let commitError = null
  for (let attempt = 0; attempt < COMMIT_ATTEMPTS; attempt++) {
    commitError = await transaction.commit().then(() => null, (e) => e)
    if (commitError === null || !commitError.retriable) break
  }
  if (commitError?.retriable) {
    unknownOutcome = commitError // never abort it: the commit may have landed
  } else if (commitError !== null) {
    throw commitError
  }
} catch (e) {
  try {
    await transaction.abort()
  } catch (abortError) {
    if (!(abortError instanceof KafkaJSApplicationRecoverableError)) {
      throw abortError
    }
    await replaceProducer()
  }
}
if (unknownOutcome !== null) {
  await replaceProducer() // sends the commit once more; the coordinator decides it
  throw unknownOutcome
}
```

A commit that still fails retriably after a few attempts has an unknown outcome: the coordinator may have committed it. The example replaces the producer, whose bounded `disconnect()` may confirm that commit before its deadline, and then throws the retriable error to stop. Do not abort the transaction or treat an unknown outcome as failure at the broker. A consume-transform-produce loop re-reads from its broker committed offsets. Commit before disconnecting when you intend to commit an open transaction; graceful close otherwise aborts it.

## <a name="transaction-versions"></a> Transaction versions

Kafka 4.x brokers offer two versions of the transaction protocol, selected by the cluster's `transaction.version` feature (KIP-890). The producer reads it from the transaction coordinator at the start of every transaction; nothing needs configuring.

- **Version 2** (the default on Kafka 4.x): a produce adds its partition to the transaction itself, and so does the offset commit, so no `AddPartitionsToTxn` or `AddOffsetsToTxn` request is sent. Every commit and abort gives the producer a new epoch: if you log `producerEpoch`, expect it to change with every transaction. A partition leader that offers no Produce v12 cannot take part: the send fails with `KafkaJSServerDoesNotSupportApiKey` naming the broker and its versions, and the producer becomes unusable, as the Java client treats an unsupported version.
- **Version 1**: the producer adds each partition to the transaction before it writes to it, and the offsets before it commits them. After an abort that a failed send forced, or one that cancelled a batch already sent once (a re-send waiting out its backoff), the producer bumps its epoch (KIP-360), so the next transaction starts clean.

An abort whose outcome is unknown - its `EndTxn` failed until its retries ran out, or a close cut it short - ends the transaction locally, and the next `producer.transaction()` first sends that `EndTxn` again with the producer id and epoch the producer holds. A coordinator that still holds the transaction aborts it; one that already aborted it answers success; one still writing the abort answers `CONCURRENT_TRANSACTIONS`, which is retried. Under version 1 the producer then bumps its epoch (an `InitProducerId` with the same producer id and epoch); under version 2 it takes the epoch the `EndTxn` answer carries. If that fails retriably, `producer.transaction()` rejects and the next call sends it again. The producer never sends an `InitProducerId` without its producer id and epoch after its first one, so it cannot fence another producer that took over the same `transactionalId`. Instead, when another producer took over the `transactionalId`, the `EndTxn` sent again is refused with `PRODUCER_FENCED` (or `INVALID_PRODUCER_ID_MAPPING`): `producer.transaction()` rejects with a `KafkaJSApplicationRecoverableError` saying the coordinator no longer accepts the producer's id and epoch (another producer took over the `transactionalId`, its transaction timed out, or the `transactionalId` expired), and the producer must be replaced. A coordinator-side transaction timeout differs by version: under version 1 the coordinator's timeout abort fences the epoch, so the `EndTxn` sent again is refused the same way and the producer must be replaced; under version 2 the coordinator accepts it as a retry of the abort it already made, and the producer goes on with the next epoch. Under version 1 a transaction owes an `EndTxn` once it attempted an `AddPartitionsToTxn` or `AddOffsetsToTxn`, answered or not, since the coordinator may have applied a request whose answer was lost: its abort sends the `EndTxn` too. The attempt counts from before the coordinator lookup, so this also covers an add the coordinator refused and one whose lookup failed before the request was written: such an abort costs one extra `EndTxn`. If the coordinator applied none of this transaction's adds it answers success (its previous transaction ended in an abort) or `INVALID_TXN_STATE` (it has none yet, or the previous one committed); both mean there is nothing to abort, and the producer bumps its epoch and goes on. For a transaction with a confirmed add, `INVALID_TXN_STATE` stays fatal. A version 1 transaction that attempted none owes no `EndTxn`. Until the abort sent again reaches the coordinator - at the next `producer.transaction()`, or when the coordinator aborts the transaction on its own transaction timeout - a coordinator that still holds the transaction keeps it open, and `read_committed` consumers of its partitions wait at its first offset meanwhile. `disconnect()` does not send it: the transaction already ended locally, so a graceful close has nothing to abort. An application that will not start another transaction soon should call `producer.transaction()` again (and abort the new transaction if it has nothing to send); a failed attempt stays owed for the next call. When the cluster moves from version 1 to 2, the next transaction first bumps the producer's epoch; if a bump fails, `producer.transaction()` rejects and the next call tries again. A transaction in which nothing was sent and no offsets were added ends without contacting the broker, under either version.
