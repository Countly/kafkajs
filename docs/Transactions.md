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

  await transaction.commit()
} catch (e) {
  await transaction.abort()
}
```

`commit()` and `abort()` first wait for every `send`, `sendBatch` and `sendOffsets` of the transaction to settle, so a send you did not await is never left out of a commit; `abort()` rejects the sends still waiting to go out before it waits for the rest. Once `commit()` or `abort()` has been called, the transaction accepts no new sends; an `abort()` called while a `commit()` is under way waits for it, and ends the transaction only if the commit failed.

### Choosing a `transactionalId`

The `transactionalId` allows Kafka to fence out zombie instances by rejecting writes from producers with the same `transactionalId`, allowing only writes from the most recently registered producer. To ensure EoS semantics in a stream processing application, it is important that the `transactionalId` is always the same for a given input topic and partition in the read-process-write cycle.

The simplest way to achieve this is to encode the topic and partition in the `transactionalId` itself such as the scheme: `"myapp-producer-" + topic + "-" + partition`.

[This article from Confluent](https://www.confluent.io/blog/transactions-apache-kafka/) goes into much greater detail on how transactions work and is a recommended read before deciding on a `transactionalId`.

### Transaction timeout

`transactionTimeout` defaults to 60000 ms, as in the Java `KafkaProducer`. The 10 s often quoted for exactly-once processing is a Kafka Streams override, not a producer default. A consume-transform-produce loop may still want a lower value: while a crashed instance's transaction stays open, the offsets it sent hold back the consumer that takes its partitions over, until the transaction times out.

A transaction left open longer than `transactionTimeout` is aborted by the transaction coordinator, which checks for timed-out transactions every 10 s by default (`transaction.abort.timed.out.transaction.cleanup.interval.ms`), and none of its records become visible. A `commit()` of it then rejects with `KafkaJSApplicationRecoverableError`. Its `abort()` resolves under transaction version 2, and the producer carries on; under version 1 the coordinator has fenced the producer's epoch, so `abort()` rejects with `KafkaJSApplicationRecoverableError` as well, and the producer has to be replaced.

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

If the offset commit is refused because the consumer is no longer the member it was — `ILLEGAL_GENERATION` or `UNKNOWN_MEMBER_ID` under the classic protocol, `STALE_MEMBER_EPOCH` under `groupProtocol: 'consumer'`, or `FENCED_INSTANCE_ID` — `sendOffsets` rejects and the transaction can only be aborted: abort it, let the consumer rejoin, and process the records again from the committed offsets.

As before, `sendOffsets` marks the offsets committed in the client's own cache as soon as they are sent, not when the transaction commits.

## <a name="errors"></a> When something fails

Every error carries a `category` saying what to do about it:

| `category`               | Meaning                                                                    | What to do                                                            |
| ------------------------ | -------------------------------------------------------------------------- | --------------------------------------------------------------------- |
| `retriable`              | A transient condition the client already retried.                          | Retry the operation, or abort the transaction.                        |
| `refreshRetriable`       | The client's view of the cluster was stale; it refreshed it and retried.   | As `retriable`.                                                       |
| `abortable`              | The transaction cannot be committed, but the producer is fine.             | `abort()`, then start a new transaction on the same producer.         |
| `applicationRecoverable` | The producer cannot continue — for instance another one took over its `transactionalId`. | Create a new producer.                                   |
| `invalidConfiguration`   | A record, a setting or an ACL is wrong.                                    | Fix it. In a transaction, also `abort()`.                             |

A send that fails inside a transaction ends that transaction's chance to commit, whatever its category — unless it was refused before any record was queued, for its own arguments or by a custom partitioner that threw: the transaction is missing that send's records. From then on `commit()`, `send()`, `sendBatch()` and `sendOffsets()` reject with the error that ended it — a `KafkaJSTransactionAbortableError` when the transaction can be aborted, a `KafkaJSApplicationRecoverableError` when the producer cannot continue (an `applicationRecoverable` error, or a missing cluster or transactional-id ACL) — and `producer.transaction()` rejects with it too, until you abort. `abort()` always ends the transaction; it rejects only when the producer cannot continue. A retriable error that ran out of retries inside a transaction reaches you as a `KafkaJSTransactionAbortableError`, with the retries-exceeded error as its `cause`. A batch dropped because the producer was disconnecting or disconnected ends the transaction's chance to commit the same way; a fault in the client's own send loop makes the producer unusable. An error the client raised itself rather than the broker — a `TypeError` while encoding a header value or an offset, say — makes the transaction abort-only too, as a `KafkaJSTransactionAbortableError` whose `cause` is that error, and leaves the producer usable. A batch that was sent and then gave up may have been written all the same, so its partition's sequence stays unresolved: `commit()` and `abort()` check every partition's sequence before `EndTxn`, so such a gap makes the transaction abort-only if nothing did already ("The client hasn't received acknowledgment for some previously sent messages and can no longer retry them. It is safe to abort the transaction and continue."), and under transaction version 1 the abort then bumps the producer's epoch.

A `commit()` whose own `EndTxn` ran out of retries rejects with a `KafkaJSTransactionAbortableError` whose `cause` is the `KafkaJSNumberOfRetriesExceeded`, and it is ambiguous: the coordinator may have committed the transaction before the answers stopped arriving. `abort()` is still allowed, as in 2.2.4, but if the commit did land, the abort is refused by the coordinator and the producer becomes unusable — while the transaction's records stay committed. Before you reprocess the input of such a transaction, check whether its output was written, or make the processing idempotent.

These categories describe what this client does, on any Kafka 4.x broker. A send rejects with its own error — the broker's, such as a `KafkaJSProtocolError` with code 90 when another producer took over the `transactionalId` — and the wrapper classes come from `commit()`, `abort()` and every later call. So abort first: `abort()` rejects only when the producer cannot continue, with a `KafkaJSApplicationRecoverableError`, and that is when to replace the producer.

```javascript
const { KafkaJSApplicationRecoverableError } = require('@countly/kafkajs')

const transaction = await producer.transaction()
try {
  await transaction.send({ topic, messages })
  await transaction.commit()
} catch (e) {
  try {
    await transaction.abort()
  } catch (abortError) {
    if (!(abortError instanceof KafkaJSApplicationRecoverableError)) {
      throw abortError
    }
    await producer.disconnect()
    producer = client.producer({ transactionalId })
    await producer.connect()
  }
}
```

## <a name="transaction-versions"></a> Transaction versions

Kafka 4.x brokers offer two versions of the transaction protocol, selected by the cluster's `transaction.version` feature (KIP-890). The producer reads it from the transaction coordinator at the start of every transaction; nothing needs configuring.

- **Version 2** (the default on Kafka 4.x): a produce adds its partition to the transaction itself, and so does the offset commit, so no `AddPartitionsToTxn` or `AddOffsetsToTxn` request is sent. Every commit and abort gives the producer a new epoch: if you log `producerEpoch`, expect it to change with every transaction. A partition leader that offers no Produce v12 cannot take part: the send fails with `KafkaJSServerDoesNotSupportApiKey` naming the broker and its versions, and the producer becomes unusable, as the Java client treats an unsupported version.
- **Version 1**: the producer adds each partition to the transaction before it writes to it, and the offsets before it commits them. After an abort that a failed send forced, or one that cancelled a batch already sent once (a re-send waiting out its backoff), the producer bumps its epoch (KIP-360), so the next transaction starts clean.

Under either version, an abort whose `EndTxn` failed also bumps the producer's epoch, so whatever the coordinator still holds of that transaction is aborted before the next one. When the cluster moves from version 1 to 2, the next transaction first bumps the producer's epoch; if a bump fails, `producer.transaction()` rejects and the next call tries again. A transaction in which nothing was sent and no offsets were added ends without contacting the broker, under either version.
