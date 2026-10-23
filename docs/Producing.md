---
id: producing
title: Producing Messages
---

To publish messages to Kafka you have to create a producer. Simply call the `producer` function of the client to create it:

```javascript
const producer = kafka.producer()
```

or with options

```javascript
const producer = kafka.producer({
    allowAutoTopicCreation: false,
    transactionTimeout: 30000
})
```

## Options

| option                 | description                                                                                                                                                                                  | default              |
| ---------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------- |
| createPartitioner      | Take a look at [Custom Partitioner](#custom-partitioner) for more information                                                                                                                | `null`               |
| retry                  | Take a look at [Producer Retry](#retry) for more information                                                                                                                                 | `null`               |
| metadataMaxAge         | The period of time in milliseconds after which we force a refresh of metadata even if we haven't seen any partition leadership changes to proactively discover any new brokers or partitions | `300000` - 5 minutes |
| allowAutoTopicCreation | Allow topic creation when querying metadata for non-existent topics                                                                                                                          | `true`               |
| transactionTimeout | The maximum amount of time in ms that the transaction coordinator will wait for a transaction status update from the producer before proactively aborting the ongoing transaction. If this value is larger than the `transaction.max.timeout.ms` setting in the __broker__, the request will fail with a `InvalidTransactionTimeout` error | `60000`                            |
| idempotent         | Write every message exactly once per partition, in order, even when the client retries a request (KIP-679). On unless `false` is passed: the producer allocates a producer id at `connect()` and needs `acks` -1 on every send, at most 5 requests in flight per connection and `retry.retries` of at least 1. A setting that conflicts turns idempotence off with one warning, or throws when `idempotent: true` or a `transactionalId` asked for it — see [Idempotence](#idempotence). If `retry.retries` is set below `5` a warning is logged that EoS guarantees may be invalidated. | `true`                            |
| maxInFlightRequests | Max number of requests in progress per broker connection; `null` means no limit. Above 5, or `null`, is more than an idempotent producer allows — see [Idempotence](#idempotence). | `5` |
| linger | ms a batch of one partition waits in the [accumulator](#accumulator) for records from further sends before it goes out; `0` sends it at once. An integer from 0 to 2147483647 | `5` |
| batchSize | Bytes of message keys and values at which a batch of the [accumulator](#accumulator) goes out without waiting for `linger`. An integer of at least 1 | `16384` |
| compressionLevels | Optional `{ gzip, zstd }` levels. Gzip: -1 or 1–9; zstd: -131072–22. LZ4 and Snappy levels are rejected. | codec defaults |
| deliveryTimeout | Bound, in ms, on the waiting and the retries of one `send()`/`sendBatch()`. Past it the send rejects with `KafkaJSDeliveryTimeout`, its `cause` the last error; running out of `retry.retries` first still rejects with `KafkaJSNumberOfRetriesExceeded`. A queued or on-wire send settles at its delivery deadline, even while the operation it awaits has not returned. A late answer does not settle it again. An explicit value is an integer of at least `linger + requestTimeout` - 30005 with both defaults - and at most 2147483647. On every producer the bound runs from the moment the send is made, so it includes waiting for `linger` and, on an idempotent producer, for the producer id, as in the Java client. | `120000`, or your `retry.maxTotalTime`, or `linger + requestTimeout` when that is larger |
| bufferMemory | Bytes the producer's sends may hold at once, its own and its transactions' together, charged as [Memory](#memory) describes. A charge, not a limit on the process's memory. An integer from 1 to `Number.MAX_SAFE_INTEGER` | `33554432` (32 MiB) |
| maxBlockMs | How long, in ms, a send waits for room in `bufferMemory` before it rejects with `KafkaJSBufferExhaustedError`, and never past its delivery deadline. An integer from 0 to 2147483647; 0 rejects a send that does not fit at once. See [Memory](#memory) | `60000` |
| maxRequestSize | The largest Produce request, in bytes, counting its whole frame after compression. A record that no request can carry rejects with `KafkaJSRecordTooLargeError`; a send larger than one request is split at record boundaries. An integer from the smallest request this producer's `clientId` and `transactionalId` allow (named in the error) to 2147483647. See [Request size](#request-size) | `1048576` (1 MiB) |

## <a name="idempotence"></a> Idempotence

A producer is idempotent unless it is created with `idempotent: false`: it allocates a producer id when it connects and numbers every batch it sends, so the client's own retry of a request can never write a record twice or out of order. A send that rejects with `KafkaJSNumberOfRetriesExceeded` has an unknown outcome, and a new `send()` of the same records may write them again: deduplicate in the application, or use a [transaction](Transactions.md) where that matters. Three settings must leave room for that. When one does not, a producer that did not ask for idempotence turns it off for the rest of its life and logs one warning naming the setting and both remedies; a producer created with `idempotent: true`, or with a `transactionalId`, refuses instead.

| Setting | Idempotence needs | `idempotent` not passed | `idempotent: true` or a `transactionalId` |
| --- | --- | --- | --- |
| `acks` on a send | `-1`, the default | the send goes out non-idempotent, with one warning | the send rejects with `KafkaJSInvalidConfigurationError` |
| `maxInFlightRequests` | 5 or less; the default is 5 | one warning at `kafka.producer()` | `kafka.producer()` throws `KafkaJSInvalidConfigurationError` |
| `retry.retries` | 1 or more; delivery defaults to 2147483647 retries, bounded by the delivery deadline | one warning at `kafka.producer()` | `kafka.producer()` throws `KafkaJSInvalidConfigurationError` |

`producer.isIdempotent()` reports `false` once idempotence is off. The retry budget is the same with or without idempotence. The principal an idempotent producer connects as needs the `IDEMPOTENT_WRITE` ACL on the cluster, or `WRITE` on any topic (Kafka 2.8 and later); without either, `connect()` fails with an error naming both remedies, and a later `send()` tries again.

Idempotence costs a little when each send is awaited before the next is made, at `linger: 0`: such a send takes about 2 % longer, about 30 µs on a 1.5 ms round trip, than the same producer with `idempotent: false` (measured on three brokers). `idempotent: false` removes that cost and gives up the protection against duplicates and reordering when a request is retried.

### <a name="delivery-timeout"></a> Delivery timeout

`deliveryTimeout` bounds the waiting and the retries of one `send()` or `sendBatch()` (KIP-91). A queued or on-wire send settles at its delivery deadline, even while the operation it awaits has not returned. A late answer does not settle it again. It defaults to 120000 ms (the Java client's `delivery.timeout.ms`). Without a `deliveryTimeout`, your `retry.maxTotalTime` is the deadline and is not held to the next rule; with neither, the default is raised to `linger + requestTimeout` when that is larger, with one warning. An explicit `deliveryTimeout` must be at least `linger + requestTimeout` (30005 ms with both defaults), so that one request can complete after the accumulator's wait, and at most 2147483647, the longest delay a Node.js timer takes; a raised default may be larger.

`KafkaJSDeliveryTimeout.outcome` is `'unsent'` only when no attempt carrying the records could have been written; otherwise it is `'unknown'`. An earlier written attempt keeps the result unknown even if a later answer was retriable. Its absence says nothing about delivery: retry exhaustion, connection errors and broker answers do not carry this annotation. A timeout copies the last cause's `type` and `code` when present. Resending an unknown delivery may duplicate records.

With `enforceRequestTimeout: false`, an outstanding request can retain its records until its answer or connection failure even though the send has already rejected. The deadline fences later work; it does not cancel the socket request.

```javascript
const producer = kafka.producer({ deliveryTimeout: 120000 })
```

### <a name="accumulator"></a> Accumulator

Every producer gathers the records of several sends to one topic-partition into one batch, in the order their records reach the accumulator, normally the order the sends were made, and sends them together - so a busy partition sends fewer, larger requests. It works the same with or without idempotence, and inside transactions. `linger` (default 5 ms) and `batchSize` (default 16384 bytes) are the Java client's settings and defaults (`linger.ms`, `batch.size`).

```javascript
const producer = kafka.producer({ linger: 10, batchSize: 65536 })
```

- A batch goes out `linger` ms after its first records arrived, or as soon as its keys and values reach `batchSize` bytes. Records that would take it past `batchSize` start the next batch, and the records of one send for one partition are cut only where one request could not carry them (see [Request size](#request-size)): a send larger than `batchSize` goes out on its own. The batches one send fills, and batches of different partitions that are due at the same moment, go out together, so they can share a request: one `send()` or `sendBatch()` usually makes one request per broker. When a batch's `linger` timer fires, every other batch that is due by then leaves with it, however late its timer fired. So does every waiting batch for the same broker with the same `acks`, `timeout` and `compression`, even before its own `linger` has run out, so sends made a millisecond apart to partitions of one broker reach it in one request. Only a firing `linger` timer pulls batches that are not yet due: a batch that fills `batchSize` leaves with the batches already due, `linger: 0` sends in the same tick, and a flush hands every batch on. The Java client readies a broker on more triggers (a full batch, a flush, a transaction that is completing). Both clients stop a request at the request-size limit, 1 MiB by default (`maxRequestSize` here, `max.request.size` in Java), so a broker's waiting batches can need several requests. `linger` is when a batch becomes due, not a bound on when `send()` settles: retries, the wait for the producer id or for metadata, and, on an idempotent producer, the batch in flight before it all come on top. A batch held behind its partition's batch in flight stays held, and a batch whose partition has no known leader leaves on its own timer. Its records that join a batch an earlier send began leave with that batch, so one call can reach a broker in two requests.
- **Latency.** A lone send waits up to `linger` before its request leaves. Code that awaits each send before making the next one gains nothing from the wait and can pass `linger: 0`.
- **`linger: 0`** sends a batch as soon as it forms, with no timer: the sends made in the same tick still go out together, and on an idempotent producer the sends that queue behind a partition's batch in flight still merge into one batch. No setting sends every `send()` as its own request.
- **Request size.** A request carries at most one batch for each partition its broker leads, and at most `maxRequestSize` bytes: see [Request size](#request-size).
- Every batch settles on its own partition's answer, with or without idempotence, as in the Java client: written, it resolves at once and is never sent again; refused with an error that is not retriable, it rejects alone; refused with a retriable one, it alone goes out again, within the retry budget. Without idempotence the batches that went out together share one `retry.retries` budget: a topic or broker that cannot be looked up holds back only its own batches, and each attempt waits for every broker's answer, so the slowest broker paces the retries - by at most `requestTimeout` per attempt. Records are assigned to partitions once, within the send's delivery deadline, before they are sent.
- An idempotent producer keeps one batch of a partition in flight at a time. While it waits for that batch's answer, the partition's next batch stays open past its `linger` and keeps gathering records, up to `batchSize`, and goes out when the answer arrives, so a busy partition sends batches as large as its round trip allows rather than many small ones queued behind each other.
- A send whose `acks`, `timeout` or `compression` differs from the batch waiting for its partition sends that batch first and starts a new one. A send with `acks` 0 or 1 that turns idempotence off sends every waiting batch first, while they still carry the producer id.
- A send resolves once every batch carrying its records has been acknowledged, with one `RecordMetadata` per partition, sorted by topic and then partition, whose `baseOffset` is the offset of its own first record there, and rejects with the error of the batch that failed - the sends that share a batch succeed or fail together. `deliveryTimeout` counts from the moment the batch's first send was made, so the bound includes waiting for `linger` and, on an idempotent producer, for the producer id, as in the Java client.
- A record whose key, value, header values or timestamp cannot be encoded fails its own send at once with `KafkaJSInvalidConfigurationError`, before it joins a batch: a key, value or header value must be a string, a `Buffer` or `null` (header values: not `null`), and a timestamp must be an integer from `0` to 2^63 - 1, the largest int64 - a number, a numeric string, a `Date`, a `BigInt` or a `Long`. The exact value is checked as it arrives, so a negative one, as Java also refuses, and one above the range, which used to wrap into another timestamp, fail the send. A timestamp of numeric `0` or `0n`, or none, means the send time, as in 2.2.4; the string `'0'`, `new Date(0)` and a `Long` of 0 are the epoch itself.
- The records of a send are read once, when `send()` or `sendBatch()` is called: the producer keeps its own copy of each record, of its headers and header-value arrays, and of a `Date` or `Long` timestamp (as its exact decimal string), and validates, charges, partitions and encodes that copy. Changing a record, its headers or its timestamp afterwards changes neither what is sent nor what is charged, and an accessor is read once. A custom partitioner sees the copy, with the record's own fields. The bytes of a `Buffer` are not copied, as in 2.2.4: changing them still changes what is sent. The exception is a small view of a large backing store, which is copied - on Node.js 24.18 and 26.3 and later that includes a `Buffer` under 4 KiB from Node's shared pool (see [Memory](#memory)).
- A transaction's `commit()` and `abort()` send its waiting batches at once - `abort()` then rejects them with the rest of the transaction's queued batches - and `producer.disconnect()` sends every waiting batch, and every send begun before it, and waits for them - retries included, up to `deliveryTimeout` or the retry budget - before it disconnects, refusing new sends meanwhile, so nothing accumulated is lost.

### <a name="memory"></a> Memory

`bufferMemory` (default 33554432 bytes, 32 MiB) bounds what the producer holds for the sends it has accepted and not finished, its own and its transactions' together, as the Java client's `buffer.memory` does; `maxBlockMs` (default 60000 ms, the Java client's `max.block.ms`) bounds how long a send waits for room in it.

```javascript
const producer = kafka.producer({ bufferMemory: 67108864, maxBlockMs: 10000 })
```

- **The charge.** Each record is charged its wire size - the Java client's upper bound: 21 bytes plus its key, value and headers with their lengths, a null or absent key or value counting 1 byte - plus 64 bytes, and 16 for each header (an array value is one header per element). Each send is charged a further 1536 bytes, and the part of every backing store its `Buffer`s keep alive beyond the bytes they show: a store several of its `Buffer`s share is counted once, and a store other sends also use is counted by each of them. Each batch is charged 1024 bytes when it forms. The charge is taken when `send()` or `sendBatch()` is called, before anything is awaited.
- **Admission.** A send that fits while no send is waiting is admitted at once. Otherwise sends wait in the order they were made: a small send waits behind a large one. A send waits at most `maxBlockMs`, and never past its delivery deadline: it then rejects with the retriable `KafkaJSBufferExhaustedError` (`reason: 'timeout'`), or with `KafkaJSDeliveryTimeout` (`outcome: 'unsent'`) when the deadline comes first. With `maxBlockMs: 0` a send that does not fit rejects at once. A send that would take the sends already waiting past `bufferMemory` rejects at once with `KafkaJSBufferExhaustedError` (`reason: 'queue-full'`). A record, or a whole send, that can never fit rejects at once with `KafkaJSRecordTooLargeError` (`reason: 'bufferMemory'`, `scope: 'record'` or `'send'`), which is not retriable and carries the `type` and `code` of the broker's `MESSAGE_TOO_LARGE`. Nothing of a refused send is queued; inside a transaction a refusal leaves the transaction able only to abort (see [Transactions](Transactions.md)), while the send rejects with its own error. A send with no record is charged nothing.
- **What is bounded**, with C the value of `bufferMemory`, in three parts: the sends admitted are charged at most C; the sends waiting for room, which keep their records, at most another C; and the batches' charges, 1024 bytes for each batch alive, come on top, taken without waiting, because an admitted send already holds its records. The total charge is therefore at most `2C + 1024 × floor(C / 88)`, about 13.6 C, when every batch holds a single empty record, and about 3 C for records of 1 KiB or more; the records held at once are at most `floor(2C / 88)`. There is no separate option for a count of records.
- **When room comes back.** A batch's share comes back when its request is answered or fails for good: not on a retry, and, for a batch that expired while its request was on its way, only when that request is answered or its connection fails (with `enforceRequestTimeout: false`, as long as the request lasts). A send's own 1536 bytes and its `Buffer`s' bytes come back after the last batch carrying its records and the last wait on it. `acks: 0` gives a batch's share back when its request is written. On a producer that is not idempotent, a batch that expired while no request carried it drops its records and gives their share back at once, and its own 1024 bytes come back when the send's routing and retries for it have ended (a broker lookup or a metadata refresh still under way).
- **A charge, not a limit on memory.** The bound counts what the producer holds for its sends, not the process's memory (RSS): encoded requests, socket buffers, decompressed fetches, your own copies and the heap's slack are outside it. A `Buffer` keeps its whole backing store alive, so the store is charged. A view of a store larger than 16 KiB and than 16 times the view is copied when `send()` is called, and only the copy is charged. That includes a small `Buffer` that `Buffer.from()` or `Buffer.allocUnsafe()` cut from Node's shared pool where the pool is 64 KiB (Node.js 24.18 and 26.3 and later): such a `Buffer` under 4 KiB is copied, so changing its bytes after the call does not change what is sent. With an 8 KiB pool (Node.js 22) it is not copied, and every send it is in is charged the pool's 8 KiB.

### <a name="request-size"></a> Request size

`maxRequestSize` (default 1048576 bytes, 1 MiB, the Java client's `max.request.size`) bounds every Produce request the producer writes, counted on the encoded frame: its length prefix, its header and fields, each batch's header and each batch's records after compression.

```javascript
const producer = kafka.producer({ maxRequestSize: 4194304 })
```

- **A record that can never fit** - its size, as the Java client sizes it (21 bytes plus its key, value and headers with their lengths), is above what one request carries once the request's framing and the worst case of the send's codec are counted - rejects its `send()` at once with `KafkaJSRecordTooLargeError` (`reason: 'maxRequestSize'`, `scope: 'record'`), before anything of the send is charged, queued or written. The error is not retriable and carries the `type` (`MESSAGE_TOO_LARGE`) and `code` (10) of the broker's own error.
- **A send larger than one request is split** at record boundaries, per partition: no record is ever cut. Its admission (see [Memory](#memory)) is whole, its delivery is per chunk, in order: a chunk waiting for a retry stays ahead of the later ones, and one that failed for good lets the next one go. The send settles once every chunk has; it resolves with one `RecordMetadata` per partition, whose `baseOffset` is that of the send's first record there, whatever order the answers came in, or rejects with the first error a chunk failed with, unchanged. **A rejected split send may have written some of its records**: resending it whole can duplicate those. With `acks: 0` it resolves `[]`, as any send does. A send that fits one request is unchanged.
- **The plan and the limit.** Requests are planned on the records' sizes before compression, plus the worst case of each built-in codec (about 0.03 % for gzip, 0.4 % for zstd, 0.01 % for LZ4 and 3 % for Snappy), so a compressible send is split sooner than its compressed size would need. Every frame is compared with `maxRequestSize` just before it is written; one above it - only possible with a codec you registered yourself, whose worst case the client does not know - is not written, and its batches, none of which was ever sent, go out in smaller requests. A batch already sent once is never divided again: if its retry would exceed the limit it fails with `KafkaJSRecordTooLargeError` without another write, and that error does **not** show that the earlier attempt was unsent.
- **The broker's limits.** Raise `maxRequestSize` only as far as the brokers' `message.max.bytes` and `socket.request.max.bytes` allow. A record under `maxRequestSize` but over a topic's `max.message.bytes` is refused by the broker with `MESSAGE_TOO_LARGE` for its partition: the send rejects with that `KafkaJSProtocolError`, a broker answer, which has the same `type` and `code` as the client's `KafkaJSRecordTooLargeError`. With `acks: 0` such a record is dropped without an error.

## Producing messages

The method `send` is used to publish messages to the Kafka cluster.

```javascript
const producer = kafka.producer()

await producer.connect()
await producer.send({
    topic: 'topic-name',
    messages: [
        { key: 'key1', value: 'hello world' },
        { key: 'key2', value: 'hey hey!' }
    ],
})
```

Example with a defined partition:

```javascript
const producer = kafka.producer()

await producer.connect()
await producer.send({
    topic: 'topic-name',
    messages: [
        { key: 'key1', value: 'hello world', partition: 0 },
        { key: 'key2', value: 'hey hey!', partition: 1 }
    ],
})
```

The method `send` has the following signature:

```javascript
await producer.send({
    topic: <String>,
    messages: <Message[]>,
    acks: <Number>,
    timeout: <Number>,
    compression: <CompressionTypes>,
})
```

| property           | description                                                                                                                                                                                                                                                                                                                                | default                            |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------- |
| topic              | topic name                                                                                                                                                                                                                                                                                                                                 |                              |
| messages           | An array of objects. See [Message structure](#message-structure) for more details. Example: <br> `[{ key: 'my-key', value: 'my-value'}]`                                                                                                                                                                                                                                          |                              |
| acks               | Control the number of required acks. <br> __-1__ = all insync replicas must acknowledge _(default)_ <br> __0__ = no acknowledgments <br> __1__ = only waits for the leader to acknowledge                                                                                                                                                         | `-1` all insync replicas must acknowledge |
| timeout            | The time to await a response in ms                                                                                                                                                                                                                                                                                                         | `30000`                            |
| compression        | Compression codec                                                                                                                                                                                                                                                                                                                          | `CompressionTypes.None`            |


### Message structure

Messages have the following properties:

| Property  | Description                                                                                                                                                                                                             | Default      |
| ----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------|
| key       | Used for partitioning. See [Key](#message-key)                                                                                                                                                                          |              |
| value     | Your message content. The value can be a Buffer, a string or null. The value will always be encoded as bytes when sent to Kafka. When consumed, the consumer will need to interpret the value according to your schema. |              |
| partition | Which partition to send the message to. See [Key](#message-key) for details on how the partition is decided if this property is omitted.                                                                                |              |
| timestamp | The timestamp of when the message was created. See [Timestamp](#message-timestamp) for details.                                                                                                                         | `Date.now()` |
| headers   | Metadata to associate with your message. See [Headers](#message-headers).                                                                                                                                               |              |

#### <a name="message-key"></a>Key

The message `key` is used to decide which partition the message will be sent to. This is important to ensure that messages relating to the same aggregate are processed in order. For example, if you use an `orderId` as the key, you can ensure that all messages regarding that order will be processed in order.

By default, the producer is configured to distribute the messages with the following logic:

- If a partition is specified in the message, use it
- If no partition is specified but a key is present choose a partition based on a hash (murmur2) of the key
- If no partition or key is present choose a partition in a round-robin fashion

#### <a name="message-timestamp"></a>Timestamp

Each message has a timestamp in the form of a UTC timestamp with millisecond precision as a string. If no timestamp was provided, the producer will use the current time as the timestamp. When the message is consumed, the broker may override this timestamp depending on the topic configuration:

* If the topic is configured to use CreateTime, the timestamp from the producer's message will be used.
* If the topic is configured to use LogAppendTime, the timestamp will be overwritten by the broker with the broker local time when it appends the message to its log.

#### <a name="message-headers"></a>Headers

Kafka v0.11 introduces record headers, which allows your messages to carry extra metadata. To send headers with your message, include the key `headers` with the values. Example:

```javascript
await producer.send({
    topic: 'topic-name',
    messages: [{
        key: 'key1',
        value: 'hello world',
        headers: {
            'correlation-id': '2bfb68bb-893a-423b-a7fa-7b568cad5b67',
            'system-id': 'my-system',
        }
    }]
})
```

A header value can be either a string or an array of strings.

## Producing to multiple topics

To produce to multiple topics at the same time, use `sendBatch`. This can be useful, for example, when migrating between two topics.

```javascript
const topicMessages = [
  {
    topic: 'topic-a',
    messages: [{ key: 'key', value: 'hello topic-a' }],
  },
  {
    topic: 'topic-b',
    messages: [{ key: 'key', value: 'hello topic-b' }],
  },
  {
    topic: 'topic-c',
    messages: [
      {
        key: 'key',
        value: 'hello topic-c',
        headers: {
          'correlation-id': '2bfb68bb-893a-423b-a7fa-7b568cad5b67',
        },
      }
    ],
  }
]
await producer.sendBatch({ topicMessages })
```

`sendBatch` has the same signature as `send`, except `topic` and `messages` are replaced with `topicMessages`:

```javascript
await producer.sendBatch({
    topicMessages: <TopicMessages[]>,
    acks: <Number>,
    timeout: <Number>,
    compression: <CompressionTypes>,
})
```

| property      | description                                                                                                |
| ------------- | ---------------------------------------------------------------------------------------------------------- |
| topicMessages | An array of objects with `topic` and `messages`.<br>`messages` is an array of the same type as for `send`. |

## <a name="custom-partitioner"></a> Custom partitioner

It's possible to assign a custom partitioner to the producer. A partitioner is a function which returns another function responsible for the partition selection, something like this:

```javascript
const MyPartitioner = () => {
    return ({ topic, partitionMetadata, message }) => {
        // select a partition based on some logic
        // return the partition number
        return 0
    }
}
```

`partitionMetadata` is an array of partitions with the following structure:

`{ partitionId: <NodeId>, leader: <NodeId> }`

Example:

```javascript
[
    { partitionId: 1, leader: 1 },
    { partitionId: 2, leader: 2 },
    { partitionId: 0, leader: 0 }
]
```

To use your custom partitioner, use the option `createPartitioner` when creating the producer.

```javascript
kafka.producer({ createPartitioner: MyPartitioner })
```

### Default Partitioners

KafkaJS ships with 3 partitioners: `DefaultPartitioner`, `UniformStickyPartitioner` and `LegacyPartitioner`.

| Partitioner | Keyed records | Records without a key | Choose it when |
|---|---|---|---|
| `DefaultPartitioner` (the default) | `murmur2(key)`, as the Java client | round-robin over the partitions that have a leader | you need Java-compatible keyed placement and an even spread of every keyless record |
| `UniformStickyPartitioner()` | the same as `DefaultPartitioner` | one partition per topic until `stickyBytes` have gone to it, then a new one picked at random (it may be the same partition again) | most records have no key and you want fewer, larger batches per partition (see below) |
| `LegacyPartitioner` | the hash KafkaJS used before v2.0.0 | round-robin | you need the partitioning of KafkaJS before v2.0.0 |

The `DefaultPartitioner` should be compatible with the default partitioner that ships with the Java Kafka client. This can be important to meet the [co-partitioning requirement](https://docs.confluent.io/current/ksql/docs/developer-guide/partition-data.html#co-partitioning-requirements) when joining multiple topics.

> 🚨 **Important**  🚨
> 
> **The `LegacyPartitioner` was the default until v2.0.0. If you are upgrading from a version
older and want to retain the previous partitioning behavior, use the `LegacyPartitioner`
by importing it and providing it to the Producer constructor:**
> 
> ```javascript
> const { Partitioners } = require('@countly/kafkajs')
> kafka.producer({ createPartitioner: Partitioners.LegacyPartitioner })
> ```

#### Uniform sticky partitioner

`UniformStickyPartitioner` is the strictly uniform sticky partitioner of KIP-794. It is opt-in, and
unlike the other two you call it: pass `Partitioners.UniformStickyPartitioner()` for the defaults, or
call it with options:

```javascript
const { Partitioners } = require('@countly/kafkajs')
kafka.producer({ createPartitioner: Partitioners.UniformStickyPartitioner({ stickyBytes: 16384 }) })
```

| Option | Description | Default |
|---|---|---|
| stickyBytes | Bytes of key and value assigned to one partition before the next record without a key picks another; a string counts in its UTF-8 bytes, as a `Buffer` of the same text does | `16384` |
| ignoreKeys | Partition keyed records by the sticky rule too, instead of by key | `false` |

A record without a key goes to its topic's sticky partition until `stickyBytes` of key and value
have been assigned to it; the record that reaches the threshold still goes there, and the next one
goes to a partition picked uniformly at random among those with a leader (among all of them when
none has a leader — the pick can land on the same partition again). The stickiness carries across
`send()` calls: the keyless records of one `send()` with many messages fill a few batches instead
of one record per partition, and records from separate `send()` calls share a batch in the
[accumulator](#accumulator). A
record with an explicit `partition` goes there. Keyed records keep
`DefaultPartitioner`'s mapping, so topics co-partitioned with Java producers stay co-partitioned —
unless you pass `ignoreKeys: true`.

The Java client's adaptive partitioning (`partitioner.adaptive.partitioning.enable`,
`partitioner.availability.timeout.ms`) is not implemented: it needs per-partition queue depths this
client does not track.

## <a name="retry"></a> Retry

The option `retry` can be used to customize the configuration for the producer.

Take a look at [Retry](Configuration.md#retry) for more information.

The retries of a send run until its [delivery deadline](#delivery-timeout) unless you set `retry.retries`: the producer retries a retriable answer, backing off from 100 ms up to 1000 ms with the retrier's jitter, for as long as the deadline lasts, and then rejects with `KafkaJSDeliveryTimeout` whose `cause` is the last error. An explicit `retry.retries`, `retry.maxTotalTime`, `retry.initialRetryTime` or `retry.maxRetryTime` is honoured, each key on its own, and `retry.retries` ends a send with `KafkaJSNumberOfRetriesExceeded` as before. The requests of a transaction (`initTransactions`, `commit`, `abort`, `sendOffsets`) and the producer id and partition registrations keep the retrier's own defaults.

## <a name="compression"></a> Compression

GZIP, Snappy, LZ4 and ZSTD are built in — no configuration is needed to consume their compressed messages. LZ4 needs the optional `lz4-napi` dependency, and ZSTD needs support in `node:zlib`. A codec you register under `CompressionCodecs` always takes priority over a built-in one.

Set optional producer levels with `kafka.producer({ compressionLevels: { gzip: 9, zstd: 7 } })`. Gzip accepts -1 (the zlib default) or 1–9; zstd accepts -131072–22. Values must be integers. Omitting a key retains the codec default. An `lz4` level is rejected because `lz4-napi` exposes no compression level; Snappy has no level. Direct broker callers may set `broker.produce({ ...request, compressionLevel: 9 })` for the selected codec.

### <a name="compression-gzip"></a> GZIP

```javascript
const { CompressionTypes } = require('@countly/kafkajs')

async () => {
  await producer.send({
    topic: 'topic-name',
    compression: CompressionTypes.GZIP,
    messages: [
        { key: 'key1', value: 'hello world' },
        { key: 'key2', value: 'hey hey!' }
    ],
  })
}
```

The consumers know how to decompress GZIP, so no further work is necessary.

### <a name="compression-snappy"></a> Snappy

Snappy is built in for both producing and consuming. It writes Kafka's xerial framing with independent 32 KiB blocks and reads both xerial streams and raw blocks. No native Snappy dependency is required.

Remove the `kafkajs-snappy` registration to use the built-in codec, or keep it: yours wins. The built-in codec runs on the main thread, yielding to the event loop after 256 KiB of work or 4,096 blocks and tokens. Reference throughput is about 100 MiB/s to decode and 40 MiB/s to encode; measure your payload and host.

### <a name="compression-lz4"></a> LZ4

LZ4 (the Kafka frame format, KIP-57) is built in, backed by the optional native dependency `lz4-napi`:

```sh
npm install lz4-napi
```

```javascript
const { CompressionTypes } = require('@countly/kafkajs')

async () => {
  await producer.send({
    topic: 'topic-name',
    compression: CompressionTypes.LZ4,
    messages: [{ key: 'key1', value: 'hello world' }],
  })
}
```

Without `lz4-napi` installed, requesting `CompressionTypes.LZ4` throws `KafkaJSNotImplemented` naming the missing package. To use a different LZ4 implementation instead, register your own codec under `CompressionCodecs[CompressionTypes.LZ4]` (see [Other](#compression-other)) — it always wins over the built-in one.

### <a name="compression-zstd"></a> ZSTD

Zstandard (KIP-110) is built in via `node:zlib`'s `zstdCompress`/`zstdDecompress`, available on Node ≥ 22.15 / ≥ 23.8:

```javascript
const { CompressionTypes } = require('@countly/kafkajs')

async () => {
  await producer.send({
    topic: 'topic-name',
    compression: CompressionTypes.ZSTD,
    messages: [{ key: 'key1', value: 'hello world' }],
  })
}
```

No dependency and no setup needed on a Node version that has zstd in `zlib`. On an older Node, requesting `CompressionTypes.ZSTD` throws `KafkaJSNotImplemented` naming the required Node version. ZSTD on the wire needs a broker that accepts Produce ≥ v7 / Fetch ≥ v10; an older negotiated version surfaces `UNSUPPORTED_COMPRESSION_TYPE` instead.

### <a name="compression-other"></a> Other

Any other codec can be easily implemented using existing libraries.

A codec is an object with two `async` functions: `compress` and `decompress`. Import the libraries and define the codec object:

```javascript
const MyCustomSnappyCodec = {
    async compress(encoder) {
        return someCompressFunction(encoder.buffer)
    },

    async decompress(buffer) {
        return someDecompressFunction(buffer)
    }
}
```

`decompress` also receives the decompression bound of the consumer that reads the batch, as `decompress(buffer, { maxOutputLength })`: the most bytes one record batch may decompress to (the consumer's `maxDecompressedBatchBytes`, 1 GiB by default). Return at most that many bytes, or throw. A longer result is rejected afterwards, and a thrown error, a `RangeError` included, ends the batch as a corrupt one (`KafkaJSCorruptRecordError`, reason `decompressed-size` or `decompression`) instead of reaching the retrier as an unrecoverable error. The bound is a request, not a guarantee: a codec that ignores the argument, as codecs written before it existed do, has already allocated its whole output when the result is checked, so it keeps working but is not protected.

Now that we have the codec object, we can wrap it in a function and add it to the implementation:

```javascript
const { CompressionTypes, CompressionCodecs } = require('@countly/kafkajs')
CompressionCodecs[CompressionTypes.Snappy] = () => MyCustomSnappyCodec
```

The new codec can now be used with the `send` method, example:

```javascript
await producer.send({
    topic: 'topic-name',
    compression: CompressionTypes.Snappy,
    messages: [
        { key: 'key1', value: 'hello world' },
        { key: 'key2', value: 'hey hey!' }
    ],
})
```
