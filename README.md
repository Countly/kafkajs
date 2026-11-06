# @countly/kafkajs

A pure-JavaScript [Apache Kafka](https://kafka.apache.org/) client for Node.js, maintained by [Countly](https://github.com/Countly) as a standalone continuation of [KafkaJS](https://github.com/tulios/kafkajs) 2.2.4.

- **Drop-in for KafkaJS users**: preserved API plus documented semantic differences. `eachMessage`/`eachBatch`, pause/resume, instrumentation events and error classes remain available; see the [migration guide](./docs/MigrationGuide-3-0-0.md).
- **Targets current infrastructure**: Apache Kafka **4.0+** (KRaft) and Node.js **22+**. Older brokers and runtimes are not supported.
- **Zero required runtime dependencies** (one optional native dependency, `lz4-napi`, enables LZ4 compression), CommonJS entry point plus an ES module entry.

> **Status**: under active development, **not yet released**. The first public version will be `3.0.0`. Until then the package is not published to npm and internal pre-release tags (`3.0.0-alpha.N`) are used.

## Features

- Producer, with idempotent and transactional modes
- Consumer groups with pause, resume and seek
- Message headers
- GZIP, Snappy, LZ4 and ZSTD compression built in (LZ4 needs the optional `lz4-napi` dependency); registered codecs take precedence
- PLAINTEXT, SSL and SASL_SSL connections
- SASL PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER, AWS IAM and GSSAPI (optional `kerberos` peer)
- OAuth bearer token helpers and recipes ([OAuth.md](docs/OAuth.md))
- Admin client

## Getting started

```sh
npm install @countly/kafkajs
```

```javascript
const { Kafka } = require('@countly/kafkajs')
// or: import { Kafka } from '@countly/kafkajs'

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['kafka1:9092', 'kafka2:9092'],
})

const producer = kafka.producer()
const consumer = kafka.consumer({ groupId: 'test-group' })

const run = async () => {
  await producer.connect()
  await producer.send({
    topic: 'test-topic',
    messages: [{ value: 'Hello Kafka user!' }],
  })

  await consumer.connect()
  await consumer.subscribe({ topic: 'test-topic', fromBeginning: true })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({ partition, offset: message.offset, value: message.value.toString() })
    },
  })
}

run().catch(console.error)
```

Documentation lives in the [`docs/`](./docs) folder of this repository:
[Getting Started](./docs/GettingStarted.md) · [Configuration](./docs/Configuration.md) · [Producing](./docs/Producing.md) · [Consuming](./docs/Consuming.md) · [Admin](./docs/Admin.md) · [Instrumentation events](./docs/InstrumentationEvents.md) · [Transactions](./docs/Transactions.md).

Compression is selected per `send()` or `sendBatch()` call; there is no producer-level default. An omitted codec sends uncompressed, and setting a codec on one send does not change later sends. See [compression](./docs/Producing.md#compression).

## Migrating from `kafkajs`

Replace the package name for the preserved API plus documented semantic differences. Read the [migration guide](./docs/MigrationGuide-3-0-0.md) for the changed defaults and behavior, and the [CHANGELOG](./CHANGELOG.md) for release details.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md) and [docs/DevelopmentEnvironment.md](./docs/DevelopmentEnvironment.md). Bugs and feature requests go to the [issue tracker](https://github.com/Countly/kafkajs/issues). Security reports: see [SECURITY.md](./SECURITY.md).

## License

MIT. See [LICENSE](./LICENSE).

This project is a fork of KafkaJS, copyright (c) 2018 Túlio Ornelas and the KafkaJS contributors, used under the MIT license. Countly's changes are copyright (c) 2026 Countly and released under the same license. KafkaJS's name, logo and website belong to the original project; this fork does not use the KafkaJS logo and is not affiliated with or endorsed by the KafkaJS maintainers.

Apache Kafka and Kafka are trademarks of The Apache Software Foundation. This project is not affiliated with or endorsed by the Apache Software Foundation.
