# @countly/kafkajs

A pure-JavaScript [Apache Kafka](https://kafka.apache.org/) client for Node.js, maintained by [Countly](https://github.com/Countly) as a standalone continuation of [KafkaJS](https://github.com/tulios/kafkajs) 2.2.4.

- **Drop-in for KafkaJS users**: same public API, same runtime semantics (`eachMessage`/`eachBatch`, pause/resume, instrumentation events, error classes).
- **Targets current infrastructure**: Apache Kafka **4.0+** (KRaft) and Node.js **22+**. Older brokers and runtimes are not supported.
- **Zero runtime dependencies**, CommonJS entry point plus an ES module entry.

> **Status**: under active development, **not yet released**. The first public version will be `3.0.0`. Until then the package is not published to npm and internal pre-release tags (`3.0.0-alpha.N`) are used.

## Features

- Producer, with idempotent and transactional modes
- Consumer groups with pause, resume and seek
- Message headers
- GZIP compression built in; Snappy, LZ4 and ZSTD through pluggable codecs
- PLAINTEXT, SSL and SASL_SSL connections
- SASL PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER and AWS IAM
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

## Migrating from `kafkajs`

Replace the package name; the API is unchanged. Behavior differences introduced by this fork (Kafka 4.0+ only, defaults that follow the Java client) are listed in the [CHANGELOG](./CHANGELOG.md) and, from 3.0.0, in a migration guide.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md) and [docs/DevelopmentEnvironment.md](./docs/DevelopmentEnvironment.md). Bugs and feature requests go to the [issue tracker](https://github.com/Countly/kafkajs/issues). Security reports: see [SECURITY.md](./SECURITY.md).

## License

MIT. See [LICENSE](./LICENSE).

This project is a fork of KafkaJS, copyright (c) 2018 Túlio Ornelas and the KafkaJS contributors, used under the MIT license. Countly's changes are copyright (c) 2026 Countly and released under the same license. KafkaJS's name, logo and website belong to the original project; this fork does not use the KafkaJS logo and is not affiliated with or endorsed by the KafkaJS maintainers.

Apache Kafka and Kafka are trademarks of The Apache Software Foundation. This project is not affiliated with or endorsed by the Apache Software Foundation.
