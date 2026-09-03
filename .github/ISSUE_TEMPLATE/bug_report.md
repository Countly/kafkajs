---
name: Bug report
about: Create a report to help us improve
title: ''
labels: ''
assignees: ''

---

**Describe the bug**
A clear and concise description of what the bug is.

**To Reproduce**
Please provide either a link to a:

1. failing test in a fork of this repository (see `src/consumer/__tests__/consumeMessages.spec.js` for the style)
2. repository with an example project reproducing the issue (see `examples/consumer.js`)

If none of the above are possible to provide, please write down the exact steps to reproduce the behavior:
1. Run a producer that continuously produces messages to a topic
2. Run a consumer that subscribes to that topic and logs each topic offset
3. After the consumer has consumed 100 messages, it...

**Expected behavior**
A clear and concise description of what you expected to happen.

**Observed behavior**
A clear and concise description of what *did* happen. Please include any relevant logs [with the log level set to debug](../../docs/Configuration.md#logging).

**Environment:**
 - OS: [e.g. Mac OS 10.15.3]
 - KafkaJS version [e.g. 1.12.0]
 - Kafka version [e.g. 2.3.1]
 - NodeJS version [e.g. 10.13.0]

**Additional context**
Add any other context about the problem here.