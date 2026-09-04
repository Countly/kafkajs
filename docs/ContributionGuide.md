---
id: contribution-guide
title: Contributing
---

This is an open-source project where development takes place in the open on GitHub. We're grateful for bug fixes, feature development and other contributions.

Issues are tracked on [GitHub](https://github.com/Countly/kafkajs/issues). If you're planning to implement a new feature or fix a bug, check the issue tracker first to see if someone is already working on it, or open an issue before you start.

## TL;DR

[Development Environment](DevelopmentEnvironment.md), [Running Kafka in Development](DockerLocal.md) and [Testing](Testing.md) cover the full setup and how to run the tests. If you're already familiar with the project setup, here's the gist:

With `docker` (Compose v2), `npm` and Node 22+ available:

```sh
npm run test:unit
# or, with a cluster running (see Development Environment):
NO_LOGS=1 ./scripts/dockerComposeUp.sh
./scripts/waitForKafka.js
./scripts/createScramCredentials.sh
npx jest --watch
```
