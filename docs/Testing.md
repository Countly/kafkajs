---
id: testing
title: Testing
---

We rely on a suite of largely integration tests, using `jest` as a test runner, alongside a smaller set of broker-free unit tests.

## Unit tests

`yarn test:unit` runs the tests that don't need a running Kafka cluster (protocol encoding/decoding, utilities, retry logic, partitioners, assigners, loggers, instrumentation). Use this for fast, iterative feedback while working on broker-free code.

## Integration tests

Most of the test suite talks to a real Kafka cluster. `yarn test` lints the code, boots the cluster, runs everything, and tears the cluster down again — this is what CI runs.

For development, it's usually more convenient to boot the cluster yourself and keep the tests running in watch mode. After starting the cluster as described in [Development Environment](DevelopmentEnvironment.md), run:

```sh
yarn test:local:watch
```

`jest`'s interactive mode lets you select which tests to run.

Alternatively, `./scripts/testWithKafka.sh '<command>'` starts the cluster if it isn't already running, waits for it, creates SCRAM credentials, runs `<command>`, and tears the cluster down afterwards:

```sh
./scripts/testWithKafka.sh 'yarn jest'
```

Set `DO_NOT_STOP=1` to leave the cluster running after the command finishes.

By default the suite targets `KAFKA_VERSION=4.3.1`; override it to test against a different broker version:

```sh
KAFKA_VERSION=4.1.2 ./scripts/testWithKafka.sh 'yarn jest'
```

There are also narrower scripts per area — `yarn test:group:broker`, `yarn test:group:admin`, `yarn test:group:producer`, `yarn test:group:consumer`, `yarn test:group:others`, `yarn test:group:oauthbearer` — each with a `:ci` variant that runs it through `testWithKafka.sh`.

### Skipped tests

Some tests are intentionally skipped against certain broker versions or environments. The reasons are tracked in `QUARANTINE.md` at the repository root — check there before assuming a skip is accidental.

## Linting and types

- `yarn lint` runs ESLint over the tracked JS/MJS files.
- `yarn test:types` type-checks the TypeScript definitions in `types/`.

## Packaging smoke test

`yarn test:pack` packs the library with `npm pack`, installs the resulting tarball into a throwaway directory, and runs an ESM/CJS interop smoke test plus a Node 16-module-resolution types check against it. This is the closest local equivalent to what a real consumer installing the package would get.

## Logging

Logs are usually disabled during test runs. To enable logging, override the log level with the environment variable `KAFKAJS_LOG_LEVEL`.

In order to avoid overloading the terminal with huge logs, we filter any buffers that would otherwise be printed from the protocol debug logs. If you need to see the actual buffer values in your logs, override this with the environment variable `KAFKAJS_DEBUG_PROTOCOL_BUFFERS`:

```sh
KAFKAJS_LOG_LEVEL=debug KAFKAJS_DEBUG_PROTOCOL_BUFFERS=1 yarn test:local:watch
```

Because the `Fetch` response log can be extremely large, we always filter the `Fetch` response payload, unless the `KAFKAJS_DEBUG_EXTENDED_PROTOCOL_BUFFERS` environment variable is *also* set. To see the full `Fetch` response buffer values, run with both flags set:

```sh
KAFKAJS_LOG_LEVEL=debug KAFKAJS_DEBUG_PROTOCOL_BUFFERS=1 KAFKAJS_DEBUG_EXTENDED_PROTOCOL_BUFFERS=1 yarn test:local:watch
```

## Helpers

[`testHelpers/index.js`](../testHelpers/index.js) provides several helpers for connecting to your cluster and asserting upon its state. Tests are typically co-located with the code that they are testing. See [`src/consumer/__tests__/consumeMessages.spec.js`](../src/consumer/__tests__/consumeMessages.spec.js) for an example that makes use of the helpers to implement an integration test.

## Countly canary (manual until 3.0.0)
1. `yarn test:pack` → note the tarball path printed (`$PACK_DIR/countly-kafkajs-3.0.0-alpha.N.tgz`).
2. In `../new_countly/countly-platform/plugins/kafka`: `npm install <tarball>` (replaces the bundled kafkajs 2.2.4 for this run only; do not commit the lockfile change).
3. Disable the two monkey-patches for the run: `api/lib/kafkaRequestQueueFix.js` and `api/lib/kafkaSyncGroupTimeoutFix.js` must not be applied — until Countly gains a bypass flag in `kafkaClient.js` (Countly follow-up), stub both files to `module.exports = () => {}` locally.
4. Run Countly's kafka unit suite (`plugins/kafka/tests/unit`, per Countly's README) against the fork.
5. Record the result in the phase's test log kept by the maintainers (not in this repo). Revert the local Countly changes.
