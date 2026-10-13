---
id: testing
title: Testing
---

We rely on a suite of largely integration tests, using `jest` as a test runner, alongside a smaller set of broker-free unit tests.

## Unit tests

`npm run test:unit` runs the tests that don't need a running Kafka cluster (protocol encoding/decoding, utilities, retry logic, partitioners, assigners, loggers, instrumentation). Use this for fast, iterative feedback while working on broker-free code.

## Integration tests

Most of the test suite talks to a real Kafka cluster. `npm test` runs `npm run lint`, `npm run compat:check` and `npm run protocol:check`, boots the cluster, runs everything, and tears the cluster down again — it's a convenient local shortcut, but it is not what CI runs.

CI (`.github/workflows/ci.yml`) has four kinds of job. `lint` runs `npm run lint`, `npm run compat:check` and `npm run test:types`; `unit` runs `npm run test:unit` on Node 22, 24 and 26; `pack` runs `npm run test:pack`; and `integration` starts a cluster per matrix row and calls `./scripts/testWithKafka.sh "npm run test:group:<name> -- --ci --maxWorkers=4"` directly, for the six groups (`broker`, `admin`, `producer`, `consumer`, `others`, `oauthbearer`) on Node 24 and for `broker` and `others` on Node 22 and 26, all against Kafka 4.3.1. `.github/workflows/weekly.yml` runs the six groups on Node 22, 24 and 26 against Kafka 4.1.2, 4.2.2 and 4.3.1 every Monday and on demand; its matrix leaves out Node 22 with 4.2.2 and Node 26 with 4.1.2 and 4.2.2.

For development, it's usually more convenient to boot the cluster yourself and keep the tests running in watch mode. After starting the cluster as described in [Development Environment](DevelopmentEnvironment.md), run:

```sh
npx jest --watch
```

`jest`'s interactive mode lets you select which tests to run. `NODE_ENV`, `KAFKA_VERSION` (default `4.3.1`) and `KAFKAJS_DEBUG_PROTOCOL_BUFFERS` all have defaults set in `testHelpers/setup.js`, so a bare `npx jest` works with no environment variables.

Alternatively, `npm run test:kafka -- '<command>'` (a thin wrapper over `./scripts/testWithKafka.sh`) starts the cluster if it isn't already running, waits for it, creates SCRAM credentials, runs `<command>`, and tears the cluster down afterwards:

```sh
npm run test:kafka -- "npx jest"
```

Set `DO_NOT_STOP=1` to leave the cluster running after the command finishes.

By default the suite targets `KAFKA_VERSION=4.3.1`; override it to test against a different broker version:

```sh
KAFKA_VERSION=4.1.2 npm run test:kafka -- "npx jest"
```

There are also narrower scripts per area — `npm run test:group:broker`, `npm run test:group:admin`, `npm run test:group:producer`, `npm run test:group:consumer`, `npm run test:group:others`, `npm run test:group:oauthbearer` — each run through `test:kafka` the same way, e.g. `npm run test:kafka -- "npm run test:group:broker"`. The OAUTHBEARER group needs the OAUTHBEARER-enabled compose file:

```sh
COMPOSE_FILE=docker-compose.oauthbearer.yml npm run test:kafka -- "npm run test:group:oauthbearer"
```

### Topics in tests

Create a spec's topics with `createTopic` from `testHelpers`. On KRaft the controller commits a new topic before every broker knows about it, so `createTopic` calls `admin.createTopics({ waitForLeaders: true })`, then waits (up to 30 seconds) until every broker serves metadata for the topic with a leader for each partition. It makes up to ten attempts, and repeats the whole attempt while a broker still answers `UNKNOWN_TOPIC_OR_PARTITION` or `LEADER_NOT_AVAILABLE`.

That repeat hides a race: on a freshly started cluster the broker that `admin.createTopics({ waitForLeaders: true })` asks for the new topic's leaders can answer `UNKNOWN_TOPIC_OR_PARTITION` for a moment, and the client's own wait now keeps waiting through it (within `timeout`). A spec that creates its topics with `createTopic` cannot show whether that wait works. To exercise it on a cold cluster, call `admin.createTopics` directly, as `src/admin/__tests__/createTopicsColdLive.spec.js` does (`npm run test:e2e:create-topics-cold`).

### Live scenarios

A scenario that needs a cluster in a particular state, or a long time, is a separate spec gated by an environment variable: it skips itself without the variable, so the groups above only skip it, and it has an `npm run test:e2e:<name>` script that starts the cluster through `test:kafka` and sets the variable, for example `npm run test:e2e:create-topics-cold`. Most scenarios write what they observed to `.tmp/e2e/<name>.jsonl` (through `writeEvidence` in `testHelpers`), which Git ignores. `npm run probe:api-versions` prints, as one JSON line, the version range the broker at `localhost:9092` offers for each API the admin and group codecs use (`KAFKA_VERSION` selects another supported cluster).

### Skipped tests

Some tests are intentionally skipped against certain broker versions or environments. The reasons are tracked in `QUARANTINE.md` at the repository root — check there before assuming a skip is accidental.

## Linting and types

- `npm run lint` runs ESLint over the whole tree; files listed in `.gitignore` are skipped (Prettier additionally honours `.prettierignore`), so there is no `git ls-files` shell-out.
- `npm run test:types` type-checks the TypeScript definitions in `types/`.

## Packaging smoke test

`npm run test:pack` packs the library with `npm pack`, fails if the tarball holds a spec file, a fixture, a type-test file or anything from `otel/`, installs the tarball into a throwaway directory, and runs an ESM/CJS interop smoke test plus a Node 16-module-resolution types check against it. This is the closest local equivalent to what a real consumer installing the package would get.

## OpenTelemetry package

`otel/` is a separate package, the OpenTelemetry instrumentation for this library, with its own `package.json`, lockfile and dependencies. `npm run test:otel` packs the library, installs the tarball into `otel/` the way a consumer would, and runs that package's own tests. It needs the npm registry and is not part of `npm test`.

## Logging

Logs are usually disabled during test runs. To enable logging, override the log level with the environment variable `KAFKAJS_LOG_LEVEL`.

In order to avoid overloading the terminal with huge logs, we filter any buffers that would otherwise be printed from the protocol debug logs. If you need to see the actual buffer values in your logs, override this with the environment variable `KAFKAJS_DEBUG_PROTOCOL_BUFFERS`:

```sh
KAFKAJS_LOG_LEVEL=debug npx jest --watch
```

Because the `Fetch` response log can be extremely large, we always filter the `Fetch` response payload, unless the `KAFKAJS_DEBUG_EXTENDED_PROTOCOL_BUFFERS` environment variable is *also* set. To see the full `Fetch` response buffer values, run with both flags set:

```sh
KAFKAJS_LOG_LEVEL=debug KAFKAJS_DEBUG_EXTENDED_PROTOCOL_BUFFERS=1 npx jest --watch
```

## Helpers

[`testHelpers/index.js`](../testHelpers/index.js) provides several helpers for connecting to your cluster and asserting upon its state. Tests are typically co-located with the code that they are testing. See [`src/consumer/__tests__/consumeMessages.spec.js`](../src/consumer/__tests__/consumeMessages.spec.js) for an example that makes use of the helpers to implement an integration test.

## Countly canary (manual until 3.0.0)

The canary runs the unit tests of the Countly platform's Kafka plugin against the packed library. It is manual and not part of CI. Never run it in the Countly checkout you work in: it swaps a dependency and rewrites several of its files (the patch files and, in step 7, `observability/otel.js`). Use a separate checkout of Countly (a worktree or a copy) derived from its `main` branch, called `$COUNTLY_COPY` below. A worktree has no `node_modules`: run `npm ci` at its root first (a plain copy of a working checkout already has them).

1. In this repository, run `PACK_DIR=$(mktemp -d) npm run test:pack`. It packs the library, checks the tarball and prints its path (`tarball: <directory>/countly-kafkajs-<version>.tgz`); the file stays in that directory.
2. In `$COUNTLY_COPY/plugins/kafka`, install the tarball under the name `kafkajs`: `npm install "kafkajs@file:<absolute path of the tarball>"`. Countly requires `kafkajs`, and a plain `npm install <tarball>` adds a second copy named `@countly/kafkajs` and leaves 2.2.4 in place, so the run would test 2.2.4. Do not commit the change to the manifest or the lockfile.
3. Prove which package loads. From `$COUNTLY_COPY/plugins/kafka`, run `node -p "require('kafkajs/package.json').name + ' ' + require('kafkajs/package.json').version"`. It must print `@countly/kafkajs` and the tarball's version, for example `@countly/kafkajs 3.0.0-alpha.8`. If it prints `kafkajs 2.2.4`, stop: Countly lists `kafkajs` at its root and in `plugins/kafka`, and an install in the wrong place silently tests 2.2.4.
4. In `$COUNTLY_COPY/plugins/kafka`, stand in for Countly's deep-path patches (the paths in this step are relative to that directory). `api/lib/kafkaRequestQueueFix.js` and `api/lib/kafkaSyncGroupTimeoutFix.js` require files under `kafkajs/src/`, which the `exports` map of this package does not expose, so against it they throw `ERR_PACKAGE_PATH_NOT_EXPORTED` and cannot load; they patch behaviour this library implements itself. Countly's `api/lib/kafkaClient.js` still calls `kafkaRequestQueueFix.isPatched()` and throws when it is false, so a stand-in keeps the export names. Replace `api/lib/kafkaRequestQueueFix.js` with `module.exports = { isPatched: () => true, restore: () => {} }`. Replace any other patch file your copy still has, such as `api/lib/kafkaSyncGroupTimeoutFix.js`, with a module that exports the same names as the real file (read them from it first), each a function that does nothing, or answers true for a check named `isPatched`.
5. From the root of `$COUNTLY_COPY`, run the plugin's unit tests without `plugins/kafka/tests/unit/kafkaClientTimeouts.unit.js`, which belongs to the patches (these are the options of Countly's own `test:unit` script, narrowed to the plugin):

   ```sh
   NODE_PATH=./node_modules/ npx mocha --preserve-symlinks --preserve-symlinks-main \
     $(ls plugins/kafka/tests/unit/*.unit.js | grep -v kafkaClientTimeouts) \
     --reporter spec --timeout 50000 --exit
   ```

6. Compare the result with the previous canary's, which the maintainers keep in their release test log (not in this repository): the same tests must pass and fail as before. Delete `$COUNTLY_COPY` when you are done (after step 7, if you run it).
7. To canary the instrumentation as well, pack it with `npm pack --pack-destination <directory>` in `otel/`, install that tarball at the root of `$COUNTLY_COPY`, and in `observability/otel.js` require `@countly/opentelemetry-instrumentation-kafkajs` instead of `@opentelemetry/instrumentation-kafkajs`. The step 5 tests never load that file, so load it yourself from the root of `$COUNTLY_COPY`: `node -e "require('./observability/otel.js')"` must finish without a `Cannot find module` or constructor error. The instrumentation hooks the module named `@countly/kafkajs`, but step 2 installs the library as `kafkajs`, so the hook never fires: this shows that the package loads, not that spans are produced. Checking spans needs Countly to require `@countly/kafkajs`.
