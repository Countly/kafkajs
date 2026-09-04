---
id: development-environment
title: Development Environment
---

## Requirements

- [Node.js](https://nodejs.org/) 22 or later (npm 10+, bundled with Node)
- [`docker`](https://docs.docker.com/) 20.10.4 or later, with the `docker compose` plugin (Compose v2 — not the standalone `docker-compose` binary)

## Running Kafka

The test suite runs against a local 3-node Kafka cluster (KRaft mode, no ZooKeeper) started with `docker compose`. See [Running Kafka in Development](DockerLocal.md) for the cluster layout.

From the root of the repository:

```sh
NO_LOGS=1 ./scripts/dockerComposeUp.sh
./scripts/waitForKafka.js
./scripts/createScramCredentials.sh
```

This boots the cluster described by `docker-compose.yml` (see [scripts/dockerComposeUp.sh](../scripts/dockerComposeUp.sh)). Without `NO_LOGS=1`, the script blocks on `docker compose logs -f`, so set it when you want the shell back. By default it runs `apache/kafka:4.3.1`; pin a different broker version with `KAFKA_VERSION`:

```sh
KAFKA_VERSION=4.1.2 NO_LOGS=1 ./scripts/dockerComposeUp.sh
```

To use a different compose file (for example the OAUTHBEARER variant), set `COMPOSE_FILE`:

```sh
COMPOSE_FILE=docker-compose.oauthbearer.yml NO_LOGS=1 ./scripts/dockerComposeUp.sh
```

`./scripts/waitForKafka.js` waits for the cluster to accept connections; run it before `createScramCredentials.sh`, since SCRAM credential creation needs a broker that is already up. Once it returns, `docker compose -f docker-compose.yml ps` should show `kafka1`, `kafka2` and `kafka3` running (no healthcheck is defined, so `docker compose ps` won't report a health status).

`./scripts/createScramCredentials.sh` registers a SCRAM user (`testscram` by default) on the running cluster; pass `-h` to see the available options. The PLAIN SASL users (`test`, `bob`, `alice`) are provisioned as part of the broker configuration itself and need no extra step.

You should now be able to connect to your cluster:

```javascript
const fs = require('fs')
const { Kafka, CompressionTypes, logLevel } = require('./index')

const kafka = new Kafka({
  logLevel: logLevel.DEBUG,
  brokers: ['localhost:9094', 'localhost:9097', 'localhost:9100'],
  clientId: 'example-producer',
  ssl: {
    servername: 'localhost',
    rejectUnauthorized: false,
    ca: [fs.readFileSync('./testHelpers/certs/cert-signed', 'utf-8')],
  },
  sasl: {
    mechanism: 'plain',
    username: 'test',
    password: 'testtest',
  },
})
```

## Running the tests

- `npm run test:unit` runs the broker-free unit tests (no cluster required).
- `npx jest` runs the full test suite against a cluster you've already started, defaulting to `KAFKA_VERSION=4.3.1`.
- `npm run test:kafka -- "npx jest"` starts the cluster (if it isn't already running), waits for it, creates SCRAM credentials, runs the given command, and tears the cluster down afterwards. Pin a broker version the same way:

  ```sh
  KAFKA_VERSION=4.1.2 npm run test:kafka -- "npx jest"
  ```

See [Testing](Testing.md) for the full breakdown of test scripts.

## Visual Studio Code Integration

In order to [better integrate with Visual Studio Code's Javascript Language Service](https://code.visualstudio.com/docs/languages/jsconfig),
you can add a `jsconfig.json` file to the root of the project.

```json
{
  "compilerOptions": {
    "baseUrl": ".",
    "module": "commonjs",
    "target": "es6",
    "paths": {
      "testHelpers": ["./testHelpers"]
    }
  },
  "include": [
    "src",
    "testHelpers"
  ]
}
```

This can help Visual Studio Code show type-hints even for modules that don't directly import each other:

![Editor screenshot showing type hinting for Cluster methods](./assets/vscode-integration.png)
