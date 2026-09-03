# Contributing to @countly/kafkajs

Contributions are made through Issues and Pull Requests on https://github.com/Countly/kafkajs.

## Issues
- Search existing issues before opening a new one; add reproduction details to an existing issue when it matches yours.
- Use the issue templates. Include Kafka broker version (4.0+ only is supported), Node.js version, and debug-level logs where relevant.
- Issues are not a support channel for general Kafka questions.

## Pull requests
- One concern per PR; add tests for fixed or changed behavior (`yarn test:unit` for broker-free specs, `./scripts/testWithKafka.sh 'yarn jest'` for the integration suite — see `docs/DevelopmentEnvironment.md`).
- Update `types/index.d.ts` when a public interface changes, and the relevant page under `docs/`.
- Behavior changes to the public API need an issue first; the API is a compatibility contract with existing KafkaJS users.
- Upstream KafkaJS fixes adopted here are credited by PR number in `CHANGELOG.md`; keep that convention.

## Development
See `docs/DevelopmentEnvironment.md` (Node 22+, Docker, `docker compose` for the KRaft test cluster) and `docs/Testing.md`.
