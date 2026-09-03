# Changelog

All notable changes to `@countly/kafkajs` are recorded here. The fork starts from kafkajs 2.2.4 (Feb 2023).
Versions before 3.0.0 are internal alpha tags and are not published.

## Unreleased — 3.0.0

### Phase 1 — foundation
- Package renamed to `@countly/kafkajs`; strict `exports` map (`src/**` no longer importable) and ESM entry `index.mjs`.
- Node.js 22+ required; Jest 30; TypeScript 5 for the type tests.
- Test cluster moved to `apache/kafka` 4.x (KRaft); Kafka < 4.0 is no longer supported.

Adopted upstream PRs are credited here by number as they land (Phase 2 onward).
