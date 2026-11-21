---
id: pre-releases
title: Pre-releases
---

Release candidates of 3.0.0 (`3.0.0-rc.N`) are published to npm on the `next` dist-tag. Install a release candidate by its exact version, for example:

```sh
npm install @countly/kafkajs@3.0.0-rc.1
```

Until 3.0.0 is released, `latest` may also point at a release candidate, so do not rely on a dist-tag to pick a version. Nothing is published automatically on a merge to `master`.

The internal alpha tags (`3.0.0-alpha.N`) used during development of this fork were never published to npm.

The release process is described in [`RELEASING.md`](../RELEASING.md) at the repo root.
