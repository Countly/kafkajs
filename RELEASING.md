# Releasing (used at 3.0.0 and later)

1. `yarn lint && yarn test:unit && yarn test:pack && yarn test:types`
2. Full integration matrix green (`.github/workflows/weekly.yml` dispatch on the release commit).
3. Countly `plugins/kafka` canary green against the packed tarball (see `docs/Testing.md`).
4. `CHANGELOG.md` section renamed from "Unreleased" to the version + date; `package.json` version bumped.
5. Tag and publish are done by the maintainer (`npm publish --access public`); never from CI in this phase.
