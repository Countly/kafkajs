const fs = require('node:fs')
const path = require('node:path')
const { execFileSync } = require('node:child_process')
const { inventory } = require('../../../ci/shareSoakJob')
const { PROFILES, TITLE, childArgs, digest } = require('../../../ci/shareSoakContract')

/**
 * Serializes scripted two-scope originals and CI/OS port observations, never a live receipt.
 * @param {string} root Temporary archive directory.
 * @param {object[]} recording Real-driver scripted recording.
 * @param {boolean} short Fixed selector.
 * @returns {object} Approved source-port contract, platform-port provenance and resealing helper.
 * @throws {Error} For missing recording/source inputs or an archive write failure.
 */
const shareArchive = (root, recording, short) => {
  const profile = short ? 'short' : 'full'
  const selected = PROFILES[profile]
  const source = {
    commit: 'a'.repeat(40),
    gitTreeObject: 'b'.repeat(40),
    files: execFileSync('git', ['ls-files', '-z'], { encoding: 'utf8' })
      .split('\0')
      .filter(Boolean)
      .sort()
      .map((file) => ({
        path: file,
        bytes: fs.statSync(file).size,
        sha256: digest(fs.readFileSync(file)),
      })),
  }
  const contract = {
    schema: 'share-soak-ci-v1',
    profile,
    selected,
    source,
    node: 'v24.0.0',
    executable: process.execPath,
    argv: childArgs(profile),
    environment: {},
    runId: '123',
    attempt: '1',
    repository: 'owner/repo',
    workflow: '.github/workflows/memory-gates.yml',
    workflowRef: 'owner/repo/.github/workflows/memory-gates.yml@refs/pull/1/merge',
    workflowSha: source.commit,
    event: 'pull_request',
    headRef: 'p9m/controls',
    head: 'c'.repeat(40),
    base: 'd'.repeat(40),
    tuple: {
      kafka: '4.3.1',
      node: 24,
      lane: 'docker',
      fixture: 'docker-compose.yml',
      profile: 'tv2',
    },
    job: `share-soak-${profile}`,
    artifact: `share-soak-${profile}-123-1`,
    validatorRevision: source.files.find(
      (f) => f.path === 'scripts/evidence/shareSoakAcceptance.js'
    ).sha256,
  }
  const rows = recording.map((row) => ({
    at: '2026-11-10T00:00:00Z',
    tree: source.commit,
    node: contract.node,
    brokerVersion: '4.3.1',
    lane: 'docker',
    wireVersions: {},
    config: {},
    ...row,
    ci: { runId: contract.runId, attempt: contract.attempt, profile },
  }))
  const done = rows.filter((r) => ['priming-complete', 'case-complete'].includes(r.event))
  const origin =
    Math.floor(done[0].cleanup.clock.completedAt - done[0].cleanup.clock.performanceAt) - 1000
  const startedAt = origin + 1000
  const exitedAt = Math.ceil(done.at(-1).cleanup.completedAt) + 1
  const write = (name, data) => fs.writeFileSync(path.join(root, name), JSON.stringify(data) + '\n')
  const writeRows = () =>
    fs.writeFileSync(
      path.join(root, `${selected.stem}.jsonl`),
      rows.map((r) => JSON.stringify(r)).join('\n') + '\n'
    )
  writeRows()
  write('contract.json', contract)
  for (const name of ['stdout.log', 'stderr.log']) fs.writeFileSync(path.join(root, name), '')
  write(`${selected.stem}-exit.json`, {
    accepted: true,
    status: 0,
    signal: null,
    watchdogTerminated: false,
    executable: contract.executable,
    argv: contract.argv,
    environment: contract.environment,
    startedAt,
    exitedAt,
    parentPid: 1,
    childPid: 2,
    totalMs: selected.totalMs,
    exitMs: 10000,
  })
  if (short)
    write('share-soak-jest.json', {
      success: true,
      numTotalTests: 1,
      numPassedTests: 1,
      numFailedTests: 0,
      numPendingTests: 0,
      numTodoTests: 0,
      testResults: [
        {
          name: '/src/consumer/share/__tests__/shareSoakLive.spec.js',
          status: 'passed',
          assertionResults: [{ title: TITLE, status: 'passed' }],
        },
      ],
    })
  const lifecycle = {
    origin,
    deadline: origin + selected.jobMs,
    wallAt: 1700000000000,
    runId: '123',
    attempt: '1',
    profile,
    project: 'owned',
    errors: [],
    ownership: { containers: ['node0', 'node1', 'node2'] },
    stages: [
      { name: 'startup', status: 'passed', startedAt: origin, completedAt: startedAt, cap: 240000 },
      {
        name: 'parent',
        status: 'passed',
        startedAt,
        completedAt: exitedAt,
        cap: selected.parentMs,
      },
    ],
  }
  write('lifecycle.json', lifecycle)
  write('admission.json', {
    origin,
    deadline: lifecycle.deadline,
    at: startedAt,
    requiredMs: selected.parentMs + 600000,
    remainingMs: lifecycle.deadline - startedAt,
  })
  const broker = {
    containers: [0, 1, 2].map((i) => ({
      Id: `node${i}`,
      Config: { Image: 'apache/kafka:4.3.1', Labels: { 'com.docker.compose.project': 'owned' } },
    })),
    versions: [0, 1, 2].map(() => ({ version: '4.3.1' })),
  }
  write('broker-startup.json', broker)
  write('broker-facts.json', broker)
  const provenance = {
    repository: contract.repository,
    head: contract.head,
    base: contract.base,
    workflowRef: contract.workflowRef,
    workflowSha: contract.workflowSha,
    runId: '123',
    attempt: '1',
    job: contract.job,
    artifact: contract.artifact,
    conclusion: 'success',
    artifactExpired: false,
    artifactDigest: `sha256:${'e'.repeat(64)}`,
    downloadDigest: `sha256:${'e'.repeat(64)}`,
    testedSha: source.commit,
  }
  const platform = {
    GITHUB_REPOSITORY: contract.repository,
    GITHUB_RUN_ID: '123',
    GITHUB_RUN_ATTEMPT: '1',
    GITHUB_SHA: source.commit,
    GITHUB_WORKFLOW_REF: contract.workflowRef,
    GITHUB_WORKFLOW_SHA: contract.workflowSha,
  }
  const seal = (validation) => {
    if (validation)
      write('validation.json', { ...validation, semanticSha256: digest(JSON.stringify(rows)) })
    return write('seal.json', {
      complete: true,
      files: inventory(root),
      contractSha256: digest(fs.readFileSync(path.join(root, 'contract.json'))),
      excluded: ['seal.json'],
      root: '.',
    })
  }
  const finish = (validation) => {
    write('validation.json', validation)
    for (const name of ['verify', 'logs', 'teardown', 'seal'])
      lifecycle.stages.push({
        name,
        status: 'passed',
        startedAt: exitedAt,
        completedAt: exitedAt,
        cap: 60000,
      })
    write('lifecycle.json', lifecycle)
    write('teardown.json', {
      project: 'owned',
      remaining: { containers: 0, networks: 0, volumes: 0 },
    })
    seal()
  }
  return { contract, rows, provenance, platform, finish, seal, writeRows }
}
module.exports = { shareArchive }
