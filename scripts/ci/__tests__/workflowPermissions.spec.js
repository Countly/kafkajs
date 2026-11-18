const fs = require('node:fs')
const path = require('node:path')
const yaml = require('js-yaml')

const WORKFLOWS_DIR = path.join(__dirname, '..', '..', '..', '.github', 'workflows')
const READ_ONLY = { contents: 'read' }
const PUBLISH_JOBS = ['release.yml#publish', 'release.yml#publish-otel']

/**
 * Parses every workflow file in the repository.
 * @returns {Object<string, any>} documents keyed by file name
 */
const loadWorkflows = () =>
  Object.fromEntries(
    fs
      .readdirSync(WORKFLOWS_DIR)
      .filter((file) => /\.ya?ml$/.test(file))
      .sort()
      .map((file) => [file, yaml.load(fs.readFileSync(path.join(WORKFLOWS_DIR, file), 'utf8'))])
  )

/**
 * @param {unknown} permissions a `permissions` value: a scope map or a `read-all`/`write-all` string
 * @returns {boolean} true when it lets the job mint an OIDC token
 */
const grantsIdToken = (permissions) =>
  permissions === 'write-all' || permissions?.['id-token'] === 'write'

describe('workflow permissions', () => {
  const workflows = loadWorkflows()

  test('ci and weekly grant only contents: read at top level', () => {
    expect(workflows['ci.yml'].permissions).toEqual(READ_ONLY)
    expect(workflows['weekly.yml'].permissions).toEqual(READ_ONLY)
  })

  test('every workflow has top-level permissions or per-job permissions on every job', () => {
    const files = Object.keys(workflows)
    const unrestricted = Object.entries(workflows)
      .filter(([, workflow]) => workflow.permissions === undefined)
      .flatMap(([file, workflow]) =>
        Object.entries(workflow.jobs)
          .filter(([, job]) => job.permissions === undefined)
          .map(([name]) => `${file}#${name}`)
      )

    expect(files).toEqual(expect.arrayContaining(['ci.yml', 'weekly.yml', 'release.yml']))
    expect(unrestricted).toEqual([])
  })

  test('id-token write appears only on release publish jobs', () => {
    const writers = Object.entries(workflows).flatMap(([file, workflow]) => [
      ...(grantsIdToken(workflow.permissions) ? [`${file}#<workflow>`] : []),
      ...Object.entries(workflow.jobs)
        .filter(([, job]) => grantsIdToken(job.permissions))
        .map(([name]) => `${file}#${name}`),
    ])

    expect(writers).toEqual(PUBLISH_JOBS)
  })
})
