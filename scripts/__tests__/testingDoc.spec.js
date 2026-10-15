const fs = require('fs')
const path = require('path')
const { scripts } = require('../../package.json')

const root = path.join(__dirname, '..', '..')
const read = (...parts) => fs.readFileSync(path.join(root, ...parts), 'utf8')

const page = read('docs', 'Testing.md')
const workflows = ['ci.yml', 'weekly.yml'].map((name) => read('.github', 'workflows', name))
const ci = page.slice(
  page.indexOf('CI (`.github/workflows/ci.yml`)'),
  page.indexOf('For development')
)

/**
 * @param {string} text
 * @param {RegExp} pattern a global pattern with one capture group
 * @returns {string[]} the distinct captures
 */
const captures = (text, pattern) => [...new Set([...text.matchAll(pattern)].map((m) => m[1]))]

describe('docs/Testing.md', () => {
  it('names only npm scripts that exist', () => {
    // A name followed by `<` is a placeholder such as `test:group:<name>`; the lookahead skips it.
    const names = captures(page, /npm run ([a-z][\w:-]*)(?![\w:<-])/g)

    expect(names).toEqual(expect.arrayContaining(['test:unit', 'test:pack', 'test:otel']))
    expect(names.filter((name) => !(name in scripts))).toEqual([])
  })

  it('names only repository paths that exist', () => {
    const tokens = [
      ...captures(page, /`((?:\.\/)?(?:scripts|testHelpers|src|\.github|otel)\/[\w./-]*)`/g),
      ...captures(page, /\b(docker-compose[\w.-]*\.yml|QUARANTINE[\w.-]*\.md)/g),
    ]
    const links = captures(page, /\]\((?!https?:)([^)#]+)\)/g).map((link) =>
      path.join('docs', link)
    )

    expect(tokens.length).toBeGreaterThan(3)
    expect(tokens).toEqual(
      expect.arrayContaining(['QUARANTINE.md', 'docker-compose.oauthbearer.yml', 'otel/'])
    )
    expect([...tokens, ...links].filter((p) => !fs.existsSync(path.join(root, p)))).toEqual([])
  })

  it('names no machine-specific path or local-only file', () => {
    expect(page).not.toMatch(
      /\/Users\/|\/home\/|new_countly|~\/|C:\\|superpowers|upgrade-ref|CLAUDE\.md|AGENT\.md|EXECUTION\.md|\b(?:Phase|Task|Wave) \d/
    )
  })

  it('describes every command and group the workflows run', () => {
    const commands = workflows.flatMap((text) =>
      captures(text, /run: (npm run [^\s"$]+|\.\/scripts\/[^\s"$]+)/g)
    )
    // Matrix rows are `group: broker` or `group: oauthbearer, …`; the weekly matrix is a list.
    const groups = workflows.flatMap((text) => [
      ...captures(text, /\bgroup: (\w+)[ ,}]/g),
      ...captures(text, /\bgroup: \[([\w, ]+)\]/g).flatMap((list) => list.split(/, */)),
    ])

    expect(commands).toEqual(
      expect.arrayContaining(['npm run lint', 'npm run test:unit', './scripts/testWithKafka.sh'])
    )
    expect(groups).toEqual(expect.arrayContaining(['broker', 'oauthbearer']))
    expect(ci).not.toBe('')
    expect(commands.filter((command) => !ci.includes(command))).toEqual([])
    expect(groups.filter((group) => !ci.includes(`\`${group}\``))).toEqual([])
  })

  it('states the Node and Kafka matrices of the workflows', () => {
    expect(workflows[0]).toContain('matrix: { node: [22, 24, 26] }')
    expect(workflows[0]).toContain('{ node: 26, kafka: 4.3.1, group: others }')
    expect(workflows[1]).toContain("kafka: ['4.1.2', '4.2.2', '4.3.1']")
    expect(workflows[1]).toContain("- { node: 26, kafka: '4.2.2' }")
    expect(workflows[1]).toContain("cron: '0 3 * * 1'")
    expect(ci).toContain('on Node 22, 24 and 26;')
    expect(ci).toContain('every Monday and on demand')
    expect(ci).toContain(
      'on Node 24 and for `broker` and `others` on Node 22 and 26, all against Kafka 4.3.1'
    )
    expect(ci).toContain('Kafka 4.1.2, 4.2.2 and 4.3.1')
    expect(ci).toContain('leaves out Node 22 with 4.2.2 and Node 26 with 4.1.2 and 4.2.2')
  })

  it('does not claim the cold-cluster createTopics race is gone from every run', () => {
    expect(page).toContain('### Topics in tests')
    const topics = page.slice(
      page.indexOf('### Topics in tests'),
      page.indexOf('### Live scenarios')
    )

    expect(topics).toContain('createTopic')
    expect(topics).toContain('UNKNOWN_TOPIC_OR_PARTITION')
    expect(topics).toContain('npm run test:e2e:create-topics-cold')
    expect(topics).toContain('up to ten attempts')
    expect(topics).toContain('cannot show whether that wait works')
    expect(topics).not.toMatch(
      /(race|createTopics)[^.]{0,60}(fixed|gone|resolved|solved|no longer)/i
    )
  })

  it('says only most live scenarios write evidence', () => {
    expect(page).toContain('### Live scenarios')
    expect(page).toContain('Most scenarios write what they observed to `.tmp/e2e/<name>.jsonl`')
  })

  it('says the workflows call the cluster script directly', () => {
    expect(workflows[0]).toContain('./scripts/testWithKafka.sh "npm run test:group:')
    expect(page).toContain('calls `./scripts/testWithKafka.sh "npm run test:group:<name>')
  })

  describe('canary section', () => {
    const canary = page.slice(page.indexOf('## Countly canary'))

    it('says why the deep-path patches are stood in for', () => {
      expect(canary).toContain('api/lib/kafkaRequestQueueFix.js')
      expect(canary).toContain('api/lib/kafkaSyncGroupTimeoutFix.js')
      expect(canary).toContain('ERR_PACKAGE_PATH_NOT_EXPORTED')
    })

    it('keeps the export names Countly calls on the request-queue stand-in', () => {
      expect(canary).toContain('module.exports = { isPatched: () => true, restore: () => {} }')
      expect(canary).toContain('kafkaRequestQueueFix.isPatched()')
    })

    it('leaves out the timeout test of the patches', () => {
      expect(canary).toContain('kafkaClientTimeouts.unit.js')
    })

    it('runs on a copy derived from main and installs the tarball as kafkajs', () => {
      expect(canary).toContain('derived from its `main` branch')
      expect(canary).toContain('npm install "kafkajs@file:')
      expect(canary).toContain('run `npm ci` at its root first')
    })

    it("runs the plugin tests with the options of Countly's own script", () => {
      expect(canary).toContain(
        'NODE_PATH=./node_modules/ npx mocha --preserve-symlinks --preserve-symlinks-main'
      )
      expect(canary).toContain('grep -v kafkaClientTimeouts')
    })

    it('proves which package the copy loads', () => {
      expect(canary).toContain(
        `node -p "require('kafkajs/package.json').name + ' ' + require('kafkajs/package.json').version"`
      )
      expect(canary).toContain('@countly/kafkajs 3.0.0-alpha')
      expect(canary).toContain('`kafkajs 2.2.4`, stop')
    })

    it('replaces the upstream instrumentation package', () => {
      expect(canary).toContain('@opentelemetry/instrumentation-kafkajs')
      expect(canary).toContain('@countly/opentelemetry-instrumentation-kafkajs')
      expect(canary).toContain(`node -e "require('./observability/otel.js')"`)
      expect(canary).toContain(`OTEL_ENABLED=true node -e "require('./observability/otel.js')"`)
      expect(canary).toContain('[OTEL] failed to start')
    })
  })
})
