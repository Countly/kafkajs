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
      expect(canary).toContain('[OTEL] ✅ OpenTelemetry initialized successfully!')
      expect(canary).toContain('[OTEL] ❌ Failed to initialize OpenTelemetry:')
      expect(canary).toContain('[OTEL] retryWithBackoff exhausted:')
      expect(canary).toContain('makes this command exit nonzero before initialization')
    })
  })
})

describe('documented consumer and retry behavior', () => {
  const { Kafka } = require('../../index')
  const resolveTimeouts = require('../../src/consumer/groupTimeouts')
  const retry = require('../../src/retry')
  const defaults = require('../../src/retry/defaults')
  const consuming = read('docs', 'Consuming.md')
  const faq = read('docs', 'FAQ.md')
  const config = read('docs', 'Configuration.md')
  const detailed = read('docs', 'RetryDetailed.md')
  const intro = read('docs', 'KafkaIntro.md')
  const readme = read('README.md')
  it('documents the validated in-flight limit', () => {
    expect(read('src', 'index.js')).toContain('const DEFAULT_MAX_IN_FLIGHT_REQUESTS = 5')
    const kafka = new Kafka({ brokers: ['localhost:1'] })
    expect(() => kafka.consumer({ groupId: 'docs', maxInFlightRequests: 0 })).toThrow(
      /maxInFlightRequests/
    )
    expect(() => kafka.consumer({ groupId: 'docs', maxInFlightRequests: null })).not.toThrow()
    const row = consuming.split('\n').find((line) => line.startsWith('| maxInFlightRequests'))
    expect(row).toContain('`5`')
    expect(row).toContain('`null` means unbounded')
    expect(row).toContain('`0` is rejected')
    expect(row).not.toMatch(/falsey|falsy/)
  })
  it('separates yielding work, event-loop blocking and the global poll guard', () => {
    const resolved = resolveTimeouts({ rebalanceTimeout: 20000, logger: { warn() {} } })
    expect(resolved.rebalanceTimeout).toBe(20000)
    expect(resolved.pollDeadline).toBe(300000)
    for (const doc of [consuming, faq]) {
      expect(doc).toContain('background heartbeat')
      expect(doc).toContain('event-loop-blocking')
      expect(doc).toContain('maxPollInterval')
      expect(doc).toContain('all partitions')
    }
  })
  it('names the permanent restart exceptions', () => {
    const consumer = read('src', 'consumer', 'index.js')
    expect(consumer).toContain(
      'isFencedInstance(e) || (isConsumerProtocol && isConsumerProtocolFatal(e))'
    )
    expect(config).toContain('fenced static instance')
    expect(config).toContain('fatal membership')
    expect(config).toContain('even if `restartOnFailure` returns `true`')
  })
  it('derives the jittered ranges from the production retry defaults', () => {
    expect(defaults).toMatchObject({ initialRetryTime: 300, factor: 0.2, multiplier: 2 })
    const random = jest.spyOn(Math, 'random')
    try {
      const backoff = retry.backoff(defaults)
      random.mockReturnValue(0)
      expect(backoff.initial()).toBe(240)
      expect(backoff.next(240)).toBe(384)
      expect(backoff.next(384)).toBe(616)
      random.mockReturnValue(1 - Number.EPSILON)
      expect(backoff.initial()).toBe(360)
      expect(backoff.next(360)).toBe(864)
      expect(backoff.next(864)).toBe(2074)
    } finally {
      random.mockRestore()
    }
    for (const range of ['240–360 ms', '384–864 ms', '616–2074 ms'])
      expect(detailed).toContain(range)
    expect(detailed).not.toContain('Always a flat')
  })
  it('describes partition leaders, group coordinators and both membership protocols', () => {
    for (const text of [
      'follower fetching',
      'broker that coordinates',
      'group leader',
      'classic',
      'consumer protocol',
    ])
      expect(intro).toContain(text)
  })
  it('links the preserved API and its documented semantic differences', () => {
    expect(readme).toContain('preserved API plus documented semantic differences')
    expect(readme).toContain('./docs/MigrationGuide-3-0-0.md')
    expect(readme).not.toContain('same runtime semantics')
    expect(readme).toContain('there is no producer-level default')
    expect(readme).toContain('An omitted codec sends uncompressed')
  })
  it('compares both canary reports and requires the span outcome proof', () => {
    expect(page).toContain('node scripts/canary/compare.js')
    expect(page).toContain('previous.json current.json')
    expect(page).toContain('--reporter json')
    expect(page).toContain('send canary-spans')
    expect(page).toContain('Exit 0 from the load command alone is insufficient')
  })
})

describe('documented offset ordering, shared state and telemetry containment', () => {
  const consuming = read('docs', 'Consuming.md')
  const recreation = consuming.slice(
    consuming.indexOf('<a name="topic-recreation"></a>'),
    consuming.indexOf('<a name="topic-metadata"></a>')
  )
  const otelReadme = read('otel', 'README.md')
  it('keeps a recreation reset owed until a confirmed commit for the new topic', () => {
    expect(recreation).toContain(
      'The reset stays owed for a partition until an offset for the new topic is durably committed for it and that commit is confirmed against the new incarnation'
    )
    expect(recreation).toContain('a `ListOffsets` request that fails')
    expect(recreation).toContain('Resolving or seeking locally does not settle it')
    expect(recreation).toContain("the consumer's automatic restart after a crash")
    expect(recreation).toContain('`consumer.disconnect()` drops the debt')
    expect(recreation).toContain('a later seek or commit for the new topic is never undone by it')
  })
  it('orders offset operations by when they began', () => {
    expect(recreation).toContain('ordered by when they began, not by when their answers arrive')
    expect(recreation).toContain('even when its offset is lower and whichever answer arrives first')
    expect(recreation).toContain('A commit already sent cannot be recalled')
  })
  it('documents the cache and ledger one Kafka instance shares, with the publication rule', () => {
    const index = read('src', 'index.js')
    expect(index).toContain('this[PRIVATE.OFFSETS] = new Map()')
    expect(index).toContain('this[PRIVATE.OPERATIONS] = new Map()')
    expect(recreation).toContain('two `Kafka` instances share neither')
    expect(recreation).toContain(
      "only after the transaction's commit is confirmed, once, and a newer seek, reset, commit or recreation"
    )
    expect(recreation).toContain('(Transactions.md#offsets)')
  })
  it('compares the one-shot reset with the Java client', () => {
    expect(recreation).toContain("resets only a partition's last-seen leader epoch")
    expect(recreation).toContain('This client is stricter on purpose')
  })
  it('states the telemetry containment the instrumentation implements', () => {
    const source = read('otel', 'src', 'instrumentation.js')
    expect(source).toContain('function contained(diag, what, work, fallback)')
    expect(source).toContain('function withContext(diag, ctx, call)')
    expect(otelReadme).not.toMatch(/Other telemetry calls are not\s+guarded/)
    expect(otelReadme).toContain('runs exactly once, with your receiver and arguments')
    expect(otelReadme).toContain('A context manager cannot run your code twice')
    expect(otelReadme).toContain('a derived promise that resolves to the same transaction')
    expect(otelReadme).toContain('a rejection with the same reason')
  })
})

describe('documented live scenario scripts', () => {
  /**
   * One row per live scenario, appended by the task that creates its script. `budgetsMs` are the
   * case budgets the scenario's paragraph states; `wording` is text only that paragraph may supply.
   */
  const scenarios = [
    {
      script: 'test:e2e:pause-resume',
      flag: 'PAUSE_RESUME_E2E',
      spec: 'src/consumer/__tests__/pauseResumeLive.spec.js',
      compose: null,
      budgetsMs: [60000],
      wording: [
        'maxWaitTimeInMs 5000',
        'less than 1000 ms',
        'remain paused through an ordinary rebalance',
        'never through this package row',
      ],
    },
    {
      script: 'test:e2e:leader-union',
      flag: 'LEADER_UNION_E2E',
      spec: 'src/consumer/__tests__/leaderUnionLive.spec.js',
      compose: null,
      budgetsMs: [120000, 90000],
      wording: [
        'roundRobin and range',
        'Java in both seats',
        'topic creation during the first round',
        'never through this package row',
      ],
    },
    {
      script: 'test:e2e:consumer-protocol-states',
      flag: 'CONSUMER_PROTOCOL_STATES_E2E',
      spec: 'src/consumer/__tests__/consumerProtocolStatesLive.spec.js',
      compose: 'testHelpers/consumerGroupStates/compose.yml',
      budgetsMs: [120000, 30000],
      wording: [
        'Reconciling and Assigning capture arms',
        'Describe must remain serviceable',
        'named topic created after subscription',
        'never through this package row',
      ],
    },
    {
      script: 'test:e2e:rack-aware',
      flag: 'RACK_AWARE_E2E',
      spec: 'src/consumer/__tests__/rackAwareLive.spec.js',
      compose: 'docker-compose.racks.yml',
      budgetsMs: [150000],
      wording: [
        'replication factor 2 Java vector',
        'next Fetch to an in-rack follower',
        'no-op controls',
        'never through this package row',
      ],
    },
    {
      script: 'test:e2e:offset-reset',
      flag: 'OFFSET_RESET_E2E',
      spec: 'src/consumer/__tests__/offsetResetLive.spec.js',
      compose: null,
      budgetsMs: [90000],
      wording: [
        'explicit record timestamps',
        'three partitions',
        'explicit-commit broker-truth arm',
      ],
    },
    {
      script: 'test:e2e:regex-discovery',
      flag: 'REGEX_DISCOVERY_E2E',
      spec: 'src/consumer/__tests__/regexDiscoveryLive.spec.js',
      compose: null,
      budgetsMs: [180000],
      wording: [
        'created after subscription',
        'removal without automatic recreation',
        "fresh incarnation's offsets",
        'metadataMaxAge: 5000',
      ],
    },
    {
      script: 'test:e2e:topic-recreated',
      flag: 'TOPIC_RECREATED_E2E',
      spec: 'src/consumer/__tests__/topicRecreatedLive.spec.js',
      compose: null,
      budgetsMs: [120000],
      wording: [
        'resumes from fromBeginning',
        'stale committed offset',
        'topic-recreated.jsonl',
        'never through this package row',
      ],
    },
    {
      script: 'test:e2e:direct-consumer',
      flag: 'DIRECT_CONSUMER_E2E',
      spec: 'src/consumer/__tests__/directConsumerLive.spec.js',
      compose: null,
      budgetsMs: [90000],
      wording: [
        'manual assignment with and without a group',
        'acceptance runs use the reviewed guarded live entry',
      ],
    },
    {
      script: 'test:e2e:rebalance-listener',
      flag: 'REBALANCE_LISTENER_E2E',
      spec: 'src/consumer/__tests__/rebalanceListenerLive.spec.js',
      compose: null,
      budgetsMs: [90000],
      wording: [
        'six partitions',
        '600 records',
        '90000 ms',
        'rebalance-listener.jsonl',
        'before first delivery',
      ],
    },
    {
      script: 'test:e2e:bounded-close',
      flag: 'BOUNDED_CLOSE_E2E',
      spec: 'src/consumer/__tests__/boundedCloseLive.spec.js',
      compose: null,
      budgetsMs: [90000],
      wording: ['coordinator blackhole'],
    },
    {
      script: 'test:e2e:auto-commit',
      flag: 'AUTO_COMMIT_E2E',
      spec: 'src/consumer/__tests__/autoCommitCadenceLive.spec.js',
      compose: null,
      budgetsMs: [90000],
      wording: ['quiet window'],
    },
    {
      script: 'test:e2e:group-faults',
      flag: 'GROUP_FAULTS_E2E',
      spec: 'src/consumer/__tests__/groupFaultsLive.spec.js',
      compose: null,
      budgetsMs: [300000],
      wording: [
        'duplicate static instance',
        'short and long coordinator outages',
        'reviewed guarded live entry',
      ],
    },
    {
      script: 'test:e2e:fetch-semantics',
      flag: 'FETCH_SEMANTICS_E2E',
      spec: 'src/consumer/__tests__/fetchSemanticsLive.spec.js',
      compose: null,
      budgetsMs: [120000, 90000],
      wording: [
        'open and aborted transactions',
        'oversized first record',
        'reviewed guarded live entry',
      ],
    },
    {
      script: 'test:e2e:fetch-routing',
      flag: 'FETCH_ROUTING_E2E',
      spec: 'src/consumer/__tests__/fetchRoutingLive.spec.js',
      compose: null,
      budgetsMs: [120000],
      wording: ['sibling has no leader', 'idle and busy bounds', 'eligible follower'],
    },
    {
      script: 'test:e2e:fetch-sessions',
      flag: 'FETCH_SESSIONS_E2E',
      spec: 'src/consumer/__tests__/fetchSessionsLive.spec.js',
      compose: 'docker-compose.racks.yml',
      budgetsMs: [120000],
      wording: ['2001-partition limit probe', 'sparse eligible-follower', 'racks compose'],
    },
    {
      script: 'test:e2e:fetch-sessions-capacity',
      flag: 'FETCH_SESSIONS_CAPACITY_E2E',
      spec: 'src/consumer/__tests__/fetchSessionsCapacityLive.spec.js',
      compose: 'docker-compose.fetch-sessions.yml',
      budgetsMs: [180000],
      wording: ['eight-slot compose variant', 'same-group oracle control'],
    },
  ]

  it.each(scenarios)('$script is one two-worker spec run through test:kafka', (row) => {
    const prefix = row.compose ? `COMPOSE_FILE=${row.compose} ` : ''
    const pattern = row.spec.replace(/\./g, '\\\\.')
    expect(fs.existsSync(path.join(root, row.spec))).toBe(true)
    expect(scripts[row.script]).toBe(
      `${prefix}npm run test:kafka -- "${row.flag}=1 npx jest --maxWorkers=2 --testPathPatterns '${pattern}'"`
    )
  })

  it.each(scenarios)('$script is documented with its budget and its own wording', (row) => {
    const block = page
      .split('\n\n')
      .find((paragraph) => paragraph.includes(`npm run ${row.script}`))
    expect(block).toBeDefined()
    for (const ms of row.budgetsMs) expect(block).toContain(`${ms} ms`)
    for (const text of row.wording) expect(block).toContain(text)
  })

  it('documents the consumer-churn soak profile and its script', () => {
    expect(scripts['soak:consumer-churn']).toBe(
      'npm run test:kafka -- "node --expose-gc scripts/soak/soak.js --profile consumer-churn"'
    )
    const block = page
      .split('\n\n')
      .find((paragraph) => paragraph.startsWith('`soak:consumer-churn`'))
    expect(block).toBeDefined()
    for (const text of [
      'five minutes',
      'three-minute warm-up',
      '2,200-second',
      'not acceptance evidence',
    ])
      expect(block).toContain(text)
    expect(page).toContain('| `npm run soak:consumer-churn` |')
  })
})
