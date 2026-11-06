const fs = require('node:fs')
const path = require('node:path')
const ts = require('typescript')

const root = path.resolve(__dirname, '../../..')
const declaration = path.join(root, 'types/index.d.ts')
const program = ts.createProgram([declaration], { strict: true, skipLibCheck: true })
const checker = program.getTypeChecker()
const source = program.getSourceFile(declaration)
const admin = source.statements.find(
  (node) => ts.isTypeAliasDeclaration(node) && node.name.text === 'Admin'
)

/** A published ledger parsed independently of the declarations it must cover. */
class AdminLedger {
  /** @param {string} markdown published Admin documentation @returns {AdminLedger} parsed rows */
  constructor(markdown) {
    this.markdown = markdown
    this.methods = this.table('Admin API ledger')
    this.families = this.table('Admin evidence by family')
    this.java = this.table('Java administration dispositions')
  }

  /**
   * Parses a single Markdown table, stopping before the next section.
   * @param {string} heading exact section heading
   * @returns {string[][]} non-header cells with code formatting removed
   * @throws {Error} missing heading
   */
  table(heading) {
    const marker = `## ${heading}\n`
    const start = this.markdown.indexOf(marker)
    expect(start).toBeGreaterThanOrEqual(0)
    return this.markdown
      .slice(start + marker.length)
      .split(/\n## /)[0]
      .split('\n')
      .filter((line) => line.startsWith('|') && !line.startsWith('|---'))
      .slice(1)
      .map((line) =>
        line
          .split('|')
          .slice(1, -1)
          .map((cell) => cell.trim().replace(/`/g, ''))
      )
  }
}

/**
 * Finds every option declared on any overload, omitting JavaScript primitive prototypes.
 * @param {import('typescript').Symbol} member one public method
 * @returns {string[]} sorted direct options; array item fields remain in method reference prose
 */
const optionsOf = (member) => {
  const names = new Set()
  for (const signature of checker.getTypeOfSymbolAtLocation(member, admin).getCallSignatures()) {
    for (const parameter of signature.getParameters()) {
      const type = checker.getNonNullableType(checker.getTypeOfSymbolAtLocation(parameter, admin))
      if (checker.isArrayType(type) || checker.isTupleType(type)) continue
      for (const property of type.getProperties()) {
        if (property.declarations?.some((node) => node.getSourceFile() === source))
          names.add(property.name)
      }
    }
  }
  return [...names].sort()
}

/** @returns {AdminLedger} fresh published table for each independent assertion */
const readLedger = () => new AdminLedger(fs.readFileSync(path.join(root, 'docs/Admin.md'), 'utf8'))

describe('admin API ledger', () => {
  test('every public method and every declared option has exactly one reference row', () => {
    const ledger = readLedger()
    const methods = checker
      .getTypeAtLocation(admin)
      .getProperties()
      .filter(
        (member) => checker.getTypeOfSymbolAtLocation(member, admin).getCallSignatures().length > 0
      )
    expect(ledger.methods.map(([method]) => method).sort()).toEqual(
      methods.map((member) => member.name).sort()
    )
    for (const member of methods) {
      const rows = ledger.methods.filter(([name]) => name === member.name)
      expect(rows).toHaveLength(1)
      const options = rows[0][1] === 'none' ? [] : rows[0][1].split(', ')
      expect(options.sort()).toEqual(optionsOf(member))
    }
  })

  test('each wire family has four evidence kinds and exactly one final live arm', () => {
    const ledger = readLedger()
    const familyNames = ledger.families.map(([family]) => family)
    expect(new Set(familyNames).size).toBe(familyNames.length)
    for (const [, , family] of ledger.methods) expect(familyNames).toContain(family)
    for (const [family, goldens, downgrade, partial, live] of ledger.families) {
      expect(
        [goldens, downgrade, partial].every((cell) => cell.length > 0 && cell !== 'pending')
      ).toBe(true)
      const arms = live.split(', ')
      expect(arms).toHaveLength(1)
      expect(arms[0]).toMatch(/^test:e2e:admin-(ledger|logdirs|tokens|lifecycle|quota|tiered)$/)
      if (family === 'lifecycle' || family === 'instrumentation')
        expect(goldens).toBe('not applicable: no wire codec')
      else expect(goldens).toContain('src/protocol/')
    }
  })

  test('Java equivalents, retained differences and deferred families have explicit dispositions', () => {
    const ledger = readLedger()
    const javaMethods = ledger.java.map(([name]) => name)
    const expected = [
      'abortTransaction',
      'addRaftVoter',
      'alterClientQuotas',
      'alterConsumerGroupOffsets',
      'alterPartitionReassignments',
      'alterReplicaLogDirs',
      'alterShareGroupOffsets',
      'alterStreamsGroupOffsets',
      'alterUserScramCredentials',
      'clientInstanceId',
      'close',
      'createAcls',
      'createDelegationToken',
      'createPartitions',
      'createTopics',
      'deleteAcls',
      'deleteConsumerGroupOffsets',
      'deleteConsumerGroups',
      'deleteRecords',
      'deleteShareGroupOffsets',
      'deleteShareGroups',
      'deleteStreamsGroupOffsets',
      'deleteStreamsGroups',
      'deleteTopics',
      'describeAcls',
      'describeClassicGroups',
      'describeClientQuotas',
      'describeCluster',
      'describeConfigs',
      'describeConsumerGroups',
      'describeDelegationToken',
      'describeFeatures',
      'describeLogDirs',
      'describeMetadataQuorum',
      'describeProducers',
      'describeReplicaLogDirs',
      'describeShareGroups',
      'describeStreamsGroups',
      'describeTopics',
      'describeTransactions',
      'describeUserScramCredentials',
      'electLeaders',
      'expireDelegationToken',
      'fenceProducers',
      'forceTerminateTransaction',
      'incrementalAlterConfigs',
      'listClientMetricsResources',
      'listConfigResources',
      'listConsumerGroupOffsets',
      'listConsumerGroups',
      'listGroups',
      'listOffsets',
      'listPartitionReassignments',
      'listShareGroupOffsets',
      'listStreamsGroupOffsets',
      'listTopics',
      'listTransactions',
      'metrics',
      'registerMetricForSubscription',
      'removeMembersFromConsumerGroup',
      'removeRaftVoter',
      'renewDelegationToken',
      'unregisterBroker',
      'unregisterMetricFromSubscription',
      'updateFeatures',
      'alterConfigs',
      'AbstractOptions.timeoutMs',
      'default.api.timeout.ms',
      'retries',
    ]
    expect(javaMethods.sort()).toEqual(expected.sort())
    for (const [, disposition, equivalent, options] of ledger.java) {
      expect([
        'have',
        'implemented',
        'equivalent',
        'retained compatibility',
        'excluded',
        'consumer groups',
        'telemetry',
      ]).toContain(disposition)
      expect(equivalent.length).toBeGreaterThan(0)
      expect(options.length).toBeGreaterThan(0)
    }
    expect(ledger.markdown.replace(/`/g, '')).toContain('createTopics returns a boolean')
    expect(ledger.markdown).toContain('legacy alterConfigs')
    expect(ledger.markdown).toContain('exactly one live arm')
  })
})
