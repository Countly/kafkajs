const { integrationPlan } = require('../integrationPlan')

const SHARE_BASE = { node: 24, kafka: '4.4.0', group: 'share-base', clients: '4.4.0', timeout: 20 }

describe('integration lane selection', () => {
  test.each(['push', 'pull_request', 'workflow_dispatch'])(
    '%s with no selector retains all broker and share variants',
    (event) => {
      const { include } = integrationPlan(event, '')
      expect(include).toHaveLength(13)
      expect(include.filter(({ group }) => group === 'broker').map(({ node }) => node)).toEqual([
        24, 22, 26,
      ])
      expect(include.filter(({ group }) => group === 'share-base')).toEqual([SHARE_BASE])
    }
  )

  test('every integration row runs on Kafka 4.4.0', () => {
    expect(new Set(integrationPlan('push', '').include.map(({ kafka }) => kafka))).toEqual(
      new Set(['4.4.0'])
    )
  })

  test('each share group has one row and the Java client follows the broker where Java runs', () => {
    const { include } = integrationPlan('push', '')
    expect(include.filter(({ group }) => group.startsWith('share'))).toEqual([
      SHARE_BASE,
      {
        node: 24,
        kafka: '4.4.0',
        group: 'share',
        compose: 'docker-compose.share.yml',
        clients: '4.4.0',
      },
      {
        node: 24,
        kafka: '4.4.0',
        group: 'share-limit',
        compose: 'docker-compose.share-limit.yml',
      },
    ])
  })

  test('manual selection trims and deduplicates groups and preserves every selected variant', () => {
    expect(integrationPlan('workflow_dispatch', ' share-base, admin,share-base ')).toEqual({
      include: [{ node: 24, kafka: '4.4.0', group: 'admin' }, SHARE_BASE],
    })
  })

  test.each(['push', 'pull_request'])('%s cannot be narrowed by a selector', (event) => {
    expect(integrationPlan(event, 'admin').include).toHaveLength(13)
  })

  test('whitespace is an empty selector', () => {
    expect(integrationPlan('workflow_dispatch', '  ').include).toHaveLength(13)
  })

  test.each(['admn', 'share-base,typo', ',', 'admin,', 'admin,,broker'])(
    'rejects invalid selector %p instead of silently dropping requested lanes',
    (lanes) => {
      expect(() => integrationPlan('workflow_dispatch', lanes)).toThrow(/Unknown integration group/)
    }
  )
})
