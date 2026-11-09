const { integrationPlan } = require('../integrationPlan')

describe('integration lane selection', () => {
  test.each(['push', 'pull_request', 'workflow_dispatch'])(
    '%s with no selector retains all broker and share variants',
    (event) => {
      const { include } = integrationPlan(event, '')
      expect(include).toHaveLength(15)
      expect(include.filter(({ group }) => group === 'broker').map(({ node }) => node)).toEqual([
        24, 22, 26,
      ])
      expect(include.filter(({ group }) => group === 'share-base')).toEqual([
        { node: 24, kafka: '4.3.1', group: 'share-base', timeout: 20 },
        { node: 24, kafka: '4.4.0', group: 'share-base', clients: '4.4.0' },
      ])
    }
  )

  test('manual selection trims and deduplicates groups and preserves every selected variant', () => {
    expect(integrationPlan('workflow_dispatch', ' share-base, admin,share-base ')).toEqual({
      include: [
        { node: 24, kafka: '4.3.1', group: 'admin' },
        { node: 24, kafka: '4.3.1', group: 'share-base', timeout: 20 },
        { node: 24, kafka: '4.4.0', group: 'share-base', clients: '4.4.0' },
      ],
    })
  })

  test.each(['push', 'pull_request'])('%s cannot be narrowed by a selector', (event) => {
    expect(integrationPlan(event, 'admin').include).toHaveLength(15)
  })

  test('whitespace is an empty selector', () => {
    expect(integrationPlan('workflow_dispatch', '  ').include).toHaveLength(15)
  })

  test.each(['admn', 'share-base,typo', ',', 'admin,', 'admin,,broker'])(
    'rejects invalid selector %p instead of silently dropping requested lanes',
    (lanes) => {
      expect(() => integrationPlan('workflow_dispatch', lanes)).toThrow(/Unknown integration group/)
    }
  )
})
