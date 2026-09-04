const websiteUrl = require('./websiteUrl')

describe('Utils > websiteUrl', () => {
  it('points at this fork, not kafka.js.org', () => {
    expect(websiteUrl('docs/faq')).not.toContain('kafka.js.org')
    expect(websiteUrl('docs/faq')).toContain('github.com/Countly/kafkajs')
  })

  it.each([
    ['docs/faq', 'docs/FAQ.md'],
    ['docs/configuration', 'docs/Configuration.md'],
    ['docs/migration-guide-v2.0.0', 'docs/MigrationGuide-2-0-0.md'],
  ])('maps the %s slug to %s', (slug, file) => {
    expect(websiteUrl(slug)).toContain(file)
  })

  it('appends the anchor', () => {
    expect(websiteUrl('docs/configuration', 'sasl')).toMatch(/#sasl$/)
    expect(websiteUrl('docs/configuration', '#sasl')).toMatch(/#sasl$/)
  })

  it('keeps an unknown slug visible instead of silently dropping it', () => {
    const url = websiteUrl('docs/unknown-page')
    expect(url).toContain('github.com/Countly/kafkajs')
    expect(url).toContain('docs/unknown-page')
  })
})
