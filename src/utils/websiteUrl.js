const BASE_URL = 'https://github.com/Countly/kafkajs/blob/master'

/**
 * kafka.js.org documents upstream kafkajs 2.2.4, not this fork, so runtime `helpUrl`s point at
 * this repository's Markdown docs instead. Callers keep passing the same slugs kafka.js.org used
 * (e.g. `docs/faq`); only the mapping to this repo's actual file names lives here.
 */
const DOC_PAGES = {
  'docs/faq': 'docs/FAQ.md',
  'docs/configuration': 'docs/Configuration.md',
  'docs/migration-guide-v2.0.0': 'docs/MigrationGuide-2-0-0.md',
  'docs/producing': 'docs/Producing.md',
  'docs/consuming': 'docs/Consuming.md',
  'docs/transactions': 'docs/Transactions.md',
  'docs/admin': 'docs/Admin.md',
  'docs/instrumentation-events': 'docs/InstrumentationEvents.md',
  'docs/retry-detailed': 'docs/RetryDetailed.md',
}

const stripLeading = (char) => (str) => (str.charAt(0) === char ? str.substring(1) : str)
const stripLeadingSlash = stripLeading('/')
const stripLeadingHash = stripLeading('#')

/**
 * Builds a help URL for a runtime error/warning message, pointing at this repository's docs.
 * @param {string} path doc slug, e.g. 'docs/faq' (leading slash optional, kept for compatibility
 *   with the kafka.js.org slugs call sites already use)
 * @param {string} [hash] anchor within the doc, with or without a leading '#'
 * @returns {string} a github.com/Countly/kafkajs blob URL. An unrecognized slug still returns a
 *   valid, browsable URL (the docs directory) with the slug appended in parentheses, so a stale
 *   call site is diagnosable from the message instead of silently linking to the repo root.
 */
module.exports = (path, hash) => {
  const slug = stripLeadingSlash(path)
  const page = DOC_PAGES[slug]
  const suffix = hash ? `#${stripLeadingHash(hash)}` : ''

  return page ? `${BASE_URL}/${page}${suffix}` : `${BASE_URL}/docs (unknown page: ${slug})`
}
