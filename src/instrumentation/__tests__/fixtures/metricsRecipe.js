const fs = require('fs')
const path = require('path')
const vm = require('vm')
const diagnosticsChannel = require('node:diagnostics_channel')

const DOC = path.join(__dirname, '..', '..', '..', '..', 'docs', 'InstrumentationEvents.md')
const RECIPE_HEADING = '## <a name="metrics"></a> Metrics with prom-client'
const FENCE = '```javascript\n'

/**
 * The recipe's code, copied from the documentation page as it is written there.
 * @returns {string} the first `javascript` block under the recipe's heading
 * @throws {Error} when the page has no such heading or block
 */
const recipeSource = () => {
  const page = fs.readFileSync(DOC, 'utf8')
  const heading = page.indexOf(RECIPE_HEADING)
  const start = heading === -1 ? -1 : page.indexOf(FENCE, heading)
  if (start === -1) {
    throw new Error(`${DOC} has no javascript block under "${RECIPE_HEADING}"`)
  }

  return page.slice(start + FENCE.length, page.indexOf('\n```', start))
}

/**
 * The part of prom-client's `Counter` and `Histogram` the recipe uses, with the one check that
 * matters to it: a label that was not declared in `labelNames` throws, as the real client does.
 * Every recorded sample is `{ labels, value }`.
 * @see https://github.com/siimon/prom-client/blob/v15.1.3/index.d.ts
 * @returns {{ Counter: Function, Histogram: Function, metrics: object[] }}
 */
const createFakeClient = () => {
  const metrics = []

  class Metric {
    constructor(config) {
      this.config = config
      this.samples = []
      metrics.push(this)
    }

    record(labels, value) {
      const declared = this.config.labelNames || []
      for (const label of Object.keys(labels)) {
        if (!declared.includes(label)) {
          throw new Error(`Added label "${label}" is not included in initial labelset`)
        }
      }
      this.samples.push({ labels, value })
    }
  }

  class Counter extends Metric {
    inc(labelsOrValue, value) {
      const labelled = typeof labelsOrValue === 'object'
      this.record(labelled ? labelsOrValue : {}, labelled ? (value ?? 1) : (labelsOrValue ?? 1))
    }
  }

  class Histogram extends Metric {
    observe(labels, value) {
      this.record(labels, value)
    }
  }

  return { Counter, Histogram, metrics }
}

/**
 * Runs the recipe against a fake `prom-client`. Its subscriptions are the real ones, made through
 * `node:diagnostics_channel`, and `stop()` takes them back.
 * @returns {{ metric: (name: string) => object, metrics: object[], stop: () => void }}
 */
const runRecipe = () => {
  const client = createFakeClient()
  const subscriptions = []
  const channels = {
    subscribe: (name, onMessage) => {
      diagnosticsChannel.subscribe(name, onMessage)
      subscriptions.push([name, onMessage])
    },
  }
  const fakeRequire = (name) => {
    if (name === 'prom-client') return client
    if (name === 'node:diagnostics_channel') return channels
    throw new Error(`the recipe asked for ${name}`)
  }

  vm.compileFunction(recipeSource(), ['require'])(fakeRequire)

  return {
    metrics: client.metrics,
    metric: (name) => client.metrics.find(({ config }) => config.name === name),
    stop: () =>
      subscriptions.forEach(([name, onMessage]) => diagnosticsChannel.unsubscribe(name, onMessage)),
  }
}

module.exports = { recipeSource, createFakeClient, runRecipe }
