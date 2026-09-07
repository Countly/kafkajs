const roundRobin = require('./roundRobinAssigner')
const range = require('./rangeAssigner')
const sticky = require('./stickyAssigner')
const cooperativeSticky = require('./cooperativeStickyAssigner')

module.exports = {
  roundRobin,
  range,
  sticky,
  cooperativeSticky,
}
