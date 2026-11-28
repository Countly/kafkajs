const { newLogger } = require('testHelpers')
const Cluster = require('../../../cluster')

/**
 * Routes through the real cluster and broker pool to an already connected scripted coordinator.
 * @param {object} coordinator broker id and scripted description methods
 * @param {Function} findGroupCoordinator scripted coordinator discovery
 * @returns {Cluster} cluster using the production coordinator dispatch path
 */
const descriptionCluster = (coordinator, findGroupCoordinator) => {
  const cluster = new Cluster({ brokers: ['127.0.0.1:9092'], logger: newLogger() })
  cluster.brokerPool.brokers[coordinator.nodeId] = { ...coordinator, isConnected: () => true }
  cluster.findGroupCoordinator = findGroupCoordinator
  return cluster
}

module.exports = descriptionCluster
