const { assign } = Object
const { sharedSetup } = require('../admin/deadline')

const LEVELS = {
  NOTHING: 0,
  ERROR: 1,
  WARN: 2,
  INFO: 4,
  DEBUG: 5,
}

/**
 * Builds one level's log function. The user's sink is invoked synchronously, outside any admin
 * call's scope, so work it starts owns its own deadline; a synchronous throw from it is swallowed
 * and a rejection of a promise it returns is discarded, so a failing sink never decides the
 * outcome of the client code that logged, nor ends the process.
 * @param {string} label
 * @param {number} level
 * @param {() => number} currentLevel
 * @param {string} [namespace]
 * @param {(entry: object) => void} logFunction the user's sink
 * @returns {(message: string, extra?: object) => void}
 */
const createLevel =
  (label, level, currentLevel, namespace, logFunction) =>
  (message, extra = {}) => {
    if (level > currentLevel()) return
    const entry = {
      namespace,
      level,
      label,
      log: assign(
        {
          timestamp: new Date().toISOString(),
          logger: 'kafkajs',
          message,
        },
        extra
      ),
    }
    try {
      sharedSetup(() => {
        const result = logFunction(entry)
        if (typeof result?.then === 'function') result.then(undefined, () => {})
      })
    } catch {}
  }

const evaluateLogLevel = (logLevel) => {
  const envLogLevel = (process.env.KAFKAJS_LOG_LEVEL || '').toUpperCase()
  return LEVELS[envLogLevel] == null ? logLevel : LEVELS[envLogLevel]
}

const createLogger = ({ level = LEVELS.INFO, logCreator } = {}) => {
  let logLevel = evaluateLogLevel(level)
  const logFunction = logCreator(logLevel)

  const createNamespace = (namespace, logLevel = null) => {
    const namespaceLogLevel = evaluateLogLevel(logLevel)
    return createLogFunctions(namespace, namespaceLogLevel)
  }

  const createLogFunctions = (namespace, namespaceLogLevel = null) => {
    const currentLogLevel = () => (namespaceLogLevel == null ? logLevel : namespaceLogLevel)
    const logger = {
      info: createLevel('INFO', LEVELS.INFO, currentLogLevel, namespace, logFunction),
      error: createLevel('ERROR', LEVELS.ERROR, currentLogLevel, namespace, logFunction),
      warn: createLevel('WARN', LEVELS.WARN, currentLogLevel, namespace, logFunction),
      debug: createLevel('DEBUG', LEVELS.DEBUG, currentLogLevel, namespace, logFunction),
      /**
       * Whether `debug()` would emit at this logger's effective level, so a caller can skip
       * building an expensive payload the level check would discard anyway. Internal: it is
       * deliberately absent from the public `Logger` type, because users supply a log *creator*
       * and never a logger object of their own.
       * @returns {boolean}
       */
      debugEnabled: () => LEVELS.DEBUG <= currentLogLevel(),
    }

    return assign(logger, {
      namespace: createNamespace,
      setLogLevel: (newLevel) => {
        logLevel = newLevel
      },
    })
  }

  return createLogFunctions()
}

module.exports = {
  LEVELS,
  createLogger,
}
