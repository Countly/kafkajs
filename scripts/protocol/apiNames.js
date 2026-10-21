'use strict'

/**
 * The names one API goes by. The Apache message is `FindCoordinatorRequest`; this client calls the
 * same API `GroupCoordinator` in `src/protocol/requests/apiKeys.js` and `requests/index.js`; its
 * codecs sit in `src/protocol/requests/findCoordinator/`. Every other API has one name in all
 * three places. This module is the one table that maps between them.
 *
 * Zero dependencies, CommonJS, Node >= 22.
 */

/** The `apiKeys.js` names that differ from the Apache message name, by `apiKeys.js` name. */
const MESSAGE_OF_KEY = { GroupCoordinator: 'FindCoordinator' }

/** @param {string} key a name in `apiKeys.js` @returns {string} the Apache message name */
const messageOfKey = (key) => MESSAGE_OF_KEY[key] || key

/** @param {string} message an Apache message name @returns {string} its name in `apiKeys.js` */
const keyOfMessage = (message) =>
  Object.keys(MESSAGE_OF_KEY).find((key) => MESSAGE_OF_KEY[key] === message) || message

/** @param {string} message an Apache message name @returns {string} its directory under `requests/` */
const directoryOfMessage = (message) => message.charAt(0).toLowerCase() + message.slice(1)

/** @param {string} directory a directory under `requests/` @returns {string} its Apache message name */
const messageOfDirectory = (directory) => directory.charAt(0).toUpperCase() + directory.slice(1)

module.exports = { messageOfKey, keyOfMessage, directoryOfMessage, messageOfDirectory }
