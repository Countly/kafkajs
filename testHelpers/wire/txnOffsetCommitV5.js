const txnOffsetCommitV4 = require('./txnOffsetCommitV4')

/**
 * TxnOffsetCommit v5, the version a transaction under transaction version 2 sends (KIP-890 part
 * 2). The message JSON adds nothing to v4's layout, so the pair is v4's under its own version.
 *
 * Evidence: Java's structured vectors `txnOffsetCommitRequest-v5-{groupMetadata,legacy}.json` and
 * `txnOffsetCommitResponse-v5-{ok,error}.json` (`hex` and `fields`), whose bytes equal v4's.
 * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/TxnOffsetCommitRequest.json
 */
module.exports = { ...txnOffsetCommitV4, version: 5 }
