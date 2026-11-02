/** Verifies semantic KDC evidence after the shared stamp verifier and before teardown.
 * @param {object[]} records parsed evidence lines
 * @returns {void} every independent live oracle matches
 * @throws {Error} absent, duplicate, incomplete or incorrect oracle
 */
module.exports = (records) => {
  const rows = records.filter((record) => record.type === 'gssapi-oracle')
  if (rows.length !== 1) throw new Error('GSSAPI evidence requires exactly one oracle')
  const row = rows[0]
  const integer = (key) => Number.isSafeInteger(row[key]) && row[key] >= 0
  const corpus = Array.from({ length: 20 }, (_, index) => `record-${index}`)
  const same = (left, right) => JSON.stringify(left) === JSON.stringify(right)
  const valid =
    row.service === 'kafka@localhost' &&
    row.principal === 'client@KAFKAJS.TEST' &&
    row.layer === 1 &&
    row.maxBuffer === 0 &&
    [
      'firstContexts',
      'reconnected',
      'reauthenticated',
      'issuedAtMs',
      'expiresAtMs',
      'observedAtMs',
      'klistStatus',
      'socketsBeforeReauth',
      'socketsAfterReauth',
    ].every(integer) &&
    row.firstContexts > 0 &&
    row.reconnected > row.firstContexts &&
    row.reauthenticated > row.reconnected &&
    row.socketsBeforeReauth > 0 &&
    row.socketsAfterReauth === row.socketsBeforeReauth &&
    row.expiresAtMs - row.issuedAtMs === 60000 &&
    row.observedAtMs >= row.expiresAtMs &&
    row.klistStatus !== 0 &&
    row.committedOffset === '20' &&
    same(row.corpus, corpus) &&
    same(row.delivered, corpus) &&
    Array.isArray(row.rawLog) &&
    row.rawLog.length === 20 &&
    row.rawLog.every(
      (record, index) => record.offset === String(index) && record.value === corpus[index]
    ) &&
    row.wireVersions?.SaslHandshake === 1 &&
    row.wireVersions?.SaslAuthenticate === 2
  if (!valid) throw new Error('GSSAPI oracle mismatch')
}
