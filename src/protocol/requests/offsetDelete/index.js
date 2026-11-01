/** Selects the classic OffsetDelete v0 codec.
 * @param {object} options negotiated version
 * @returns {Function} request builder with response codec
 */
const protocol = ({ version }) => {
  if (Number(version) !== 0) throw new Error('Unsupported OffsetDelete version')
  return (options) => ({
    request: require('./v0/request')(options),
    response: require('./v0/response'),
  })
}
module.exports = { versions: ['0'], protocol }
