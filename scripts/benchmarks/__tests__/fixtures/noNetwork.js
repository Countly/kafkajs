/**
 * Preloaded into a benchmark that a spec runs only to see it refuse its arguments: any socket it
 * opens ends the process at once with status 97, so a run that should have been refused never
 * reaches a broker on this host.
 */
const net = require('net')

net.Socket.prototype.connect = function connect() {
  process.stderr.write('a run that should have been refused tried to connect\n')
  process.exit(97)
}
