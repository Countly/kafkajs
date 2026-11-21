const { NetemDelay } = require('../netemDelay')

/** A scripted `execFile`: logs each command and fails the ones `fail` names. */
const scripted = ({ pid = '4242', fail = () => null } = {}) => {
  const commands = []
  const run = async (file, args) => {
    const line = [file, ...args].join(' ')
    commands.push(line)
    const error = fail(line)
    if (error)
      throw Object.assign(new Error(`Command failed: ${line}\n${error}`), { stderr: error })
    return { stdout: file === 'docker' ? `${pid}\n` : '', stderr: '' }
  }
  return { commands, run }
}

const TC = 'sudo -n nsenter --target 4242 --net tc'

describe('netem delay on one broker', () => {
  it("delays only the egress of the broker's client port, inside its network namespace, and clears it", async () => {
    const { commands, run } = scripted()
    const netem = new NetemDelay({ container: 'kafka2', port: 9095, delayMs: 300, run })
    await netem.apply()
    await netem.clear()
    expect(commands).toEqual([
      'docker inspect --format {{.State.Pid}} kafka2',
      `${TC} qdisc add dev eth0 root handle 1: prio bands 3 priomap 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1`,
      `${TC} qdisc add dev eth0 parent 1:3 handle 30: netem delay 300ms`,
      `${TC} filter add dev eth0 parent 1: protocol ip prio 1 u32 match ip sport 9095 0xffff flowid 1:3`,
      'docker inspect --format {{.State.Pid}} kafka2',
      `${TC} qdisc del dev eth0 root`,
    ])
  })

  it('checks the mechanism with a real rule that it removes again', async () => {
    const { commands, run } = scripted()
    await new NetemDelay({ container: 'kafka2', port: 9095, delayMs: 300, run }).check()
    expect(commands.filter((c) => c.includes(' tc '))).toEqual([
      `${TC} qdisc add dev eth0 root handle 1: prio bands 3 priomap 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1`,
      `${TC} qdisc add dev eth0 parent 1:3 handle 30: netem delay 1ms`,
      `${TC} filter add dev eth0 parent 1: protocol ip prio 1 u32 match ip sport 9095 0xffff flowid 1:3`,
      `${TC} qdisc del dev eth0 root`,
    ])
  })

  it('fails the check loudly, naming what is missing, and removes a half-applied rule', async () => {
    const { commands, run } = scripted({
      fail: (line) =>
        line.includes('netem delay') ? 'Error: Specified qdisc kind is unknown.' : null,
    })
    const check = new NetemDelay({ container: 'kafka2', port: 9095, delayMs: 300, run }).check()
    await expect(check).rejects.toThrow(
      /^slow-broker needs sudo, nsenter, tc and the netem qdisc on the runner to delay kafka2: Command failed: .*\nError: Specified qdisc kind is unknown\.$/
    )
    expect(commands[commands.length - 1]).toBe(`${TC} qdisc del dev eth0 root`)
  })

  it('fails the check when the broker container is not running', async () => {
    const { run } = scripted({ pid: '0' })
    await expect(
      new NetemDelay({ container: 'kafka2', port: 9095, delayMs: 300, run }).check()
    ).rejects.toThrow(
      'slow-broker needs sudo, nsenter, tc and the netem qdisc on the runner to delay kafka2: kafka2 is not running (pid 0)'
    )
  })
})
