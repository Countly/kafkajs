const fs = require('fs')
const net = require('net')
const path = require('path')
const { createScriptedCluster } = require('../scriptedCluster')
const {
  NODES,
  answers,
  waitForFullIsr,
  BrokerBouncer,
  killBroker,
  pauseBroker,
  unpauseBroker,
} = require('../leaderElection')

/**
 * A bouncer over a recorded `docker compose`, a scripted set of answering nodes and a clock that
 * moves only when the bouncer waits.
 * @param {object} [options]
 * @param {(command: string, service: string) => void} [options.onRun] may throw for a command
 * @param {(service: string, probe: number) => boolean} [options.up] whether a node answers
 */
const harness = ({ onRun = () => {}, up = () => true } = {}) => {
  const commands = []
  const waits = []
  const isrWaits = []
  const probes = new Map()
  let clock = 1000
  const bouncer = new BrokerBouncer({
    run: (command, service) => {
      commands.push(`${command} ${service}`)
      onRun(command, service)
    },
    answers: async (node) => {
      const service = Object.keys(NODES).find((name) => NODES[name] === node)
      const probe = (probes.get(service) || 0) + 1
      probes.set(service, probe)
      clock += 1
      return up(service, probe)
    },
    waitForFullIsr: async (admin, topics) => {
      clock += 100
      isrWaits.push({ admin, topics })
    },
    now: () => clock,
    sleep: async (ms) => {
      waits.push(ms)
      clock += ms
    },
  })
  return { bouncer, commands, waits, isrWaits, probes }
}

const cluster = { admin: 'admin', topics: ['workload'] }

describe('broker bounce helpers', () => {
  describe('the compose commands', () => {
    const env = { ...process.env }
    let execFileSync

    beforeEach(() => {
      delete process.env.KAFKAJS_CLUSTER_GUARD
      delete process.env.KAFKAJS_CLUSTER_MANIFEST
    })

    afterEach(() => {
      jest.dontMock('child_process')
      process.env = { ...env }
    })

    test.each([
      ['killBroker', 'kill'],
      ['pauseBroker', 'pause'],
      ['unpauseBroker', 'unpause'],
    ])('%s runs docker compose %s on the service', (name, verb) => {
      execFileSync = jest.fn(() => Buffer.from(''))
      let helpers
      jest.isolateModules(() => {
        jest.doMock('child_process', () => ({ execFileSync }))
        helpers = require('../leaderElection')
      })

      helpers[name]('kafka2')

      expect(execFileSync.mock.calls[0][1].slice(3)).toEqual([verb, 'kafka2'])
    })

    test('are the module exports the bouncer uses', () => {
      expect([killBroker, pauseBroker, unpauseBroker].map((fn) => typeof fn)).toEqual([
        'function',
        'function',
        'function',
      ])
    })
  })

  test('the nodes are the compose services, their node ids and advertised plaintext addresses', () => {
    const compose = fs.readFileSync(path.join(__dirname, '..', '..', 'docker-compose.yml'), 'utf8')
    const services = compose.split(/^ {2}(?=kafka\d:$)/m).slice(1)
    const advertised = /KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT_HOST:\/\/([^:]+):(\d+)/
    const parsed = Object.fromEntries(
      services.map((block) => [
        block.match(/^(kafka\d):/)[1],
        {
          nodeId: Number(block.match(/KAFKA_NODE_ID: '(\d+)'/)[1]),
          host: block.match(advertised)[1],
          port: Number(block.match(advertised)[2]),
        },
      ])
    )

    expect(parsed).toEqual(NODES)
  })

  describe('answers', () => {
    test('is true for a node that negotiates ApiVersions and serves Metadata', async () => {
      const scripted = createScriptedCluster({ nodes: [{ id: 0, host: 'node0', port: 9092 }] })

      await expect(
        answers({ host: 'node0', port: 9092 }, { socketFactory: scripted.socketFactory })
      ).resolves.toBe(true)
      expect(scripted.requests().map(({ api }) => api)).toEqual(['ApiVersions', 'Metadata'])
      scripted.assertClean()
    })

    test('is false, promptly, for an address nothing listens on', async () => {
      const server = net.createServer()
      await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve))
      const { port } = server.address()
      await new Promise((resolve) => server.close(resolve))

      await expect(answers({ host: '127.0.0.1', port }, { timeout: 1000 })).resolves.toBe(false)
    })
  })

  describe('waitForFullIsr', () => {
    const admin = (answersInOrder, internal = ['__consumer_offsets']) => ({
      listTopics: jest.fn(async () => ['workload', ...internal]),
      fetchTopicMetadata: jest.fn(async () => answersInOrder.shift()),
    })
    const partition = (partitionId, isr, replicas = [1, 2, 3]) => ({ partitionId, isr, replicas })

    test('returns once every partition of the topics and the existing internal topics is in sync', async () => {
      const client = admin([
        { topics: [{ name: 'workload', partitions: [partition(0, [1, 2])] }] },
        {
          topics: [
            { name: 'workload', partitions: [partition(0, [1, 2, 3])] },
            { name: '__consumer_offsets', partitions: [partition(0, [2, 3], [2, 3])] },
          ],
        },
      ])

      await waitForFullIsr(client, ['workload'], { delayMs: 1 })

      expect(client.fetchTopicMetadata).toHaveBeenCalledTimes(2)
      expect(client.fetchTopicMetadata).toHaveBeenLastCalledWith({
        topics: ['workload', '__consumer_offsets'],
      })
    })

    test('names the partitions still short when its budget runs out', async () => {
      const short = { topics: [{ name: 'workload', partitions: [partition(4, [1])] }] }
      const client = admin(
        Array.from({ length: 1000 }, () => short),
        []
      )

      await expect(
        waitForFullIsr(client, ['workload'], { budgetMs: 20, delayMs: 5 })
      ).rejects.toThrow('ISR not full after 20 ms: workload-4')
    })
  })

  describe('BrokerBouncer', () => {
    test('refuses a step while another node is down, probing and running nothing', async () => {
      const { bouncer, commands, probes } = harness()
      bouncer.down.add('kafka3')

      await expect(bouncer.step({ service: 'kafka1', kind: 'kill' }, cluster)).rejects.toThrow(
        'Refusing a bounce step while kafka3 is down'
      )
      expect(commands).toEqual([])
      expect(probes.size).toBe(0)
    })

    test('refuses a step when a node does not answer, and runs nothing', async () => {
      const { bouncer, commands } = harness({ up: (service) => service !== 'kafka2' })

      await expect(bouncer.step({ service: 'kafka1', kind: 'pause' }, cluster)).rejects.toThrow(
        'Refusing a bounce step: kafka2 does not answer'
      )
      expect(commands).toEqual([])
    })

    test('concurrent bounce preflights cannot take two nodes down', async () => {
      const commands = []
      let release
      const holding = new Promise((resolve) => {
        release = resolve
      })
      const bouncer = new BrokerBouncer({
        run: (command, service) => commands.push(`${command} ${service}`),
        answers: async () => true,
        sleep: async () => holding,
        waitForFullIsr: async () => {},
      })

      const outcomes = Promise.allSettled([
        bouncer.step({ service: 'kafka1', kind: 'stop' }, cluster),
        bouncer.step({ service: 'kafka2', kind: 'stop' }, cluster),
      ])
      await new Promise((resolve) => setImmediate(resolve))
      const downCommands = commands.filter((command) => command.startsWith('stop '))
      release()
      const [first, second] = await outcomes

      expect(downCommands).toEqual(['stop kafka1'])
      expect(first.status).toBe('fulfilled')
      expect(second.status).toBe('rejected')
      expect(second.reason.message).toBe('Refusing a bounce step while kafka1 is down')
      expect(commands).toEqual(['stop kafka1', 'start kafka1'])
    })

    test('a pause holds the node, unpauses it and measures until its first answer', async () => {
      const { bouncer, commands, waits, isrWaits } = harness({
        up: (service, probe) => service !== 'kafka2' || probe !== 2,
      })

      const outage = await bouncer.step({ service: 'kafka2', kind: 'pause', holdMs: 2000 }, cluster)

      expect(commands).toEqual(['pause kafka2', 'unpause kafka2'])
      expect(waits).toEqual([2000, 200])
      expect(isrWaits).toEqual([])
      expect(outage).toEqual({
        service: 'kafka2',
        kind: 'pause',
        tDown: 1003,
        tRestored: 3205,
        outageMs: 2202,
      })
      expect(bouncer.down.size).toBe(0)
    })

    test('a restart starts the node and measures until its answer and the full ISR', async () => {
      const { bouncer, commands, isrWaits } = harness()

      const outage = await bouncer.step({ service: 'kafka3', kind: 'kill' }, cluster)

      expect(commands).toEqual(['kill kafka3', 'start kafka3'])
      expect(isrWaits).toEqual([cluster])
      expect(outage).toMatchObject({ tDown: 1003, tRestored: 1104, outageMs: 101 })
    })

    test('a failure while the node is down still restores it and keeps the first failure', async () => {
      const failure = new Error('hold failed')
      const { bouncer, commands } = harness()
      bouncer.wait = async () => {
        throw failure
      }

      await expect(
        bouncer.step({ service: 'kafka1', kind: 'stop', holdMs: 5 }, cluster)
      ).rejects.toBe(failure)
      expect(commands).toEqual(['stop kafka1', 'start kafka1'])
      expect(bouncer.down.size).toBe(0)
    })

    test('a failed down command still runs the restore', async () => {
      const failure = new Error('kill failed')
      const { bouncer, commands } = harness({
        onRun: (command) => {
          if (command === 'kill') throw failure
        },
      })

      await expect(bouncer.step({ service: 'kafka1', kind: 'kill' }, cluster)).rejects.toBe(failure)
      expect(commands).toEqual(['kill kafka1', 'start kafka1'])
    })

    test('a failed restore keeps the first failure and the node marked down', async () => {
      const first = new Error('hold failed')
      const { bouncer, commands } = harness({
        onRun: (command) => {
          if (command === 'unpause') throw new Error('unpause failed')
        },
      })
      bouncer.wait = async () => {
        throw first
      }

      await expect(bouncer.step({ service: 'kafka2', kind: 'pause' }, cluster)).rejects.toBe(first)
      await expect(bouncer.step({ service: 'kafka3', kind: 'stop' }, cluster)).rejects.toThrow(
        'while kafka2 is down'
      )
      expect(commands).toEqual(['pause kafka2', 'unpause kafka2'])
    })

    test('waitForReachable answers the time of the first answer, or fails at its budget', async () => {
      const { bouncer } = harness({ up: (service, probe) => probe >= 3 })

      await expect(bouncer.waitForReachable('kafka1')).resolves.toBe(1403)
      await expect(
        harness({ up: () => false }).bouncer.waitForReachable('kafka1', { budgetMs: 500 })
      ).rejects.toThrow('kafka1 did not answer within 500 ms')
    })

    test('a rolling bounce visits every node in order, `between` apart, killing when hard', async () => {
      const { bouncer, commands, waits } = harness()

      const outages = await bouncer.rollingBounce({ hard: true, between: 50, ...cluster })

      expect(commands).toEqual([
        'kill kafka1',
        'start kafka1',
        'kill kafka2',
        'start kafka2',
        'kill kafka3',
        'start kafka3',
      ])
      expect(waits).toEqual([0, 50, 0, 50, 0])
      expect(outages.map(({ service, kind }) => `${kind} ${service}`)).toEqual([
        'kill kafka1',
        'kill kafka2',
        'kill kafka3',
      ])
    })

    describe('with a signal', () => {
      const abortedWith = (reason) => {
        const controller = new AbortController()
        controller.abort(reason)
        return controller.signal
      }

      test('an already aborted signal refuses a step after its preflight, before it issues anything', async () => {
        const reason = new Error('the case ended')
        const { bouncer, commands, probes } = harness()

        await expect(
          bouncer.step(
            { service: 'kafka2', kind: 'stop' },
            { ...cluster, signal: abortedWith(reason) }
          )
        ).rejects.toBe(reason)
        expect(probes.size).toBe(3)
        expect(commands).toEqual([])
        expect(bouncer.down.size).toBe(0)
      })

      test('an already aborted signal issues no stop and probes no node in a rolling bounce', async () => {
        const reason = new Error('the case ended')
        const { bouncer, commands, probes } = harness()

        await expect(
          bouncer.rollingBounce({ hard: true, ...cluster, signal: abortedWith(reason) })
        ).rejects.toBe(reason)
        expect(commands).toEqual([])
        expect(probes.size).toBe(0)
      })

      test('an abort during a hold still restores the node, and the next step is refused', async () => {
        const reason = new Error('the case ended')
        const controller = new AbortController()
        const { bouncer, commands } = harness()
        bouncer.wait = async (ms) => {
          if (ms === 5) controller.abort(reason)
        }

        const outage = await bouncer.step(
          { service: 'kafka1', kind: 'stop', holdMs: 5 },
          { ...cluster, signal: controller.signal }
        )
        expect(outage.service).toBe('kafka1')
        expect(commands).toEqual(['stop kafka1', 'start kafka1'])
        await expect(
          bouncer.step(
            { service: 'kafka2', kind: 'stop' },
            { ...cluster, signal: controller.signal }
          )
        ).rejects.toBe(reason)
        expect(commands).toEqual(['stop kafka1', 'start kafka1'])
        expect(bouncer.down.size).toBe(0)
      })

      test('an abort during a step restores that node and issues no further step', async () => {
        const reason = new Error('the case ended')
        const controller = new AbortController()
        const { bouncer, commands } = harness()
        bouncer.wait = async () => controller.abort(reason)

        await expect(bouncer.rollingBounce({ ...cluster, signal: controller.signal })).rejects.toBe(
          reason
        )
        expect(commands).toEqual(['stop kafka1', 'start kafka1'])
        expect(bouncer.down.size).toBe(0)
      })

      test('a signal that never aborts changes nothing', async () => {
        const { bouncer, commands } = harness()

        const outages = await bouncer.rollingBounce({
          ...cluster,
          signal: new AbortController().signal,
        })

        expect(outages).toHaveLength(3)
        expect(commands).toHaveLength(6)
      })
    })

    test('a rolling bounce stops at the first failed step, after restoring that node', async () => {
      const failure = new Error('stop failed')
      const { bouncer, commands } = harness({
        onRun: (command, service) => {
          if (command === 'stop' && service === 'kafka2') throw failure
        },
      })

      await expect(bouncer.rollingBounce({ ...cluster })).rejects.toBe(failure)
      expect(commands).toEqual(['stop kafka1', 'start kafka1', 'stop kafka2', 'start kafka2'])
    })
  })
})
