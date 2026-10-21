'use strict'

const fs = require('fs')
const os = require('os')
const path = require('path')
const { requests } = require('../../../src/protocol/requests')
const { compute, render, pinnedJar, checkProvenance, writeProvenance } = require('../provenance')

const COMMITTED = path.join(__dirname, '..', 'provenance.json')
const SHA256 = /^[0-9a-f]{64}$/

describe('scripts/protocol > provenance', () => {
  let directory
  const copyOfCommitted = (change) => {
    const provenance = JSON.parse(fs.readFileSync(COMMITTED, 'utf8'))
    change(provenance)
    const file = path.join(directory, 'provenance.json')
    fs.writeFileSync(file, render(provenance))
    return file
  }
  const failuresFor = (file) => {
    const failures = []
    checkProvenance(failures, undefined, file)
    return failures
  }

  beforeEach(() => {
    directory = fs.mkdtempSync(path.join(os.tmpdir(), 'provenance-'))
  })

  afterEach(() => {
    fs.rmSync(directory, { recursive: true, force: true })
  })

  describe('what is recorded', () => {
    const provenance = compute()

    test('one entry per implemented API, by its Apache message name', () => {
      const implemented = Object.entries(requests).filter(([, { versions }]) => versions.length > 0)

      expect(Object.keys(provenance)).toHaveLength(implemented.length)
      expect(Object.keys(provenance)).toContain('FindCoordinator')
      expect(Object.keys(provenance)).not.toContain('GroupCoordinator')
      expect(Object.keys(provenance)).toEqual([...Object.keys(provenance)].sort())
    })

    test('the Apache tag of the snapshot and the SHA-256 of both message definitions', () => {
      for (const entry of Object.values(provenance)) {
        expect(entry.kafkaTag).toBe('4.3.1')
        expect(entry.messageSha256.Request).toMatch(SHA256)
        expect(entry.messageSha256.Response).toMatch(SHA256)
      }
    })

    test('the jar of the Java vectors for an API that has some, and none for one that has none', () => {
      expect(provenance.Metadata.javaClientsJar).toBe('4.3.1')
      expect(provenance.CreateTopics.javaClientsJar).toBe('4.3.1')
      expect(provenance.SaslHandshake.javaClientsJar).toBeNull()
    })

    test('generate writes the text the check compares with', () => {
      const file = path.join(directory, 'written.json')

      expect(writeProvenance(file)).toBe('scripts/protocol/provenance.json')
      expect(fs.readFileSync(file, 'utf8')).toBe(render(provenance))
    })

    test('the committed file is what generate writes', () => {
      expect(fs.readFileSync(COMMITTED, 'utf8')).toBe(render(provenance))
      expect(failuresFor(COMMITTED)).toEqual([])
    })
  })

  describe('the check', () => {
    test('fails for an API whose message definition changed since the file was written', () => {
      const file = copyOfCommitted((p) => {
        p.Heartbeat.messageSha256.Response = '0'.repeat(64)
      })

      expect(failuresFor(file)).toEqual([
        'P2 scripts/protocol/provenance.json differs for Heartbeat - run `npm run protocol:generate`',
      ])
    })

    test('fails for an API that is missing from the file and for one that is no longer implemented', () => {
      const file = copyOfCommitted((p) => {
        delete p.Heartbeat
        p.Imaginary = p.Produce
      })

      expect(failuresFor(file)).toEqual([
        'P2 scripts/protocol/provenance.json differs for Heartbeat - run `npm run protocol:generate`',
        'P2 scripts/protocol/provenance.json differs for Imaginary - run `npm run protocol:generate`',
      ])
    })

    test('fails when the jar recorded for an API is not the one the generators download', () => {
      const file = copyOfCommitted((p) => {
        p.Metadata.javaClientsJar = '4.2.2'
      })

      expect(failuresFor(file)).toHaveLength(1)
    })

    test('fails when the file is missing, and when its text is not the text generate writes', () => {
      expect(failuresFor(path.join(directory, 'absent.json'))).toEqual([
        'P2 scripts/protocol/provenance.json is missing - run `npm run protocol:generate`',
      ])
      const file = path.join(directory, 'compact.json')
      fs.writeFileSync(file, JSON.stringify(JSON.parse(fs.readFileSync(COMMITTED, 'utf8'))))

      expect(failuresFor(file)).toEqual([
        'P2 scripts/protocol/provenance.json is not in the form `npm run protocol:generate` writes',
      ])
    })
  })

  describe('the jar the Java generators pin', () => {
    const writeScripts = (pins) => {
      const folder = path.join(directory, 'scripts', 'protocol', 'javaVectors')
      fs.mkdirSync(folder, { recursive: true })
      for (const [name, pin] of Object.entries(pins)) {
        fs.writeFileSync(
          path.join(folder, name),
          `KAFKA_CLIENTS_VERSION="\${KAFKA_CLIENTS_VERSION:-${pin}}"\n`
        )
      }
      return directory
    }

    test('is the default both scripts agree on', () => {
      expect(pinnedJar(writeScripts({ 'run.sh': '4.3.1', 'runAssignors.sh': '4.3.1' }))).toBe(
        '4.3.1'
      )
      expect(pinnedJar()).toBe('4.3.1')
    })

    test('is an error when the two scripts pin different versions', () => {
      expect(() =>
        pinnedJar(writeScripts({ 'run.sh': '4.3.1', 'runAssignors.sh': '4.2.2' }))
      ).toThrow('the Java generators pin different kafka-clients versions: 4.3.1, 4.2.2')
    })

    test('is an error when a script pins none', () => {
      const root = writeScripts({ 'run.sh': '4.3.1' })
      fs.writeFileSync(
        path.join(root, 'scripts', 'protocol', 'javaVectors', 'runAssignors.sh'),
        'echo\n'
      )

      expect(() => pinnedJar(root)).toThrow('runAssignors.sh pins no kafka-clients version')
    })
  })
})
