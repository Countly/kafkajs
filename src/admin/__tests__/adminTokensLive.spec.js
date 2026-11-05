const { inspect } = require('util')
const { timingSafeEqual } = require('crypto')
const { AdminLiveProbe } = require('./fixtures/liveAdmin')
const { saslBrokers, saslConnectionOpts } = require('testHelpers')
const { liveCase, caseTimeout } = require('../../producer/__tests__/fixtures/bounded')

const suite = process.env.ADMIN_TOKENS_E2E ? describe : describe.skip
const budget = 120000
/**
 * The fixture brokers keep the default 24 hour token expiry. A new token expires at the earlier of
 * its maximum lifetime and 24 hours from issue; a renewal, at the earlier of the maximum lifetime
 * and the renew period (at most 24 hours) from now. A lifetime beyond 24 hours and a 24 hour
 * period let a renewal made after time has passed move the expiry further.
 */
const LIFETIME_MS = '172800000'
const RENEW_PERIOD_MS = '86400000'

/** @param {Error} error public error @returns {Error} deepest typed protocol cause */
const causeOf = (error) => {
  while (error?.cause) error = error.cause
  return error
}

suite('admin delegation tokens', () => {
  test(
    'creates renews and expires delegation tokens without diagnostic secrets',
    liveCase(budget, async (signal) => {
      const probe = new AdminLiveProbe({ stem: 'admin-tokens', signal, budgetMs: budget })
      await probe.finish(async () => {
        let configuredCount = 0
        for (const service of ['kafka1', 'kafka2', 'kafka3']) {
          const configured = await probe.host(service, [
            'sh',
            '-c',
            'if test -n "$KAFKA_DELEGATION_TOKEN_SECRET_KEY"; then printf configured; else printf disabled; fi',
          ])
          expect(['configured', 'disabled']).toContain(configured)
          if (configured === 'configured') configuredCount++
        }
        const { ssl, sasl } = saslConnectionOpts()
        const previousBuffers = process.env.KAFKAJS_DEBUG_PROTOCOL_BUFFERS
        const restoreBuffers = () => {
          if (previousBuffers === undefined) delete process.env.KAFKAJS_DEBUG_PROTOCOL_BUFFERS
          else process.env.KAFKAJS_DEBUG_PROTOCOL_BUFFERS = previousBuffers
        }
        probe.effects.add('restore diagnostic buffers', restoreBuffers)
        for (const buffers of ['0', '1']) {
          process.env.KAFKAJS_DEBUG_PROTOCOL_BUFFERS = buffers
          const admin = await probe.start({ brokers: saslBrokers(), ssl, sasl })
          const renewer = { principalType: 'User', principalName: 'test' }
          const created = await probe
            .invoke(() =>
              admin.createDelegationToken({ renewers: [renewer], maxLifetimeMs: LIFETIME_MS })
            )
            .catch((error) => {
              if (configuredCount !== 3) {
                const refusal = causeOf(error)
                expect(refusal).toMatchObject({
                  name: 'KafkaJSProtocolError',
                  code: 61,
                  type: 'DELEGATION_TOKEN_AUTH_DISABLED',
                })
                probe.record('disabled token fixture broker refusal', {
                  configuredCount,
                  refusalCode: refusal.code,
                })
              }
              throw error
            })
          expect(typeof created.tokenId).toBe('string')
          expect(created.tokenId.length).toBeGreaterThan(0)
          expect(Buffer.isBuffer(created.hmac)).toBe(true)
          expect(created.hmac.length).toBeGreaterThan(0)
          probe.credentials.push(created.hmac)
          probe.effects.add('expire token', async () => {
            try {
              await admin.expireDelegationToken({ hmac: created.hmac, expiryTimePeriodMs: '0' })
            } catch (error) {
              if (![62, 66].includes(causeOf(error)?.code)) throw error
            }
          })
          expect(configuredCount).toBe(3)
          const version = probe.requests
            .filter(({ apiName }) => apiName === 'CreateDelegationToken')
            .at(-1).apiVersion
          if (version >= 3) {
            const owner = {
              principalType: 'User',
              principalName: `e2e-token-owner-${created.tokenId}`,
            }
            const owned = await probe.invoke(() =>
              admin.createDelegationToken({ owner, renewers: [renewer], maxLifetimeMs: '60000' })
            )
            probe.credentials.push(owned.hmac)
            probe.effects.add('expire distinct owner token', () =>
              admin.expireDelegationToken({ hmac: owned.hmac, expiryTimePeriodMs: '0' })
            )
            expect(owned.owner).toEqual(owner)
            expect(owned.owner).not.toEqual(created.owner)
            const tokens = await probe.invoke(() =>
              admin.describeDelegationToken({ owners: [owner] })
            )
            probe.credentials.push(...tokens.map(({ hmac }) => hmac))
            expect(tokens.map(({ tokenId }) => tokenId)).toContain(owned.tokenId)
          } else {
            await expect(
              probe.invoke(() => admin.createDelegationToken({ owner: renewer }))
            ).rejects.toMatchObject({ name: 'KafkaJSServerDoesNotSupportApiKey' })
          }
          expect(BigInt(created.issueTimestampMs)).toBeLessThanOrEqual(
            BigInt(created.expiryTimestampMs)
          )
          expect(BigInt(created.expiryTimestampMs)).toBeLessThanOrEqual(
            BigInt(created.maxTimestampMs)
          )
          const listed = await probe.invoke(() =>
            admin.describeDelegationToken({ owners: [created.owner] })
          )
          const described = listed.find(({ tokenId }) => tokenId === created.tokenId)
          expect(Boolean(described)).toBe(true)
          const equal =
            Buffer.isBuffer(described?.hmac) &&
            described.hmac.length === created.hmac.length &&
            timingSafeEqual(described.hmac, created.hmac)
          expect(equal).toBe(true)
          probe.credentials.push(...listed.map(({ hmac }) => hmac))
          const count = probe.requests.length
          expect(await probe.invoke(() => admin.describeDelegationToken({ owners: [] }))).toEqual(
            []
          )
          expect(probe.requests).toHaveLength(count)
          await probe.poll(
            async () => BigInt(Date.now()) > BigInt(created.issueTimestampMs) + 1000n
          )
          const renewed = await probe.invoke(() =>
            admin.renewDelegationToken({ hmac: created.hmac, renewPeriodMs: RENEW_PERIOD_MS })
          )
          expect(BigInt(renewed.expiryTimestampMs)).toBeGreaterThan(
            BigInt(created.expiryTimestampMs)
          )
          expect(BigInt(renewed.expiryTimestampMs)).toBeLessThanOrEqual(
            BigInt(created.maxTimestampMs)
          )
          const afterRenewal = (
            await probe.invoke(() => admin.describeDelegationToken({ owners: [created.owner] }))
          ).filter(({ tokenId }) => tokenId === created.tokenId)
          probe.credentials.push(...afterRenewal.map(({ hmac }) => hmac))
          expect(afterRenewal).toHaveLength(1)
          expect(BigInt(afterRenewal[0].expiryTimestampMs)).toBe(BigInt(renewed.expiryTimestampMs))
          await probe.invoke(() =>
            admin.expireDelegationToken({ hmac: created.hmac, expiryTimePeriodMs: '0' })
          )
          await probe.poll(async () => {
            const tokens = await probe.invoke(() =>
              admin.describeDelegationToken({ owners: [created.owner] })
            )
            probe.credentials.push(...tokens.map(({ hmac }) => hmac))
            return !tokens.some(
              ({ tokenId, expiryTimestampMs }) =>
                tokenId === created.tokenId && BigInt(expiryTimestampMs) > BigInt(Date.now())
            )
          })
          const expired = await probe
            .invoke(() => admin.renewDelegationToken({ hmac: created.hmac, renewPeriodMs: '1000' }))
            .then(() => null, causeOf)
          expect([62, 66].includes(expired?.code)).toBe(true)
          const plain = await probe.start()
          const refused = await probe
            .invoke(() => plain.createDelegationToken({ renewers: [renewer] }))
            .then(() => null, causeOf)
          expect(refused?.code).toBe(64)
          const diagnostics = inspect(
            { logs: probe.logs, requests: probe.requests, errors: [expired, refused] },
            { depth: null, maxArrayLength: null }
          )
          const leaked = [
            ...probe.credentials
              .filter(Buffer.isBuffer)
              .flatMap((credential) =>
                ['hex', 'base64', 'utf8'].map((encoding) => credential.toString(encoding))
              ),
            sasl.password,
          ]
            .filter(Boolean)
            .some((marker) => diagnostics.includes(marker))
          expect(leaked).toBe(false)
          probe.record('token lifecycle and plaintext refusal', {
            tokenId: created.tokenId,
            diagnosticModeId: buffers,
            tokenVersion: version,
            secretAbsent: true,
            issueOffset: created.issueTimestampMs,
            expiryOffset: created.expiryTimestampMs,
            plaintextCode: refused.code,
            expiredCode: expired.code,
          })
        }
        restoreBuffers()
      })
    }),
    caseTimeout(budget)
  )
})
