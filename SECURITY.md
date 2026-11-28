# Security policy

If you discover a security issue in `@countly/kafkajs`, please report it privately by email to security@count.ly, not by opening a public GitHub issue, pull request or discussion.

# No bug bounty

Countly does not run a bug bounty programme. We do not offer, promise or negotiate payment, rewards or swag for security reports, and a report does not create any obligation to pay. Please do not send reports in expectation of a reward, and do not attach payment details.

Reports are still very welcome, and they are part of how we keep this client secure.

# What happens to a report

- **Every report is evaluated.** Each report that reaches security@count.ly is recorded and assessed, and we act on what we confirm: valid issues are fixed and released according to their severity.
- **Replies are not guaranteed.** We cannot promise to acknowledge or answer every report, to give status updates, or to discuss our assessment. Not hearing back does not mean a report was not read.
- **Fixes ship in regular releases.** Security fixes are released through the normal release process and noted in the changelog. We do not commit to a timeline for any single report.
- **Duplicates.** Once a root cause is known to us, further reports of it are recorded against the original assessment.

# How to report

A report we can act on quickly includes:

- the package version, or the commit you tested;
- the Node.js version and the Kafka broker version;
- the client configuration involved (connection, SASL, TLS, producer, consumer or admin options), with every password, key and token removed;
- clear steps to reproduce against a broker you control, and a working proof of concept;
- what an attacker needs beforehand (for example a malicious broker, a network position, or control of a topic's records) and what they gain.

Please send one root cause per report, in plain text or Markdown. Reports produced by scanners or AI tools without a verified, working reproduction will be given low priority.

# Testing and disclosure

- Test only against Kafka brokers and services you control. Do not test against anyone else's brokers, services or data.
- Do not run tests that degrade service for anyone else.
- Keep the details private until a fix has been released, and give us reasonable time to release one.
- We will not pursue legal action against good-faith research that follows this policy.

# Supported versions

Security fixes are made on the current `3.x` release line only. Older releases may not receive fixes, so please test against the latest release or the `master` branch.
