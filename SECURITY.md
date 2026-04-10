# Security Policy

## Supported versions

Only the **latest released version** on the `main` branch is supported with
security fixes. Older tagged releases are frozen and will not receive backports.

| Version        | Supported |
|----------------|-----------|
| Latest release | ✅        |
| Anything else  | ❌        |

## Reporting a vulnerability

If you believe you've found a security issue in Shelly Energy Analyzer — for
example a way to read `config.json`, exfiltrate a Telegram token, bypass the
optional login token, inject data into the SQLite database, execute arbitrary
code via the updater flow, or anything else that could compromise a user's
data or home network — **please do not open a public GitHub issue**.

Report it privately via GitHub's **[Security Advisories → Report a
vulnerability](https://github.com/robeertm/shelly-energy-analyzer/security/advisories/new)**
form. You will get an initial response within a few days.

Please include:

- A clear description of the issue and the affected version
- Steps to reproduce or proof-of-concept code
- The potential impact (what an attacker could do)
- Any suggested fix or mitigation, if you have one

## Scope

In scope:
- The Flask web app, REST API, `/api/updates/install` flow, authentication
  bypass, persisted storage of secrets (SSL keys, API tokens, Telegram tokens).
- The `updater_helper.py` file-replacement path.
- Any issue where network-adjacent attackers could read or influence another
  LAN user's data without authorisation.

Out of scope:
- Denial of service by overloading the SQLite database with synthetic samples.
- Issues that require the attacker to already be logged in as the owner of
  the running process (by definition they already own everything).
- Vulnerabilities in third-party dependencies that have already been
  disclosed upstream — please report those to the upstream project.
- Self-signed TLS certificate warnings in browsers (that is the default
  "auto" mode; users who need a trusted cert should switch to "custom").

## Disclosure

After a fix is released we will publish a GitHub Security Advisory and credit
the reporter (unless they wish to remain anonymous). There is no bug bounty.
