# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| 0.1.x   | Yes       |

## Reporting a Vulnerability

**Please do NOT open a public GitHub issue for security vulnerabilities.**

Instead, report vulnerabilities by emailing **dmytro.chystiakov@gmail.com** with:

1. Description of the vulnerability
2. Steps to reproduce
3. Impact assessment
4. Suggested fix (if any)

You should receive a response within 48 hours. We will work with you to understand
the issue and coordinate a fix and disclosure timeline.

## Scope

The following are in scope:

- SQL injection or query escaping issues
- Authentication / authorization bypass
- Data exposure or leakage
- Denial of service via crafted queries or network input
- Memory safety issues (buffer overflow, use-after-free, etc.)
- Cryptographic weaknesses in encryption-at-rest or TLS

## Disclosure Policy

- We will acknowledge receipt within 48 hours
- We will confirm the vulnerability and determine its impact within 7 days
- We will release a fix within 30 days of confirmation
- We will credit reporters in the release notes (unless they prefer anonymity)
