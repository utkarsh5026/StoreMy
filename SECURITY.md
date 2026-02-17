# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| `main`  | :white_check_mark: |

## Reporting a Vulnerability

**Please do not report security vulnerabilities through public GitHub issues.**

Instead, open a [private security advisory](https://github.com/utkarsh5026/StoreMy/security/advisories/new)
on GitHub. This keeps the details confidential until a fix is available.

When reporting, include:

- A clear description of the vulnerability
- Steps to reproduce the issue
- The potential impact (what an attacker could achieve)
- Any suggested fix or mitigation, if known

### What to Expect

| Timeline | Action |
| -------- | ------ |
| 72 hours | Acknowledgment of your report |
| 7 days   | Initial assessment and severity classification |
| 14 days  | Patch for confirmed critical/high issues |
| 90 days  | Public disclosure (coordinated with reporter) |

We appreciate responsible disclosure. Reporters of confirmed vulnerabilities
will be credited in the changelog unless they prefer to remain anonymous.

## Scope

The following are **in scope**:

- SQL injection or parsing vulnerabilities that bypass intended query restrictions
- Buffer overflows or memory corruption in the storage engine
- Race conditions in the transaction/concurrency layer leading to data corruption
- Authentication or authorization bypasses in any future network interface

The following are **out of scope**:

- Denial of service through extremely large inputs (StoreMy is a local embedded database, not a network service)
- Issues in third-party dependencies (report these to the dependency maintainers directly)
- Vulnerabilities in the example or benchmark code under `pkg/examples/`
