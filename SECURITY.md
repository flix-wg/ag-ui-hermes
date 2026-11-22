# Security policy

## Reporting a vulnerability

Report vulnerabilities through [GitHub Security Advisories](https://github.com/flix-wg/ag-ui-hermes/security/advisories/new)
or contact the maintainer privately. Please keep vulnerability reports out of public issues.

## Scope

ag-ui-hermes does not authenticate or authorize callers. Authorization is the host application's job: wire it through the endpoint's `authorize` hook and do not expose the endpoint without it. Keep the Hermes API key server-side. See [docs/security.md](docs/security.md) for the threat model and what to configure.
