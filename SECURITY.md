# Security policy

## Reporting a vulnerability

Please report vulnerabilities privately to security@example.invalid (placeholder, to be replaced before release). Do not open a public issue. Expect an acknowledgement within a few days.

## Scope

ag-ui-hermes does not authenticate or authorize callers. Authorization is the host application's job: wire it through the endpoint's `authorize` hook and do not expose the endpoint without it. Keep the Hermes API key server-side. See [docs/security.md](docs/security.md) for the threat model and what to configure.
