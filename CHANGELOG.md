# Changelog

All notable changes to this project are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- AG-UI 1.0 translation of the Hermes Agent `/v1/runs` event stream (run lifecycle, text, tool calls, reasoning, state).
- Connect and reconnect: a client can rejoin a running thread and receive the replayed event backlog before live events.
- Approvals surfaced as AG-UI interrupts, resumable by the next run.
- Thread store with compare-and-swap (CAS) updates, so concurrent runs on one thread cannot overwrite each other.
- FastAPI endpoint (`ag-ui-hermes[fastapi]`) with an `authorize` hook for host-side access control.
- TypeScript `HttpAgent` subpackage (`js/`) with run rejoin through `connect()`.
