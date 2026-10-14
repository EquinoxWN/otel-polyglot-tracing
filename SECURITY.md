# Security policy

## Reporting a vulnerability

Please report security problems privately through GitHub:
**Security > Report a vulnerability** on this repository
(`https://github.com/EquinoxWN/otel-polyglot-tracing/security/advisories/new`). Do not open a public issue.

Include what you found, how to reproduce it, and the impact you expect. You will get an answer
within 7 days. Fixes are released as soon as they are ready, and you are credited unless you ask
not to be.

## Supported versions

This project is pre-1.0. Only the latest commit on `main` receives fixes.

## Scope

- **In scope:** crashes or unbounded memory from crafted requests, queue messages, trace headers or
  OTLP bodies; trace context from one request or message ending up in another; path or query
  injection through ids; services listening beyond 127.0.0.1 by default.
- **Out of scope:** the demo has no authentication on purpose (it is a local learning system);
  latency differences between machines.

## How this repository protects itself

- Every GitHub Action is pinned to a full commit SHA, and workflows run with read-only
  permissions and without persisted credentials.
- Dependabot update pull requests are paused (`open-pull-requests-limit: 0`) so the repository keeps a single `main` branch; dependencies are updated by hand and checked by the CI audit.
- CI runs lint, unit tests, the end-to-end test and a known-vulnerability check (govulncheck,
  pip-audit, npm audit) on every push and pull request.
