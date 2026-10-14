# Contributing

Thanks for helping. This is a Go, Python and TypeScript (Node.js) project; issues and pull requests are welcome.

## Set up and check your change

Needs Go 1.26+, Python 3.11+ and Node.js 24+. Caches and virtual environments stay in git-ignored folders inside the repo. No Docker is needed.

```bash
python -m venv .venv && source .venv/bin/activate
make setup   # install dependencies
make lint    # formatting, static checks, strict types
make test    # unit tests in all three languages
make e2e     # the three services as processes, one trace checked end to end
make audit   # known-vulnerability check (as in CI)
```

A pull request is ready when `make lint`, `make test` and `make e2e` pass and CI is green.

## Guidelines

- Keep each change focused; explain the problem it solves in the pull request.
- Add or update a test for every behaviour change, and update `docs/results/` when numbers change.
- A change to the queue table changes `spec/queue.sql` and both embedded copies (the tests check).
- Update the OpenTelemetry API, SDK and instrumentation packages of one language together.
- Significant design changes need an ADR in `docs/adr/` (copy `0001` for the format).
- Keep function comments to one short phrase; let names and tests explain the rest.
- Report security problems privately, as described in [SECURITY.md](SECURITY.md).

By contributing you agree that your work is released under the [MIT License](LICENSE).
