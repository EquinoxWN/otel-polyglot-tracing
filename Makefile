# PY is the Python with the dev extras installed (the repo's .venv locally, setup-python's in CI).
# RACE is "-race" in CI (Linux); the Go race detector needs cgo, which this Windows machine lacks.
PY ?= python
RACE ?=

.PHONY: setup lint test e2e audit ci

setup:
	cd python && $(PY) -m pip install --upgrade pip && $(PY) -m pip install -e ".[dev]"
	cd js && npm ci
	cd go && go mod download

# gofmt + go vet; ruff, ruff format and mypy --strict; tsc --strict.
lint:
	@cd go && files="$$(gofmt -l .)" && test -z "$$files" || (echo "gofmt needed: $$files"; exit 1)
	cd go && go vet ./... && go vet -tags e2e ./e2e
	cd python && $(PY) -m ruff check . && $(PY) -m ruff format --check . && $(PY) -m mypy
	cd js && npm run -s lint

# Unit tests in each language.
test:
	cd go && go test $(RACE) -count=1 ./...
	cd python && $(PY) -m pytest -q
	cd js && npm test

# The three services as local processes on 127.0.0.1, exporting to an in-process OTLP receiver.
e2e:
	cd js && npm run -s build
	cd go && PYTHON="$(PY)" go test -tags e2e -count=1 -v ./e2e

# Known vulnerabilities: Go code paths actually called, Python environment, npm tree.
audit:
	cd go && go run golang.org/x/vuln/cmd/govulncheck@v1.8.0 ./...
	cd python && $(PY) -m pip_audit --skip-editable --cache-dir ../.tmp/pip-audit
	cd js && npm audit --audit-level=low

ci: setup lint test e2e
