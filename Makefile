# vgi-bluesky -- dev and test targets. `make help` lists them.
#
#   make check       # everything CI's check + SQL jobs run (lint, pytest, SQL over every transport)
#   make test-sql    # test/sql/*.test through DuckDB: make test-sql TRANSPORT=stdio|launch|http
#
# ci/run-integration.sh (adapted from the vgi-starter python template) runs
# test/sql/*.test through the pinned haybarn-unittest and the signed community
# `vgi` extension, against ci/worker, with the API pointed at the local mock in
# test/mock_api/server.py -- so the SQL suite is offline and deterministic.
# The live pytest tests stay opt-in (`uv run pytest -m live`).

HAYBARN_VERSION  ?= 2.0.1rc3
WORKER           := $(CURDIR)/ci/worker
TOOLS            := $(CURDIR)/.tools
HAYBARN_UNITTEST := $(TOOLS)/bin/haybarn-unittest
TRANSPORT        ?= stdio

.DEFAULT_GOAL := help
.PHONY: help build tools lint test test-unit test-sql test-sql-all check

help: ## List the targets
	@awk 'BEGIN{FS=":.*## "} /^[a-zA-Z_-]+:.*## /{printf "  %-14s %s\n",$$1,$$2}' $(MAKEFILE_LIST)

build: ## Install the worker and dev tools into .venv
	uv sync --all-extras

tools: ## Install the pinned SQL test runner into .tools
	@test -x $(HAYBARN_UNITTEST) || { uv venv -q $(TOOLS) && uv pip install -q -p $(TOOLS) haybarn-unittest==$(HAYBARN_VERSION); }

lint: build ## Ruff lint + format check
	uv run ruff check .
	uv run ruff format --check .

test-unit: build ## Offline pytest suite (live tests are opt-in: uv run pytest -m live)
	uv run pytest -q

test-sql: build tools ## SQL tests through DuckDB: make test-sql TRANSPORT=stdio|launch|http
	WORKER=$(WORKER) HAYBARN_UNITTEST=$(HAYBARN_UNITTEST) TRANSPORT=$(TRANSPORT) PYTHON="uv run --no-project python" ci/run-integration.sh

test-sql-all: ## SQL tests over every transport
	$(MAKE) test-sql TRANSPORT=stdio
	$(MAKE) test-sql TRANSPORT=launch
	$(MAKE) test-sql TRANSPORT=http

test: test-unit test-sql-all ## Unit + SQL tests

check: lint test ## Everything CI runs. Done = this passes.
