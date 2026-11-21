PYTEST := uv run pytest -n auto --maxprocesses 8

.PHONY: default
default: test

.PHONY: install
install:
	uv sync
	uv run prek install

.PHONY: format
format:
	uv run ruff check --fix
	uv run ruff format

.PHONY: lint
lint:
	uv run ruff check
	uv run ruff format --check
	uv run basedpyright

.PHONY: test
test:
	$(PYTEST)

.PHONY: test-cov
test-cov:
	COVERAGE_CORE=ctrace $(PYTEST) --cov --cov-context=test --cov-report=html --cov-fail-under=0

.PHONY: test-ci
test-ci:
	$(PYTEST) --cov --cov-report=term --cov-report=xml
