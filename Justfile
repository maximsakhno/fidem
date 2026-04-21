default:
    @just --list

install:
    uv sync --group dev
    uv run prek install

format:
    uv run ruff check src tests --fix
    uv run ruff format src tests

lint:
    uv run ruff check src tests
    uv run ruff format src tests --check
    uv run basedpyright src tests

test *args:
    uv run pytest {{args}}

cov-flags := "--cov=src/fidem --cov-branch --cov-report=term-missing"

test-cov:
    just --justfile {{justfile()}} test {{cov-flags}} --cov-report=html

test-ci:
    just --justfile {{justfile()}} test {{cov-flags}} --cov-report=xml --cov-fail-under=85
