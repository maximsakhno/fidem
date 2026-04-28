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

test:
    uv run pytest --no-cov

test-cov:
    uv run pytest --cov-report=html

test-ci:
    uv run pytest --cov-report=xml --cov-fail-under=85
